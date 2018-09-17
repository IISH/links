package modulemain;

import java.io.File;
import java.io.PrintStream;

import java.util.ArrayList;

import java.sql.ResultSet;

import javax.swing.JTextArea;
import javax.swing.JTextField;

import com.google.common.base.Strings;

import connectors.MySqlConnector;
import dataset.Options;
import general.Functions;
import general.PrintLogger;
import prematch.Lvs;

/**
 * @author Omar Azouguagh
 * @author Fons Laan
 *
 * <p/>
 * FL-29-Jul-2014 Remove hard-code usr's/pwd's
 * FL-17-Nov-2014 Processing all of links_cleaned: not selecting by "... AND id_source = ..."
 * FL-10-Dec-2014 links_base table moved from links_base db to links_prematch db
 * FL-18-Feb-2015 Both str & int names in freq_* & ls_* tables
 * FL-13-Mar-2015 Split firstnames: (also) make firstname4 free of spaces
 * FL-02-Feb-2016 Show # of updated records when links_base is re-created
 * FL-31-Jan-2017 Two more name frequency count queries
 * FL-21-Aug-2017 NamesToNumbers per id_source & rmtype
 * FL-04-Sep-2017 Frequency tables per source & rmtype
 * FL-28-Aug-2018 Cosmetic update in links_base update
 */

public class LinksPrematch extends Thread
{
    private Options opts;

    private boolean debug;
    private PrintLogger plog;

    private String db_url;
    private String db_user;
    private String db_pass;
    private String sourceIdsGui;
    private String RMtypes;

    private boolean bSplitFirstames;
    private boolean bFrequencyTables;
    private boolean bStandardization;
    private boolean bNamesToNos;
    private boolean bBaseTable;
    private boolean bLevenshtein;
    private boolean bExactMatches;

    private JTextField outputLine;
    private JTextArea  outputArea;

    private MySqlConnector conCleaned;
    private MySqlConnector conPrematch;
    private MySqlConnector conTemp;

    private java.io.FileWriter writerFirstname;

    /**
     * Constructor
     *
     * @param opts
     * @param outputLine
     * @param outputArea
     * @param bSplitFirstnames
     * @param bFrequencyTables
     * @param bStandardization
     * @param bNamesToNos
     * @param bBaseTable
     * @param bLevenshtein
     * @param bExactMatches
     * @throws Exception
     */
    public LinksPrematch
    (
        Options opts,

        JTextField outputLine,
        JTextArea  outputArea,

        boolean bSplitFirstnames,
        boolean bFrequencyTables,
        boolean bStandardization,
        boolean bNamesToNos,
        boolean bBaseTable,
        boolean bLevenshtein,
        boolean bExactMatches
    )
    throws Exception
    {
        this.opts = opts;

        this.debug = opts.isDbgPrematch();
        this.plog  = opts.getLogger();

        this.db_url  = opts.getDb_url();
        this.db_user = opts.getDb_user();
        this.db_pass = opts.getDb_pass();

        this.sourceIdsGui = opts.getSourceIds();
        this.RMtypes      = opts.getRMtypes();

        this.bSplitFirstames  = bSplitFirstnames;
        this.bFrequencyTables = bFrequencyTables;
        this.bStandardization = bStandardization;
        this.bNamesToNos      = bNamesToNos;
        this.bBaseTable       = bBaseTable;
        this.bLevenshtein     = bLevenshtein;
        this.bExactMatches    = bExactMatches;

        this.outputLine = outputLine;
        this.outputArea  = outputArea;

        conCleaned   = new MySqlConnector( db_url, "links_cleaned",   db_user, db_pass );
        conPrematch  = new MySqlConnector( db_url, "links_prematch",  db_user, db_pass );
        conTemp      = new MySqlConnector( db_url, "links_temp",      db_user, db_pass );
    }


    /**
     *
     */
    @Override
    public void run()
    {
        System.out.println( "LinksPrematch/run()" );
        outputLine.setText( "" );
        outputArea.setText( "" );

        long timeStart = System.currentTimeMillis();

        showMessage( "LinksPrematch/run()", false, true );
        showMessage( "debug: " + debug, false, true );

        showMessage( "db_url: "  + db_url,  false, true );
        showMessage( "db_user: " + db_user, false, true );
        showMessage( "db_pass: " + db_pass, false, true );

        showMessage( "sourceIdsGui: " + sourceIdsGui, false, true );
        showMessage( "RMtypes: " + RMtypes, false, true );

        try
        {
            doSplitFirstnames( debug, bSplitFirstames );

            String idsStr[] = sourceIdsGui.split( " " );

            String rmtype = "";
            if( RMtypes.length() > 0 ) {
                String rmtStr[] = RMtypes.split( " " );
                rmtype = rmtStr[ 0 ];       // only use the first rmtype !
            }

            if( debug ) {
                String msg = String.format( "sourceIds: %s, rmtype: %s", sourceIdsGui, rmtype );
                showMessage( msg, false, true );
            }

            if( Strings.isNullOrEmpty( sourceIdsGui ) && Strings.isNullOrEmpty( rmtype  ) )
            {
                // re-create the freq_* tables
                doFrequencyTablesFull( debug, bFrequencyTables );
            }
            else
            {
                for( String source : idsStr )
                { doFrequencyTablesUpdate( debug, bFrequencyTables, source, rmtype ); }
            }

            doStandardization( debug, bStandardization );

            if( Strings.isNullOrEmpty( sourceIdsGui ) && Strings.isNullOrEmpty( rmtype  ) )
            {
                String source = "";
                doNamesToNumbers( debug, bNamesToNos, source, rmtype );
                doCreateNewBaseTable( debug, bBaseTable, source, rmtype );
            }                      // new links_base
            else
            {
                for( String source : idsStr )
                { doNamesToNumbers( debug, bNamesToNos, source, rmtype ); }     // update per source & rmtype

                for( String source : idsStr )
                { doCreateNewBaseTable( debug, bBaseTable, source, rmtype ); }  // update per source & rmtype
            }


            //doLevenshtein( debug, bLevenshtein, bExactMatches );        // now here in main
            String funcname = "doLevenshtein";

            if( ! bLevenshtein ) { showMessage( "Skipping " + funcname, false, true ); }
            else
            {
                if( debug ) { System.out.println( funcname ); }
                // prematch.Lvs is a separate thread, so timing should be done there internally
                showMessage( funcname + ", using 6 threads", false, true );

                //the 5th parameter (boolean) specifies 'strict' or 'non-strict' Levenshtein method.
                Lvs lvs1 = new Lvs( debug, conPrematch, "links_prematch", "freq_firstname",  "strict", bExactMatches, outputLine, outputArea, plog );
                Lvs lvs2 = new Lvs( debug, conPrematch, "links_prematch", "freq_firstname",  "normal", bExactMatches, outputLine, outputArea, plog );
                Lvs lvs3 = new Lvs( debug, conPrematch, "links_prematch", "freq_firstname",  "first",  bExactMatches, outputLine, outputArea, plog );

                Lvs lvs4 = new Lvs( debug, conPrematch, "links_prematch", "freq_familyname", "strict", bExactMatches, outputLine, outputArea, plog );
                Lvs lvs5 = new Lvs( debug, conPrematch, "links_prematch", "freq_familyname", "normal", bExactMatches, outputLine, outputArea, plog );
                Lvs lvs6 = new Lvs( debug, conPrematch, "links_prematch", "freq_familyname", "first",  bExactMatches, outputLine, outputArea, plog );

                lvs1.start();    // firstname   table: ls_firstname
                lvs2.start();    // firstname   table: ls_firstname_strict
                lvs3.start();    // firstname   table: ls_firstname_first

                lvs4.start();    // familyname  table: ls_familyname
                lvs5.start();    // familyname  table: ls_familyname_strict
                lvs6.start();    // familyname  table: ls_familyname_first

                lvs1.join();
                lvs2.join();
                lvs3.join();

                lvs4.join();
                lvs5.join();
                lvs6.join();
            }

            showMessage( "", false, true );
            String msg = String.format( "Prematching Finished." );
            elapsedShowMessage( msg, timeStart, System.currentTimeMillis() );
            System.out.println( msg );

        } catch( Exception ex ) { showMessage( ex.getMessage(), false, true ); }

    } // run


    /*===< Helper functions >=================================================*/

    /**
     * @param logText
     * @param isMinOnly
     * @param newLine
     */
    private void showMessage( String logText, boolean isMinOnly, boolean newLine )
    {
        outputLine.setText( logText );

        if( !isMinOnly ) {
            String newLineToken = "";
            if( newLine ) {
                newLineToken = "\r\n";
            }

            outputArea.append( logText + newLineToken );
            //System.out.printf( "%s%s", logText, newLineToken );
            try { plog.show( logText ); }
            catch( Exception ex ) {
                System.out.println( ex.getMessage() );
                ex.printStackTrace( new PrintStream( System.out ) );
            }
        }
    } // showMessage


    /**
     * @param msg
     * @param start
     * @param stop
     */
    private void elapsedShowMessage( String msg, long start, long stop )
    {
        String elapsed = Functions.millisec2hms( start, stop );
        showMessage( msg + " " + elapsed, false, true );
    } // elapsedShowMessage


    private void showMessage_nl()
    {
        String newLineToken = "\r\n";

        outputArea.append( newLineToken );

        try { plog.show( "" ); }
        catch( Exception ex ) {
            System.out.println( ex.getMessage() );
            ex.printStackTrace( new PrintStream( System.out ) );
        }
    } // showMessage_nl


    /*---< End Helper functions >---------------------------------------------*/


    /*===< functions corresponding to GUI Cleaning options >==================*/

    /*---< Split Firstnames >-------------------------------------------------*/

    /**
     * @param debug
     * @param go
     * @throws Exception 
     */
    public void doSplitFirstnames( boolean debug, boolean go ) throws Exception
    {
        String funcname = "doSplitFirstnames";

        if( !go ) {
            showMessage( "Skipping " + funcname, false, true );
            return;
        }

        if( debug ) { System.out.println( funcname ); }

        long funcstart = System.currentTimeMillis();
        showMessage( funcname + "...", false, true );

        if( debug ) {
            String query = "SELECT COUNT(*) AS count FROM person_c WHERE firstname IS NOT NULL AND firstname <> '';";
            showMessage( query, false, true);
            ResultSet rsFirstNameCount = conCleaned.runQueryWithResult( query );
            rsFirstNameCount.first();
            int count = rsFirstNameCount.getInt( "count" );
            System.out.println( "count: " + count );
            showMessage( "count: " + count, false, true);
        }

        String query = "SELECT id_person , firstname FROM person_c WHERE firstname IS NOT NULL AND firstname <> '';";
        if( debug ) { showMessage( query, false, true); }

        ResultSet rsFirstName = conCleaned.runQueryWithResult( query );
        if( debug ) { showMessage( "query done", false, true); }
        // A communication failure here, may be caused by too little memory granted for the JVM; see the startup script

        //removeFirstnameFile();
        //removeFirstnameTable();

        if( debug ) { showMessage( "createTempFirstname()", false, true); }
        createTempFirstname( debug );
        if( debug ) { showMessage( "createTempFirstnameFile()", false, true); }
        createTempFirstnameFile();

        int count = 0;

        while( rsFirstName.next() )
        {
            count++;

            int id_person        = rsFirstName.getInt( "id_person" );
            String firstnames_in = rsFirstName.getString( "firstname" );

            // remove leading and trailing spaces, and replace serried spaces by just 1
            String firstnames = firstnames_in.replaceAll( "\\s+", " " ).trim();

            // limit -- This controls the number of times the pattern is applied and therefore affects the length of the resulting array
            String[] fn = firstnames.split( " ", 5 );   // using '5' makes the first 4 components free of spaces

            String p0 = "";
            String p1 = "";
            String p2 = "";
            String p3 = "";

            if( fn.length > 0 ) {
                p0 = fn[ 0 ];

                if( fn.length > 1 ) {
                    p1 = fn[ 1 ];

                    if( fn.length > 2 ) {
                        p2 = fn[ 2 ];

                        if( fn.length >  3) {
                            p3 = fn[ 3 ];
                        }
                    }
                }
            }

            String q = id_person + "," + p0 + "," + p1 + "," + p2 + "," + p3;
            //System.out.println( q );

            writerFirstname.write( q + "\n" );
        }

        writerFirstname.close();

        rsFirstName.close();
        rsFirstName = null;

        loadFirstnameToTable( debug );
        updateFirstnameToPersonC( debug );

        removeFirstnameFile();
        removeFirstnameTable();

        showMessage( count + " firstnames split", false, true );
        elapsedShowMessage( funcname, funcstart, System.currentTimeMillis() );
        showMessage_nl();
    } // doSplitFirstNames


    /**
     * @param debug
     * @throws Exception
     */
    private void createTempFirstname( boolean debug ) throws Exception
    {
        String table_name = "links_temp.firstname_t_split";

        String[] queries =
        {
            "DROP TABLE IF EXISTS " + table_name,

            "CREATE  TABLE " + table_name + " ("
                + " person_id INT UNSIGNED NOT NULL AUTO_INCREMENT ,"
                + " firstname1 VARCHAR(30) NULL ,"
                + " firstname2 VARCHAR(30) NULL ,"
                + " firstname3 VARCHAR(30) NULL ,"
                + " firstname4 VARCHAR(30) NULL ,"
                + " PRIMARY KEY (person_id) );"
        };

        for( String query : queries ) {
            if( debug ) { showMessage( query, false, true); }
            conTemp.runQuery( query );
        }
    } // createTempFirstname


    /**
     * @throws Exception
     */
    private void createTempFirstnameFile() throws Exception
    {
        String filename = "firstname_t_split.csv";
        showMessage( "Creating file " + filename, false, true);
        writerFirstname = new java.io.FileWriter( filename );
    } // createTempFirstnameFile


    /**
     * @param debug
     * @throws Exception
     */
    private void loadFirstnameToTable( boolean debug ) throws Exception
    {
        showMessage( "Loading CSV data into temp table...", false, true );

        String query = "LOAD DATA LOCAL INFILE 'firstname_t_split.csv'"
                + " INTO TABLE firstname_t_split FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n'"
                + " ( person_id , firstname1 , firstname2 , firstname3 , firstname4 );";

        if( debug ) { showMessage( query, false, true); }
        conTemp.runQuery( query );
    } // loadFirstnameToTable


    /**
     * @param debug
     * @throws Exception
     */
    private void updateFirstnameToPersonC( boolean debug ) throws Exception
    {
        showMessage( "Moving first names from temp table to person_c...", false, true );

        String query = "UPDATE links_cleaned.person_c, links_temp.firstname_t_split"
                + " SET "
                + " links_cleaned.person_c.firstname1 = links_temp.firstname_t_split.firstname1 ,"
                + " links_cleaned.person_c.firstname2 = links_temp.firstname_t_split.firstname2 ,"
                + " links_cleaned.person_c.firstname3 = links_temp.firstname_t_split.firstname3 ,"
                + " links_cleaned.person_c.firstname4 = links_temp.firstname_t_split.firstname4"
                + " WHERE links_cleaned.person_c.id_person = links_temp.firstname_t_split.person_id;";

        if( debug ) { showMessage( query, false, true); }
        conTemp.runQuery( query );
    } // updateFirstnameToPersonC(


    /**
     * @throws Exception
     */
    public void removeFirstnameFile() throws Exception
    {
        String filename = "firstname_t_split.csv";
        showMessage( "Removing file " + filename, false, true);
        File file = new File( filename );
        file.delete();
    } // removeFirstnameFile


    /**
     * @throws Exception
     */
    public void removeFirstnameTable() throws Exception
    {
        String tablename = "firstname_t_split";
        showMessage( "Removing table " + tablename, false, true);
        String query = "DROP TABLE " + tablename + ";";
        conTemp.runQuery( query );
    } // removeFirstnameTable

    /*---< Frequency Tables >-------------------------------------------------*/

    /**
     * @param debug
     * @param go
     * @throws Exception
     */
    public void doFrequencyTablesOld( boolean debug, boolean go ) throws Exception
    {
        String funcname = "doFrequencyTablesOld";

        if( !go ) {
            showMessage( "Skipping " + funcname, false, true );
            return;
        }

        if( debug ) { System.out.println( funcname ); }

        long funcstart = System.currentTimeMillis();
        showMessage( funcname + "...", false, true );

        //dropCreateDbFrequency();
        //truncateFreqTables();

        // Execute queries
        int nqFirst = 1;
        int nqLast = 20;
        String qPrefix = "FrequencyTables/FrequencyTables_q";

        for( int n = nqFirst; n <= nqLast; n++ )
        {
            long start = System.currentTimeMillis();
            String qPath = String.format( qPrefix + "%02d", n );
            if( debug ) { showMessage( qPath, false, true ); }
            String query = LinksSpecific.getSqlQuery( qPath );

            String msg = "";
            if( query == null || query.isEmpty() ) { showMessage( "Empty query " + qPath, false, true ); }
            else {
                msg = "query " + n + "-of-" + nqLast;
                showMessage( msg + "...", false, true );
                if( debug ) { showMessage( query, false, true ); }
                conPrematch.runQuery( query );
            }
            elapsedShowMessage( msg, start, System.currentTimeMillis() );
        }

        elapsedShowMessage( funcname, funcstart, System.currentTimeMillis() );
        showMessage_nl();
    } // doFrequencyTablesOld


    /**
     * @param debug
     * @param go
     * @throws Exception
     */
    public void doFrequencyTablesFull( boolean debug, boolean go ) throws Exception
    {
        String funcname = "doFrequencyTablesFull";

        if( !go ) {
            showMessage( "Skipping " + funcname, false, true );
            return;
        }

        if( debug ) { System.out.println( funcname ); }

        long funcstart = System.currentTimeMillis();
        showMessage( funcname + "...", false, true );

        String[] queries =
        {
            // query 01
            "CREATE TABLE IF NOT EXISTS links_prematch.freq_familyname ("
                + " id INT UNSIGNED NOT NULL AUTO_INCREMENT ,"
                + " name_str VARCHAR(100) NULL ,"
                + " frequency INT UNSIGNED NULL ,"
                + " PRIMARY KEY (id) ,"
                + " INDEX `name_str` (`name_str`)"
                + " ) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin ;",

            // query 02
            "CREATE TABLE IF NOT EXISTS links_prematch.freq_firstname ("
                + " id INT UNSIGNED NOT NULL AUTO_INCREMENT ,"
                + " name_str VARCHAR(30) NULL ,"
                + " frequency INT UNSIGNED NULL ,"
                + " PRIMARY KEY (id) ,"
                + " INDEX `name_str` (`name_str`)"
                + " ) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin ;",

            // query 03
            "CREATE TABLE IF NOT EXISTS links_prematch.freq_firstname_sex ("
                + " id INT UNSIGNED NOT NULL AUTO_INCREMENT ,"
                + " name_str VARCHAR(30) NULL ,"
                + " sex VARCHAR(2) NULL ,"
                + " frequency INT UNSIGNED NULL ,"
                + " PRIMARY KEY (id) ,"
                + " INDEX `name_str` (`name_str`)"
                + " ) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin ;",

            // query 04
            "CREATE TABLE IF NOT EXISTS links_prematch.freq_firstname_sex_tmp ("
                + " id INT UNSIGNED NOT NULL AUTO_INCREMENT ,"
                + " name_str VARCHAR(30) NULL ,"
                + " sex VARCHAR(2) NULL ,"
                + " PRIMARY KEY (id) ,"
                + " INDEX `name_str` (`name_str`)"
                + " ) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin ;",

            // query 05
            "TRUNCATE TABLE links_prematch.freq_familyname ;",

            // query 06
            "ALTER TABLE links_prematch.freq_familyname AUTO_INCREMENT = 1 ;",

            // query 07
            "TRUNCATE TABLE links_prematch.freq_firstname ;",

            // query 08
            "ALTER TABLE links_prematch.freq_firstname AUTO_INCREMENT = 1 ;",

            // query 09
            "TRUNCATE TABLE links_prematch.freq_firstname_sex ;",

            // query 10
            "ALTER TABLE links_prematch.freq_firstname_sex AUTO_INCREMENT = 1 ;",

            // query 11
            "TRUNCATE TABLE links_prematch.freq_firstname_sex_tmp ;",

            // query 12
            "ALTER TABLE links_prematch.freq_firstname_sex_tmp AUTO_INCREMENT = 1 ;",

            // query 13
            "INSERT INTO links_prematch.freq_familyname( name_str , frequency )"
                + " SELECT familyname , COUNT(*) AS frequency"
                + " FROM links_cleaned.person_c"
                + " WHERE familyname IS NOT NULL AND familyname <> 'null' AND familyname <> ''"
                + " GROUP BY familyname ;",

            // query 14
            "INSERT INTO links_prematch.freq_firstname_sex_tmp( name_str , sex )"
                + " SELECT firstname1 , sex"
                + " FROM links_cleaned.person_c"
                + " WHERE firstname1 IS NOT NULL AND firstname1 <> 'null' AND firstname1 <> '' ;",

            // query 15
            "INSERT INTO links_prematch.freq_firstname_sex_tmp( name_str , sex )"
                + " SELECT firstname2 , sex"
                + " FROM links_cleaned.person_c"
                + " WHERE firstname2 IS NOT NULL AND firstname2 <> 'null' AND firstname2 <> '' ;",

            // query 16
            "INSERT INTO links_prematch.freq_firstname_sex_tmp( name_str , sex )"
                + " SELECT firstname3 , sex"
                + " FROM links_cleaned.person_c"
                + " WHERE firstname3 IS NOT NULL AND firstname3 <> 'null' AND firstname3 <> '' ;",

            // query 17
            "INSERT INTO links_prematch.freq_firstname_sex_tmp( name_str , sex )"
                + " SELECT firstname4 , sex"
                + " FROM links_cleaned.person_c"
                + " WHERE firstname4 IS NOT NULL AND firstname4 <> 'null' AND firstname4 <> '' ;",

            // query 18
            "INSERT INTO links_prematch.freq_firstname ( name_str , frequency )"
                + " SELECT name_str , COUNT(*) AS frequency"
                + " FROM links_prematch.freq_firstname_sex_tmp"
                + " GROUP BY name_str ;",

            // query 19
            "INSERT INTO links_prematch.freq_firstname_sex ( name_str , sex, frequency )"
                + " SELECT name_str , sex, COUNT(*) AS frequency"
                + " FROM links_prematch.freq_firstname_sex_tmp"
                + " GROUP BY sex , name_str ;",

            // query 20
            "DROP TABLE links_prematch.freq_firstname_sex_tmp ;"
        };

        int nq = 0;
        for( String query : queries )
        {
            nq++;
            long start = System.currentTimeMillis();
            String msg = "query " + nq + "-of-" + queries.length;
            showMessage( msg + "...", false, true );
            System.out.println( query );
            if( debug ) { showMessage( query, false, true ); }

            conPrematch.runQuery( query );
            elapsedShowMessage( msg, start, System.currentTimeMillis() );
        }

        elapsedShowMessage( funcname, funcstart, System.currentTimeMillis() );
        showMessage_nl();
    } // doFrequencyTablesFull


    /**
     * @param debug
     * @param go
     * @param source
     * @param rmtype
     * @throws Exception
     */
    public void doFrequencyTablesUpdate( boolean debug, boolean go, String source, String rmtype ) throws Exception
    {
        String funcname = "doFrequencyTablesUpdate";

        if( !go ) {
            showMessage( "Skipping " + funcname, false, true );
            return;
        }

        if( debug ) { System.out.println( funcname ); }

        long funcstart = System.currentTimeMillis();
        showMessage( funcname + "...", false, true );

        String query_1 = "CREATE TABLE IF NOT EXISTS links_prematch.freq_firstname_sex_tmp ("
            + " id INT UNSIGNED NOT NULL AUTO_INCREMENT ,"
            + " name_str VARCHAR(30) NULL ,"
            + " sex VARCHAR(2) NULL ,"
            + " PRIMARY KEY (id) ,"
            + " INDEX `name_str` (`name_str`)"
            + " ) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin";

        String query_2 = "TRUNCATE TABLE links_prematch.freq_firstname_sex_tmp";

        String query_3 = "ALTER TABLE links_prematch.freq_firstname_sex_tmp AUTO_INCREMENT = 1";

        String query_4 = "INSERT INTO links_prematch.freq_firstname_sex_tmp( name_str , sex )"
            + " SELECT firstname1 , sex"
            + " FROM links_cleaned.person_c"
            + " WHERE firstname1 IS NOT NULL AND firstname1 <> 'null' AND firstname1 <> '' ";
        query_4 += " AND person_c.id_source = " + source;
        if ( ! rmtype.isEmpty() ) { query_4 += " AND registration_maintype = " + rmtype; }

        String query_5 = "INSERT INTO links_prematch.freq_firstname_sex_tmp( name_str , sex )"
            + " SELECT firstname2 , sex"
            + " FROM links_cleaned.person_c"
            + " WHERE firstname2 IS NOT NULL AND firstname2 <> 'null' AND firstname2 <> '' ";
        query_5 += " AND person_c.id_source = " + source;
        if ( ! rmtype.isEmpty() ) { query_5 += " AND registration_maintype = " + rmtype; }

        String query_6 = "INSERT INTO links_prematch.freq_firstname_sex_tmp( name_str , sex )"
            + " SELECT firstname3 , sex"
            + " FROM links_cleaned.person_c"
            + " WHERE firstname3 IS NOT NULL AND firstname3 <> 'null' AND firstname3 <> '' ";
        query_6 += " AND person_c.id_source = " + source;
        if ( ! rmtype.isEmpty() ) { query_6 += " AND registration_maintype = " + rmtype; }

        String query_7 = "INSERT INTO links_prematch.freq_firstname_sex_tmp( name_str , sex )"
            + " SELECT firstname4 , sex"
            + " FROM links_cleaned.person_c"
            + " WHERE firstname4 IS NOT NULL AND firstname4 <> 'null' AND firstname4 <> '' ";
        query_7 += " AND person_c.id_source = " + source;
        if ( ! rmtype.isEmpty() ) { query_7 += " AND registration_maintype = " + rmtype; }

        String query_8 = "INSERT IGNORE INTO links_prematch.freq_firstname ( name_str , frequency )"
            + " SELECT name_str , 1"
            + " FROM links_prematch.freq_firstname_sex_tmp";

        String query_9 = "INSERT IGNORE INTO links_prematch.freq_firstname_sex ( name_str , sex, frequency )"
            + " SELECT name_str , sex, 1"
            + " FROM links_prematch.freq_firstname_sex_tmp";

        String query_10 = "INSERT IGNORE INTO links_prematch.freq_familyname( name_str , frequency )"
            + " SELECT familyname , 1"
            + " FROM links_cleaned.person_c"
            + " WHERE familyname IS NOT NULL AND familyname <> 'null' AND familyname <> '' ";
        query_10 += " AND person_c.id_source = " + source;
        if ( ! rmtype.isEmpty() ) { query_10 += " AND registration_maintype = " + rmtype; }

        String query_11 = "DROP TABLE links_prematch.freq_firstname_sex_tmp";


        String[] queries = {
            query_1, query_2, query_3, query_4, query_5, query_6, query_7, query_8, query_9, query_10, query_11
        };

        int nq = 0;
        for( String query : queries )
        {
            nq++;
            long start = System.currentTimeMillis();
            String msg = "query " + nq + "-of-" + queries.length;
            showMessage( msg + "...", false, true );
            System.out.println( query );
            if( debug ) { showMessage( query, false, true ); }

            //conPrematch.runQuery( query );
            elapsedShowMessage( msg, start, System.currentTimeMillis() );
        }

        elapsedShowMessage( funcname, funcstart, System.currentTimeMillis() );
        showMessage_nl();
    } // doFrequencyTablesUpdate

    /*---< Automatic Standardization >----------------------------------------*/

    /**
     * @param debug
     * @param go
     * @throws Exception
     */
    public void doStandardization( boolean debug, boolean go )
    throws Exception
    {
        String funcname = "doStandardization";

        if( !go ) {
            showMessage( "Skipping " + funcname, false, true );
            return;
        }

        go = false;
        if( !go ) {
            showMessage( "Automatic standardization is currently not enabled", false, true );
            return;
        }

        if( debug ) { System.out.println( funcname ); }

        long funcstart = System.currentTimeMillis();
        showMessage( funcname + "...", false, true );

        // The 18 standardization queries below are applied to freq_familyname_and freq_firstnames.
        // Copy freq_familyname_and freq_firstnames to freq_familyname_cpy and freq_firstnames_cpy
        String[] queriesFamilyname =
        {
            "DROP TABLE IF EXISTS links_prematch.freq_familyname_cpy;",
            "CREATE TABLE links_prematch.freq_familyname_cpy LIKE links_prematch.freq_familyname;",
            "ALTER TABLE  links_prematch.freq_familyname_cpy DISABLE KEYS;",
            "INSERT INTO  links_prematch.freq_familyname_cpy SELECT * FROM links_prematch.freq_familyname;",
            "ALTER TABLE  links_prematch.freq_familyname_cpy ENABLE KEYS;",
        };

        // run queries
        for( String q : queriesFamilyname ) {
            if( debug ) { showMessage( q, false, true ); }
            conPrematch.runQuery( q );
        }

        String[] queriesFirstname =
        {
            "DROP TABLE IF EXISTS links_prematch.freq_firstnames_cpy;",
            "CREATE TABLE links_prematch.freq_firstnames_cpy LIKE links_prematch.freq_firstnames;",
            "ALTER TABLE  links_prematch.freq_firstnames_cpy DISABLE KEYS;",
            "INSERT INTO  links_prematch.freq_firstnames_cpy SELECT * FROM links_prematch.freq_firstnames;",
            "ALTER TABLE  links_prematch.freq_firstnames_cpy ENABLE KEYS;"
        };

        // run queries
        for( String q : queriesFirstname ) {
            if( debug ) { showMessage( q, false, true ); }
            conPrematch.runQuery( q );
        }


        // THESE QUERIES DO THE REAL WORK,
        // enable them for automatic levenshtein standardization of infrequent names
        // Execute queries
        /*
        int nqFirst = 1;
        int nqLast = 18;    // used to be 28 ?
        String qPrefix = "FrequencyTables/FrequencyTables_q";

        for (int n = nqFirst; n <= nqLast; n++) {
            String qPath = String.format(qPrefix + "%02d", n);
            showMessage("Running query " + qPath, false, true);

            String query = LinksSpecific.getSqlQuery(qPath);
            if (debug) {
                showMessage(query, false, true);
            }

            conPrematch.runQuery(query);
        }
        */

        elapsedShowMessage( funcname, funcstart, System.currentTimeMillis() );
        showMessage_nl();
    } // doStandardization(


    /**
     * @throws Exception
     */
    /*
    public void doUniqueNameTablesTemp() throws Exception
    {
        //if( debug ) { System.out.println( funcname ); }

        //dropCreateDbFrequency();

        // Execute queries
        int nqFirst = 1;
        int nqLast = 18;
        String qPrefix = "FrequencyTablesTemp/FrequencyTablesTemp_q";

        for( int n = nqFirst; n <= nqLast; n++ ) {
            String qPath = String.format( qPrefix + "%02d", n );
            showMessage( "Running query " + qPath, false, true );

            //System.out.println( qPath );
            String query = LinksSpecific.getSqlQuery( qPath );
            conPrematch.runQuery( query );
        }

    } // doUniqueNameTablesTemp
    */

    /**
     *
     */
    /*
    private void dropCreateDbFrequency() throws Exception
    {
        String qDropFrequency = "DROP SCHEMA links_frequency ;";

        String qCreateFrequency = "CREATE SCHEMA `links_frequency` DEFAULT CHARACTER SET latin1 COLLATE latin1_general_ci ;";

        // run queries
        if( showmsg ) { showMessage( "Dropping database links_frequency", false, true ); }
        conPrematch.runQuery( qDropFrequency );

        if( showmsg ) { showMessage( "Creating database links_frequency", false, true ); }
        conPrematch.runQuery( qCreateFrequency );
    } // dropCreateDbFrequency
    */


    /**
     *
     */
    /*
    private void truncateFreqTables() throws Exception
    {
        String[] queries =
        {
            "TRUNCATE TABLE links_prematch.freq_familyname;",
            "TRUNCATE TABLE links_prematch.freq_familyname_tmp;",
            "TRUNCATE TABLE links_prematch.freq_firstnames;",
            "TRUNCATE TABLE links_prematch.freq_firstnames_sex;",
            "TRUNCATE TABLE links_prematch.freq_firstnames_tmp;"
        };

        for( String q : queries ) {
            if( debug ) { showMessage( q, false, true ); }
            conPrematch.runQuery( q );
        }
    }
    */

    /*---< Names to Numbers >-------------------------------------------------*/

    /**
     * @param debug
     * @param go
     * @throws Exception
     */
    public void doNamesToNumbersOld( boolean debug, boolean go ) throws Exception
    {
        String funcname = "doNamesToNumbers";

        if( !go ) {
            showMessage( "Skipping " + funcname, false, true );
            return;
        }

        if( debug ) { System.out.println( funcname ); }

        long funcstart = System.currentTimeMillis();
        showMessage( funcname + "...", false, true );

        // Execute queries
        int nqFirst = 1;
        int nqLast  = 5;
        String qPrefix = "NamesToNumbers/NameToNumber_q";

        for( int n = nqFirst; n <= nqLast; n++ )
        {
            long start = System.currentTimeMillis();
            String qPath = String.format( qPrefix + "%02d", n );

            String query = LinksSpecific.getSqlQuery( qPath );
            String msg = "query " + n + "-of-" + nqLast;
            showMessage( msg + "...", false, true );
            if( debug ) { showMessage( query, false, true ); }

            conPrematch.runQuery( query );
            elapsedShowMessage( msg, start, System.currentTimeMillis() );
        }

        elapsedShowMessage( funcname, funcstart, System.currentTimeMillis() );
        showMessage_nl();
    } // doNamesToNumbersOld


    /**
     * @param debug
     * @param go
     * @param source
     * @param rmtype
     * @throws Exception
     */
    public void doNamesToNumbers( boolean debug, boolean go, String source, String rmtype ) throws Exception
    {
        String funcname = "doNamesToNumbers";

        if( !go ) {
            showMessage( "Skipping " + funcname, false, true );
            return;
        }

        if( debug ) { System.out.println( funcname ); }

        long funcstart = System.currentTimeMillis();
        showMessage( funcname + "...", false, true );

        String[] queries =
        {
            "UPDATE links_cleaned.person_c, links_prematch.freq_familyname"
                + " SET links_cleaned.person_c.familyname_no = links_prematch.freq_familyname.id"
                + " WHERE links_cleaned.person_c.familyname = links_prematch.freq_familyname.name_str",     // Query 01

            "UPDATE links_cleaned.person_c, links_prematch.freq_firstname"
                + " SET links_cleaned.person_c.firstname1_no = links_prematch.freq_firstname.id"
                + " WHERE links_cleaned.person_c.firstname1 = links_prematch.freq_firstname.name_str",      // Query 02

            "UPDATE links_cleaned.person_c, links_prematch.freq_firstname"
                + " SET links_cleaned.person_c.firstname2_no = links_prematch.freq_firstname.id"
                + " WHERE links_cleaned.person_c.firstname2 = links_prematch.freq_firstname.name_str",      // Query 03

            "UPDATE links_cleaned.person_c, links_prematch.freq_firstname"
                + " SET links_cleaned.person_c.firstname3_no = links_prematch.freq_firstname.id"
                + " WHERE links_cleaned.person_c.firstname3 = links_prematch.freq_firstname.name_str",      //Query 04

            "UPDATE links_cleaned.person_c, links_prematch.freq_firstname"
                + " SET links_cleaned.person_c.firstname4_no = links_prematch.freq_firstname.id"
                + " WHERE links_cleaned.person_c.firstname4 = links_prematch.freq_firstname.name_str"       //Query 05
        };

        int nq = 0;
        for( String query : queries )
        {
            nq++;
            if( ! source.isEmpty() ) { query += String.format( " AND id_source = %s", source ); }
            if( ! rmtype.isEmpty() ) { query += String.format( " AND registration_maintype = %s", rmtype ); }

            long start = System.currentTimeMillis();
            String msg = "query " + nq + "-of-" + queries.length;
            showMessage( msg + "...", false, true );
            System.out.println( query );
            if( debug ) { showMessage( query, false, true ); }

            conPrematch.runQuery( query );
            elapsedShowMessage( msg, start, System.currentTimeMillis() );
        }

        elapsedShowMessage( funcname, funcstart, System.currentTimeMillis() );
        showMessage_nl();
    } // doNamesToNumbers


    /*---< Base Table >-------------------------------------------------------*/

    /**
     * @param debug
     * @param go
     * @throws Exception
     */
    /*
    public void doCreateNewBaseTableOld( boolean debug, boolean go ) throws Exception
    {
        String funcname = "doCreateNewBaseTableOld";

        if( !go ) {
            showMessage( "Skipping " + funcname, false, true );
            return;
        }

        if( debug ) { System.out.println( funcname ); }

        long funcstart = System.currentTimeMillis();
        showMessage( funcname + "...", false, true );

        String qtruncate = "TRUNCATE TABLE links_base";     // We must delete the previous stuff
        showMessage( qtruncate, false, true );
        try { conPrematch.runQuery( qtruncate ); }          // links_base table moved to links_prematch db
        catch( Exception ex ) { showMessage( ex.getMessage(), false, true ); }


        String qPrefix = "FillBaseTable/FillBaseTable_q";
        int nqFirst = 1;
        int nqLast  = 4;

        // 4 queries: Ego, Mother, Father, Partner
        for( int n = nqFirst; n <= nqLast; n++ )
        {
            long start = System.currentTimeMillis();
            String qPath = String.format( qPrefix + "%02d", n );

            String query = LinksSpecific.getSqlQuery( qPath );
            String msg = "query " + n + "-of-" + nqLast;
            showMessage( msg + "...", false, true );

            //if( debug ) { showMessage( query, false, true ); }
            showMessage( query, false, true );

            try {
                int updated = conPrematch.runQueryUpdate( query );
                showMessage( "Number of updated records: " + updated, false, true );
            }
            catch( Exception ex ) { showMessage( ex.getMessage(), false, true ); }
            elapsedShowMessage( msg, start, System.currentTimeMillis() );
        }

        elapsedShowMessage( funcname, funcstart, System.currentTimeMillis() );
        showMessage_nl();
    } // doCreateNewBaseTableOld
    */

    /**
     * @param debug
     * @param go
     * @param source
     * @param rmtype
     * @throws Exception
     */
    public void doCreateNewBaseTable( boolean debug, boolean go, String source, String rmtype ) throws Exception
    {
        String funcname = "doCreateNewBaseTable";

        if( !go ) {
            showMessage( "Skipping " + funcname, false, true );
            return;
        }

        if( debug ) { System.out.println( funcname ); }

        long funcstart = System.currentTimeMillis();
        showMessage( funcname + "...", false, true );

        boolean have_source;
        boolean have_rmtype;

        if( Strings.isNullOrEmpty( source ) ) { have_source = false; }
        else { have_source = true; }

        if( Strings.isNullOrEmpty( rmtype ) ) { have_rmtype = false; }
        else { have_rmtype = true; }

        // delete the previous records
        long start_del = System.currentTimeMillis();
        String qdelete = "";
        if( have_source ) {
            qdelete =  "DELETE FROM links_base WHERE id_source = " + source;
            if( have_rmtype ) { qdelete += " AND registration_maintype = " + rmtype; }
        }
        else
        {
            if( have_rmtype ) {
                qdelete += " DELETE FROM links_base WHERE registration_maintype = " + rmtype;
            }
            else { qdelete = "TRUNCATE TABLE links_base"; }
        }

        showMessage( qdelete, false, true );

        try {
            conPrematch.runQuery( qdelete );
            int rowsAffected = conPrematch.runQueryUpdate( qdelete );
            showMessage( "Number of records deleted: " + rowsAffected, false, true );
        }
        catch( Exception ex ) { showMessage( ex.getMessage(), false, true ); }
        elapsedShowMessage( "delete done in", start_del, System.currentTimeMillis() );

        String[] queries = getNewBaseTableQueries( debug, source, rmtype );
        int nupdated = 0;
        int n = 0;

        for( String query : queries )
        {
            n++;
            String msg = "query " + n + "-of-" + queries.length;
            showMessage( msg, false, true );
            System.out.println( "\n" + query );

            long start = System.currentTimeMillis();
            try {
                int count = conPrematch.runQueryUpdate( query );
                if( n == 1 )
                { showMessage( "Number of inserted records from query " + n + ": " + count, false, true ); }
                else
                {
                    nupdated += count;
                    showMessage( "Number of updated records from query " + n + ": " + count, false, true );
                }
            }
            catch( Exception ex ) { showMessage( ex.getMessage(), false, true ); }
            elapsedShowMessage( msg + " done in", start, System.currentTimeMillis() );
        }
        //showMessage( "Total updated records: " + nupdated, false, true );

        elapsedShowMessage( funcname, funcstart, System.currentTimeMillis() );
        showMessage_nl();
    } // doCreateNewBaseTable


     /**
     * @param debug
     * @param source
     * @param rmtype
     * @throws Exception
     */
    public String[] getNewBaseTableQueries( boolean debug, String source, String rmtype )
    throws Exception
    {
        boolean have_source;
        boolean have_rmtype;

        if( Strings.isNullOrEmpty( source ) ) { have_source = false; }
        else { have_source = true; }

        if( Strings.isNullOrEmpty( rmtype ) ) { have_rmtype = false; }
        else { have_rmtype = true; }

        String query1a = ""
            + "INSERT INTO links_prematch.links_base "
            + "( "
            + "id_registration , "
            + "id_source , "
            + "id_persist_registration , "
            + "registration_maintype , "
            + "registration_type , "
            + "extract , "
            + "registration_days , "
            + "registration_location , "
            + "ego_id , "
            + "ego_familyname_fc , "
            + "ego_familyname_prefix , "
            + "ego_familyname_str , "
            + "ego_familyname , "
            + "ego_firstname , "
            + "ego_firstname1_str , "
            + "ego_firstname1 , "
            + "ego_firstname2 , "
            + "ego_firstname3 , "
            + "ego_firstname4 , "
            + "ego_sex , "
            + "ego_birth_min , "
            + "ego_birth_max , "
            + "ego_birth_loc , "
            + "ego_marriage_min , "
            + "ego_marriage_max , "
            + "ego_marriage_loc , "
            + "ego_death_min , "
            + "ego_death_max , "
            + "ego_death_loc , "
            + "ego_role "
            + ") "
            + "SELECT "
            + "links_cleaned.registration_c.id_registration , "
            + "links_cleaned.registration_c.id_source , "
            + "links_cleaned.registration_c.id_persist_registration , "
            + "links_cleaned.registration_c.registration_maintype , "
            + "links_cleaned.registration_c.registration_type , "
            + "links_cleaned.registration_c.extract , "
            + "links_cleaned.registration_c.registration_days , "
            + "links_cleaned.registration_c.registration_location_no , "
            + "links_cleaned.person_c.id_person , "
            + "LEFT( links_cleaned.person_c.familyname, 1 ), "
            + "links_cleaned.person_c.prefix , "
            + "links_cleaned.person_c.familyname , "
            + "links_cleaned.person_c.familyname_no , "
            + "links_cleaned.person_c.firstname , "
            + "links_cleaned.person_c.firstname1 , "
            + "links_cleaned.person_c.firstname1_no , "
            + "links_cleaned.person_c.firstname2_no , "
            + "links_cleaned.person_c.firstname3_no , "
            + "links_cleaned.person_c.firstname4_no , "
            + "links_cleaned.person_c.sex , "
            + "links_cleaned.person_c.birth_min_days , "
            + "links_cleaned.person_c.birth_max_days , "
            + "links_cleaned.person_c.birth_location , "
            + "links_cleaned.person_c.mar_min_days , "
            + "links_cleaned.person_c.mar_max_days , "
            + "links_cleaned.person_c.mar_location , "
            + "links_cleaned.person_c.death_min_days , "
            + "links_cleaned.person_c.death_max_days , "
            + "links_cleaned.person_c.death_location , "
            + "links_cleaned.person_c.role "
            + "FROM links_cleaned.registration_c , links_cleaned.person_c"
            + "";

        if( have_source ) {
            query1a += " WHERE links_cleaned.registration_c.id_source = " + source;
            if( have_rmtype ) { query1a += " AND links_cleaned.registration_c.registration_maintype = " + rmtype; }
            query1a += " AND";
        }
        else
        {
            if( have_rmtype ) {
                query1a += " WHERE links_cleaned.registration_c.registration_maintype = " + rmtype;
                query1a += " AND";
            }
            else { query1a += " WHERE"; }
        }

        String query1b = ""
            + " links_cleaned.registration_c.id_registration = links_cleaned.person_c.id_registration AND ( "
            + " ( links_cleaned.registration_c.registration_maintype = 1 AND ( "
            + "    links_cleaned.person_c.role = 1 OR "
            + "    links_cleaned.person_c.role = 2 OR "
            + "    links_cleaned.person_c.role = 3 ) "
            + " ) OR "
            + " ( links_cleaned.registration_c.registration_maintype = 2 AND ( "
            + "    links_cleaned.person_c.role = 4 OR "
            + "    links_cleaned.person_c.role = 5 OR "
            + "    links_cleaned.person_c.role = 6 OR "
            + "    links_cleaned.person_c.role = 7 OR "
            + "    links_cleaned.person_c.role = 8 OR "
            + "    links_cleaned.person_c.role = 9 ) "
            + " ) OR "
            + " ( links_cleaned.registration_c.registration_maintype = 3 AND ( "
            + "    links_cleaned.person_c.role =  2 OR "
            + "    links_cleaned.person_c.role =  3 OR "
            + "    links_cleaned.person_c.role = 10 OR "
            + "    links_cleaned.person_c.role = 11 ) "
            + " ) "
            + " ) ; ";

        String query1 = query1a + query1b;


        String query2a = ""
            + "UPDATE links_prematch.links_base , links_cleaned.person_c "
            + "SET "
            + "mother_id                = links_cleaned.person_c.id_person , "
            + "mother_familyname_fc     = LEFT( links_cleaned.person_c.familyname, 1) , "
            + "mother_familyname_prefix = links_cleaned.person_c.prefix , "
            + "mother_familyname_str    = links_cleaned.person_c.familyname , "
            + "mother_familyname        = links_cleaned.person_c.familyname_no , "
            + "mother_firstname         = links_cleaned.person_c.firstname , "
            + "mother_firstname1_str    = links_cleaned.person_c.firstname1 , "
            + "mother_firstname1        = links_cleaned.person_c.firstname1_no , "
            + "mother_firstname2        = links_cleaned.person_c.firstname2_no , "
            + "mother_firstname3        = links_cleaned.person_c.firstname3_no , "
            + "mother_firstname4        = links_cleaned.person_c.firstname4_no , "
            + "mother_sex               = links_cleaned.person_c.sex , "
            + "mother_birth_min         = links_cleaned.person_c.birth_min_days , "
            + "mother_birth_max         = links_cleaned.person_c.birth_max_days , "
            + "mother_birth_loc         = links_cleaned.person_c.birth_location , "
            + "mother_marriage_min      = links_cleaned.person_c.mar_min_days , "
            + "mother_marriage_max      = links_cleaned.person_c.mar_max_days , "
            + "mother_marriage_loc      = links_cleaned.person_c.mar_location , "
            + "mother_death_min         = links_cleaned.person_c.death_min_days , "
            + "mother_death_max         = links_cleaned.person_c.death_max_days , "
            + "mother_death_loc         = links_cleaned.person_c.death_location "
            + "";

        if( have_source ) {
            query2a += " WHERE links_prematch.links_base.id_source = " + source;
            if( have_rmtype ) { query2a += " AND links_prematch.links_base.registration_maintype = " + rmtype; }
            query2a += " AND";
        }
        else
        {
            if( have_rmtype ) {
                query2a += " WHERE links_prematch.links_base.registration_maintype = " + rmtype;
                query2a += " AND";
            }
            else { query2a += " WHERE"; }
        }

        String query2b = ""
            + " links_prematch.links_base.id_registration = links_cleaned.person_c.id_registration AND "
            + "( "
            + " ( links_prematch.links_base.registration_maintype = 1 AND links_cleaned.person_c.role = 2 AND links_prematch.links_base.ego_role = 1 ) OR "
            + ""
            + " ( links_prematch.links_base.registration_maintype = 2 AND links_cleaned.person_c.role = 5 AND links_prematch.links_base.ego_role = 4 ) OR "
            + " ( links_prematch.links_base.registration_maintype = 2 AND links_cleaned.person_c.role = 8 AND links_prematch.links_base.ego_role = 7 ) OR "
            + ""
            + " ( links_prematch.links_base.registration_maintype = 3 AND links_cleaned.person_c.role = 2 AND links_prematch.links_base.ego_role = 10 ) "
            + ") ; ";

        String query2 = query2a + query2b;


        String query3a = ""
            + "UPDATE links_prematch.links_base , links_cleaned.person_c "
            + "SET "
            + "father_id                = links_cleaned.person_c.id_person , "
            + "father_familyname_fc     = LEFT( links_cleaned.person_c.familyname, 1) , "
            + "father_familyname_prefix = links_cleaned.person_c.prefix , "
            + "father_familyname_str    = links_cleaned.person_c.familyname , "
            + "father_familyname        = links_cleaned.person_c.familyname_no , "
            + "father_firstname         = links_cleaned.person_c.firstname , "
            + "father_firstname1_str    = links_cleaned.person_c.firstname1 , "
            + "father_firstname1        = links_cleaned.person_c.firstname1_no , "
            + "father_firstname2        = links_cleaned.person_c.firstname2_no , "
            + "father_firstname3        = links_cleaned.person_c.firstname3_no , "
            + "father_firstname4        = links_cleaned.person_c.firstname4_no , "
            + "father_sex               = links_cleaned.person_c.sex , "
            + "father_birth_min         = links_cleaned.person_c.birth_min_days , "
            + "father_birth_max         = links_cleaned.person_c.birth_max_days , "
            + "father_birth_loc         = links_cleaned.person_c.birth_location , "
            + "father_marriage_min      = links_cleaned.person_c.mar_min_days , "
            + "father_marriage_max      = links_cleaned.person_c.mar_max_days , "
            + "father_marriage_loc      = links_cleaned.person_c.mar_location , "
            + "father_death_min         = links_cleaned.person_c.death_min_days , "
            + "father_death_max         = links_cleaned.person_c.death_max_days , "
            + "father_death_loc         = links_cleaned.person_c.death_location "
            + "";

        if( have_source ) {
            query3a += " WHERE links_prematch.links_base.id_source = " + source;
            if( have_rmtype ) { query3a += " AND links_prematch.links_base.registration_maintype = " + rmtype; }
            query3a += " AND";
        }
        else
        {
            if( have_rmtype ) {
                query3a += " WHERE links_prematch.links_base.registration_maintype = " + rmtype;
                query3a += " AND";
            }
            else { query3a += " WHERE"; }
        }

        String query3b = ""
            + " links_prematch.links_base.id_registration = links_cleaned.person_c.id_registration AND "
            + "( "
            + " ( links_prematch.links_base.registration_maintype = 1 AND links_cleaned.person_c.role = 3 AND links_prematch.links_base.ego_role =  1 ) OR "
            + " "
            + " ( links_prematch.links_base.registration_maintype = 2 AND links_cleaned.person_c.role = 6 AND links_prematch.links_base.ego_role =  4 ) OR "
            + " ( links_prematch.links_base.registration_maintype = 2 AND links_cleaned.person_c.role = 9 AND links_prematch.links_base.ego_role =  7 ) OR "
            + ""
            + " ( links_prematch.links_base.registration_maintype = 3 AND links_cleaned.person_c.role = 3 AND links_prematch.links_base.ego_role = 10 ) "
            + " ) ; ";

        String query3 = query3a + query3b;


        String query4a = ""
            + "UPDATE links_prematch.links_base , links_cleaned.person_c "
            + "SET "
            + "partner_id                = links_cleaned.person_c.id_person , "
            + "partner_familyname_fc     = LEFT( links_cleaned.person_c.familyname, 1) , "
            + "partner_familyname_prefix = links_cleaned.person_c.prefix , "
            + "partner_familyname_str    = links_cleaned.person_c.familyname , "
            + "partner_familyname        = links_cleaned.person_c.familyname_no , "
            + "partner_firstname         = links_cleaned.person_c.firstname , "
            + "partner_firstname1_str    = links_cleaned.person_c.firstname1 , "
            + "partner_firstname1        = links_cleaned.person_c.firstname1_no , "
            + "partner_firstname2        = links_cleaned.person_c.firstname2_no , "
            + "partner_firstname3        = links_cleaned.person_c.firstname3_no , "
            + "partner_firstname4        = links_cleaned.person_c.firstname4_no , "
            + "partner_sex               = links_cleaned.person_c.sex , "
            + "partner_birth_min         = links_cleaned.person_c.birth_min_days , "
            + "partner_birth_max         = links_cleaned.person_c.birth_max_days , "
            + "partner_birth_loc         = links_cleaned.person_c.birth_location , "
            + "partner_marriage_min      = links_cleaned.person_c.mar_min_days , "
            + "partner_marriage_max      = links_cleaned.person_c.mar_max_days , "
            + "partner_marriage_loc      = links_cleaned.person_c.mar_location , "
            + "partner_death_min         = links_cleaned.person_c.death_min_days , "
            + "partner_death_max         = links_cleaned.person_c.death_max_days , "
            + "partner_death_loc         = links_cleaned.person_c.death_location "
            + "";

        if( have_source ) {
            query4a += " WHERE links_prematch.links_base.id_source = " + source;
            if( have_rmtype ) { query4a += " AND links_prematch.links_base.registration_maintype = " + rmtype; }
            query4a += " AND";
        }
        else
        {
            if( have_rmtype ) {
                query4a += " WHERE links_prematch.links_base.registration_maintype = " + rmtype;
                query4a += " AND";
            }
            else { query4a += " WHERE"; }
        }

        String query4b = ""
            + " links_prematch.links_base.id_registration = links_cleaned.person_c.id_registration AND "
            + "( "
            + " ( links_prematch.links_base.registration_maintype = 1 AND links_cleaned.person_c.role = 2 AND links_prematch.links_base.ego_role = 3 ) OR "
            + " ( links_prematch.links_base.registration_maintype = 1 AND links_cleaned.person_c.role = 3 AND links_prematch.links_base.ego_role = 2 ) OR "
            + ""
            + " ( links_prematch.links_base.registration_maintype = 2 AND links_cleaned.person_c.role = 4 AND links_prematch.links_base.ego_role = 7 ) OR "
            + " ( links_prematch.links_base.registration_maintype = 2 AND links_cleaned.person_c.role = 7 AND links_prematch.links_base.ego_role = 4 ) OR "
            + " ( links_prematch.links_base.registration_maintype = 2 AND links_cleaned.person_c.role = 5 AND links_prematch.links_base.ego_role = 6 ) OR "
            + " ( links_prematch.links_base.registration_maintype = 2 AND links_cleaned.person_c.role = 6 AND links_prematch.links_base.ego_role = 5 ) OR "
            + " ( links_prematch.links_base.registration_maintype = 2 AND links_cleaned.person_c.role = 8 AND links_prematch.links_base.ego_role = 9 ) OR "
            + " ( links_prematch.links_base.registration_maintype = 2 AND links_cleaned.person_c.role = 9 AND links_prematch.links_base.ego_role = 8 ) OR "
            + ""
            + " ( links_prematch.links_base.registration_maintype = 3 AND links_cleaned.person_c.role =  2 AND links_prematch.links_base.ego_role =  3 ) OR "
            + " ( links_prematch.links_base.registration_maintype = 3 AND links_cleaned.person_c.role =  3 AND links_prematch.links_base.ego_role =  2 ) OR "
            + " ( links_prematch.links_base.registration_maintype = 3 AND links_cleaned.person_c.role = 10 AND links_prematch.links_base.ego_role = 11 ) OR "
            + " ( links_prematch.links_base.registration_maintype = 3 AND links_cleaned.person_c.role = 11 AND links_prematch.links_base.ego_role = 10 ) "
            + ") ; ";

        String query4 = query4a + query4b;

        String[] queries = { query1, query2, query3, query4 };

        return queries;

    } // doCreateNewBaseTableQueries


    /*---< Levenshtein >------------------------------------------------------*/
    // now in main
    /**
     * @param debug
     * @param go
     * @param bExactMatches
     * @throws Exception
     */
    /*
    public void doLevenshtein( boolean debug, boolean go, boolean bExactMatches ) throws Exception
    {
        String funcname = "doLevenshtein";

        if( !go ) {
            showMessage( "Skipping " + funcname, false, true );
            return;
        }

        if( debug ) { System.out.println( funcname ); }

        // prematch.Lv is a separate thread, so timing should be done there internally
        showMessage( funcname + "...", false, true );

        //the 5th parameter (boolean) specifies 'strict' or 'non-strict' Levenshtein method.
        prematch.Lv lv1 = new Lv( debug, conPrematch, "links_prematch", "freq_firstname",  true,  bExactMatches, outputLine, outputArea, plog );
        prematch.Lv lv2 = new Lv( debug, conPrematch, "links_prematch", "freq_firstname",  false, bExactMatches, outputLine, outputArea, plog );
        prematch.Lv lv3 = new Lv( debug, conPrematch, "links_prematch", "freq_familyname", true,  bExactMatches, outputLine, outputArea, plog );
        prematch.Lv lv4 = new Lv( debug, conPrematch, "links_prematch", "freq_familyname", false, bExactMatches, outputLine, outputArea, plog );

        lv1.start();
        lv2.start();
        lv3.start();
        lv4.start();
    } // doLevenshtein
    */

    /**
     * @param s
     * @param t
     * @return
     */
    public static int levenshtein( String s, String t )
    {
        int n = s.length(); // length of s
        int m = t.length(); // length of t

        int p[] = new int[n + 1]; //'previous' cost array, horizontally
        int d[] = new int[n + 1]; // cost array, horizontally
        int _d[]; //placeholder to assist in swapping p and d

        // indexes into strings s and t
        int i; // iterates through s
        int j; // iterates through t

        char t_j; // jth character of t

        int cost; // cost

        for (i = 0; i <= n; i++) {
            p[i] = i;
        }

        for (j = 1; j <= m; j++) {
            t_j = t.charAt(j - 1);
            d[0] = j;

            for (i = 1; i <= n; i++) {
                cost = s.charAt(i - 1) == t_j ? 0 : 1;
                d[i] = Math.min(Math.min(d[i - 1] + 1, p[i] + 1), p[i - 1] + cost);
            }

            _d = p;
            p = d;
            d = _d;
        }

        return p[n];
    } // levenshtein


    /*---< Obsolete >---------------------------------------------------------*/

    /**
     * @param debug
     * @throws Exception
     */
    public void doBasicName( boolean debug ) throws Exception
    {
        //if( debug ) { System.out.println( funcname ); }

        String qPath = "";

        // run preparing queries
        outputArea.append( "01" + "\r\n" );
        qPath = "SetVariants/SetVariants_q01";
        showMessage( "Running query " + qPath, false, true );
        String s01 = LinksSpecific.getSqlQuery( qPath );
        String[] a01 = s01.split(";");

        for (int i = 0; i < a01.length; i++) {
            conPrematch.runQuery( a01[ i ] );
        }

        outputArea.append( "02" + "\r\n" );
        qPath = "SetVariants/SetVariants_q02";
        showMessage( "Running query " + qPath, false, true );
        String s02 = LinksSpecific.getSqlQuery( qPath );
        String[] a02 = s02.split(";");

        for (int i = 0; i < a02.length; i++) {
            conPrematch.runQuery( a02[ i ] );
        }

        outputArea.append( "03" + "\r\n" );
        qPath = "SetVariants/SetVariants_q03";
        showMessage( "Running query " + qPath, false, true );
        String s03 = LinksSpecific.getSqlQuery( qPath );
        String[] a03 = s03.split(";");

        for (int i = 0; i < a03.length; i++) {
            conPrematch.runQuery( a03[ i ] );
        }

        outputArea.append("First 3 SQL statements done, beginning with LV" + "\r\n");

        // Run the variants
        prematch.VariantLs vlFam = new prematch.VariantLs(outputArea, outputLine, "familyname");
        prematch.VariantLs vlFir = new prematch.VariantLs(outputArea, outputLine, "firstname");

        vlFam.computeVariants();
        vlFir.computeVariants();

        outputArea.append("LV DONE" + "\r\n");

        qPath = "SetVariants/SetVariants_q04";
        showMessage( "Running query " + qPath, false, true );
        String s04 = LinksSpecific.getSqlQuery( qPath );
        String[] a04 = s04.split(";");

        for (int i = 0; i < a04.length; i++) {
            conPrematch.runQuery( a04[ i ] );
        }

        outputArea.append( "04" + "\r\n" );
    } // doBasicName

}

