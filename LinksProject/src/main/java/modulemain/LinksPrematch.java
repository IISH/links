package modulemain;

import java.io.File;
import java.io.PrintStream;

import java.util.ArrayList;

import java.sql.ResultSet;

import javax.swing.JTextArea;
import javax.swing.JTextField;

import connectors.MySqlConnector;
import dataset.Options;
import general.Functions;
import general.PrintLogger;
import prematch.Lv;

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
 * FL-20-Nov-2015 Latest change
 */

public class LinksPrematch extends Thread
{
    private Options opts;

    private boolean debug;
    private PrintLogger plog;

    private String db_url;
    private String db_user;
    private String db_pass;

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
        this.debug = opts.isDbgPrematch();
        this.plog  = opts.getLogger();

        this.db_url  = opts.getDb_url();
        this.db_user = opts.getDb_user();
        this.db_pass = opts.getDb_pass();

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

        try
        {
            doSplitFirstnames( debug, bSplitFirstames );

            doFrequencyTables( debug, bFrequencyTables );               // creates the freq_* tables

            doStandardization( debug, bStandardization );

            doNamesToNumbers( debug, bNamesToNos );

            doCreateNewBaseTable( debug, bBaseTable );

            //doLevenshtein( debug, bLevenshtein, bExactMatches );        // now here in main
            String funcname = "doLevenshtein";

            if( ! bLevenshtein ) { showMessage( "Skipping " + funcname, false, true ); }
            else
            {
                if( debug ) { System.out.println( funcname ); }

                // prematch.Lv is a separate thread, so timing should be done there internally
                showMessage( funcname + ", using 4 threads", false, true );

                //the 5th parameter (boolean) specifies 'strict' or 'non-strict' Levenshtein method.
                Lv lv1 = new Lv( debug, conPrematch, "links_prematch", "freq_firstname",  true,  bExactMatches, outputLine, outputArea, plog );
                Lv lv2 = new Lv( debug, conPrematch, "links_prematch", "freq_firstname",  false, bExactMatches, outputLine, outputArea, plog );
                Lv lv3 = new Lv( debug, conPrematch, "links_prematch", "freq_familyname", true,  bExactMatches, outputLine, outputArea, plog );
                Lv lv4 = new Lv( debug, conPrematch, "links_prematch", "freq_familyname", false, bExactMatches, outputLine, outputArea, plog );

                lv1.start();
                lv2.start();
                lv3.start();
                lv4.start();

                lv1.join();
                lv2.join();
                lv3.join();
                lv4.join();
            }

            String msg = String.format( "\nPrematching Finished." );
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
     * 
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

        String query = "SELECT id_person , firstname FROM person_c WHERE firstname IS NOT NULL AND firstname <> ''";
        if( debug ) { showMessage( query, false, true); }

        ResultSet rsFirstName = conCleaned.runQueryWithResult( query );

        //removeFirstnameFile();
        //removeFirstnameTable();

        createTempFirstname( debug );
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
     *
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
     *
     * @throws Exception
     */
    private void createTempFirstnameFile() throws Exception {
        String filename = "firstname_t_split.csv";
        showMessage( "Creating file " + filename, false, true);
        writerFirstname = new java.io.FileWriter( filename );
    } // createTempFirstnameFile


    /**
     *
     * @throws Exception
     */
    private void loadFirstnameToTable( boolean debug )
            throws Exception
    {
        showMessage( "Loading CSV data into temp table...", false, true );

        String query = "LOAD DATA LOCAL INFILE 'firstname_t_split.csv'"
                + " INTO TABLE firstname_t_split FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n'"
                + " ( person_id , firstname1 , firstname2 , firstname3 , firstname4 );";

        if( debug ) { showMessage( query, false, true); }
        conTemp.runQuery( query );
    } // loadFirstnameToTable


    /**
     *
     */
    private void updateFirstnameToPersonC( boolean debug )
            throws Exception
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
     *
     * @throws Exception
     */
    public void removeFirstnameFile() throws Exception {
        String filename = "firstname_t_split.csv";
        showMessage( "Removing file " + filename, false, true);
        File file = new File( filename );
        file.delete();
    } // removeFirstnameFile


    /**
     *
     * @throws Exception
     */
    public void removeFirstnameTable() throws Exception {
        String tablename = "firstname_t_split";
        showMessage( "Removing table " + tablename, false, true);
        String query = "DROP TABLE " + tablename + ";";
        conTemp.runQuery( query );
    } // removeFirstnameTable


    /*---< Frequency Tables >-------------------------------------------------*/

    /**
     *
     */
    public void doFrequencyTables( boolean debug, boolean go ) throws Exception
    {
        String funcname = "doFrequencyTables";

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
        int nqLast = 18;
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
    } // doFrequencyTables


    /*---< Automatic Standardization >----------------------------------------*/

    /**
     *
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

        // Copy freq_familyname_and freq_firstnames to freq_familyname_tmp_and freq_firstnames_tmp
        String[] queriesFamilyname =
        {
            "DROP TABLE IF EXISTS links_prematch.freq_familyname_tmp;",
            "CREATE TABLE links_prematch.freq_familyname_tmp LIKE links_prematch.freq_familyname;",
            "ALTER TABLE  links_prematch.freq_familyname_tmp DISABLE KEYS;",
            "INSERT INTO  links_prematch.freq_familyname_tmp SELECT * FROM links_prematch.freq_familyname;",
            "ALTER TABLE  links_prematch.freq_familyname_tmp ENABLE KEYS;",
        };

        // run queries
        for( String q : queriesFamilyname ) {
            if( debug ) { showMessage( q, false, true ); }
            conPrematch.runQuery( q );
        }

        String[] queriesFirstname =
        {
            "DROP TABLE IF EXISTS links_prematch.freq_firstnames_tmp;",
            "CREATE TABLE links_prematch.freq_firstnames_tmp LIKE links_prematch.freq_firstnames;",
            "ALTER TABLE  links_prematch.freq_firstnames_tmp DISABLE KEYS;",
            "INSERT INTO  links_prematch.freq_firstnames_tmp SELECT * FROM links_prematch.freq_firstnames;",
            "ALTER TABLE  links_prematch.freq_firstnames_tmp ENABLE KEYS;"
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
        int nqLast = 28;
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
     *
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
     *
     * @throws Exception
     */
    public void doNamesToNumbers( boolean debug, boolean go ) throws Exception
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
    } // doNamesToNumbers


    /*---< Base Table >-------------------------------------------------------*/

    /**
     * @param debug
     * @throws Exception
     */
    public void doCreateNewBaseTable( boolean debug, boolean go ) throws Exception
    {
        String funcname = "doCreateNewBaseTable";

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
        //int nqLast = 33;  // reduced Omars's 33 queries to 4
        // q41...q47 were no longer used by Omar; what is their function? (and q34...q40 are missing.)

        for( int n = nqFirst; n <= nqLast; n++ )
        {
            long start = System.currentTimeMillis();
            String qPath = String.format( qPrefix + "%02d", n );

            String query = LinksSpecific.getSqlQuery( qPath );
            String msg = "query " + n + "-of-" + nqLast;
            showMessage( msg + "...", false, true );
            if( debug ) { showMessage( query, false, true ); }

            try { conPrematch.runQuery( query ); }
            catch( Exception ex ) { showMessage( ex.getMessage(), false, true ); }
            elapsedShowMessage( msg, start, System.currentTimeMillis() );
        }

        elapsedShowMessage( funcname, funcstart, System.currentTimeMillis() );
        showMessage_nl();
    } // doCreateNewBaseTable


    /*---< Levenshtein >------------------------------------------------------*/
    // now in main
    /**
     * @param debug
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
     *
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
     *
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

