package modulemain;

import java.io.File;
import java.io.PrintStream;

import java.sql.ResultSet;

import javax.swing.JTextArea;
import javax.swing.JTextField;

import connectors.MySqlConnector;
import dataset.Options;
import general.Functions;
import general.PrintLogger;

/**
 * @author Omar Azouguagh
 * @author Fons Laan
 *
 * <p/>
 * FL-29-Jul-2014 Remove hard-code usr's/pwd's
 * FL-17-Nov-2014 Processing all of links_cleaned: not selecting by "... AND id_source = ..."
 * FL-19-Nov-2014 Latest change
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
    private boolean bLevenshtein;
    private boolean bNamesToNos;
    private boolean bBaseTable;

    private boolean showmsg = true;

    private JTextField outputLine;
    private JTextArea  outputArea;

    private MySqlConnector conCleaned;
    private MySqlConnector conPrematch;
    private MySqlConnector conTemp;
    private MySqlConnector conBase;
    private MySqlConnector conFrequency;

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
     * @param bLevenshtein
     * @param bNamesToNos
     * @param bBaseTable
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
        boolean bLevenshtein,
        boolean bNamesToNos,
        boolean bBaseTable
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
        this.bLevenshtein     = bLevenshtein;
        this.bNamesToNos      = bNamesToNos;
        this.bBaseTable       = bBaseTable;

        this.outputLine = outputLine;
        this.outputArea  = outputArea;

        conCleaned   = new MySqlConnector( db_url, "links_cleaned",   db_user, db_pass );
        conPrematch  = new MySqlConnector( db_url, "links_prematch",  db_user, db_pass );
        conTemp      = new MySqlConnector( db_url, "links_temp",      db_user, db_pass );
        conBase      = new MySqlConnector( db_url, "links_base",      db_user, db_pass );
        conFrequency = new MySqlConnector( db_url, "links_frequency", db_user, db_pass );
    }

    /**
     * Constructor
     * called from linksCleaned
     *
     * @throws Exception 
     */
    public LinksPrematch( String db_url, String db_user, String db_pass, JTextArea outputArea, JTextField outputLine )
    throws Exception
    {
        conCleaned   = new MySqlConnector( db_url, "links_cleaned",   db_user, db_pass );
        conPrematch  = new MySqlConnector( db_url, "links_prematch",  db_user, db_pass );
        conTemp      = new MySqlConnector( db_url, "links_temp",      db_user, db_pass );
        conBase      = new MySqlConnector( db_url, "links_base",      db_user, db_pass );
        conFrequency = new MySqlConnector( db_url, "links_frequency", db_user, db_pass );

        this.outputLine = outputLine;
        this.outputArea = outputArea;
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

        showMessage( "LinksPrematch/run()", false, true );
        showMessage( "debug: " + debug, false, true );

        showMessage( "db_url: "  + db_url,  false, true );
        showMessage( "db_user: " + db_user, false, true );
        showMessage( "db_pass: " + db_pass, false, true );

        long timeStart = System.currentTimeMillis();

        try
        {
            doSplitFirstnames( debug, bSplitFirstames );

            doFrequencyTables( debug, bFrequencyTables );

            doStandardization( debug, bStandardization );

            doLevenshtein( debug, bLevenshtein );           // start 4 threads

            doNamesToNumbers( debug, bNamesToNos );

            doUpdateBaseTable( debug, bBaseTable );

            String msg = "Prematching is done";
            elapsedShowMessage( msg, timeStart, System.currentTimeMillis() );
            System.out.println( msg );

        } catch( Exception ex ) { showMessage( ex.getMessage(), false, true ); }

        this.stop();
    } // run


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

            int id_person    = rsFirstName.getInt( "id_person" );
            String firstname = rsFirstName.getString( "firstname" );

            // limit -- This controls the number of times the pattern is applied and therefore affects the length of the resulting array
            String[] fn = firstname.split( " ", 4 );

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

        dropCreateDbFrequency();

        // Execute queries
        int nqFirst = 1;
        int nqLast = 15;
        String qPrefix = "FrequencyTables/FrequencyTables_q";

        for( int n = nqFirst; n <= nqLast; n++ ) {
            String qPath = String.format( qPrefix + "%02d", n );
            String query = LinksSpecific.getSqlQuery( qPath );

            if( query.isEmpty() ) { showMessage( "Empty query " + qPath, false, true ); }
            else {
                showMessage( "Running query " + qPath, false, true );
                if( debug ) { showMessage( query, false, true ); }
                conFrequency.runQuery( query );
            }
        }

        elapsedShowMessage( funcname, funcstart, System.currentTimeMillis() );
        showMessage_nl();
    } // doFrequencyTables

    /**
     *
     */
    public void doStandardization( boolean debug, boolean go ) throws Exception {
        String funcname = "doStandardization";

        if( !go ) {
            showMessage( "Skipping " + funcname, false, true );
            return;
        }

        if( debug ) { System.out.println( funcname ); }

        long funcstart = System.currentTimeMillis();
        showMessage( funcname + "...", false, true );

        // Copy freq_familyname_and freq_firstnames to freq_familyname_tmp_and freq_firstnames_tmp
        String[] queries =
        {
            "DROP TABLE IF EXISTS freq_familyname_tmp;",
            "CREATE TABLE freq_familyname_tmp LIKE freq_familyname;",
            "ALTER TABLE  freq_familyname_tmp DISABLE KEYS;",
            "INSERT INTO  freq_familyname_tmp SELECT * FROM freq_familyname;",
            "ALTER TABLE  freq_familyname_tmp ENABLE KEYS;",
            "DROP TABLE IF EXISTS freq_firstnames_tmp;",
            "CREATE TABLE freq_firstnames_tmp LIKE freq_firstnames;",
            "ALTER TABLE  freq_firstnames_tmp DISABLE KEYS;",
            "INSERT INTO  freq_firstnames_tmp SELECT * FROM freq_firstnames;",
            "ALTER TABLE  freq_firstnames_tmp ENABLE KEYS;"
        };

        // run queries
        for( String q : queries ) {
            if( debug ) { showMessage( q, false, true ); }
            conFrequency.runQuery( q );
        }


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

            conFrequency.runQuery(query);
        }
        */

        elapsedShowMessage( funcname, funcstart, System.currentTimeMillis() );
        showMessage_nl();
    } // doStandardization(


    /**
     *
     */
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
            conFrequency.runQuery( query );
        }

    } // doUniqueNameTablesTemp


    /**
     *
     */
    private void dropCreateDbFrequency() throws Exception
    {
        String qDropFrequency = "DROP SCHEMA links_frequency ;";

        String qCreateFrequency = "CREATE SCHEMA `links_frequency` DEFAULT CHARACTER SET latin1 COLLATE latin1_general_ci ;";

        // run queries
        if( showmsg ) { showMessage( "Dropping database links_frequency", false, true ); }
        conFrequency.runQuery( qDropFrequency );

        if( showmsg ) { showMessage( "Creating database links_frequency", false, true ); }
        conFrequency.runQuery( qCreateFrequency );
    } // dropCreateDbFrequency


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
            conFrequency.runQuery(a01[i]);
        }

        outputArea.append( "02" + "\r\n" );
        qPath = "SetVariants/SetVariants_q02";
        showMessage( "Running query " + qPath, false, true );
        String s02 = LinksSpecific.getSqlQuery( qPath );
        String[] a02 = s02.split(";");

        for (int i = 0; i < a02.length; i++) {
            conFrequency.runQuery(a02[i]);
        }

        outputArea.append( "03" + "\r\n" );
        qPath = "SetVariants/SetVariants_q03";
        showMessage( "Running query " + qPath, false, true );
        String s03 = LinksSpecific.getSqlQuery( qPath );
        String[] a03 = s03.split(";");

        for (int i = 0; i < a03.length; i++) {
            conFrequency.runQuery(a03[i]);
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
            conFrequency.runQuery(a04[i]);
        }

        outputArea.append( "04" + "\r\n" );
    } // doBasicName


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

        for( int n = nqFirst; n <= nqLast; n++ ) {
            String qPath = String.format( qPrefix + "%02d", n );

            String query = LinksSpecific.getSqlQuery( qPath );
            if( debug ) { showMessage(query, false, true); }

            conFrequency.runQuery( query );
        }

        /*
        // Create Runtime Object
        Runtime runtime = Runtime.getRuntime();
        int exitValue = 0;
        */

        /* Creating name files */
        /*
        Process process = runtime.exec(new String[]{"/bin/bash", "-c", "./latest.pl familyname familyname"});
        exitValue = process.waitFor();
        outputArea.append("Exitcode0 = " + exitValue + "\r\n");

        process = runtime.exec(new String[]{"/bin/bash", "-c", "./latest.pl firstname firstname1"});
        exitValue = process.waitFor();
        outputArea.append("Exitcode1 = " + exitValue + "\r\n");

        process = runtime.exec(new String[]{"/bin/bash", "-c", "./latest.pl firstname firstname2"});
        exitValue = process.waitFor();
        outputArea.append("Exitcode2 = " + exitValue + "\r\n");

        process = runtime.exec(new String[]{"/bin/bash", "-c", "./latest.pl firstname firstname3"});
        exitValue = process.waitFor();
        outputArea.append("Exitcode3 = " + exitValue + "\r\n");

        process = runtime.exec(new String[]{"/bin/bash", "-c", "./latest.pl firstname firstname4"});
        exitValue = process.waitFor();
        outputArea.append("Exitcode4 = " + exitValue + "\r\n");

        // run File
        Process process = runtime.exec(new String[]{"/bin/bash", "-c", "mysql links_cleaned --user=linksdev --password=db_pass < updates0.sql"});
        exitValue = process.waitFor();
        outputArea.append("Exitcode_1 = " + exitValue + "\r\n");
        */

        /////
        //Process process = runtime.exec(new String[]{"/bin/bash", "-c", "/etc/init.d/mysqld restart"});
        //exitValue = process.waitFor();
        //outputArea.append("restart = " + exitValue + "\r\n");
        
        //process = runtime.exec(new String[]{"/bin/bash", "-c", "mysqlcheck --user=db_user --password=db_pass --auto-repair -c -o links_cleaned"});
        //exitValue = process.waitFor();
        //outputArea.append("optimize = " + exitValue + "\r\n");
        
//        conFrequency.runQuery(LinksSpecific.getSqlQuery("NameToNumber/NameToNumber_q02"));


//        process = runtime.exec(new String[]{"/bin/bash", "-c", "mysql links_cleaned --user=db_user --password=db_pass < updates1.sql"});
//        exitValue = process.waitFor();
//        outputArea.append("Exitcode_1 = " + exitValue + "\r\n");

        
        
//        process = runtime.exec(new String[]{"/bin/bash", "-c", "/etc/init.d/mysqld restart"});
//        exitValue = process.waitFor();
//        outputArea.append("restart = " + exitValue + "\r\n");
        
//        process = runtime.exec(new String[]{"/bin/bash", "-c", "mysqlcheck --user=db_user --password=db_pass --auto-repair -c -o links_cleaned"});
//        exitValue = process.waitFor();
//        outputArea.append("optimize = " + exitValue + "\r\n");
        
//        conFrequency.runQuery(LinksSpecific.getSqlQuery("NameToNumber/NameToNumber_q03"));
        
//        process = runtime.exec(new String[]{"/bin/bash", "-c", "mysql links_cleaned --user=db_user --password=db_pass < updates2.sql"});
//        exitValue = process.waitFor();
//        outputArea.append("Exitcode_2 = " + exitValue + "\r\n");

        
        
//        process = runtime.exec(new String[]{"/bin/bash", "-c", "/etc/init.d/mysqld restart"});
//        exitValue = process.waitFor();
//        outputArea.append("restart = " + exitValue + "\r\n");
        
//        process = runtime.exec(new String[]{"/bin/bash", "-c", "mysqlcheck --user=db_user --password=db_pass --auto-repair -c -o links_cleaned"});
//        exitValue = process.waitFor();
//        outputArea.append("optimize = " + exitValue + "\r\n");
        
//        conFrequency.runQuery(LinksSpecific.getSqlQuery("NameToNumber/NameToNumber_q04"));
//        conFrequency.runQuery(LinksSpecific.getSqlQuery("NameToNumber/NameToNumber_q05"));

        
        //        process = runtime.exec(new String[]{"/bin/bash", "-c", "mysql links_cleaned --user=db_user --password=db_pass < updates3.sql"});
        //        exitValue = process.waitFor();
        //        outputArea.append("Exitcode_3 = " + exitValue + "\r\n");
        //        
        //        process = runtime.exec(new String[]{"/bin/bash", "-c", "mysql links_cleaned --user=db_user --password=db_pass < updates4.sql"});
        //        exitValue = process.waitFor();
        //        outputArea.append("Exitcode_4 = " + exitValue + "\r\n");

        elapsedShowMessage( funcname, funcstart, System.currentTimeMillis() );
        showMessage_nl();
    } // doNamesToNumbers


    /**
     * @param debug
     * @throws Exception 
     */
    public void doLevenshtein( boolean debug, boolean go ) throws Exception
    {
        String funcname = "doLevenshtein";

        if( !go ) {
            showMessage( "Skipping " + funcname, false, true );
            return;
        }

        if( debug ) { System.out.println( funcname ); }

        // prematch.Lv is a separate thread, so timing should be done there internally
        showMessage( funcname + "...", false, true );

        prematch.Lv lv1 = new prematch.Lv( debug, conFrequency, "links_frequency", "freq_firstnames", true,  outputLine, outputArea );
        prematch.Lv lv2 = new prematch.Lv( debug, conFrequency, "links_frequency", "freq_firstnames", false, outputLine, outputArea );
        prematch.Lv lv3 = new prematch.Lv( debug, conFrequency, "links_frequency", "freq_familyname", true,  outputLine, outputArea );
        prematch.Lv lv4 = new prematch.Lv( debug, conFrequency, "links_frequency", "freq_familyname", false, outputLine, outputArea );

        lv1.start();
        lv2.start();
        lv3.start();
        lv4.start();
    } // doLevenshtein


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


    /**
     * @param debug
     * @throws Exception 
     */
    public void doUpdateBaseTable( boolean debug, boolean go ) throws Exception
    {
        String funcname = "doUpdateBaseTable";

        if( !go ) {
            showMessage( "Skipping " + funcname, false, true );
            return;
        }

        if( debug ) { System.out.println( funcname ); }

        long funcstart = System.currentTimeMillis();
        showMessage( funcname + "...", false, true );

        String qPrefix = "FillBaseTable/FillBaseTable_q";
        int nqFirst = 1;
        int nqLast = 33;
        // q41...q47 were no longer used by Omar; what is their function? (and q34...q40 are missing.)

        for( int n = nqFirst; n <= nqLast; n++ ) {
            String qPath = String.format( qPrefix + "%02d", n );
            showMessage( "Running query: " + qPath, false, true );

            String query = LinksSpecific.getSqlQuery( qPath );
            if( debug ) { showMessage( query, false, true ); }

            try { conBase.runQuery( query ); }
            catch( Exception ex ) { showMessage( ex.getMessage(), false, true ); }
        }

        elapsedShowMessage( funcname, funcstart, System.currentTimeMillis() );
        showMessage_nl();
    } // doUpdateBaseTable


    /**
     *
     * @param seconds
     * @return
     */
    public static String stopWatch( int seconds )
    {
        int minutes = seconds / 60;
        int restsec = seconds % 60;
        int uren    = minutes / 60;
        int restmin = minutes % 60;

        String urenText     = "";
        String minutenText  = "";
        String secondenText = "";

        if( uren < 10 ) { urenText = "0"; }

        if( restmin < 10 ) { minutenText = "0"; }

        if( restsec < 10 ) { secondenText = "0"; }

        return urenText + uren + ":" + minutenText + restmin + ":" + secondenText + restsec;
    } // stopWatch


    /**
     *
     * @throws Exception
     */
    private void createTempFirstname( boolean debug ) throws Exception
    {
        String query = "CREATE  TABLE links_temp.firstname_t_split ("
            + " person_id INT UNSIGNED NOT NULL AUTO_INCREMENT ,"
            + " firstname1 VARCHAR(30) NULL ,"
            + " firstname2 VARCHAR(30) NULL ,"
            + " firstname3 VARCHAR(30) NULL ,"
            + " firstname4 VARCHAR(30) NULL ,"
            + " PRIMARY KEY (person_id) );";

        if( debug ) { showMessage( query, false, true); }
        conTemp.runQuery( query );
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

}

