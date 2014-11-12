package modulemain;

import java.io.File;
import java.io.PrintStream;

import java.sql.ResultSet;

import javax.swing.JTextArea;
import javax.swing.JTextField;

import com.google.common.collect.Range;

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
 * FL-11-Nov-2014 Latest change
 */

public class LinksPrematch extends Thread
{
    private Options opts;
    private PrintLogger plog;

    private String db_url;
    private String db_user;
    private String db_pass;

    private boolean debug;

    private boolean bSplitNames;
    private boolean bUniqueTables;
    private boolean bLevenshtein;
    private boolean bNamesToNo;
    private boolean bBaseTable;

    private boolean showmsg = true;

    private JTextField outputLine;              // used-to-be ti
    private JTextArea  outputArea;              // used-to-be ta

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
     * @param bSplitNames
     * @param bUniqueTables
     * @param bLevenshtein
     * @param bNamesToNo
     * @param bBaseTable
     * @throws Exception
     */
    public LinksPrematch
    (
        Options opts,

        JTextField outputLine,
        JTextArea  outputArea,

        boolean bSplitNames,
        boolean bUniqueTables,
        boolean bLevenshtein,
        boolean bNamesToNo,
        boolean bBaseTable
    )
    throws Exception
    {
        this.debug = opts.isDbgPrematch();
        this.plog  = opts.getLogger();

        this.db_url  = opts.getDb_url();
        this.db_user = opts.getDb_user();
        this.db_pass = opts.getDb_pass();

        this.bSplitNames   = bSplitNames;
        this.bUniqueTables = bUniqueTables;
        this.bLevenshtein  = bLevenshtein;
        this.bNamesToNo    = bNamesToNo;
        this.bBaseTable    = bBaseTable;

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

        showMessage( "LinksPrematch/run()", false, true );
        showMessage( "debug: " + debug, false, true );

        showMessage( "db_url: "  + db_url,  false, true );
        showMessage( "db_user: " + db_user, false, true );
        showMessage( "db_pass: " + db_pass, false, true );

        long startTotal = System.currentTimeMillis();

        String mmss = "";
        String msg  = "";

        try {
            System.out.println( "SplitNames" );
            if( bSplitNames ) {
                showMessage( "Splitting names...", false, true );
                long start = System.currentTimeMillis();
                doSplitName( debug );
                long stop = System.currentTimeMillis();
                mmss = Functions.millisec2hms( start, stop );
                msg = "Splitting names OK " + mmss;
                showMessage( msg, false, true );
                showMessage_nl();

            }
            else { showMessage( "skipping Splitting names", false, true ); }

            System.out.println( "bUniqueTables" );
            if( bUniqueTables ) {
                showMessage( "Creating unique name tables...", false, true );
                long start = System.currentTimeMillis();
                doUniqueNameTables( debug );
                long stop = System.currentTimeMillis();
                mmss = Functions.millisec2hms( start, stop );
                msg = "Creating Unique name table OK " + mmss;
                showMessage( msg, false, true );
                showMessage_nl();
            }
            else { showMessage( "skipping Creating unique name tables", false, true ); }

            System.out.println( "bLevenshtein" );
            if( bLevenshtein ) {
                showMessage( "Computing Levenshtein...", false, true );
                long start = System.currentTimeMillis();
                doLevenshtein( debug );
                long stop = System.currentTimeMillis();
                mmss = Functions.millisec2hms( start, stop );
                msg = "Computing Levenshtein OK " + mmss;
                showMessage( msg, false, true );
                showMessage_nl();
            }
            else { showMessage( "skipping Computing Levenshtein", false, true ); }

            System.out.println( "NamesToNo" );
            if( bNamesToNo ) {
                showMessage( "Converting Names to Numbers...", false, true );
                long start = System.currentTimeMillis();
                doToNumber( debug );
                long stop = System.currentTimeMillis();
                mmss = Functions.millisec2hms( start, stop );
                msg = "Converting Names to Numbers OK " + mmss;
                showMessage( msg, false, true );
                showMessage_nl();
            }
            else { showMessage( "skipping Converting Names to Numbers", false, true ); }

            System.out.println( "bBaseTable" );
            if( bBaseTable ) {
                showMessage( "Creating Base Table...", false, true );
                long start = System.currentTimeMillis();
                doCreateBaseTable( debug );
                long stop = System.currentTimeMillis();
                mmss = Functions.millisec2hms( start, stop );
                msg = "Creating Base Table OK " + mmss;
                showMessage( msg, false, true );
                showMessage_nl();
            }
            else { showMessage( "skipping Creating Base Table", false, true ); }

            long stopTotal = System.currentTimeMillis();
            mmss = Functions.millisec2hms( startTotal, stopTotal );
            msg = "Prematch finished, Elapsed: " + mmss;
            showMessage( msg, false, true );

            this.stop();

        } catch( Exception ex ) { showMessage( ex.getMessage(), false, true ); }
    }


    //public boolean isSplitNames()   { return bSplitNames; }
    //public boolean isUniqueTables() { return bUniqueTables; }
    //public boolean isLevenshtein()  { return bLevenshtein; }
    //public boolean isNamesToNo()    { return bNamesToNo; }
    //public boolean isBaseTable()    { return bBaseTable; }


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

            /*
            if( logText != endl ) {
                String ts = LinksSpecific.getTimeStamp2( "HH:mm:ss" );

                outputArea.append( ts + " " );
                // System.out.printf( "%s ", ts );
                //logger.info( logText );
                try { plog.show( logText ); }
                catch( Exception ex ) {
                    System.out.println( ex.getMessage() );
                    ex.printStackTrace( new PrintStream( System.out ) );
                }
            }
            */

            outputArea.append( logText + newLineToken );
            //System.out.printf( "%s%s", logText, newLineToken );
            try { plog.show( logText ); }
            catch( Exception ex ) {
                System.out.println( ex.getMessage() );
                ex.printStackTrace( new PrintStream( System.out ) );
            }
        }
    } // showMessage


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
    public void doSplitName( boolean debug ) throws Exception
    {
        showMessage( "doSplitName()", false, true);

        String query = "SELECT id_person , firstname FROM person_c WHERE firstname is not null AND firstname <> ''";
        if( debug ) { showMessage( query, false, true); }

        ResultSet rsFirstName = conCleaned.runQueryWithResult( query );

        createTempFirstname( debug );
        createTempFirstnameFile();

        int count = 0;

        while( rsFirstName.next() )
        {
            count++;

            int id_person    = rsFirstName.getInt( "id_person" );
            String firstname = rsFirstName.getString( "firstname" );

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

            writerFirstname.write( q + "\n" );
        }
        showMessage( count + " firstnames split", false, true );

        rsFirstName.close();
        rsFirstName = null;

        loadFirstnameToTable( debug );
        updateFirstnameToPersonC( debug );
        removeFirstnameFile();
        removeFirstnameTable();
    }


    /**
     *
     */
    public void doUniqueNameTables( boolean debug ) throws Exception
    {
        showMessage( "Creating unique tables...", false, true );

        dropTableFrequency();

        // Execute queries
        int nqFirst = 1;
        int nqLast = 28;
        String qPrefix = "FrequencyTables/FrequencyTables_q";

        for( int n = nqFirst; n <= nqLast; n++ ) {
            String qPath = String.format( qPrefix + "%02d", n );
            //System.out.println( qPath );

            showMessage( "Running query " + qPath, false, true );

            String query = LinksSpecific.getSqlQuery( qPath );
            query = query.replaceAll( "\\s+", " " );      // remove double whitespacing

            if( debug ) { showMessage( query, false, true ); }
            conBase.runQuery( query );
        }

        /*
        // TODO: delete this
        conFrequency.runQuery( LinksSpecific.getSqlQuery( "FrequencyTables/FrequencyTables_q01" ) );
        conFrequency.runQuery( LinksSpecific.getSqlQuery( "FrequencyTables/FrequencyTables_q02" ) );
        conFrequency.runQuery( LinksSpecific.getSqlQuery( "FrequencyTables/FrequencyTables_q03" ) );
        conFrequency.runQuery( LinksSpecific.getSqlQuery( "FrequencyTables/FrequencyTables_q04" ) );
        conFrequency.runQuery( LinksSpecific.getSqlQuery( "FrequencyTables/FrequencyTables_q05" ) );
        conFrequency.runQuery( LinksSpecific.getSqlQuery( "FrequencyTables/FrequencyTables_q06" ) );
        conFrequency.runQuery( LinksSpecific.getSqlQuery( "FrequencyTables/FrequencyTables_q07" ) );
        conFrequency.runQuery( LinksSpecific.getSqlQuery( "FrequencyTables/FrequencyTables_q08" ) );
        conFrequency.runQuery( LinksSpecific.getSqlQuery( "FrequencyTables/FrequencyTables_q09" ) );
        conFrequency.runQuery( LinksSpecific.getSqlQuery( "FrequencyTables/FrequencyTables_q10" ) );
        conFrequency.runQuery( LinksSpecific.getSqlQuery( "FrequencyTables/FrequencyTables_q11" ) );
        conFrequency.runQuery( LinksSpecific.getSqlQuery( "FrequencyTables/FrequencyTables_q12" ) );
        conFrequency.runQuery( LinksSpecific.getSqlQuery( "FrequencyTables/FrequencyTables_q13" ) );
        conFrequency.runQuery( LinksSpecific.getSqlQuery( "FrequencyTables/FrequencyTables_q14" ) );
        conFrequency.runQuery( LinksSpecific.getSqlQuery( "FrequencyTables/FrequencyTables_q15" ) );
        conFrequency.runQuery( LinksSpecific.getSqlQuery( "FrequencyTables/FrequencyTables_q16" ) );
        conFrequency.runQuery( LinksSpecific.getSqlQuery( "FrequencyTables/FrequencyTables_q17" ) );
        conFrequency.runQuery( LinksSpecific.getSqlQuery( "FrequencyTables/FrequencyTables_q18" ) );
        conFrequency.runQuery( LinksSpecific.getSqlQuery( "FrequencyTables/FrequencyTables_q19" ) );
        conFrequency.runQuery( LinksSpecific.getSqlQuery( "FrequencyTables/FrequencyTables_q20" ) );
        conFrequency.runQuery( LinksSpecific.getSqlQuery( "FrequencyTables/FrequencyTables_q21" ) );
        conFrequency.runQuery( LinksSpecific.getSqlQuery( "FrequencyTables/FrequencyTables_q22" ) );
        conFrequency.runQuery( LinksSpecific.getSqlQuery( "FrequencyTables/FrequencyTables_q23" ) );
        conFrequency.runQuery( LinksSpecific.getSqlQuery( "FrequencyTables/FrequencyTables_q24" ) );
        conFrequency.runQuery( LinksSpecific.getSqlQuery( "FrequencyTables/FrequencyTables_q25" ) );
        conFrequency.runQuery( LinksSpecific.getSqlQuery( "FrequencyTables/FrequencyTables_q26" ) );
        conFrequency.runQuery( LinksSpecific.getSqlQuery( "FrequencyTables/FrequencyTables_q27" ) );
        conFrequency.runQuery( LinksSpecific.getSqlQuery( "FrequencyTables/FrequencyTables_q28" ) );
        */
    }


    /**
     *
     */
    public void doUniqueNameTablesTemp() throws Exception
    {
        dropTableFrequency();

        // Execute queries
        int nqFirst = 1;
        int nqLast = 18;
        String qPrefix = "FrequencyTablesTemp/FrequencyTablesTemp_q";

        for( int n = nqFirst; n <= nqLast; n++ ) {
            String qPath = String.format( qPrefix + "%02d", n );
            showMessage( "Running query " + qPath, false, true );
            //System.out.println( qPath );
            String query = LinksSpecific.getSqlQuery( qPath );
            conBase.runQuery( query );
        }

        /*
        // TODO: delete this
        conFrequency.runQuery( LinksSpecific.getSqlQuery( "FrequencyTablesTemp/FrequencyTablesTemp_q01" ) );
        conFrequency.runQuery( LinksSpecific.getSqlQuery( "FrequencyTablesTemp/FrequencyTablesTemp_q02" ) );
        conFrequency.runQuery( LinksSpecific.getSqlQuery( "FrequencyTablesTemp/FrequencyTablesTemp_q03" ) );
        conFrequency.runQuery( LinksSpecific.getSqlQuery( "FrequencyTablesTemp/FrequencyTablesTemp_q04" ) );
        conFrequency.runQuery( LinksSpecific.getSqlQuery( "FrequencyTablesTemp/FrequencyTablesTemp_q05" ) );
        conFrequency.runQuery( LinksSpecific.getSqlQuery( "FrequencyTablesTemp/FrequencyTablesTemp_q06" ) );
        conFrequency.runQuery( LinksSpecific.getSqlQuery( "FrequencyTablesTemp/FrequencyTablesTemp_q07" ) );
        conFrequency.runQuery( LinksSpecific.getSqlQuery( "FrequencyTablesTemp/FrequencyTablesTemp_q08" ) );
        conFrequency.runQuery( LinksSpecific.getSqlQuery( "FrequencyTablesTemp/FrequencyTablesTemp_q09" ) );
        conFrequency.runQuery( LinksSpecific.getSqlQuery( "FrequencyTablesTemp/FrequencyTablesTemp_q10" ) );
        conFrequency.runQuery( LinksSpecific.getSqlQuery( "FrequencyTablesTemp/FrequencyTablesTemp_q11" ) );
        conFrequency.runQuery( LinksSpecific.getSqlQuery( "FrequencyTablesTemp/FrequencyTablesTemp_q12" ) );
        conFrequency.runQuery( LinksSpecific.getSqlQuery( "FrequencyTablesTemp/FrequencyTablesTemp_q13" ) );
        conFrequency.runQuery( LinksSpecific.getSqlQuery( "FrequencyTablesTemp/FrequencyTablesTemp_q14" ) );
        conFrequency.runQuery( LinksSpecific.getSqlQuery( "FrequencyTablesTemp/FrequencyTablesTemp_q15" ) );
        conFrequency.runQuery( LinksSpecific.getSqlQuery( "FrequencyTablesTemp/FrequencyTablesTemp_q16" ) );
        conFrequency.runQuery( LinksSpecific.getSqlQuery( "FrequencyTablesTemp/FrequencyTablesTemp_q17" ) );
        conFrequency.runQuery( LinksSpecific.getSqlQuery( "FrequencyTablesTemp/FrequencyTablesTemp_q18" ) );
        */
    }


    /**
     *
     */
    private void dropTableFrequency() throws Exception
    {
        String qDropFrequency = "DROP SCHEMA links_frequency ;";

        String qCreateFrequency = "CREATE SCHEMA `links_frequency` DEFAULT CHARACTER SET latin1 COLLATE latin1_general_ci ;";

        // run queries
        if( showmsg ) { showMessage( "Dropping database links_frequency", false, true ); }
        conFrequency.runQuery( qDropFrequency );

        if( showmsg ) { showMessage( "Creating database links_frequency", false, true ); }
        conFrequency.runQuery( qCreateFrequency );
    }


    /**
     * 
     */
    public void doBasicName( boolean debug ) throws Exception
    {
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
    }


    /**
     * 
     * @throws Exception 
     */
    public void doToNumber( boolean debug ) throws Exception
    {
        // Create Runtime Object
        Runtime runtime = Runtime.getRuntime();
        int exitValue = 0;

        // Execute queries
        int nqFirst = 1;
        int nqLast  = 5;
        String qPrefix = "NameToNumber/NameToNumber_q";

        for( int n = nqFirst; n <= nqLast; n++ ) {
            String qPath = String.format( qPrefix + "%02d", n );
            //System.out.println( qPath );

            showMessage( "Running query " + qPath, false, true );

            String query = LinksSpecific.getSqlQuery( qPath );
            conBase.runQuery( query );
        }

        /*
        // TODO: delete this
        conFrequency.runQuery(LinksSpecific.getSqlQuery("NameToNumber/NameToNumber_q01"));
        conFrequency.runQuery(LinksSpecific.getSqlQuery("NameToNumber/NameToNumber_q02"));
        conFrequency.runQuery(LinksSpecific.getSqlQuery("NameToNumber/NameToNumber_q03"));
        conFrequency.runQuery(LinksSpecific.getSqlQuery("NameToNumber/NameToNumber_q04"));
        conFrequency.runQuery(LinksSpecific.getSqlQuery("NameToNumber/NameToNumber_q05"));
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

    }


    /**
     * @param debug
     * @throws Exception 
     */
    public void doLevenshtein( boolean debug ) throws Exception
    {
        // "firstname" and "familyname" are links_frequency tables
        //System.out.println( conFrequency );

        prematch.Lv lv1 = new prematch.Lv( debug, conFrequency, "firstname",  true,  outputLine, outputArea );
        prematch.Lv lv2 = new prematch.Lv( debug, conFrequency, "firstname",  false, outputLine, outputArea );
        prematch.Lv lv3 = new prematch.Lv( debug, conFrequency, "familyname", true,  outputLine, outputArea );
        prematch.Lv lv4 = new prematch.Lv( debug, conFrequency, "familyname", false, outputLine, outputArea );

        lv1.start();
        lv2.start();
        lv3.start();
        lv4.start();
    }


    /**
     * @param debug
     * @throws Exception 
     */
    public void doCreateBaseTable( boolean debug ) throws Exception
    {
        if( showmsg ) { showMessage( "Creating LINKS_BASE tables...", false, true ); }

        String qPrefix = "FillBaseTable/FillBaseTable_q";
        int nqFirst = 1;
        int nqLast = 33;
        // q41...q47 were no longer used by Omar; what is their function? (and q34...q40 are missing.)

        for( int n = nqFirst; n <= nqLast; n++ ) {
            String qPath = String.format( qPrefix + "%02d", n );
            //System.out.println( qPath );

            showMessage( "Running query " + qPath, false, true );
            String query = LinksSpecific.getSqlQuery( qPath );

            query = query.replaceAll( "\\r\\n", " " );
            query = query.replaceAll( "  ", " " );

            if( debug ) { showMessage( query, false, true ); }
            try { conBase.runQuery( query ); }
            catch( Exception ex ) { showMessage( ex.getMessage(), false, true ); }
        }

        /*
        {
            // TODO remove this
            outputArea.append("Running query 1...\r\n");
            conBase.runQuery(LinksSpecific.getSqlQuery("FillBaseTable/FillBaseTable_q01"));

            outputArea.append("Running query 2...\r\n");
            conBase.runQuery(LinksSpecific.getSqlQuery("FillBaseTable/FillBaseTable_q02"));

            outputArea.append("Running query 3...\r\n");
            conBase.runQuery(LinksSpecific.getSqlQuery("FillBaseTable/FillBaseTable_q03"));

            outputArea.append("Running query 4...\r\n");
            conBase.runQuery(LinksSpecific.getSqlQuery("FillBaseTable/FillBaseTable_q04"));

            outputArea.append("Running query 5...\r\n");
            conBase.runQuery(LinksSpecific.getSqlQuery("FillBaseTable/FillBaseTable_q05"));

            outputArea.append("Running query 6...\r\n");
            conBase.runQuery(LinksSpecific.getSqlQuery("FillBaseTable/FillBaseTable_q06"));

            outputArea.append("Running query 7...\r\n");
            conBase.runQuery(LinksSpecific.getSqlQuery("FillBaseTable/FillBaseTable_q07"));

            outputArea.append("Running query 8...\r\n");
            conBase.runQuery(LinksSpecific.getSqlQuery("FillBaseTable/FillBaseTable_q08"));

            outputArea.append("Running query 9...\r\n");
            conBase.runQuery(LinksSpecific.getSqlQuery("FillBaseTable/FillBaseTable_q09"));

            outputArea.append("Running query 10...\r\n");
            conBase.runQuery(LinksSpecific.getSqlQuery("FillBaseTable/FillBaseTable_q10"));

            outputArea.append("Running query 11...\r\n");
            conBase.runQuery(LinksSpecific.getSqlQuery("FillBaseTable/FillBaseTable_q11"));

            outputArea.append("Running query 12...\r\n");
            conBase.runQuery(LinksSpecific.getSqlQuery("FillBaseTable/FillBaseTable_q12"));

            outputArea.append("Running query 13...\r\n");
            conBase.runQuery(LinksSpecific.getSqlQuery("FillBaseTable/FillBaseTable_q13"));

            outputArea.append("Running query 14...\r\n");
            conBase.runQuery(LinksSpecific.getSqlQuery("FillBaseTable/FillBaseTable_q14"));

            outputArea.append("Running query 15...\r\n");
            conBase.runQuery(LinksSpecific.getSqlQuery("FillBaseTable/FillBaseTable_q15"));

            outputArea.append("Running query 16...\r\n");
            conBase.runQuery(LinksSpecific.getSqlQuery("FillBaseTable/FillBaseTable_q16"));

            outputArea.append("Running query 17...\r\n");
            conBase.runQuery(LinksSpecific.getSqlQuery("FillBaseTable/FillBaseTable_q17"));

            outputArea.append("Running query 18...\r\n");
            conBase.runQuery(LinksSpecific.getSqlQuery("FillBaseTable/FillBaseTable_q18"));

            outputArea.append("Running query 19...\r\n");
            conBase.runQuery(LinksSpecific.getSqlQuery("FillBaseTable/FillBaseTable_q19"));

            outputArea.append("Running query 20...\r\n");
            conBase.runQuery(LinksSpecific.getSqlQuery("FillBaseTable/FillBaseTable_q20"));

            outputArea.append("Running query 21...\r\n");
            conBase.runQuery(LinksSpecific.getSqlQuery("FillBaseTable/FillBaseTable_q21"));

            outputArea.append("Running query 22...\r\n");
            conBase.runQuery(LinksSpecific.getSqlQuery("FillBaseTable/FillBaseTable_q22"));

            outputArea.append("Running query 23...\r\n");
            conBase.runQuery(LinksSpecific.getSqlQuery("FillBaseTable/FillBaseTable_q23"));

            outputArea.append("Running query 24...\r\n");
            conBase.runQuery(LinksSpecific.getSqlQuery("FillBaseTable/FillBaseTable_q24"));

            outputArea.append("Running query 25...\r\n");
            conBase.runQuery(LinksSpecific.getSqlQuery("FillBaseTable/FillBaseTable_q25"));

            outputArea.append("Running query 26...\r\n");
            conBase.runQuery(LinksSpecific.getSqlQuery("FillBaseTable/FillBaseTable_q26"));

            outputArea.append("Running query 27...\r\n");
            conBase.runQuery(LinksSpecific.getSqlQuery("FillBaseTable/FillBaseTable_q27"));

            outputArea.append("Running query 28...\r\n");
            conBase.runQuery(LinksSpecific.getSqlQuery("FillBaseTable/FillBaseTable_q28"));

            outputArea.append("Running query 29...\r\n");
            conBase.runQuery(LinksSpecific.getSqlQuery("FillBaseTable/FillBaseTable_q29"));

            outputArea.append("Running query 30...\r\n");
            conBase.runQuery(LinksSpecific.getSqlQuery("FillBaseTable/FillBaseTable_q30"));

            outputArea.append("Running query 31...\r\n");
            conBase.runQuery(LinksSpecific.getSqlQuery("FillBaseTable/FillBaseTable_q31"));

            outputArea.append("Running query 32...\r\n");
            conBase.runQuery(LinksSpecific.getSqlQuery("FillBaseTable/FillBaseTable_q32"));

            outputArea.append("Running query 33...\r\n");
            conBase.runQuery(LinksSpecific.getSqlQuery("FillBaseTable/FillBaseTable_q33"));
            
            //outputArea.append("Running query 41...\r\n");
            //conBase.runQuery(LinksSpecific.getSqlQuery("FillBaseTable/FillBaseTable_q41"));

            //outputArea.append("Running query 42...\r\n");
            //conBase.runQuery(LinksSpecific.getSqlQuery("FillBaseTable/FillBaseTable_q42"));

            //outputArea.append("Running query 43...\r\n");
            //conBase.runQuery(LinksSpecific.getSqlQuery("FillBaseTable/FillBaseTable_q43"));

            //outputArea.append("Running query 44...\r\n");
            //conBase.runQuery(LinksSpecific.getSqlQuery("FillBaseTable/FillBaseTable_q44"));

            //outputArea.append("Running query 45...\r\n");
            //conBase.runQuery(LinksSpecific.getSqlQuery("FillBaseTable/FillBaseTable_q45"));

            //outputArea.append("Running query 46...\r\n");
            //conBase.runQuery(LinksSpecific.getSqlQuery("FillBaseTable/FillBaseTable_q46"));

            //outputArea.append("Running query 47...\r\n");
            //conBase.runQuery(LinksSpecific.getSqlQuery("FillBaseTable/FillBaseTable_q47"));
        }
        */

        //if( showmsg ) { showMessage( endl, false, false ); }
    }


    /**
     *
     * @param s
     * @param t
     * @return
     */
    public static int levenshtein(String s, String t)
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
    }


    /**
     *
     * @param seconds
     * @return
     */
    public static String stopWatch(int seconds)
    {
        int minutes = seconds / 60;
        int restsec = seconds % 60;
        int uren = minutes / 60;
        int restmin = minutes % 60;

        String urenText = "";
        String minutenText = "";
        String secondenText = "";

        if (uren < 10) {
            urenText = "0";
        }
        if (restmin < 10) {
            minutenText = "0";
        }
        if (restsec < 10) {
            secondenText = "0";
        }

        return urenText + uren + ":" + minutenText + restmin + ":" + secondenText + restsec;
    }


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
    }


    /**
     *
     * @throws Exception
     */
    private void createTempFirstnameFile() throws Exception {
        String filename = "firstname_t_split.csv";
        showMessage( "Creating file " + filename, false, true);
        writerFirstname = new java.io.FileWriter( filename );
    }


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
    }


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
    }


    /**
     *
     * @throws Exception
     */
    public void removeFirstnameFile() throws Exception {
        String filename = "firstname_t_split.csv";
        showMessage( "Removing file " + filename, false, true);
        File file = new File( filename );
        file.delete();
    }


    /**
     *
     * @throws Exception
     */
    public void removeFirstnameTable() throws Exception {
        String tablename = "firstname_t_split";
        showMessage( "Removing table " + tablename, false, true);
        String query = "DROP TABLE " + tablename + ";";
        conTemp.runQuery( query );
    }
}

