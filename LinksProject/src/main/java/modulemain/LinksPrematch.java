package modulemain;

import java.io.File;
import java.sql.ResultSet;

import javax.swing.JTextArea;
import javax.swing.JTextField;

import com.google.common.collect.Range;

import connectors.MySqlConnector;
import general.Functions;

/**
 * @author Omar Azouguagh
 * @author Fons Laan
 *
 * <p/>
 * FL-29-Jul-2014 Remove hard-code usr's/pwd's
 * FL-05-Aug-2014 Latest change
 */

public class LinksPrematch extends Thread
{
    private String db_url;
    private String db_user;
    private String db_pass;

    private boolean bSplitNames;
    private boolean bUniqueTables;
    private boolean bLevenshtein;
    private boolean bNamesToNo;
    private boolean bBaseTable;

    private boolean showmsg = true;
    private String endl = ". OK.";              // ".";

    private JTextField ti;
    private JTextArea ta;

    private MySqlConnector conCleaned;
    private MySqlConnector conPrematch;
    private MySqlConnector conTemp;
    private MySqlConnector conBase;
    private MySqlConnector conFrequency;

    private java.io.FileWriter writerFirstname;

    /**
     * Constructor
     * called from linksPrematch
     *
     * @param db_url
     * @param db_user
     * @param db_pass
     * @param ti
     * @param ta
     * @param bSplitNames
     * @param bUniqueTables
     * @param bLevenshtein
     * @param bNamesToNo
     * @param bBaseTable
     * @throws Exception
     */
    public LinksPrematch
    (
        String db_url,
        String db_user,
        String db_pass,
        JTextField ti,
        JTextArea ta,

        boolean bSplitNames,
        boolean bUniqueTables,
        boolean bLevenshtein,
        boolean bNamesToNo,
        boolean bBaseTable
    )
    throws Exception
    {
        this.db_url  = db_url;
        this.db_user = db_user;
        this.db_pass = db_pass;

        this.bSplitNames   = bSplitNames;
        this.bUniqueTables = bUniqueTables;
        this.bLevenshtein  = bLevenshtein;
        this.bNamesToNo    = bNamesToNo;
        this.bBaseTable    = bBaseTable;

        this.ti = ti;
        this.ta = ta;

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
    public LinksPrematch( String db_url, String db_user, String db_pass, JTextArea t, JTextField ti )
    throws Exception
    {
        conCleaned   = new MySqlConnector( db_url, "links_cleaned",   db_user, db_pass );
        conPrematch  = new MySqlConnector( db_url, "links_prematch",  db_user, db_pass );
        conTemp      = new MySqlConnector( db_url, "links_temp",      db_user, db_pass );
        conBase      = new MySqlConnector( db_url, "links_base",      db_user, db_pass );
        conFrequency = new MySqlConnector( db_url, "links_frequency", db_user, db_pass );

        this.ti = ti;
        this.ta = t;
    }

    /**
     *
     */
    @Override
    public void run()
    {
        String mmss = "";
        String msg  = "";

        showMessage( "LinksPrematch/run()", false, true );

        long startTotal = System.currentTimeMillis();

        try {
            if( bSplitNames ) {
                showMessage( "Splitting names...", false, true );
                long start = System.currentTimeMillis();
                doSplitName();
                long stop = System.currentTimeMillis();
                mmss = Functions.millisec2hms( start, stop );
                msg = "Splitting names OK " + mmss;
                showMessage( msg, false, true );
            }

            if( bUniqueTables ) {
                showMessage( "Creating unique name tables...", false, true );
                long start = System.currentTimeMillis();
                doUniqueNameTables();
                long stop = System.currentTimeMillis();
                mmss = Functions.millisec2hms( start, stop );
                msg = "Creating Unique name table OK " + mmss;
                showMessage( msg, false, true );
            }

            if( bLevenshtein ) {
                showMessage( "Computing Levenshtein...", false, true );
                long start = System.currentTimeMillis();
                doLevenshtein();
                long stop = System.currentTimeMillis();
                mmss = Functions.millisec2hms( start, stop );
                msg = "Computing Levenshtein OK " + mmss;
                showMessage( msg, false, true );
            }

            if( bNamesToNo ) {
                showMessage( "Converting Names to Numbers...", false, true );
                long start = System.currentTimeMillis();
                doToNumber();
                long stop = System.currentTimeMillis();
                mmss = Functions.millisec2hms( start, stop );
                msg = "Converting Names to Numbers OK " + mmss;
                showMessage( msg, false, true );
            }

            if( bBaseTable ) {
                showMessage( "Creating Base Table...", false, true );
                long start = System.currentTimeMillis();
                doCreateBaseTable();
                long stop = System.currentTimeMillis();
                mmss = Functions.millisec2hms( start, stop );
                msg = "Creating Base Table OK " + mmss;
                showMessage( msg, false, true );
            }

            long stopTotal = System.currentTimeMillis();
            mmss = Functions.millisec2hms( startTotal, stopTotal );
            msg = "Elapsed: " + mmss;
            showMessage( msg, false, true );

            this.stop();

        } catch( Exception ex ) { ta.append( ex.getMessage() ); }

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
        ti.setText( logText );

        if( !isMinOnly ) {
            String newLineToken = "";
            if( newLine ) {
                newLineToken = "\r\n";
            }

            if( logText != endl ) {
                String ts = LinksSpecific.getTimeStamp2( "hh:mm:ss" );
                System.out.printf( "%s ", ts );
                ta.append( ts + " " );
            }

            System.out.printf( "%s%s", logText, newLineToken );
            ta.append( logText + newLineToken );
        }
    }



    /**
     * 
     * @throws Exception 
     */
    public void doSplitName() throws Exception
    {
        String query = "SELECT id_person , firstname FROM person_c WHERE firstname is not null AND firstname <> ''";

        ResultSet rsFirstName = conCleaned.runQueryWithResult(query);

        createTempFirstname();
        createTempFirstnameFile();

        while (rsFirstName.next()) {

            int id_person = rsFirstName.getInt("id_person");
            String firstname = rsFirstName.getString("firstname");

            String[] fn = firstname.split(" ", 4);

            String p0 = "";
            String p1 = "";
            String p2 = "";
            String p3 = "";

            if (fn.length > 0) {
                p0 = fn[0];

                if (fn.length > 1) {
                    p1 = fn[1];

                    if (fn.length > 2) {
                        p2 = fn[2];

                        if (fn.length > 3) {
                            p3 = fn[3];
                        }
                    }
                }
            }

            String q = id_person + "," + p0 + "," + p1 + "," + p2 + "," + p3;

            writerFirstname.write(q + "\n");
        }

        rsFirstName.close();
        rsFirstName = null;

        loadFirstnameToTable();
        updateFirstnameToPersonC();
        removeFirstnameFile();
        removeFirstnameTable();
    }


    /**
     *
     */
    public void doUniqueNameTables() throws Exception
    {
        if( showmsg ) { showMessage( "Creating unique tables...", false, true ); }

        dropTableFrequency();

        // Execute queries
        int nqFirst = 1;
        int nqLast = 28;
        String qPrefix = "FrequencyTables/FrequencyTables_q";

        for( int n = nqFirst; n <= nqLast; n++ ) {
            String qPath = String.format( qPrefix + "%02d", n );
            showMessage( "Running query " + qPath, false, true );
            //System.out.println( qPath );
            String query = LinksSpecific.getSqlQuery( qPath );
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
    public void doBasicName() throws Exception
    {
        String qPath = "";

        // run preparing queries
        ta.append( "01" + "\r\n" );
        qPath = "SetVariants/SetVariants_q01";
        showMessage( "Running query " + qPath, false, true );
        String s01 = LinksSpecific.getSqlQuery( qPath );
        String[] a01 = s01.split(";");

        for (int i = 0; i < a01.length; i++) {
            conFrequency.runQuery(a01[i]);
        }

        ta.append( "02" + "\r\n" );
        qPath = "SetVariants/SetVariants_q02";
        showMessage( "Running query " + qPath, false, true );
        String s02 = LinksSpecific.getSqlQuery( qPath );
        String[] a02 = s02.split(";");

        for (int i = 0; i < a02.length; i++) {
            conFrequency.runQuery(a02[i]);
        }

        ta.append( "03" + "\r\n" );
        qPath = "SetVariants/SetVariants_q03";
        showMessage( "Running query " + qPath, false, true );
        String s03 = LinksSpecific.getSqlQuery( qPath );
        String[] a03 = s03.split(";");

        for (int i = 0; i < a03.length; i++) {
            conFrequency.runQuery(a03[i]);
        }

        ta.append("First 3 SQL statements done, beginning with LV" + "\r\n");

        // Run the variants
        prematch.VariantLs vlFam = new prematch.VariantLs(ta, ti, "familyname");
        prematch.VariantLs vlFir = new prematch.VariantLs(ta, ti, "firstname");

        vlFam.computeVariants();
        vlFir.computeVariants();

        ta.append("LV DONE" + "\r\n");

        qPath = "SetVariants/SetVariants_q04";
        showMessage( "Running query " + qPath, false, true );
        String s04 = LinksSpecific.getSqlQuery( qPath );
        String[] a04 = s04.split(";");

        for (int i = 0; i < a04.length; i++) {
            conFrequency.runQuery(a04[i]);
        }

        ta.append( "04" + "\r\n" );
    }


    /**
     * 
     * @throws Exception 
     */
    public void doToNumber() throws Exception
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
            showMessage( "Running query " + qPath, false, true );
            //System.out.println( qPath );
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
        ta.append("Exitcode0 = " + exitValue + "\r\n");

        process = runtime.exec(new String[]{"/bin/bash", "-c", "./latest.pl firstname firstname1"});
        exitValue = process.waitFor();
        ta.append("Exitcode1 = " + exitValue + "\r\n");

        process = runtime.exec(new String[]{"/bin/bash", "-c", "./latest.pl firstname firstname2"});
        exitValue = process.waitFor();
        ta.append("Exitcode2 = " + exitValue + "\r\n");

        process = runtime.exec(new String[]{"/bin/bash", "-c", "./latest.pl firstname firstname3"});
        exitValue = process.waitFor();
        ta.append("Exitcode3 = " + exitValue + "\r\n");

        process = runtime.exec(new String[]{"/bin/bash", "-c", "./latest.pl firstname firstname4"});
        exitValue = process.waitFor();
        ta.append("Exitcode4 = " + exitValue + "\r\n");

        // run File
        Process process = runtime.exec(new String[]{"/bin/bash", "-c", "mysql links_cleaned --user=linksdev --password=db_pass < updates0.sql"});
        exitValue = process.waitFor();
        ta.append("Exitcode_1 = " + exitValue + "\r\n");
        */

        /////
        //Process process = runtime.exec(new String[]{"/bin/bash", "-c", "/etc/init.d/mysqld restart"});
        //exitValue = process.waitFor();
        //ta.append("restart = " + exitValue + "\r\n");
        
        //process = runtime.exec(new String[]{"/bin/bash", "-c", "mysqlcheck --user=db_user --password=db_pass --auto-repair -c -o links_cleaned"});
        //exitValue = process.waitFor();
        //ta.append("optimize = " + exitValue + "\r\n");
        
//        conFrequency.runQuery(LinksSpecific.getSqlQuery("NameToNumber/NameToNumber_q02"));


//        process = runtime.exec(new String[]{"/bin/bash", "-c", "mysql links_cleaned --user=db_user --password=db_pass < updates1.sql"});
//        exitValue = process.waitFor();
//        ta.append("Exitcode_1 = " + exitValue + "\r\n");

        
        
//        process = runtime.exec(new String[]{"/bin/bash", "-c", "/etc/init.d/mysqld restart"});
//        exitValue = process.waitFor();
//        ta.append("restart = " + exitValue + "\r\n");
        
//        process = runtime.exec(new String[]{"/bin/bash", "-c", "mysqlcheck --user=db_user --password=db_pass --auto-repair -c -o links_cleaned"});
//        exitValue = process.waitFor();
//        ta.append("optimize = " + exitValue + "\r\n");
        
//        conFrequency.runQuery(LinksSpecific.getSqlQuery("NameToNumber/NameToNumber_q03"));
        
//        process = runtime.exec(new String[]{"/bin/bash", "-c", "mysql links_cleaned --user=db_user --password=db_pass < updates2.sql"});
//        exitValue = process.waitFor();
//        ta.append("Exitcode_2 = " + exitValue + "\r\n");

        
        
//        process = runtime.exec(new String[]{"/bin/bash", "-c", "/etc/init.d/mysqld restart"});
//        exitValue = process.waitFor();
//        ta.append("restart = " + exitValue + "\r\n");
        
//        process = runtime.exec(new String[]{"/bin/bash", "-c", "mysqlcheck --user=db_user --password=db_pass --auto-repair -c -o links_cleaned"});
//        exitValue = process.waitFor();
//        ta.append("optimize = " + exitValue + "\r\n");
        
//        conFrequency.runQuery(LinksSpecific.getSqlQuery("NameToNumber/NameToNumber_q04"));
//        conFrequency.runQuery(LinksSpecific.getSqlQuery("NameToNumber/NameToNumber_q05"));

        
        //        process = runtime.exec(new String[]{"/bin/bash", "-c", "mysql links_cleaned --user=db_user --password=db_pass < updates3.sql"});
        //        exitValue = process.waitFor();
        //        ta.append("Exitcode_3 = " + exitValue + "\r\n");
        //        
        //        process = runtime.exec(new String[]{"/bin/bash", "-c", "mysql links_cleaned --user=db_user --password=db_pass < updates4.sql"});
        //        exitValue = process.waitFor();
        //        ta.append("Exitcode_4 = " + exitValue + "\r\n");

    }


    /**
     * 
     * @throws Exception 
     */
    public void doLevenshtein() throws Exception
    {
        // "firstname" and "familyname" are links_frequency tables
        System.out.println( conFrequency );
        prematch.Lv lv1 = new prematch.Lv( conFrequency, "firstname",  true,  ti, ta );
        prematch.Lv lv2 = new prematch.Lv( conFrequency, "firstname",  false, ti, ta );
        prematch.Lv lv3 = new prematch.Lv( conFrequency, "familyname", true,  ti, ta );
        prematch.Lv lv4 = new prematch.Lv( conFrequency, "familyname", false, ti, ta );

        lv1.start();
        lv2.start();
        lv3.start();
        lv4.start();
    }


    /**
     * 
     * @throws Exception 
     */
    public void doCreateBaseTable() throws Exception
    {
        if( showmsg ) { showMessage( "Creating LINKS_BASE tables...", false, true ); }

        String qPrefix = "FillBaseTable/FillBaseTable_q";
        int nqFirst = 1;
        int nqLast = 33;
        // q41...q47 were no longer used by Omar; what is their function? (and q34...q40 are missing.)

        for( int n = nqFirst; n <= nqLast; n++ ) {
            String qPath = String.format( qPrefix + "%02d", n );
            showMessage( "Running query " + qPath, false, true );
            //System.out.println( qPath );
            String query = LinksSpecific.getSqlQuery( qPath );

            try {
                conBase.runQuery( query );
            }
            catch( Exception ex ) {
                showMessage( ex.getMessage(), false, true );
            }
        }

        /*
        {
            // TODO remove this
            ta.append("Running query 1...\r\n");
            conBase.runQuery(LinksSpecific.getSqlQuery("FillBaseTable/FillBaseTable_q01"));

            ta.append("Running query 2...\r\n");
            conBase.runQuery(LinksSpecific.getSqlQuery("FillBaseTable/FillBaseTable_q02"));

            ta.append("Running query 3...\r\n");
            conBase.runQuery(LinksSpecific.getSqlQuery("FillBaseTable/FillBaseTable_q03"));

            ta.append("Running query 4...\r\n");
            conBase.runQuery(LinksSpecific.getSqlQuery("FillBaseTable/FillBaseTable_q04"));

            ta.append("Running query 5...\r\n");
            conBase.runQuery(LinksSpecific.getSqlQuery("FillBaseTable/FillBaseTable_q05"));

            ta.append("Running query 6...\r\n");
            conBase.runQuery(LinksSpecific.getSqlQuery("FillBaseTable/FillBaseTable_q06"));

            ta.append("Running query 7...\r\n");
            conBase.runQuery(LinksSpecific.getSqlQuery("FillBaseTable/FillBaseTable_q07"));

            ta.append("Running query 8...\r\n");
            conBase.runQuery(LinksSpecific.getSqlQuery("FillBaseTable/FillBaseTable_q08"));

            ta.append("Running query 9...\r\n");
            conBase.runQuery(LinksSpecific.getSqlQuery("FillBaseTable/FillBaseTable_q09"));

            ta.append("Running query 10...\r\n");
            conBase.runQuery(LinksSpecific.getSqlQuery("FillBaseTable/FillBaseTable_q10"));

            ta.append("Running query 11...\r\n");
            conBase.runQuery(LinksSpecific.getSqlQuery("FillBaseTable/FillBaseTable_q11"));

            ta.append("Running query 12...\r\n");
            conBase.runQuery(LinksSpecific.getSqlQuery("FillBaseTable/FillBaseTable_q12"));

            ta.append("Running query 13...\r\n");
            conBase.runQuery(LinksSpecific.getSqlQuery("FillBaseTable/FillBaseTable_q13"));

            ta.append("Running query 14...\r\n");
            conBase.runQuery(LinksSpecific.getSqlQuery("FillBaseTable/FillBaseTable_q14"));

            ta.append("Running query 15...\r\n");
            conBase.runQuery(LinksSpecific.getSqlQuery("FillBaseTable/FillBaseTable_q15"));

            ta.append("Running query 16...\r\n");
            conBase.runQuery(LinksSpecific.getSqlQuery("FillBaseTable/FillBaseTable_q16"));

            ta.append("Running query 17...\r\n");
            conBase.runQuery(LinksSpecific.getSqlQuery("FillBaseTable/FillBaseTable_q17"));

            ta.append("Running query 18...\r\n");
            conBase.runQuery(LinksSpecific.getSqlQuery("FillBaseTable/FillBaseTable_q18"));

            ta.append("Running query 19...\r\n");
            conBase.runQuery(LinksSpecific.getSqlQuery("FillBaseTable/FillBaseTable_q19"));

            ta.append("Running query 20...\r\n");
            conBase.runQuery(LinksSpecific.getSqlQuery("FillBaseTable/FillBaseTable_q20"));

            ta.append("Running query 21...\r\n");
            conBase.runQuery(LinksSpecific.getSqlQuery("FillBaseTable/FillBaseTable_q21"));

            ta.append("Running query 22...\r\n");
            conBase.runQuery(LinksSpecific.getSqlQuery("FillBaseTable/FillBaseTable_q22"));

            ta.append("Running query 23...\r\n");
            conBase.runQuery(LinksSpecific.getSqlQuery("FillBaseTable/FillBaseTable_q23"));

            ta.append("Running query 24...\r\n");
            conBase.runQuery(LinksSpecific.getSqlQuery("FillBaseTable/FillBaseTable_q24"));

            ta.append("Running query 25...\r\n");
            conBase.runQuery(LinksSpecific.getSqlQuery("FillBaseTable/FillBaseTable_q25"));

            ta.append("Running query 26...\r\n");
            conBase.runQuery(LinksSpecific.getSqlQuery("FillBaseTable/FillBaseTable_q26"));

            ta.append("Running query 27...\r\n");
            conBase.runQuery(LinksSpecific.getSqlQuery("FillBaseTable/FillBaseTable_q27"));

            ta.append("Running query 28...\r\n");
            conBase.runQuery(LinksSpecific.getSqlQuery("FillBaseTable/FillBaseTable_q28"));

            ta.append("Running query 29...\r\n");
            conBase.runQuery(LinksSpecific.getSqlQuery("FillBaseTable/FillBaseTable_q29"));

            ta.append("Running query 30...\r\n");
            conBase.runQuery(LinksSpecific.getSqlQuery("FillBaseTable/FillBaseTable_q30"));

            ta.append("Running query 31...\r\n");
            conBase.runQuery(LinksSpecific.getSqlQuery("FillBaseTable/FillBaseTable_q31"));

            ta.append("Running query 32...\r\n");
            conBase.runQuery(LinksSpecific.getSqlQuery("FillBaseTable/FillBaseTable_q32"));

            ta.append("Running query 33...\r\n");
            conBase.runQuery(LinksSpecific.getSqlQuery("FillBaseTable/FillBaseTable_q33"));
            
            //ta.append("Running query 41...\r\n");
            //conBase.runQuery(LinksSpecific.getSqlQuery("FillBaseTable/FillBaseTable_q41"));

            //ta.append("Running query 42...\r\n");
            //conBase.runQuery(LinksSpecific.getSqlQuery("FillBaseTable/FillBaseTable_q42"));

            //ta.append("Running query 43...\r\n");
            //conBase.runQuery(LinksSpecific.getSqlQuery("FillBaseTable/FillBaseTable_q43"));

            //ta.append("Running query 44...\r\n");
            //conBase.runQuery(LinksSpecific.getSqlQuery("FillBaseTable/FillBaseTable_q44"));

            //ta.append("Running query 45...\r\n");
            //conBase.runQuery(LinksSpecific.getSqlQuery("FillBaseTable/FillBaseTable_q45"));

            //ta.append("Running query 46...\r\n");
            //conBase.runQuery(LinksSpecific.getSqlQuery("FillBaseTable/FillBaseTable_q46"));

            //ta.append("Running query 47...\r\n");
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
    private void createTempFirstname() throws Exception
    {
        String c = "CREATE  TABLE links_temp.firstname_t_split ("
                + " person_id INT UNSIGNED NOT NULL AUTO_INCREMENT ,"
                + " firstname1 VARCHAR(30) NULL ,"
                + " firstname2 VARCHAR(30) NULL ,"
                + " firstname3 VARCHAR(30) NULL ,"
                + " firstname4 VARCHAR(30) NULL ,"
                + " PRIMARY KEY (person_id) );";

        conTemp.runQuery(c);
    }


    /**
     *
     * @throws Exception
     */
    private void createTempFirstnameFile() throws Exception {
        writerFirstname = new java.io.FileWriter( "firstname_t_split.csv" );
    }


    /**
     *
     * @throws Exception
     */
    private void loadFirstnameToTable()
    throws Exception
    {
        if( showmsg ) { showMessage( "Loading CSV data into temp table...", false, false ); }
        {
            String query = "LOAD DATA LOCAL INFILE 'firstname_t_split.csv' INTO TABLE firstname_t_split FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' ( person_id , firstname1 , firstname2 , firstname3 , firstname4 );";
            conTemp.runQuery(query);
        }
        if( showmsg ) { showMessage( endl, false, true ); }
    }


    /**
     *
     */
    private void updateFirstnameToPersonC()
    throws Exception
    {
        if( showmsg ) { showMessage( "Moving first names from temp table to person_c...", false, false ); }

        String query = "UPDATE links_cleaned.person_c, links_temp.firstname_t_split"
                + " SET "
                + " links_cleaned.person_c.firstname1 = links_temp.firstname_t_split.firstname1 ,"
                + " links_cleaned.person_c.firstname2 = links_temp.firstname_t_split.firstname2 ,"
                + " links_cleaned.person_c.firstname3 = links_temp.firstname_t_split.firstname3 ,"
                + " links_cleaned.person_c.firstname4 = links_temp.firstname_t_split.firstname4"
                + " WHERE links_cleaned.person_c.id_person = links_temp.firstname_t_split.person_id;";

        conTemp.runQuery(query);

        if( showmsg ) { showMessage(endl, false, true); }
    }


    /**
     *
     * @throws Exception
     */
    public void removeFirstnameFile() throws Exception {
        File file = new File("firstname_t_split.csv");
        file.delete();
    }


    /**
     *
     * @throws Exception
     */
    public void removeFirstnameTable() throws Exception {
        String query = "DROP TABLE firstname_t_split;";
        conTemp.runQuery(query);
    }
}
