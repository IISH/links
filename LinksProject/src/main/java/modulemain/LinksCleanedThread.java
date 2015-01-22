package modulemain;

import java.io.File;
import java.io.FileWriter;
import java.io.PrintStream;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.LinkedList;
import java.util.Queue;
import java.util.Set;

import javax.swing.JTextArea;
import javax.swing.JTextField;

import dataset.DateYearMonthDaySet;
import dataset.DivideMinMaxDatumSet;
import dataset.Options;
import dataset.MinMaxDateSet;
import dataset.MinMaxMainAgeSet;
import dataset.MinMaxYearSet;
import dataset.PersonC;
import dataset.RegistrationC;
import dataset.TabletoArrayListMultimap;

import connectors.MySqlConnector;
import enumdefinitions.TableType;
import enumdefinitions.TimeType;
import linksmanager.ManagerGui;
import general.Functions;
import general.PrintLogger;

/**
 * @author Omar Azouguagh
 * @author Fons Laan
 *
 * <p/>
 * FL-30-Jun-2014 Imported from OA backup
 * FL-28-Jul-2014 Timing functions
 * FL-20-Aug-2014 Occupation added
 * FL-13-Oct-2014 Removed ttal code
 * FL-22-Jan-2015 Latest change
 *
 * TODO check all occurrences of TODO
 */

public class LinksCleanedThread extends Thread
{
    // Table -> ArrayListMultiMap
    private TabletoArrayListMultimap almmPrepiece;      // Names
    private TabletoArrayListMultimap almmSuffix;        // Names
    private TabletoArrayListMultimap almmAlias;         // Names
    private TabletoArrayListMultimap almmFirstname;     // Names
    private TabletoArrayListMultimap almmFamilyname;    // Names
    private TabletoArrayListMultimap almmLocation;      // Location
    private TabletoArrayListMultimap almmOccupation;    // Occupation
    private TabletoArrayListMultimap almmReport;        // Report warnings
    private TabletoArrayListMultimap almmRole;          // Role
    private TabletoArrayListMultimap almmCivilstatus;   // Civilstatus & Gender
    private TabletoArrayListMultimap almmSex;           // Civilstatus & Gender
    private TabletoArrayListMultimap almmMarriageYear;  // min/max marriage year
    private TabletoArrayListMultimap almmLitAge;        // age_literal

    private JTextField outputLine;
    private JTextArea  outputArea;

    private MySqlConnector dbconRefWrite;       // [remote] reference db for writing new occurrences
    private MySqlConnector dbconRefRead;        // [local] reference db for reading
    private MySqlConnector dbconLog;            // logging of errors/warnings
    private MySqlConnector dbconOriginal;       // original data from A2A
    private MySqlConnector dbconCleaned;        // cleaned, from original

    private Runtime r = Runtime.getRuntime();
    private String logTableName;
    private Options opts;

    private final static String SC_U = "u"; // Unknown Standard value assigned (although the original value is not valid)
    private final static String SC_X = "x"; //    X    Standard yet to be assigned
    private final static String SC_N = "n"; //    No   standard value assigned (original value is not valid)
    private final static String SC_Y = "y"; //    Yes  Standard value assigned (original value is valid)

    private FileWriter writerFirstname;
    private FileWriter writerFamilyname;

    private ManagerGui mg;

    private String ref_url = "";            // reference db access
    private String ref_user = "";
    private String ref_pass = "";
    private String ref_db = "";

    private String url = "";                // links db's access
    private String user = "";
    private String pass = "";

    private String sourceIdsGui;

    private int[] sourceList;                   // either sourceListAvail, or [sourceId] from GUI

    private String endl = ". OK.";              // ".";

    private PrintLogger plog;
    private boolean showskip = false;

    private int count_step = 10000;              // used to be 1000

    /**
     * Constructor
     *
     * @param outputLine
     * @param outputArea
     * @param opts
     * @param mg
     */
    public LinksCleanedThread
    (
            Options opts,
            JTextField outputLine,
            JTextArea  outputArea,
            ManagerGui mg
    )
    {
        this.opts = opts;

        this.plog = opts.getLogger();
        this.sourceIdsGui = opts.getSourceIds();

        this.ref_url  = opts.getDb_ref_url();
        this.ref_user = opts.getDb_ref_user();
        this.ref_pass = opts.getDb_ref_pass();
        this.ref_db   = opts.getDb_ref_db();

        this.url  = opts.getDb_url();
        this.user = opts.getDb_user();
        this.pass = opts.getDb_pass();

        this.outputLine = outputLine;
        this.outputArea = outputArea;

        this.mg = mg;

        /*
        String timestamp = LinksSpecific.getTimeStamp2( "HH:mm:ss" );
        System.out.println( timestamp + "  linksCleaned()" );

        System.out.println( "mysql_hsnref_hosturl:\t"  + ref_url );
        System.out.println( "mysql_hsnref_username:\t" + ref_user );
        System.out.println( "mysql_hsnref_password:\t" + ref_pass );
        System.out.println( "mysql_hsnref_dbname:\t"   + ref_db );
        */
    }


    @Override
    /**
     * run
     */
    public void run()
    {

        /*
        class CleaningThread extends Thread
        {
            String source;

            CleaningThread( String source ) {
                this.source = source;
            }

            public void run()
            {
                long begintime = System.currentTimeMillis();

                int ncores = Runtime.getRuntime().availableProcessors();
                showMessage( "Available cores: " + ncores, false, true );
                int nthreads = java.lang.Thread.activeCount();
                showMessage( "Active threads: " + nthreads, false, true );

                String msg = "CleaningThread, source: " + source;
                System.out.println(msg);
                showMessage(msg, false, true);

                try
                {
                    doRenewData( debug, opts.isDoRenewData(), source );                 // GUI cb: Remove previous data

                    doPrepieceSuffix( debug, opts.isDoNames(), source );                // GUI cb: Prepiece, Suufix

                    doNames( debug, opts.isDoNames(), source );                         // GUI cb: Names

                    doLocations( debug, opts.isDoLocations(), source );                 // GUI cb: Locations

                    doStatusSex( debug, opts.isDoStatusSex(), source );                 // GUI cb: Status and Sex

                    doRegistrationType( debug, opts.isDoRegType(), source );            // GUI cb: Registration Type

                    doOccupation( debug, opts.isDoOccupation(), source );               // GUI cb: Occupation

                    doDates( debug, opts.isDoDates(), source );                         // GUI cb: Dates

                    doMinMaxMarriage( debug, opts.isDoMinMaxMarriage(), source );       // GUI cb: Min Max Marriage

                    doPartsToFullDate( debug, opts.isDoPartsToFullDate(), source );     // GUI cb: Parts to Full Date

                    doDaysSinceBegin( debug, opts.isDoDaysSinceBegin(), source );       // GUI cb: Days since begin

                    doPostTasks( debug, opts.isDoPostTasks(), source );                 // GUI cb: Post Tasks
                }
                catch( Exception ex ) {
                    showMessage( "Error: " + ex.getMessage(), false, true );
                    ex.printStackTrace( new PrintStream( System.out ) );
                }

                msg = "Cleaning sourceId " + source + " is done";
                showTimingMessage( msg, begintime );
                System.out.println( msg );
            }
        }
        */

        try {
            long cleanStart = System.currentTimeMillis();

            //plog.show( "Links Match Manager 2.0" );
            plog.show( "LinksCleanedThread/run()" );
            //int ncores = Runtime.getRuntime().availableProcessors();
            //plog.show( "Available cores: " + ncores );

            logTableName = LinksSpecific.getLogTableName();

            outputLine.setText( "" );
            outputArea.setText( "" );

            connectToDatabases();                                       // Create databases connectors
            createLogTable();                                           // Create log table with timestamp


            int[] sourceListAvail = getOrigSourceIds();                 // get source ids from links_original.registration_o
            sourceList  = createSourceList( sourceIdsGui, sourceListAvail );

            String s = "";
            if( sourceList.length == 1 ) { s = "Processing source: "; }
            else { s = "Processing sources: "; }
            for( int i : sourceList ) { s = s + i + " "; }
            showMessage( s, false, true );


            // links_general.ref_report contains about 75 error definitions,
            // to be used when the normalization encounters errors
            showMessage( "Loading report table...", false, true );
            almmReport = new TabletoArrayListMultimap( dbconRefRead, null, "ref_report", "type", null );
            //almmReport.contentsOld();

            for( int sourceId : sourceList )
            {
                long sourceStart = System.currentTimeMillis();

                String source = Integer.toString( sourceId );

                //CleaningThread ct = new CleaningThread( source );
                //ct.start();

                showMessage_nl();
                showMessage( "Processing sourceId: " + source, false, true );

                doRenewData( opts.isDbgRenewData(), opts.isDoRenewData(), source );                     // GUI cb: Remove previous data

                doPrepieceSuffix( opts.isDbgPrepieceSuffix(), opts.isDoPrepieceSuffix(), source );      // GUI cb: Prepiece, Suufix

                doFirstnames( opts.isDbgFirstnames(), opts.isDoFirstnames(), source );                  // GUI cb: Names

                doFamilynames( opts.isDbgFamilynames(), opts.isDoFamilynames(), source );               // GUI cb: Names

                doLocations( opts.isDbgLocations(), opts.isDoLocations(), source );                     // GUI cb: Locations

                doStatusSex( opts.isDbgStatusSex(), opts.isDoStatusSex(), source );                     // GUI cb: Status and Sex

                doRegistrationType( opts.isDbgRegType(), opts.isDoRegType(), source );                  // GUI cb: Registration Type

                doOccupation( opts.isDbgOccupation(), opts.isDoOccupation(), source );                  // GUI cb: Occupation

                doAge(   opts.isDbgAge(),   opts.isDoDates(), source );                                 // GUI cb: Age
                doRole(  opts.isDbgRole(),  opts.isDoDates(), source );                                 // GUI cb: Role
                doDates( opts.isDbgDates(), opts.isDoDates(), source );                                 // GUI cb: Dates

                doMinMaxMarriage( opts.isDbgMinMaxMarriage(), opts.isDoMinMaxMarriage(), source );      // GUI cb: Min Max Marriage

                doPartsToFullDate( opts.isDbgPartsToFullDate(), opts.isDoPartsToFullDate(), source );   // GUI cb: Parts to Full Date

                doDaysSinceBegin( opts.isDbgDaysSinceBegin(), opts.isDoDaysSinceBegin(), source );      // GUI cb: Days since begin

                doPostTasks( false, opts.isDoPostTasks(), source );                                     // GUI cb: Post Tasks

                String msg = "Cleaning sourceId " + sourceId + " is done";
                elapsedShowMessage( msg, sourceStart, System.currentTimeMillis() );
                System.out.println( msg );
            }

            /*
            // we need to open/close the connections in the threads
            // Close db connections
            dbconRefWrite.close();
            dbconRefRead.close();
            dbconLog.close();
            dbconOriginal.close();
            dbconCleaned.close();
            */

            for( int sourceId : sourceList ) {
                String source = Integer.toString( sourceId );
                doPrematch( opts.isDoPrematch(), source );                   // GUI cb: Run PreMatch
            }

            String msg = "Cleaning is done";
            elapsedShowMessage( msg, cleanStart, System.currentTimeMillis() );
            System.out.println( msg );

        }
        catch (Exception ex) {
            showMessage( "Error: " + ex.getMessage(), false, true );
            ex.printStackTrace( new PrintStream( System.out ) );
        }
    } // run


    /*===< Helper functions >=================================================*/

    /**
     * Read distinct source ids from links_original.registration_o
     * @return
     */
    private int[] getOrigSourceIds()
    {
        ArrayList< String > ids = new ArrayList();
        String query = "SELECT DISTINCT id_source FROM registration_o ORDER BY id_source;";
        try {
            ResultSet rs = dbconOriginal.runQueryWithResult( query );
            int count = 0;
            while( rs.next() ) {
                count += 1;
                String id = rs.getString( "id_source" );
                if( id == null || id.isEmpty() ) { break; }
                else {
                    //System.out.printf( "id: %s\n", id );
                    ids.add(id);
                }
            }
            if( count == 0 ) { showMessage( "Empty links_original ?", false , true); }
        }
        catch( Exception ex ) {
            if( ex.getMessage() != "After end of result set" ) {
                System.out.printf("'%s'\n", ex.getMessage());
                ex.printStackTrace( new PrintStream( System.out ) );
            }
        }
        //System.out.println( ids );

        int[] idsInt = new int[ ids.size() ];
        int i = 0;
        for( String id : ids ) {
            //System.out.println( id );
            idsInt[ i ] = Integer.parseInt(id);
            i += 1;
        }
        return idsInt;
    } // getOrigSourceIds


    /**
     * Get source ids from GUI or links_original.registration_o
     * @return
     */
    private int[] createSourceList( String sourceIdsGui, int[] sourceListAvail )
    {
        int[] idsInt;

        String idsStr[] = sourceIdsGui.split( " " );

        if( idsStr.length == 0  )           // nothing from GUI
        { idsInt = sourceListAvail; }       // use all Ids from links_original.registration_o
        else                                // use GUI supplied Ids
        {
            idsInt = new int[ idsStr.length ];
            for( int i = 0; i < idsStr.length; i++ ) {
            idsInt[ i ] = Integer.parseInt( idsStr[ i ] );
            }
        }

        return idsInt;
    } // createSourceList


    /**
     * @throws Exception
     */
    private void connectToDatabases()
    throws Exception
    {
        boolean debug = false;

        showMessage( "Connecting to databases:", false, true );

        if( debug ) { showMessage( ref_db + " (ref)", false, true ); }
        dbconRefWrite = new MySqlConnector( ref_url, ref_db, ref_user, ref_pass );

        if( debug ) { showMessage( "links_general", false, true ); }
        dbconRefRead = new MySqlConnector( url, "links_general", user, pass );

        if( debug ) { showMessage( "links_original", false, true ); }
        dbconOriginal = new MySqlConnector( url, "links_original", user, pass );

        if( debug ) { showMessage( "links_logs", false, true ); }
        dbconLog = new MySqlConnector( url, "links_logs", user, pass );

        if( debug ) { showMessage( "links_cleaned", false, true ); }
        dbconCleaned = new MySqlConnector( url, "links_cleaned", user, pass );

    } // connectToDatabases


    private Connection getConnection( String dbName )
    throws Exception
    {
        String driver = "org.gjt.mm.mysql.Driver";

        String _url = "jdbc:mysql://" + this.url + "/" + dbName + "?dontTrackOpenResources=true";
        String username = user;
        String password = pass;

        Class.forName( driver );

        // Class.forName("externalModules.jdbcDriver.Driver").newInstance();

        Connection conn = DriverManager.getConnection(_url, username, password);

        return conn;
    }


    private void createLogTable()
    throws Exception
    {
        showMessage( "Creating logging table: " + logTableName , false, true );

        String query = ""
                + " CREATE  TABLE `links_logs`.`" + logTableName + "` ("
                + " `id_log`       INT UNSIGNED NOT NULL AUTO_INCREMENT ,"
                + " `id_source`    INT UNSIGNED NULL ,"
                + " `archive`      VARCHAR(30)  NULL ,"
                + " `location`     VARCHAR(50)  NULL ,"
                + " `reg_type`     VARCHAR(30)  NULL ,"
                + " `date`         VARCHAR(20)  NULL ,"
                + " `sequence`     VARCHAR(20)  NULL ,"
                + " `role`         VARCHAR(30)  NULL ,"
                + " `guid`         VARCHAR(80)  NULL ,"
                + " `reg_key`      INT UNSIGNED NULL ,"
                + " `pers_key`     INT UNSIGNED NULL ,"
                + " `report_class` VARCHAR(2)   NULL ,"
                + " `report_type`  INT UNSIGNED NULL ,"
                + " `content`      VARCHAR(200) NULL ,"
                + " `date_time`    DATETIME NOT NULL ,"
                + " PRIMARY KEY (`id_log`) );";

        dbconLog.runQuery( query );

    } // createLogTable


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

            // prefix string with timestamp
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
     */
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
     * @param logText
     * @param start
     */
    private void showTimingMessage( String logText, long start )
    {
        long stop = System.currentTimeMillis();
        String mmss = Functions.millisec2hms( start, stop );
        String msg = logText + " " + mmss;
        showMessage( msg, false, true );
    }


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


    /**
     * @param id
     * @param id_source
     * @param errorCode
     * @param value
     * @throws Exception
     */
    private void addToReportRegistration( int id, String id_source, int errorCode, String value )
    throws Exception
    {
        boolean debug = false;
        if( debug ) { showMessage( "addToReportRegistration()", false, true ); }

        String errorCodeStr = Integer.toString( errorCode );

        String cla = almmReport.value( "class",   errorCodeStr );
        String con = almmReport.value( "content", errorCodeStr );

        if( debug ) { System.out.println( "cla: " + cla + ", con: " + con ); }

        // WORKAROUND
        // replace error chars
        value = value.replaceAll( "\\\\", "" );
        value = value.replaceAll( "\\$", "" );
        value = value.replaceAll( "\\*", "" );

        con = con.replaceAll( "<.*>", value );
        con = LinksSpecific.prepareForMysql( con );

        // get registration values from links_original.registration_o
        String location = "";
        String reg_type = "";
        String date = "";
        String sequence  = "";
        String guid = "";

        String selectQuery = "SELECT registration_location , registration_type , registration_date , registration_seq , id_persist_registration"
            + " FROM registration_o WHERE id_registration = " + id;

        try {
            ResultSet rs = dbconOriginal.runQueryWithResult( selectQuery );
            rs.next();
            location = rs.getString( "registration_location" );
            reg_type = rs.getString( "registration_type" );
            date     = rs.getString( "registration_date" );
            sequence = rs.getString( "registration_seq" );
            guid     = rs.getString( "id_persist_registration" );
        }
        catch( Exception ex ) {
            showMessage(ex.getMessage(), false, true);
            ex.printStackTrace( new PrintStream( System.out ) );
        }

        // save to links_logs
        String insertQuery = ""
            + " INSERT INTO links_logs.`" + logTableName + "`"
            + " ( reg_key , id_source , report_class , report_type , content , date_time ,"
            + " location , reg_type , date , sequence , guid )"
            + " VALUES ( " + id + " , " + id_source + " , '" + cla.toUpperCase() + "' , " + errorCode + " , '" + con + "' , NOW() ,"
            + " \"" + location + "\" ,"
            + " '" + reg_type + "' , '" + date + "' , '" + sequence + "' , '" + guid + "' ) ; ";

        if( debug ) { showMessage( insertQuery, false, true ); }

        dbconLog.runQuery( insertQuery );
    } // addToReportRegistration


    /**
     * @param id
     * @param id_source
     * @param errorCode
     * @param value
     * @throws Exception
     */
    private void addToReportPerson( int id, String id_source, int errorCode, String value )
    throws Exception
    {
        boolean debug = false;
        if( debug ) { showMessage( "addToReportPerson()", false, true ); }

        String errorCodeStr = Integer.toString( errorCode );

        String cla = almmReport.value( "class",   errorCodeStr );
        String con = almmReport.value( "content", errorCodeStr );

        if( debug ) { System.out.println( "cla: " + cla + ", con: " + con ); }

        // WORKAROUND
        // replace error chars
        value = value.replaceAll( "\\\\", "" );
        value = value.replaceAll( "\\$", "" );
        value = value.replaceAll( "\\*", "" );

        con = con.replaceAll( "<.*>", value );
        con = LinksSpecific.prepareForMysql( con );

        // get id_registration from links_original.person_o
        String id_registration = "";
        String selectQuery1 = "SELECT id_registration FROM person_o WHERE id_person = " + id;
        try {
            ResultSet rs = dbconOriginal.runQueryWithResult( selectQuery1 );
            rs.next();
            id_registration = rs.getString( "id_registration" );
        }
        catch( Exception ex ) {
            showMessage( ex.getMessage(), false, true );
            ex.printStackTrace( new PrintStream( System.out ) );
        }

        // get registration values from links_original.registration_o
        String location = "";
        String reg_type = "";
        String date = "";
        String sequence  = "";
        String guid = "";

        if( !id_registration.isEmpty() )
        {
            String selectQuery2 = "SELECT registration_location , registration_type , registration_date , registration_seq , id_persist_registration"
                 + " FROM registration_o WHERE id_registration = " + id_registration;

            if( debug ) { showMessage( selectQuery2, false, true ); }

            try {
                ResultSet rs = dbconOriginal.runQueryWithResult( selectQuery2 );
                rs.next();
                location = rs.getString( "registration_location" );
                reg_type = rs.getString( "registration_type" );
                date     = rs.getString( "registration_date" );
                sequence = rs.getString( "registration_seq" );
                guid     = rs.getString( "id_persist_registration" );
            }
            catch( Exception ex ) {
                showMessage(ex.getMessage(), false, true);
                ex.printStackTrace( new PrintStream( System.out ) );
            }
        }

        // save to links_logs
        String insertQuery = ""
            + " INSERT INTO links_logs.`" + logTableName + "`"
            + " ( pers_key , id_source , report_class , report_type , content , date_time ,"
            + " location , reg_type , date , sequence , reg_key , guid )"
            + " VALUES ( " + id + " , " + id_source + " , '" + cla.toUpperCase() + "' , " + errorCode + " , '" + con + "' , NOW() ,"
            + " \"" + location + "\" ,"
            + " '" + reg_type + "' , '" + date + "' ,"
            + " \"" + sequence + "\" ,"
            + " '" + id_registration + "' , '" + guid + "' ) ; ";

        if( debug ) { showMessage( insertQuery, false, true ); }

        dbconLog.runQuery( insertQuery );
    } // addToReportPerson

    /*---< End Helper functions >---------------------------------------------*/


    /*===< functions corresponding to GUI Cleaning options >==================*/

    /*---< Remove previous data >---------------------------------------------*/

    /**
     * Remove previous data
     * @param go
     * @throws Exception
     */
    private void doRenewData( boolean debug, boolean go, String source )
    throws Exception
    {
        String funcname = "doRenewData";
        if( !go ) {
            if( showskip ) { showMessage( "Skipping " + funcname, false, true ); }
            return;
        }

        long timeStart = System.currentTimeMillis();
        showMessage( funcname + "...", false, true );

        // Delete cleaned data for given source
        String deleteRegist = "DELETE FROM registration_c WHERE id_source = " + source;
        String deletePerson = "DELETE FROM person_c WHERE id_source = " + source;

        showMessage( "Deleting previous data for source: " + source, false, true );
        if( debug ) {
            showMessage( deleteRegist, false, true );
            showMessage( deletePerson, false, true );
        }
        dbconCleaned.runQuery( deleteRegist );
        dbconCleaned.runQuery( deletePerson );


        // if links_cleaned is now empty, we reset the AUTO_INCREMENT
        // that eases comparison with links_a2a tables
        String qRegistCCount = "SELECT COUNT(*) FROM registration_c";
        String qPersonCCount = "SELECT COUNT(*) FROM person_c";
        ResultSet rsR = dbconCleaned.runQueryWithResult( qRegistCCount );
        ResultSet rsP = dbconCleaned.runQueryWithResult( qPersonCCount );
        rsR.first();
        int registCCount = rsR.getInt( "COUNT(*)" );
        rsP.first();
        int personCCount = rsP.getInt( "COUNT(*)" );

        if( registCCount == 0 && personCCount == 0 ) {
            showMessage( "Resetting AUTO_INCREMENTs for links_cleaned", false, true );
            String auincRegist = "ALTER TABLE registration_c AUTO_INCREMENT = 1";
            String auincPerson = "ALTER TABLE person_c AUTO_INCREMENT = 1";
            dbconCleaned.runQuery( auincRegist );
            dbconCleaned.runQuery( auincPerson );
        }


        // Copy key column data from links_original to links_cleaned
        String keysRegistration = ""
            + "INSERT INTO links_cleaned.registration_c"
            +      " ( id_registration, id_source, id_persist_registration, id_orig_registration, registration_maintype, registration_seq )"
            + " SELECT id_registration, id_source, id_persist_registration, id_orig_registration, registration_maintype, registration_seq"
            + " FROM links_original.registration_o"
            + " WHERE registration_o.id_source = " + source;

        showMessage( "Copying links_original registration keys to links_cleaned", false, true );
        if( debug ) { showMessage( keysRegistration, false, true ); }
        dbconCleaned.runQuery( keysRegistration );

        String keysPerson = ""
            + "INSERT INTO links_cleaned.person_c"
            +      " ( id_person, id_registration, id_source, registration_maintype, id_person_o )"
            + " SELECT id_person, id_registration, id_source, registration_maintype, id_person_o"
            + " FROM links_original.person_o"
            + " WHERE person_o.id_source = " + source;

        showMessage( "Copying links_original person keys to links_cleaned", false, true );
        if( debug ) { showMessage( keysPerson, false, true ); }
        dbconCleaned.runQuery( keysPerson );

        elapsedShowMessage( funcname, timeStart, System.currentTimeMillis() );

    } // doRenewData


    /*---< First- and Familynames >-------------------------------------------*/

    /**
     * Prepiece, Suffix
     * @param go
     * @throws Exception
     */
    private void doPrepieceSuffix( boolean debug, boolean go, String source ) throws Exception
    {
        String funcname = "doPrepieceSuffix";
        if( !go ) {
            if( showskip ) { showMessage( "Skipping " + funcname, false, true ); }
            return;
        }

        long timeStart = System.currentTimeMillis();
        showMessage( funcname + "...", false, true );

        almmPrepiece = new TabletoArrayListMultimap( dbconRefRead, dbconRefWrite, "ref_prepiece", "original", "prefix" );
        almmSuffix   = new TabletoArrayListMultimap( dbconRefRead, dbconRefWrite, "ref_suffix",   "original", "standard" );
        //almmAlias    = new TabletoArrayListMultimap( dbconRefRead, dbconRefWrite, "ref_alias",    "original",  null );

        showMessage( "standardPrepiece", false, true );
        standardPrepiece( source );

        showMessage( "standardSuffix", false, true );
        standardSuffix( source );

        // Update reference
        showMessage( "Updating reference tables: Prepiece/Suffix", false, true );
        almmPrepiece.updateTable();
        almmSuffix.updateTable();
        // almmAlias.updateTable();     // almmAlias.add() never called; nothing added to almmAlias

        almmPrepiece.free();
        almmSuffix.free();
        //almmAlias.free();

        elapsedShowMessage( funcname, timeStart, System.currentTimeMillis() );
        showMessage_nl();
    } // doPrepieceSuffix


    /**
     * Firstnames
     * @param go
     * @throws Exception
     */
    private void doFirstnames( boolean debug, boolean go, String source ) throws Exception
    {
        String funcname = "doFirstnames";
        if( !go ) {
            if( showskip ) { showMessage( "Skipping " + funcname, false, true ); }
            return;
        }

        long timeStart = System.currentTimeMillis();
        showMessage( funcname + "...", false, true );

        MySqlConnector dbconTemp = new MySqlConnector( url, "links_temp", user, pass );

        String msg = "";
        long start = 0;

        // Loading Prepiece/Suffix/Alias reference tables
        start = System.currentTimeMillis();
        msg = "Loading Prepiece/Suffix/Alias reference tables";
        showMessage( msg + "...", false, true );

        // almmPrepiece, almmSuffix and almmAlias used by Firstnames & Familynames
        almmPrepiece = new TabletoArrayListMultimap( dbconRefRead, dbconRefWrite, "ref_prepiece", "original", "prefix" );
        almmSuffix   = new TabletoArrayListMultimap( dbconRefRead, dbconRefWrite, "ref_suffix",   "original", "standard" );
        almmAlias    = new TabletoArrayListMultimap( dbconRefRead, dbconRefWrite, "ref_alias",    "original",  null );
        showTimingMessage( msg, start );

        // Firstnames
        String tmp_firstname = "firstname_t_" + source;
        if( doesTableExist( dbconTemp, "links_temp", tmp_firstname ) ) {
            showMessage( "Deleting table links_temp." + tmp_firstname, false, true );
            dropTable( dbconTemp, "links_temp", tmp_firstname );
        }
        createTempFirstnameTable( dbconTemp, source );
        createTempFirstnameFile(  source );

        start = System.currentTimeMillis();
        msg = "Loading reference table: ref_firstname";
        showMessage( msg + "...", false, true );

        almmFirstname = new TabletoArrayListMultimap( dbconRefRead, dbconRefWrite, "ref_firstname", "original", "standard" );
        int numrows = almmFirstname.numrows();
        int numkeys = almmFirstname.numkeys();
        showMessage( "Number of rows in reference table: " + numrows, false, true );
        if( numrows != numkeys )
        { showMessage( "Number of keys in arraylist multimap: " + numkeys, false, true ); }
        showTimingMessage( msg, start );

        msg = "standardFirstname";
        showMessage( msg + "...", false, true );
        standardFirstname( debug, source );
        showTimingMessage( "standardFirstname", start );

        start = System.currentTimeMillis();
        almmFirstname.updateTable();
        almmFirstname.free();

        writerFirstname.close();
        loadFirstnameToTable(     dbconTemp, source );
        updateFirstnameToPersonC( dbconTemp, source );
        removeFirstnameFile(      source );
        removeFirstnameTable(     dbconTemp, source );
        showTimingMessage( "remains Firstname", start );

        // Firstnames to lowercase
        start = System.currentTimeMillis();
        msg = "Converting firstnames to lowercase";
        showMessage( msg + "...", false, true ) ;
        String qLower = "UPDATE links_cleaned.person_c SET firstname = LOWER( firstname );";
        dbconCleaned.runQuery( qLower );
        showTimingMessage( msg, start );

        almmPrepiece.free();
        almmSuffix.free();
        almmAlias.free();

        dbconTemp.close();

        elapsedShowMessage( funcname, timeStart, System.currentTimeMillis() );
        showMessage_nl();
    } // doFirstnames


    /**
     * Familynames
     * @param go
     * @throws Exception
     */
    private void doFamilynames( boolean debug, boolean go, String source ) throws Exception
    {
        String funcname = "doFamilynames";
        if( !go ) {
            if( showskip ) { showMessage( "Skipping " + funcname, false, true ); }
            return;
        }

        long timeStart = System.currentTimeMillis();
        showMessage( funcname + "...", false, true );

        MySqlConnector dbconTemp = new MySqlConnector( url, "links_temp", user, pass );

        String msg = "";
        long start = 0;

        // Loading Prepiece/Suffix/Alias reference tables
        start = System.currentTimeMillis();
        msg = "Loading Prepiece/Suffix/Alias reference tables";
        showMessage( msg + "...", false, true );

        // almmPrepiece, almmSuffix and almmAlias used by Firstnames & Familynames
        almmPrepiece = new TabletoArrayListMultimap( dbconRefRead, dbconRefWrite, "ref_prepiece", "original", "prefix" );
        almmSuffix   = new TabletoArrayListMultimap( dbconRefRead, dbconRefWrite, "ref_suffix",   "original", "standard" );
        almmAlias    = new TabletoArrayListMultimap( dbconRefRead, dbconRefWrite, "ref_alias",    "original",  null );
        showTimingMessage( msg, start );

        // Familynames
        String tmp_familyname = "familyname_t_" + source;
        if( doesTableExist( dbconTemp, "links_temp",tmp_familyname  ) ) {
            showMessage( "Deleting table links_temp." + tmp_familyname, false, true );
            dropTable( dbconTemp, "links_temp", tmp_familyname );
        }

        createTempFamilynameTable( dbconTemp, source );
        createTempFamilynameFile(  source );

        start = System.currentTimeMillis();
        msg = "Loading reference table: ref_familyname";
        showMessage( msg + "...", false, true );

        almmFamilyname = new TabletoArrayListMultimap( dbconRefRead, dbconRefWrite, "ref_familyname", "original", "standard" );
        int numrows = almmFamilyname.numrows();
        int numkeys = almmFamilyname.numkeys();
        showMessage( "Number of rows in reference table: " + almmFamilyname.numrows(), false, true );
        if( numrows != numkeys )
        { showMessage( "Number of keys in arraylist multimap: " + almmFamilyname.numkeys(), false, true ); }
        showTimingMessage( msg, start );

        msg = "standardFamilyname";
        showMessage( msg + "...", false, true );
        standardFamilyname( debug, source );
        showTimingMessage( msg, start );

        start = System.currentTimeMillis();
        msg = "remains Familyname";
        showMessage( msg + "...", false, true );

        almmFamilyname.updateTable();
        almmFamilyname.free();

        writerFamilyname.close();
        loadFamilynameToTable(     dbconTemp, source );
        updateFamilynameToPersonC( dbconTemp, source );
        removeFamilynameFile(      source );
        removeFamilynameTable(     dbconTemp, source );
        showTimingMessage( msg, start );

        // Familynames to lowercase
        start = System.currentTimeMillis();
        msg = "Converting familynames to lowercase";
        showMessage( msg + "...", false, true );
        String qLower = "UPDATE links_cleaned.person_c SET familyname = LOWER( familyname );";
        dbconCleaned.runQuery( qLower );
        showTimingMessage( msg, start );

        almmPrepiece.free();
        almmSuffix.free();
        almmAlias.free();

        dbconTemp.close();

        elapsedShowMessage( funcname, timeStart, System.currentTimeMillis() );
        showMessage_nl();
    } // doFamilynames


    /**
     * @param source
     * @throws Exception
     */
    public void standardFirstname( boolean debug, String source )
    {
        int count = 0;
        int count_empty = 0;
        int count_still = 0;
        int stepstate = count_step;

        try
        {
            // WHY IS A LOCAL CONNECTION USED?
            Connection con = getConnection( "links_original" );
            con.isReadOnly();       // TODO did Omar mean con.setReadOnly(true); ?

            String selectQuery = "SELECT id_person , firstname , stillbirth FROM person_o WHERE id_source = " + source;

            ResultSet rsFirstName = con.createStatement().executeQuery( selectQuery );
            con.createStatement().close();

            // get total
            rsFirstName.last();
            int total = rsFirstName.getRow();
            rsFirstName.beforeFirst();

            while( rsFirstName.next() )
            {
                count++;
                if( count == stepstate ) {
                    showMessage( count + " of " + total, true, true );
                    stepstate += count_step;
                }

                int id_person     = rsFirstName.getInt( "id_person" );
                String firstname  = rsFirstName.getString( "firstname" );
                String stillbirth = rsFirstName.getString( "stillbirth" );

                // Is firstname empty?
                if( firstname != null && !firstname.isEmpty() )
                {
                    firstname = cleanFirstName( firstname );
                    firstname = firstname.toLowerCase();

                    // Check name on aliases
                    String nameNoAlias = standardAlias( debug, id_person, source, firstname, 1107 );

                    // Check on serried spaces; split name on spaces
                    String[] names = nameNoAlias.split( " " );
                    boolean spaces = false;

                    ArrayList< String > preList  = new ArrayList< String >();
                    ArrayList< String > postList = new ArrayList< String >();

                    for( String n : names ) {
                        if( n.isEmpty() ) {
                            spaces = true;
                        } else { // add to list
                            preList.add( n );
                        }
                    }

                    if( spaces ) {
                        addToReportPerson( id_person, source, 1103, "" );      // EC 1103
                    }

                    // loop through the pieces of the name
                    for( int i = 0; i < preList.size(); i++ )
                    {
                        String prename = preList.get( i );       // does this name part exist in ref_firstname?

                        if( almmFirstname.contains( prename ) )
                        {
                            // Check the standard code
                            String standard_code = almmFirstname.code( prename );

                            if( standard_code.equals( SC_Y ) )
                            {
                                postList.add( almmFirstname.standard( prename ) );

                                // stillbirth involves an extra query here.
                                // it should be written to the csv file, and then via the temp table to person_c
                                if( stillbirth == null && 1 == 0 ) {
                                    String updateQuery = PersonC.updateQuery( "stillbirth", stillbirth, id_person );
                                    dbconCleaned.runQuery( updateQuery );
                                    if( stillbirth.equals( "y" ) ) { count_still++; }
                                }
                            }
                            else if( standard_code.equals( SC_U ) )
                            {
                                addToReportPerson( id_person, source, 1100, prename );           // EC 1100
                                postList.add( almmFirstname.standard( prename ) );
                            }
                            else if( standard_code.equals( SC_N ) )
                            {
                                addToReportPerson( id_person, source, 1105, prename );           // EC 1105
                            }
                            else if( standard_code.equals( SC_X ) )
                            {
                                addToReportPerson( id_person, source, 1109, prename );           // EC 1109
                                postList.add(preList.get(i));
                            }
                            else {
                                addToReportPerson( id_person, source, 1100, prename );           // EC 1100
                            }
                        }
                        else    // name part does not exist in ref_firstname
                        {
                            // check on invalid token
                            String nameNoInvalidChars = cleanName( prename );

                            // name contains invalid chars ?
                            if( ! prename.equalsIgnoreCase( nameNoInvalidChars ) )
                            {
                                addToReportPerson( id_person, source, 1104, prename );  // EC 1104

                                // Check if name exists in ref
                                // Does this part exists in ref_name?
                                if( almmFirstname.contains( nameNoInvalidChars ) )
                                {
                                    // Check the standard code
                                    String standard_code = almmFirstname.code( nameNoInvalidChars );

                                    if( standard_code.equals( SC_Y ) )
                                    {
                                        postList.add( almmFirstname.standard( nameNoInvalidChars ) );
                                    }
                                    else if( standard_code.equals( SC_U ) )
                                    {
                                        addToReportPerson( id_person, source, 1100, nameNoInvalidChars );    // EC 1100
                                        postList.add( almmFirstname.standard( nameNoInvalidChars ) );
                                    }
                                    else if( standard_code.equals( SC_N ) )
                                    {
                                        addToReportPerson( id_person, source, 1105, nameNoInvalidChars );    // EC 1105
                                    }
                                    else if( standard_code.equals( SC_X ) )
                                    {
                                        addToReportPerson(id_person, source, 1109, nameNoInvalidChars);      // EC 1109
                                        postList.add( nameNoInvalidChars );
                                    }
                                    else { // EC 1100, standard_code not invalid
                                        addToReportPerson(id_person, source, 1100, nameNoInvalidChars);      // EC 1100
                                    }

                                    continue;
                                }

                                // Check on suffix
                                Set< String > keys = almmSuffix.keySet();
                                for( String key : keys )
                                {
                                    if( nameNoInvalidChars.endsWith( " " + key ) && almmSuffix.code( key ).equals( SC_Y ) )
                                    {
                                        addToReportPerson( id_person, source, 1106, nameNoInvalidChars );   // EC 1106

                                        nameNoInvalidChars = nameNoInvalidChars.replaceAll( " " + key, "" );

                                        String query = PersonC.updateQuery( "suffix", key, id_person );     // Set suffix

                                        dbconCleaned.runQuery( query );
                                    }
                                }

                                // check ref_prepiece
                                String nameNoPieces = namePrepiece( debug, nameNoInvalidChars, id_person );

                                if( !nameNoPieces.equals( nameNoInvalidChars ) ) {
                                    addToReportPerson(id_person, source, 1107, nameNoInvalidChars);  // EC 1107
                                }

                                // last check on ref
                                if( almmFirstname.contains( nameNoPieces ) )
                                {
                                    // Check the standard code
                                    String standard_code = almmFirstname.code(nameNoPieces);

                                    if( standard_code.equals( SC_Y ) )
                                    {
                                        postList.add( almmFirstname.standard( nameNoPieces ) );
                                    }
                                    else if( standard_code.equals( SC_U ) )
                                    {
                                        addToReportPerson( id_person, source, 1100, nameNoPieces );    // EC 1100
                                        postList.add( almmFirstname.standard( nameNoPieces ) );
                                    }
                                    else if( standard_code.equals( SC_N ) )
                                    {
                                        addToReportPerson( id_person, source, 1105, nameNoPieces );    // EC 1105
                                    }
                                    else if( standard_code.equals( SC_X ) )
                                    {
                                        addToReportPerson( id_person, source, 1109, nameNoPieces );   // EC 1109
                                        postList.add( nameNoPieces );
                                    }
                                    else { // EC 1100, standard_code not invalid
                                        addToReportPerson( id_person, source, 1100, nameNoPieces );    // EC 1100
                                    }
                                }
                                else {
                                    // name must be added to ref_firstname with standard_code x
                                    almmFirstname.add( nameNoPieces );
                                    postList.add( nameNoPieces );   // Also add name to postlist
                                }
                            }
                            else  // no invalid token
                            {
                                // Check on suffix
                                Set< String > keys = almmSuffix.keySet();
                                for( String key : keys )
                                {
                                    if( nameNoInvalidChars.endsWith( " " + key ) && almmSuffix.code( key ).equals( SC_Y ) )
                                    {
                                        addToReportPerson( id_person, source, 1106, nameNoInvalidChars );   // EC 1106

                                        nameNoInvalidChars = nameNoInvalidChars.replaceAll( " " + key, "" );

                                        String query = PersonC.updateQuery( "suffix", key, id_person );     // Set suffix

                                        dbconCleaned.runQuery( query );
                                    }
                                }

                                // check ref_prepiece
                                String nameNoPieces = namePrepiece( debug, nameNoInvalidChars, id_person );

                                if( !nameNoPieces.equals( nameNoInvalidChars ) ) {
                                    addToReportPerson(id_person, source, 1107, nameNoInvalidChars);   // EC 1107
                                }

                                // last check on ref
                                if( almmFirstname.contains( nameNoPieces ) )
                                {
                                    // Check the standard code
                                    String standard_code = almmFirstname.code(nameNoPieces);

                                    if( standard_code.equals( SC_Y ) )
                                    {
                                        postList.add( almmFirstname.standard( nameNoPieces ) );
                                    }
                                    else if( standard_code.equals( SC_U ) )
                                    {
                                        addToReportPerson( id_person, source, 1100, nameNoPieces );   // EC 1100
                                        postList.add( almmFirstname.standard( nameNoPieces ) );
                                    }
                                    else if( standard_code.equals( SC_N ) )
                                    {
                                        addToReportPerson( id_person, source, 1105, nameNoPieces );    // EC 1105
                                    }
                                    else if( standard_code.equals( SC_X ) )
                                    {
                                        addToReportPerson( id_person, source, 1109, nameNoPieces );    // EC 1109
                                        postList.add( nameNoPieces );
                                    }
                                    else { // EC 1100, standard_code not invalid
                                        addToReportPerson(id_person, source, 1100, nameNoPieces);    // EC 1100
                                    }
                                }
                                else {
                                    // name must be added to ref_firstname with standard_code x
                                    almmFirstname.add( nameNoPieces );
                                    postList.add( nameNoPieces );   // Also add name to postlist
                                }
                            }
                        }
                    }

                    // Write all parts to Person postList
                    String vn = "";

                    for( int i = 0; i < postList.size(); i++ )
                    {
                        vn += postList.get( i );

                        // posible space
                        if( i != ( postList.size() - 1) ) {
                            vn += " ";
                        }
                    }

                    // if vn not empty write to vn
                    if( !vn.isEmpty() ) {
                        //String query = PersonC.updateQuery("firstname", vn, id_person);
                        //dbconCleaned.runQuery(query);

                        writerFirstname.write( id_person + "," + vn.trim().toLowerCase() + "\n" );

                    }

                    preList.clear();
                    postList.clear();
                    preList = null;
                    postList = null;
                }
                else    // First name is empty
                {
                    count_empty++;
                    addToReportPerson( id_person, source, 1101, "" );        // EC 1101
                }

                // close this
                id_person = 0;
                firstname = null;
            }

            // TODO: empty resultset
            //rsFirstName = con.createStatement().executeQuery("SELECT 0;");

            rsFirstName.close();
            con.close();

            int count_new = almmFirstname.newcount();
            String strNew = "";
            if( count_new == 0 ) { strNew = "no new firstnames"; }
            else if( count_new == 1 ) { strNew = "1 new firstname"; }
            else { strNew = "" + count_new + " new firstnames"; }

            String strStill = "";
            if( count_still == 0 ) { strStill = "no stillbirths"; }
            else if( count_still == 1 ) { strStill = "1 stillbirth"; }
            else { strStill = "" + count_still + " stillbirths"; }

            showMessage( count + " firstname records, " + count_empty + " without a firstname, " + strNew + ", " + strStill, false, true );
        }
        catch( Exception ex ) {
            showMessage( "count: " + count + " Exception while cleaning Firstname: " + ex.getMessage(), false, true );
            ex.printStackTrace( new PrintStream( System.out ) );
        }
    } // standardFirstname


    /**
     * @param source
     * @throws Exception
     */
    public void standardFamilyname( boolean debug, String source )
    {
        int count = 0;
        int count_empty = 0;
        int stepstate = count_step;

        try {
            String selectQuery = "SELECT id_person , familyname FROM person_o WHERE id_source = " + source;

            // WHY IS A LOCAL CONNECTION USED?
            Connection con = getConnection( "links_original" );
            con.isReadOnly();       // TODO did Omar mean con.setReadOnly(true); ?

            // Read family names from table
            ResultSet rsFamilyname = con.createStatement().executeQuery( selectQuery );
            con.createStatement().close();

            // get total
            rsFamilyname.last();
            int total = rsFamilyname.getRow();
            rsFamilyname.beforeFirst();

            while( rsFamilyname.next() )
            {
                count++;
                if( count == stepstate ) {
                    showMessage( count + " of " + total, true, true );
                    stepstate += count_step;
                }

                // Get family name
                String familyname = rsFamilyname.getString( "familyname" );
                int id_person = rsFamilyname.getInt( "id_person" );
                if( debug ) { showMessage("count: " + count + ", id_person: " + id_person + ", familyname: " + familyname, false, true); }

                // Check is Familyname is not empty or null
                if( familyname != null && !familyname.isEmpty() )
                {
                    familyname = cleanFamilyname( familyname );
                    familyname = familyname.toLowerCase();

                    // familyname in ref_familyname ?
                    if( almmFamilyname.contains( familyname ) )
                    {
                        // get standard_code
                        String standard_code = almmFamilyname.code(familyname);
                        if( debug ) { showMessage( "code: " + standard_code, false, true ); }

                        // Check the standard code
                        if( standard_code.equals( SC_Y ) )
                        {
                            writerFamilyname.write( id_person + "," + almmFamilyname.standard( familyname ).toLowerCase() + "\n" );
                        }
                        else if( standard_code.equals( SC_U ) )
                        {
                            addToReportPerson( id_person, source, 1000, familyname ); // EC 1000

                            writerFamilyname.write( id_person + "," + almmFamilyname.standard( familyname ).toLowerCase() + "\n" );
                        }
                        else if( standard_code.equals( SC_N ) )
                        {
                            addToReportPerson( id_person, source, 1005, familyname );  // EC 1005
                        }
                        else if( standard_code.equals( SC_X ) )
                        {
                            addToReportPerson( id_person, source, 1009, familyname );  // EC 1009

                            writerFamilyname.write( id_person + "," + familyname.toLowerCase() + "\n" );
                        }
                        else {
                            addToReportPerson( id_person, source, 1010, familyname );  // EC 1010
                        }
                    }
                    else        // Familyname does not exists in ref_familyname
                    {
                        if( debug ) { showMessage( "not in ref_familyname: " + familyname, false, true ); }
                        addToReportPerson( id_person, source, 1002, familyname );  // EC 1002

                        String nameNoSerriedSpaces = familyname.replaceAll( " [ ]+", " " );

                        // Family name contains two or more serried spaces?
                        if( !nameNoSerriedSpaces.equalsIgnoreCase( familyname ) ) {
                            addToReportPerson(id_person, source, 1003, familyname);  // EC 1003
                        }

                        String nameNoInvalidChars = cleanName( nameNoSerriedSpaces );

                        // Family name contains invalid chars ?
                        if( !nameNoSerriedSpaces.equalsIgnoreCase( nameNoInvalidChars ) ) {
                            addToReportPerson( id_person, source, 1004, familyname );   // EC 1004
                        }

                        // check if name has prepieces
                        String nameNoPrePiece = namePrepiece( debug, nameNoInvalidChars, id_person );

                        // Family name contains invalid chars ?
                        if( !nameNoPrePiece.equalsIgnoreCase( nameNoInvalidChars ) ) {
                            addToReportPerson( id_person, source, 1008, familyname );  // EC 1008
                        }

                        // Check on aliases
                        String nameNoAlias = standardAlias( debug, id_person, source, nameNoPrePiece, 1007 );

                        // Check on suffix
                        if( debug ) { showMessage( "suffix keySet()", false, true ); }
                        Set< String > keys = almmSuffix.keySet();
                        for( String key : keys )
                        {
                            if( debug ) { showMessage( "suffix key: " + key, false, true ); }
                            if( nameNoAlias.endsWith( " " + key ) ) {
                                addToReportPerson( id_person, source, 1006, nameNoAlias );      // EC 1006

                                nameNoAlias = nameNoAlias.replaceAll( " " + key, "" );

                                PersonC.updateQuery( "suffix", key, id_person );                // Set alias
                            }
                        }

                        // Clean name one more time
                        String nameNoSuffix = LinksSpecific.funcCleanSides( nameNoAlias );

                        // Check name in original
                        if( almmFamilyname.contains( nameNoSuffix ) )
                        {
                            // get standard_code
                            String standard_code = almmFamilyname.code( nameNoSuffix );

                            // Check the standard code
                            if( standard_code.equals( SC_Y ) )
                            {
                                writerFamilyname.write( id_person + "," + almmFamilyname.standard( nameNoSuffix ).toLowerCase() + "\n" );
                            }
                            else if( standard_code.equals( SC_U ) ) {
                                addToReportPerson( id_person, source, 1000, nameNoSuffix );    // EC 1000

                                writerFamilyname.write( id_person + "," + almmFamilyname.standard( nameNoSuffix ).toLowerCase() + "\n" );
                            }
                            else if( standard_code.equals( SC_N ) ) {
                                addToReportPerson( id_person, source, 1005, nameNoSuffix );     // EC 1005
                            }
                            else if( standard_code.equals( SC_X ) ) {
                                addToReportPerson( id_person, source, 1009, nameNoSuffix );    // EC 1009

                                writerFamilyname.write( id_person + "," + nameNoSuffix.toLowerCase() + "\n" );
                            }
                            else {
                                addToReportPerson( id_person, source, 1010, nameNoSuffix );    // EC 1010
                            }
                        } else {
                            // add new familyname
                            almmFamilyname.add( nameNoSuffix );

                            addToReportPerson( id_person, source, 1009, nameNoSuffix );    // EC 1009

                            writerFamilyname.write( id_person + "," + nameNoSuffix.trim().toLowerCase() + "\n" );

                        }
                    }
                }
                else {  // Familyname empty
                    count_empty++;
                    addToReportPerson( id_person, source, 1001, "" );  // EC 1001
                }
            }
            con.close();
            rsFamilyname.close();

            int count_new = almmFamilyname.newcount();
            String strNew = "";
            if( count_new == 0 ) { strNew = "no new familynames"; }
            else if( count_new == 1 ) { strNew = "1 new familyname"; }
            else { strNew = "" + count_new + " new familynames"; }
            showMessage( count + " familyname records, " + count_empty + " without a familyname, " + strNew, false, true );
        }
        catch( Exception ex) {
            showMessage( "count: " + count + " Exception while cleaning familyname: " + ex.getMessage(), false, true );
            ex.printStackTrace( new PrintStream( System.out ) );
        }
    } // standardFamilyname


    /**
     * @param name
     * @param id
     * @return
     * @throws Exception
     */
    private String namePrepiece( boolean debug, String name, int id ) throws Exception
    {
        if( !name.contains( " " ) ) {
            return name;            // if no spaces return name
        }

        String fullName = "";

        String list_TN = "";
        String list_TO = "";
        String list_PF = "";

        // Split name
        Queue< String > names = new LinkedList();

        String[] namesArray = name.split( " " );

        for( int i = 0; i < namesArray.length; i++ ) {
            names.add( namesArray[ i ] );
        }

        // Check pieces
        while( !names.isEmpty() )
        {
            // Get part
            String part = names.poll();

            if( almmPrepiece.contains( part ) && almmPrepiece.code( part ).equalsIgnoreCase( SC_Y ) )
            {
                // Add to person
                if( almmPrepiece.value( "title_noble", part ) != null && !almmPrepiece.value( "title_noble", part ).isEmpty() )
                {
                    list_TN += almmPrepiece.value( "title_noble", part ) + " ";
                }
                else if( almmPrepiece.value( "title_other", part ) != null && !almmPrepiece.value( "title_other", part ).isEmpty() )
                {
                    list_TO += almmPrepiece.value( "title_other", part ) + " ";
                }
                else if( almmPrepiece.value("prefix", part) != null && !almmPrepiece.value( "prefix", part ).isEmpty() )
                {
                    list_PF += almmPrepiece.value( "prefix", part) + " ";
                }

            }
            else    // return name
            {
                while( !names.isEmpty() ) {
                    fullName += " " + names.poll();
                }

                fullName = part + fullName;      // add part to name

                break;
            }
        }

        // remove last spaces
        if( !list_TN.isEmpty() ) {
            list_TN = list_TN.substring( 0, ( list_TN.length() - 1 ) );

            dbconCleaned.runQuery( PersonC.updateQuery( "title_noble", list_TN, id ) );
        }

        if( !list_TO.isEmpty() ) {
            list_TO = list_TO.substring( 0, ( list_TO.length() - 1 ) );

            dbconCleaned.runQuery( PersonC.updateQuery( "title_other", list_TO, id ) );
        }

        if( !list_PF.isEmpty() ) {
            list_PF = list_PF.substring( 0, ( list_PF.length() - 1 ) );

            dbconCleaned.runQuery( PersonC.updateQuery( "prefix", list_PF, id ) );
        }

        return fullName;
    } // namePrepiece


    /**
     * @param source
     * @throws Exception
     */
    public void standardPrepiece( String source )
    {
        int count = 0;
        int stepstate = count_step;

        try {

            String selectQuery = "SELECT id_person , prefix FROM person_o WHERE id_source = " + source + " AND prefix <> ''";

            // WHY IS A LOCAL CONNECTION USED?
            Connection con = getConnection( "links_original" );
            con.isReadOnly();       // TODO did Omar mean con.setReadOnly(true); ?

            // Read family names from table
            ResultSet rsPrepiece = con.createStatement().executeQuery( selectQuery );
            con.createStatement().close();

            // get total
            rsPrepiece.last();
            int total = rsPrepiece.getRow();
            rsPrepiece.beforeFirst();

            while (rsPrepiece.next()) {

                // Create lists
                String listPF = "";
                String listTO = "";
                String listTN = "";

                count++;

                if( count == stepstate ) {
                    showMessage( count + " of " + total, true, true );
                    stepstate += count_step;
                }

                int id_person = rsPrepiece.getInt( "id_person" );
                String prepiece = rsPrepiece.getString( "prefix" ).toLowerCase();

                prepiece = cleanName( prepiece );

                String[] prefixes = prepiece.split( " " );

                for( String part : prefixes )
                {
                    // Does Prefix exist in ref table
                    if( almmPrepiece.contains( part ) )
                    {
                        String standard_code = almmPrepiece.code( part );
                        String prefix        = almmPrepiece.value( "prefix",      part );
                        String title_noble   = almmPrepiece.value( "title_noble", part );
                        String title_other   = almmPrepiece.value( "title_other", part );

                        // standard code x
                        if( standard_code.equals( SC_X ) )
                        {
                            addToReportPerson(id_person, source, 81, part);       // EC 81

                            listPF += part + " ";
                        }
                        else if( standard_code.equals( SC_N ) )
                        {
                            addToReportPerson( id_person, source, 83, part );     // EC 83
                        }
                        else if( standard_code.equals( SC_U ) )
                        {
                            addToReportPerson( id_person, source, 85, part );     // EC 85

                            if( prefix != null && !prefix.isEmpty() ) {
                                listPF += prefix + " ";
                            }
                            else if( title_noble != null && !title_noble.isEmpty() ) {
                                listTN += title_noble + " ";
                            }
                            else if( title_other != null && !title_other.isEmpty() ) {
                                listTO += title_other + " ";
                            }
                        }
                        else if( standard_code.equals( SC_Y ) )
                        {
                            if( prefix != null && !prefix.isEmpty() ) {
                                listPF += prefix + " ";
                            }
                            else if( title_noble != null && !title_noble.isEmpty() ) {
                                listTN += title_noble + " ";
                            }
                            else if( title_other != null && !title_other.isEmpty() ) {
                                listTO += title_other + " ";
                            }
                        }
                        else {  // Standard_code invalid
                            addToReportPerson(id_person, source, 89, part);       // EC 89
                        }
                    }
                    else    // Prefix not in ref
                    {
                        addToReportPerson(id_person, source, 81, part);           // EC 81

                        almmPrepiece.add( part );               // Add Prefix

                        listPF += part + " ";                   // Add to list
                    }
                }

                // write lists to person_c
                if( !listTN.isEmpty() ) {
                    dbconCleaned.runQuery( PersonC.updateQuery( "title_noble", listTN.substring( 0, ( listTN.length() - 1 ) ), id_person ) );
                }

                if( !listTO.isEmpty() ) {
                    dbconCleaned.runQuery( PersonC.updateQuery( "title_other", listTO.substring( 0, ( listTO.length() - 1 ) ), id_person ) );
                }

                if( !listPF.isEmpty() ) {
                    dbconCleaned.runQuery( PersonC.updateQuery( "prefix", listPF.substring( 0, ( listPF.length() - 1 ) ), id_person) ) ;
                }
            }

            rsPrepiece.close();
            con.close();
        }
        catch( Exception ex ) {
            showMessage( "count: " + count + " Exception while cleaning Prepiece: " + ex.getMessage(), false, true );
            ex.printStackTrace( new PrintStream( System.out ) );
        }
    } // standardPrepiece


    /**
     * @param source
     */
    public void standardSuffix( String source )
    {
        int count = 0;
        int stepstate = count_step;

        try {
            String selectQuery = "SELECT id_person , suffix FROM person_o WHERE id_source = " + source + " AND suffix <> ''";

            // WHY IS A LOCAL CONNECTION USED?
            Connection con = getConnection( "links_original" );
            con.isReadOnly();       // TODO did Omar mean con.setReadOnly(true); ?

            // Read family names from table
            ResultSet rsSuffix = con.createStatement().executeQuery( selectQuery );
            con.createStatement().close();

            // get total
            rsSuffix.last();
            int total = rsSuffix.getRow();
            rsSuffix.beforeFirst();

            while( rsSuffix.next() )
            {
                count++;
                if( count == stepstate ) {
                    showMessage( count + " of " + total, true, true );
                    stepstate += count_step;
                }

                int id_person = rsSuffix.getInt( "id_person" );
                String suffix = rsSuffix.getString( "suffix" ).toLowerCase();

                suffix = cleanName( suffix );

                // Check occurrence in ref table
                if( almmSuffix.contains( suffix ) )
                {
                    String standard_code = almmSuffix.code( suffix );

                    if( standard_code.equals( SC_X ) )
                    {
                        addToReportPerson(id_person, source, 71, suffix);     // EC 71

                        String query = PersonC.updateQuery( "suffix", suffix, id_person );
                        dbconCleaned.runQuery( query );
                    }
                    else if( standard_code.equals( SC_N ) )
                    {
                        addToReportPerson( id_person, source, 73, suffix );   // EC 73
                    }
                    else if( standard_code.equals( SC_U ) )
                    {
                        addToReportPerson( id_person, source, 75, suffix );   // EC 74

                        String query = PersonC.updateQuery( "suffix", suffix, id_person );
                        dbconCleaned.runQuery( query );
                    }
                    else if( standard_code.equals( SC_Y ) )
                    {
                        String query = PersonC.updateQuery( "suffix", suffix, id_person );
                        dbconCleaned.runQuery( query );
                    }
                    else {
                        addToReportPerson(id_person, source, 79, suffix);     // EC 75
                    }
                }
                else // Standard code x
                {
                    addToReportPerson( id_person, source, 71, suffix);        // EC 71

                    almmSuffix.add( suffix );

                    String query = PersonC.updateQuery( "suffix", suffix, id_person );
                    dbconCleaned.runQuery( query );

                }
            }

            rsSuffix.close();
            con.close();

        }
        catch( Exception ex ) {
            showMessage( "count: " + count + " Exception while cleaning Suffix: " + ex.getMessage(), false, true );
            ex.printStackTrace( new PrintStream( System.out ) );
        }
    } // standardSuffix


    /**
     * @param id
     * @param source
     * @param name
     * @return
     */
    private String standardAlias( boolean debug, int id, String source, String name, int errCode )
            throws Exception
    {
        name = name.toLowerCase();
        Set< String > keys = almmAlias.keySet();

        for( String keyword : keys )
        {
            if( name.contains( " " + keyword + " " ) )
            {
                addToReportPerson( id, source, errCode, name );      // firstname: EC 1107, familyname: EC 1007

                // prepare on braces
                if( keyword.contains( "\\(" ) || keyword.contains( "\\(" ) ) {
                    keyword = keyword.replaceAll( "\\(", "" ).replaceAll( "\\)", "" );
                }

                String[] names = name.toLowerCase().split( keyword, 2 );

                // we must clean the name because of the braces used in aliases
                // Set alias
                PersonC.updateQuery( "alias", LinksSpecific.funcCleanSides( cleanName( names[ 1 ] ) ), id );

                return LinksSpecific.funcCleanSides( cleanName( names[ 0 ] ) );
            }
        }

        return name;
    } // standardAlias


    /**
     * @param name
     * @return
     */
    private String cleanName( String name ) {
        return name.replaceAll( "[^A-Za-z0-9 '\\-\\.,èêéëÈÊÉËùûúüÙÛÚÜiìîíïÌÎÍÏòôóöÒÔÓÖàâáöÀÂÁÄçÇ]+", "" );
    } // cleanName


    /**
     * @param name
     * @return
     */
    private String cleanFirstName( String name ) {
        return name.replaceAll( "[^A-Za-z0-9 '\\-èêéëÈÊÉËùûúüÙÛÚÜiìîíïÌÎÍÏòôóöÒÔÓÖàâáöÀÂÁÄçÇ]+", "" );
    } // cleanFirstName


    /**
     * @param name
     * @return
     */
    private String cleanFamilyname( String name ) {
        return name.replaceAll( "[^A-Za-z0-9 '\\-èêéëÈÊÉËùûúüÙÛÚÜiìîíïÌÎÍÏòôóöÒÔÓÖàâáöÀÂÁÄçÇ]+", "").replaceAll("\\-", " " );
    } // cleanFamilyname


    /**
     * @throws Exception
     */
    private void createTempFamilynameTable( MySqlConnector dbconTemp, String source ) throws Exception
    {
        String tablename = "familyname_t_" + source;

        showMessage( "Creating " + tablename + " table", false, true );

        String query = "CREATE  TABLE links_temp." + tablename + " ("
            + " person_id INT UNSIGNED NOT NULL AUTO_INCREMENT ,"
            + " familyname VARCHAR(80) NULL ,"
            + " PRIMARY KEY (person_id) );";

        dbconTemp.runQuery( query );

    } // createTempFamilynameTable


    /**
     * @throws Exception
     */
    private void createTempFamilynameFile( String source ) throws Exception
    {
        String filename = "familyname_t_" + source + ".csv";
        showMessage( "Creating " + filename, false, true );

        File f = new File( filename );
        if( f.exists() ) { f.delete(); }
        writerFamilyname = new FileWriter( filename );
    } // createTempFamilynameFile


    /**
     * @throws Exception
     */
    private void loadFamilynameToTable( MySqlConnector dbconTemp, String source ) throws Exception
    {
        showMessage( "Loading CSV data into temp table", false, true );

        String csvname   = "familyname_t_" + source + ".csv";
        String tablename = "familyname_t_" + source;

        String query = "LOAD DATA LOCAL INFILE '" + csvname + "'"
            + " INTO TABLE " + tablename
            + " FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' ( person_id , familyname );";

        dbconTemp.runQuery( query );

    } // loadFamilynameToTable


    /**
     *
     */
    private void updateFamilynameToPersonC( MySqlConnector dbconTemp, String source ) throws Exception
    {
        showMessage( "Moving familynames from temp table to person_c", false, true );

        String tablename = "familyname_t_" + source;

        String query = "UPDATE links_cleaned.person_c, links_temp."   + tablename
            + " SET links_cleaned.person_c.familyname = links_temp."  + tablename + ".familyname"
            + " WHERE links_cleaned.person_c.id_person = links_temp." + tablename + ".person_id;";

        dbconTemp.runQuery( query );
    } // updateFamilynameToPersonC


    public void removeFamilynameFile( String source ) throws Exception
    {
        String csvname = "familyname_t_" + source + ".csv";

        showMessage( "Removing file " + csvname, false, true );

        java.io.File f = new java.io.File( csvname );
        f.delete();
    } // removeFamilynameFile


    public void removeFamilynameTable( MySqlConnector dbconTemp, String source ) throws Exception
    {
        String tablename = "familyname_t_" + source;

        showMessage( "Removing table " + tablename, false, true );

        String query = "DROP TABLE IF EXISTS " + tablename + ";";

        dbconTemp.runQuery( query );
    } // removeFamilynameTable


    /**
     * @throws Exception
     */
    private void createTempFirstnameTable( MySqlConnector dbconTemp, String source ) throws Exception
    {
        String tablename = "firstname_t_" + source;

        showMessage( "Creating " + tablename + " table", false, true );

        // Notice: the stillbirth column is not yet used
        String query = "CREATE  TABLE links_temp." + tablename + " ("
            + " person_id INT UNSIGNED NOT NULL AUTO_INCREMENT ,"
            + " firstname VARCHAR(80) NULL ,"
            + " stillbirth VARCHAR(3) NULL ,"
            + " PRIMARY KEY (person_id) );";

        dbconTemp.runQuery( query );
    } // createTempFirstnameTable


    /**
     * @throws Exception
     */
    private void createTempFirstnameFile( String source ) throws Exception
    {
        String filename = "firstname_t_" + source + ".csv";
        showMessage( "Creating " + filename, false, true );

        File f = new File( filename );
        if( f.exists() ) { f.delete(); }
        writerFirstname = new FileWriter( filename );
    } // createTempFirstnameFile


    /**
     * @throws Exception
     */
    private void loadFirstnameToTable( MySqlConnector dbconTemp, String source ) throws Exception
    {
        showMessage( "Loading CSV data into temp table", false, true );

        String csvname   = "firstname_t_" + source + ".csv";
        String tablename = "firstname_t_" + source;

        String query = "LOAD DATA LOCAL INFILE '" + csvname + "'"
            + " INTO TABLE " + tablename
            + " FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' ( person_id , firstname );";

        dbconTemp.runQuery( query );
    } // loadFirstnameToTable


    /**
     *
     */
    private void updateFirstnameToPersonC( MySqlConnector dbconTemp, String source ) throws Exception
    {
        showMessage( "Moving first names from temp table to person_c...", false, true );

        String tablename = "firstname_t_" + source;

        String query = "UPDATE links_cleaned.person_c, links_temp."   + tablename
            + " SET links_cleaned.person_c.firstname = links_temp."   + tablename + ".firstname"
            + " WHERE links_cleaned.person_c.id_person = links_temp." + tablename + ".person_id;";

        dbconTemp.runQuery(query);
    } // updateFirstnameToPersonC


    /**
     * @throws Exception
     */
    public void removeFirstnameFile( String source ) throws Exception
    {
        String csvname = "firstname_t_" + source + ".csv";

        showMessage( "Removing file " + csvname, false, true );

        File f = new File( csvname );
        f.delete();
    } // removeFirstnameFile


    /**
     * @throws Exception
     */
    public void removeFirstnameTable( MySqlConnector dbconTemp, String source ) throws Exception
    {
        String tablename = "firstname_t_" + source;

        showMessage( "Removing table " + tablename, false, true );

        String query = "DROP TABLE IF EXISTS " + tablename + ";";
        dbconTemp.runQuery( query );
    } // removeFirstnameTable


    /**
     * @throws Exception
     */
    private boolean doesTableExist( MySqlConnector db_conn, String db_name, String table_name ) throws Exception
    {
        String query = "SELECT COUNT(*) FROM information_schema.tables"
                + " WHERE table_schema = '" + db_name + "'"
                + " AND table_name = '" + table_name + "'";

        ResultSet rs = db_conn.runQueryWithResult( query );
        rs.first();
        int count = rs.getInt( "COUNT(*)" );
        //showMessage( "doesTableExist: " + db_name + " " + table_name + " : " + count, false, true );

        if( count == 1 ) return true;
        else return false;
    } // doesTableExist


    /**
     * @throws Exception
     */
    private void dropTable( MySqlConnector db_conn, String db_name, String table_name ) throws Exception
    {
        String query = "DROP TABLE `" + db_name + "`.`" + table_name + "`";
        db_conn.runQuery( query );
    } // dropTable

    /*
    private void deleteRows()
    throws Exception
    {
        showMessage( "deleteRows() deleting empty links_cleaned.person_c records.", false, true );
        String q1 = "DELETE FROM links_cleaned.person_c WHERE ( familyname = '' OR familyname is null ) AND ( firstname = '' OR firstname is null )";
        dbconCleaned.runQuery( q1 );
    } // deleteRows
    */


    /*---< Locations >--------------------------------------------------------*/

    /**
     * Locations
     * @param go
     * @throws Exception
     */
    private void doLocations( boolean debug, boolean go, String source ) throws Exception
    {
        String funcname = "doLocations";

        if( !go ) {
            if( showskip ) { showMessage( "Skipping " + funcname, false, true ); }
            return;
        }

        long funcstart = System.currentTimeMillis();
        showMessage( funcname + "...", false, true );

        long timeStart = System.currentTimeMillis();
        String msg = "Loading reference table: location";
        showMessage( msg + "...", false, true );
        long start = System.currentTimeMillis();
        almmLocation = new TabletoArrayListMultimap( dbconRefRead, dbconRefWrite, "ref_location", "original", "location_no" );
        showTimingMessage( msg, start );
        int numrows = almmLocation.numrows();
        int numkeys = almmLocation.numkeys();
        showMessage( "Number of rows in reference table: " + numrows, false, true );
        if( numrows != numkeys )
        { showMessage( "Number of keys in arraylist multimap: " + numkeys, false, true ); }

        start = System.currentTimeMillis();
        standardRegistrationLocation( debug, source );
        showTimingMessage( "standardRegistrationLocation ", start );

        start = System.currentTimeMillis();
        standardBirthLocation( debug, source );
        showTimingMessage( "standardBirthLocation ", start );

        start = System.currentTimeMillis();
        standardMarLocation( debug, source );
        showTimingMessage( "standardMarLocation ", start );

        start = System.currentTimeMillis();
        standardLivingLocation( debug, source );
        showTimingMessage( "standardLivingLocation ", start );

        start = System.currentTimeMillis();
        standardDeathLocation( debug, source );
        showTimingMessage( "standardDeathLocation ", start );

        start = System.currentTimeMillis();
        showMessage( "Updating reference table: location...", false, true );
        almmLocation.updateTable();
        showTimingMessage( "Updating reference table: location", start );

        elapsedShowMessage( funcname, timeStart, System.currentTimeMillis() );
        showMessage_nl();
    } // doLocations


    /**
     * @param rs
     * @param idFieldO
     * @param locationFieldO
     * @param locationFieldC
     * @param id_source
     * @param tt
     */
    private void standardLocation( boolean debug, ResultSet rs, String idFieldO, String locationFieldO, String locationFieldC, String id_source, TableType tt )
    throws Exception
    {
        int count = 0;
        int count_empty = 0;
        int stepstate = count_step;

        try
        {
            while( rs.next() )
            {
                count++;
                if( count == stepstate ) {
                    showMessage( count + "", true, true );
                    stepstate += count_step;
                }

                int id = rs.getInt( idFieldO );
                String location = rs.getString( locationFieldO );

                if( location != null && !location.isEmpty() )
                {
                    location = location.toLowerCase();
                    if( debug ) { System.out.println( "id_person: " + id + ", original: " + locationFieldO + ", location: " + location ); }
                    if( almmLocation.contains( location ) )

                    {
                        String refSCode = almmLocation.code( location );
                        if( debug ) { System.out.println( "refSCode: " + refSCode );  }

                        if( refSCode.equals( SC_X ) )             // EC 91
                        {
                            if( tt == TableType.REGISTRATION )
                            {
                                addToReportRegistration( id, id_source, 91, location );
                                String query = RegistrationC.updateIntQuery( locationFieldC, "10010", id );
                                dbconCleaned.runQuery( query );
                            }
                            else
                            {
                                addToReportPerson( id, id_source, 91, location );
                                String query = PersonC.updateIntQuery( locationFieldC, "10010", id );
                                dbconCleaned.runQuery( query );
                            }
                        }
                        else if( refSCode.equals( SC_N ) )       // EC 93
                        {
                            if( tt == TableType.REGISTRATION ) {
                                addToReportRegistration( id, id_source, 93, location );
                            }
                            else
                            {
                                addToReportPerson( id, id_source, 93, location );
                            }
                        }
                        else if( refSCode.equals( SC_U ) )       // EC 95
                        {
                            if( tt == TableType.REGISTRATION )
                            {
                                addToReportRegistration( id, id_source, 95, location );

                                String locationnumber = almmLocation.locationno( location );

                                String query = RegistrationC.updateIntQuery( locationFieldC, locationnumber, id );
                                dbconCleaned.runQuery( query );
                            }
                            else
                            {
                                addToReportPerson( id, id_source, 95, location );

                                String locationnumber = almmLocation.locationno( location );

                                String query = PersonC.updateIntQuery( locationFieldC, locationnumber, id );
                                dbconCleaned.runQuery( query );
                            }
                        }
                        else if( refSCode.equals( SC_Y ) )
                        {
                            if( tt == TableType.REGISTRATION )
                            {
                                String locationnumber = almmLocation.locationno( location );

                                String query = RegistrationC.updateIntQuery( locationFieldC, locationnumber, id );
                                dbconCleaned.runQuery( query );
                            }
                            else
                            {
                                String locationnumber = almmLocation.locationno( location );

                                String query = PersonC.updateIntQuery( locationFieldC, locationnumber, id );
                                dbconCleaned.runQuery( query );
                            }
                        }
                        else
                        {
                            // EC 99
                            if( tt == TableType.REGISTRATION ) {
                                addToReportRegistration( id, id_source, 99, location );
                            } else {
                                addToReportPerson( id, id_source, 99, location );
                            }
                        }
                    }
                    else     // EC 91
                    {
                        if( tt == TableType.REGISTRATION )
                        {
                            addToReportRegistration( id, id_source, 91, location );
                            String query = RegistrationC.updateIntQuery( locationFieldC, "10010", id );
                            dbconCleaned.runQuery( query );
                        }
                        else
                        {
                            addToReportPerson( id, id_source, 91, location );
                            String query = PersonC.updateIntQuery( locationFieldC, "10010", id );
                            dbconCleaned.runQuery( query );
                        }

                        almmLocation.add( location );
                    }
                }
                else
                { count_empty++; }
            }

            int count_new = almmLocation.newcount();
            String strNew = "";
            if( count_new == 0 ) { strNew = "no new locations"; }
            else if( count_new == 1 ) { strNew = "1 new location"; }
            else { strNew = "" + count_new + " new locations"; }
            showMessage( count + " " + locationFieldO + " records, " + count_empty + " without location, " + strNew, false, true );
        }
        catch( Exception ex ) {
            throw new Exception( "count: " + count + " Exception while cleaning Location: " + ex.getMessage() );
        }
    } // standardLocation


    /**
     * @param source
     */
    public void standardRegistrationLocation( boolean debug, String source )
    {
        String selectQuery = "SELECT id_registration , registration_location FROM registration_o WHERE id_source = " + source;
        if( debug ) { showMessage( "standardRegistrationLocation: " + selectQuery, false, true ); }

        try {
            ResultSet rs = dbconOriginal.runQueryWithResult( selectQuery );

            standardLocation( debug, rs, "id_registration", "registration_location", "registration_location_no", source, TableType.REGISTRATION );
        }
        catch( Exception ex ) {
            showMessage( ex.getMessage(), false, true );
            ex.printStackTrace( new PrintStream( System.out ) );
        }
    } // standardRegistrationLocation


    /**
     * @param source
     */
    public void standardBirthLocation( boolean debug, String source )
    {
        String selectQuery = "SELECT id_person , birth_location FROM person_o WHERE id_source = " + source + " AND birth_location <> ''";
        if( debug ) { showMessage( "standardBirthLocation: " + selectQuery, false, true ); }

        try {
            ResultSet rs = dbconOriginal.runQueryWithResult( selectQuery );

            standardLocation( debug, rs, "id_person", "birth_location", "birth_location", source, TableType.PERSON );
        }
        catch( Exception ex ) {
            showMessage( ex.getMessage(), false, true );
            ex.printStackTrace( new PrintStream( System.out ) );
        }
    } // standardBirthLocation


    /**
     * @param source
     */
    public void standardMarLocation( boolean debug, String source )
    {
        String selectQuery = "SELECT id_person , mar_location FROM person_o WHERE id_source = " + source + " AND mar_location <> ''";
        if( debug ) { showMessage( "standardMarLocation: " + selectQuery, false, true ); }

        try {
            ResultSet rs = dbconOriginal.runQueryWithResult( selectQuery );
            standardLocation( debug, rs, "id_person", "mar_location", "mar_location", source, TableType.PERSON );
        }
        catch( Exception ex ) {
            showMessage( ex.getMessage(), false, true );
            ex.printStackTrace( new PrintStream( System.out ) );
        }
    } // standardMarLocation


    /**
     * @param source
     */
    public void standardLivingLocation( boolean debug, String source )
    {
        String selectQuery = "SELECT id_person , living_location FROM person_o WHERE id_source = " + source + " AND living_location <> ''";
        if( debug ) { showMessage( "standardLivingLocation: " + selectQuery, false, true ); }

        try {
            ResultSet rs = dbconOriginal.runQueryWithResult( selectQuery );
            standardLocation( debug, rs, "id_person", "living_location", "living_location", source, TableType.PERSON );
        }
        catch( Exception ex ) {
            showMessage( ex.getMessage(), false, true );
            ex.printStackTrace( new PrintStream( System.out ) );
        }
    } // standardLivingLocation


    /**
     * @param source
     */
    public void standardDeathLocation( boolean debug, String source )
    {
        String selectQuery = "SELECT id_person , death_location FROM person_o WHERE id_source = " + source + " AND death_location <> ''";
        if( debug ) { showMessage( "standardDeathLocation: " + selectQuery, false, true ); }

        try {
            ResultSet rs = dbconOriginal.runQueryWithResult( selectQuery );
            standardLocation( debug, rs, "id_person", "death_location", "death_location", source, TableType.PERSON );
        }
        catch( Exception ex ) {
            showMessage( ex.getMessage(), false, true );
            ex.printStackTrace( new PrintStream( System.out ) );
        }
    } // standardDeathLocation


    /*---< Civil status and Sex >---------------------------------------------*/

    /**
     * Sex and Civilstatus
     * @param go
     * @throws Exception
     */
    private void doStatusSex( boolean debug, boolean go, String source ) throws Exception
    {
        String funcname = "doStatusSex";
        if( !go ) {
            if( showskip ) { showMessage( "Skipping " + funcname, false, true ); }
            return;
        }

        long timeStart = System.currentTimeMillis();
        showMessage( funcname + "...", false, true );

        showMessage( "Loading reference table: ref_status_sex (sex as key)...", false, true );
        almmSex = new TabletoArrayListMultimap( dbconRefRead, dbconRefWrite, "ref_status_sex", "original", "standard_sex" );

        showMessage("Loading reference table: status_sex (civil status as key)...", false, true);
        almmCivilstatus = new TabletoArrayListMultimap( dbconRefRead, dbconRefWrite, "ref_status_sex", "original", "standard_civilstatus" );

        int numrows = almmCivilstatus.numrows();
        int numkeys = almmCivilstatus.numkeys();
        showMessage( "Number of rows in reference table: " + numrows, false, true );
        if( numrows != numkeys )
        { showMessage( "Number of keys in arraylist multimap: " + numkeys, false, true ); }

        standardSex( debug, source );
        standardCivilstatus( debug, source );

        showMessage("Updating reference table: ref_status_sex", false, true);
        almmCivilstatus.updateTable();

        almmSex.free();
        almmCivilstatus.free();

        elapsedShowMessage( funcname, timeStart, System.currentTimeMillis() );
        showMessage_nl();
    } // doStatusSex


    /**
     * @param source
     */
    public void standardSex( boolean debug, String source )
    {
        int count = 0;
        int count_empty = 0;
        int stepstate = count_step;

        try
        {
            String selectQuery = "SELECT id_person , sex FROM person_o WHERE id_source = " + source;

            ResultSet rs = dbconOriginal.runQueryWithResult( selectQuery );

            while( rs.next() )
            {
                count++;
                if( count == stepstate ) {
                    showMessage( count + "", true, true );
                    stepstate += count_step;
                }

                int id_person = rs.getInt( "id_person" );

                String sex = rs.getString( "sex" ) != null ? rs.getString( "sex" ).toLowerCase() : "";
                if( sex.startsWith( "other:" ) ) { sex = sex.substring( 6 ); }

                if( debug ) { showMessage( "sex: " + sex , false, true ); }

                if( sex != null && !sex.isEmpty() )                 // check presence of the gender
                {
                    if( almmSex.contains( sex ) )                   // check presence in original

                    {
                        String refSCode = almmSex.code(sex);
                        if( debug ) { showMessage( "refSCode: " + refSCode , false, true ); }

                        if( refSCode.equals( SC_X ) ) {
                            if( debug ) { showMessage( "Warning 31: id_person: " + id_person + ", sex: " + sex, false, true ); }

                            addToReportPerson( id_person, source, 31, sex );     // warning 31

                            String query = PersonC.updateQuery( "sex", sex, id_person );
                            dbconCleaned.runQuery( query );
                        }
                        else if( refSCode.equals( SC_N ) ) {
                            if( debug ) { showMessage( "Warning 33: id_person: " + id_person + ", sex: " + sex, false, true ); }

                            addToReportPerson( id_person, source, 33, sex );     // warning 33
                        }
                        else if( refSCode.equals( SC_U ) ) {
                            if( debug ) { showMessage( "Warning 35: id_person: " + id_person + ", sex: " + sex, false, true ); }

                            addToReportPerson( id_person, source, 35, sex );     // warning 35

                            String query = PersonC.updateQuery( "sex", almmSex.standard(sex), id_person );
                            dbconCleaned.runQuery( query );
                        }
                        else if( refSCode.equals( SC_Y ) ) {
                            if( debug ) { showMessage( "Standard sex: id_person: " + id_person + ", sex: " + sex, false, true ); }

                            String query = PersonC.updateQuery( "sex", almmSex.standard(sex), id_person );
                            dbconCleaned.runQuery( query );
                        }
                        else {     // Invalid standard code
                            if( debug ) { showMessage( "Warning 39: id_person: " + id_person + ", sex: " + sex, false, true ); }

                            addToReportPerson( id_person, source, 39, sex );     // warning 39
                        }
                    }
                    else // not present in original
                    {
                        if( debug ) { showMessage( "standardSex: not present in original", false, true ); }
                        if( debug ) { showMessage( "Warning 31: id_person: " + id_person + ", sex: " + sex, false, true ); }

                        addToReportPerson( id_person, source, 31, sex );         // warning 31
                        almmCivilstatus.add( sex );         // only almmCivilstatus is used for update

                        String query = PersonC.updateQuery( "sex", sex, id_person );
                        dbconCleaned.runQuery( query );
                    }
                }
                else { count_empty++; }
            }

            int count_new = almmCivilstatus.newcount();     // only almmCivilstatus is used for update
            String strNew = "";
            if( count_new == 0 ) { strNew = "no new genders"; }
            else if( count_new == 1 ) { strNew = "1 new gender"; }
            else { strNew = "" + count_new + " new genders"; }
            showMessage( count + " gender records, " + count_empty + " without gender, " + strNew, false, true );
        }
        catch( Exception ex ) {
            showMessage( "count: " + count + " Exception while cleaning Sex: " + ex.getMessage(), false, true );
            ex.printStackTrace( new PrintStream( System.out ) );
        }
    } // standardSex


    /**
     * @param source
     */
    public void standardCivilstatus( boolean debug, String source )
    {
        int count = 0;
        int count_empty = 0;
        int stepstate = count_step;

        int count_sex_new = almmCivilstatus.newcount();     // standardSex also writes to almmCivilstatus

        try
        {
            String selectQuery = "SELECT id_person , sex , civil_status FROM person_o WHERE id_source = " + source;

            ResultSet rs = dbconOriginal.runQueryWithResult( selectQuery );

            while( rs.next() )
            {
                count++;
                if( count == stepstate ) {
                    showMessage( count + "", true, true );
                    stepstate += count_step;
                }

                int id_person = rs.getInt( "id_person" );

                String sex = rs.getString( "sex" ) != null ? rs.getString( "sex" ).toLowerCase() : "";
                if( sex.startsWith( "other:" ) ) { sex = sex.substring( 6 ); }

                String civil_status = rs.getString( "civil_status" ) != null ? rs.getString( "civil_status" ).toLowerCase() : "";
                if( civil_status.startsWith( "other:" ) ) { civil_status = civil_status.substring( 6 ); }

                //showMessage( "id: " + id_person + ", sex: " + sex + ", civil: " + civil_status, false, true );

                if( civil_status != null && !civil_status.isEmpty() )       // check presence of civil status
                {
                    if( almmCivilstatus.contains(civil_status) )          // check presence in original

                    {
                        String refSCode = almmCivilstatus.code(civil_status);
                        //showMessage( "code: " + refSCode, false, true );

                        if( refSCode.equals( SC_X ) ) {
                            addToReportPerson( id_person, source, 61, civil_status );            // warning 61

                            String query = PersonC.updateQuery( "civil_status", civil_status, id_person );
                            dbconCleaned.runQuery( query );
                        }
                        else if( refSCode.equals( SC_N ) ) {
                            addToReportPerson( id_person, source, 63, civil_status );            // warning 63
                        }
                        else if( refSCode.equals( SC_U ) ) {
                            addToReportPerson( id_person, source, 65, civil_status );            // warning 65

                            String query = PersonC.updateQuery( "civil_status", almmCivilstatus.standard( civil_status ), id_person );
                            dbconCleaned.runQuery( query );

                            if( sex != null && !sex.isEmpty() ) {           // Extra check on sex
                                if( !sex.equalsIgnoreCase( this.almmCivilstatus.value( "standard_sex", civil_status) ) ) {
                                    if( sex != "u" ) {
                                        addToReportPerson(id_person, source, 68, civil_status);    // warning 68
                                    }
                                }
                            }
                            else            // Sex is empty
                            {
                                String sexQuery = PersonC.updateQuery( "sex", almmCivilstatus.value("standard_sex", civil_status), id_person );
                                dbconCleaned.runQuery( sexQuery );
                            }

                            String sexQuery = PersonC.updateQuery( "civil_status", almmCivilstatus.standard(civil_status), id_person );
                            dbconCleaned.runQuery( sexQuery );
                        }
                        else if( refSCode.equals( SC_Y ) ) {
                            String query = PersonC.updateQuery( "civil_status", almmCivilstatus.standard(civil_status), id_person );
                            dbconCleaned.runQuery( query );

                            if( sex == null || sex.isEmpty() )  {      // Sex is empty
                                String sexQuery = PersonC.updateQuery( "sex", almmCivilstatus.value("standard_sex", civil_status), id_person );
                                dbconCleaned.runQuery( sexQuery );
                            }

                            String sexQuery = PersonC.updateQuery( "civil_status", almmCivilstatus.standard(civil_status), id_person );
                            dbconCleaned.runQuery( sexQuery );
                        }
                        else {          // Invalid SC
                            addToReportPerson( id_person, source, 69, civil_status );            // warning 68
                        }
                    }
                    else {      // add to ref
                        if( debug ) { showMessage( "standardCivilstatus: not present in original", false, true ); }
                        if( debug ) { showMessage( "Warning 31: id_person: " + id_person + ", sex: " + sex, false, true ); }

                        addToReportPerson( id_person, source, 61, civil_status );                // warning 61

                        almmCivilstatus.add(civil_status);                                        // Add new civil_status

                        String query = PersonC.updateQuery( "civil_status", civil_status, id_person );  // Write to Person
                        dbconCleaned.runQuery( query );
                    }
                }
                else { count_empty++; }
            }

            int count_civil_new = almmCivilstatus.newcount() - count_sex_new;
            String strNew = "";
            if( count_civil_new == 0 ) { strNew = "no new civil statusses"; }
            else if( count_civil_new == 1 ) { strNew = "1 new civil status"; }
            else { strNew = "" + count_civil_new + " new civil statusses"; }
            showMessage( count + " civil status records, " + count_empty + " without civil status, " + strNew, false, true );
        }
        catch( Exception ex ) {
            showMessage( "count: " + count + " Exception while cleaning Civil Status: " + ex.getMessage(), false, true );
            ex.printStackTrace( new PrintStream( System.out ) );
        }
    } // standardCivilstatus


    /*---< Registration Type >------------------------------------------------*/

    /**
     * Registration Type
     * @param go
     * @throws Exception
     */
    private void doRegistrationType( boolean debug, boolean go, String source ) throws Exception
    {
        String funcname = "doRegistrationType";
        if( !go ) {
            if( showskip ) { showMessage( "Skipping " + funcname, false, true ); }
            return;
        }

        long timeStart = System.currentTimeMillis();
        showMessage( funcname + "...", false, true );

        standardRegistrationType( debug, source );

        elapsedShowMessage( funcname, timeStart, System.currentTimeMillis() );
        showMessage_nl();
    } // doRegistrationType


    /**
     *
     * @param debug
     * @param source
     */
    public void standardRegistrationType( boolean debug, String source )
    {
        int count = 0;
        int stepstate = count_step;

        try
        {
            String selectQuery = "SELECT id_registration, registration_maintype, registration_type FROM registration_o WHERE id_source = " + source;

            ResultSet rs = dbconOriginal.runQueryWithResult( selectQuery );

            while( rs.next() )      // process data from links_original
            {
                count++;
                if( count == stepstate ) {
                    showMessage( count + "", true, true );
                    stepstate += count_step;
                }

                int id_registration = rs.getInt( "id_registration" );
                int registration_maintype = rs.getInt( "registration_maintype" );
                String registration_type = rs.getString( "registration_type" ) != null ? rs.getString( "registration_type" ).toLowerCase() : "";

                String refQuery = "SELECT * FROM ref_registration WHERE main_type = '" + registration_maintype + "' AND original = '" + registration_type + "'";
                ResultSet ref = dbconRefRead.runQueryWithResult( refQuery );

                if( ref.next() )        // compare with reference
                {
                    String refSCode = ref.getString( "standard_code" ).toLowerCase();

                    if( refSCode.equals( SC_X ) ) {
                        if( debug ) { showMessage( "Warning 51: id_registration: " + id_registration + ", reg type: " + registration_type, false, true ); }

                        addToReportRegistration( id_registration, source, 51, registration_type );       // warning 51

                        String query = RegistrationC.updateQuery( "registration_type", registration_type, id_registration );
                        dbconCleaned.runQuery( query );
                    }
                    else if( refSCode.equals( SC_N ) ) {
                        if( debug ) { showMessage( "Warning 53: id_registration: " + id_registration + ", reg type: " + registration_type, false, true ); }

                        addToReportRegistration( id_registration, source, 53, registration_type );       // warning 53
                    }
                    else if( refSCode.equals( SC_U ) ) {
                        if( debug ) { showMessage( "Warning 55: id_registration: " + id_registration + ", reg type: " + registration_type, false, true ); }

                        addToReportRegistration( id_registration, source, 55, registration_type );       // warning 55

                        String query = RegistrationC.updateQuery( "registration_type", ref.getString( "standard" ).toLowerCase(), id_registration );
                        dbconCleaned.runQuery( query );
                    } else if( refSCode.equals( SC_Y ) ) {
                        if( debug ) { showMessage( "Standard reg type: id_person: " + id_registration + ", reg type: " + registration_type, false, true ); }

                        String query = RegistrationC.updateQuery( "registration_type", ref.getString( "standard" ).toLowerCase(), id_registration );
                        dbconCleaned.runQuery( query );
                    }
                    else {    // invalid SC
                        if( debug ) { showMessage( "Warning 59: id_registration: " + id_registration + ", reg type: " + registration_type, false, true ); }

                        addToReportRegistration( id_registration, source, 59, registration_type );       // warning 59
                    }
                }
                else
                {      // not in reference; add to reference with "x"
                    if( debug ) { showMessage( "Warning 51 (not in ref): id_registration: " + id_registration + ", reg type: " + registration_type, false, true ); }

                    addToReportRegistration( id_registration, source, 51, registration_type );           // warning 51

                    // add to links_general
                    dbconRefRead.runQuery( "INSERT INTO ref_registration( original, main_type, standard_code ) VALUES ('" + registration_type + "', '" + registration_maintype + "', 'x')" );

                    // update links_cleaned_.registration_c
                    String query = RegistrationC.updateQuery( "registration_type", registration_type.length() < 50 ? registration_type : registration_type.substring(0, 50), id_registration );
                    dbconCleaned.runQuery( query );
                }
            }
        } catch( Exception ex ) {
            showMessage( "count: " + count + ", Exception while cleaning Registration Type: " + ex.getMessage(), false, true );
            ex.printStackTrace( new PrintStream( System.out ) );
        }
    } // standardRegistrationType


    /*---< Occupation >-------------------------------------------------------*/

    /**
     * doOccupation
     * @param go
     * @throws Exception
     */
    private void doOccupation( boolean debug, boolean go, String source ) throws Exception
    {
        String funcname = "doOccupation";

        if( !go ) {
            if( showskip ) { showMessage( "Skipping " + funcname, false, true ); }
            return;
        }

        long funcstart = System.currentTimeMillis();
        showMessage( funcname + "...", false, true );

        long start = System.currentTimeMillis();
        String msg = "Loading reference table: ref_occupation";
        showMessage( msg + "...", false, true );
        almmOccupation = new TabletoArrayListMultimap( dbconRefRead, dbconRefWrite, "ref_occupation", "original", "standard" );
        elapsedShowMessage( msg, start, System.currentTimeMillis() );

        int numrows = almmOccupation.numrows();
        int numkeys = almmOccupation.numkeys();
        showMessage( "Number of rows in reference table: " + numrows, false, true );
        if( numrows != numkeys )
        { showMessage( "Number of keys in arraylist multimap: " + numkeys, false, true ); }

        showMessage( "Processing standardOccupation for source: " + source + "...", false, true );
        standardOccupation( debug, source );

        showMessage( "Updating reference table: ref_occupation", false, true );
        almmOccupation.updateTable();
        almmOccupation.free();

        elapsedShowMessage( funcname, funcstart, System.currentTimeMillis() );
        showMessage_nl();
    } // doOccupation


    /**
     * @param debug
     * @param source
     */
    public void standardOccupation( boolean debug, String source )
    {
        int count = 0;
        int count_empty = 0;
        int stepstate = count_step;

        String query = "SELECT id_person , occupation FROM person_o WHERE id_source = " + source;
        if( debug ) { showMessage( query, false, true ); }

        try
        {
            ResultSet rs = dbconOriginal.runQueryWithResult( query );           // Get occupation

            while( rs.next() )
            {
                count++;
                if( count == stepstate ) {
                    showMessage( count + "", true, true );
                    stepstate += count_step;
                }

                int id_person = rs.getInt( "id_person" );
                String occupation = rs.getString( "occupation") != null ? rs.getString( "occupation" ).toLowerCase() : "";
                if( occupation.isEmpty() ) {
                    count_empty += 1;
                }
                else {
                    if( debug ) { showMessage( "count: " + count + ", id_person: " + id_person + ", occupation: " + occupation, false, true ); }
                    standardOccupationRecord( debug, count, source, id_person, occupation );
                }
            }

            int count_new = almmOccupation.newcount();
            String strNew = "";
            if( count_new == 0 ) { strNew = "no new occupations"; }
            else if( count_new == 1 ) { strNew = "1 new occupation"; }
            else { strNew = "" + count_new + " new occupations"; }
            showMessage( count + " occupation records, " + count_empty + " without occupation, " + strNew, false, true );
        }
        catch( SQLException sex ) {
            showMessage( "count: " + count + " SQLException while cleaning Occupation: " + sex.getMessage(), false, true );
            sex.printStackTrace( new PrintStream( System.out ) );
        }
        catch( Exception jex ) {
            showMessage( "count: " + count + " Exception while cleaning Occupation: " + jex.getMessage(), false, true );
            jex.printStackTrace( new PrintStream( System.out ) );
        }

    } // standardOccupation


    /**
     * @param debug
     * @param id_person
     * @param occupation
     */
    public void standardOccupationRecord( boolean debug, int count, String source, int id_person, String occupation )
    {
        try
        {
            if( !occupation.isEmpty() )                 // check presence of the occupation
            {
                boolean exists = false;
                exists = almmOccupation.contains( occupation );
                if( exists )
                {
                    //showMessage( "old: " + occupation, false, true );
                    if( debug ) { showMessage("getStandardCodeByOriginal: " + occupation, false, true); }

                    String refSCode = almmOccupation.code(occupation);

                    if( debug ) { showMessage( "refSCode: " + refSCode, false, true ); }

                    if( refSCode.equals( SC_X ) )
                    {
                        if( debug ) { showMessage( "Warning 41 (via SC_X): id_person: " + id_person + ", occupation: " + occupation, false, true ); }
                        addToReportPerson( id_person, source, 41, occupation );      // warning 41

                        String query = PersonC.updateQuery( "occupation", occupation, id_person );
                        dbconCleaned.runQuery( query );
                    }
                    else if( refSCode.equals( SC_N ) )
                    {
                        if( debug ) { showMessage( "Warning 43: id_person: " + id_person + ", occupation: " + occupation, false, true ); }
                        addToReportPerson( id_person, source, 43, occupation );      // warning 43
                    }
                    else if( refSCode.equals(SC_U) )
                    {
                        if( debug ) { showMessage( "Warning 45: id_person: " + id_person + ", occupation: " + occupation, false, true ); }
                        addToReportPerson( id_person, source, 45, occupation );      // warning 45

                        String refOccupation = almmOccupation.standard( occupation );

                        String query = PersonC.updateQuery( "occupation", refOccupation, id_person );
                        dbconCleaned.runQuery( query );
                    }
                    else if( refSCode.equals( SC_Y ) )
                    {
                        String refOccupation = almmOccupation.standard( occupation );

                        if( debug ) { showMessage( "occupation: " + refOccupation, false, true ); }

                        String query = PersonC.updateQuery( "occupation", refOccupation, id_person );
                        dbconCleaned.runQuery( query );
                    }
                    else      // Invalid standard code
                    {
                        if( debug ) { showMessage( "Warning 49: id_person: " + id_person + ", occupation: " + occupation, false, true ); }
                        addToReportPerson( id_person, source, 49, occupation );      // warning 49
                    }
                }
                else        // not present, collect as new
                {
                    //showMessage( "new: " + occupation, false, true );
                    if( debug ) { showMessage( "Warning 41 (not in ref_): id_person: " + id_person + ", occupation: " + occupation, false, true ); }
                    addToReportPerson( id_person, source, 41, occupation );       // warning 41

                    almmOccupation.add( occupation );

                    String query = PersonC.updateQuery( "occupation", occupation, id_person );
                    dbconCleaned.runQuery( query );
                }
            }
        }
        catch( Exception ex3 ) {
            showMessage( "count: " + count + " Exception while cleaning Occupation: " + ex3.getMessage(), false, true);
            ex3.printStackTrace( new PrintStream( System.out ) );
        }
    } // standardOccupationRecord


    /*---< Age, Role, Dates >-------------------------------------------------*/

    /**
     * doAge()
     * @param debug
     * @param go
     * @throws Exception
     */
    private void doAge( boolean debug, boolean go, String source ) throws Exception
    {
        String funcname = "doAge";
        if( !go ) {
            if( showskip ) { showMessage( "Skipping " + funcname, false, true ); }
            return;
        }

        long timeStart = System.currentTimeMillis();

        showMessage( funcname + "...", false, true );

        almmLitAge = new TabletoArrayListMultimap( dbconRefRead, dbconRefWrite, "ref_age", "original", "standard_year" );
        int size = almmLitAge.numkeys();
        showMessage( "Reference table: ref_age [" + size + " records]", false, true );

        long timeSAL = System.currentTimeMillis();
        String msg = "Processing standardAgeLiteral for source: " + source ;
        showMessage( msg + "...", false, true );
        standardAgeLiteral( debug, source );
        elapsedShowMessage( msg, timeSAL, System.currentTimeMillis() );

        long timeSA = System.currentTimeMillis();
        msg = "Processing standardAge for source: " + source ;
        showMessage( msg + "...", false, true );
        standardAge( debug, source );
        elapsedShowMessage( msg, timeSA, System.currentTimeMillis() );

        almmLitAge.free();

        elapsedShowMessage( funcname, timeStart, System.currentTimeMillis() );
        showMessage_nl();
    } // doAge



        /**
     * @param source
     */
    public void standardAgeLiteral( boolean debug, String source )
    {
        int count = 0;
        int stepstate = count_step;

        try
        {
            String selectQuery = "SELECT id_person , age_literal, age_year , age_month , age_week , age_day FROM links_original.person_o WHERE id_source = " + source;
            if( debug ) { showMessage( selectQuery, false, true ); }

            ResultSet rs = dbconOriginal.runQueryWithResult( selectQuery );

            while( rs.next() )
            {
                count++;
                if( count == stepstate ) {
                    showMessage( count + "", true, true );
                    stepstate += count_step;
                }

                int id_person = rs.getInt( "id_person" );
                int age_year  = rs.getInt( "age_year" );
                int age_month = rs.getInt( "age_month" );
                int age_week  = rs.getInt( "age_week" );
                int age_day   = rs.getInt( "age_day" );

                String age_literal = rs.getString( "age_literal" );

                if( debug ) { showMessage( "id_person: " + id_person + ", age_literal: " + age_literal, false, true ); }

                boolean numeric = true;
                int lit_year = 0;

                if( age_literal == null ) {
                    continue;
                }
                else
                {
                    try {
                        lit_year = Integer.parseInt( age_literal );
                        numeric = true;
                        if( debug ) { showMessage( "lit_year: " + lit_year, false, true ); }
                    }
                    catch( NumberFormatException nfe ) {
                        numeric = false;        // not a single number
                        if( debug ) { showMessage( "not a single number", false, true ); }
                    }
                    catch( Exception lex ) {
                        numeric = false;        // not a single number
                        showMessage( "count: " + count + " Exception during standardAgeLiteral: " + lex.getMessage(), false, true );
                        lex.printStackTrace( new PrintStream( System.out ) );
                    }
                }

                if( numeric && lit_year > 0) {
                    String standard_year_str = almmLitAge.value( "standard_year", age_literal );
                    if( standard_year_str.isEmpty() ) { standard_year_str = "0"; }
                    int standard_year = Integer.parseInt( standard_year_str );

                    if( age_year > 0 && age_year != standard_year )
                    { addToReportPerson( id_person, source, 261, lit_year + " != " + standard_year ); }    // warning 261

                    String queryLiteral = PersonC.updateQuery( "age_literal", age_literal, id_person );
                    if( debug ) { showMessage( queryLiteral, false, true ); }
                    dbconCleaned.runQuery( queryLiteral );

                    String queryYear = PersonC.updateQuery( "age_year", age_literal, id_person );
                    if( debug ) { showMessage( queryYear, false, true ); }
                    dbconCleaned.runQuery( queryYear );

                    continue;       // done
                }

                boolean check4 = false;
                boolean exists = false;
                exists = almmLitAge.contains( age_literal );
                if( exists )
                {
                    String refSCode = almmLitAge.code(age_literal);
                    if( debug ) { showMessage( "refSCode: " + refSCode, false, true ); }

                    if( refSCode.equals( SC_X ) )
                    {
                        if( debug ) { showMessage( "Warning 251 (via SC_X): id_person: " + id_person + ", age_literal: " + age_literal, false, true ); }
                        addToReportPerson( id_person, source, 251, age_literal );      // warning 251

                        String query = PersonC.updateQuery( "age_literal", age_literal, id_person) ;
                        dbconCleaned.runQuery( query );
                    }
                    else if( refSCode.equals( SC_N ) )
                    {
                        if( debug ) { showMessage( "Warning 253: id_person: " + id_person + ", age_literal: " + age_literal, false, true ); }
                        addToReportPerson( id_person, source, 253, age_literal );      // warning 253
                    }
                    else if( refSCode.equals( SC_U ) )
                    {
                        check4 = true;      // below

                        if (debug) { showMessage("Warning 255: id_person: " + id_person + ", age_literal: " + age_literal, false, true); }
                        addToReportPerson(id_person, source, 255, age_literal);      // warning 255

                        String standard_year_str = almmLitAge.value( "standard_year", age_literal );

                        String query = PersonC.updateQuery("age_literal", standard_year_str, id_person);
                        dbconCleaned.runQuery(query);
                    }
                    else if( refSCode.equals( SC_Y ) )
                    {
                        check4 = true;      // below
                    }
                    else      // Invalid standard code
                    {
                        if( debug ) { showMessage("Warning 259: id_person: " + id_person + ", age_literal: " + age_literal, false, true); }
                        addToReportPerson( id_person, source, 259, age_literal );      // warning 259
                    }

                    if( check4 )
                    {
                        String standard_year_str  = almmLitAge.value( "standard_year", age_literal );
                        String standard_month_str = almmLitAge.value( "standard_month", age_literal );
                        String standard_week_str  = almmLitAge.value( "standard_week",  age_literal );
                        String standard_day_str   = almmLitAge.value( "standard_day",   age_literal );

                        int standard_year  = Integer.parseInt( standard_year_str );
                        int standard_month = Integer.parseInt( standard_month_str );
                        int standard_week  = Integer.parseInt( standard_week_str );
                        int standard_day   = Integer.parseInt( standard_day_str );

                        if( debug ) {
                            showMessage( "age_literal: " + age_literal + ", year: " + standard_year + ", month: "
                                + standard_month + ", week: " + standard_week + ", day: " + standard_day, false, true);
                        }

                        String query = PersonC.updateQuery( "age_literal", age_literal, id_person );
                        dbconCleaned.runQuery( query );

                        if( age_year > 0 && age_year != standard_year )
                        { addToReportPerson( id_person, source, 261, standard_year_str ); }     // warning 261

                        query = PersonC.updateQuery( "age_year", standard_year_str, id_person );
                        dbconCleaned.runQuery( query );

                        if( age_month > 0 && age_month != standard_month )
                        { addToReportPerson( id_person, source, 262, standard_month_str ); }  // warning 262

                        query = PersonC.updateQuery( "age_month", standard_month_str, id_person );
                        dbconCleaned.runQuery( query );

                        if( age_week > 0 && age_week != standard_week )
                        { addToReportPerson( id_person, source, 263, standard_week_str ); }     // warning 263

                        query = PersonC.updateQuery( "age_week", standard_week_str, id_person );
                        dbconCleaned.runQuery( query );

                        if( age_day > 0 && age_day != standard_day )
                        { addToReportPerson( id_person, source, 264, standard_day_str ); }        // warning 264

                        query = PersonC.updateQuery( "age_day", standard_day_str, id_person );
                        dbconCleaned.runQuery( query );
                    }
                }
                else        // not present in ref, collect as new
                {
                    if (debug) { showMessage( "Warning 251: id_person: " + id_person + ", age_literal: " + age_literal, false, true ); }
                    addToReportPerson( id_person, source, 251, age_literal );      // warning 251

                    almmLitAge.add( age_literal );

                    String query = PersonC.updateQuery( "age_literal", age_literal, id_person );
                    dbconCleaned.runQuery( query );
                }
            }
        }
        catch( Exception ex ) {
            showMessage( "count: " + count + " Exception during standardAgeLiteral: " + ex.getMessage(), false, true );
            ex.printStackTrace( new PrintStream( System.out ) );
        }
    } // standardAgeLiteral



    /**
     * @param source
     */
    public void standardAge( boolean debug, String source )
    {
        int count = 0;
        int stepstate = count_step;

        try
        {
            String selectQuery = "SELECT id_person , age_year , age_month , age_week , age_day FROM links_original.person_o WHERE id_source = " + source;
            if( debug ) { showMessage( selectQuery, false, true ); }

            ResultSet rs = dbconOriginal.runQueryWithResult( selectQuery );

            while( rs.next() )
            {
                count++;
                if( count == stepstate ) {
                    showMessage( count + "", true, true );
                    stepstate += count_step;
                }

                int id_person = rs.getInt( "id_person" );
                int age_year  = rs.getInt( "age_year" );
                int age_month = rs.getInt( "age_month" );
                int age_week  = rs.getInt( "age_week" );
                int age_day   = rs.getInt( "age_day" );

                boolean update = false;

                if( ( age_year >= 0 ) && ( age_year < 115 ) ) { update = true; }
                else { addToReportPerson( id_person, source, 244, age_year + "" ); }

                if( ( age_month >= 0 ) && ( age_month < 50 ) ) { update = true; }
                else { addToReportPerson( id_person, source, 243, age_month + "" ); }

                if( ( age_week >= 0 ) && ( age_week < 50 ) ) { update = true; }
                else { addToReportPerson( id_person, source, 242, age_week + "" ); }

                if( ( age_day >= 0 ) && ( age_day < 100 ) ) { update = true; }
                else { addToReportPerson( id_person, source, 241, age_day + "" ); }

                if( update )
                {
                    // Notice: death = 'a' means alive
                    if( age_day > 0 || age_week > 0 || age_month > 0 || age_year > 0 )
                    {
                        String updateQuery = "UPDATE links_cleaned.person_c SET death = 'a'"
                            + " WHERE id_person = " + id_person + " AND id_source = " + source;

                        if( debug ) { showMessage( updateQuery, false, true ); }
                        dbconCleaned.runQuery( updateQuery );
                    }
                }
            }
            //showMessage( count + " person records", false, true );
        }
        catch( Exception ex ) {
            showMessage( "count: " + count + " Exception during standardAge: " + ex.getMessage(), false, true );
            ex.printStackTrace( new PrintStream( System.out ) );
        }
    } // standardAge


    /**
     * Role
     * @param debug
     * @param go
     * @throws Exception
     */
    private void doRole( boolean debug, boolean go, String source ) throws Exception
    {
        String funcname = "doRole";
        if( !go ) {
            if( showskip ) { showMessage( "Skipping " + funcname, false, true ); }
            return;
        }

        showMessage( funcname + "...", false, true );

        long timeStart = System.currentTimeMillis();
        String msg = "Loading reference table: ref_role";
        showMessage( msg + "...", false, true );

        almmRole = new TabletoArrayListMultimap( dbconRefRead, dbconRefWrite, "ref_role", "original", "standard" );
        int size = almmRole.numkeys();
        showMessage( "Reference table: ref_role [" + size + " records]", false, true );

        showMessage( "Processing standardRole for source: " + source + "...", false, true );
        standardRole( debug, source );

        almmRole.updateTable();
        almmRole.free();

        showMessage( "Processing standardAlive for source: " + source + "...", false, true );
        standardAlive( debug, source );

        elapsedShowMessage( funcname, timeStart, System.currentTimeMillis() );
        showMessage_nl();
    } // doRole


    /**
     *
     * @param debug
     * @param source
     */
    private void standardRole( boolean debug, String source )
    {
        /*
        String query = "UPDATE links_original.person_o, links_cleaned.person_c, links_general.ref_role "
            + "SET links_cleaned.person_c.role = links_general.ref_role.role_nr "
            + "WHERE links_original.person_o.role = links_general.ref_role.original "
            + "AND links_original.person_o.id_person = links_cleaned.person_c.id_person "
            + "AND links_original.person_o.id_source = " + source;

        try {
            dbconCleaned.runQuery( query );
        }
        catch( Exception ex ) {
            showMessage( "Exception while running standardRole: " + ex.getMessage(), false, true );
            ex.printStackTrace( new PrintStream( System.out ) );
        }
        */

        int count = 0;
        int count_empty = 0;
        int count_noref = 0;
        int stepstate = count_step;

        try
        {
            String selectQuery = "SELECT id_person , role FROM links_original.person_o WHERE id_source = " + source;

            ResultSet rs = dbconOriginal.runQueryWithResult( selectQuery );

            while( rs.next() )
            {
                count++;
                if( count == stepstate ) {
                    showMessage( count + "", true, true );
                    stepstate += count_step;
                }

                int id_person = rs.getInt( "id_person" );
                String role = rs.getString( "role" ) != null ? rs.getString( "role" ).toLowerCase() : "";
                if( debug ) { System.out.println( "role: " + role  ); }

                if( role.isEmpty() ) { count_empty++; }
                else
                {
                    if( almmRole.contains( role ) )             // present in ref_role.original
                    {
                        String refSCode = almmRole.code( role );
                        if( debug ) { System.out.println( "refSCode: " + refSCode ); }

                        if( refSCode.equals( SC_Y ) ) {
                            if( debug ) { showMessage( "Standard Role: id_person: " + id_person + ", role: " + role, false, true ); }
                            String role_nr = almmRole.value( "role_nr", role );
                            if( debug ) { showMessage( "role_nr: " + role_nr, false, true ); }
                            String updateQuery = PersonC.updateQuery( "role", role_nr, id_person );
                            if( debug ) { showMessage( updateQuery, false, true ); }
                            dbconCleaned.runQuery( updateQuery );
                        }
                        else
                        {
                            if( debug ) { showMessage( "Warning 101: id_person: " + id_person + ", role: " + role, false, true ); }

                            addToReportPerson( id_person, source, 101, role );      // report warning 101
                            almmRole.add( role );                                   // add new role
                        }
                    }
                    else
                    {
                        count_noref++;
                        if( debug ) { showMessage( "Warning 101: id_person: " + id_person + ", role: " + role, false, true ); }

                        addToReportPerson( id_person, source, 101, role );      // report warning 101
                        almmRole.add( role );                                   // add new role
                    }
                }
            }
            showMessage( count + " person records, " + count_empty + " without a role, and " + count_noref + " without a standard role", false, true );
        }
        catch( Exception ex ) {
            showMessage( "count: " + count + " Exception while cleaning Role: " + ex.getMessage(), false, true );
            ex.printStackTrace( new PrintStream( System.out ) );
        }
    } // standardRole


    /**
     * @param source
     */
    public void standardAlive( boolean debug, String source )
    {
        int count = 0;
        int count_empty = 0;
        int stepstate = count_step;

        try
        {
            //String selectQuery = "SELECT id_person , role , death , occupation FROM links_cleaned.person_c WHERE id_source = " + source;
            String selectQuery = "SELECT id_registration , id_person , role , death , occupation FROM links_cleaned.person_c WHERE id_source = " + source;
            if( debug ) { showMessage( "standardAlive() " + selectQuery, false, true ); }

            ResultSet rs = dbconCleaned.runQueryWithResult( selectQuery );

            while( rs.next() )
            {
                count++;
                if( count == stepstate ) {
                    showMessage( count + "", true, true );
                    stepstate += count_step;
                }

                int id_person       = rs.getInt( "id_person" );
                int id_registration = rs.getInt( "id_registration" );
                int role            = rs.getInt( "role" );
                String death        = rs.getString( "death" );
                String occupation   = rs.getString( "occupation" );

                if( debug && id_registration == 668084 ) {
                    showMessage( "count: " + count + ", id_person: " + id_person + ", role: " + role + ", death: " + death + ", occupation: " + occupation, false, true ); }

                if( death != null ) { death = death.toLowerCase(); }
                if( death == null || death.isEmpty() ) { count_empty++; }

                if( role == 1 || role == 4 || role == 7 || role == 10 ) {
                    if( debug && id_registration == 668084 ) { showMessage( "role: " + role + ", death -> 'a'", false, true ); }

                    String updateQuery = PersonC.updateQuery( "death", "a", id_person );        // set death to a[live]
                    dbconCleaned.runQuery( updateQuery );
                }
                else
                {
                    if( occupation != null ) {
                        if( debug && id_registration == 668084 ) { showMessage( "occupation: " + occupation + ", death -> 'a'", false, true ); }

                        String updateQuery = PersonC.updateQuery( "death", "a", id_person );     // set death to a[live]
                        dbconCleaned.runQuery( updateQuery );
                    }
                    else
                    {
                        if( death == null ) {
                            if( debug && id_registration == 668084 ) { showMessage( "death: " + death + ", death -> 'n'", false, true ); }

                            String updateQuery = PersonC.updateQuery( "death", "n", id_person );     // set death to n[o]
                            dbconCleaned.runQuery( updateQuery );
                        }
                        else
                        { if( debug && id_registration == 668084 ) { showMessage( "death stays: " + death, false, true ); } }
                    }
                }
            }
            showMessage( count + " person records, " + count_empty + " without alive specification", false, true );
        }
        catch( Exception ex ) {
            showMessage( "count: " + count + " Exception while cleaning Alive: " + ex.getMessage(), false, true );
            ex.printStackTrace( new PrintStream( System.out ) );
        }
    } // standardAlive


    /**
     * doDates
     * @param go
     * @throws Exception
     */
    private void doDates( boolean debug, boolean go, String source ) throws Exception
    {
        String funcname = "doDates";
        if( !go ) {
            if( showskip ) { showMessage( "Skipping " + funcname, false, true ); }
            return;
        }

        long timeStart = System.currentTimeMillis();
        showMessage( funcname + "...", false, true );

        //doAge(  debug, go, source );        // required for dates, again separate call
        //doRole( debug, go, source );        // required for dates, again separate call

        long ts = System.currentTimeMillis();
        showMessage( "Processing standardRegistrationDate for source: " + source + "...", false, true );
        standardRegistrationDate( debug, source );

        String type = "birth";
        showMessage( "Processing standardDate for source: " + source + " for: " + type + "...", false, true );
        standardDate( debug, source, type );

        type = "mar";
        showMessage( "Processing standardDate for source: " + source + " for: " + type + "...", false, true );
        standardDate( debug, source, type );

        type = "death";
        showMessage( "Processing standardDate for source: " + source + " for: " + type + "...", false, true );
        standardDate( debug, source, type );
        elapsedShowMessage( "Processing standard dates", ts, System.currentTimeMillis() );

        // Fill empty dates with registration dates
        ts = System.currentTimeMillis();
        showMessage( "Flagging empty dates (-> Reg dates) for source: " + source + "...", false, true );
        flagBirthDate( debug );
        flagMarriageDate( debug );
        flagDeathDate( debug );
        elapsedShowMessage( "Flagging empty dates", ts, System.currentTimeMillis() );

        ts = System.currentTimeMillis();
        showMessage( "Processing minMaxValidDate for source: " + source + "...", false, true );
        minMaxValidDate( debug, source );
        elapsedShowMessage( "Processing minMaxValidDate", ts, System.currentTimeMillis() );

        ts = System.currentTimeMillis();
        showMessage( "Processing minMaxDateMain for source: " + source + "...", false, true );
        minMaxDateMain( debug, source );
        elapsedShowMessage( "Processing Processing minMaxDateMain", ts, System.currentTimeMillis() );

        elapsedShowMessage( funcname, timeStart, System.currentTimeMillis() );
        showMessage_nl();
    } // doDates


    /**
     * minMaxDateMain
     * @param debug
     * @param source
     * @throws Exception
     */
    public void minMaxDateMain( boolean debug, String source ) throws Exception
    {
        int count = 0;
        int stepstate = count_step;

        // Because this query acts on person_c and registration_c (instead of person_o and registration_o),
        // the cleaning options Age and Role must be run together with (i.e. before) Dates.
        String startQuery = ""
            + " SELECT "
            + " registration_c.id_registration ,"
            + " registration_c.id_source ,"
            + " registration_c.registration_date ,"
            + " registration_c.registration_maintype ,"
            + " person_c.id_person ,"
            + " person_c.role ,"
            + " person_c.age_year ,"
            + " person_c.age_month ,"
            + " person_c.age_week ,"
            + " person_c.age_day ,"
            + " person_c.birth_date ,"
            + " person_c.mar_date ,"
            + " person_c.death_date ,"
            + " person_c.birth_year ,"
            + " person_c.birth_date_valid ,"
            + " person_c.mar_date_valid ,"
            + " person_c.death_date_valid ,"
            + " person_c.death"
            + " FROM person_c , registration_c"
            + " WHERE person_c.id_registration = registration_c.id_registration"
            + " AND links_cleaned.person_c.id_source = " + source;

        if( debug ) {
            showMessage( "minMaxDateMain()", false, true );
            //showMessage( startQuery, false, true );
        }

        try
        {
            ResultSet rsPersons = dbconCleaned.runQueryWithResult( startQuery );            // Run person query

            // Count hits
            rsPersons.last();
            int total = rsPersons.getRow();
            rsPersons.beforeFirst();
            showMessage( "person_c records: " + total, true, true );
            showMessage( "person_c records: " + total, false, true );

            while( rsPersons.next() )
            {
                count++;

                if( count == stepstate ) {
                    showMessage( count + " of " + total, true, true );
                    stepstate += count_step;
                }

                // Get
                int    id_registration      = rsPersons.getInt(    "id_registration" );
                int    id_source            = rsPersons.getInt(    "id_source" );
                String registrationDate     = rsPersons.getString( "registration_date" );
                int    registrationMaintype = rsPersons.getInt(    "registration_maintype" );
                int    id_person            = rsPersons.getInt(    "id_person" );
                int    role                 = rsPersons.getInt(    "role" );
                int    age_year             = rsPersons.getInt(    "age_year" );
                int    age_month            = rsPersons.getInt(    "age_month" );
                int    age_week             = rsPersons.getInt(    "age_week" );
                int    age_day              = rsPersons.getInt(    "age_day" );
                String birth_date           = rsPersons.getString( "person_c.birth_date" );
                String mar_date             = rsPersons.getString( "person_c.mar_date" );
                String death_date           = rsPersons.getString( "person_c.death_date" );
                int    birth_year           = rsPersons.getInt(    "birth_year" );
                int birth_date_valid        = rsPersons.getInt(    "birth_date_valid" );
                int mar_date_valid          = rsPersons.getInt(    "mar_date_valid" );
                int death_date_valid        = rsPersons.getInt(    "death_date_valid" );
                String death                = rsPersons.getString( "person_c.death" );

                //if( role == 0 ) { showMessage( "minMaxDateMain() role = 0", false, true ); }

                if( id_person == 102266679 ) { debug = true; }
                else  { debug = false; }

                if( debug ) {
                    showMessage_nl();
                    showMessage( "id_registration: "      + id_registration,      false, true );
                    showMessage( "id_person: "            + id_person,            false, true );
                    showMessage( "id_source: "            + id_source,            false, true );
                    showMessage( "registrationDate: "     + registrationDate,     false, true );
                    showMessage( "registrationMaintype: " + registrationMaintype, false, true );
                    showMessage( "role: "                 + role,                 false, true );
                    showMessage( "age_year: "             + age_year,             false, true );
                    showMessage( "age_month: "            + age_month,            false, true );
                    showMessage( "age_week: "             + age_week,             false, true );
                    showMessage( "age_day: "              + age_day,              false, true );
                    showMessage( "birth_date: "           + birth_date,           false, true );
                    showMessage( "mar_date: "             + mar_date,             false, true );
                    showMessage( "death_date: "           + death_date,           false, true );
                    showMessage( "birth_year: "           + birth_year,           false, true );
                    showMessage( "birth_date_valid: "     + birth_date_valid,     false, true );
                    showMessage( "mar_date_valid: "       + mar_date_valid,       false, true );
                    showMessage( "death_date_valid: "     + death_date_valid,     false, true );
                    showMessage( "death: "                + death,                false, true );
                }

                MinMaxDateSet mmds = new MinMaxDateSet();

                mmds.setRegistrationId( id_registration );
                mmds.setSourceId( id_source );
                mmds.setRegistrationDate( registrationDate );
                mmds.setRegistrationMaintype( registrationMaintype );
                mmds.setPersonId( id_person );
                mmds.setPersonRole( role );
                mmds.setPersonAgeYear( age_year );
                mmds.setPersonAgeMonth( age_month );
                mmds.setPersonAgeWeek( age_week );
                mmds.setPersonAgeDay( age_day );
                mmds.setPersonBirthYear( birth_year );
                mmds.setDeathDate( death_date );
                mmds.setDeath( death );

                int mainrole;

                switch( registrationMaintype ) {
                    case 1:
                        mainrole = 1;
                        break;
                    case 2:
                        if( (role == 7) || (role == 8) || (role == 9) ) {
                            mainrole = 7;
                        } else {
                            mainrole = 4;
                        }
                        break;
                    case 3:
                        mainrole = 10;
                        break;
                    default:
                        continue;
                }

                if( debug ) { showMessage( "mainrole: " + mainrole, false, true ); }
                mmds.setRegistrationMainRole( mainrole );                // main role

                String type_date = "";

                if( mmds.getPersonRole() == 0 ) { showMessage( "minMaxDateMain() role = 0", false, true ); }

                if( birth_date_valid != 1 )                 // invalid birth date
                {
                    if( debug ) { showMessage( "invalid birth date", false, true ); }

                    mmds.setTypeDate( "birth_date" );
                    type_date = "birth";
                    mmds.setDate( birth_date );

                    DivideMinMaxDatumSet ddmdBirth = minMaxDate( debug, mmds );
                    ddmdBirth.nonnegative();        // is this necessary?

                    if( debug ) { showMessage( ddmdBirth.getMinMaxDate(), false, true ); }

                    // Min Max to table
                    String queryBirth = "UPDATE person_c"
                        + " SET "
                        + type_date + "_year_min"  + " = " + ddmdBirth.getMinYear() + " ,"
                        + type_date + "_month_min" + " = " + ddmdBirth.getMinMonth() + " ,"
                        + type_date + "_day_min"   + " = " + ddmdBirth.getMinDay() + " ,"
                        + type_date + "_year_max"  + " = " + ddmdBirth.getMaxYear() + " ,"
                        + type_date + "_month_max" + " = " + ddmdBirth.getMaxMonth() + " ,"
                        + type_date + "_day_max"   + " = " + ddmdBirth.getMaxDay()
                        + " WHERE person_c.id_person = "   + id_person;

                    dbconCleaned.runQuery( queryBirth );
                }
                else { if( debug ) { showMessage( "birth date is valid: " + birth_date, false, true ); } }

                if( mar_date_valid != 1 )                // invalid marriage date
                {
                    if( debug ) { showMessage( "invalid marriage date", false, true ); }

                    mmds.setTypeDate( "marriage_date" );
                    type_date = "mar";
                    mmds.setDate( mar_date );

                    DivideMinMaxDatumSet ddmdMarriage = minMaxDate( debug, mmds );
                    ddmdMarriage.nonnegative();        // is this necessary?

                    if( debug ) { showMessage( ddmdMarriage.getMinMaxDate(), false, true ); }

                    // Min Max to table
                    String queryMar = "UPDATE person_c"
                        + " SET "
                        + type_date + "_year_min"  + " = " + ddmdMarriage.getMinYear() + " ,"
                        + type_date + "_month_min" + " = " + ddmdMarriage.getMinMonth() + " ,"
                        + type_date + "_day_min"   + " = " + ddmdMarriage.getMinDay() + " ,"
                        + type_date + "_year_max"  + " = " + ddmdMarriage.getMaxYear() + " ,"
                        + type_date + "_month_max" + " = " + ddmdMarriage.getMaxMonth() + " ,"
                        + type_date + "_day_max"   + " = " + ddmdMarriage.getMaxDay()
                        + " WHERE person_c.id_person = "   + id_person;

                    dbconCleaned.runQuery( queryMar );
                }
                else { if( debug ) { showMessage( "mar date is valid: " + mar_date, false, true ); } }

                if( death_date_valid != 1 )                // invalid death date
                {
                    if( debug ) { showMessage( "invalid death date", false, true ); }

                    mmds.setTypeDate( "death_date" );
                    type_date = "death";
                    mmds.setDate( death_date );

                    DivideMinMaxDatumSet ddmdDeath = minMaxDate( debug, mmds );
                    ddmdDeath.nonnegative();        // is this necessary?

                    if( debug ) { showMessage( ddmdDeath.getMinMaxDate(), false, true ); }

                    // Min Max to table
                    String queryDeath = "UPDATE person_c"
                        + " SET "
                        + type_date + "_year_min"  + " = " + ddmdDeath.getMinYear()  + " ,"
                        + type_date + "_month_min" + " = " + ddmdDeath.getMinMonth() + " ,"
                        + type_date + "_day_min"   + " = " + ddmdDeath.getMinDay()   + " ,"
                        + type_date + "_year_max"  + " = " + ddmdDeath.getMaxYear()  + " ,"
                        + type_date + "_month_max" + " = " + ddmdDeath.getMaxMonth() + " ,"
                        + type_date + "_day_max"   + " = " + ddmdDeath.getMaxDay()
                        + " WHERE person_c.id_person = "   + id_person;

                    dbconCleaned.runQuery( queryDeath );
                }
                else { if( debug ) { showMessage( "death date is valid: " + death_date, false, true ); } }
            }
        }
        catch( Exception ex ) {
            showMessage( "count: " + count + " Exception in minMaxDateMain(): " + ex.getMessage(), false, true );
            ex.printStackTrace( new PrintStream( System.out ) );
        }
    } // minMaxDateMain


    /**
     * minMaxDate : called by minMaxDateMain for invalid dates
     *
     * @param inputInfo
     * @return
     * @throws Exception
     */
    private DivideMinMaxDatumSet minMaxDate( boolean debug, MinMaxDateSet inputInfo )
    throws Exception
    {
        if( inputInfo.getPersonRole() == 0 ) { showMessage( "minMaxDate() role = 0", false, true ); }

        if( debug ) { showMessage( "minMaxDate()", false, true ); }

        // registration date
        DateYearMonthDaySet inputregistrationYearMonthDday =
                LinksSpecific.divideCheckDate( inputInfo.getRegistrationDate() );

        // Check: age in years given?
        if( inputInfo.getPersonAgeYear() > 0 )                              // age given in years ?
        {
            if( debug ) { showMessage( "age given in years: " + inputInfo.getPersonAgeYear() , false, true ); }

            DivideMinMaxDatumSet returnSet = new DivideMinMaxDatumSet();    // Create new return set

            if( inputInfo.getPersonRole() == 10 )   // it is the deceased
            {
                // registration date
                DateYearMonthDaySet inputDeathDate = LinksSpecific.divideCheckDate( inputInfo.getDeathDate() );

                // check death date
                if( inputDeathDate.isValidDate() )
                {
                    // Day no and month no are similar to death date
                    returnSet.setMaxDay(   inputDeathDate.getDay() );
                    returnSet.setMaxMonth( inputDeathDate.getMonth() );
                    returnSet.setMinDay(   inputDeathDate.getDay() );
                    returnSet.setMinMonth( inputDeathDate.getMonth() );
                }
                else
                {
                    // Day no and month no are similar to regis date
                    returnSet.setMaxDay(   inputregistrationYearMonthDday.getDay() );
                    returnSet.setMaxMonth( inputregistrationYearMonthDday.getMonth() );
                    returnSet.setMinDay(   inputregistrationYearMonthDday.getDay() );
                    returnSet.setMinMonth( inputregistrationYearMonthDday.getMonth() );
                }
            }
            else        // it is not the deceased
            {
                // Day no en month no are similar to regis date
                returnSet.setMaxDay(   inputregistrationYearMonthDday.getDay() );
                returnSet.setMaxMonth( inputregistrationYearMonthDday.getMonth() );
                returnSet.setMinDay(   inputregistrationYearMonthDday.getDay() );
                returnSet.setMinMonth( inputregistrationYearMonthDday.getMonth() );
            }

            MinMaxYearSet mmj = minMaxCalculation(
                debug,
                inputInfo.getSourceId(),
                inputInfo.getPersonId(),
                inputInfo.getRegistrationId(),
                inputregistrationYearMonthDday.getYear(),
                inputInfo.getRegistrationMainType(),
                inputInfo.getTypeDate(),
                inputInfo.getPersonRole(),
                inputInfo.getDeath(),
                inputInfo.getPersonAgeYear() );

            returnSet.setMinYear( mmj.getMinYear() );
            returnSet.setMaxYear( mmj.getMaxYear() );

            return returnSet;
        } // age is given in years


        // Check: Is birth year given?
        if( inputInfo.getPersonBirthYear() > 0 )           // birth year given
        {
            if( debug ) { showMessage( "birth year given: " + inputInfo.getPersonBirthYear() , false, true ); }

            // age is = regis jaar - birth year
            int birth_year = inputInfo.getPersonBirthYear();
            int regis_year = inputregistrationYearMonthDday.getYear();
            int AgeInYears = regis_year - birth_year;

            // Create new set
            DivideMinMaxDatumSet returnSet = new DivideMinMaxDatumSet();

            // Day no and month similar to regis date
            returnSet.setMaxDay(   inputregistrationYearMonthDday.getDay() );
            returnSet.setMaxMonth( inputregistrationYearMonthDday.getMonth() );
            returnSet.setMinDay(   inputregistrationYearMonthDday.getDay() );
            returnSet.setMinMonth( inputregistrationYearMonthDday.getMonth() );

            MinMaxYearSet mmj = minMaxCalculation(
                debug,
                inputInfo.getSourceId(),
                inputInfo.getPersonId(),
                inputInfo.getRegistrationId(),
                regis_year,
                inputInfo.getRegistrationMainType(),
                inputInfo.getTypeDate(),
                inputInfo.getPersonRole(),
                inputInfo.getDeath(),
                AgeInYears );

            returnSet.setMinYear( mmj.getMinYear() );
            returnSet.setMaxYear( mmj.getMaxYear() );

            return returnSet;
        } // birth year given

        // Check: Is it the deceased himself?
        if( inputInfo.getPersonRole() != 10 )           // not the deceased
        {
            if( debug ) { showMessage( "not the deceased, role: " + inputInfo.getPersonRole() , false, true ); }

            // Days, month, weeks to years, round up
            int ageinYears = roundUpAge(
                inputInfo.getPersonAgeYear(),
                inputInfo.getPersonAgeMonth(),
                inputInfo.getPersonAgeWeek(),
                inputInfo.getPersonAgeDay() );

            DivideMinMaxDatumSet returnSet = new DivideMinMaxDatumSet();            // New return set

            // day and month is similar to act date
            returnSet.setMaxDay(   inputregistrationYearMonthDday.getDay() );
            returnSet.setMaxMonth( inputregistrationYearMonthDday.getMonth() );
            returnSet.setMinDay(   inputregistrationYearMonthDday.getDay() );
            returnSet.setMinMonth( inputregistrationYearMonthDday.getMonth() );

            MinMaxYearSet mmj = minMaxCalculation(
                debug,
                inputInfo.getSourceId(),
                inputInfo.getPersonId(),
                inputInfo.getRegistrationId(),
                inputregistrationYearMonthDday.getYear(),
                inputInfo.getRegistrationMainType(),
                inputInfo.getTypeDate(),
                inputInfo.getPersonRole(),
                inputInfo.getDeath(),
                ageinYears );

            returnSet.setMinYear( mmj.getMinYear() );
            returnSet.setMaxYear( mmj.getMaxYear() );

            return returnSet;
        } // not the deceased


        // Check: combination of month days and weeks?
        int areMonths = 0;
        int areWeeks  = 0;
        int areDays   = 0;

        if( inputInfo.getPersonAgeMonth() > 0 ) { areMonths = 1; }
        if( inputInfo.getPersonAgeWeek()  > 0 ) { areWeeks  = 1; }
        if( inputInfo.getPersonAgeDay()   > 0 ) { areDays   = 1; }

        // TODO: ADDED
        // If marriage date, return 0-0-0
        if( inputInfo.getTypeDate().equalsIgnoreCase( "marriage_date" ) && ( (areMonths + areWeeks + areDays) > 0) )
        {
            DivideMinMaxDatumSet returnSet = new DivideMinMaxDatumSet();

            returnSet.setMaxDay(   0 );
            returnSet.setMaxMonth( 0 );
            returnSet.setMaxYear(  0 );
            returnSet.setMinDay(   0 );
            returnSet.setMinMonth( 0 );
            returnSet.setMinYear(  0 );

            return returnSet;
        }

        // TODO: added
        DateYearMonthDaySet inputDeathDate = LinksSpecific.divideCheckDate( inputInfo.getDeathDate() );

        int useYear;
        int useMonth;
        int useDay;

        if( inputDeathDate.isValidDate() )
        {
            useYear  = inputDeathDate.getYear();
            useMonth = inputDeathDate.getMonth();
            useDay   = inputDeathDate.getDay();
        }
        else
        {
            useYear  = inputregistrationYearMonthDday.getYear();
            useMonth = inputregistrationYearMonthDday.getMonth();
            useDay   = inputregistrationYearMonthDday.getDay();
        }

        if( ( areMonths + areWeeks + areDays ) >= 2 )         // at least 2 given
        {
            // weeks and months to days
            int days = inputInfo.getPersonAgeMonth() * 30;
            days += inputInfo.getPersonAgeWeek() * 7;

            // Date calculation

            // new date -> date - (days - 1)

            int mindays = (days - 1) * -1;
            int maxdays = (days + 1) * -1;

            // Min date
            String minDate = addTimeToDate(
                useYear,
                useMonth,
                useDay,
                TimeType.DAY,
                mindays );

            // Max date
            String maxDate = addTimeToDate(
                useYear,
                useMonth,
                useDay,
                TimeType.DAY,
                maxdays );

            // New date return return values
            DateYearMonthDaySet computedMinDate = LinksSpecific.divideCheckDate( minDate );
            DateYearMonthDaySet computedMaxDate = LinksSpecific.divideCheckDate( maxDate );

            // Checken if max date not later than actdate
            DateYearMonthDaySet dymd = checkMaxDate(
                computedMaxDate.getYear(),
                computedMaxDate.getMonth(),
                computedMaxDate.getDay(),
                useYear,
                useMonth,
                useDay );

            // returnen
            DivideMinMaxDatumSet returnSet = new DivideMinMaxDatumSet();

            returnSet.setMaxDay(   dymd.getDay() );
            returnSet.setMaxMonth( dymd.getMonth() );
            returnSet.setMaxYear(  dymd.getYear() );
            returnSet.setMinDay(   computedMinDate.getDay() );
            returnSet.setMinMonth( computedMinDate.getMonth() );
            returnSet.setMinYear(  computedMinDate.getYear() );

            return returnSet;
        }

        else if( areMonths == 1 )       // age in months given
        {
            // convert months
            int days = inputInfo.getPersonAgeMonth() * 30;

            // compute date
            // new date -> date - (days - 1)
            days++;

            int mindagen = (days + 30) * -1;
            int maxdagen = (days - 30) * -1;

            // Min date
            String minDate = addTimeToDate(
                useYear,
                useMonth,
                useDay,
                TimeType.DAY,
                mindagen );

            // Max date
            String maxDate = addTimeToDate(
                useYear,
                useMonth,
                useDay,
                TimeType.DAY,
                maxdagen );

            // New date to return values
            DateYearMonthDaySet computedMinDate = LinksSpecific.divideCheckDate( minDate );
            DateYearMonthDaySet computedMaxDate = LinksSpecific.divideCheckDate( maxDate );

            // returnen
            DivideMinMaxDatumSet returnSet = new DivideMinMaxDatumSet();

            returnSet.setMaxDay(   computedMaxDate.getDay() );
            returnSet.setMaxMonth( computedMaxDate.getMonth() );
            returnSet.setMaxYear(  computedMaxDate.getYear() );
            returnSet.setMinDay(   computedMinDate.getDay() );
            returnSet.setMinMonth( computedMinDate.getMonth() );
            returnSet.setMinYear(  computedMinDate.getYear() );

            return returnSet;
        }

        else if( areWeeks == 1 )             // age in weeks given
        {
            // weeks and months to days
            int days = inputInfo.getPersonAgeWeek() * 7;

            // compute date

            // new date -> date - (days - 1)
            days++;

            int mindays = (days + 8) * -1;
            int maxdays = (days - 8) * -1;

            // Min date
            String minDate = addTimeToDate(
                useYear,
                useMonth,
                useDay,
                TimeType.DAY,
                mindays );

            // Max datum
            String maxDate = addTimeToDate(
                useYear,
                useMonth,
                useDay,
                TimeType.DAY,
                maxdays );

            // date to return values
            DateYearMonthDaySet computedMinDate = LinksSpecific.divideCheckDate( minDate );
            DateYearMonthDaySet computedMaxDate = LinksSpecific.divideCheckDate( maxDate );

            // return
            DivideMinMaxDatumSet returnSet = new DivideMinMaxDatumSet();

            returnSet.setMaxDay(   computedMaxDate.getDay() );
            returnSet.setMaxMonth( computedMaxDate.getMonth() );
            returnSet.setMaxYear(  computedMaxDate.getYear() );
            returnSet.setMinDay(   computedMinDate.getDay() );
            returnSet.setMinMonth( computedMinDate.getMonth() );
            returnSet.setMinYear(  computedMinDate.getYear() );

            return returnSet;
        }

        else if( areDays == 1 )             // age in days given
        {
            // weeks and months to days
            int days = inputInfo.getPersonAgeDay();

            // new date -> date - (days - 1)

            int mindays = (days + 4) * -1;
            //int maxdays = (days - 4) * -1;    //NO, set to registration date (below)

            // min date
            String minDate = addTimeToDate(
                useYear,
                useMonth,
                useDay,
                TimeType.DAY,
                mindays );

            String maxDate = inputInfo.getRegistrationDate();


            // New date to return value
            DateYearMonthDaySet computedMinDate = LinksSpecific.divideCheckDate( minDate );
            DateYearMonthDaySet computedMaxDate = LinksSpecific.divideCheckDate( maxDate );


            // Check if max date not later than registration date
            DateYearMonthDaySet dymd = checkMaxDate(
                computedMaxDate.getYear(),
                computedMaxDate.getMonth(),
                computedMaxDate.getDay(),
                useYear,
                useMonth,
                useDay );

            // return
            DivideMinMaxDatumSet returnSet = new DivideMinMaxDatumSet();

            returnSet.setMaxDay(   dymd.getDay() );
            returnSet.setMaxMonth( dymd.getMonth() );
            returnSet.setMaxYear(  dymd.getYear() );
            returnSet.setMinDay(   computedMinDate.getDay() );
            returnSet.setMinMonth( computedMinDate.getMonth() );
            returnSet.setMinYear(  computedMinDate.getYear() );

            return returnSet;
        }

        // No age given
        DivideMinMaxDatumSet returnSet = new DivideMinMaxDatumSet();

        // day and month similar to registration date
        returnSet.setMaxDay(   inputregistrationYearMonthDday.getDay() );
        returnSet.setMaxMonth( inputregistrationYearMonthDday.getMonth() );
        returnSet.setMinDay(   inputregistrationYearMonthDday.getDay() );
        returnSet.setMinMonth( inputregistrationYearMonthDday.getMonth() );

        MinMaxYearSet mmj = minMaxCalculation(
            debug,
            inputInfo.getSourceId(),
            inputInfo.getPersonId(),
            inputInfo.getRegistrationId(),
            inputregistrationYearMonthDday.getYear(),
            inputInfo.getRegistrationMainType(),
            inputInfo.getTypeDate(),
            inputInfo.getPersonRole(),
            inputInfo.getDeath(),
            0 );

        returnSet.setMinYear( mmj.getMinYear() );
        returnSet.setMaxYear( mmj.getMaxYear() );

        return returnSet;
    } // minMaxDate


    /**
     * @param id_person
     * @param id_registration
     * @param reg_year
     * @param main_type
     * @param date_type
     * @param role
     * @param age
     * @return
     * @throws Exception
     */
    private MinMaxYearSet minMaxCalculation
    (
        boolean debug,
        int     id_source,
        int     id_person,
        int     id_registration,
        int     reg_year,
        int     main_type,
        String  date_type,
        int     role,
        String  death,
        int     age
    )
    throws Exception
    {
        if( role == 0 ) { showMessage( "minMaxCalculation() role = 0", false, true ); }

        if( debug ) { showMessage( "minMaxCalculation()", false, true ); }

        String min_age_0 = "n";
        String max_age_0 = "n";
        String age_main_role = "nvt";

        String age_reported  = "";
        String function = "";
        int min_year  = 0;
        int max_year  = 0;
        int main_role = 0;

        if( age > 0 )
        {
            age_reported  = "y";
        }
        else
        {
            if( death == null || death.isEmpty() ) { death = "n"; }
            age_reported = death;

            MinMaxMainAgeSet mmmas = minMaxMainAge
            (
                debug,
                id_source,
                id_person,
                id_registration,
                main_type,
                date_type,
                role,
                age_reported,
                age_main_role
            );

            main_role = mmmas.getMainRole();
            age       = mmmas.getAgeYear();
            min_age_0 = mmmas.getMinAge0();
            max_age_0 = mmmas.getMaxAge0();

            function = mmmas.getFunction();
            min_year = mmmas.getMinYear();
            max_year = mmmas.getMaxYear();

            mmmas = null;

            if( debug ) { showMessage( "after minMaxMainAge() age: " + age + ", function: " + function +
                ", min_year: " + min_year + ", max_year: " + max_year + ", main_role: " + main_role, false, true ); }

            if( role == main_role ) { age_main_role = "nvt"; }
            else {
                if( age > 0 ) { age_main_role = "y"; }
                else { age_main_role = "n"; }
            }
        }

        String query = "SELECT function, min_year, max_year FROM ref_date_minmax"
            + " WHERE maintype = '"    + main_type + "'"
            + " AND role = '"          + role + "'"
            + " AND date_type = '"     + date_type + "'"
            + " AND age_reported = '"  + age_reported + "'"
            + " AND age_main_role = '" + age_main_role  + "'";

        if( debug ) { showMessage( query, false, true ); }

        ResultSet rs = dbconRefRead.runQueryWithResult( query );

        if( !rs.next() )
        {
            if( debug ) {
                showMessage( "Not found", false, true );
                showMessage( query, false, true );
            }

            addToReportPerson( id_person, "0", 105, "Null -> [rh:" + main_type + "][ad:" + date_type + "][rol:" + role + "][lg:" + age_reported + "][lh:" + age_main_role + "]" );

            MinMaxYearSet mmj = new MinMaxYearSet();

            return mmj;
        }

        rs.last();        // to last row

        function = rs.getString( "function" );
        min_year = rs.getInt( "min_year" );
        max_year = rs.getInt( "max_year" );

        if( debug ) { showMessage( "minMaxCalculation() age: " + age + ", function: " + function +
            ", min_year: " + min_year + ", max_year: " + max_year + ", main_role: " + main_role, false, true ); }

        int agecopy = age;
        if( min_age_0 == "y" ) { agecopy = 0; }
        int minimum_year = reg_year - agecopy + min_year;
        if( debug ) { showMessage( "minMaxCalculation() min_age_0: " + min_age_0 + ", agecopy: " + agecopy + ", min_year: " + min_year, false, true ); }

        agecopy = age;
        if( max_age_0 == "y" ) { agecopy = 0; }
        int maximum_year = reg_year - agecopy + max_year;
        if( debug ) { showMessage( "minMaxCalculation() max_age_0: " + max_age_0 + ", agecopy: " + agecopy + ", max_year: " + max_year, false, true ); }

        MinMaxYearSet mmj = new MinMaxYearSet();

        mmj.setMinYear( minimum_year );
        mmj.setMaxYear( maximum_year );

        // function "A" means: the contents of mmj is already OK
        // function "B" is not needed here; its role is being dealt with somewhere else [minMaxDate() ?]

        if( function.equals( "A" ) )                    // function A, nothing to do
        {
            ;
        }
        else if( function.equals( "C" ) )                    // function C, check by reg year
        {
            if( maximum_year > reg_year ) { mmj.setMaxYear( reg_year ); }
        }
        else if( function.equals( "D" ) )               // function D
        {
            if( minimum_year > (reg_year - 14) ) { mmj.setMinYear( reg_year - 14 ); }
            if( maximum_year > (reg_year - 14) ) { mmj.setMaxYear( reg_year - 14 ); }
        }
        else if( function.equals( "E" ) )               // If E, deceased
        {
            if( minimum_year > reg_year ) { mmj.setMinYear( reg_year ); }
            if( maximum_year > reg_year ) { mmj.setMaxYear( reg_year ); }
        }
        else if( function.equals( "F" ) )               // function F
        {
            if( minimum_year < reg_year ) { mmj.setMinYear( reg_year ); }
        }
        else if( function.equals( "G" ) )               // function F
        {
            if( minimum_year < reg_year ) { mmj.setMinYear( reg_year ); }
            if( maximum_year > (reg_year + 86) ) { mmj.setMaxYear( reg_year + 86 ); }
        }
        else if( function.equals( "H" ) )               // function H
        {
            if( maximum_year > (reg_year + 86) ) { mmj.setMaxYear( reg_year + 86 ); }
        }
        else
        {
            if( debug ) { showMessage( "minMaxCalculation() function = " + function, false, true ); }
            addToReportPerson( id_person, "0", 104, "Null -> [rh:" + main_type + "][ad:" + date_type + "][rol:" + role + "][lg:" + age_reported + "][lh:" + age_main_role + "]" );
        }

        return mmj;

    } // minMaxCalculation


    /**
     * @param debug
     * @param id_person
     * @param id_registration
     * @param main_type
     * @param date_type
     * @param role
     * @param age_reported
     * @param age_main_role
     * @return
     * @throws Exception
     */
    private MinMaxMainAgeSet minMaxMainAge
    (
        boolean debug,
        int     id_source,
        int     id_person,
        int     id_registration,
        int     main_type,
        String  date_type,
        int     role,
        String  age_reported,
        String  age_main_role
    )
    throws Exception
    {
        if( role == 0 ) { showMessage( "minMaxMainAge() role = 0", false, true ); }

        boolean done = false;

        MinMaxMainAgeSet mmmas = new MinMaxMainAgeSet();

        if( debug ) { showMessage( "minMaxMainAge() function: " + mmmas.getFunction(), false, true ); }

        mmmas.setMinAge0( "n" );
        mmmas.setMaxAge0( "n" );

        int loop = 0;
        while( !done )
        {
            if( debug ) { showMessage( "minMaxMainAge(): age_main_role = " + age_main_role + " [loop = " + loop + "]", false, true ); }

            String queryRef = "SELECT function, min_year, max_year, min_person, max_person FROM links_general.ref_date_minmax"
                + " WHERE maintype = '"    + main_type + "'"
                + " AND role = '"          + role + "'"
                + " AND date_type = '"     + date_type + "'"
                + " AND age_reported = '"  + age_reported + "'"
                + " AND age_main_role = '" + age_main_role + "'";

            if( debug ) { showMessage( queryRef, false, true ); }
            ResultSet rs_ref = dbconRefRead.runQueryWithResult( queryRef );

            int min_person_role = 0;
            int max_person_role = 0;

            if( !rs_ref.next() ) {
                if( debug ) { showMessage( "Not found; age_main_role = " + age_main_role, false, true ); }

                if( age_main_role.equals( "nvt" ) ) { age_main_role = "y"; }
                else { age_main_role = "n"; }
            }
            else
            {
                if( debug ) { showMessage( "Found", false, true ); }

                min_person_role = rs_ref.getInt( "min_person" );
                max_person_role = rs_ref.getInt( "max_person" );
                if( debug ) { showMessage( "min_person_role = " + min_person_role + ", max_person_role = " + max_person_role, false, true ); }

                mmmas.setFunction( rs_ref.getString( "function" ) );
                mmmas.setMinYear(rs_ref.getInt( "min_year" ));
                mmmas.setMaxYear(rs_ref.getInt( "max_year" ));

                boolean readPc = false;
                int mm_main_role = 0;

                if( min_person_role > 0 ) {
                    readPc = true;
                    mm_main_role = min_person_role;
                }
                else {
                    done = true;
                    mmmas.setMinAge0( "y" );
                }

                if( max_person_role > 0 ) {
                    readPc = true;
                    mm_main_role = max_person_role;
                }
                else {
                    done = true;
                    mmmas.setMaxAge0( "y" );
                }

                if( debug ) { showMessage( "querying person_c: " + readPc, false, true ); }
                if( readPc ) {
                    String queryPc = "SELECT age_day, age_week, age_month, age_year, role FROM links_cleaned.person_c"
                        + " WHERE id_registration = " + id_registration
                        + " AND role = " + mm_main_role;

                    if( debug ) { showMessage( queryPc, false, true ); }
                    ResultSet rs_pc = dbconCleaned.runQueryWithResult( queryPc );

                    int age_day = 0;
                    int age_week = 0;
                    int age_month = 0;
                    int age_year = 0;

                    int main_role = 0;
                    int countPc = 0;

                    while( rs_pc.next() ) {
                        countPc++;
                        age_day   = rs_pc.getInt( "age_day" );
                        age_week  = rs_pc.getInt( "age_week" );
                        age_month = rs_pc.getInt( "age_month" );
                        age_year  = rs_pc.getInt( "age_year" );
                        main_role = rs_pc.getInt( "role" );

                        mmmas.setAgeYear( age_year );
                        mmmas.setMainRole( main_role );
                    }

                    if( countPc == 0 ) {
                        //showMessage( "minMaxMainAge: zero person_c count, with query:", false, true );
                        //showMessage( queryPc, false, true );
                             if( mm_main_role ==  1 ) { addToReportRegistration( id_registration, "" + id_source, 271, "" ); }   // EC 271
                        else if( mm_main_role ==  4 ) { addToReportRegistration( id_registration, "" + id_source, 272, "" ); }   // EC 272
                        else if( mm_main_role ==  7 ) { addToReportRegistration( id_registration, "" + id_source, 273, "" ); }   // EC 273
                        else if( mm_main_role == 10 ) { addToReportRegistration( id_registration, "" + id_source, 274, "" ); }   // EC 274
                    }
                    else if( countPc > 1 ) {
                        showMessage( queryPc, false, true );
                        throw new Exception( "minMaxMainAge: person_c count = " + countPc );
                    }

                    if( debug ) { showMessage( "minMaxMainAge() age_day: " + age_day + ", age_week: " + age_week + ", age_month: " + age_month + ", age_year: " + age_year + ", main_role: " + main_role, false, true ); }

                    if( age_day > 0 || age_week > 0 || age_month > 0 ) {
                        int iyear = age_month / 12;
                        // Adding 1 is only for getting proper values of date range estimates (e.g., of parents of a baby that only lived a few days).
                        // the person_c.age_year should not be changed; it may be 0
                        age_year += ( 1 + iyear );
                        mmmas.setAgeYear( age_year );
                    }

                    if( age_year > 0 ) { done = true; }
                    else {
                        if( age_main_role.equals( "y" ) ) { age_main_role = "n"; }
                        else { done = true; }
                    }
                }
                else { done = true; }

            }

            if( done == false && loop > 2 ) {
                if( debug ) {
                    // no ref_date_minmax entry that satisfies the conditions
                    showMessage( "minMaxMainAge(): id_person = " + id_person + ", looping too much, quit (warning 106)", false, true );
                    showMessage( queryRef, false, true );
                }
                addToReportPerson( id_person, "0", 106, "" );
                done = true;
            }
            loop++;
        } // while

        return mmmas;

    } // minMaxMainAge


    /**
     * @param year
     * @param month
     * @param week
     * @param day
     * @return
     */
    public int roundUpAge( int year, int month, int week, int day )
    {
        int tempYear  = year;
        int tempMonth = month;
        int tempWeek  = week;

        // day to week
        if( day > 0 ) {
            tempWeek += (day / 7);

            if( (day % 7) != 0 ) { tempWeek++; }
        }
        week = tempWeek;

        // week to month
        if( week > 0 ) {
            tempMonth += (week / 4);

            if( (week % 4 ) != 0) { tempMonth++; }
        }

        month = tempMonth;

        // week to month
        if( month > 0 ) {
            tempYear += (month / 12);

            if( (month % 12 ) != 0) { tempYear++; }
        }

        return tempYear;
    } // roundUpAge


    /**
     * @param source
     */
    public void standardRegistrationDate( boolean debug, String source )
    {
        int count = 0;
        int stepstate = count_step;

        try
        {
            String startQuery = "SELECT id_registration , registration_date FROM registration_o WHERE id_source = " + source;

            ResultSet rs = dbconOriginal.runQueryWithResult( startQuery );

            while( rs.next() )
            {
                count++;
                if( count == stepstate ) {
                    showMessage( count + "", true, true );
                    stepstate += count_step;
                }

                // Get Opmerking
                int id_registration      = rs.getInt( "id_registration" );
                String registration_date = rs.getString( "registration_date" );

                if( registration_date == null ) {
                    addToReportRegistration( id_registration, source, 202, "" );   // EC 202

                    continue;
                }

                DateYearMonthDaySet dymd = LinksSpecific.divideCheckDate( registration_date );

                if( ! dymd.isValidDate() )
                {
                    if( debug ) { showMessage( "registration_date: " + registration_date, false, true ); }

                    addToReportRegistration( id_registration, source, 201, dymd.getReports() );    // EC 201

                    int day   = dymd.getDay();
                    int month = dymd.getMonth();
                    int year  = dymd.getYear();

                    if( month == 2 && day > 28 ) {
                        day = 1;
                        month = 3;
                        dymd.setDay( day );
                        dymd.setMonth( month );
                    }

                    if( day > 30 && ( month == 4 || month == 6 || month == 9 || month == 11 ) ) {
                        day = 1;
                        month += 1;
                        dymd.setDay( day );
                        dymd.setMonth( month );
                    }

                    if( day > 31 ) {
                        day = 1;
                        month += 1;
                        if( month > 12 ) {
                            month = 1;
                            year += 1;
                        }
                        dymd.setDay( day );
                        dymd.setMonth( month );
                        dymd.setYear( year );
                    }

                    if( month > 12 ) {
                        month = 12;
                        dymd.setMonth( month );
                    }

                    registration_date = String.format( "%02d-%02d-%04d", day, month, year );
                }

                String query = "UPDATE registration_c"
                    + " SET registration_c.registration_date = '" + registration_date + "' , "
                    + "registration_c.registration_day = "        + dymd.getDay() + " , "
                    + "registration_c.registration_month = "      + dymd.getMonth() + " , "
                    + "registration_c.registration_year = "       + dymd.getYear()
                    + " WHERE registration_c.id_registration = "  + id_registration;

                dbconCleaned.runQuery( query );

            }
            rs = null;
        }
        catch( Exception ex ) {
            showMessage( "count: " + count + " Exception while cleaning Registration date: " + ex.getMessage(), false, true );
            ex.printStackTrace( new PrintStream( System.out ) );
        }
    } // standardRegistrationDate


    /**
     * @param type      // "birth", "mar", or "death"
     */
    public void standardDate( boolean debug, String source, String type )
    {
        int count = 0;
        int count_empty = 0;
        int count_invalid = 0;
        int stepstate = count_step;

        try
        {
            String startQuery = "SELECT id_person , id_source , " + type + "_date FROM person_o WHERE " + type + "_date is not null";
            startQuery =  startQuery + " AND id_source = " + source;

            ResultSet rs = dbconOriginal.runQueryWithResult( startQuery );

            while( rs.next() )
            {
                // GUI info
                count++;
                if( count == stepstate ) {
                    showMessage( count + "", true, true );
                    stepstate += count_step;
                }

                int id_person = rs.getInt( "id_person" );
                int id_source = rs.getInt( "id_source" );
                String date   = rs.getString( type + "_date" );

                if( date.isEmpty() ) {
                    count_empty++;
                    continue;
                }

                DateYearMonthDaySet dymd = LinksSpecific.divideCheckDate( date );

                if( dymd.isValidDate() )
                {
                    String query = ""
                        + "UPDATE person_c "
                        + "SET person_c." + type + "_date = '" + dymd.getDay() + "-" + dymd.getMonth() + "-" + dymd.getYear() + "' , "
                        + "person_c." + type + "_day = " + dymd.getDay() + " , "
                        + "person_c." + type + "_month = " + dymd.getMonth() + " , "
                        + "person_c." + type + "_year = " + dymd.getYear() + " , "
                        + "person_c." + type + "_date_valid = 1 "
                        + "WHERE person_c.id_person = " + id_person;

                    dbconCleaned.runQuery( query );
                }
                else
                {
                    count_invalid++;

                    int errno = 0;
                    if(       type.equals( "birth" ) ) { errno = 211; }
                    else if ( type.equals( "mar" ) )   { errno = 221; }
                    else if ( type.equals( "death" ) ) { errno = 231; }

                    addToReportPerson( id_person, id_source + "", errno, dymd.getReports() );   // EC 211 / 221 / 231

                    String query = ""
                        + "UPDATE person_c "
                        + "SET person_c." + type + "_date = '" + date + "' , "
                        + "person_c." + type + "_day = " + dymd.getDay() + " , "
                        + "person_c." + type + "_month = " + dymd.getMonth() + " , "
                        + "person_c." + type + "_year = " + dymd.getYear() + " "
                        + "WHERE person_c.id_person = " + id_person;

                    dbconCleaned.runQuery( query );
                }
            }

            showMessage( "Number of " + type + " records: " + count + ", empty dates: " + count_empty + ", invalid dates: " + count_invalid, false, true );
            rs = null;
        } catch( Exception ex ) {
            showMessage( "count: " + count + " Exception while cleaning " + type + " date: " + ex.getMessage(), false, true );
            ex.printStackTrace( new PrintStream( System.out ) );
        }
    } // standardDate


    /**
     * @param debug
     * @param source
     * @throws Exception
     *
     * if the date is valid, set the min en max values of date, year, month, day equal to the given values
     * do this for birth, marriage and death
     */
    private void minMaxValidDate( boolean debug, String source ) throws Exception
    {
        String q1 = ""
            + "UPDATE person_c "
            + "SET "
            + "birth_date_min  = birth_date , "
            + "birth_date_max  = birth_date , "
            + "birth_year_min  = birth_year , "
            + "birth_year_max  = birth_year , "
            + "birth_month_min = birth_month , "
            + "birth_month_max = birth_month , "
            + "birth_day_min   = birth_day , "
            + "birth_day_max   = birth_day "
            + "WHERE "
            + "birth_date_valid = 1 "
            + "AND links_cleaned.person_c.id_source = " + source;

        String q2 = ""
            + "UPDATE person_c "
            + "SET "
            + "mar_date_min  = mar_date , "
            + "mar_date_max  = mar_date , "
            + "mar_year_min  = mar_year , "
            + "mar_year_max  = mar_year , "
            + "mar_month_min = mar_month , "
            + "mar_month_max = mar_month , "
            + "mar_day_min   = mar_day , "
            + "mar_day_max   = mar_day "
            + "WHERE "
            + "mar_date_valid = 1 "
            + "AND links_cleaned.person_c.id_source = " + source;

        String q3 = ""
            + "UPDATE person_c "
            + "SET "
            + "death_date_min  = death_date , "
            + "death_date_max  = death_date , "
            + "death_year_min  = death_year , "
            + "death_year_max  = death_year , "
            + "death_month_min = death_month , "
            + "death_month_max = death_month , "
            + "death_day_min   = death_day , "
            + "death_day_max   = death_day "
            + "WHERE "
            + "death_date_valid = 1 "
            + "AND links_cleaned.person_c.id_source = " + source;

        dbconCleaned.runQuery( q1 );
        dbconCleaned.runQuery( q2 );
        dbconCleaned.runQuery( q3 );
    } // minMaxValidDate


    /**
     * Use this function to add or subtract an amount of time from a date.
     *
     * @param year
     * @param month
     * @param day
     * @param tt
     * @param timeAmount
     * @return
     */
    private String addTimeToDate(
            int year,
            int month,
            int day,
            TimeType tt,
            int timeAmount)
    {
        Calendar c1 = Calendar.getInstance();       // new calendar instance

        c1.set( year, month, day );                 // set(int year, int month, int date)

        // Check of time type
        if( tt == tt.DAY ) {
            c1.add( Calendar.DAY_OF_MONTH, timeAmount );
        }
        else if( tt == tt.WEEK ) {
            c1.add( Calendar.WEEK_OF_MONTH, timeAmount );
        }
        else if( tt == tt.MONTH ) {
            c1.add( Calendar.MONTH, timeAmount );
        }
        else if( tt == tt.YEAR ) {
            c1.add( Calendar.YEAR, timeAmount );
        }

        // return new date
        String am = "" + c1.get( Calendar.DATE ) + "-" + c1.get( Calendar.MONTH ) + "-" + c1.get( Calendar.YEAR );

        return am;
    } // addTimeToDate


    /**
     * @param pYear
     * @param pMonth
     * @param pDay
     * @param rYear
     * @param rMonth
     * @param rDay
     * @return
     */
    private DateYearMonthDaySet checkMaxDate( int pYear, int pMonth, int pDay, int rYear, int rMonth, int rDay )
    {
        // year is greater than age year
        if (pYear > rYear) {

            //return registration date
            DateYearMonthDaySet dy = new DateYearMonthDaySet( "" );
            dy.setYear(rYear);
            dy.setMonth(rMonth);
            dy.setDay(rDay);
            return dy;

        } // lower, date is correct, return original date
        else if (pYear < rYear) {

            // return person date
            DateYearMonthDaySet dy = new DateYearMonthDaySet( "" );
            dy.setYear(pYear);
            dy.setMonth(pMonth);
            dy.setDay(pDay);
            return dy;

        }

        // years are equal, rest must be checked

        // month is higher than registration month
        if (pMonth > rMonth) {

            // return return act month
            DateYearMonthDaySet dy = new DateYearMonthDaySet( "" );
            dy.setYear(rYear);
            dy.setMonth(rMonth);
            dy.setDay(rDay);
            return dy;

        } // month is correct, return original month
        else if (pMonth < rMonth) {

            // return return persons date
            DateYearMonthDaySet dy = new DateYearMonthDaySet( "" );
            dy.setYear(pYear);
            dy.setMonth(pMonth);
            dy.setDay(pDay);
            return dy;
        }

        // months are equal, check rest

        // day is higher than registration day
        if (pDay > rDay) {

            // return act date
            DateYearMonthDaySet dy = new DateYearMonthDaySet( "" );
            dy.setYear(rYear);
            dy.setMonth(rMonth);
            dy.setDay(rDay);
            return dy;
        }

        // day is lower or similar to registration day
        DateYearMonthDaySet dy = new DateYearMonthDaySet( "" );
        dy.setYear(pYear);
        dy.setMonth(pMonth);
        dy.setDay(pDay);
        return dy;
    } // checkMaxDate


    /**
     *
     * @param debug
     */
    public void flagBirthDate( boolean debug )
    {
        String query1 = "UPDATE person_c, registration_c"
            + " SET"
            + " person_c.birth_date_flag  = 2,"
            + " person_c.birth_date       = registration_c.registration_date ,"
            + " person_c.birth_year       = registration_c.registration_year ,"
            + " person_c.birth_month      = registration_c.registration_month ,"
            + " person_c.birth_day        = registration_c.registration_day ,"
            + " person_c.birth_date_valid = 1"
            + " WHERE person_c.birth_date IS NULL"
            + " AND registration_c.registration_maintype = 1"
            + " AND person_c.role = 1"
            + " AND person_c.id_registration = registration_c.id_registration; ";

        String query2 = "UPDATE person_c, registration_c"
            + " SET"
            + " person_c.birth_date_flag  = 3,"
            + " person_c.birth_date       = registration_c.registration_date ,"
            + " person_c.birth_year       = registration_c.registration_year ,"
            + " person_c.birth_month      = registration_c.registration_month ,"
            + " person_c.birth_day        = registration_c.registration_day ,"
            + " person_c.birth_date_valid = 1"
            + " WHERE person_c.birth_date_valid = 0"
            + " AND person_c.birth_date_flag = 0"
            + " AND registration_c.registration_maintype = 1"
            + " AND person_c.role = 1"
            + " AND person_c.id_registration = registration_c.id_registration; ";

        String query3 = "UPDATE person_c, registration_c"
            + " SET person_c.birth_date_flag = 1"
            + " WHERE person_c.birth_date_valid = 1"
            + " AND person_c.birth_date_flag = 0"
            + " AND registration_c.registration_maintype = 1"
            + " AND person_c.role = 1"
            + " AND person_c.id_registration = registration_c.id_registration; ";

        try {
            dbconCleaned.runQuery( query1 );
            dbconCleaned.runQuery( query2 );
            dbconCleaned.runQuery( query3) ;
        }
        catch( Exception ex ) {
            showMessage( "Exception while flagging Birth date: " + ex.getMessage(), false, true );
            ex.printStackTrace( new PrintStream( System.out ) );
        }
    } // flagBirthDate


    /**
     *
     * @param debug
     */
    public void flagMarriageDate( boolean debug )
    {
        String query1 = "UPDATE person_c, registration_c"
            + " SET"
            + " person_c.mar_date_flag  = 2,"
            + " person_c.mar_date       = registration_c.registration_date ,"
            + " person_c.mar_year       = registration_c.registration_year ,"
            + " person_c.mar_month      = registration_c.registration_month ,"
            + " person_c.mar_day        = registration_c.registration_day ,"
            + " person_c.mar_date_valid = 1"
            + " WHERE person_c.mar_date IS NULL"
            + " AND registration_c.registration_maintype = 2"
            + " AND ( ( person_c.role = 4 ) || ( person_c.role = 7 ) )"
            + " AND person_c.id_registration = registration_c.id_registration;";

        String query2 = "UPDATE person_c, registration_c"
            + " SET"
            + " person_c.mar_date_flag  = 3,"
            + " person_c.mar_date       = registration_c.registration_date ,"
            + " person_c.mar_year       = registration_c.registration_year ,"
            + " person_c.mar_month      = registration_c.registration_month ,"
            + " person_c.mar_day        = registration_c.registration_day ,"
            + " person_c.mar_date_valid = 1"
            + " WHERE registration_c.registration_maintype = 2"
            + " AND person_c.mar_date_valid = 0"
            + " AND person_c.mar_date_flag = 0"
            + " AND ( ( person_c.role = 4 ) || ( person_c.role = 7 ) )"
            + " AND person_c.id_registration = registration_c.id_registration; ";

        String query3 = "UPDATE person_c, registration_c"
            + " SET person_c.mar_date_flag = 1"
            + " WHERE person_c.mar_date_valid = 1"
            + " AND person_c.mar_date_flag = 0"
            + " AND registration_c.registration_maintype = 2"
            + " AND ( ( person_c.role = 4 ) || ( person_c.role = 7 ) )"
            + " AND person_c.id_registration = registration_c.id_registration; ";

        try {
            dbconCleaned.runQuery( query1 );
            dbconCleaned.runQuery( query2 );
            dbconCleaned.runQuery( query3 );

        }
        catch( Exception ex ) {
            showMessage( "Exception while flagging Marriage date: " + ex.getMessage(), false, true );
            ex.printStackTrace( new PrintStream( System.out ) );
        }
    } // flagMarriageDate


    /**
     *
     * @param debug
     */
    public void flagDeathDate( boolean debug )
    {
        String query1 = "UPDATE person_c, registration_c"
            + " SET"
            + " person_c.death_date_flag  = 2 ,"
            + " person_c.death_date       = registration_c.registration_date ,"
            + " person_c.death_year       = registration_c.registration_year ,"
            + " person_c.death_month      = registration_c.registration_month ,"
            + " person_c.death_day        = registration_c.registration_day , "
            + " person_c.death_date_valid = 1"
            + " WHERE person_c.death_date IS NULL"
            + " AND registration_c.registration_maintype = 3"
            + " AND person_c.role = 10"
            + " AND person_c.id_registration = registration_c.id_registration; ";

        String query2 = "UPDATE person_c, registration_c "
            + " SET "
            + " person_c.death_date_flag = 3 ,"
            + " person_c.death_date      = registration_c.registration_date ,"
            + " person_c.death_year      = registration_c.registration_year ,"
            + " person_c.death_month     = registration_c.registration_month ,"
            + " person_c.death_day       = registration_c.registration_day ,"
            + " person_c.death_date_valid = 1"
            + " WHERE person_c.death_date_flag = 0"
            + " AND person_c.death_date_valid = 0"
            + " AND registration_c.registration_maintype = 3"
            + " AND person_c.role = 10"
            + " AND person_c.id_registration = registration_c.id_registration; ";

        String query3 = "UPDATE person_c, registration_c "
            + " SET "
            + " person_c.death_date_flag = 1 "
            + " WHERE person_c.death_date_valid = 1"
            + " AND person_c.death_date_flag = 0"
            + " AND registration_c.registration_maintype = 3"
            + " AND person_c.role = 10"
            + " AND person_c.id_registration = registration_c.id_registration; ";

        try {
            dbconCleaned.runQuery( query1 );
            dbconCleaned.runQuery( query2 );
            dbconCleaned.runQuery( query3 );

        }
        catch( Exception ex ) {
            showMessage( "Exception while flagging Death date: " + ex.getMessage(), false, true );
            ex.printStackTrace( new PrintStream( System.out ) );
        }
    } // flagDeathDate


    /*---< Min Max Marriage >-------------------------------------------------*/

    /**
     * Min Max Marriage
     * @param go
     * @throws Exception
     */
    private void doMinMaxMarriage( boolean debug, boolean go, String source ) throws Exception
    {
        String funcname = "doMinMaxMarriage";
        if( !go ) {
            if( showskip ) { showMessage( "Skipping " + funcname, false, true ); }
            return;
        }

        long timeStart = System.currentTimeMillis();
        showMessage( funcname + "...", false, true );

        almmMarriageYear = new TabletoArrayListMultimap( dbconRefRead, dbconRefWrite, "ref_minmax_marriageyear", "role_A", "role_B" );
        //almmMarriageYear.contentsOld();

        minMaxMarriageYear( debug, source );

        almmMarriageYear.free();

        elapsedShowMessage( funcname, timeStart, System.currentTimeMillis() );
        showMessage_nl();
    } // doMinMaxMarriage


     /**
     * @param debug
     * @param source
     * @throws Exception
     */
    private void minMaxMarriageYear( boolean debug, String source )
    throws Exception
    {
        int count = 0;
        int stepstate = count_step;

        String selectQueryA = ""
            + " SELECT "
            + " person_c.id_person ,"
            + " person_c.id_registration ,"
            + " person_c.registration_maintype ,"
            + " person_c.role ,"
            + " person_c.mar_day_min ,"
            + " person_c.mar_day_max ,"
            + " person_c.mar_month_min ,"
            + " person_c.mar_month_max ,"
            + " person_c.mar_year_min ,"
            + " person_c.mar_year_max"
            + " FROM person_c"
            + " WHERE id_source = " + source;

        if( debug ) { showMessage( "standardMinMaxMarriageYear() " + selectQueryA, false, true ); }

        try {
            ResultSet rsA = dbconCleaned.runQueryWithResult( selectQueryA );

            while (rsA.next()) {
                count++;
                if( count == stepstate ) {
                    showMessage( count + "", true, true );
                    stepstate += count_step;
                }

                int id_personA = rsA.getInt( "id_person" );
                int id_registration = rsA.getInt( "id_registration" );
                int roleA = rsA.getInt( "role" );

                int mar_day_minA   = rsA.getInt( "mar_day_min" );
                int mar_day_maxA   = rsA.getInt( "mar_day_max" );
                int mar_month_minA = rsA.getInt( "mar_month_min" );
                int mar_month_maxA = rsA.getInt( "mar_month_max" );
                int mar_year_minA  = rsA.getInt( "mar_year_min" );
                int mar_year_maxA  = rsA.getInt( "mar_year_max" );

                if( ( mar_year_minA == 0 && mar_month_minA == 0 && mar_day_minA == 0 ) ||
                    ( mar_year_maxA == 0 && mar_month_maxA == 0 && mar_day_maxA == 0 ) )
                { continue; }       // do nothing

                String minA = "";
                String maxA = "";
                if( debug ) {
                    minA = "[" + mar_year_minA + "." + mar_month_minA + "." + mar_day_minA + "]";
                    maxA = "[" + mar_year_maxA + "." + mar_month_maxA + "." + mar_day_maxA + "]";

                    showMessage( "id_person: " + id_personA, false, true );
                    showMessage( "roleA: " + roleA + " " + minA + "-" + maxA, false, true );
                }

                String roleB = almmMarriageYear.standard( Integer.toString( roleA ) );
                if( roleB.isEmpty() ) { continue; }     // role 1, 4 & 7 not in ref table (because not needed)

                String selectQueryB = ""
                    + " SELECT "
                    + " person_c.id_person ,"
                    + " person_c.registration_maintype ,"
                    + " person_c.role ,"
                    + " person_c.mar_day_min ,"
                    + " person_c.mar_day_max ,"
                    + " person_c.mar_month_min ,"
                    + " person_c.mar_month_max ,"
                    + " person_c.mar_year_min ,"
                    + " person_c.mar_year_max"
                    + " FROM person_c"
                    + " WHERE person_c.id_registration = " + id_registration
                    + " AND role = " + roleB;

                if( debug) { showMessage( selectQueryB, false, true ); }
                ResultSet rsB = dbconCleaned.runQueryWithResult( selectQueryB );

                int id_personB     = 0;
                int mar_day_minB   = 0;
                int mar_day_maxB   = 0;
                int mar_month_minB = 0;
                int mar_month_maxB = 0;
                int mar_year_minB  = 0;
                int mar_year_maxB  = 0;

                int countB = 0;
                while (rsB.next()) {
                    countB++;
                    id_personB     = rsB.getInt( "id_person" );
                    mar_day_minB   = rsB.getInt( "mar_day_min" );
                    mar_day_maxB   = rsB.getInt( "mar_day_max" );
                    mar_month_minB = rsB.getInt( "mar_month_min" );
                    mar_month_maxB = rsB.getInt( "mar_month_max" );
                    mar_year_minB  = rsB.getInt( "mar_year_min" );
                    mar_year_maxB  = rsB.getInt( "mar_year_max" );
                }

                if( countB == 0 ) {
                    if( debug ) { showMessage( "No match for roleB: " + roleB, false, true ); }
                    continue;
                }
                else if( countB != 1 ) {
                    showMessage( "standardMinMaxMarriageYear() id_person: " + id_personA + "has multiple roleB matches", false, true );
                    addToReportPerson( id_personA, source + "", 107, "" );
                }

                String minB = "";
                String maxB = "";
                if( debug ) {
                    minB = "[" + mar_year_minB + "." + mar_month_minB + "." + mar_day_minB + "]";
                    maxB = "[" + mar_year_maxB + "." + mar_month_maxB + "." + mar_day_maxB + "]";
                    showMessage( "roleB: " + roleB + " " + minB + "-" + maxB, false, true );
                }

                // choose greater of min dates
                if( ! datesEqual( mar_year_minA, mar_month_minA, mar_day_minA, mar_year_minB, mar_month_minB, + mar_day_minB ) )
                {
                    if( dateLeftIsGreater( mar_year_minA, mar_month_minA, mar_day_minA, mar_year_minB, mar_month_minB, + mar_day_minB ) ) {
                        if( debug ) { showMessage( "update id_personB: " + id_personB + " with minimum " + minA, false, true ); }

                        String updateQuery = ""
                            + "UPDATE links_cleaned.person_c SET"
                            + " mar_day_min = "   + mar_day_minA   + " , "
                            + " mar_month_min = " + mar_month_minA + " , "
                            + " mar_year_min = "  + mar_year_minA
                            + " WHERE id_person = " + "" + id_personB;

                        if( debug) { showMessage( updateQuery, false, true ); }
                        dbconCleaned.runQuery( updateQuery );
                    }
                    else {
                        if( debug ) { showMessage( "update id_personA: " + id_personA + " with minimum " + minB, false, true ); }

                        String updateQuery = ""
                            + "UPDATE links_cleaned.person_c SET"
                            + " mar_day_min = "   + mar_day_minB   + " , "
                            + " mar_month_min = " + mar_month_minB + " , "
                            + " mar_year_min = "  + mar_year_minB
                            + " WHERE id_person = " + "" + id_personA;

                        if( debug) { showMessage( updateQuery, false, true ); }
                        dbconCleaned.runQuery( updateQuery );
                    }
                }

                 // choose lesser of max dates
                if( ! datesEqual( mar_year_maxA, mar_month_maxA, mar_day_maxA, mar_year_maxB, mar_month_maxB, + mar_day_maxB ) )
                {
                    if( dateLeftIsGreater( mar_year_maxA, mar_month_maxA, mar_day_maxA, mar_year_maxB, mar_month_maxB, + mar_day_maxB ) ) {
                        if( debug ) { showMessage( "update id_personA: " + id_personA + " with maximum " + maxB, false, true ); }

                        String updateQuery = ""
                            + "UPDATE links_cleaned.person_c SET"
                            + " mar_day_max = "   + mar_day_maxB   + " , "
                            + " mar_month_max = " + mar_month_maxB + " , "
                            + " mar_year_max = "  + mar_year_maxB
                            + " WHERE id_person = " + "" + id_personA;

                        if( debug) { showMessage( updateQuery, false, true ); }
                        dbconCleaned.runQuery( updateQuery );
                    }
                    else {
                        if( debug ) { showMessage( "update id_personB: " + id_personB + " with maximun " + maxA, false, true ); }

                        String updateQuery = ""
                            + "UPDATE links_cleaned.person_c SET"
                            + " mar_day_max = "   + mar_day_maxA   + " , "
                            + " mar_month_max = " + mar_month_maxA + " , "
                            + " mar_year_max = "  + mar_year_maxA
                            + " WHERE id_person = " + "" + id_personB;

                        if( debug) { showMessage( updateQuery, false, true ); }
                        dbconCleaned.runQuery( updateQuery );
                    }
                }
            }
        }
        catch( Exception ex ) { showMessage( ex.getMessage(), false, true ); }

    } // minMaxMarriageYear


    /**
     * @param lYear
     * @param lMonth
     * @param lDay
     * @param rYear
     * @param rMonth
     * @param rDay
     * @return
     */
    private boolean dateLeftIsGreater( int lYear, int lMonth, int lDay, int rYear, int rMonth, int rDay )
    {
        if( lYear > rYear )      { return true; }   // lower, date is correct, return original date
        else if( lYear < rYear ) { return false; }  // return person date

        // years are equal, check months
        if( lMonth > rMonth ) { return true; }      // month is correct, return original month
        else if (lMonth < rMonth) { return false; }

        // months are equal, check days
        if( lDay > rDay ) { return true; }

        return false;
    } // dateLeftIsGreater


    /**
     * @param lYear
     * @param lMonth
     * @param lDay
     * @param rYear
     * @param rMonth
     * @param rDay
     * @return
     */
    private boolean datesEqual( int lYear, int lMonth, int lDay, int rYear, int rMonth, int rDay )
    {
        if( lYear == rYear && lMonth == rMonth && lDay == rDay ) { return true; }

        return false;
    } // datesEqual


    /*---< Parts to Full Date >-----------------------------------------------*/

    /**
     * Parts to Full Date
     * @param go
     * @throws Exception
     */
    private void doPartsToFullDate( boolean debug, boolean go, String source ) throws Exception
    {
        String funcname = "doPartsToFullDate";
        if( !go ) {
            if( showskip ) { showMessage( "Skipping " + funcname, false, true ); }
            return;
        }

        long timeStart = System.currentTimeMillis();
        showMessage( funcname + "...", false, true );

        showMessage( "processing partsToDate for source: " + source + "...", false, true );
        partsToDate( source );

        elapsedShowMessage( funcname, timeStart, System.currentTimeMillis() );
        showMessage_nl();
    } // doPartsToFullDate


    private void partsToDate( String source )
    {
        String query = "UPDATE links_cleaned.person_c SET "
                + "links_cleaned.person_c.birth_date_min  = CONCAT( links_cleaned.person_c.birth_day_min , '-' , links_cleaned.person_c.birth_month_min , '-' , links_cleaned.person_c.birth_year_min ) ,"
                + "links_cleaned.person_c.mar_date_min    = CONCAT( links_cleaned.person_c.mar_day_min ,   '-' , links_cleaned.person_c.mar_month_min ,   '-' , links_cleaned.person_c.mar_year_min ) ,"
                + "links_cleaned.person_c.death_date_min  = CONCAT( links_cleaned.person_c.death_day_min , '-' , links_cleaned.person_c.death_month_min , '-' , links_cleaned.person_c.death_year_min ) ,"
                + "links_cleaned.person_c.birth_date_max  = CONCAT( links_cleaned.person_c.birth_day_max , '-' , links_cleaned.person_c.birth_month_max , '-' , links_cleaned.person_c.birth_year_max ) ,"
                + "links_cleaned.person_c.mar_date_max    = CONCAT( links_cleaned.person_c.mar_day_max ,   '-' , links_cleaned.person_c.mar_month_max ,   '-' , links_cleaned.person_c.mar_year_max ) ,"
                + "links_cleaned.person_c.death_date_max  = CONCAT( links_cleaned.person_c.death_day_max , '-' , links_cleaned.person_c.death_month_max , '-' , links_cleaned.person_c.death_year_max ) "
                + "WHERE id_source = " + source;

        try {
            dbconCleaned.runQuery( query );
        }
        catch( Exception ex ) {
            showMessage( "Exception while Creating full dates from parts: " + ex.getMessage(), false, true );
            ex.printStackTrace( new PrintStream( System.out ) );
        }
    } // partsToDate


    /*---< Days Since Begin >-------------------------------------------------*/

    /**
     * Days since begin
     * @param go
     * @throws Exception
     */
    private void doDaysSinceBegin( boolean debug, boolean go, String source ) throws Exception
    {
        String funcname = "doDaysSinceBegin";
        if( !go ) {
            if( showskip ) { showMessage( "Skipping " + funcname, false, true ); }
            return;
        }

        long timeStart = System.currentTimeMillis();
        showMessage( funcname + "...", false, true );

        showMessage( "Processing daysSinceBegin for source: " + source + "...", false, true );
        daysSinceBegin( debug, source );

        elapsedShowMessage( funcname, timeStart, System.currentTimeMillis() );
        showMessage_nl();
    } // doDaysSinceBegin


    private void daysSinceBegin( boolean debug, String source )
    {
        String query1 = "UPDATE IGNORE person_c SET birth_min_days = DATEDIFF( date_format( str_to_date( birth_date_min, '%d-%m-%Y' ), '%Y-%m-%d' ) , '1-1-1' ) WHERE birth_date_min  NOT LIKE '0-%' AND birth_date_min   NOT LIKE '%-0-%'";
        String query2 = "UPDATE IGNORE person_c SET birth_max_days = DATEDIFF( date_format( str_to_date( birth_date_max, '%d-%m-%Y' ), '%Y-%m-%d' ) , '1-1-1' ) WHERE birth_date_max  NOT LIKE '0-%' AND birth_date_max   NOT LIKE '%-0-%'";
        String query3 = "UPDATE IGNORE person_c SET mar_min_days   = DATEDIFF( date_format( str_to_date( mar_date_min,   '%d-%m-%Y' ), '%Y-%m-%d' ) , '1-1-1' ) WHERE mar_date_min    NOT LIKE '0-%' AND mar_date_min     NOT LIKE '%-0-%'";
        String query4 = "UPDATE IGNORE person_c SET mar_max_days   = DATEDIFF( date_format( str_to_date( mar_date_max,   '%d-%m-%Y' ), '%Y-%m-%d' ) , '1-1-1' ) WHERE mar_date_max    NOT LIKE '0-%' AND mar_date_max     NOT LIKE '%-0-%'";
        String query5 = "UPDATE IGNORE person_c SET death_min_days = DATEDIFF( date_format( str_to_date( death_date_min, '%d-%m-%Y' ), '%Y-%m-%d' ) , '1-1-1' ) WHERE death_date_min  NOT LIKE '0-%' AND death_date_min   NOT LIKE '%-0-%'";
        String query6 = "UPDATE IGNORE person_c SET death_max_days = DATEDIFF( date_format( str_to_date( death_date_max, '%d-%m-%Y' ), '%Y-%m-%d' ) , '1-1-1' ) WHERE death_date_max  NOT LIKE '0-%' AND death_date_max   NOT LIKE '%-0-%'";

        query1 += "AND id_source = " + source;
        query2 += "AND id_source = " + source;
        query3 += "AND id_source = " + source;
        query4 += "AND id_source = " + source;
        query5 += "AND id_source = " + source;
        query6 += "AND id_source = " + source;

        String queryReg = "UPDATE registration_c SET "
            + "registration_days = DATEDIFF( date_format( str_to_date( registration_date, '%d-%m-%Y' ), '%Y-%m-%d' ) , '1-1-1' ) "
            + "WHERE registration_date  NOT LIKE '0-%' AND registration_date   NOT LIKE '%-0-%' "
            + "AND id_source = " + source;

        try
        {
            if( debug ) { showMessage( query1, false, true ); }
            else { showMessage( "1-of-7: birth_date_min", false, true ); }
            dbconCleaned.runQuery( query1 );

            if( debug ) { showMessage( query2, false, true ); }
            else { showMessage( "2-of-7: birth_date_max", false, true ); }
            dbconCleaned.runQuery( query2 );

            if( debug ) { showMessage( query3, false, true ); }
            else { showMessage( "3-of-7: mar_date_min", false, true ); }
            dbconCleaned.runQuery( query3 );

            if( debug ) { showMessage( query4, false, true ); }
            else { showMessage( "4-of-7: mar_date_max", false, true ); }
            dbconCleaned.runQuery( query4 );

            if( debug ) { showMessage( query5, false, true ); }
            else { showMessage( "5-of-7: death_date_min", false, true ); }
            dbconCleaned.runQuery( query5 );

            if( debug ) { showMessage( query6, false, true ); }
            else { showMessage( "6-of-7: death_date_max", false, true ); }
            dbconCleaned.runQuery( query6 );

            if( debug ) { showMessage( queryReg, false, true ); }
            else { showMessage( "7-of-7: registration_days", false, true ); }
            dbconCleaned.runQuery( queryReg );
        }
        catch( Exception ex ) {
            showMessage( "Exception while computing days since 1-1-1: " + ex.getMessage(), false, true );
            ex.printStackTrace( new PrintStream( System.out ) );
        }
    } // daysSinceBegin


    /*---< Post Tasks >-------------------------------------------------------*/

    /**
     * Post Tasks
     * @param go
     * @throws Exception
     */
    private void doPostTasks( boolean debug, boolean go, String source ) throws Exception
    {
        String funcname = "doPostTasks";
        if( !go ) {
            if( showskip ) { showMessage( "Skipping " + funcname, false, true ); }
            return;
        }

        long timeStart = System.currentTimeMillis();
        showMessage( funcname + "...", false, true );

        showMessage( "Processing postTasks for source: " + source + "...", false, true );
        postTasks( debug, source );

        elapsedShowMessage( funcname, timeStart, System.currentTimeMillis() );
        showMessage_nl();
    } // doPostTasks


    /**
     * @param source
     * @throws Exception
     */
    private void postTasks( boolean debug, String source ) throws Exception
    {
        // Notice:
        // UPDATE IGNORE means "ignore rows that break unique constraints, instead of failing the query".

        String[] queries =
        {
            "UPDATE links_cleaned.person_c SET sex = 'v' WHERE role = 2 AND id_source = " + source,
            "UPDATE links_cleaned.person_c SET sex = 'm' WHERE role = 3 AND id_source = " + source,
            "UPDATE links_cleaned.person_c SET sex = 'v' WHERE role = 4 AND id_source = " + source,
            "UPDATE links_cleaned.person_c SET sex = 'v' WHERE role = 5 AND id_source = " + source,
            "UPDATE links_cleaned.person_c SET sex = 'm' WHERE role = 6 AND id_source = " + source,
            "UPDATE links_cleaned.person_c SET sex = 'm' WHERE role = 7 AND id_source = " + source,
            "UPDATE links_cleaned.person_c SET sex = 'v' WHERE role = 8 AND id_source = " + source,
            "UPDATE links_cleaned.person_c SET sex = 'm' WHERE role = 9 AND id_source = " + source,

            "UPDATE links_cleaned.person_c SET sex = '' WHERE sex <> 'm' AND sex <> 'v' AND id_source = " + source,


            "CREATE TABLE links_match.male   ( id_registration INT NOT NULL , PRIMARY KEY (id_registration) );",

            "CREATE TABLE links_match.female ( id_registration INT NOT NULL , PRIMARY KEY (id_registration) );",

            "INSERT INTO links_match.male   (id_registration) SELECT id_registration FROM links_cleaned.person_c WHERE role = 10 AND sex = 'm';",

            "INSERT INTO links_match.female (id_registration) SELECT id_registration FROM links_cleaned.person_c WHERE role = 10 AND sex = 'v';",

            "UPDATE links_cleaned.person_c, links_match.male SET sex = 'v' WHERE links_match.male.id_registration = links_cleaned.person_c.id_registration AND role = 11 "
                    + " AND id_source = " + source,

            "UPDATE links_cleaned.person_c, links_match.female SET sex = 'm' WHERE links_match.female.id_registration = links_cleaned.person_c.id_registration AND role = 11 "
                    + " AND id_source = " + source,

            "DROP TABLE links_match.male;",

            "DROP TABLE links_match.female;"
        };

        // suppressed queries
        /*
            // superfluous: must have been done in doNames
            //"UPDATE links_cleaned.person_c SET firstname = LOWER(firstname),  familyname = LOWER(familyname) WHERE id_source = " + source,


            "UPDATE IGNORE links_cleaned.person_c "
                + "SET age_year = FLOOR( DATEDIFF( STR_TO_DATE( mar_date , '%d-%m-%Y' ) , STR_TO_DATE( birth_date , '%d-%m-%Y') ) / 365 ) "
                + "WHERE birth_date_valid = 1 "
                + "AND mar_date_valid = 1 "
                + "AND age_year is null "
                + "AND ( role = 7 OR role = 4 ) "
                + "AND mar_date   NOT LIKE '0-%' "
                + "AND mar_date   NOT LIKE '%-0-%' "
                + "AND birth_date NOT LIKE '0-%' "
                + "AND birth_date NOT LIKE '%-0-%' "
                + "AND id_source = " + source
        */

        // Execute queries
        for( String s : queries ) {
            dbconCleaned.runQuery( s );
        }
    } // postTasks


    /*---< Run PreMatch >-----------------------------------------------------*/

    /**
     * Run PreMatch
     * @param go
     * @throws Exception
     */
    private void doPrematch( boolean go, String source ) throws Exception
    {
        String funcname = "doPrematch";
        if( !go ) {
            if( showskip ) { showMessage( "Skipping " + funcname, false, true ); }
            return;
        }

        long timeStart = System.currentTimeMillis();
        showMessage( funcname + "...", false, true );

        showMessage( "Running PREMATCH...", false, false );
        mg.firePrematch();

        elapsedShowMessage( funcname, timeStart, System.currentTimeMillis() );
        showMessage_nl();
    } // doPrematch

     /*---< end Cleaning options >-------------------------------------------*/

}

// [eof]
