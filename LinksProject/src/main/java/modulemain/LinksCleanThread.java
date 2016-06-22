package modulemain;

import java.io.File;
import java.io.FileWriter;
import java.io.PrintStream;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;

import java.time.LocalDateTime;     // Java SE 8, based on Joda-Time

import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.LinkedList;
import java.util.Queue;
import java.util.Set;
import java.util.Vector;

import javax.swing.JTextArea;
import javax.swing.JTextField;

import com.google.common.base.Splitter;

import dataset.DateYearMonthDaySet;
import dataset.DivideMinMaxDatumSet;
import dataset.MinMaxMainAgeSet;
import dataset.MinMaxDateSet;
import dataset.MinMaxYearSet;
import dataset.Options;
import dataset.PersonC;
import dataset.RegistrationC;
import dataset.Remarks;
import dataset.TableToArrayListMultimap;
import dataset.ThreadManager;

import connectors.MySqlConnector;
import enumdefinitions.TableType;
import enumdefinitions.TimeType;
import general.Functions;
import general.PrintLogger;
import linksmanager.ManagerGui;


/**
 * @author Omar Azouguagh
 * @author Fons Laan
 *
 * <p/>
 * FL-30-Jun-2014 Imported from OA backup
 * FL-28-Jul-2014 Timing functions
 * FL-20-Aug-2014 Occupation added
 * FL-13-Oct-2014 Removed ttal code
 * FL-04-Feb-2015 dbconRefWrite instead of dbconRefRead for writing in standardRegistrationType
 * FL-01-Apr-2015 DivorceLocation
 * FL-08-Apr-2015 Remove duplicate registrations from links_cleaned
 * FL-27-Jul-2015 Bad registration dates in id_source = 10 (HSN)
 * FL-17-Sep-2015 Bad registration dates: db NULLs
 * FL-30-Oct-2015 minMaxCalculation() function C omission
 * FL-20-Nov-2015 registration_days bug with date strings containing leading zeros
 * FL-22-Jan-2016 registration_days bug with date strings containing leading zeros
 * FL-13-May-2016 split firstnames now in standardFirstnames()
 * FL-21-May-2016 Each thread its own ref table multimaps
 * FL-22-May-2016 Latest change
 *
 * TODO:
 * - check all occurrences of TODO
 * - in order to use TableToArrayListMultimap almmRegisType, we need to create a variant for almmRegisType
 *   that can store (and write) not only the the registration_maintype but also the registration_type.
 */

public class LinksCleanThread extends Thread
{
    boolean multithreaded = false;

    // Reference Table -> ArrayListMultiMap
    //private TableToArrayListMultimap almmPrepiece     = null;   // Names
    //private TableToArrayListMultimap almmSuffix       = null;   // Names
    //private TableToArrayListMultimap almmAlias        = null;   // Names
    //private TableToArrayListMultimap almmFirstname    = null;   // Names
    //private TableToArrayListMultimap almmFamilyname   = null;   // Names
    //private TableToArrayListMultimap almmLocation     = null;   // Location
    //private TableToArrayListMultimap almmRegisType   = null;  // Registration Type
    //private TableToArrayListMultimap almmOccupation   = null;   // Occupation
    private TableToArrayListMultimap almmReport       = null;   // Report warnings; read-only, so can remain file global
    //private TableToArrayListMultimap almmRole         = null;   // Role
    //private TableToArrayListMultimap almmCivilstatus  = null;   // Civilstatus & Gender
    //private TableToArrayListMultimap almmSex          = null;   // Civilstatus & Gender
    //private TableToArrayListMultimap almmMarriageYear = null;   // min/max marriage year
    //private TableToArrayListMultimap almmLitAge       = null;   // age_literal

    private JTextField outputLine;
    private JTextArea  outputArea;

    private boolean dbconref_single = true;     // true: same ref for reading and writing
    private MySqlConnector dbconRefWrite;       // [remote] reference db for writing new occurrences
    private MySqlConnector dbconRefRead;        // [local]  reference db for reading
    private MySqlConnector dbconLog;            // logging  of errors/warnings
    private MySqlConnector dbconOriginal;       // original data from A2A
    private MySqlConnector dbconCleaned;        // cleaned, from original

    private Runtime r = Runtime.getRuntime();
    private String logTableName;
    private Options opts;

    private final static String SC_U = "u"; // Unknown Standard value assigned (although the original value is not valid)
    private final static String SC_X = "x"; //    X    Standard yet to be assigned
    private final static String SC_N = "n"; //    No   standard value assigned (original value is not valid)
    private final static String SC_Y = "y"; //    Yes  Standard value assigned (original value is valid)

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

    private int count_step = 10000;             // used to be 1000

    /**
     * Constructor
     *
     * @param outputLine
     * @param outputArea
     * @param opts
     * @param mg
     */
    public LinksCleanThread
    (
        Options opts,
        JTextField outputLine,
        JTextArea outputArea,
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
        int max_threads_simul = opts.getMaxThreadsSimul();

        multithreaded = false;
        if( max_threads_simul > 1 ) { multithreaded = true; }

        String rmtype = "";


        // inner class for cleaning a single id_source
        class CleaningThread extends Thread
        {
            ThreadManager tm;
            String source;
            String rmtype;

            CleaningThread
            (
                ThreadManager tm,
                String source,
                String rmtype
            )
            {
                this.tm = tm;
                this.source = source;
                this.rmtype = rmtype;
            }

            public void run()
            {
                long threadStart = System.currentTimeMillis();

                long threadId = Thread.currentThread().getId();

                try
                {
                    //elapsedShowMessage( String.format( "Thread id %02d; Pre-loading all reference tables", threadId ), threadStart, System.currentTimeMillis() );

                    String msg = String.format( "Thread id %02d; CleaningThread/run(): running for source %s", threadId, source );
                    plog.show( msg ); showMessage( msg, false, true );

                    doRenewData( opts.isDbgRenewData(), opts.isDoRenewData(), source, rmtype );                     // GUI cb: Remove previous data

                    doPrepieceSuffix( opts.isDbgPrepieceSuffix(), opts.isDoPrepieceSuffix(), source, rmtype );      // GUI cb: Prepiece, Suffix

                    doFirstnames( opts.isDbgFirstnames(), opts.isDoFirstnames(), source, rmtype );                  // GUI cb: Firstnames

                    doFamilynames( opts.isDbgFamilynames(), opts.isDoFamilynames(), source, rmtype );               // GUI cb: Familynames

                    doLocations( opts.isDbgLocations(), opts.isDoLocations(), source, rmtype );                     // GUI cb: Locations

                    doStatusSex( opts.isDbgStatusSex(), opts.isDoStatusSex(), source, rmtype );                     // GUI cb: Status and Sex

                    doRegistrationType( opts.isDbgRegType(), opts.isDoRegType(), source, rmtype );                  // GUI cb: Registration Type

                    doOccupation( opts.isDbgOccupation(), opts.isDoOccupation(), source, rmtype );                  // GUI cb: Occupation

                    doAge( opts.isDbgAge(), opts.isDoAge(), source, rmtype );                                       // GUI cb: Age, Role,Dates

                    doRole( opts.isDbgRole(), opts.isDoRole(), source, rmtype );                                    // GUI cb: Age, Role, Dates

                    doDates( opts.isDbgDates(), opts.isDoDates(), source, rmtype );                                 // GUI cb: Age, Role, Dates

                    doMinMaxMarriage( opts.isDbgMinMaxMarriage(), opts.isDoMinMaxMarriage(), source, rmtype );      // GUI cb: Min Max Marriage

                    doPartsToFullDate( opts.isDbgPartsToFullDate(), opts.isDoPartsToFullDate(), source, rmtype );   // GUI cb: Parts to Full Date

                    doDaysSinceBegin( opts.isDbgDaysSinceBegin(), opts.isDoDaysSinceBegin(), source, rmtype );      // GUI cb: Days since begin

                    doPostTasks( opts.isDbgPostTasks(), opts.isDoPostTasks(), source, rmtype );                     // GUI cb: Post Tasks

                    doRemoveEmptyDateRegs( opts.isDbgRemoveEmptyDateRegs(), opts.isDoRemoveEmptyDateRegs(), source, rmtype );   // GUI cb: Remove Empty Role Reg's

                    doRemoveEmptyRoleRegs( opts.isDbgRemoveEmptyRoleRegs(), opts.isDoRemoveEmptyRoleRegs(), source, rmtype );   // GUI cb: Remove Empty Role Reg's

                    doRemoveDuplicateRegs( opts.isDbgRemoveDuplicateRegs(), opts.isDoRemoveDuplicateRegs(), source, rmtype );   // GUI cb: Remove Duplicate Reg's

                    doScanRemarks( opts.isDbgScanRemarks(), opts.isDoScanRemarks(), source, rmtype );                           // GUI cb: Scan Remarks
                }
                catch( Exception ex ) {
                    String msg = String.format( "Thread id %02d; Exception: %s", threadId, ex.getMessage() );
                    showMessage( msg, false, true );
                    ex.printStackTrace( new PrintStream( System.out ) );
                }

                String msg = String.format( "Thread id %02d; Cleaning source %s is done", threadId, source );
                showTimingMessage( msg, threadStart );
                System.out.println( msg );

                LocalDateTime timePoint = LocalDateTime.now();  // The current date and time
                msg = String.format( "Thread id %02d; current time: %s", threadId, timePoint.toString() );
                showMessage( msg, false, true );

                int count = tm.removeThread();
                msg = String.format( "Thread id %02d; Remaining cleaning threads: %d\n", threadId, count );
                showMessage( msg, false, true );
                System.out.println( msg );
            }
        } // CleaningThread inner class


        long mainThreadId = Thread.currentThread().getId();

        try
        {
            long cleanStart = System.currentTimeMillis();

            plog.show( String.format( "Thread id %02d; Main thread, LinksCleanThread/run()", mainThreadId ) );

            String msg = "";
            if( dbconref_single ) { msg = String.format( "Thread id %02d; Using the same reference db for reading and writing", mainThreadId ); }
            else { msg = String.format( "Thread id %02d; Reference db: reading locally, writing to remote db", mainThreadId ); }
            plog.show( msg );
            showMessage( msg, false, true );

            logTableName = LinksSpecific.getLogTableName();

            outputLine.setText( "" );
            outputArea.setText( "" );

            connectToDatabases();                                       // Create databases connectors
            createLogTable();                                           // Create log table with timestamp

            int[] sourceListAvail = getOrigSourceIds();                 // get source ids from links_original.registration_o
            sourceList = createSourceList( sourceIdsGui, sourceListAvail );

            String s = "";
            if( sourceList.length == 1 )
            {
                multithreaded = false;  // only 1 source
                s = String.format( "Thread id %02d; Processing source: ", mainThreadId );
            }
            else
            { s = String.format("Thread id %02d; Processing sources: ", mainThreadId ); }

            for( int i : sourceList ) { s = s + i + " "; }
            showMessage( s, false, true );

            // links_general.ref_report contains about 75 error definitions,
            // to be used when the normalization encounters errors
            showMessage( String.format( "Thread id %02d; Loading report table", mainThreadId ), false, true );
            almmReport = new TableToArrayListMultimap( dbconRefRead, null, "ref_report", "type", null );
            //almmReport.contentsOld();


            if( multithreaded )
            {
                ThreadManager tm = new ThreadManager( max_threads_simul );
                msg = String.format( "Thread id %02d; Multi-threaded cleaning with max %d simultaneous cleaning threads", mainThreadId, max_threads_simul );

                plog.show( msg ); showMessage( msg, false, true );

                long timeStart = System.currentTimeMillis();

                /*
                msg = String.format( "Thread id %02d; Pre-loading all reference tables...", mainThreadId );   // with multi-threaded they are not explicitly freed
                plog.show( msg ); showMessage( msg, false, true );

                almmPrepiece     = new TableToArrayListMultimap( dbconRefRead, dbconRefWrite, "ref_prepiece", "original", "prefix" );
                almmSuffix       = new TableToArrayListMultimap( dbconRefRead, dbconRefWrite, "ref_suffix",   "original", "standard" );
                almmAlias        = new TableToArrayListMultimap( dbconRefRead, dbconRefWrite, "ref_alias",    "original",  null );
                almmFirstname    = new TableToArrayListMultimap( dbconRefRead, dbconRefWrite, "ref_firstname", "original", "standard" );
                almmFamilyname   = new TableToArrayListMultimap( dbconRefRead, dbconRefWrite, "ref_familyname", "original", "standard" );
                almmLocation     = new TableToArrayListMultimap( dbconRefRead, dbconRefWrite, "ref_location", "original", "location_no" );
              //almmRegisType    = new TableToArrayListMultimap( dbconRefRead, dbconRefWrite, "ref_registration", "original", "standard" );
                almmOccupation   = new TableToArrayListMultimap( dbconRefRead, dbconRefWrite, "ref_occupation", "original", "standard" );
              // almmReport  : see above, also loaded for single-threaded
                almmRole         = new TableToArrayListMultimap( dbconRefRead, dbconRefWrite, "ref_role", "original", "standard" );
                almmCivilstatus  = new TableToArrayListMultimap( dbconRefRead, dbconRefWrite, "ref_status_sex", "original", "standard_civilstatus" );
                almmSex          = new TableToArrayListMultimap( dbconRefRead, dbconRefWrite, "ref_status_sex", "original", "standard_sex" );
                almmMarriageYear = new TableToArrayListMultimap( dbconRefRead, dbconRefWrite, "ref_minmax_marriageyear", "role_A", "role_B" );
                almmLitAge       = new TableToArrayListMultimap( dbconRefRead, dbconRefWrite, "ref_age", "original", "standard_year" );

                elapsedShowMessage( String.format( "Thread id %02d; Pre-loading all reference tables", mainThreadId ), timeStart, System.currentTimeMillis() );
                */

                ArrayList< CleaningThread > threads = new ArrayList();

                for ( int sourceId : sourceList )
                {
                    while( ! tm.allowNewThread() )  // Wait until our thread manager gives permission
                    {
                        plog.show( String.format( "Thread id %02d; No permission for new thread: Waiting 60 seconds", mainThreadId ) );
                        Thread.sleep( 60000 );
                    }
                    tm.addThread();        // Add a thread to the thread count

                    String source = Integer.toString( sourceId );
                    CleaningThread ct = new CleaningThread( tm, source, rmtype );
                    ct.start();
                    threads.add( ct );
                }

                // join the threads: main thread must wait for children to finish
                for( CleaningThread ct : threads ) { ct.join(); }

                msg = String.format( String.format( "Thread id %02d; Main thread; Cleaning Finished.", mainThreadId ) );
                elapsedShowMessage( msg, cleanStart, System.currentTimeMillis() );
                System.out.println( msg );

                LocalDateTime timePoint = LocalDateTime.now();  // The current date and time
                msg = String.format( "Thread id %02d; current time: %s", mainThreadId, timePoint.toString() );
                showMessage( msg, false, true );
            }

            else    // single-threaded cleaning for multiple sources
            {
                msg = String.format( "Thread id %02d; Single-threaded cleaning", mainThreadId );
                plog.show( msg ); showMessage( msg, false, true );

                for ( int sourceId : sourceList )
                {
                    long sourceStart = System.currentTimeMillis();

                    String source = Integer.toString( sourceId );

                    showMessage_nl();
                    showMessage( String.format( "Thread id %02d; Processing sourceId: %s", mainThreadId, source ), false, true );

                    doRenewData( opts.isDbgRenewData(), opts.isDoRenewData(), source, rmtype );                     // GUI cb: Remove previous data

                    doPrepieceSuffix( opts.isDbgPrepieceSuffix(), opts.isDoPrepieceSuffix(), source, rmtype );      // GUI cb: Prepiece, Suffix

                    doFirstnames( opts.isDbgFirstnames(), opts.isDoFirstnames(), source, rmtype );                  // GUI cb: Firstnames

                    doFamilynames( opts.isDbgFamilynames(), opts.isDoFamilynames(), source, rmtype );               // GUI cb: Familynames

                    doLocations( opts.isDbgLocations(), opts.isDoLocations(), source, rmtype );                     // GUI cb: Locations

                    doStatusSex( opts.isDbgStatusSex(), opts.isDoStatusSex(), source, rmtype );                     // GUI cb: Status and Sex

                    doRegistrationType( opts.isDbgRegType(), opts.isDoRegType(), source, rmtype );                  // GUI cb: Registration Type

                    doOccupation( opts.isDbgOccupation(), opts.isDoOccupation(), source, rmtype );                  // GUI cb: Occupation

                    doAge( opts.isDbgAge(), opts.isDoAge(), source, rmtype );                                       // GUI cb: Age

                    doRole( opts.isDbgRole(), opts.isDoRole(), source, rmtype );                                    // GUI cb: Role

                    doDates( opts.isDbgDates(), opts.isDoDates(), source, rmtype );                                 // GUI cb: Dates

                    doMinMaxMarriage( opts.isDbgMinMaxMarriage(), opts.isDoMinMaxMarriage(), source, rmtype );      // GUI cb: Min Max Marriage

                    doPartsToFullDate( opts.isDbgPartsToFullDate(), opts.isDoPartsToFullDate(), source, rmtype );   // GUI cb: Parts to Full Date

                    doDaysSinceBegin( opts.isDbgDaysSinceBegin(), opts.isDoDaysSinceBegin(), source, rmtype );      // GUI cb: Days since begin

                    doPostTasks( opts.isDbgPostTasks(), opts.isDoPostTasks(), source, rmtype );                     // GUI cb: Post Tasks

                    doRemoveEmptyDateRegs( opts.isDbgRemoveEmptyDateRegs(), opts.isDoRemoveEmptyDateRegs(), source, rmtype );   // GUI cb: Remove Empty Role Reg's

                    doRemoveEmptyRoleRegs( opts.isDbgRemoveEmptyRoleRegs(), opts.isDoRemoveEmptyRoleRegs(), source, rmtype );   // GUI cb: Remove Empty Role Reg's

                    doRemoveDuplicateRegs( opts.isDbgRemoveDuplicateRegs(), opts.isDoRemoveDuplicateRegs(), source, rmtype );   // GUI cb: Remove Duplicate Reg's

                    doScanRemarks( opts.isDbgScanRemarks(), opts.isDoScanRemarks(), source, rmtype );                           // GUI cb: Scan Remarks

                    msg = String.format( "Thread id %02d; Cleaning sourceId %d is done", mainThreadId, sourceId );
                    elapsedShowMessage( msg, sourceStart, System.currentTimeMillis() );
                    System.out.println( msg );

                    LocalDateTime timePoint = LocalDateTime.now();  // The current date and time
                    msg = String.format( "Thread id %02d; timestamp: %s", mainThreadId, timePoint.toString() );
                    showMessage( msg, false, true );
                }

                // Close db connections
                dbconRefWrite.close();
                if( !dbconref_single ) { dbconRefRead.close(); }
                dbconLog.close();
                dbconOriginal.close();
                dbconCleaned.close();

                //doPrematch( opts.isDoPrematch() );            // Prematch now has its own GUI tab

                msg = String.format( "Thread id %02d; Cleaning is done", mainThreadId );
                elapsedShowMessage( msg, cleanStart, System.currentTimeMillis() );
                System.out.println( msg );
            }
        }
        catch( Exception ex ) {
            String msg = String.format( "Thread id %02d; Exception: %s", mainThreadId, ex.getMessage() );
            showMessage( msg, false, true );
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
            if( count == 0 ) { showMessage( "Empty links_original ?", false , true ); }

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

        if( sourceIdsGui.isEmpty() )
        { idsInt = sourceListAvail; }           // use all Ids from links_original.registration_o
        else
        {
            String idsStr[] = sourceIdsGui.split( " " );

            if( idsStr.length == 0  )           // nothing from GUI
            { idsInt = sourceListAvail; }       // use all Ids from links_original.registration_o
            else                                // use GUI supplied Ids
            {
                idsInt = new int[ idsStr.length ];
                for( int i = 0; i < idsStr.length; i++ )
                { idsInt[ i ] = Integer.parseInt( idsStr[ i ] ); }
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

        long threadId = Thread.currentThread().getId();

        showMessage( String.format( "Thread id %02d; Connecting to databases:", threadId ), false, true );

        if( debug ) { showMessage( ref_db + " (ref)", false, true ); }
        dbconRefWrite = new MySqlConnector( ref_url, ref_db, ref_user, ref_pass );

        if( debug ) { showMessage( "links_general", false, true ); }
        if( dbconref_single ) { dbconRefRead = dbconRefWrite; } // same ref for reading and writing
        else {  dbconRefRead = new MySqlConnector( url, "links_general", user, pass ); }

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
        long threadId = Thread.currentThread().getId();

        showMessage( String.format( "Thread id %02d; Creating logging table: ", threadId ) + logTableName , false, true );

        String query = ""
                + " CREATE  TABLE `links_logs`.`" + logTableName + "` ("
                + " `id_log`       INT UNSIGNED NOT NULL AUTO_INCREMENT ,"
                + " `id_source`    INT UNSIGNED NULL ,"
                + " `archive`      VARCHAR(30)  NULL ,"
                + " `location`     VARCHAR(120) NULL ,"
                + " `reg_type`     VARCHAR(50)  NULL ,"
                + " `date`         VARCHAR(25)  NULL ,"
                + " `sequence`     VARCHAR(60)  NULL ,"
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
        String location  = "";
        String reg_type  = "";
        String date      = "";
        String sequence  = "";
        String guid      = "";

        String selectQuery = "SELECT registration_location , registration_type , registration_date , registration_seq , id_persist_registration"
            + " FROM registration_o WHERE id_registration = " + id;

        if( debug ) {
            System.out.println( selectQuery );
            showMessage( selectQuery, false, true );
        }

        try {
            ResultSet rs = dbconOriginal.runQueryWithResult( selectQuery );

            rs.first();
            location = rs.getString( "registration_location" );
            reg_type = rs.getString( "registration_type" );
            date     = rs.getString( "registration_date" );
            sequence = rs.getString( "registration_seq" );
            guid     = rs.getString( "id_persist_registration" );
        }
        catch( Exception ex ) {
            showMessage( ex.getMessage(), false, true );
            ex.printStackTrace( new PrintStream( System.out ) );
        }

        // save to links_logs
        /*
        String insertQuery = ""
            + " INSERT INTO links_logs.`" + logTableName + "`"
            + " ( reg_key , id_source , report_class , report_type , content , date_time ,"
            + " location , reg_type , date , sequence , guid )"
            + " VALUES ( "
                + id + " , "
                        + id_source + " , "
                + "'"   + cla.toUpperCase() + "' , "
                        + errorCode + " , "
                + "'"   + con + "' , NOW() ,"
                + " \"" + location + "\" ,"
                + " '"  + reg_type + "' , '"
                        + date     + "' , '"
                        + sequence + "' , '"
                        + guid     + "' ) ; ";
        */

        // not robust, for the time being it works
        String s = "";
        if( location != null && location.contains( "\"") )      // also in content; e.g. ss "Maasdam"
        {
            s = "INSERT INTO links_logs.`" + logTableName + "`"
                + " ( reg_key , id_source , report_class , report_type , content ,"
                + " date_time , location , reg_type , date , sequence , guid )"
                + " VALUES ( %d , \"%s\" , \"%s\" , \"%s\" , \'%s\' , NOW() , \'%s\' , \"%s\" , \"%s\" , \"%s\" , \"%s\" ) ;";
        }
        else
        {
            s = "INSERT INTO links_logs.`" + logTableName + "`"
                + " ( reg_key , id_source , report_class , report_type , content ,"
                + " date_time , location , reg_type , date , sequence , guid )"
                + " VALUES ( %d , \"%s\" , \"%s\" , \"%s\" , \"%s\" , NOW() , \"%s\" , \"%s\" , \"%s\" , \"%s\" , \"%s\" ) ;";
        }

        if( debug ) {
            System.out.println( s );
            showMessage( s, false, true );
        }

        String insertQuery = "";
        try {
            insertQuery = String.format ( s,
                id, id_source, cla.toUpperCase(), errorCode, con, location, reg_type, date, sequence, guid );
            if( debug ) {
                System.out.println( insertQuery );
                showMessage( insertQuery, false, true );
            }
        }
        catch( Exception ex )
        {
            System.out.println( "reg_key     : " + id );
            System.out.println( "id_source   : " + id_source );
            System.out.println( "report_class: " + cla.toUpperCase() );
            System.out.println( "report_type : " + errorCode );
            System.out.println( "content     : " + con );
            System.out.println( "date_time:  : " + "NOW()" );
            System.out.println( "location    : " + location );
            System.out.println( "reg_type    : " + reg_type );
            System.out.println( "date        : " + date );
            System.out.println( "sequence    : " + sequence );
            System.out.println( "guid        : " + guid );

            showMessage( s, false, true );
            showMessage( insertQuery, false, true );
            showMessage( ex.getMessage(), false, true );
            ex.printStackTrace();
        }

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
        String location  = "";
        String reg_type  = "";
        String date      = "";
        String sequence  = "";
        String guid      = "";

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
                showMessage( ex.getMessage(), false, true );
                ex.printStackTrace( new PrintStream( System.out ) );
            }
        }

        // to prevent: Data truncation: Data too long for column 'sequence'
        if( sequence != null && sequence.length() > 20 )
        { sequence = sequence.substring( 0, 20 ); }

        // save to links_logs
        String insertQuery = ""
            + " INSERT INTO links_logs.`" + logTableName + "`"
            + " ( pers_key , id_source , report_class , report_type , content , date_time ,"
            + " location , reg_type , date , sequence , reg_key , guid )"
            + " VALUES ( "
                + id + " , " + id_source + " , '" + cla.toUpperCase() + "' , " + errorCode + " , '" + con + "' , NOW() ,"
                + " \"" + location + "\" ,"
                + " '"  + reg_type + "' , '" + date + "' ,"
                + " \"" + sequence + "\" ,"
                + " '"  + id_registration + "' , '" + guid + "' ) ; ";

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
    private void doRenewData( boolean debug, boolean go, String source, String rmtype )
    throws Exception
    {
        long threadId = Thread.currentThread().getId();

        String funcname = String.format( "Thread id %02d; doRenewData for source %s", threadId, source );

        if( !go ) {
            if( showskip ) { showMessage( "Skipping " + funcname, false, true ); }
            return;
        }

        long timeStart = System.currentTimeMillis();
        showMessage( funcname + "...", false, true );


        // Delete cleaned data for given source
        String deleteRegist = String.format( "DELETE FROM registration_c WHERE id_source = %s", source );
        String deletePerson = String.format( "DELETE FROM person_c WHERE id_source = %s", source );

        String msg = "";
        if( rmtype.isEmpty() )
        { msg = String.format( "Thread id %02d; Deleting previous cleaned data for source: %s", threadId, source ); }
        else {
            deleteRegist += String.format( " AND registration_maintype = %s", rmtype );
            deletePerson += String.format( " AND registration_maintype = %s", rmtype );

            msg = String.format( "Thread id %02d; Deleting previous data for source: %s and rmtype: %d", threadId, source, rmtype );
        }

        showMessage( msg, false, true );
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

        if( registCCount == 0 && personCCount == 0 )
        {
            msg = String.format( "Thread id %02d; Resetting AUTO_INCREMENTs for links_cleaned", threadId );
            showMessage( msg, false, true );
            String auincRegist = "ALTER TABLE registration_c AUTO_INCREMENT = 1";
            String auincPerson = "ALTER TABLE person_c AUTO_INCREMENT = 1";
            dbconCleaned.runQuery( auincRegist );
            dbconCleaned.runQuery( auincPerson );
        }


        // Copy key column data from links_original to links_cleaned
        String keysRegistration = ""
            + "INSERT INTO links_cleaned.registration_c"
            + " ( id_registration, id_source, id_persist_registration, id_orig_registration, registration_maintype, registration_seq )"
            + " SELECT id_registration, id_source, id_persist_registration, id_orig_registration, registration_maintype, registration_seq"
            + " FROM links_original.registration_o"
            + " WHERE registration_o.id_source = " + source;

        if( ! rmtype.isEmpty() )
        { keysRegistration += String.format( " AND registration_maintype = %s", rmtype ); }

        msg = String.format( "Thread id %02d; Copying links_original registration keys to links_cleaned", threadId );
        showMessage( msg, false, true );
        if( debug ) { showMessage( keysRegistration, false, true ); }
        dbconCleaned.runQuery( keysRegistration );

        String keysPerson = ""
            + "INSERT INTO links_cleaned.person_c"
            + " ( id_person, id_registration, id_source, registration_maintype, id_person_o )"
            + " SELECT id_person, id_registration, id_source, registration_maintype, id_person_o"
            + " FROM links_original.person_o"
            + " WHERE person_o.id_source = " + source;

        if( ! rmtype.isEmpty() )
        { keysPerson += String.format( " AND registration_maintype = %s", rmtype ); }

        msg = String.format( "Thread id %02d; Copying links_original person keys to links_cleaned", threadId );
        showMessage( msg, false, true );
        if( debug ) { showMessage( keysPerson, false, true ); }
        dbconCleaned.runQuery( keysPerson );

        elapsedShowMessage( funcname, timeStart, System.currentTimeMillis() );
        showMessage_nl();
    } // doRenewData


    /*---< First- and Familynames >-------------------------------------------*/

    /**
     * Prepiece, Suffix
     * @param go
     * @throws Exception
     */
    private void doPrepieceSuffix( boolean debug, boolean go, String source, String rmtype ) throws Exception
    {
        long threadId = Thread.currentThread().getId();

        String funcname = String.format( "Thread id %02d; doPrepieceSuffix for source %s", threadId, source );

        if( !go ) {
            if( showskip ) { showMessage( "Skipping " + funcname, false, true ); }
            return;
        }

        long timeStart = System.currentTimeMillis();
        showMessage( funcname + "...", false, true );

        //if( ! multithreaded ) {
            long start = System.currentTimeMillis();
            TableToArrayListMultimap almmPrepiece = new TableToArrayListMultimap( dbconRefRead, dbconRefWrite, "ref_prepiece", "original", "prefix" );
            TableToArrayListMultimap almmSuffix   = new TableToArrayListMultimap( dbconRefRead, dbconRefWrite, "ref_suffix",   "original", "standard" );
          //TableToArrayListMultimap almmAlias    = new TableToArrayListMultimap( dbconRefRead, dbconRefWrite, "ref_alias",    "original",  null );
            showTimingMessage( String.format( "Thread id %02d; Loaded Prepiece/Suffix/Alias reference tables", threadId ), start );
        //}

        showMessage( String.format( "Thread id %02d; standardPrepiece", threadId ), false, true );
        standardPrepiece( debug, almmPrepiece, source );

        showMessage( String.format( "Thread id %02d; standardSuffix", threadId ), false, true );
        standardSuffix( debug, almmSuffix, source );

        // Wait until we can update
        while( almmPrepiece.isBusy().get() ) {
            plog.show( "No permission to update ref_prepiece: Waiting 60 seconds" );
            Thread.sleep( 60000 );
        }
        if( almmPrepiece.updateTable() )
        { showMessage( String.format( "Thread id %02d; Updated reference table ref_prepiece", threadId ), false, true ); }
        else
        { showMessage( String.format( "Thread id %02d; Updating ref_prepiece FAILED, was busy", threadId ), false, true ); }

        while( almmSuffix.isBusy().get() ) {
            plog.show( "No permission to update ref table: Waiting 60 seconds" );
            Thread.sleep( 60000 );
        }
        if( almmSuffix.updateTable() )
        { showMessage( String.format( "Thread id %02d; Updated reference table ref_suffix", threadId ), false, true ); }
        else
        { showMessage( String.format( "Thread id %02d; Updating ref_suffix FAILED, was busy", threadId ), false, true ); }

        // almmAlias.updateTable();     // almmAlias.add() never called; nothing added to almmAlias

        //if( ! multithreaded ) {
            almmPrepiece.free();
            almmSuffix.free();
            showMessage( String.format( "Thread id %02d; Freed almmPrepiece/almmSuffix", threadId ), false, true );
          //almmAlias.free();
        // }

        elapsedShowMessage( funcname, timeStart, System.currentTimeMillis() );
        showMessage_nl();
    } // doPrepieceSuffix


    /**
     * Firstnames
     * @param go
     * @throws Exception
     */
    private void doFirstnames( boolean debug, boolean go, String source, String rmtype ) throws Exception
    {
        long threadId = Thread.currentThread().getId();

        String funcname = String.format( "Thread id %02d; doFirstnames for source %s", threadId, source );

        if( !go ) {
            if( showskip ) { showMessage( "Skipping " + funcname, false, true ); }
            return;
        }

        long timeStart = System.currentTimeMillis();
        showMessage( funcname + "...", false, true );

        MySqlConnector dbconTemp = new MySqlConnector( url, "links_temp", user, pass );

        String msg = "";

        //if( ! multithreaded ) {
            // almmPrepiece, almmSuffix and almmAlias used by Firstnames & Familynames
            long start = System.currentTimeMillis();
            TableToArrayListMultimap almmPrepiece = new TableToArrayListMultimap( dbconRefRead, dbconRefWrite, "ref_prepiece", "original", "prefix" );
            TableToArrayListMultimap almmSuffix   = new TableToArrayListMultimap( dbconRefRead, dbconRefWrite, "ref_suffix",   "original", "standard" );
            TableToArrayListMultimap almmAlias    = new TableToArrayListMultimap( dbconRefRead, dbconRefWrite, "ref_alias",    "original",  null );
            showTimingMessage( String.format( "Thread id %02d; Loaded Prepiece/Suffix/Alias reference tables", threadId ), start );
        //}


        // Firstnames
        String tmp_firstname = "firstname_t_" + source;
        if( doesTableExist( dbconTemp, "links_temp", tmp_firstname ) ) {
            msg = String.format( "Thread id %02d; Deleting table links_temp.%s", threadId, tmp_firstname );
            showMessage( msg, false, true );
            dropTable( dbconTemp, "links_temp", tmp_firstname );
        }

        createTempFirstnameTable( dbconTemp, source );

        FileWriter writerFirstname = createTempFirstnameFile( source );

        //if( ! multithreaded ) {
            start = System.currentTimeMillis();
            TableToArrayListMultimap almmFirstname = new TableToArrayListMultimap( dbconRefRead, dbconRefWrite, "ref_firstname", "original", "standard" );
            showTimingMessage( String.format( "Thread id %02d; Loaded Firstname reference table", threadId ), start );
        //}

        int numrows = almmFirstname.numrows();
        int numkeys = almmFirstname.numkeys();
        showMessage( String.format( "Thread id %02d; Number of rows in reference table ref_firstname: %d", threadId, numrows ), false, true );
        if( numrows != numkeys )
        { showMessage( String.format( "Thread id %02d; Number of keys in arraylist multimap: %d", threadId, numkeys ), false, true ); }

        if( ! multithreaded ) { showTimingMessage( msg, start ); }

        msg = String.format( "Thread id %02d; standardFirstname...", threadId );
        showMessage( msg, false, true );
        standardFirstname( debug, almmPrepiece, almmSuffix, almmAlias, almmFirstname, writerFirstname, source );
        msg = String.format( "Thread id %02d; standardFirstname ", threadId );
        showTimingMessage( msg, start );

        start = System.currentTimeMillis();

        while( almmFirstname.isBusy().get() ) {
            plog.show( String.format( "Thread id %02d; No permission to update ref_firstname: Waiting 60 seconds", threadId ) );
            Thread.sleep( 60000 );
        }
        if( almmFirstname.updateTable() )
        { showMessage( String.format( "Thread id %02d; Updated reference table ref_firstname", threadId ), false, true ); }
        else
        { showMessage( String.format( "Thread id %02d; Updating ref_firstname FAILED, was busy", threadId ), false, true ); }

        //if( ! multithreaded ) {
            almmFirstname.free();
            showMessage( String.format( "Thread id %02d; Freed almmFirstname", threadId ), false, true );
        //}

        writerFirstname.close();
        loadFirstnameCsvToTableT( dbconTemp, source );      // insert csv -> temp table
        updateFirstnameToPersonC( dbconTemp, source );      // update person_c with temp table
        removeFirstnameFile(      source );                 // cleanup
        removeFirstnameTable(     dbconTemp, source );      // cleanup

        msg = String.format( "Thread id %02d; remains Firstname", threadId );
        showTimingMessage( msg, start );

        // Firstnames to lowercase
        start = System.currentTimeMillis();
        msg = String.format( "Thread id %02d; Converting firstnames to lowercase...", threadId );
        showMessage( msg, false, true );
        String qLower = "UPDATE links_cleaned.person_c SET firstname = LOWER( firstname ) WHERE id_source = " +  source + ";";
        dbconCleaned.runQuery( qLower );

        msg = String.format( "Thread id %02d; Converting firstnames to lowercase ", threadId );
        showTimingMessage( msg, start );

        //if( ! multithreaded ) {
            almmPrepiece.free();
            almmSuffix.free();
            almmAlias.free();
            showMessage( String.format( "Thread id %02d; Freed almmPrepiece/almmSuffix/almmAlias", threadId ), false, true );
        //}

        dbconTemp.close();

        elapsedShowMessage( funcname, timeStart, System.currentTimeMillis() );
        showMessage_nl();
    } // doFirstnames


    /**
     * Familynames
     * @param go
     * @throws Exception
     */
    private void doFamilynames( boolean debug, boolean go, String source, String rmtype ) throws Exception
    {
        long threadId = Thread.currentThread().getId();

        String funcname = String.format( "Thread id %02d; doFamilynames for source %s", threadId, source );

        if( !go ) {
            if( showskip ) { showMessage( "Skipping " + funcname, false, true ); }
            return;
        }

        long timeStart = System.currentTimeMillis();
        showMessage( funcname + "...", false, true );

        MySqlConnector dbconTemp = new MySqlConnector( url, "links_temp", user, pass );

        String msg = "";

        //if( ! multithreaded ) {
            // almmPrepiece, almmSuffix and almmAlias used by Firstnames & Familynames
            long start = System.currentTimeMillis();
            TableToArrayListMultimap almmPrepiece = new TableToArrayListMultimap( dbconRefRead, dbconRefWrite, "ref_prepiece", "original", "prefix" );
            TableToArrayListMultimap almmSuffix   = new TableToArrayListMultimap( dbconRefRead, dbconRefWrite, "ref_suffix",   "original", "standard" );
            TableToArrayListMultimap almmAlias    = new TableToArrayListMultimap( dbconRefRead, dbconRefWrite, "ref_alias",    "original",  null );
            showTimingMessage( String.format( "Thread id %02d; Loaded Prepiece/Suffix/Alias reference tables", threadId ), start );
        //}

        // Familynames
        String tmp_familyname = "familyname_t_" + source;
        if( doesTableExist( dbconTemp, "links_temp",tmp_familyname  ) ) {
            msg = String.format( "Thread id %02d; Deleting table links_temp.%s", threadId, tmp_familyname );
            showMessage( msg, false, true );
            dropTable( dbconTemp, "links_temp", tmp_familyname );
        }

        createTempFamilynameTable( dbconTemp, source );

        FileWriter writerFamilyname = createTempFamilynameFile(  source );

        //if( ! multithreaded ) {
            start = System.currentTimeMillis();
            TableToArrayListMultimap almmFamilyname = new TableToArrayListMultimap( dbconRefRead, dbconRefWrite, "ref_familyname", "original", "standard" );
            showTimingMessage( String.format( "Thread id %02d; Loaded Familyname reference table", threadId ), start );
        //}

        int numrows = almmFamilyname.numrows();
        int numkeys = almmFamilyname.numkeys();
        showMessage( String.format( "Thread id %02d; Number of rows in reference table ref_familyname: %d", threadId, numrows ), false, true );
        if( numrows != numkeys )
        { showMessage( String.format( "Number of keys in arraylist multimap: %d", threadId, numkeys ), false, true ); }

        if( ! multithreaded ) { showTimingMessage( msg, start ); }

        msg = String.format( "Thread id %02d; standardFamilyname", threadId );
        showMessage( msg + "...", false, true );
        standardFamilyname( debug, almmPrepiece, almmSuffix, almmAlias, almmFamilyname, writerFamilyname, source );
        showTimingMessage( msg, start );

        start = System.currentTimeMillis();
        msg = String.format( "Thread id %02d; remains Familyname", threadId );
        showMessage( msg + "...", false, true );

        while( almmFamilyname.isBusy().get() ) {
            plog.show( String.format( "Thread id %02d; No permission to update ref_familyname: Waiting 60 seconds", threadId ) );
            Thread.sleep( 60000 );
        }
        if( almmFamilyname.updateTable() )
        { showMessage( String.format( "Thread id %02d; Updated reference table ref_familyname", threadId ), false, true ); }
        else
        { showMessage( String.format( "Thread id %02d; Updating ref_familyname FAILED, was busy", threadId ), false, true ); }

        //if( ! multithreaded ) {
            almmFamilyname.free();
            showMessage( String.format( "Thread id %02d; Freed almmFamilyname", threadId ), false, true );
        //}

        writerFamilyname.close();
        loadFamilynameCsvToTableT(     dbconTemp, source );
        updateFamilynameToPersonC( dbconTemp, source );
        removeFamilynameFile(      source );
        removeFamilynameTable(     dbconTemp, source );
        showTimingMessage( msg, start );

        // Familynames to lowercase
        start = System.currentTimeMillis();
        msg = String.format( "Thread id %02d; Converting familynames to lowercase...", threadId );
        showMessage( msg, false, true );
        String qLower = "UPDATE links_cleaned.person_c SET familyname = LOWER( familyname ) WHERE id_source = " + source + ";";
        dbconCleaned.runQuery( qLower );
        msg = String.format( "Thread id %02d; Converting familynames to lowercase ", threadId );
        showTimingMessage( msg, start );

        //if( ! multithreaded ) {
            almmPrepiece.free();
            almmSuffix.free();
            almmAlias.free();
            showMessage( String.format( "Thread id %02d; Freed almmPrepiece/almmSuffix/almmAlias", threadId ), false, true );
        //}

        dbconTemp.close();

        elapsedShowMessage( funcname, timeStart, System.currentTimeMillis() );
        showMessage_nl();
    } // doFamilynames


    /**
     * @param source
     * @throws Exception
     */
    public void standardFirstname( boolean debug, TableToArrayListMultimap almmPrepiece, TableToArrayListMultimap almmSuffix, TableToArrayListMultimap almmAlias,
        TableToArrayListMultimap almmFirstname, FileWriter writerFirstname, String source )
    {
        long threadId = Thread.currentThread().getId();

        int count = 0;
        int count_empty = 0;
        int count_still = 0;
        int stepstate = count_step;

        try
        {
            // WHY IS A LOCAL CONNECTION USED?
            Connection con = getConnection( "links_original" );
            con.setReadOnly( true );

            String selectQuery = "SELECT id_person , firstname , stillbirth FROM person_o WHERE id_source = " + source;
            //String selectQuery = "SELECT id_person , firstname , stillbirth FROM person_o WHERE id_source = " + source + " ORDER BY id_person";

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

                int id_person    = rsFirstName.getInt( "id_person" );
                String firstname = rsFirstName.getString( "firstname" );

                // currently never filled in person_o, but flagged by having a firstname 'Levenloos'
                //String stillbirth = rsFirstName.getString( "stillbirth" );

                // Is firstname empty?
                if( firstname != null && !firstname.isEmpty() )
                {
                    if( debug ) { System.out.println( "firstname: " + firstname ); }
                    firstname = cleanFirstname( debug, source, id_person, firstname );

                    // cleanFirstname() assumes the presence of uppercase letters, so only now to lowercase
                    firstname = firstname.toLowerCase();
                    if( debug ) { System.out.println( "firstname: " + firstname ); }

                    // Check name on aliases
                    String nameNoAlias = standardAlias( debug, almmAlias, id_person, source, firstname, 1107 );

                    // Check on serried spaces; split name on spaces
                    String[] names = nameNoAlias.split( " " );

                    ArrayList< String > preList  = new ArrayList< String >();
                    ArrayList< String > postList = new ArrayList< String >();

                    boolean empty_name = false;
                    for( String name : names ) {
                        if( name.isEmpty() ) { empty_name = true; }
                        else { preList.add( name ); }       // add to list
                    }

                    if( empty_name ) { addToReportPerson( id_person, source, 1103, "" ); }  // EC 1103

                    String stillbirth = "";
                    // loop through the pieces of the name
                    for( int i = 0; i < preList.size(); i++ )
                    {
                        String prename = preList.get( i );       // does this name part exist in ref_firstname?
                        if( debug ) { System.out.println( "prename: " + prename ); }

                        if( almmFirstname.contains( prename ) )
                        {
                            // Check the standard code
                            String standard_code = almmFirstname.code( prename );
                            if( standard_code == null ) { standard_code = ""; }
                            if( debug ) { System.out.println( "standard_code: " + standard_code ); }

                            if( standard_code.equals( SC_Y ) )
                            {
                                almmFirstname.standard( "Levenloos" );
                                String standard = almmFirstname.standard( prename );
                                if( debug ) { System.out.println( "standard: " + standard ); }
                                postList.add( standard );

                                // if stillbirth has been set to 'y' for this firstname we keep it,
                                // and do not let it be overwritten to '' by another prename of the same firstname
                                if( stillbirth.isEmpty() )
                                {
                                    // if the firstname equals or contains 'Levenloos' the stillbirth column contains 'y'
                                    stillbirth = almmFirstname.value( "stillbirth", prename );
                                    if( debug ) { System.out.println( "stillbirth: " + stillbirth ); }
                                    if( stillbirth == null ) { stillbirth = ""; }
                                    else if( stillbirth.equals( "y" ) ) {
                                        if( debug ) {
                                            String msg = String.format( "#: %d, id_person: %d, firstname: %s, prename: %s",
                                                count_still, id_person, firstname, prename );
                                            System.out.println( msg );
                                        }
                                        count_still++;
                                    }
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
                            String nameNoInvalidChars = cleanName( debug, source, id_person, prename );

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
                                String nameNoPieces = namePrepiece( debug, almmPrepiece, nameNoInvalidChars, id_person );

                                if( !nameNoPieces.equals( nameNoInvalidChars ) ) {
                                    addToReportPerson( id_person, source, 1107, nameNoInvalidChars );  // EC 1107
                                }

                                // last check on ref
                                if( almmFirstname.contains( nameNoPieces ) )
                                {
                                    // Check the standard code
                                    String standard_code = almmFirstname.code( nameNoPieces );

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
                                String nameNoPieces = namePrepiece( debug, almmPrepiece, nameNoInvalidChars, id_person );

                                if( !nameNoPieces.equals( nameNoInvalidChars ) ) {
                                    addToReportPerson(id_person, source, 1107, nameNoInvalidChars);   // EC 1107
                                }

                                // last check on ref
                                if( almmFirstname.contains( nameNoPieces ) )
                                {
                                    // Check the standard code
                                    String standard_code = almmFirstname.code( nameNoPieces );

                                    if( standard_code.equals( SC_Y ) )
                                    {
                                        postList.add( almmFirstname.standard( nameNoPieces ) );
                                    }
                                    else if( standard_code.equals( SC_U ) )
                                    {
                                        addToReportPerson( id_person, source, 1100, nameNoPieces );     // EC 1100
                                        postList.add( almmFirstname.standard( nameNoPieces ) );
                                    }
                                    else if( standard_code.equals( SC_N ) )
                                    {
                                        addToReportPerson( id_person, source, 1105, nameNoPieces );     // EC 1105
                                    }
                                    else if( standard_code.equals( SC_X ) )
                                    {
                                        addToReportPerson( id_person, source, 1109, nameNoPieces );     // EC 1109
                                        postList.add( nameNoPieces );
                                    }
                                    else { // EC 1100, standard_code not invalid
                                        addToReportPerson( id_person, source, 1100, nameNoPieces );     // EC 1100
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

                    // recollect firstnames, and set individual firstnames
                    String firstnames = "";
                    String firstname1 = "";
                    String firstname2 = "";
                    String firstname3 = "";
                    String firstname4 = "";
                    for( int n = 0; n < postList.size(); n++ )
                    {
                        if( n > 0 ) { firstnames += " "; }      // add space
                        String name = postList.get( n );
                        firstnames += name;

                        if( n == 0 ) { firstname1 = name; }
                        if( n == 1 ) { firstname2 = name; }
                        if( n == 2 ) { firstname3 = name; }
                        if( n == 3 ) { firstname4 = name; }
                    }

                    // if firstnames not empty write to csv
                    if( !firstnames.isEmpty() ) {
                        //String query = PersonC.updateQuery("firstname", vn, id_person);
                        //dbconCleaned.runQuery(query);

                        //writerFirstname.write( id_person + "," + firstnames + "," + stillbirth + "\n" );
                        String line = String.format( "%d,%s,%s,%s,%s,%s,%s\n",
                            id_person, firstnames, firstname1, firstname2, firstname3, firstname4, stillbirth );
                        writerFirstname.write( line );
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
            }

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

            showMessage( String.format( "Thread id %02d; %d firstname records, %d without a firstname, %s, %s", threadId, count, count_empty, strNew, strStill ), false, true );
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
    public void standardFamilyname( boolean debug, TableToArrayListMultimap almmPrepiece, TableToArrayListMultimap almmSuffix, TableToArrayListMultimap almmAlias,
        TableToArrayListMultimap almmFamilyname, FileWriter writerFamilyname, String source )
    {
        long threadId = Thread.currentThread().getId();

        int count = 0;
        int count_empty = 0;
        int stepstate = count_step;

        try {
            String selectQuery = "SELECT id_person , familyname FROM person_o WHERE id_source = " + source;

            // WHY IS A LOCAL CONNECTION USED?
            Connection con = getConnection( "links_original" );
            con.setReadOnly( true );

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
                if( debug ) { showMessage( "count: " + count + ", id_person: " + id_person + ", familyname: " + familyname, false, true ); }

                // Check is Familyname is not empty or null
                if( familyname != null && !familyname.isEmpty() )
                {
                    familyname = cleanFamilyname( debug, source, id_person, familyname );
                    familyname = familyname.toLowerCase();

                    // familyname in ref_familyname ?
                    if( almmFamilyname.contains( familyname ) )
                    {
                        // get standard_code
                        String standard_code = almmFamilyname.code( familyname );
                        if( debug ) { showMessage( "code: " + standard_code, false, true ); }

                        // Check the standard code
                        if( standard_code.equals( SC_Y ) )
                        {
                            writerFamilyname.write( id_person + "," + almmFamilyname.standard( familyname ) + "\n" );
                        }
                        else if( standard_code.equals( SC_U ) )
                        {
                            addToReportPerson( id_person, source, 1000, familyname ); // EC 1000

                            writerFamilyname.write( id_person + "," + almmFamilyname.standard( familyname ) + "\n" );
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

                        String nameNoInvalidChars = cleanName( debug, source, id_person, nameNoSerriedSpaces );

                        // Family name contains invalid chars ?
                        if( !nameNoSerriedSpaces.equalsIgnoreCase( nameNoInvalidChars ) ) {
                            addToReportPerson( id_person, source, 1004, familyname );   // EC 1004
                        }

                        // check if name has prepieces
                        String nameNoPrePiece = namePrepiece( debug, almmPrepiece, nameNoInvalidChars, id_person );

                        // Family name contains invalid chars ?
                        if( !nameNoPrePiece.equalsIgnoreCase( nameNoInvalidChars ) ) {
                            addToReportPerson( id_person, source, 1008, familyname );  // EC 1008
                        }

                        // Check on aliases
                        String nameNoAlias = standardAlias( debug, almmAlias, id_person, source, nameNoPrePiece, 1007 );

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

            showMessage( String.format( "Thread id %02d; %d familyname records, %d without a familyname, %s", threadId, count, count_empty, strNew ), false, true );
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
    private String namePrepiece( boolean debug, TableToArrayListMultimap almmPrepiece, String name, int id ) throws Exception
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
    public void standardPrepiece( boolean debug, TableToArrayListMultimap almmPrepiece, String source )
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

                prepiece = cleanName( debug, source, id_person, prepiece );

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
                    dbconCleaned.runQuery( PersonC.updateQuery( "prefix", listPF.substring( 0, ( listPF.length() - 1 ) ), id_person) );
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
    public void standardSuffix( boolean debug, TableToArrayListMultimap almmSuffix, String source )
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

                suffix = cleanName( debug, source, id_person, suffix );

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
    private String standardAlias( boolean debug, TableToArrayListMultimap almmAlias, int id, String source, String name, int errCode )
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
                String clean = cleanName( debug, source, id, names[ 1 ] );
                PersonC.updateQuery( "alias", LinksSpecific.funcCleanSides( clean ), id );

                clean = cleanName( debug, source, id, names[ 0 ] );
                return LinksSpecific.funcCleanSides( clean );
            }
        }

        return name;
    } // standardAlias


    /**
     * @param name
     * @return
     */
    private String cleanName( boolean debug, String id_source, int id_person, String name )
    throws Exception
    {
        String clean = name.replaceAll( "[^A-Za-z0-9 '\\-\\.,èêéëÈÊÉËùûúüÙÛÚÜiìîíïÌÎÍÏòôóöÒÔÓÖàâáöÀÂÁÄçÇ]+", "" );

        if( ! clean.contains( " " ) && clean.length() > 18 ) {
            if( debug ) { System.out.println( "cleanName() long name: " + clean ); }
            addToReportPerson( id_person, id_source, 1121, clean );
        }

        return clean;
    } // cleanName


    /**
     * @param name
     * @return
     */
    private String cleanFirstname( boolean debug, String id_source, int id_person, String name )
    throws Exception
    {
        // some characters should be interpreted as a missing <space>, e.g.:
        // '/' -> ' ', '<br/>' -> ' '
        // check components

        String new_name = "";
        Iterable< String > rawparts = Splitter.on( ' ' ).split( name );
        for ( String part : rawparts )
        {
            // forward slash ?
            if( part.contains( "/" ) ) {
                if( debug ) { System.out.println( "cleanFirstame() id_person: " + id_person + ", part contains '/': " + part ); }
                addToReportPerson(id_person, id_source, 1113, part);
                part = part.replace( "/", " ");
            }

            // <br/> ?
            if( part.contains( "<br/>" ) ) {
                if( debug ) { System.out.println( "cleanFirstame() id_person: " + id_person + ", part contains '<br/>': " + part ); }
                addToReportPerson(id_person, id_source, 1114, part);
                part = part.replace( "<br/>", " ");
            }

            // intermediate uppercase letter ? -> insert space
            // check for uppercase letters beyond the second char; notice: IJ should be an exception (IJsbrand)
            // there are many garbage characters
            if( part.length() > 2 ) {
                for( int i = 2; i < part.length(); i++) {
                    char ch = part.charAt( i );
                    if( Character.isUpperCase( ch ) )
                    {
                        // only insert a space before the uppercase char if it is not preceded by a '-'
                        if( part.charAt( i-1 ) != '-' ) {
                            if( debug ) { System.out.println( "cleanFirstname() id_person: " + id_person + ", part contains uppercase letter: " + part ); }
                            addToReportPerson( id_person, id_source, 1112, part );
                            part =  part.substring( 0, i-1 ) + " " + part.substring( i );
                        }
                    }
                }
            }

            if(! new_name.isEmpty() ) { new_name += " "; }
            new_name += part;
        }

        if( debug && ! name.equals( new_name ) ) { System.out.println( " -> " + new_name ); }
        name = new_name;

        String clean = name.replaceAll( "[^A-Za-z0-9 '\\-èêéëÈÊÉËùûúüÙÛÚÜiìîíïÌÎÍÏòôóöÒÔÓÖàâáöÀÂÁÄçÇ]+", "" );


        if( clean.contains( " " ) ) {
            // check components
            Iterable< String > cleanparts = Splitter.on( ' ' ).split( clean );
            for ( String part : cleanparts ) {
                if( part.length() > 18 ) {
                    if( debug ) { System.out.println( "cleanFirstname() id_person: " + id_person + ", long firstname: " + part + " in: " + clean ); }
                    addToReportPerson( id_person, id_source, 1121, part );
                }
            }
        }
        else {
            if( clean.length() > 18 ) {
                if( debug ) { System.out.println( "cleanFirstname() id_person: " + id_person + ", long firstname: " + clean ); }
                addToReportPerson( id_person, id_source, 1121, clean );
            }
        }

        return clean;
    } // cleanFirstname


    /**
     * @param name
     * @return
     */
    private String cleanFamilyname( boolean debug, String id_source, int id_person, String name )
    throws Exception
    {
        String clean = name.replaceAll( "[^A-Za-z0-9 '\\-èêéëÈÊÉËùûúüÙÛÚÜiìîíïÌÎÍÏòôóöÒÔÓÖàâáöÀÂÁÄçÇ]+", "").replaceAll("\\-", " " );

        if( ! clean.contains( " " ) && clean.length() > 18 ) {
            if( debug ) { System.out.println( "cleanFamilyname() long familyname: " + clean ); }
            addToReportPerson( id_person, id_source, 1121, clean );
        }

        return clean;
    } // cleanFamilyname


    /**
     * @throws Exception
     */
    private void createTempFamilynameTable( MySqlConnector dbconTemp, String source ) throws Exception
    {
        long threadId = Thread.currentThread().getId();

        String tablename = "familyname_t_" + source;

        showMessage( String.format( "Thread id %02d; Creating %s table", threadId, tablename  ), false, true );

        String query = "CREATE  TABLE links_temp." + tablename + " ("
            + " person_id INT UNSIGNED NOT NULL AUTO_INCREMENT ,"
            + " familyname VARCHAR(110) NULL ,"
            + " PRIMARY KEY (person_id) );";

        dbconTemp.runQuery( query );

    } // createTempFamilynameTable


    /**
     * @throws Exception
     */
    private FileWriter createTempFamilynameFile( String source ) throws Exception
    {
        long threadId = Thread.currentThread().getId();

        String filename = "familyname_t_" + source + ".csv";

        File f = new File( filename );
        if( f.exists() ) {
            showMessage( String.format( "Thread id %02d; Deleting file %s", threadId, filename ), false, true );
            f.delete();
        }

        showMessage( String.format( "Thread id %02d; Creating %s", threadId, filename ), false, true );
        return new FileWriter( filename );
    } // createTempFamilynameFile


    /**
     * @throws Exception
     */
    private void loadFamilynameCsvToTableT( MySqlConnector dbconTemp, String source ) throws Exception
    {
        long threadId = Thread.currentThread().getId();

        String csvname   = "familyname_t_" + source + ".csv";
        String tablename = "familyname_t_" + source;

        showMessage( String.format( "Thread id %02d; Loading %s into %s table", threadId, csvname, tablename ), false, true );

        String query = "LOAD DATA LOCAL INFILE '" + csvname + "'"
            + " INTO TABLE " + tablename
            + " FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' ( person_id , familyname );";

        dbconTemp.runQuery( query );

    } // loadFamilynameCsvToTableT


    /**
     *
     */
    private void updateFamilynameToPersonC( MySqlConnector dbconTemp, String source ) throws Exception
    {
        long threadId = Thread.currentThread().getId();

        showMessage( String.format( "Thread id %02d; Moving familynames from temp table to person_c", threadId ), false, true );

        String tablename = "familyname_t_" + source;

        String query = "UPDATE links_cleaned.person_c, links_temp."   + tablename
            + " SET links_cleaned.person_c.familyname = links_temp."  + tablename + ".familyname"
            + " WHERE links_cleaned.person_c.id_person = links_temp." + tablename + ".person_id;";

        dbconTemp.runQuery( query );
    } // updateFamilynameToPersonC


    public void removeFamilynameFile( String source ) throws Exception
    {
        long threadId = Thread.currentThread().getId();

        String csvname = "familyname_t_" + source + ".csv";

        showMessage( String.format( "Thread id %02d; Deleting file %s", threadId, csvname ), false, true );

        java.io.File f = new java.io.File( csvname );
        f.delete();
    } // removeFamilynameFile


    public void removeFamilynameTable( MySqlConnector dbconTemp, String source ) throws Exception
    {
        long threadId = Thread.currentThread().getId();

        String tablename = "familyname_t_" + source;

        showMessage( String.format( "Thread id %02d; Deleting table %s", threadId, tablename ), false, true );

        String query = "DROP TABLE IF EXISTS " + tablename + ";";

        dbconTemp.runQuery( query );
    } // removeFamilynameTable


    /**
     * @throws Exception
     */
    private void createTempFirstnameTable( MySqlConnector dbconTemp, String source ) throws Exception
    {
        long threadId = Thread.currentThread().getId();

        String tablename = "firstname_t_" + source;

        showMessage( String.format( "Thread id %02d; Creating %s", threadId, tablename + " table" ), false, true );

        // Notice: the stillbirth column is not yet used
        String query = "CREATE  TABLE links_temp." + tablename + " ("
            + " person_id INT UNSIGNED NOT NULL AUTO_INCREMENT ,"
            + " firstname VARCHAR(100) NULL ,"
            + " firstname1 VARCHAR(30) NULL ,"
            + " firstname2 VARCHAR(30) NULL ,"
            + " firstname3 VARCHAR(30) NULL ,"
            + " firstname4 VARCHAR(30) NULL ,"
            + " stillbirth VARCHAR(3) NULL ,"
            + " PRIMARY KEY (person_id) );";

        dbconTemp.runQuery( query );
    } // createTempFirstnameTable


    /**
     * @throws Exception
     */
    private FileWriter createTempFirstnameFile( String source ) throws Exception
    {
        long threadId = Thread.currentThread().getId();

        String filename = "firstname_t_" + source + ".csv";

        File file = new File( filename );
        if( file.exists() ) {
            showMessage( String.format( "Thread id %02d; Deleting file %s", threadId, filename ), false, true );
            file.delete();
        }

        showMessage( String.format( "Thread id %02d; Creating %s", threadId, filename ), false, true );
        return new FileWriter( filename );
    } // createTempFirstnameFile


    /**
     * @throws Exception
     */
    private void loadFirstnameCsvToTableT( MySqlConnector dbconTemp, String source ) throws Exception
    {
        long threadId = Thread.currentThread().getId();

        String csvname   = "firstname_t_" + source + ".csv";
        String tablename = "firstname_t_" + source;

        showMessage( String.format( "Thread id %02d; Loading %s into %s table", threadId, csvname, tablename ), false, true );

        String query = "LOAD DATA LOCAL INFILE '" + csvname + "'"
            + " INTO TABLE " + tablename
            + " FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n'"
            + " ( person_id , firstname , firstname1 , firstname2 , firstname3 , firstname4 , stillbirth );";

        dbconTemp.runQuery( query );
    } // loadFirstnameCsvToTableT


    /**
     *
     */
    private void updateFirstnameToPersonC( MySqlConnector dbconTemp, String source ) throws Exception
    {
        long threadId = Thread.currentThread().getId();

        showMessage( String.format( "Thread id %02d; Moving first names from temp table to person_c...", threadId ), false, true );

        String tablename = "firstname_t_" + source;

        String query   = "UPDATE links_cleaned.person_c, links_temp." + tablename
            + " SET"
            + " links_cleaned.person_c.firstname = links_temp."  + tablename + ".firstname ,"
            + " links_cleaned.person_c.firstname1 = links_temp." + tablename + ".firstname1 ,"
            + " links_cleaned.person_c.firstname2 = links_temp." + tablename + ".firstname2 ,"
            + " links_cleaned.person_c.firstname3 = links_temp." + tablename + ".firstname3 ,"
            + " links_cleaned.person_c.firstname4 = links_temp." + tablename + ".firstname4 ,"
            + " links_cleaned.person_c.stillbirth = links_temp." + tablename + ".stillbirth"
            + " WHERE links_cleaned.person_c.id_person = links_temp." + tablename + ".person_id;";

        dbconTemp.runQuery(query);
    } // updateFirstnameToPersonC


    /**
     * @throws Exception
     */
    public void removeFirstnameFile( String source ) throws Exception
    {
        long threadId = Thread.currentThread().getId();

        String csvname = "firstname_t_" + source + ".csv";

        showMessage( String.format( "Thread id %02d; Deleting file %s", threadId, csvname ), false, true );

        File f = new File( csvname );
        f.delete();
    } // removeFirstnameFile


    /**
     * @throws Exception
     */
    public void removeFirstnameTable( MySqlConnector dbconTemp, String source ) throws Exception
    {
        long threadId = Thread.currentThread().getId();

        String tablename = "firstname_t_" + source;

        showMessage( String.format( "Thread id %02d; Deleting table %s", threadId, tablename ), false, true );

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
    private void doLocations( boolean debug, boolean go, String source, String rmtype ) throws Exception
    {
        long threadId = Thread.currentThread().getId();

        String funcname = String.format( "Thread id %02d; doLocations for source %s", threadId, source );

        if( !go ) {
            if( showskip ) { showMessage( "Skipping " + funcname, false, true ); }
            return;
        }

        long funcStart = System.currentTimeMillis();
        showMessage( funcname + "...", false, true );

        //if( ! multithreaded ) {
            long start = System.currentTimeMillis();
            TableToArrayListMultimap almmLocation = new TableToArrayListMultimap( dbconRefRead, dbconRefWrite, "ref_location", "original", "location_no" );
            showTimingMessage( String.format( "Thread id %02d; Loaded Location reference table", threadId ), start );
        //}

        int numrows = almmLocation.numrows();
        int numkeys = almmLocation.numkeys();
        showMessage( String.format( "Thread id %02d; Number of rows in reference table ref_location: %d", threadId, numrows ), false, true );
        if( numrows != numkeys )
        { showMessage( String.format( "Thread id %02d; Number of keys in arraylist multimap: %d", threadId, numkeys ), false, true ); }

        String msg = "";

        start = System.currentTimeMillis();
        msg = String.format( "Thread id %02d; standardRegistrationLocation...", threadId );
        showMessage( msg, false, true );
        standardRegistrationLocation( debug, almmLocation, source );
        msg = String.format( "Thread id %02d; standardRegistrationLocation ", threadId );
        showTimingMessage( msg, start );

        start = System.currentTimeMillis();
        msg = String.format( "Thread id %02d; standardBirthLocation...", threadId );
        showMessage( msg, false, true );
        standardBirthLocation( debug, almmLocation, source );
        msg = String.format( "Thread id %02d; standardBirthLocation ", threadId );
        showTimingMessage( msg, start );

        start = System.currentTimeMillis();
        msg = String.format( "Thread id %02d; standardMarriageLocation...", threadId );
        showMessage( msg, false, true );
        standardMarriageLocation( debug, almmLocation, source );
        msg = String.format( "Thread id %02d; standardMarriageLocation ", threadId );
        showTimingMessage( msg, start );

        start = System.currentTimeMillis();
        msg = String.format( "Thread id %02d; standardDivorceLocation...", threadId );
        showMessage( msg, false, true );
        standardDivorceLocation( debug, almmLocation, source );
        msg = String.format( "Thread id %02d; standardDivorceLocation ", threadId );
        showTimingMessage( msg, start );

        start = System.currentTimeMillis();
        msg = String.format( "Thread id %02d; standardLivingLocation...", threadId );
        showMessage( msg, false, true );
        standardLivingLocation( debug, almmLocation, source );
        msg = String.format( "Thread id %02d; standardLivingLocation ", threadId );
        showTimingMessage( msg, start );

        start = System.currentTimeMillis();
        msg = String.format( "Thread id %02d; standardDeathLocation...", threadId );
        showMessage( msg, false, true );
        standardDeathLocation( debug,  almmLocation, source );
        msg = String.format( "Thread id %02d; standardDeathLocation ", threadId );
        showTimingMessage( msg, start );


        while( almmLocation.isBusy().get() ) {
            plog.show( String.format( "Thread id %02d; No permission to update ref_location: Waiting 60 seconds", threadId ) );
            Thread.sleep( 60000 );
        }
        if( almmLocation.updateTable() )
        { showMessage( String.format( "Thread id %02d; Updated reference table ref_location", threadId ), false, true ); }
        else
        { showMessage( String.format( "Thread id %02d; Updating ref_location FAILED, was busy", threadId ), false, true ); }

        //if( ! multithreaded ) {
            almmLocation.free();
            showMessage( String.format( "Thread id %02d; Freed almmLocation", threadId ), false, true );
        //}

        elapsedShowMessage( funcname, funcStart, System.currentTimeMillis() );
        showMessage_nl();
    } // doLocations


    /**
     * @param debug
     * @param rs
     * @param idFieldO
     * @param locationFieldO
     * @param locationFieldC
     * @param id_source
     * @param tt
     */
    private void standardLocation( boolean debug, TableToArrayListMultimap almmLocation, ResultSet rs, String idFieldO, String locationFieldO, String locationFieldC, String id_source, TableType tt )
    throws Exception
    {
        long threadId = Thread.currentThread().getId();

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

                int id = rs.getInt(idFieldO);
                String location = rs.getString( locationFieldO );

                if( location != null && !location.isEmpty() )
                {
                    location = location.toLowerCase();
                    if( debug ) { System.out.println( "id_person: " + id + ", original: " + locationFieldO + ", location: " + location ); }
                    if( almmLocation.contains( location ) )
                    {
                        String refSCode = almmLocation.code( location );
                        if( debug ) { System.out.println( "refSCode: " + refSCode ); }

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

            showMessage( String.format( "Thread id %02d; %d %s records, %d without location, %s", threadId, count, locationFieldO, count_empty, strNew ), false, true );
        }
        catch( Exception ex ) {
            throw new Exception( "count: " + count + " Exception while cleaning Location: " + ex.getMessage() );
        }
    } // standardLocation


    /**
     * @param debug
     * @param source
     */
    public void standardRegistrationLocation( boolean debug, TableToArrayListMultimap almmLocation, String source )
    {
        String selectQuery = "SELECT id_registration , registration_location FROM registration_o WHERE id_source = " + source;
        if( debug ) { showMessage( "standardRegistrationLocation: " + selectQuery, false, true ); }

        try {
            ResultSet rs = dbconOriginal.runQueryWithResult( selectQuery );

            standardLocation( debug, almmLocation, rs, "id_registration", "registration_location", "registration_location_no", source, TableType.REGISTRATION );
        }
        catch( Exception ex ) {
            showMessage( ex.getMessage(), false, true );
            ex.printStackTrace( new PrintStream( System.out ) );
        }
    } // standardRegistrationLocation


    /**
     * @param debug
     * @param source
     */
    public void standardBirthLocation( boolean debug, TableToArrayListMultimap almmLocation, String source )
    {
        String selectQuery = "SELECT id_person , birth_location FROM person_o WHERE id_source = " + source + " AND birth_location <> ''";
        if( debug ) { showMessage( "standardBirthLocation: " + selectQuery, false, true ); }

        try {
            ResultSet rs = dbconOriginal.runQueryWithResult( selectQuery );

            standardLocation( debug, almmLocation, rs, "id_person", "birth_location", "birth_location", source, TableType.PERSON );
        }
        catch( Exception ex ) {
            showMessage( ex.getMessage(), false, true );
            ex.printStackTrace( new PrintStream( System.out ) );
        }
    } // standardBirthLocation


    /**
     * @param debug
     * @param source
     */
    public void standardMarriageLocation( boolean debug, TableToArrayListMultimap almmLocation, String source )
    {
        String selectQuery = "SELECT id_person , mar_location FROM person_o WHERE id_source = " + source + " AND mar_location <> ''";
        if( debug ) { showMessage( "standardMarLocation: " + selectQuery, false, true ); }

        try {
            ResultSet rs = dbconOriginal.runQueryWithResult( selectQuery );
            standardLocation( debug, almmLocation, rs, "id_person", "mar_location", "mar_location", source, TableType.PERSON );
        }
        catch( Exception ex ) {
            showMessage( ex.getMessage(), false, true );
            ex.printStackTrace( new PrintStream( System.out ) );
        }
    } // standardMarriageLocation


    /**
     * @param debug
     * @param source
     */
    public void standardDivorceLocation( boolean debug, TableToArrayListMultimap almmLocation, String source )
    {
        String selectQuery = "SELECT id_person , divorce_location FROM person_o WHERE id_source = " + source + " AND divorce_location <> ''";
        if( debug ) { showMessage( "standardDivorceLocation: " + selectQuery, false, true ); }

        try {
            ResultSet rs = dbconOriginal.runQueryWithResult( selectQuery );
            standardLocation( debug, almmLocation, rs, "id_person", "divorce_location", "divorce_location", source, TableType.PERSON );
        }
        catch( Exception ex ) {
            showMessage( ex.getMessage(), false, true );
            ex.printStackTrace( new PrintStream( System.out ) );
        }
    } // standardDivorceLocation


    /**
     * @param debug
     * @param source
     */
    public void standardLivingLocation( boolean debug, TableToArrayListMultimap almmLocation, String source )
    {
        String selectQuery = "SELECT id_person , living_location FROM person_o WHERE id_source = " + source + " AND living_location <> ''";
        if( debug ) { showMessage( "standardLivingLocation: " + selectQuery, false, true ); }

        try {
            ResultSet rs = dbconOriginal.runQueryWithResult( selectQuery );
            standardLocation( debug, almmLocation, rs, "id_person", "living_location", "living_location", source, TableType.PERSON );
        }
        catch( Exception ex ) {
            showMessage( ex.getMessage(), false, true );
            ex.printStackTrace( new PrintStream( System.out ) );
        }
    } // standardLivingLocation


    /**
     * @param debug
     * @param source
     */
    public void standardDeathLocation( boolean debug, TableToArrayListMultimap almmLocation, String source )
    {
        String selectQuery = "SELECT id_person , death_location FROM person_o WHERE id_source = " + source + " AND death_location <> ''";
        if( debug ) { showMessage( "standardDeathLocation: " + selectQuery, false, true ); }

        try {
            ResultSet rs = dbconOriginal.runQueryWithResult( selectQuery );
            standardLocation( debug, almmLocation, rs, "id_person", "death_location", "death_location", source, TableType.PERSON );
        }
        catch( Exception ex ) {
            showMessage( ex.getMessage(), false, true );
            ex.printStackTrace( new PrintStream( System.out ) );
        }
    } // standardDeathLocation


    /**
     * @param debug
     * @param source
     */
    public void flagBirthLocation( boolean debug, String source )
    {
        long threadId = Thread.currentThread().getId();

        String[] queries =
        {
            "UPDATE person_c, registration_c"
                + " SET"
                + " person_c.birth_location_flag = 1"
                + " WHERE person_c.id_source = " + source
                + " AND person_c.registration_maintype = 1"
                + " AND person_c.role = 1"
                + " AND person_c.birth_location IS NOT NULL"
                + " AND person_c.id_registration = registration_c.id_registration; ",

            "UPDATE person_c, registration_c"
                + " SET"
                + " person_c.birth_location_flag = 2,"
                + " person_c.birth_location = registration_c.registration_location_no"
                + " WHERE person_c.id_source = " + source
                + " AND person_c.registration_maintype = 1"
                + " AND person_c.role = 1"
                + " AND person_c.birth_location IS NULL"
                + " AND person_c.id_registration = registration_c.id_registration; ",
        };

        int n = 0;
        for( String query : queries )
        {
            try
            {
                int rowsAffected = dbconCleaned.runQueryUpdate( query );
                String msg = "";
                if( n == 0 )
                { msg = String.format( "Thread id %02d; flag = 1 # of rows affected: %d", threadId, rowsAffected ); }
                else
                { msg = String.format( "Thread id %02d; flag = 2 # of rows affected: %d", threadId, rowsAffected ); }

                showMessage( msg, false, true );
                n++;
            }
            catch( Exception ex ) {
                showMessage( query, false, true );
                showMessage( "Exception in flagBirthLocation(): " + ex.getMessage(), false, true );
                ex.printStackTrace( new PrintStream( System.out ) );
            }
        }
    } // flagBirthLocation


    /**
     * @param debug
     * @param source
     */
    public void flagMarriageLocation( boolean debug, String source )
    {
        long threadId = Thread.currentThread().getId();

        String[] queries =
        {
            "UPDATE person_c, registration_c"
                + " SET"
                + " person_c.mar_location_flag = 1"
                + " WHERE person_c.id_source = " + source
                + " AND person_c.registration_maintype = 2"
                + " AND ( ( person_c.role = 4 ) || ( person_c.role = 7 ) )"
                + " AND person_c.mar_location IS NOT NULL"
                + " AND person_c.id_registration = registration_c.id_registration;",

            "UPDATE person_c, registration_c"
                + " SET"
                + " person_c.mar_location_flag = 2,"
                + " person_c.mar_location = registration_c.registration_location_no"
                + " WHERE person_c.id_source = " + source
                + " AND person_c.registration_maintype = 2"
                + " AND ( ( person_c.role = 4 ) || ( person_c.role = 7 ) )"
                + " AND person_c.mar_location IS NULL"
                + " AND person_c.id_registration = registration_c.id_registration; ",
        };

        int n = 0;
        for( String query : queries )
        {
            try
            {
                int rowsAffected = dbconCleaned.runQueryUpdate( query );
                String msg = "";
                if( n == 0 )
                { msg = String.format( "Thread id %02d; flag = 1 # of rows affected: %d", threadId, rowsAffected ); }
                else
                { msg = String.format( "Thread id %02d; flag = 2 # of rows affected: %d", threadId, rowsAffected ); }

                showMessage( msg, false, true );
                n++;
            }
            catch( Exception ex ) {
                showMessage( query, false, true );
                showMessage( "Exception in flagMarriageLocation(): " + ex.getMessage(), false, true );
                ex.printStackTrace( new PrintStream( System.out ) );
            }
        }
    } // flagMarriageLocation


    /**
     * @param debug
     * @param source
     */
    public void flagDeathLocation( boolean debug, String source )
    {
        long threadId = Thread.currentThread().getId();

        String[] queries =
        {
            "UPDATE person_c, registration_c"
                + " SET"
                + " person_c.death_location_flag = 1"
                + " WHERE person_c.id_source = " + source
                + " AND person_c.registration_maintype = 3"
                + " AND person_c.role = 10"
                + " AND person_c.death_location IS NOT NULL"
                + " AND person_c.id_registration = registration_c.id_registration; ",

            "UPDATE person_c, registration_c "
                + " SET "
                + " person_c.death_location_flag = 2,"
                + " person_c.death_location = registration_c.registration_location_no"
                + " WHERE person_c.id_source = " + source
                + " AND person_c.registration_maintype = 3"
                + " AND person_c.role = 10"
                + " AND person_c.death_location IS NULL"
                + " AND person_c.id_registration = registration_c.id_registration; ",
        };

        int n = 0;
        for( String query : queries )
        {
            try
            {
                int rowsAffected = dbconCleaned.runQueryUpdate( query );
                String msg = "";
                if( n == 0 )
                { msg = String.format( "Thread id %02d; flag = 1 # of rows affected: %d", threadId, rowsAffected ); }
                else
                { msg = String.format( "Thread id %02d; flag = 2 # of rows affected: %d", threadId, rowsAffected ); }

                showMessage( msg, false, true );
                n++;
            }
            catch( Exception ex ) {
                showMessage( query, false, true );
                showMessage( "Exception in flagDeathLocation(): " + ex.getMessage(), false, true );
                ex.printStackTrace( new PrintStream( System.out ) );
            }
        }
    } // flagDeathLocation


    /*---< Civil status and Sex >---------------------------------------------*/

    /**
     * Sex and Civilstatus
     * @param go
     * @throws Exception
     */
    private void doStatusSex( boolean debug, boolean go, String source, String rmtype ) throws Exception
    {
        long threadId = Thread.currentThread().getId();

        String funcname = String.format( "Thread id %02d; doStatusSex for source %s", threadId, source );

        if( !go ) {
            if( showskip ) { showMessage( "Skipping " + funcname, false, true ); }
            return;
        }

        long timeStart = System.currentTimeMillis();
        showMessage( funcname + "...", false, true );

        //if( ! multithreaded ) {
            long start = System.currentTimeMillis();
            TableToArrayListMultimap almmCivilstatus = new TableToArrayListMultimap( dbconRefRead, dbconRefWrite, "ref_status_sex", "original", "standard_civilstatus" );
            TableToArrayListMultimap almmSex = new TableToArrayListMultimap( dbconRefRead, dbconRefWrite, "ref_status_sex", "original", "standard_sex" );
            showTimingMessage( String.format( "Thread id %02d; Loaded Civilstatus/Sex reference table", threadId ), start );
        //}

        int numrows = almmCivilstatus.numrows();
        int numkeys = almmCivilstatus.numkeys();
        showMessage( String.format( "Thread id %02d; Number of rows in reference table ref_status_sex: %d", threadId, numrows ), false, true );
        if( numrows != numkeys )
        { showMessage( String.format( "Thread id %02d; Number of keys in arraylist multimap: %d", threadId, numkeys ), false, true ); }

        standardSex( debug, almmCivilstatus, almmSex, source );
        standardCivilstatus( debug, almmCivilstatus, source );

        while( almmCivilstatus.isBusy().get() ) {
            plog.show( String.format( "Thread id %02d; No permission to update ref table: Waiting 60 seconds", threadId ) );
            Thread.sleep( 60000 );
        }
        if( almmCivilstatus.updateTable() )
        { showMessage( String.format( "Thread id %02d; Updated reference table ref_status_sex", threadId ), false, true ); }
        else
        { showMessage( String.format( "Thread id %02d; Updating ref_status_sex FAILED, was busy", threadId ), false, true ); }

        //if( ! multithreaded ) {
            almmCivilstatus.free();
            almmSex.free();
        //}

        elapsedShowMessage( funcname, timeStart, System.currentTimeMillis() );
        showMessage_nl();
    } // doStatusSex


    /**
     * @param source
     */
    public void standardSex( boolean debug, TableToArrayListMultimap almmCivilstatus, TableToArrayListMultimap almmSex, String source )
    {
        long threadId = Thread.currentThread().getId();

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
                        String refSCode = almmSex.code( sex );
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

            showMessage( String.format( "Thread id %02d; %d gender records, %d without gender, %s", threadId, count, count_empty, strNew ), false, true );
        }
        catch( Exception ex ) {
            showMessage( "count: " + count + " Exception while cleaning Sex: " + ex.getMessage(), false, true );
            ex.printStackTrace( new PrintStream( System.out ) );
        }
    } // standardSex


    /**
     * @param source
     */
    public void standardCivilstatus( boolean debug, TableToArrayListMultimap almmCivilstatus, String source )
    {
        long threadId = Thread.currentThread().getId();

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
                    if( almmCivilstatus.contains( civil_status ) )          // check presence in original

                    {
                        String refSCode = almmCivilstatus.code( civil_status );
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
                                if( !sex.equalsIgnoreCase( almmCivilstatus.value( "standard_sex", civil_status) ) ) {
                                    if( sex != SC_U ) {
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
                            addToReportPerson( id_person, source, 69, civil_status );               // warning 68
                        }
                    }
                    else {      // add to ref
                        if( debug ) { showMessage( "standardCivilstatus: not present in original", false, true ); }
                        if( debug ) { showMessage( "Warning 31: id_person: " + id_person + ", sex: " + sex, false, true ); }

                        addToReportPerson( id_person, source, 61, civil_status );                   // warning 61

                        almmCivilstatus.add( civil_status );                                        // Add new civil_status

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

            showMessage( String.format( "Thread id %02d; %d civil status records, %d without civil status, %s", threadId, count, count_empty, strNew ), false, true );
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
    private void doRegistrationType( boolean debug, boolean go, String source, String rmtype ) throws Exception
    {
        long threadId = Thread.currentThread().getId();

        String funcname = String.format( "Thread id %02d; doRegistrationType for source %s", threadId, source );

        if( !go ) {
            if( showskip ) { showMessage( "Skipping " + funcname, false, true ); }
            return;
        }

        long timeStart = System.currentTimeMillis();
        showMessage( funcname + "...", false, true );

        /*
        long start = System.currentTimeMillis();
        String msg = "Loading reference table: ref_registration";
        showMessage( msg + "...", false, true );
        if( ! multithreaded ) { almmRegisType = new TableToArrayListMultimap( dbconRefRead, dbconRefWrite, "ref_registration", "original", "standard" ); }
        elapsedShowMessage( msg, start, System.currentTimeMillis() );
        */

        standardRegistrationType( debug, source );

        /*
        showMessage( "Updating reference table: ref_registration", false, true );
        almmRegisType.updateTable();
        if( ! multithreaded ) { almmRegisType.free(); }
        */

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
                    }
                    else if( refSCode.equals( SC_Y ) ) {
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
                {      // not in reference; add to reference with SC_X
                    if( debug ) { showMessage( "Warning 51 (not in ref): id_registration: " + id_registration + ", reg type: " + registration_type, false, true ); }

                    addToReportRegistration( id_registration, source, 51, registration_type );           // warning 51

                    //almmRegisType.add( occupation );      //need adapted almmRegisType

                    // column 'original' now has a UNIQUE key: using IGNORE to skip duplicates, preventing failing queries
                    dbconRefWrite.runQuery( "INSERT IGNORE INTO ref_registration( original, main_type, standard_code ) VALUES ('" + registration_type + "', '" + registration_maintype + "', 'x')" );

                    // update links_cleaned.registration_c
                    String query = RegistrationC.updateQuery( "registration_type", registration_type.length() < 50 ? registration_type : registration_type.substring(0, 50), id_registration );
                    dbconCleaned.runQuery( query );
                }
            }
        }
        catch( Exception ex ) {
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
    private void doOccupation( boolean debug, boolean go, String source, String rmtype ) throws Exception
    {
        long threadId = Thread.currentThread().getId();

        String funcname = String.format( "Thread id %02d; doOccupation for source %s", threadId, source );

        if( !go ) {
            if( showskip ) { showMessage( "Skipping " + funcname, false, true ); }
            return;
        }

        long funcstart = System.currentTimeMillis();
        showMessage( funcname + "...", false, true );

        //if( ! multithreaded ) {
            long start = System.currentTimeMillis();
            TableToArrayListMultimap almmOccupation = new TableToArrayListMultimap( dbconRefRead, dbconRefWrite, "ref_occupation", "original", "standard" );
            showTimingMessage( String.format( "Thread id %02d; Loaded Occupation reference table", threadId ), start );
        //}

        int numrows = almmOccupation.numrows();
        int numkeys = almmOccupation.numkeys();
        showMessage( String.format( "Thread id %02d; Number of rows in reference table ref_occupation: %d", threadId, numrows ), false, true );
        if( numrows != numkeys )
        { showMessage( String.format( "Thread id %02d; Number of keys in arraylist multimap: %d", threadId, numkeys ), false, true ); }

        String msg = String.format( "Thread id %02d; Processing standardOccupation for source: %s", threadId, source );
        showMessage( msg, false, true );
        standardOccupation( debug, almmOccupation, source );

        msg = String.format( "Thread id %02d; Updating ref_occupation", threadId );
        showMessage( msg, false, true );

        while( almmOccupation.isBusy().get() ) {
            plog.show( String.format( "Thread id %02d; No permission to update ref_occupation: Waiting 60 seconds", threadId ) );
            Thread.sleep( 60000 );
        }
        if( almmOccupation.updateTable() )
        { showMessage( String.format( "Thread id %02d; Updated ref_occupation", threadId ), false, true ); }
        else
        { showMessage( String.format( "Thread id %02d; Updating ref_occupation FAILED, was busy", threadId ), false, true ); }

        //if( ! multithreaded ) {
            almmOccupation.free();
            showMessage( String.format( "Thread id %02d; Freed almmOccupation", threadId ), false, true );
        //}

        elapsedShowMessage( funcname, funcstart, System.currentTimeMillis() );
        showMessage_nl();
    } // doOccupation


    /**
     * @param debug
     * @param source
     */
    public void standardOccupation( boolean debug, TableToArrayListMultimap almmOccupation, String source )
    {
        long threadId = Thread.currentThread().getId();

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
                    standardOccupationRecord( debug, almmOccupation, count, source, id_person, occupation );
                }
            }

            int count_new = almmOccupation.newcount();
            String strNew = "";
            if( count_new == 0 ) { strNew = "no new occupations"; }
            else if( count_new == 1 ) { strNew = "1 new occupation"; }
            else { strNew = "" + count_new + " new occupations"; }

            showMessage( String.format( "Thread id %02d; %d occupation records, %d without occupation, %s", threadId, count, count_empty, strNew ), false, true );
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
    public void standardOccupationRecord( boolean debug, TableToArrayListMultimap almmOccupation, int count, String source, int id_person, String occupation )
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
                    if( debug ) { showMessage("getStandardCodeByOriginal: " + occupation, false, true ); }

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
            showMessage( "count: " + count + " Exception while cleaning Occupation: " + ex3.getMessage(), false, true );
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
    private void doAge( boolean debug, boolean go, String source, String rmtype ) throws Exception
    {
        long threadId = Thread.currentThread().getId();

        String funcname = String.format( "Thread id %02d; doAge for source %s", threadId, source );

        if( !go ) {
            if( showskip ) { showMessage( "Skipping " + funcname, false, true ); }
            return;
        }

        long timeStart = System.currentTimeMillis();

        showMessage( funcname + "...", false, true );

        //if( ! multithreaded ) {
            long start = System.currentTimeMillis();
            TableToArrayListMultimap almmLitAge = new TableToArrayListMultimap( dbconRefRead, dbconRefWrite, "ref_age", "original", "standard_year" );
            showTimingMessage( String.format( "Thread id %02d; Loaded LitAge reference table", threadId ), start );
        //}

        int size = almmLitAge.numkeys();
        String msg = String.format( "Thread id %02d; Reference table: ref_age [%d records]", threadId, size );
        showMessage( msg, false, true );

        long timeSAL = System.currentTimeMillis();
        msg = String.format( "Thread id %02d; Processing standardAgeLiteral for source: %s...", threadId, source );
        showMessage( msg, false, true );
        standardAgeLiteral( debug, almmLitAge, source );
        msg = String.format( "Thread id %02d; Processing standardAgeLiteral for source: %s ", threadId, source );
        elapsedShowMessage( msg, timeSAL, System.currentTimeMillis() );

        long timeSA = System.currentTimeMillis();
        msg = String.format( "Thread id %02d; Processing standardAge for source: %s...", threadId, source );
        showMessage( msg, false, true );
        standardAge( debug, source );
        msg = String.format( "Thread id %02d; Processing standardAge for source: %s ", threadId, source );
        elapsedShowMessage( msg, timeSA, System.currentTimeMillis() );

        msg =String.format( "Thread id %02d; Updating ref_age: ref_age", threadId );
        showMessage( msg, false, true );

        while( almmLitAge.isBusy().get() ) {
            plog.show( "No permission to update ref table: Waiting 60 seconds" );
            Thread.sleep( 60000 );
        }
        if( almmLitAge.updateTable() )
        { showMessage( String.format( "Thread id %02d; Updated reference table ref_age", threadId ), false, true ); }
        else
        { showMessage( String.format( "Thread id %02d; Updating ref_age FAILED, was busy", threadId ), false, true ); }

        //if( ! multithreaded ) {
            almmLitAge.free();
            showMessage( String.format( "Thread id %02d; Freed almmLitAge", threadId ), false, true );
        //}

        elapsedShowMessage( funcname, timeStart, System.currentTimeMillis() );
        showMessage_nl();
    } // doAge



     /**
     * @param source
     */
    public void standardAgeLiteral( boolean debug, TableToArrayListMultimap almmLitAge, String source )
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

                if( numeric && lit_year > 0 && lit_year < 115 )
                {
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

                        String query = PersonC.updateQuery( "age_literal", age_literal, id_person);
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

                        if (debug) { showMessage("Warning 255: id_person: " + id_person + ", age_literal: " + age_literal, false, true ); }
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
                        if( debug ) { showMessage("Warning 259: id_person: " + id_person + ", age_literal: " + age_literal, false, true ); }
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
                                + standard_month + ", week: " + standard_week + ", day: " + standard_day, false, true );
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

                if( ( age_year >= 0 ) && ( age_year < 115 ) )
                { update = true; }
                else
                {
                    addToReportPerson( id_person, source, 244, age_year + "" );
                    System.out.println( "standardAge: " + age_year );

                    String updateQuery = "UPDATE links_cleaned.person_c SET age_year = null"
                        + " WHERE id_person = " + id_person + " AND id_source = " + source;

                    if( debug ) { showMessage( updateQuery, false, true ); }
                    dbconCleaned.runQuery( updateQuery );
                }

                if( ( age_month >= 0 ) && ( age_month < 50 ) ) { update = true; }
                else { addToReportPerson( id_person, source, 243, age_month + "" ); }

                if( ( age_week >= 0 ) && ( age_week < 53 ) ) { update = true; }
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
    private void doRole( boolean debug, boolean go, String source, String rmtype ) throws Exception
    {
        long threadId = Thread.currentThread().getId();

        String funcname = String.format( "Thread id %02d; doRole for source %s", threadId, source );

        if( !go ) {
            if( showskip ) { showMessage( "Skipping " + funcname, false, true ); }
            return;
        }

        showMessage( funcname + "...", false, true );

        long timeStart = System.currentTimeMillis();

        //if( ! multithreaded ) {
            long start = System.currentTimeMillis();
            TableToArrayListMultimap almmRole = new TableToArrayListMultimap( dbconRefRead, dbconRefWrite, "ref_role", "original", "standard" );
            showTimingMessage( String.format( "Thread id %02d; Loaded Role reference table", threadId ), start );
        //}

        int size = almmRole.numkeys();
        String msg = String.format( "Thread id %02d; Reference table: ref_role [%d records]", threadId, size );
        showMessage( msg, false, true );

        msg = String.format( "Thread id %02d; Processing standardRole for source: %s...", threadId, source );
        showMessage( msg, false, true );

        standardRole( debug, almmRole, source );

        while( almmRole.isBusy().get() ) {
            plog.show( "No permission to update ref_role: Waiting 60 seconds" );
            Thread.sleep( 60000 );
        }
        if( almmRole.updateTable() )
        { showMessage( String.format( "Thread id %02d; Updated reference table ref_role", threadId ), false, true ); }
        else
        { showMessage( String.format( "Thread id %02d; Updating ref_role FAILED, was busy", threadId ), false, true ); }

        //if( ! multithreaded ) {
            almmRole.free();
            showMessage( String.format( "Thread id %02d; Freed almmRole", threadId ), false, true );
        //}

        msg = String.format( "Thread id %02d; Processing standardAlive for source: %s...", threadId, source );
        showMessage( msg, false, true );
        standardAlive( debug, source );

        elapsedShowMessage( funcname, timeStart, System.currentTimeMillis() );
        showMessage_nl();
    } // doRole


    /**
     *
     * @param debug
     * @param source
     */
    private void standardRole( boolean debug, TableToArrayListMultimap almmRole, String source )
    {
        long threadId = Thread.currentThread().getId();
        
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
                    if( almmRole.contains( role ) )             // present in ref_role
                    {
                        String refSCode = almmRole.code( role );
                        if( debug ) { System.out.println( "refSCode: " + refSCode ); }

                        if( refSCode.equals( SC_X ) )
                        {
                            if( debug ) { showMessage( "Warning 141 (via SC_X): id_person: " + id_person + ", role: " + role, false, true ); }
                            addToReportPerson( id_person, source, 141, role );      // warning 141

                            String role_nr = "99";
                            String updateQuery = PersonC.updateQuery( "role", role_nr, id_person );
                            if( debug ) { showMessage( updateQuery, false, true ); }
                            dbconCleaned.runQuery( updateQuery );
                        }
                        else if( refSCode.equals( SC_N ) )
                        {
                            if( debug ) { showMessage( "Warning 143: id_person: " + id_person + ", role: " + role, false, true ); }
                            addToReportPerson( id_person, source, 143, role );      // warning 143
                        }
                        else if( refSCode.equals( SC_U ) )
                        {
                            if( debug ) { showMessage( "Warning 145: id_person: " + id_person + ", role: " + role, false, true ); }
                            addToReportPerson( id_person, source, 145, role );      // warning 145

                            String role_nr = almmRole.value( "role_nr", role );
                            String updateQuery = PersonC.updateQuery( "role", role_nr, id_person );
                            if( debug ) { showMessage( updateQuery, false, true ); }
                            dbconCleaned.runQuery( updateQuery );
                        }
                        else if( refSCode.equals( SC_Y ) )
                        {
                            if( debug ) { showMessage( "Standard Role: id_person: " + id_person + ", role: " + role, false, true ); }
                            String role_nr = almmRole.value( "role_nr", role );

                            String updateQuery = PersonC.updateQuery( "role", role_nr, id_person );
                            if( debug ) { showMessage( updateQuery, false, true ); }
                            dbconCleaned.runQuery( updateQuery );
                        }
                        else
                        {
                            if( debug ) { showMessage( "Warning 149: id_person: " + id_person + ", role: " + role, false, true ); }

                            addToReportPerson( id_person, source, 149, role );      // report warning 149
                            almmRole.add( role );                                   // add new role
                        }
                    }
                    else    // not in ref_role
                    {
                        count_noref++;
                        if( debug ) { showMessage( "Warning 141 (not in ref_): id_person: " + id_person + ", role: " + role, false, true ); }

                        addToReportPerson( id_person, source, 141, role );      // report warning 141
                        almmRole.add( role );                                   // add new role

                        String role_nr = "99";
                        String updateQuery = PersonC.updateQuery( "role", role_nr, id_person );
                        if( debug ) { showMessage( updateQuery, false, true ); }
                        dbconCleaned.runQuery( updateQuery );
                    }
                }
            }

            showMessage( String.format( "Thread id %02d; %d person records, %d without a role, and %d without a standard role", threadId, count, count_empty, count_noref ), false, true );
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
        long threadId = Thread.currentThread().getId();
        
        int count = 0;
        int count_empty = 0;
        int stepstate = count_step;

        try
        {
            //String selectQuery = "SELECT id_person , role , death , occupation FROM links_cleaned.person_c WHERE id_source = " + source;
            String selectQuery = "SELECT id_registration , id_person , role , death , occupation, age_year FROM links_cleaned.person_c WHERE id_source = " + source;
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
                int age_year        = rs.getInt( "age_year" );      // read as int: NULL -> 0

                if( debug ) {
                    showMessage( "count: " + count + ", id_person: " + id_person + ", role: " + role + ", death: " + death + ", occupation: " + occupation, false, true ); }

                if( death != null ) { death = death.toLowerCase(); }
                if( death == null || death.isEmpty() ) { count_empty++; }

                if( ( ( role > 1 && role < 10 ) || role == 11 ) && ( age_year != 0 && age_year < 14 ) )  {
                    addToReportPerson( id_person, source, 265, Integer.toString( role ) );      // report warning 265
                }

                if( role == 1 || role == 4 || role == 7 || role == 10 ) {
                    if( debug ) { showMessage( "role: " + role + ", death -> 'a'", false, true ); }

                    String updateQuery = PersonC.updateQuery( "death", "a", id_person );        // set death to a[live]
                    dbconCleaned.runQuery( updateQuery );
                }
                else
                {
                    if( occupation != null ) {
                        if( debug ) { showMessage( "occupation: " + occupation + ", death -> 'a'", false, true ); }

                        String updateQuery = PersonC.updateQuery( "death", "a", id_person );     // set death to a[live]
                        dbconCleaned.runQuery( updateQuery );
                    }
                    else
                    {
                        if( death == null ) {
                            if( debug ) { showMessage( "death: " + death + ", death -> 'n'", false, true ); }

                            String updateQuery = PersonC.updateQuery( "death", "n", id_person );     // set death to n[o]
                            dbconCleaned.runQuery( updateQuery );
                        }
                        else
                        { if( debug ) { showMessage( "death stays: " + death, false, true ); } }
                    }
                }
            }
            
            showMessage( String.format( "Thread id %02d; %d person records, %d without alive specification", threadId, count, count_empty ), false, true );
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
    private void doDates( boolean debug, boolean go, String source, String rmtype ) throws Exception
    {
        long threadId = Thread.currentThread().getId();

        String funcname = String.format( "Thread id %02d; doDates for source %s", threadId, source );

        if( !go ) {
            if( showskip ) { showMessage( "Skipping " + funcname, false, true ); }
            return;
        }

        long timeStart = System.currentTimeMillis();
        showMessage( funcname + "...", false, true );

        //doAge(  debug, go, source );        // required for dates, again separate call (see above)
        //doRole( debug, go, source );        // required for dates, again separate call (see above)

        long ts = System.currentTimeMillis();
        String msg = "";

        //msg = "skipping till standardRegistrationDate()";
        //showMessage( msg, false, true );
        ///*
        ts = System.currentTimeMillis();
        String type = "birth";
        msg = String.format( "Thread id %02d; Processing standardDate for source: %s for: %s...", threadId, source, type );
        showMessage( msg, false, true );
        standardDate( debug, source, type );
        msg = String.format( "Thread id %02d; Processing standard dates ", threadId );
        elapsedShowMessage( msg, ts, System.currentTimeMillis() );

        ts = System.currentTimeMillis();
        type = "mar";
        msg = String.format( "Thread id %02d; Processing standardDate for source: %s for: %s...", threadId, source, type );
        showMessage( msg, false, true );
        standardDate( debug, source, type );
        msg = String.format( "Thread id %02d; Processing standard dates ", threadId );
        elapsedShowMessage( msg, ts, System.currentTimeMillis() );

        ts = System.currentTimeMillis();
        type = "death";
        msg = String.format( "Thread id %02d; Processing standardDate for source: %s for: %s...", threadId, source, type );
        showMessage( msg, false, true );
        standardDate( debug, source, type );
        msg = String.format( "Thread id %02d; Processing standard dates ", threadId );
        elapsedShowMessage( msg, ts, System.currentTimeMillis() );
        //*/

        ts = System.currentTimeMillis();
        msg = String.format( "Thread id %02d; Processing standardRegistrationDate for source: %s...", threadId, source );
        showMessage( msg, false, true );
        standardRegistrationDate( debug, source );
        msg = String.format( "Thread id %02d; Processing standardRegistrationDate ", threadId );
        elapsedShowMessage( msg, ts, System.currentTimeMillis() );

        //msg = "skipping remaining date functions";
        //showMessage( msg, false, true );
        ///*
        // Fill empty event dates with registration dates
        ts = System.currentTimeMillis();
        msg = String.format( "Thread id %02d; Flagging empty birth dates (-> Reg dates) for source: %s...", threadId, source );
        showMessage( msg, false, true );
        flagBirthDate( debug, source );
        msg = String.format( "Thread id %02d; Flagging empty marriage dates (-> Reg dates) for source: %s...", threadId, source );
        showMessage( msg, false, true );
        flagMarriageDate( debug, source );
        msg = String.format( "Thread id %02d; Flagging empty death dates (-> Reg dates) for source: %s...", threadId, source );
        showMessage( msg, false, true );
        flagDeathDate( debug, source );
        msg = String.format( "Thread id %02d; Flagging empty dates ", threadId );
        elapsedShowMessage( msg, ts, System.currentTimeMillis() );

        ts = System.currentTimeMillis();
        msg = String.format( "Thread id %02d; Processing minMaxValidDate for source: %s...", threadId, source );
        showMessage( msg, false, true );
        minMaxValidDate( debug, source );
        msg = String.format( "Thread id %02d; Processing minMaxValidDate ", threadId );
        elapsedShowMessage( msg, ts, System.currentTimeMillis() );

        // Make minMaxDateMain() a separate GUI option:
        // we often have date issues, and redoing the whole date cleaning takes so long.
        ts = System.currentTimeMillis();
        msg = String.format( "Thread id %02d; Processing minMaxDateMain for source: %s...", threadId, source );
        showMessage( msg, false, true );
        minMaxDateMain( debug, source );
        msg = String.format( "Thread id %02d; Processing minMaxDateMain ", threadId );
        elapsedShowMessage( msg, ts, System.currentTimeMillis() );
        //*/

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
        long threadId = Thread.currentThread().getId();

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

        int id_registration = -1;   // want to show it when exception occurs
        int id_person = -1;         // want to show it when exception occurs

        try
        {
            ResultSet rsPersons = dbconCleaned.runQueryWithResult( startQuery );            // Run person query

            // Count hits
            rsPersons.last();
            int total = rsPersons.getRow();
            rsPersons.beforeFirst();

            String msg = String.format( "Thread id %02d; person_c records: %d", threadId, total );
            showMessage( msg, true, true );
            showMessage( msg, false, true );

            while( rsPersons.next() )
            {
                count++;

                if( count == stepstate ) {
                    showMessage( count + " of " + total, true, true );
                    stepstate += count_step;
                }

                // Get
                id_registration             = rsPersons.getInt(    "id_registration" );
                int    id_source            = rsPersons.getInt(    "id_source" );
                String registrationDate     = rsPersons.getString( "registration_date" );
                int    registrationMaintype = rsPersons.getInt(    "registration_maintype" );
                id_person                   = rsPersons.getInt(    "id_person" );
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

                if( debug )
                {
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

                if( debug && mmds.getPersonRole() == 0 ) { showMessage( "minMaxDateMain() role = 0", false, true ); }

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
            showMessage( "id_registration: " + id_registration + ", id_person: " + id_person, false, true );
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
        if( debug && inputInfo.getPersonRole() == 0 ) { showMessage( "minMaxDate() role = 0", false, true ); }

        if( debug ) { showMessage( "minMaxDate()", false, true ); }

        // registration date
        if( debug ) { System.out.println( "inputInfo.getRegistrationDate()" ); }
        DateYearMonthDaySet inputregistrationYearMonthDday = LinksSpecific.divideCheckDate( inputInfo.getRegistrationDate() );

        // age given in years
        if( inputInfo.getPersonAgeYear() > 0 )
        {
            if( debug ) { showMessage( "age given in years: " + inputInfo.getPersonAgeYear() , false, true ); }

            DivideMinMaxDatumSet returnSet = new DivideMinMaxDatumSet();

            if( inputInfo.getPersonRole() == 10 )   // it is the deceased
            {
                // death date
                if( debug ) { System.out.println( "inputInfo.getDeathDate()" ); }
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

            //showMessage( "1 - age given in years", false, true );
            return returnSet;
        } // age given in years


        // birth year given?
        if( inputInfo.getPersonBirthYear() > 0 )
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

            //showMessage( "2 - birth year given", false, true );
            return returnSet;
        } // birth year given


        // not the deceased
        if( inputInfo.getPersonRole() != 10 )
        {
            if( debug ) { showMessage( "not the deceased, role: " + inputInfo.getPersonRole() , false, true ); }

            // Days, month, weeks to 1 additional year ?
            int ageinYears = roundDownAge(
                inputInfo.getPersonAgeYear(),
                inputInfo.getPersonAgeMonth(),
                inputInfo.getPersonAgeWeek(),
                inputInfo.getPersonAgeDay() );

            DivideMinMaxDatumSet returnSet = new DivideMinMaxDatumSet();

            // day and month is similar to registration date
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

            //showMessage( "3 - not the deceased", false, true );
            return returnSet;
        } // not the deceased


        // combination of days and weeks and months
        int areMonths = 0;
        int areWeeks  = 0;
        int areDays   = 0;

        if( inputInfo.getPersonAgeMonth() > 0 ) { areMonths = 1; }
        if( inputInfo.getPersonAgeWeek()  > 0 ) { areWeeks  = 1; }
        if( inputInfo.getPersonAgeDay()   > 0 ) { areDays   = 1; }

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

            //showMessage( "4 - combination; marriage date, return 0-0-0", false, true );
            return returnSet;
        } // combination; marriage date, return 0-0-0


        if( debug ) { System.out.println( "inputInfo.getDeathDate()" ); }
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

            if( debug ) { System.out.println( "at least 2 given minDate" ); }
            DateYearMonthDaySet computedMinDate = LinksSpecific.divideCheckDate( minDate );
            if( debug ) { System.out.println( "at least 2 given maxDate" ); }
            DateYearMonthDaySet computedMaxDate = LinksSpecific.divideCheckDate( maxDate );

            // Check if max date not later than registration date
            DateYearMonthDaySet dymd = checkMaxDate(
                computedMaxDate.getYear(),
                computedMaxDate.getMonth(),
                computedMaxDate.getDay(),
                useYear,
                useMonth,
                useDay );

            DivideMinMaxDatumSet returnSet = new DivideMinMaxDatumSet();

            returnSet.setMaxDay(   dymd.getDay() );
            returnSet.setMaxMonth( dymd.getMonth() );
            returnSet.setMaxYear(  dymd.getYear() );
            returnSet.setMinDay(   computedMinDate.getDay() );
            returnSet.setMinMonth( computedMinDate.getMonth() );
            returnSet.setMinYear(  computedMinDate.getYear() );

            //showMessage( "5 - combination", false, true );
            return returnSet;
        } // combination


        // age given in months
        else if( areMonths == 1 )
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

            // New date
            if( debug ) { System.out.println( "age given in months minDate" ); }
            DateYearMonthDaySet computedMinDate = LinksSpecific.divideCheckDate( minDate );
            if( debug ) { System.out.println( "age given in months maxDate" ); }
            DateYearMonthDaySet computedMaxDate = LinksSpecific.divideCheckDate( maxDate );

            DivideMinMaxDatumSet returnSet = new DivideMinMaxDatumSet();

            returnSet.setMaxDay(   computedMaxDate.getDay() );
            returnSet.setMaxMonth( computedMaxDate.getMonth() );
            returnSet.setMaxYear(  computedMaxDate.getYear() );
            returnSet.setMinDay(   computedMinDate.getDay() );
            returnSet.setMinMonth( computedMinDate.getMonth() );
            returnSet.setMinYear(  computedMinDate.getYear() );

            //showMessage( "6 - combination", false, true );
            return returnSet;
        } // age given in months


        // age given in weeks
        else if( areWeeks == 1 )
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

            // Max date
            String maxDate = addTimeToDate(
                useYear,
                useMonth,
                useDay,
                TimeType.DAY,
                maxdays );

            if( debug ) { System.out.println( "age given in weeks minDate" ); }
            DateYearMonthDaySet computedMinDate = LinksSpecific.divideCheckDate( minDate );
            if( debug ) { System.out.println( "age given in weeks maxDate" ); }
            DateYearMonthDaySet computedMaxDate = LinksSpecific.divideCheckDate( maxDate );

            // return
            DivideMinMaxDatumSet returnSet = new DivideMinMaxDatumSet();

            returnSet.setMaxDay(   computedMaxDate.getDay() );
            returnSet.setMaxMonth( computedMaxDate.getMonth() );
            returnSet.setMaxYear(  computedMaxDate.getYear() );
            returnSet.setMinDay(   computedMinDate.getDay() );
            returnSet.setMinMonth( computedMinDate.getMonth() );
            returnSet.setMinYear(  computedMinDate.getYear() );

            //showMessage( "7 - age given in weeks", false, true );
            return returnSet;
        } // age given in weeks


        // age given in days
        else if( areDays == 1 )
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
            if( debug ) { System.out.println( "age given in days minDate" ); }
            DateYearMonthDaySet computedMinDate = LinksSpecific.divideCheckDate( minDate );
            if( debug ) { System.out.println( "age given in days maxDate" ); }
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

            //showMessage( "8 - age given in weeks", false, true );
            return returnSet;
        } // age given in days


        // age not given
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

        //showMessage( "9 - age not given", false, true );
        return returnSet;   // age not given
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
        if( debug ) {
            showMessage( "minMaxCalculation()", false, true );
            if( role == 0 ) { showMessage( "minMaxCalculation() role = 0, id_person = " + id_person, false, true ); }
        }

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

        if( ! rs.next() )
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
        else if( function.equals( "C" ) )                // function C, check by reg year
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
            if( minimum_year <  reg_year )       { mmj.setMinYear( reg_year ); }
            if( maximum_year > (reg_year + 86) ) { mmj.setMaxYear( reg_year + 86 ); }
        }
        else if( function.equals( "H" ) )               // function H
        {
            if( maximum_year > (reg_year + 86) ) { mmj.setMaxYear( reg_year + 86 ); }
        }
        else if( function.equals( "I" ) )               // function I
        {
            if( maximum_year > reg_year ) { mmj.setMaxYear( reg_year ); }
            if( minimum_year > mmj.getMaxYear() ) { mmj.setMinYear( mmj.getMaxYear() ); }
        }
        else
        {
            if( debug ) { showMessage( "minMaxCalculation() function = " + function, false, true ); }
            addToReportPerson( id_person, "0", 104, "Null -> [rh:" + main_type + "][ad:" + date_type + "][rol:" + role + "][lg:" + age_reported + "][lh:" + age_main_role + "]" );
        }

        if( mmj.getMinYear() > mmj.getMaxYear() )   // min/max consistency check
        {
            //showMessage( "minMaxCalculation() Error: min_year exceeds max_year for id_person = " + id_person, false, true );
            String msg_minmax = "minYear: " + mmj.getMinYear() + ", maxYear: " +  mmj.getMaxYear();
            addToReportPerson( id_person, "0", 266, msg_minmax  );       // error 266 + min & max

            // KM: day & month both from registration date, so with min_year = max_year date are equal
            mmj.setMinYear( mmj.getMaxYear() );                 // min = max
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
        if( debug && role == 0 ) { showMessage( "minMaxMainAge() role = 0, id_person = " + id_person , false, true ); }

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

                    if( countPc == 0 )
                    {
                        if( debug ) { showMessage( "minMaxMainAge: person_c count = 0, with query:", false, true ); }
                        if( debug ) { showMessage( queryPc, false, true ); }
                             if( mm_main_role ==  1 ) { addToReportRegistration( id_registration, "" + id_source, 271, "" ); }   // WA 271
                        else if( mm_main_role ==  4 ) { addToReportRegistration( id_registration, "" + id_source, 272, "" ); }   // WA 272
                        else if( mm_main_role ==  7 ) { addToReportRegistration( id_registration, "" + id_source, 273, "" ); }   // WA 273
                        else if( mm_main_role == 10 ) { addToReportRegistration( id_registration, "" + id_source, 274, "" ); }   // WA 274
                    }
                    else if( countPc > 1 )
                    {
                        if( debug ) { showMessage( "minMaxMainAge: person_c count > 1, with query:", false, true ); }
                        if( debug ) { showMessage( queryPc, false, true ); }
                             if( mm_main_role ==  1 ) { addToReportRegistration( id_registration, "" + id_source, 281, "" ); }   // WA 281
                        else if( mm_main_role ==  4 ) { addToReportRegistration( id_registration, "" + id_source, 282, "" ); }   // WA 282
                        else if( mm_main_role ==  7 ) { addToReportRegistration( id_registration, "" + id_source, 283, "" ); }   // WA 283
                        else if( mm_main_role == 10 ) { addToReportRegistration( id_registration, "" + id_source, 284, "" ); }   // WA 284
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
    public int roundDownAge( int year, int month, int week, int day )
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
    } // roundDownAge


    /**
     * @param source
     */
    public void standardRegistrationDate( boolean debug, String source )
    {
        long threadId = Thread.currentThread().getId();
        
        int count = 0;
        int stepstate = count_step;
        int nInvalidRegDates = 0;
        int nTooManyHyphens = 0;

        try
        {
            String query_r = "SELECT id_registration, registration_maintype, registration_date, registration_day, registration_month, registration_year ";
            query_r += "FROM registration_o WHERE id_source = " + source;

            ResultSet rs_r = dbconOriginal.runQueryWithResult( query_r );

            while( rs_r.next() )
            {
                count++;
                if( count == stepstate ) {
                    showMessage( count + "", true, true );
                    stepstate += count_step;
                }

                int id_registration       = rs_r.getInt( "id_registration" );
                int registration_maintype = rs_r.getInt( "registration_maintype" );
                String registration_date  = rs_r.getString( "registration_date" );

                int regist_day      = rs_r.getInt( "registration_day" );
                int regist_month    = rs_r.getInt( "registration_month" );
                int regist_year     = rs_r.getInt( "registration_year" );

                //if( id_registration == 7117706 ) { debug = true; }
                //else { debug = false; continue; }

                if( debug )
                {
                    System.out.println( "id_registration: "    + id_registration );
                    System.out.println( "registration_date: "  + registration_date );
                    System.out.println( "registration_day: "   + regist_day );
                    System.out.println( "registration_month: " + regist_month );
                    System.out.println( "registration_year: "  + regist_year );
                }

                // valid date string: dd-mm-yyyy
                // exactly 2 hyphens should occur, but substrings like '-1', '-2', '-3', and '-4' are used to flag
                // e.g. unreadable date strings
                int nhyphens = 0;
                if( registration_date != null  ) {
                    for( int i = 0; i < registration_date.length(); i++ ) {
                        if( registration_date.charAt(i) == '-' ) { nhyphens++; }
                    }
                    if( nhyphens > 2 ) { nTooManyHyphens++; }
                }

                // The initial registration_date is copied from the a2a field source.literaldate, which sometimes is NULL.
                // Then we will try to use the source fields day-month-year,
                // and all 3 data components must be numeric and > 0.
                // Otherwise we try to replace the date by the event date.

                // date object from links_original date string
                // -1- FIRST dymd
                DateYearMonthDaySet dymd = LinksSpecific.divideCheckDate( registration_date );
                if( debug ) { System.out.println( "dymd.isValidDate(): " + dymd.isValidDate() ); }

                // date object from links_original date components
                String regist_comp = String.format( "%02d-%02d-%04d", regist_day, regist_month, regist_year );
                DateYearMonthDaySet dymd_comp = LinksSpecific.divideCheckDate( regist_comp );
                if( debug ) { System.out.println( "dymd_comp.isValidDate(): " + dymd_comp.isValidDate() ); }

                // compare the string date and the components date
                boolean use_event_date = false;

                // divideCheckDate() fucks up with additional hyphens!
                if( nhyphens == 2 && dymd.isValidDate() )   // valid registration_date
                {
                    if( dymd_comp.isValidDate() )           // valid components date
                    {
                        if( ! ( dymd.getDay() == regist_day && dymd.getMonth() == regist_month && dymd.getYear() == regist_year ) )
                        {
                            // both valid but unequal; components from dymd will be used
                            String unequal_str = String.format( "[ %s not equal %d %d %d ]", registration_date, regist_day, regist_month, regist_year );
                            addToReportRegistration( id_registration, source + "", 206, unequal_str );
                        }    // EC 206
                    }
                    else                                    // invalid components date + valid registration_date
                    {
                        // components from dymd will be used, extracted after the  "if( use_event_date )" loop
                        //if( ! ( regist_day == 0 && regist_month == 0 && regist_year == 0 ) )  // only report if components are not all 3 empty
                        //{ addToReportRegistration( id_registration, source + "", 204, dymd_comp.getReports() ); }  // EC 204
                    }
                }
                else                                        // invalid registration_date
                {
                    if( dymd_comp.isValidDate() )           // valid components date + invalid registration_date
                    {
                        //if( registration_date != "" )       // only report if registration_date is not empty
                        //{ addToReportRegistration( id_registration, source + "", 203, dymd.getReports() ); }    // EC 203
                        // -2- REPLACE dymd with comp values
                        dymd = dymd_comp;                   // use components date
                        registration_date = String.format( "%02d-%02d-%04d", regist_day, regist_month, regist_year );
                    }
                    else { use_event_date = true; }         // both invalid, try to use event_date
                }

                // replace registration date with event date
                int registration_flag = 0;

                if( use_event_date )
                {
                    if( debug ) { System.out.println( "try to use event_date" ); }
                    // try to replace invalid registration date with birth-/marriage-/death- date
                    nInvalidRegDates++;

                    registration_flag = 1;

                    if( nhyphens > 2 ) {
                        String msg = String.format( "Thread id %02d; id_registration: %d, registration_date: %s", threadId, id_registration, registration_date );
                        System.out.println( msg );
                    }

                    String query_p = "SELECT registration_maintype , birth_date , mar_date , death_date FROM person_c WHERE id_registration = " + id_registration;
                    ResultSet rs_p = dbconCleaned.runQueryWithResult( query_p );

                    DateYearMonthDaySet dymd_event = null;
                    while( rs_p.next() )
                    {
                        // try to use the event date
                        String event_date = "";
                             if( registration_maintype == 1 ) { event_date = rs_p.getString( "birth_date" ); }
                        else if( registration_maintype == 2 ) { event_date = rs_p.getString( "mar_date" ); }
                        else if( registration_maintype == 3 ) { event_date = rs_p.getString( "death_date" ); }

                        dymd_event = LinksSpecific.divideCheckDate( event_date );

                        if( dymd_event.isValidDate() ) {
                            // we have a valid event date; skip the remaining reg persons
                            if( debug ) { System.out.println( "valid event_date: " + event_date ); }
                            registration_date = event_date;
                            break;
                        }
                        else
                        if( debug ) { System.out.println( "invalid event_date: " + event_date ); }
                    }

                    if( dymd_event != null && dymd_event.isValidDate() )
                    {
                        // -3- REPLACE registration_date with event_date
                        dymd = dymd_event;
                        int event_day   = dymd.getDay();
                        int event_month = dymd.getMonth();
                        int event_year  = dymd.getYear();
                        registration_date = String.format( "%02d-%02d-%04d", event_day, event_month, event_year );

                        //if( debug ) { System.out.println( "use event_date: " + registration_date ); }
                    }
                    else
                    {
                        if( debug ) { System.out.println( "invalid event_date; use/set registration components " ); }

                        // (invalid) event_date not used; continue with registration_date components
                        // Notice: (HSN date) components may be negative if registration_date contains those

                        // -4- conditionally REPLACE dymd components of registration_date
                        int day   = dymd_comp.getDay();
                        int month = dymd_comp.getMonth();
                        int year  = dymd_comp.getYear();

                        if( debug ) { System.out.println( String.format( "date components (initial) %02d-%02d-%04d", day, month, year ) ); }

                        if( day <= 0 && month <= 0 && year > 0 )
                        {
                            // KM: for HSN data with negative date components in day and/or month:
                            // birth    -> registration_date = 01-01-yyyy
                            // marriage -> registration_date = 01-07-yyyy
                            // death    -> registration_date = 31-12-yyyy

                            addToReportRegistration( id_registration, source, 202, dymd.getReports() ); // EC 202

                            registration_flag = 2;

                            if( registration_maintype == 1 ) {
                                day   = 1;
                                month = 1;
                            }
                            else if( registration_maintype == 2 ) {
                                day   = 1;
                                month = 7;
                            }
                            else if( registration_maintype == 3 ) {
                                day   = 31;
                                month = 12;
                            }
                        }
                        else
                        {
                            addToReportRegistration( id_registration, source, 201, dymd.getReports() ); // EC 201

                            if( month == 2 && day > 28 ) {
                                day   = 1;
                                month = 3;
                            }

                            if( day > 30 && ( month == 4 || month == 6 || month == 9 || month == 11 ) ) {
                                day    = 1;
                                month += 1;
                            }

                            if( day   > 31 ) { day   = 31; }
                            if( month > 12 ) { month = 12; }

                            // HSN data (id_source = 10) may contain negative data components, that have special meaning,
                            // but we do not want invalid date strings in links_cleaned
                            if (day   < 1 ) { day   = 1; }
                            if( month < 1 ) { month = 1; }
                        }

                        // final check
                        if( debug ) { System.out.println( String.format( "date components (changed) %02d-%02d-%04d", day, month, year ) ); }

                        registration_date = String.format( "%02d-%02d-%04d", day, month, year );
                        if( debug ) { System.out.println( "registration_date from components: " + registration_date ); }

                        dymd = LinksSpecific.divideCheckDate( registration_date );
                        if( debug ) { System.out.println( "dymd.isValidDate(): " + dymd.isValidDate() ); }
                    }
                }

                String query = "";
                if( dymd.isValidDate() )
                {
                    regist_day   = dymd.getDay();
                    regist_month = dymd.getMonth();
                    regist_year  = dymd.getYear();
                }
                else    // could not get a valid registration_date; avoid confusing garbage
                {
                    registration_date  = "";
                    regist_day   = 0;
                    regist_month = 0;
                    regist_year  = 0;
                    addToReportRegistration( id_registration, source, 205, dymd.getReports() ); // EC 205
                }

                query = "UPDATE registration_c SET "
                    + "registration_c.registration_date = '" + registration_date  + "' , "
                    + "registration_c.registration_day = "   + regist_day   + " , "
                    + "registration_c.registration_month = " + regist_month + " , "
                    + "registration_c.registration_year = "  + regist_year  + " ";

                if( use_event_date && registration_flag != 0 )  // invalid registration date: set registration_flag to 1 or 2
                { query += ", registration_c.registration_flag = " + registration_flag + " "; }

                query += "WHERE registration_c.id_registration = "  + id_registration;
                if( debug ) { System.out.println( "query: " + query ); }
                dbconCleaned.runQuery( query );
            }

            if( nInvalidRegDates > 0 ) 
            { showMessage( String.format( "Thread id %02d; Number of registrations without a (valid) reg date: %d", threadId, nInvalidRegDates ), false, true ); }
            
            if( nTooManyHyphens  > 0 ) 
            { showMessage( String.format( "Thread id %02d; Number of registrations with too many hyphens in reg date: %s", threadId, nTooManyHyphens ), false, true ); }
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
        long threadId = Thread.currentThread().getId();

        int count = 0;
        int count_empty = 0;
        int count_invalid = 0;
        int stepstate = count_step;

        try
        {
            String startQuery = "SELECT id_person , id_source , " + type + "_date , "
                + type + "_day , " + type + "_month , " + type + "_year "
                + " FROM person_o WHERE id_source = " + source;

            ResultSet rs = dbconOriginal.runQueryWithResult( startQuery );

            while( rs.next() )
            {
                // GUI info
                count++;
                if( count == stepstate ) {
                    showMessage( count + "", true, true );
                    stepstate += count_step;
                }

                String date_str = rs.getString( type + "_date" );

                if( date_str == null || date_str.isEmpty() )    // try to repair date from components
                {
                    int year = rs.getInt( type + "_year" );
                    if( year == 0 ) {
                        count_empty++;
                        continue;           // cannot be repaired
                    }

                    // create new date from components
                    int day   = rs.getInt( type + "_day" );
                    int month = rs.getInt( type + "_month" );
                    date_str  = String.format( "%02d-%02d-%04d", day, month, year );
                }

                int id_person = rs.getInt( "id_person" );
                int id_source = rs.getInt( "id_source" );

                DateYearMonthDaySet dymd = LinksSpecific.divideCheckDate( date_str );

                if( dymd.isValidDate() )
                {
                    int day   = dymd.getDay();
                    int month = dymd.getMonth();
                    int year  = dymd.getYear();
                    date_str  = String.format( "%02d-%02d-%04d", day, month, year );

                    String query = ""
                        + "UPDATE person_c SET "
                        + "person_c." + type + "_date = '" + date_str + "' , "
                        + "person_c." + type + "_day = " + day + " , "
                        + "person_c." + type + "_month = " + month + " , "
                        + "person_c." + type + "_year = " + year + " , "
                        + "person_c." + type + "_date_valid = 1 "
                        + "WHERE person_c.id_person = " + id_person;

                    dbconCleaned.runQuery( query );
                }
                else
                {
                    String query = ""
                        + "UPDATE person_c SET "
                        + "person_c." + type + "_date_valid = 0 "
                        + "WHERE person_c.id_person = " + id_person;

                    dbconCleaned.runQuery( query );

                    count_invalid++;

                    int errno = 0;
                    if(       type.equals( "birth" ) ) { errno = 211; }
                    else if ( type.equals( "mar" ) )   { errno = 221; }
                    else if ( type.equals( "death" ) ) { errno = 231; }

                    addToReportPerson( id_person, id_source + "", errno, dymd.getReports() );   // EC 211 / 221 / 231
                }
            }

            String msg = String.format( "Thread id %02d; Number of %s records: %d, empty dates: %d, invalid dates: %d", threadId, type, count, count_empty, count_invalid );
            showMessage( msg, false, true );
            rs = null;
        }
        catch( Exception ex ) {
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
    private String addTimeToDate( int year, int month, int day, TimeType tt, int timeAmount )
    {
        Calendar c1 = Calendar.getInstance();       // new calendar instance

        c1.set( year, month, day );                 // set(int year, int month, int date)

        // Check of time type
             if( tt == tt.DAY )   { c1.add( Calendar.DAY_OF_MONTH,  timeAmount ); }
        else if( tt == tt.WEEK )  { c1.add( Calendar.WEEK_OF_MONTH, timeAmount ); }
        else if( tt == tt.MONTH ) { c1.add( Calendar.MONTH,         timeAmount ); }
        else if( tt == tt.YEAR )  { c1.add( Calendar.YEAR,          timeAmount ); }

        //String am = "" + c1.get( Calendar.DATE ) + "-" + c1.get( Calendar.MONTH ) + "-" + c1.get( Calendar.YEAR );
        // our standard date string: dd-mm-yyyy
        String am = String.format( "%02d-%02d-%04d", c1.get( Calendar.DATE ), c1.get( Calendar.MONTH ), c1.get( Calendar.YEAR ) );

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
     * @param debug
     * @param source
     */
    public void flagBirthDate( boolean debug, String source )
    {
        // birth_date_flag is not used elsewhere in the cleaning.
        // In standardDate() birth_date_valid is set to either 1 (valid birth date) or 0 (invalid birth date)

        String[] queries =
        {
            "UPDATE person_c, registration_c"
                + " SET"
                + " person_c.birth_date_flag = 0"
                + " WHERE person_c.id_source = " + source
                + " AND person_c.registration_maintype = 1"
                + " AND person_c.birth_date_valid = 0"
                + " AND person_c.role = 1"
                + " AND registration_c.registration_flag <> 0"
                + " AND person_c.id_registration = registration_c.id_registration;",

            "UPDATE person_c, registration_c"
                + " SET"
                + " person_c.birth_date_flag = 1"
                + " WHERE person_c.id_source = " + source
                + " AND person_c.registration_maintype = 1"
                + " AND person_c.birth_date_valid = 1"
                + " AND person_c.role = 1"
                + " AND person_c.id_registration = registration_c.id_registration;",

            "UPDATE person_c, registration_c"
                + " SET"
                + " person_c.birth_date_flag  = 2,"
                + " person_c.birth_date_valid = 1,"
                + " person_c.birth_date  = registration_c.registration_date,"
                + " person_c.birth_year  = registration_c.registration_year,"
                + " person_c.birth_month = registration_c.registration_month,"
                + " person_c.birth_day   = registration_c.registration_day"
                + " WHERE person_c.id_source = " + source
                + " AND person_c.registration_maintype = 1"
                + " AND ( person_c.birth_date IS NULL OR person_c.birth_date = '' )"
                + " AND person_c.role = 1"
                + " AND registration_c.registration_flag = 0"
                + " AND person_c.id_registration = registration_c.id_registration;",

            "UPDATE person_c, registration_c"
                + " SET"
                + " person_c.birth_date_flag  = 3,"
                + " person_c.birth_date_valid = 1,"
                + " person_c.birth_date  = registration_c.registration_date,"
                + " person_c.birth_year  = registration_c.registration_year,"
                + " person_c.birth_month = registration_c.registration_month,"
                + " person_c.birth_day   = registration_c.registration_day"
                + " WHERE person_c.id_source = " + source
                + " AND person_c.registration_maintype = 1"
                + " AND person_c.birth_date_valid = 0"
                + " AND NOT ( person_c.birth_date IS NULL OR person_c.birth_date = '' )"
                + " AND person_c.role = 1"
                + " AND registration_c.registration_flag = 0"
                + " AND person_c.id_registration = registration_c.id_registration;"
        };

        for( String query : queries )
        {
            try { dbconCleaned.runQuery( query ); }
            catch( Exception ex ) {
                showMessage( query, false, true );
                showMessage( "Exception while flagging Birth date: " + ex.getMessage(), false, true );
                ex.printStackTrace( new PrintStream( System.out ) );
            }
        }
    } // flagBirthDate


    /**
     * @param debug
     * @param source
     */
    public void flagMarriageDate( boolean debug, String source )
    {
        // mar_date_flag is not used elsewhere in the cleaning.
        // // In standardDate() mar_date_valid is set to either 1 (valid mar date) or 0 (invalid mar date)

        String[] queries =
        {
            "UPDATE person_c, registration_c"
                + " SET"
                + " person_c.mar_date_flag = 0"
                + " WHERE person_c.id_source = " + source
                + " AND person_c.registration_maintype = 2"
                + " AND person_c.mar_date_valid = 0"
                + " AND ( ( person_c.role = 4 ) || ( person_c.role = 7 ) )"
                + " AND registration_c.registration_flag <> 0"
                + " AND person_c.id_registration = registration_c.id_registration;",

            "UPDATE person_c, registration_c"
                + " SET"
                + " person_c.mar_date_flag = 1"
                + " WHERE person_c.id_source = " + source
                + " AND person_c.registration_maintype = 2"
                + " AND person_c.mar_date_valid = 1"
                + " AND ( ( person_c.role = 4 ) || ( person_c.role = 7 ) )"
                + " AND person_c.id_registration = registration_c.id_registration;",

            "UPDATE person_c, registration_c"
                + " SET"
                + " person_c.mar_date_flag  = 2,"
                + " person_c.mar_date_valid = 1,"
                + " person_c.mar_date  = registration_c.registration_date,"
                + " person_c.mar_year  = registration_c.registration_year,"
                + " person_c.mar_month = registration_c.registration_month,"
                + " person_c.mar_day   = registration_c.registration_day"
                + " WHERE person_c.id_source = " + source
                + " AND person_c.registration_maintype = 2"
                + " AND ( person_c.mar_date IS NULL OR person_c.mar_date = '' )"
                + " AND ( ( person_c.role = 4 ) || ( person_c.role = 7 ) )"
                + " AND registration_c.registration_flag = 0"
                + " AND person_c.id_registration = registration_c.id_registration;",

            "UPDATE person_c, registration_c"
                + " SET"
                + " person_c.mar_date_flag  = 3,"
                + " person_c.mar_date_valid = 1,"
                + " person_c.mar_date  = registration_c.registration_date,"
                + " person_c.mar_year  = registration_c.registration_year,"
                + " person_c.mar_month = registration_c.registration_month,"
                + " person_c.mar_day   = registration_c.registration_day"
                + " WHERE person_c.id_source = " + source
                + " AND person_c.registration_maintype = 2"
                + " AND person_c.mar_date_valid = 0"
                + " AND NOT ( person_c.mar_date IS NULL OR person_c.mar_date = '' )"
                + " AND ( ( person_c.role = 4 ) || ( person_c.role = 7 ) )"
                + " AND registration_c.registration_flag = 0"
                + " AND person_c.id_registration = registration_c.id_registration;"
        };

        for( String query : queries )
        {
            try { dbconCleaned.runQuery( query ); }
            catch( Exception ex ) {
                showMessage( query, false, true );
                showMessage( "Exception while flagging Marriage date: " + ex.getMessage(), false, true );
                ex.printStackTrace( new PrintStream( System.out ) );
            }
        }
    } // flagMarriageDate


    /**
     * @param debug
     * @param source
     */
    public void flagDeathDate( boolean debug, String source )
    {
        // death_date_flag is not used elsewhere in the cleaning.
        // In standardDate() death_date_valid is set to either 1 (valid death date) or 0 (invalid death date)

        String[] queries =
        {
            "UPDATE person_c, registration_c"
                + " SET"
                + " person_c.death_date_flag = 0"
                + " WHERE person_c.id_source = " + source
                + " AND person_c.registration_maintype = 3"
                + " AND person_c.death_date_valid = 0"
                + " AND person_c.role = 10"
                + " AND registration_c.registration_flag <> 0"
                + " AND person_c.id_registration = registration_c.id_registration;",

            "UPDATE person_c, registration_c "
                + " SET"
                + " person_c.death_date_flag = 1"
                + " WHERE person_c.id_source = " + source
                + " AND person_c.registration_maintype = 3"
                + " AND person_c.death_date_valid = 1"
                + " AND person_c.role = 10"
                + " AND person_c.id_registration = registration_c.id_registration;",

            "UPDATE person_c, registration_c"
                + " SET"
                + " person_c.death_date_flag  = 2,"
                + " person_c.death_date_valid = 1,"
                + " person_c.death_date  = registration_c.registration_date,"
                + " person_c.death_year  = registration_c.registration_year,"
                + " person_c.death_month = registration_c.registration_month,"
                + " person_c.death_day   = registration_c.registration_day"
                + " WHERE person_c.id_source = " + source
                + " AND person_c.registration_maintype = 3"
                + " AND ( person_c.death_date IS NULL OR person_c.death_date = '' )"
                + " AND person_c.role = 10"
                + " AND registration_c.registration_flag = 0"
                + " AND person_c.id_registration = registration_c.id_registration;",

            "UPDATE person_c, registration_c "
                + " SET "
                + " person_c.death_date_flag  = 3,"
                + " person_c.death_date_valid = 1,"
                + " person_c.death_date  = registration_c.registration_date,"
                + " person_c.death_year  = registration_c.registration_year,"
                + " person_c.death_month = registration_c.registration_month,"
                + " person_c.death_day   = registration_c.registration_day"
                + " WHERE person_c.id_source = " + source
                + " AND person_c.registration_maintype = 3"
                + " AND person_c.death_date_valid = 0"
                + " AND NOT ( person_c.death_date IS NULL OR person_c.death_date = '' )"
                + " AND person_c.role = 10"
                + " AND registration_c.registration_flag = 0"
                + " AND person_c.id_registration = registration_c.id_registration;"
        };

        for( String query : queries )
        {
            try { dbconCleaned.runQuery( query ); }
            catch( Exception ex ) {
                showMessage( query, false, true );
                showMessage( "Exception while flagging Death date: " + ex.getMessage(), false, true );
                ex.printStackTrace( new PrintStream( System.out ) );
            }
        }
    } // flagDeathDate


    /*---< Min Max Marriage >-------------------------------------------------*/

    /**
     * Min Max Marriage
     * @param go
     * @throws Exception
     */
    private void doMinMaxMarriage( boolean debug, boolean go, String source, String rmtype ) throws Exception
    {
        long threadId = Thread.currentThread().getId();

        String funcname = String.format( "Thread id %02d; doMinMaxMarriage for source %s", threadId, source );

        if( !go ) {
            if( showskip ) { showMessage( "Skipping " + funcname, false, true ); }
            return;
        }

        long timeStart = System.currentTimeMillis();
        showMessage( funcname + "...", false, true );

        //if( ! multithreaded ) {
            long start = System.currentTimeMillis();
            TableToArrayListMultimap almmMarriageYear = new TableToArrayListMultimap( dbconRefRead, dbconRefWrite, "ref_minmax_marriageyear", "role_A", "role_B" );
            showTimingMessage( String.format( "Thread id %02d; Loaded MarriageYear reference table", threadId ), start );
        //}

        minMaxMarriageYear( debug, almmMarriageYear, source );

        //if( ! multithreaded ) {
            almmMarriageYear.free();
            showMessage( String.format( "Thread id %02d; Freed almmMarriageYear", threadId ), false, true );
        //}

        elapsedShowMessage( funcname, timeStart, System.currentTimeMillis() );
        showMessage_nl();
    } // doMinMaxMarriage


     /**
     * @param debug
     * @param source
     * @throws Exception
     */
    private void minMaxMarriageYear( boolean debug, TableToArrayListMultimap almmMarriageYear, String source )
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
                    if( debug ) { showMessage( "standardMinMaxMarriageYear() id_person: " + id_personA + " has multiple roleB matches", false, true ); }
                    addToReportPerson( id_personA, source + "", 107, "" );
                    continue;
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
    private void doPartsToFullDate( boolean debug, boolean go, String source, String rmtype ) throws Exception
    {
        long threadId = Thread.currentThread().getId();

        String funcname = String.format( "Thread id %02d; doPartsToFullDate for source %s", threadId, source );

        if( !go ) {
            if( showskip ) { showMessage( "Skipping " + funcname, false, true ); }
            return;
        }

        long timeStart = System.currentTimeMillis();
        showMessage( funcname + "...", false, true );

        String msg = String.format( "Thread id %02d; Processing partsToFullDate for source: %s...", threadId, source );
        showMessage( msg, false, true );
        partsToFullDate(source);

        elapsedShowMessage( funcname, timeStart, System.currentTimeMillis() );
        showMessage_nl();
    } // doPartsToFullDate


    private void partsToFullDate( String source )
    {
        /*
        Notice: the date components from person_c are INTs.
        Below they are CONCATenated directly, implying that no leading zeros are inserted in the output string,
        so we do NOT comply to the format '%02d-%02d-%04d' as dd-mm-yyyy.

        If we would additionally use the the MySQL STR_TO_DATE as:
        DATE = STR_TO_DATE( CONCAT( d, '-', m , '-', y), '%d-%m-%Y' );
        then we get our wanted leading zeros, but we also get by definition the DATE format as YYY-MM-DD.
        Because the format '%d-%m-%Y' is only used for the input; the output is fixed to YYY-MM-DD.

        We actually like YYY-MM-DD, but then we should convert ALL our date strings in links_cleaned to YYY-MM-DD.
        Also notice that STR_TO_DATE accepts 0's for components, but acts strange with 0 year:
        SELECT STR_TO_DATE( CONCAT( 0, '-', 0, '-', 0 ), '%d-%m-%Y' );
        +--------------------------------------------------------+
        | STR_TO_DATE( CONCAT( 0, '-', 0, '-', 0 ), '%d-%m-%Y' ) |
        +--------------------------------------------------------+
        | 2000-00-00                                             |
        +--------------------------------------------------------+
        The documented supported range of DATE is '1000-01-01' to '9999-12-31'.
        We should always check for 0 years, because then we have no valid date anyway
        The MySQL date/time functions are not rock solid, we maybe be we should avoid them, and use Java Joda-time.
        */

        String query = "UPDATE links_cleaned.person_c SET "
                + "links_cleaned.person_c.birth_date_min  = CONCAT( links_cleaned.person_c.birth_day_min , '-' , links_cleaned.person_c.birth_month_min , '-' , links_cleaned.person_c.birth_year_min ) ,"
                + "links_cleaned.person_c.mar_date_min    = CONCAT( links_cleaned.person_c.mar_day_min ,   '-' , links_cleaned.person_c.mar_month_min ,   '-' , links_cleaned.person_c.mar_year_min ) ,"
                + "links_cleaned.person_c.death_date_min  = CONCAT( links_cleaned.person_c.death_day_min , '-' , links_cleaned.person_c.death_month_min , '-' , links_cleaned.person_c.death_year_min ) ,"
                + "links_cleaned.person_c.birth_date_max  = CONCAT( links_cleaned.person_c.birth_day_max , '-' , links_cleaned.person_c.birth_month_max , '-' , links_cleaned.person_c.birth_year_max ) ,"
                + "links_cleaned.person_c.mar_date_max    = CONCAT( links_cleaned.person_c.mar_day_max ,   '-' , links_cleaned.person_c.mar_month_max ,   '-' , links_cleaned.person_c.mar_year_max ) ,"
                + "links_cleaned.person_c.death_date_max  = CONCAT( links_cleaned.person_c.death_day_max , '-' , links_cleaned.person_c.death_month_max , '-' , links_cleaned.person_c.death_year_max ) "
                + "WHERE id_source = " + source;

        try { dbconCleaned.runQuery( query ); }
        catch( Exception ex ) {
            showMessage( "Exception while Creating full dates from parts: " + ex.getMessage(), false, true );
            ex.printStackTrace( new PrintStream( System.out ) );
        }
    } // partsToFullDate


    /*---< Days Since Begin >-------------------------------------------------*/

    /**
     * Days since begin
     * @param go
     * @throws Exception
     */
    private void doDaysSinceBegin( boolean debug, boolean go, String source, String rmtype ) throws Exception
    {
        long threadId = Thread.currentThread().getId();

        String funcname = String.format( "Thread id %02d; doDaysSinceBegin for source %s", threadId, source );

        if( !go ) {
            if( showskip ) { showMessage( "Skipping " + funcname, false, true ); }
            return;
        }

        long timeStart = System.currentTimeMillis();
        showMessage( funcname + "...", false, true );

        String msg = String.format( "Thread id %02d; Processing daysSinceBegin for source: %s...", threadId, source );
        showMessage( msg, false, true );
        daysSinceBegin( debug, source );

        elapsedShowMessage( funcname, timeStart, System.currentTimeMillis() );
        showMessage_nl();
    } // doDaysSinceBegin


    private void daysSinceBegin( boolean debug, String source )
    {
        long threadId = Thread.currentThread().getId();

        String qRclean  = "UPDATE links_cleaned.registration_c SET "
            + "registration_date = NULL "
            + "WHERE ( registration_date = '00-00-0000' OR registration_date = '0000-00-00' ) "
            + "AND id_source = " + source;

        if( debug ) { showMessage( qRclean, false, true ); }
        else {
            showMessage( String.format( "Thread id %02d; reg dates '00-00-0000' -> NULL", threadId ), false, true );
            showMessage( String.format( "Thread id %02d; reg dates '0000-00-00' -> NULL", threadId ), false, true );
        }

        try { dbconCleaned.runQuery( qRclean ); }
        catch( Exception ex ) {
            showMessage( "Exception in daysSinceBegin(): " + ex.getMessage(), false, true );
            ex.printStackTrace( new PrintStream( System.out ) );
        }

        String queryP1 = "UPDATE IGNORE person_c SET birth_min_days = DATEDIFF( DATE_FORMAT( STR_TO_DATE( birth_date_min, '%d-%m-%Y' ), '%Y-%m-%d' ) , '1-1-1' ) WHERE birth_date_min IS NOT NULL AND birth_date_min NOT LIKE '0-%' AND birth_date_min NOT LIKE '%-0-%' AND birth_date_min NOT LIKE '%-0' ";
        String queryP2 = "UPDATE IGNORE person_c SET birth_max_days = DATEDIFF( DATE_FORMAT( STR_TO_DATE( birth_date_max, '%d-%m-%Y' ), '%Y-%m-%d' ) , '1-1-1' ) WHERE birth_date_max IS NOT NULL AND birth_date_max NOT LIKE '0-%' AND birth_date_max NOT LIKE '%-0-%' AND birth_date_max NOT LIKE '%-0' ";
        String queryP3 = "UPDATE IGNORE person_c SET mar_min_days   = DATEDIFF( DATE_FORMAT( STR_TO_DATE( mar_date_min,   '%d-%m-%Y' ), '%Y-%m-%d' ) , '1-1-1' ) WHERE mar_date_min   IS NOT NULL AND mar_date_min   NOT LIKE '0-%' AND mar_date_min   NOT LIKE '%-0-%' AND mar_date_min   NOT LIKE '%-0' ";
        String queryP4 = "UPDATE IGNORE person_c SET mar_max_days   = DATEDIFF( DATE_FORMAT( STR_TO_DATE( mar_date_max,   '%d-%m-%Y' ), '%Y-%m-%d' ) , '1-1-1' ) WHERE mar_date_max   IS NOT NULL AND mar_date_max   NOT LIKE '0-%' AND mar_date_max   NOT LIKE '%-0-%' AND mar_date_max   NOT LIKE '%-0' ";
        String queryP5 = "UPDATE IGNORE person_c SET death_min_days = DATEDIFF( DATE_FORMAT( STR_TO_DATE( death_date_min, '%d-%m-%Y' ), '%Y-%m-%d' ) , '1-1-1' ) WHERE death_date_min IS NOT NULL AND death_date_min NOT LIKE '0-%' AND death_date_min NOT LIKE '%-0-%' AND death_date_min NOT LIKE '%-0' ";
        String queryP6 = "UPDATE IGNORE person_c SET death_max_days = DATEDIFF( DATE_FORMAT( STR_TO_DATE( death_date_max, '%d-%m-%Y' ), '%Y-%m-%d' ) , '1-1-1' ) WHERE death_date_max IS NOT NULL AND death_date_max NOT LIKE '0-%' AND death_date_max NOT LIKE '%-0-%' AND death_date_max NOT LIKE '%-0' ";

        // The min/max dates in person_c are not normalized to '%02d-%02d-%04d'; there are no leading zero's.
        // See partsToFullDate()

        queryP1 += "AND id_source = " + source;
        queryP2 += "AND id_source = " + source;
        queryP3 += "AND id_source = " + source;
        queryP4 += "AND id_source = " + source;
        queryP5 += "AND id_source = " + source;
        queryP6 += "AND id_source = " + source;

        // registration_date strings '01-01-0000' give a negative DATEDIFF, which gives an exception
        // because the column links_cleaned.registration_days is defined as unsigned.
        // We skip such negative results.
        // 22-Jan-2016: we now assume that the registration_date of registration_c is formatted as '%02d-%02d-%04d'
        // 15-Mar-2016: check for STR_TO_DATE result NOT NULL
        /*
        String queryR = "UPDATE registration_c SET "
            + "registration_days = DATEDIFF( DATE_FORMAT( STR_TO_DATE( registration_date, '%d-%m-%Y' ), '%Y-%m-%d' ) , '1-1-1' ) "
            + "WHERE registration_date IS NOT NULL "
            + "AND registration_date NOT LIKE '00-%' "
            + "AND registration_date NOT LIKE '0000-%' "
            + "AND registration_date NOT LIKE '%-00-%' "
            + "AND registration_date NOT LIKE '%-0000' "
            + "AND STR_TO_DATE( registration_date, '%d-%m-%Y' ) IS NOT NULL "
            + "AND STR_TO_DATE( registration_date, '%d-%m-%Y' ) <> '0000-00-00' "
            + "AND DATEDIFF( DATE_FORMAT( STR_TO_DATE( registration_date, '%d-%m-%Y' ), '%Y-%m-%d' ) , '1-1-1' ) > 0 "
            + "AND id_source = " + source;
        */

        try
        {
            if( debug ) { showMessage( queryP1, false, true ); }
            else { showMessage( String.format( "Thread id %02d; 1-of-7: birth_date_min", threadId ), false, true ); }
            dbconCleaned.runQuery( queryP1 );

            if( debug ) { showMessage( queryP2, false, true ); }
            else { showMessage( String.format( "Thread id %02d; 2-of-7: birth_date_max", threadId ), false, true ); }
            dbconCleaned.runQuery( queryP2 );

            if( debug ) { showMessage( queryP3, false, true ); }
            else { showMessage( String.format( "Thread id %02d; 3-of-7: mar_date_min", threadId ), false, true ); }
            dbconCleaned.runQuery( queryP3 );

            if( debug ) { showMessage( queryP4, false, true ); }
            else { showMessage( String.format( "Thread id %02d; 4-of-7: mar_date_max", threadId ), false, true ); }
            dbconCleaned.runQuery( queryP4 );

            if( debug ) { showMessage( queryP5, false, true ); }
            else { showMessage( String.format( "Thread id %02d; 5-of-7: death_date_min", threadId ), false, true ); }
            dbconCleaned.runQuery( queryP5 );

            if( debug ) { showMessage( queryP6, false, true ); }
            else { showMessage( String.format( "Thread id %02d; 6-of-7: death_date_max", threadId ), false, true ); }
            dbconCleaned.runQuery( queryP6 );

            /*
            // sometimes exceptions for invalid dates...
            if( debug ) { showMessage( queryR, false, true ); }
            else { showMessage( String.format( "Thread id %02d; 7-of-7: registration_days", threadId ), false, true ); }
            int rowsAffected = dbconCleaned.runQueryUpdate( queryR );
            showMessage( "registration_days rows affected: " + rowsAffected, false, true );
            */
        }
        catch( Exception ex ) {
            showMessage( "Exception in daysSinceBegin(): " + ex.getMessage(), false, true );
            ex.printStackTrace( new PrintStream( System.out ) );
        }


        try
        {
            showMessage( String.format( "Thread id %02d; 7-of-7: registration_days", threadId ), false, true );

            String queryS = "SELECT id_registration, registration_date FROM registration_c WHERE id_source = " + source;

            ResultSet rs_s = dbconCleaned.runQueryWithResult( queryS );

            int count = 0;
            int stepstate = count_step;
            while( rs_s.next() )
            {
                count++;
                if( count == stepstate ) {
                    showMessage( count + "", true, true );
                    stepstate += count_step;
                }

                int id_registration = rs_s.getInt( "id_registration" );
                String registration_date  = rs_s.getString( "registration_date" );
                //System.out.println( "count: " + count + ", id_registration: " + id_registration + ", registration_date: " + registration_date );

                String queryU = "UPDATE registration_c SET "
                    + "registration_days = DATEDIFF( DATE_FORMAT( STR_TO_DATE( registration_date, '%d-%m-%Y' ), '%Y-%m-%d' ) , '1-1-1' ) "
                    + "WHERE registration_date IS NOT NULL "
                    + "AND registration_date NOT LIKE '00-%' "
                    + "AND registration_date NOT LIKE '0000-%' "
                    + "AND registration_date NOT LIKE '%-00-%' "
                    + "AND registration_date NOT LIKE '%-0000' "
                    + "AND STR_TO_DATE( registration_date, '%d-%m-%Y' ) IS NOT NULL "
                    + "AND STR_TO_DATE( registration_date, '%d-%m-%Y' ) <> '0000-00-00' "
                    + "AND DATEDIFF( DATE_FORMAT( STR_TO_DATE( registration_date, '%d-%m-%Y' ), '%Y-%m-%d' ) , '1-1-1' ) > 0 "
                    + "AND id_source = " + source + " "
                    + "AND id_registration = " + id_registration;

                 try {
                    int rowsAffected = dbconCleaned.runQueryUpdate( queryU );
                    //System.out.println( "registration_days rows affected: " + rowsAffected );
                }
                catch( Exception ex ) {
                    showMessage( "Exception in daysSinceBegin(): " + ex.getMessage(), false, true );
                    ex.printStackTrace( new PrintStream( System.out ) );
                    System.out.println( "count: " + count + ", id_registration: " + id_registration + ", registration_date: " + registration_date );
                }
            }
            //System.out.println( "end count: " + count );
        }
        catch( Exception ex ) {
            showMessage( "Exception in daysSinceBegin(): " + ex.getMessage(), false, true );
            ex.printStackTrace( new PrintStream( System.out ) );
        }

    } // daysSinceBegin



    /*---< Post Tasks >-------------------------------------------------------*/

    /**
     * Post Tasks
     * @param go
     * @throws Exception
     */
    private void doPostTasks( boolean debug, boolean go, String source, String rmtype ) throws Exception
    {
        long threadId = Thread.currentThread().getId();

        String funcname = String.format( "Thread id %02d; doPostTasks for source %s", threadId, source );

        if( !go ) {
            if( showskip ) { showMessage( "Skipping " + funcname, false, true ); }
            return;
        }

        long start = System.currentTimeMillis();
        showMessage( funcname + "...", false, true );

        String msg = String.format( "Thread id %02d; Processing postTasks for source: %s...", threadId, source );
        showMessage( msg, false, true );
        postTasks( debug, source );

        elapsedShowMessage( funcname, start, System.currentTimeMillis() );
        showMessage_nl();


        // The location flag functions below use the "role" variable. Therefore they must be called after
        // doRole() has been run. The calling of these functions has been (temporarily?) moved from doLocations() to
        // here in doPostTasks(). (The function bodies were left in the Locations segment.)
        start = System.currentTimeMillis();
        msg = String.format( "Thread id %02d; flagBirthLocation for source %s...", threadId, source );
        showMessage( msg, false, true );
        flagBirthLocation( debug, source );
        msg = String.format( "Thread id %02d; flagBirthLocation ", threadId );
        showTimingMessage( msg, start );

        start = System.currentTimeMillis();
        msg = String.format( "Thread id %02d; flagMarriageLocation for source %s...", threadId, source );
        showMessage( msg, false, true );
        flagMarriageLocation( debug, source );
        msg = String.format( "Thread id %02d; flagMarriageLocation ", threadId );
        showTimingMessage( msg, start );

        start = System.currentTimeMillis();
        msg = String.format( "Thread id %02d; flagDeathLocation for source %s...", threadId, source );
        showMessage( msg, false, true );
        flagDeathLocation( debug, source );
        msg = String.format( "Thread id %02d; flagDeathLocation ", threadId );
        showTimingMessage( msg, start );
        showMessage_nl();

    } // doPostTasks


    /**
     * @param debug
     * @param source
     * @throws Exception
     */
    private void postTasks( boolean debug, String source )
    throws Exception
    {
        long threadId = Thread.currentThread().getId();

        if( debug ) { showMessage( "postTasks()", false, true ); }
        // Notice:
        // UPDATE IGNORE means "ignore rows that break unique constraints, instead of failing the query".

        String table_male   = "links_temp.male_"   + Long.toString( threadId );
        String table_female = "links_temp.female_" + Long.toString( threadId );

        String[] queries =
        {
            "UPDATE links_cleaned.person_c SET sex = 'f' WHERE role = 2 AND id_source = " + source,
            "UPDATE links_cleaned.person_c SET sex = 'm' WHERE role = 3 AND id_source = " + source,
            "UPDATE links_cleaned.person_c SET sex = 'f' WHERE role = 4 AND id_source = " + source,
            "UPDATE links_cleaned.person_c SET sex = 'f' WHERE role = 5 AND id_source = " + source,
            "UPDATE links_cleaned.person_c SET sex = 'm' WHERE role = 6 AND id_source = " + source,
            "UPDATE links_cleaned.person_c SET sex = 'm' WHERE role = 7 AND id_source = " + source,
            "UPDATE links_cleaned.person_c SET sex = 'f' WHERE role = 8 AND id_source = " + source,
            "UPDATE links_cleaned.person_c SET sex = 'm' WHERE role = 9 AND id_source = " + source,

            "UPDATE links_cleaned.person_c SET sex = 'u' WHERE (sex IS NULL OR (sex <> 'm' AND sex <> 'f')) AND id_source = " + source,


            "DROP TABLE IF EXISTS " + table_male   + ";",
            "DROP TABLE IF EXISTS " + table_female + ";",

            "CREATE TABLE " + table_male   + " ( id_registration INT NOT NULL , PRIMARY KEY (id_registration) );",
            "CREATE TABLE " + table_female + " ( id_registration INT NOT NULL , PRIMARY KEY (id_registration) );",

            "INSERT INTO " + table_male   + " (id_registration) SELECT id_registration FROM links_cleaned.person_c "
                + "WHERE role = 10 AND sex = 'm' AND id_source = " + source,

            "INSERT INTO " + table_female + " (id_registration) SELECT id_registration FROM links_cleaned.person_c "
                + "WHERE role = 10 AND sex = 'f' AND id_source = " + source,

            "UPDATE links_cleaned.person_c, " + table_male   + " SET sex = 'f' "
                + "WHERE " + table_male   + ".id_registration = links_cleaned.person_c.id_registration AND role = 11 "
                + "AND id_source = " + source,

            "UPDATE links_cleaned.person_c, " + table_female + " SET sex = 'm' "
                + "WHERE " + table_female + ".id_registration = links_cleaned.person_c.id_registration AND role = 11 "
                + "AND id_source = " + source,

            "DROP TABLE IF EXISTS " + table_male + ";",
            "DROP TABLE IF EXISTS " + table_female + ";",
        };

        // Execute queries
        int n = 0;
        for( String query : queries )
        {
            n++;

            if( debug ) { System.out.println( query ); }

            String msg = String.format( "Thread id %02d; query %d-of-%d", threadId, n, queries.length );
            showMessage( msg, false, true );
            dbconCleaned.runQuery( query );
        }

    } // postTasks


    /*---< Remove Bad Registrations >-----------------------------------------*/

    /**
     * @param debug
     * @param go
     * @throws Exception
     */
    private void doRemoveEmptyDateRegs( boolean debug, boolean go, String source, String rmtype ) throws Exception
    {
        long threadId = Thread.currentThread().getId();

        String funcname = String.format( "Thread id %02d; doRemoveEmptyDateRegs for source %s", threadId, source );

        if( !go ) {
            if( showskip ) { showMessage( "Skipping " + funcname, false, true ); }
            return;
        }

        long timeStart = System.currentTimeMillis();
        String msg = String.format( "Thread id %02d; Removing Registrations without dates...", threadId );
        showMessage( msg, false, true );

        removeEmptyDateRegs( debug, source );      // needs only registration_c, so do this one first

        elapsedShowMessage( funcname, timeStart, System.currentTimeMillis() );
        showMessage_nl();

    } // doRemoveEmptyDateRegs


    /**
     * Delete registrations with empty registration dates from links_cleaned
     *
     * @param debug
     * @throws Exception
     */
    private void removeEmptyDateRegs( boolean debug, String source )
    throws Exception
    {
        long threadId = Thread.currentThread().getId();
        
        showMessage( String.format( "Thread id %02d; removeEmptyDateRegs for source %s", threadId, source ), false, true );

        String query_r = "SELECT id_registration, id_source, registration_date FROM registration_c  WHERE id_source = " + source;

        if( debug ) { showMessage( query_r, false, true ); }

        int nNoRegDate = 0;
        int stepstate = count_step;

        try
        {
            ResultSet rs_r = dbconCleaned.runQueryWithResult( query_r );

            int row = 0;

            while( rs_r.next() )        // process all results
            {
                row++;
                if( row == stepstate ) {
                    showMessage( "" + row, true, true );
                    stepstate += count_step;
                }

                int id_registration      = rs_r.getInt( "id_registration" );
                int id_source            = rs_r.getInt( "id_source" );
                String registration_date = rs_r.getString( "registration_date" );

                if( registration_date == null   ||
                        registration_date.isEmpty() ||
                        registration_date.equals( "null" ) )
                {
                    nNoRegDate++;

                    // Delete records with this registration
                    String deleteRegist = "DELETE FROM registration_c WHERE id_registration = " + id_registration;
                    String deletePerson = "DELETE FROM person_c WHERE id_registration = " + id_registration;

                    if( debug ) {
                        showMessage( "Deleting id_registration without date: " + id_registration, false, true );
                        showMessage( deleteRegist, false, true );
                        showMessage( deletePerson, false, true );
                    }

                    String id_source_str = Integer.toString( id_source );
                    addToReportRegistration( id_registration, id_source_str, 2, "" );       // warning 2

                    dbconCleaned.runQuery( deleteRegist );
                    dbconCleaned.runQuery( deletePerson );
                }
            }
            
            String msg =  String.format( "Thread id %02d; Number of registrations without date: %d", threadId, nNoRegDate );
            showMessage( msg, false, true );
        }
        catch( Exception ex ) {
            if( ex.getMessage() != "After end of result set" ) {
                System.out.printf("'%s'\n", ex.getMessage());
                ex.printStackTrace( new PrintStream( System.out ) );
            }
        }
    } // removeEmptyDateRegs


    /**
     * @param debug
     * @param go
     * @throws Exception
     */
    private void doRemoveEmptyRoleRegs( boolean debug, boolean go, String source, String rmtype ) throws Exception
    {
        long threadId = Thread.currentThread().getId();

        String funcname = String.format( "Thread id %02d; doRemoveEmptyRoleRegs for source %s", threadId, source );

        if( !go ) {
            if( showskip ) { showMessage( "Skipping " + funcname, false, true ); }
            return;
        }

        long timeStart = System.currentTimeMillis();
        String msg = String.format( "Thread id %02d; Removing Registrations without roles...", threadId );
        showMessage( msg, false, true );

        removeEmptyRoleRegs( debug, source );    // needs registration_c plus person_c

        elapsedShowMessage( funcname, timeStart, System.currentTimeMillis() );
        showMessage_nl();
    } // doRemoveEmptyRoleRegs


    /**
     * Delete registrations with empty person roles from links_cleaned
     *
     * @param debug
     * @throws Exception
     */
    private void removeEmptyRoleRegs( boolean debug, String source )
    throws Exception
    {
        long threadId = Thread.currentThread().getId();

        showMessage( String.format( "Thread id %02d; removeEmptyRoleRegs for source %s", threadId, source ), false, true );

        String query_r = "SELECT id_registration, id_source, registration_maintype FROM registration_c WHERE id_source = " + source;
        if( debug ) { showMessage( query_r, false, true ); }

        int nNoRole = 0;
        int stepstate = count_step;

        try
        {
            ResultSet rs_r = dbconCleaned.runQueryWithResult( query_r );

            int row = 0;

            while( rs_r.next() )        // process all registrations
            {
                row++;
                if( row == stepstate ) {
                    showMessage( "" + row, true, true );
                    stepstate += count_step;
                }

                row++;
                int id_registration       = rs_r.getInt( "id_registration" );
                int id_source             = rs_r.getInt( "id_source" );
                int registration_maintype = rs_r.getInt( "registration_maintype" );

                String query_p = "SELECT id_person, role FROM person_c WHERE id_registration = " + id_registration;
                if ( debug ) { showMessage( query_p, false, true ); }

                ResultSet rs_p = dbconCleaned.runQueryWithResult( query_p );

                boolean norole = false;
                while (rs_p.next())        // process the persons of this registration
                {
                    int id_person = rs_p.getInt( "id_person" );
                    String role   = rs_p.getString( "role" );

                    if( role == null || role.isEmpty() || role.equals( "null" ) ) {
                        norole =  true;

                        if( debug ) {
                            String msg = String.format( "No role: id_registration: %d, id_person: %d, registration_maintype: %d, role: %d",
                               id_registration, id_person, registration_maintype );
                            System.out.println( msg ); showMessage( msg, false, true );
                        }
                        break;  // Kees: all persons of a registration must have a role, so we are done for this reg
                    }
                }

                if( norole )
                {
                    nNoRole++;

                    // Delete records with this registration
                    String deleteRegist = "DELETE FROM registration_c WHERE id_registration = " + id_registration;
                    String deletePerson = "DELETE FROM person_c WHERE id_registration = " + id_registration;

                    if( debug ) {
                        showMessage( "Deleting id_registration without role: " + id_registration, false, true );
                        showMessage( deleteRegist, false, true );
                        showMessage( deletePerson, false, true );
                    }

                    String id_source_str = Integer.toString( id_source );
                    addToReportRegistration( id_registration, id_source_str, 3, "" );       // warning 3

                    dbconCleaned.runQuery( deleteRegist );
                    dbconCleaned.runQuery( deletePerson );
                }
            }

            String msg = String.format( "Thread id %02d; Number of registrations with missing role(s) removed: %d", threadId, nNoRole );
            showMessage( msg, false, true );
        }
        catch( Exception ex ) {
            System.out.printf("'%s'\n", ex.getMessage());
            ex.printStackTrace( new PrintStream( System.out ) );
        }
    } // removeEmptyRoleRegs


    /**
     * @param debug
     * @param go
     * @throws Exception
     */
    private void doRemoveDuplicateRegs( boolean debug, boolean go, String source, String rmtype ) throws Exception
    {
        long threadId = Thread.currentThread().getId();

        String funcname = String.format( "Thread id %02d; doRemoveDuplicateRegs for source %s", threadId, source );

        if( !go ) {
            if( showskip ) { showMessage( "Skipping " + funcname, false, true ); }
            return;
        }

        long timeStart = System.currentTimeMillis();
        String msg = String.format( "Thread id %02d; Removing Duplicate Registrations...", threadId );
        showMessage( msg, false, true );

        removeDuplicateRegs( debug, source );

        elapsedShowMessage( funcname, timeStart, System.currentTimeMillis() );
        showMessage_nl();
    } // doRemoveDuplicateRegs


    /**
     * @param debug
     * @throws Exception
     */
    private void removeDuplicateRegs( boolean debug, String source )
    throws Exception
    {
        long threadId = Thread.currentThread().getId();

        showMessage( String.format( "Thread id %02d; removeDuplicateRegs for source %s", threadId, source ), false, true );
        showMessage( String.format( "Thread id %02d; Notice: the familyname prefix is not used for comparisons", threadId ), false , true );

        int min_cnt = 2;    // in practice we see double, triples and quadruples

        // The GROUP_CONCAT on id_registration is needed to get the different registration ids corresponding to the count.
        // And we require that the 4 grouping variables have normal values.
        String query_r = "SELECT GROUP_CONCAT(id_registration), registration_maintype, registration_location_no, registration_date, registration_seq, COUNT(*) AS cnt "
            + "FROM registration_c "
            + "WHERE id_source = " + source + " "
            + "AND registration_maintype IS NOT NULL AND registration_maintype <> 0 "
            + "AND registration_location_no IS NOT NULL AND registration_location_no <> 0 "
            + "AND registration_date IS NOT NULL AND registration_date <> '' "
            + "AND registration_seq IS NOT NULL AND registration_seq <> '' "
            + "GROUP BY registration_maintype, registration_location_no, registration_date, registration_seq "
            + "HAVING cnt >= " + min_cnt + " "
            + "ORDER BY cnt DESC;";

        if( debug ) { showMessage( query_r, false, true ); }

        int nDuplicates = 0;

        try
        {
            ResultSet rs_r = dbconCleaned.runQueryWithResult( query_r );

            int ndeleteRegist = 0;
            int ndeletePerson = 0;

            int row = 0;
            while( rs_r.next() )        // process all groups
            {
                row++;

                String registrationIds_str = rs_r.getString( "GROUP_CONCAT(id_registration)" );
                String registration_date   = rs_r.getString( "registration_date" );
                String registration_seq    = rs_r.getString( "registration_seq" );

                int registration_maintype    = rs_r.getInt( "registration_maintype" );
                int registration_location_no = rs_r.getInt( "registration_location_no" );

                if( debug ) {
                    String msg = String.format( "reg_maintype: %d, reg_location_no: %d, registration_date: %s, reg_loc_no: %s",
                        registration_maintype, registration_location_no, registration_date, registration_location_no );
                    System.out.println( msg );
                }

                String registrationIdsStrs[] = registrationIds_str.split( "," );
                Vector< Integer > registrationIds = new Vector< Integer >();
                for( String registrationId : registrationIdsStrs ) {
                    registrationIds.add( Integer.parseInt( registrationId ) );
                }

                if( debug ) { showMessage( registrationIds.toString(), false, true ); }
                Collections.sort( registrationIds );

                if( debug ) {
                    showMessage( registrationIds.toString(), false, true );
                    showMessage( "Id group of " + registrationIds.size() + ": " + registrationIds.toString(), false, true );
                }

                if( registrationIds.size() > 2 )   // useless registrations, remove them all
                {
                    for( int id_regist : registrationIds )
                    {
                        String queryDeleteRegist = "DELETE FROM registration_c WHERE id_registration = " + id_regist;
                        String queryDeletePerson = "DELETE FROM person_c WHERE id_registration = " + id_regist;

                        if( debug)
                        {
                            showMessage( "Deleting multi duplicate registration: " + id_regist, false, true );
                            showMessage( queryDeleteRegist, false, true );
                            showMessage( queryDeletePerson, false, true );
                        }

                        int countRegist = dbconCleaned.runQueryUpdate( queryDeleteRegist );
                        int countPerson = dbconCleaned.runQueryUpdate( queryDeletePerson );
                        ndeleteRegist += countRegist;
                        ndeletePerson += countPerson;

                        if( countRegist != 1 ) {
                            String msg = String.format( "removeDuplicateRegs() id_registration: %d already removed", id_regist );
                            showMessage( msg, false, true );
                        }
                    }
                }
                else    // test pairs
                {
                    int rid1 = 0;
                    int rid2 = 1;
                    boolean isDuplicate = compare2Registrations( debug, rid1, rid2, registrationIds, registration_maintype );
                    if( isDuplicate ) { nDuplicates++; }
                }
                /*
                for( int rid1 = 0; rid1 < registrationIdsStrs.length; rid1++ )
                {
                    for( int rid2 = rid1 + 1; rid2 < registrationIdsStrs.length; rid2++ )
                    {
                        boolean isDuplicate = compare2Registrations( debug, rid1, rid2, registrationIds, registration_maintype );
                        if( isDuplicate ) { nDuplicates++; }
                    }
                }
                */

                // free
                registrationIds.clear();
                registrationIds = null;
            }

            String msg = String.format( "Thread id %02d; Number of duplicate regs removed from duplicate tuples: %d", threadId, ndeleteRegist );
            showMessage( msg, false, true );

            msg = String.format( "Thread id %02d; Number of duplicate regs removed from duplicate pairs: %d", threadId, nDuplicates );
            showMessage( msg, false, true );
        }
        catch( Exception ex ) {
            System.out.printf("'%s'\n", ex.getMessage());
            ex.printStackTrace( new PrintStream( System.out ) );
        }
    } // removeDuplicateRegs


    /**
     * @param debug
     * @param rid1
     * @param rid2
     * @param registrationIds
     * @param registration_maintype
     *
     * @throws Exception
     */
    private boolean compare2Registrations( boolean debug, int rid1, int rid2, Vector< Integer > registrationIds, int registration_maintype )
    throws Exception
    {
        boolean isDeleted = false;

        if( debug ) {
            System.out.println( String.format( "rid1: %d, rid2: %d", rid1, rid2 ) );
            System.out.println( String.format( "registrationIds.size(): %d", registrationIds.size() ) );
            for( int rid : registrationIds ) { System.out.println( rid ); }
        }

        int id_registration1 = registrationIds.get( rid1 );
        int id_registration2 = registrationIds.get( rid2 );

        if( debug ) { showMessage( "Comparing, " + rid1 + ": " + id_registration1 + ", " + rid2 + ": " + id_registration2, false, true ); }

        String id_source1 = "";
        String id_source2 = "";

        if( registration_maintype == 1 )
        {
            String query_p1 = "SELECT id_source, role, firstname, prefix, familyname FROM person_c WHERE id_registration = " + id_registration1 + " AND role = 1";
            if( debug ) { System.out.println( query_p1 ); }
            ResultSet rs_p1 = dbconCleaned.runQueryWithResult( query_p1 );

            String newborn_id_source1  = "";
            String newborn_firstname1  = "";
            String newborn_prefix1     = "";
            String newborn_familyname1 = "";

            while( rs_p1.next() )
            {
                int role = rs_p1.getInt( "role" );

                newborn_id_source1  = rs_p1.getString( "id_source" );
                newborn_firstname1  = rs_p1.getString( "firstname" );
                newborn_prefix1     = rs_p1.getString( "prefix" );
                newborn_familyname1 = rs_p1.getString( "familyname" );

                if( newborn_id_source1  == null ) { newborn_id_source1  = ""; }
                if( newborn_firstname1  == null ) { newborn_firstname1  = ""; }
                if( newborn_prefix1     == null ) { newborn_prefix1     = ""; }
                if( newborn_familyname1 == null ) { newborn_familyname1 = ""; }

                id_source1 = newborn_id_source1;

                //if( debug ) { System.out.printf( "role: %d, familyname: %s, prefix: %s, firstname: %s\n", role, familyname, prefix, firstname ); }
            }

            String query_p2 = "SELECT id_source, role, firstname, prefix, familyname FROM person_c WHERE id_registration = " + id_registration2 + " AND role = 1";
            if( debug ) { System.out.println( query_p2 ); }
            ResultSet rs_p2 = dbconCleaned.runQueryWithResult( query_p2 );

            String newborn_id_source2  = "";
            String newborn_firstname2  = "";
            String newborn_prefix2     = "";
            String newborn_familyname2 = "";

            while( rs_p2.next() )
            {
                int role = rs_p2.getInt( "role" );

                newborn_id_source2  = rs_p2.getString( "id_source" );
                newborn_firstname2  = rs_p2.getString( "firstname" );
                newborn_prefix2     = rs_p2.getString( "prefix" );
                newborn_familyname2 = rs_p2.getString( "familyname" );

                if( newborn_id_source2  == null ) { newborn_id_source2  = ""; }
                if( newborn_firstname2  == null ) { newborn_firstname2  = ""; }
                if( newborn_prefix2     == null ) { newborn_prefix2     = ""; }
                if( newborn_familyname2 == null ) { newborn_familyname2 = ""; }

                id_source2 = newborn_id_source2;
            }

            if( newborn_firstname1.equals( newborn_firstname2 ) && newborn_familyname1.equals( newborn_familyname2 ) )
            {
                if( debug ) {
                    showMessage_nl();
                    String msg = String.format( "Duplicate registrations, ids, %d: %d, %d: %d (registration_maintype: %d)",
                        rid1, id_registration1, rid2, id_registration2, registration_maintype );
                    showMessage( msg, false, true );

                    showMessage( "newborn_familyname1: " + newborn_familyname1 + ", newborn_prefix1: " + newborn_prefix1 + ", newborn_firstname1: " + newborn_firstname1, false, true );
                    showMessage( "newborn_familyname2: " + newborn_familyname2 + ", newborn_prefix2: " + newborn_prefix2 + ", newborn_firstname2: " + newborn_firstname2, false, true );
                }

                int delcnt = removeDuplicate( debug, registrationIds, id_source1, id_source2, id_registration1, id_registration2, registration_maintype );
                if( delcnt > 0 ) { return true; }
            }
        }

        else if( registration_maintype == 2 )   // marriage
        {

            String query_p1 = "SELECT id_source, role, firstname, prefix, familyname FROM person_c WHERE id_registration = " + id_registration1 + " AND (role = 4 OR role = 7)";
            if( debug ) { System.out.println( query_p1 ); }
            ResultSet rs_p1 = dbconCleaned.runQueryWithResult( query_p1 );

            String bride_id_source1  = "";
            String bride_firstname1  = "";
            String bride_prefix1     = "";
            String bride_familyname1 = "";

            String groom_id_source1  = "";
            String groom_firstname1  = "";
            String groom_prefix1     = "";
            String groom_familyname1 = "";

            while( rs_p1.next() )
            {
                int role = rs_p1.getInt( "role" );

                if( role == 4 )
                {
                    bride_id_source1  = rs_p1.getString( "id_source" );
                    bride_firstname1  = rs_p1.getString( "firstname" );
                    bride_prefix1     = rs_p1.getString( "prefix" );
                    bride_familyname1 = rs_p1.getString( "familyname" );

                    if( bride_id_source1  == null ) { bride_id_source1  = ""; }
                    if( bride_firstname1  == null ) { bride_firstname1  = ""; }
                    if( bride_prefix1     == null ) { bride_prefix1     = ""; }
                    if( bride_familyname1 == null ) { bride_familyname1 = ""; }

                    id_source1 = bride_id_source1;
                }
                else    // role == 7
                {
                    groom_id_source1  = rs_p1.getString( "id_source" );
                    groom_firstname1  = rs_p1.getString( "firstname" );
                    groom_prefix1     = rs_p1.getString( "prefix" );
                    groom_familyname1 = rs_p1.getString( "familyname" );

                    if( groom_id_source1  == null ) { groom_id_source1  = ""; }
                    if( groom_firstname1  == null ) { groom_firstname1  = ""; }
                    if( groom_prefix1     == null ) { groom_prefix1     = ""; }
                    if( groom_familyname1 == null ) { groom_familyname1 = ""; }

                    id_source1 = groom_id_source1;
                }
            }

            String query_p2 = "SELECT id_source, role, firstname, prefix, familyname FROM person_c WHERE id_registration = " + id_registration2 + " AND (role = 4 OR role = 7)";
            if( debug ) { System.out.println( query_p2 ); }
            ResultSet rs_p2 = dbconCleaned.runQueryWithResult( query_p2 );

            String bride_id_source2  = "";
            String bride_firstname2  = "";
            String bride_prefix2     = "";
            String bride_familyname2 = "";

            String groom_id_source2  = "";
            String groom_firstname2  = "";
            String groom_prefix2     = "";
            String groom_familyname2 = "";

            while( rs_p2.next() )
            {
                int role = rs_p2.getInt( "role" );

                if( role == 4 )
                {
                    bride_id_source2  = rs_p2.getString( "id_source" );
                    bride_firstname2  = rs_p2.getString( "firstname" );
                    bride_prefix2     = rs_p2.getString( "prefix" );
                    bride_familyname2 = rs_p2.getString( "familyname" );

                    if( bride_id_source2  == null ) { bride_id_source2  = ""; }
                    if( bride_firstname2  == null ) { bride_firstname2  = ""; }
                    if( bride_prefix2     == null ) { bride_prefix2     = ""; }
                    if( bride_familyname2 == null ) { bride_familyname2 = ""; }

                    id_source2 = bride_id_source2;
                }
                else    // role == 7
                {
                    groom_id_source2  = rs_p2.getString( "id_source" );
                    groom_firstname2  = rs_p2.getString( "firstname" );
                    groom_prefix2     = rs_p2.getString( "prefix" );
                    groom_familyname2 = rs_p2.getString( "familyname" );

                    if( groom_id_source2  == null ) { groom_id_source2  = ""; }
                    if( groom_firstname2  == null ) { groom_firstname2  = ""; }
                    if( groom_prefix2     == null ) { groom_prefix2     = ""; }
                    if( groom_familyname2 == null ) { groom_familyname2 = ""; }

                    id_source2 = groom_id_source2;
                }
            }

            if( bride_firstname1.equals( bride_firstname2 ) && bride_familyname1.equals( bride_familyname2 ) &&
                groom_firstname1.equals( groom_firstname2 ) && groom_familyname1.equals( groom_familyname2 ) )
            {
                if( debug ) {
                    showMessage_nl();
                    String msg = String.format( "Duplicate registrations, ids, %d: %d, %d: %d (registration_maintype: %d)",
                        rid1, id_registration1, rid2, id_registration2, registration_maintype );
                    showMessage( msg, false, true );

                    showMessage( "bride_familyname1: " + bride_familyname1 + ", bride_prefix1: " + bride_prefix1 + ", bride_firstname1: " + bride_firstname1, false, true );
                    showMessage( "bride_familyname2: " + bride_familyname2 + ", bride_prefix2: " + bride_prefix2 + ", bride_firstname2: " + bride_firstname2, false, true );

                    showMessage( "groom_familyname1: " + groom_familyname1 + ", groom_prefix1: " + groom_prefix1 + ", groom_firstname1: " + groom_firstname1, false, true );
                    showMessage( "groom_familyname2: " + groom_familyname2 + ", groom_prefix2: " + groom_prefix2 + ", groom_firstname2: " + groom_firstname2, false, true );
                }

                int delcnt = removeDuplicate( debug, registrationIds, id_source1, id_source2, id_registration1, id_registration2, registration_maintype );
                if( delcnt > 0 ) { return true; }
            }
        }

        else if( registration_maintype == 3 )   // death
        {
            String query_p1 = "SELECT id_source, role, firstname, prefix, familyname FROM person_c WHERE id_registration = " + id_registration1 + " AND role = 10";
            if( debug ) { System.out.println( query_p1 ); }
            ResultSet rs_p1 = dbconCleaned.runQueryWithResult( query_p1 );

            String deceased_id_source1  = "";

            String deceased_firstname1  = "";
            String deceased_prefix1     = "";
            String deceased_familyname1 = "";

            while( rs_p1.next() )
            {
                int role = rs_p1.getInt( "role" );

                deceased_id_source1  = rs_p1.getString( "id_source" );
                deceased_firstname1  = rs_p1.getString( "firstname" );
                deceased_prefix1     = rs_p1.getString( "prefix" );
                deceased_familyname1 = rs_p1.getString( "familyname" );

                if( deceased_id_source1  == null ) { deceased_id_source1  = ""; }
                if( deceased_firstname1  == null ) { deceased_firstname1  = ""; }
                if( deceased_prefix1     == null ) { deceased_prefix1     = ""; }
                if( deceased_familyname1 == null ) { deceased_familyname1 = ""; }

                id_source1 = deceased_id_source1;
            }

            String query_p2 = "SELECT id_source, role, firstname, prefix, familyname FROM person_c WHERE id_registration = " + id_registration2 + " AND role = 10";
            if( debug ) { System.out.println( query_p2 ); }
            ResultSet rs_p2 = dbconCleaned.runQueryWithResult( query_p2 );

            String deceased_id_source2  = "";
            String deceased_firstname2  = "";
            String deceased_prefix2     = "";
            String deceased_familyname2 = "";

            while( rs_p2.next() )
            {
                int role = rs_p2.getInt( "role" );

                deceased_id_source2  = rs_p2.getString( "id_source" );
                deceased_firstname2  = rs_p2.getString( "firstname" );
                deceased_prefix2     = rs_p2.getString( "prefix" );
                deceased_familyname2 = rs_p2.getString( "familyname" );

                if( deceased_id_source2  == null ) { deceased_id_source2  = ""; }
                if( deceased_firstname2  == null ) { deceased_firstname2  = ""; }
                if( deceased_prefix2     == null ) { deceased_prefix2     = ""; }
                if( deceased_familyname2 == null ) { deceased_familyname2 = ""; }

                id_source2 = deceased_id_source2;
            }

            if( deceased_firstname1.equals( deceased_firstname2 ) && deceased_familyname1.equals( deceased_familyname2 ) )
            {
                if( debug ) {
                    showMessage_nl();
                    String msg = String.format( "Duplicate registrations, ids, %d: %d, %d: %d (registration_maintype: %d)",
                        rid1, id_registration1, rid2, id_registration2, registration_maintype );
                    showMessage( msg, false, true );

                    showMessage( "deceased_familyname1: " + deceased_familyname1 + ", deceased_prefix1: " + deceased_prefix1 + ", deceased_firstname1: " + deceased_firstname1, false, true );
                    showMessage( "deceased_familyname2: " + deceased_familyname2 + ", deceased_prefix2: " + deceased_prefix2 + ", deceased_firstname2: " + deceased_firstname2, false, true );
                }

                int delcnt = removeDuplicate( debug, registrationIds, id_source1, id_source2, id_registration1, id_registration2, registration_maintype );
                if( delcnt > 0 ) { return true; }
            }
        }

        return false;
    } // compare2Registrations


    /**
     * @param debug
     *
     * @throws Exception
     */
    private int removeDuplicate( boolean debug, Vector< Integer > registrationIds, String id_source1, String id_source2,
        int id_registration1, int id_registration2, int registration_maintype )
    throws Exception
    {
        if( debug ) {
            showMessage_nl();
            showMessage( "Duplicate in Id group of " + registrationIds.size() + ": " + registrationIds.toString(), false, true );
        }

        int id_reg_keep = 0;
        int id_reg_remove = 0;
        String id_source_remove = "";

        // keep the smallest id_reg
        if( id_registration2 > id_registration1 )
        {
            id_reg_keep      = id_registration1;
            id_reg_remove    = id_registration2;
            id_source_remove = id_source2;
        }
        else
        {
            id_reg_keep      = id_registration2;
            id_reg_remove    = id_registration1;
            id_source_remove = id_source1;
        }

        //String msgt = "TEST RUN; NOT DELETING";
        //System.out.println( msgt ); showMessage( msgt, false, true );

        if( debug ) {
            String msg = "keep id: " + id_reg_keep + ", delete: " + id_reg_remove + " (registration_maintype: " + registration_maintype + ")";
            //System.out.println( msg );
            showMessage( msg, false, true );
        }

        // write error msg with EC=1
        if( id_source_remove.isEmpty() ) { id_source_remove = "0"; }    // it must be a valid integer string for the log table
        String value = "";      // nothing to add
        addToReportRegistration( id_reg_remove, id_source_remove, 1, value );       // warning 1

        // remove second member of duplicates from registration_c and person_c
        String queryDeleteRegist = "DELETE FROM registration_c WHERE id_registration = " + id_reg_remove;
        String queryDeletePerson = "DELETE FROM person_c WHERE id_registration = " + id_reg_remove;

        if( debug ) {
            showMessage( "Deleting duplicate registration: " + id_reg_remove, false, true );
            showMessage( queryDeleteRegist, false, true );
            showMessage( queryDeletePerson, false, true );
        }

        int countRegist = dbconCleaned.runQueryUpdate( queryDeleteRegist );
        int countPerson = dbconCleaned.runQueryUpdate( queryDeletePerson );

        // we expect regist_count == 1, otherwise complain
        if( countRegist != 1 )
        {
            String msg = String.format( "removeDuplicate() id_registration: %d already removed", id_reg_remove );
            showMessage( msg, false, true );

            //showMessage( deleteRegist, false, true );
            //msg = String.format( "removeDuplicate() countRegist: %d", countRegist );
            //showMessage( msg, false, true );

            //showMessage( deletePerson, false, true );
            //msg = String.format( "removeDuplicate() countPerson: %d", countPerson );
            //showMessage( msg, false, true );
        }

        return countRegist;
    } // removeDuplicate


    /**
     * @param debug
     * @param go
     * @throws Exception
     */
    private void doScanRemarks( boolean debug, boolean go, String source, String rmtype ) throws Exception
    {
        long threadId = Thread.currentThread().getId();

        String funcname = String.format( "Thread id %02d; doScanRemarks for source %s", threadId, source );

        if( !go ) {
            if( showskip ) { showMessage( "Skipping " + funcname, false, true ); }
            return;
        }

        long timeStart = System.currentTimeMillis();
        String msg = funcname + "...";
        showMessage( msg, false, true );

        scanRemarks( debug, source );

        elapsedShowMessage( funcname, timeStart, System.currentTimeMillis() );
        showMessage_nl();
    } // doScanRemarks


    /**
     * @param debug
     * @throws Exception
     */
    private void scanRemarks( boolean debug, String source ) throws Exception
    {
        long threadId = Thread.currentThread().getId();

        showMessage( String.format( "Thread id %02d; scanRemarks()", threadId ), false, true );

        // Clear previous values for given source
        String clearQuery_r = "UPDATE registration_c SET extract = NULL WHERE id_source = " + source + ";";
        dbconCleaned.runQuery( clearQuery_r );

        String clearQuery_p1 = "UPDATE person_c SET status_mother = NULL WHERE id_source = " + source + ";";
        dbconCleaned.runQuery( clearQuery_p1 );

        String clearQuery_p2 = "UPDATE person_c SET stillbirth = NULL WHERE stillbirth = 'y-r' AND id_source = " + source + ";";
        dbconCleaned.runQuery( clearQuery_p2 );


        // load the table data from links_general.scan_remarks
        String selectQuery_r = "SELECT * FROM scan_remarks ORDER BY id_scan";
        if( debug ) {
            System.out.printf( "\n%s\n", selectQuery_r );
            showMessage( selectQuery_r, false, true );
        }

        Vector< Remarks > remarksVec = new Vector();

        try
        {
            ResultSet rs_r = dbconRefRead.runQueryWithResult( selectQuery_r );

            int nrecord = 0;
            while( rs_r.next() )
            {
                int id_scan  = rs_r.getInt( "id_scan" );
                int maintype = rs_r.getInt( "maintype" );
                int role     = rs_r.getInt( "role" );

                String string_1   = rs_r.getString( "string_1" );
                String string_2   = rs_r.getString( "string_2" );
                String string_3   = rs_r.getString( "string_3" );
                String not_string = rs_r.getString( "not_string" );
                String name_table = rs_r.getString( "name_table" );
                String name_field = rs_r.getString( "name_field" );
                String value      = rs_r.getString( "value" );

                if( string_1   != null ) { string_1   = string_1  .toLowerCase(); }
                if( string_2   != null ) { string_2   = string_2  .toLowerCase(); }
                if( string_3   != null ) { string_3   = string_3  .toLowerCase(); }
                if( not_string != null ) { not_string = not_string.toLowerCase(); }
                if( name_table != null ) { name_table = name_table.toLowerCase(); }
                if( name_field != null ) { name_field = name_field.toLowerCase(); }
                if( value      != null ) { value      = value     .toLowerCase(); }

                Remarks remarks = new Remarks();

                remarks.setIdScan(id_scan);
                remarks.setMaintype(  maintype );
                remarks.setRole(      role );
                remarks.setString_1(  string_1 );
                remarks.setString_2(  string_2 );
                remarks.setString_3(  string_3 );
                remarks.setNotString( not_string );
                remarks.setNameTable( name_table );
                remarks.setNameField( name_field );
                remarks.setValue(     value );

                remarksVec.add( remarks  );

                if( debug ) { System.out.printf( "%2d, id_scan: %3d, maintype: %d, role : %2d,  string_1: %s,  string_2: %s,  string_3: %s,  not_string: %s,  name_table: %s, name_field: %s, value: %s\n",
                        nrecord, id_scan, maintype, role, string_1, string_2, string_3, not_string, name_table, name_field, value ); }

                nrecord++;
            }
            if( debug ) { System.out.println( "" ); }
        }
        catch( Exception ex ) {
            showMessage( ex.getMessage(), false, true );
            ex.printStackTrace( new PrintStream( System.out ) );
        }


        // loop through the registration remarks
        String selectQuery_o = "SELECT id_registration , registration_maintype , remarks FROM registration_o WHERE id_source = " + source + " ORDER BY id_registration";
        if( debug ) {
            System.out.printf( "%s\n\n", selectQuery_o );
            showMessage( selectQuery_o, false, true );
        }

        try
        {
            ResultSet rs_o = dbconOriginal.runQueryWithResult( selectQuery_o );

            int nupdates = 0;

            while( rs_o.next() )
            {
                int id_registration       = rs_o.getInt( "id_registration" );
                int registration_maintype = rs_o.getInt( "registration_maintype" );

                String remarks_str = rs_o.getString( "remarks" );

                if( remarks_str == null || remarks_str.isEmpty() ) { continue; }
                else { remarks_str = remarks_str.toLowerCase(); }

                //System.out.printf( "id_registration: %d, registration_maintype: %d, remarks: %s\n" ,
                //    id_registration, registration_maintype, remarks_str );

                // compare against remarks table
                int record = 0;

                for( Remarks remarks : remarksVec )
                {
                    boolean found = false;

                    int id_scan   = remarks.getIdScan();
                    int maintype  = remarks.getMaintype();
                    int role      = remarks.getRole();

                    String string_1   = remarks.getString_1();
                    String string_2   = remarks.getString_2();
                    String string_3   = remarks.getString_3();
                    String not_string = remarks.getNotString();
                    String name_table = remarks.getNameTable();
                    String name_field = remarks.getNameField();
                    String value      = remarks.getValue();

                    if( registration_maintype ==  maintype )
                    {
                        if( string_2 == null || string_2.isEmpty() )
                        {
                            if( remarks_str.indexOf( string_1 ) != -1 )     // string_1 found in remarks
                            { found = true; }
                        }
                        else        // string_2 not empty
                        {
                            if( string_3 == null || string_3.isEmpty() )
                            {
                                if( remarks_str.indexOf( string_1 ) != -1 &&
                                    remarks_str.indexOf( string_2 ) != -1 )     // first 2 found in remarks
                                { found = true; }
                            }
                            else    // string_3 not empty
                            {
                                if( remarks_str.indexOf( string_1 ) != -1 &&
                                    remarks_str.indexOf( string_2 ) != -1 &&
                                    remarks_str.indexOf( string_3 ) != -1 )     // all 3 found in remarks
                                { found = true; }
                            }
                        }
                    }

                    if( found )
                    {
                        if( not_string == null || not_string.isEmpty() || remarks_str.indexOf( not_string ) == -1 ) {
                            nupdates++;
                            scanRemarksUpdate( debug, nupdates, remarks_str, id_scan, id_registration, role, name_table, name_field, value );
                        }
                        /*
                        else    // not_string not empty
                        {
                            if( remarks_str.indexOf( not_string ) == -1 )   // but not found
                            { scanRemarksUpdate( debug, nupdates, remarks_str, id_scan, id_registration, role, name_table, name_field, value ); }
                        }
                        */
                    }
                }
            }

            String msg = String.format( "Thread id %02d; scanRemarks: Number of updates: %d", threadId, nupdates );
            showMessage( msg, false, true );
        }
        catch( Exception ex ) {
            showMessage( ex.getMessage(), false, true );
            ex.printStackTrace( new PrintStream( System.out ) );
        }
    } // scanRemarks


    /**
     * @param debug
     * @throws Exception
     */
    private void scanRemarksUpdate( boolean debug, int nupdates, String remarks_str, int id_scan, int id_registration, int role, String name_table, String name_field, String value )
    //throws Exception
    {
        long threadId = Thread.currentThread().getId();

        String query_u = "";

        if( name_table.equals( "registration_c" ) ) {
            query_u = String.format( "UPDATE links_cleaned.registration_c SET %s = '%s' WHERE id_registration = %d",
                name_field, value, id_registration );
        }
        else if( name_table.equals( "person_c" ) ) {
            query_u = String.format( "UPDATE links_cleaned.person_c SET %s = '%s' WHERE id_registration = %d AND role = %d",
                name_field, value, id_registration, role );
        }

        if( debug ) {
            System.out.printf( "Update: %d, based on id_scan: %d, and id_registration: %d, and remarks: %s\n", nupdates, id_scan, id_registration, remarks_str );
            System.out.printf("%s\n", query_u);
        }

        try {  dbconCleaned.runQuery( query_u ); }
        catch( Exception ex ) {
            showMessage( "Query: " + query_u, false, true );
            String msg = String.format( "Thread id %02d; Exception: %s", threadId, ex.getMessage() );
            showMessage( msg, false, true );
            ex.printStackTrace( new PrintStream( System.out ) );
        }
    }


    /*---< Run PreMatch >-----------------------------------------------------*/

    /**
     * Run PreMatch
     * @param go
     * @throws Exception
     */
    private void doPrematch( boolean go) throws Exception
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
