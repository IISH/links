package modulemain;

import java.io.PrintStream;

import java.sql.ResultSet;

import java.sql.SQLException;

import java.time.LocalDateTime;     // java.time Java SE 8, based on Joda-Time

import java.util.ArrayList;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import javax.swing.JTextArea;
import javax.swing.JTextField;

//import com.joestelmach.natty.Parser;    // NLP date parser

//import org.threeten.extra.Date

import com.zaxxer.hikari.HikariDataSource;

import connectors.HikariConnection;
import connectors.HikariCPool;

import dataset.Options;
import dataset.TableToArrayListMultimapHikari;

import general.Functions;
import general.PrintLogger;

import linksmanager.ManagerGui;


/**
 * @author Fons Laan
 *
 * FL-30-Jun-2014 Imported from @author Omar Azouguagh backup
 * FL-08-Oct-2019 Begin using HikariCP
 * FL-21-Oct-2019 Start using LinksCleanedAsync
 * FL-05-Nov-2019 Extensive cleanup
 * FL-29-Dec-2019 Separate DB pool for HSN
 *
 */

public class LinksCleanMain extends Thread
{
    private JTextField guiLine;		// different for PreMatch
    private JTextArea guiArea;		// different for PreMatch

    private boolean multithreaded = true;
    private boolean use_links_logs = true;

    private boolean dbconref_single = true;     // true: same ref for reading and writing

    private static HikariDataSource dsLog      = null;
    private static HikariDataSource dsRefRead  = null;
    private static HikariDataSource dsRefWrite = null;
    private static HikariDataSource dsOriginal = null;
    private static HikariDataSource dsCleaned  = null;
    private static HikariDataSource dsTemp     = null;

    //private Runtime r = Runtime.getRuntime();
    private String logTableName;
    private Options opts;

    private final static String SC_U = "u"; // Unknown Standard value assigned (although the original value is not valid)
    private final static String SC_X = "x"; //    X    Standard yet to be assigned
    private final static String SC_N = "n"; //    No   standard value assigned (original value is not valid)
    private final static String SC_Y = "y"; //    Yes  Standard value assigned (original value is valid)

    private ManagerGui mg;

    private String ref_url = "";               // reference db access
    private String ref_user = "";
    private String ref_pass = "";
    private String ref_db = "";

    private String db_url = "";                 // links db's access
    private String db_user = "";
    private String db_pass = "";

    private String sourceIdsGui;
    private String RMtypesGui;

    private int[] sourceList;                   // either sourceListAvail, or [sourceId] from GUI

    private String endl = ". OK.";              // ".";

    private PrintLogger plog;
    private boolean showskip = false;


    /**
     * Constructor
     *
     * @param guiLine
     * @param guiArea
     * @param opts
     * @param mg
     */
    public LinksCleanMain
    (
        Options opts,
        JTextField guiLine,
        JTextArea guiArea,
        ManagerGui mg
    )
    {
        this.opts = opts;

        this.use_links_logs = opts.getUseLinksLogs();

        this.plog = opts.getLogger();
        this.sourceIdsGui = opts.getSourceIds();
        this.RMtypesGui = opts.getRMtypes();

        this.ref_url  = opts.getDb_ref_url();
        this.ref_user = opts.getDb_ref_user();
        this.ref_pass = opts.getDb_ref_pass();
        this.ref_db   = opts.getDb_ref_db();

        this.db_url  = opts.getDb_url();
        this.db_user = opts.getDb_user();
        this.db_pass = opts.getDb_pass();

        this.guiLine = guiLine;
        this.guiArea = guiArea;

        this.mg = mg;
    }


    @Override
    /**
     * run
     */
    public void run()
    {
        int max_threads_simul = opts.getMaxThreadsSimul();

        multithreaded = true;
        //if( max_threads_simul > 1 ) { multithreaded = true; }

        String rmtype = "";

        long mainThreadId = Thread.currentThread().getId();

        try
        {
            long cleanStart = System.currentTimeMillis();

            plog.show(String.format("Thread id %02d; Main thread, LinksCleanThread/run()", mainThreadId));

            String msg = "";
            if (dbconref_single) {
                msg = String.format("Thread id %02d; Using the same reference db for reading and writing", mainThreadId);
            } else {
                msg = String.format("Thread id %02d; Reference db: reading locally, writing to remote db", mainThreadId);
            }
            plog.show(msg);
            showMessage(msg, false, true);

            guiLine.setText("");
            guiArea.setText("");

            showOptions();

            // Maybe use connection pool from mariaDB
            // https://mariadb.com/kb/en/library/pool-datasource-implementation/
            //int max_pool_size = 10;
            int max_pool_size = 2 + max_threads_simul;
            // with autoCommit = false, after the queries[s] a separate dbcon.commit() call is required to effectuate the transaction!
            boolean autoCommit = true;

            // separate pool for each database server
            System.out.println( "ref_url: " + ref_url );
            String hikariConfigPathname_hsnref = "";      // not used
            HikariCPool conPool_hsnref = new HikariCPool( max_pool_size, autoCommit, hikariConfigPathname_hsnref, ref_url, ref_user, ref_pass );

            System.out.println( "db_url: " + db_url );
            String hikariConfigPathname_links = "";      // not used
            HikariCPool conPool_links  = new HikariCPool( max_pool_size, autoCommit, hikariConfigPathname_links, db_url,  db_user,  db_pass );

            dsRefRead  = conPool_hsnref.getDataSource( "links_general" );
            dsRefWrite = conPool_hsnref.getDataSource( "links_general" );

            dsLog      = conPool_links.getDataSource( "links_logs" );
            dsOriginal = conPool_links.getDataSource( "links_original" );
            dsCleaned  = conPool_links.getDataSource( "links_cleaned" );
            dsTemp     = conPool_links.getDataSource( "links_temp" );

            System.out.println( "Using links_logs for error logging: " + use_links_logs );
            msg = String.format( "Thread id %02d; Using links_logs for error logging: %s", mainThreadId, use_links_logs );
            plog.show( msg );
            showMessage( msg, false, true );

            if( use_links_logs ) {
                logTableName = LinksSpecific.getLogTableName();
                createLogTable();                                           // Create log table with timestamp
            }

            int[] sourceListAvail = getOrigSourceIds();                 // get source ids from links_original.registration_o
            sourceList = createSourceList(sourceIdsGui, sourceListAvail);

            String s = "";
            if (sourceList.length == 1) {
                //multithreaded = false;  // only 1 source
                s = String.format("Thread id %02d; Processing source: ", mainThreadId);
            } else {
                s = String.format("Thread id %02d; Processing sources: ", mainThreadId);
            }

            for (int i : sourceList) {
                s = s + i + " ";
            }
            showMessage(s, false, true);

            // we currently only support a single rmtype, not a list
            try {
                int rmtype_int = Integer.parseInt(RMtypesGui);    // test for (single) int
                rmtype = RMtypesGui;                                // but we use the string
            } catch (NumberFormatException nfex) {
                rmtype = "";
                //msg = String.format( "Thread id %02d; Exception: %s", mainThreadId, nfex.getMessage() );
                //nfex.printStackTrace( new PrintStream( System.out ) );
                if (!RMtypesGui.isEmpty()) {
                    showMessage("Not using registration_maintype", false, true);
                }
            }
            msg = String.format("Thread id %02d; rmtype: %s", mainThreadId, rmtype);
            showMessage(msg, false, true);

            if( multithreaded )
            {
                msg = String.format( "Thread id %02d; Max simultaneous active cleaning threads: %d", mainThreadId, max_threads_simul );
                System.out.println( msg ); plog.show( msg );

                //ThreadManager tm = new ThreadManager( max_threads_simul );
                //msg = String.format( "Thread id %02d; Multi-threaded cleaning with max %d simultaneous cleaning threads", mainThreadId, max_threads_simul );
                final Semaphore semaphore = new Semaphore( max_threads_simul, true );

                int ncores = Runtime.getRuntime().availableProcessors();
                msg = String.format( "Thread id %02d; Available cores: %d", mainThreadId, ncores );
                System.out.println( msg ); plog.show( msg );

                int nthreads_active = java.lang.Thread.activeCount();
                msg = String.format( "Thread id %02d; Currently active threads: %d", mainThreadId, nthreads_active );
                System.out.println( msg ); plog.show( msg );

                long timeStart = System.currentTimeMillis();

                ArrayList< LinksCleanAsync > threads = new ArrayList();

                for( int sourceId : sourceList )
                {
                    /*
                    while( !tm.allowNewThread() )  // Wait until our thread manager gives permission
                    {
                        plog.show(String.format("Thread id %02d; No permission for new thread: Waiting 60 seconds", mainThreadId));
                        Thread.sleep(60000);
                    }
                    tm.addThread();        // Add a thread to the thread count
                    */

                    // Wait until semaphore gives permission
                    int npermits = semaphore.availablePermits();
                    msg = String.format( "Thread id %02d; Semaphore: # of permits: %d", mainThreadId, npermits );
                    plog.show( msg );

                    while( ! semaphore.tryAcquire( 0, TimeUnit.SECONDS ) ) {
                        msg = String.format( "Thread id %02d; No permission for new thread: Waiting 60 seconds", mainThreadId );
                        plog.show( msg );
                        Thread.sleep( 60000 );
                    }

                    String source = Integer.toString( sourceId );

                    LinksCleanAsync lca = new LinksCleanAsync( semaphore, opts, guiLine, guiArea, source, rmtype, showskip,
                        logTableName, dsLog, dsRefRead, dsRefWrite, dsOriginal, dsCleaned, dsTemp );

                    msg = String.format( "Thread id %02d; Start cleaning source: %s, rmtype: %s", mainThreadId, source, rmtype );
                    plog.show( msg ); showMessage(msg, false, true);

                    lca.start();
                    threads.add( lca );
                }

                // join the threads: main thread must wait for children to finish
                for( LinksCleanAsync lca : threads ) { lca.join(); }

                // Close data sources
                if( dsRefRead  != null ) { dsRefRead.close();  dsRefRead = null; }
                if( dsRefWrite != null ) { dsRefWrite.close(); dsRefWrite = null; }
                if( dsLog      != null ) { dsLog.close();      dsLog = null; }
                if( dsOriginal != null ) { dsOriginal.close(); dsOriginal = null; }
                if( dsCleaned  != null ) { dsCleaned.close();  dsCleaned = null; }
                if( dsTemp     != null ) { dsTemp.close();     dsTemp = null; }

                showMessage_nl();
                msg = String.format("Thread id %02d; Cleaning is done", mainThreadId);
                elapsedShowMessage(msg, cleanStart, System.currentTimeMillis());
                System.out.println(msg);

                msg = String.format(String.format("Thread id %02d; Main thread; Cleaning Finished.", mainThreadId));
                elapsedShowMessage(msg, cleanStart, System.currentTimeMillis());
                System.out.println(msg);

                LocalDateTime timePoint = LocalDateTime.now();  // The current date and time
                msg = String.format("Thread id %02d; current time: %s", mainThreadId, timePoint.toString());
                showMessage(msg, false, true);
            }

        } catch (Exception ex) {
            String msg = String.format("Thread id %02d; Exception: %s", mainThreadId, ex.getMessage());
            showMessage(msg, false, true);
            ex.printStackTrace(new PrintStream(System.out));
        }
    } // run


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
        showMessage( funcname + " ...", false, true );

        showMessage( "Running PREMATCH ...", false, false );
        mg.firePrematch();

        elapsedShowMessage( funcname, timeStart, System.currentTimeMillis() );
        showMessage_nl();
    } // doPrematch


    /*===< Helper functions >=================================================*/
	private void showOptions()
	{
		System.out.println( "showOptions()" );
		System.out.printf( "DbgRefreshData: %s\n", opts.isDbgRefreshData() );
	} // showOptions


    /**
     * Read distinct source ids from links_original.registration_o
     *
     * @return
     */
    private int[] getOrigSourceIds()
    throws SQLException
    {
        ArrayList< String > ids = new ArrayList();
        String query = "SELECT DISTINCT id_source FROM registration_o ORDER BY id_source;";
        HikariConnection dbconOriginal = new HikariConnection( dsOriginal.getConnection() );

        try( ResultSet rs = dbconOriginal.executeQuery( query ) )
        {
            int count = 0;
            while( rs.next() )
            {
                count += 1;
                String id = rs.getString("id_source" );
                if( id == null || id.isEmpty() ) {
                    break;
                } else {
                    //System.out.printf( "id: %s\n", id );
                    ids.add( id );
                }
            }

            if( count == 0 ) { showMessage("Empty links_original ?", false, true ); }

        }
        catch( Exception ex )
        {
            if( ex.getMessage() != "After end of result set" ) {
                System.out.printf( "'%s'\n", ex.getMessage() );
                ex.printStackTrace( new PrintStream( System.out ) ) ;
            }
        }
        //System.out.println( ids );
        dbconOriginal.close();

        int[] idsInt = new int[ ids.size() ];
        int i = 0;
        for( String id : ids ) {
            //System.out.println( id );
            idsInt[ i ] = Integer.parseInt( id );
            i += 1;
        }
        return idsInt;
    } // getOrigSourceIds


    /**
     * Get source ids from GUI or links_original.registration_o
     *
     * @return
     */
    private int[] createSourceList(String sourceIdsGui, int[] sourceListAvail) {
        int[] idsInt;

        if (sourceIdsGui.isEmpty()) {
            idsInt = sourceListAvail;
        }           // use all Ids from links_original.registration_o
        else {
            String idsStr[] = sourceIdsGui.split(" ");

            if (idsStr.length == 0)           // nothing from GUI
            {
                idsInt = sourceListAvail;
            }       // use all Ids from links_original.registration_o
            else                                // use GUI supplied Ids
            {
                idsInt = new int[idsStr.length];
                for (int i = 0; i < idsStr.length; i++) {
                    idsInt[i] = Integer.parseInt(idsStr[i]);
                }
            }
        }

        return idsInt;
    } // createSourceList


    private void createLogTable()
    throws Exception
    {
        long threadId = Thread.currentThread().getId();

        showMessage( String.format( "Thread id %02d; Creating logging table: ", threadId ) + logTableName , false, true );

        String query = ""
            + " CREATE TABLE `links_logs`.`" + logTableName + "` ("
            + " `id_log`       INT UNSIGNED NOT NULL AUTO_INCREMENT ,"
            + " `id_source`    INT UNSIGNED NULL ,"
            + " `archive`      VARCHAR(30)  NULL ,"
            + " `location`     VARCHAR(120) NULL ,"
            + " `reg_type`     VARCHAR(50)  NULL ,"
            + " `date`         VARCHAR(25)  NULL ,"
            + " `sequence`     VARCHAR(60)  NULL ,"
            + " `role`         VARCHAR(50)  NULL ,"
            + " `guid`         VARCHAR(80)  NULL ,"
            + " `reg_key`      INT UNSIGNED NULL ,"
            + " `pers_key`     INT UNSIGNED NULL ,"
            + " `report_class` VARCHAR(2)   NULL ,"
            + " `report_type`  INT UNSIGNED NULL ,"
            + " `content`      VARCHAR(200) NULL ,"
            + " `date_time`    DATETIME NOT NULL ,"
            + " PRIMARY KEY (`id_log`) );";

        HikariConnection dbconLog = new HikariConnection( dsLog.getConnection() );
        dbconLog.executeUpdate( query );
        dbconLog.close();
    } // createLogTable


    /**
     * @param logText
     * @param isMinOnly
     * @param newLine
     */
    public void showMessage( String logText, boolean isMinOnly, boolean newLine )
    {
        guiLine.setText( logText );

        if( !isMinOnly ) {
            String newLineToken = "";
            if( newLine ) {
                newLineToken = "\r\n";
            }

            // prefix string with timestamp
            if( logText != endl ) {
                String ts = LinksSpecific.getTimeStamp2( "HH:mm:ss" );

                guiArea.append( ts + " " );
                // System.out.printf( "%s ", ts );
                //logger.info( logText );
                try { plog.show( logText ); }
                catch( Exception ex ) {
                    System.out.println( ex.getMessage() );
                    ex.printStackTrace( new PrintStream( System.out ) );
                }
            }

            guiArea.append( logText + newLineToken );
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

        guiArea.append( newLineToken );

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

}

// [eof]
