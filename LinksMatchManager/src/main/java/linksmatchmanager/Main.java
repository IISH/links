/*
Copyright (C) IISH (www.iisg.nl)

This program is free software; you can redistribute it and/or modify
it under the terms of version 3.0 of the GNU General Public License as
published by the Free Software Foundation.


This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program; if not, write to the Free Software
Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
*/

package linksmatchmanager;

import java.text.SimpleDateFormat;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;

//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;

import com.zaxxer.hikari.HikariDataSource;

//import linksmatchmanager.DatabaseManager;

import linksmatchmanager.DataSet.InputSet;
import linksmatchmanager.DataSet.NameLvsVariants;
import linksmatchmanager.DataSet.QueryGroupSet;
import linksmatchmanager.DataSet.QuerySet;


/**
 * @author Omar Azouguagh
 * @author Fons Laan
 *
 * FL-30-Jun-2014 Imported from OA backup
 * FL-15-Jan-2015 Each thread its own db connectors
 * FL-07-Jul-2016 Match names from low to high name frequency
 * FL-13-Sep-2017 Beginning of SampleLoader use
 * FL-05-Jan-2018 Do not keep db connections endlessly open (connection timeouts)
 * FL-29-Jan-2018 New db manager
 * FL-26-Feb-2018 MatchMain => Main
 * FL-11-Mar-2019 HikariCPDataSource
 */

public class Main
{
    // class global vars
    //private static final Logger logger = LoggerFactory.getLogger( Main.class );
    private static Logger logger = Logger.getLogger( Main.class.getName() );

    private static boolean debug;

    //private static final String hikariConfigPathname = "/data/links/match/hikaricp.properties";
    //private static final String hikariConfigPathname = "/home/fons/projects/links/match/hikaricp.properties";
    private static final String hikariConfigPathname = null;

    private static String dbnameMatch    = "links_match";
    private static String dbnamePrematch = "links_prematch";
    private static String dbnameTemp     = "links_temp";

    private static HikariDataSource dsrcPrematch = null;
    private static HikariDataSource dsrcMatch    = null;
    private static HikariDataSource dsrcTemp     = null;

    private static Connection dbconPrematch = null;
    private static Connection dbconMatch    = null;

    private static QueryLoader ql;
    private static SampleLoader s1, s2;
    private static PrintLogger plog;

    private static int[][] variantFamilyName;
    private static int[][] variantFirstName;
    // private static String[] rootFamilyName;
    // private static String[] rootFirstName;
    
    private static int[][] rootFamilyName;
    private static int[][] rootFirstName;



    /**
     * Main method of the Match Manager software
     * @param args It is mandatory to use three arguments which are
     * the url, the user and the pass of the database server 
     */
    public static void main( String[] args )
    {
        logger.info( "LinksMatchManager Main/main()" );

        // Notice: if debug = true (see below), dry_run => true, use_memory_tables => false
        boolean dry_run = false;     // true: do not delete former matches, + do not write new matches
        boolean use_memory_tables = true;
        String name_postfix = "_mem";

        // restore before exit; but still a mysql restart is needed to the release the memory used
        String OLD_max_heap_table_size = "";

        long mainThreadId = Thread.currentThread().getId();
        ArrayList< MatchAsync > threads = new ArrayList();

        try
        {
            plog = new PrintLogger( "LMM-" );

            long matchStart = System.currentTimeMillis();
            String timestamp1 = "11-Mar-2019 17:10";
            String timestamp2 = getTimeStamp2( "yyyy.MM.dd-HH:mm:ss" );
            plog.show( "Links Match Manager 2.0 timestamp: " + timestamp1 );
            plog.show( "Matching names from low-to-high frequency" );
            plog.show( "Start at: " + timestamp2 );

            String version = System.getProperty( "java.version" );
            plog.show( "Java version: " + version );
            System.out.println( "Java version: " + version );

            //String dbg = String.format( "Main thread (id %d); Matching EXIT." );
            //System.out.println( dbg ); plog.show( dbg );
            //System.exit( 0 );

            // Load cmd line arguments; check length
            if( args.length != 8 ) {
                String msg ="Invalid argument length " + args.length + ", it should be 8";
                plog.show( msg ); System.out.println( msg );

                msg = "Usage: java -jar LinksMatchManager-2.0.jar <db_host> <db_username> <db_password> <max_threads> <s1_sample_limit> <s2_sample_limit> <debug>";
                plog.show( msg ); System.out.println( msg );

                return;
            }

            String msg = "Assuming SINGLE-SIZED (ASYMMETRIC) Levenshtein tables";
            //String msg = "Assuming DOUBLE-SIZED (SYMMETRIC) Levenshtein tables";
            System.out.println( msg ); plog.show( msg );

            // cmd line args
            String db_host             = args[ 0 ];
            String db_user             = args[ 1 ];
            String db_pass             = args[ 2 ];
            String max_threads_str     = args[ 3 ];
            String max_heap_table_size = args[ 4 ];
            String s1_sample_limit     = args[ 5 ];
            String s2_sample_limit     = args[ 6 ];
            String debug_str           = args[ 7 ];

            //System.out.println( "debug_str: '" + debug_str + "'" );
            if( debug_str.equals( "true" ) ) { debug = true; }
            else { debug = false; }
            System.out.println( "debug: '" + debug + "'" );
            if( debug ) {
                dry_run = true;
                use_memory_tables = false;
            }
            // @ FL-18-Feb-2019 hmemleak TEST
            //dry_run = true;
            //use_memory_tables = false;

            System.out.println( "dry_run: '" + dry_run + "'" );
            System.out.println( "use_memory_tables: '" + use_memory_tables + "'" );

            msg = String.format( "db_host: %s, db_username: %s, db_password: %s, max_threads: %s, max_heap_table_size: %s, s1_sample_limit: %s, s2_sample_limit: %s, debug: %s",
                db_host, db_user, db_pass, max_threads_str, max_heap_table_size, s1_sample_limit, s2_sample_limit, debug );
            System.out.println( msg ); plog.show( msg );

            //Properties properties = Functions.getProperties();  // Read properties file

            plog.show( "Matching process started." );
            int max_threads_simul = Integer.parseInt( max_threads_str );
            msg = String.format( "Max simultaneous active matching threads: %d", max_threads_simul );
            System.out.println( msg ); plog.show( msg );

            final Semaphore sem = new Semaphore( max_threads_simul, true );

            int ncores = Runtime.getRuntime().availableProcessors();
            msg = String.format( "Available cores: %d", ncores );
            System.out.println( msg ); plog.show( msg );

            int nthreads_active = java.lang.Thread.activeCount();
            msg = String.format( "Currently active threads: %d", nthreads_active );
            System.out.println( msg ); plog.show( msg );

            System.out.println( "Create database connections" );
            try
            {
                //dbconPrematch = DatabaseManager.getConnection( db_host, dbnamePrematch, db_user, db_pass );
                //dbconMatch    = DatabaseManager.getConnection( db_host, dbnameMatch,    db_user, db_pass );

                HikariCP hikariCP = new HikariCP( hikariConfigPathname, db_host, db_user, db_pass );

                dsrcPrematch = hikariCP.getDataSource( "links_prematch" );
                dsrcMatch    = hikariCP.getDataSource( "links_match" );
                dsrcTemp     = hikariCP.getDataSource( "links_temp" );

                dbconPrematch = dsrcPrematch.getConnection();
                dbconMatch    = dsrcMatch.getConnection();

                DatabaseMetaData meta = dbconPrematch.getMetaData();
                System.out.println( meta.getDatabaseProductName() );
                System.out.println( meta.getDatabaseProductVersion() );

                int networkTimeout = dbconPrematch.getNetworkTimeout();
                System.out.printf( "networkTimeout: %d (milliseconds)\n", networkTimeout );

            }
            catch( Exception ex ) {
                msg = String.format( "Main thread (id %02d); LinksMatchManager/main() Exception: %s", mainThreadId, ex.getMessage() );
                System.out.println( msg );
                ex.printStackTrace();
            }

            try
            {
                String query = "SHOW GLOBAL VARIABLES LIKE 'max_connections'";
                System.out.println( query );
                ResultSet rs = dbconPrematch.createStatement().executeQuery( query );
                rs.first();
                int max_connections = rs.getInt( "Value" );
                msg = String.format( "MySQL max_connections: %s", max_connections );
                System.out.println( msg ); plog.show( msg );

                query = "SHOW GLOBAL VARIABLES LIKE 'max_heap_table_size'";
                System.out.println( query );
                rs = dbconPrematch.createStatement().executeQuery( query );
                rs.first();
                OLD_max_heap_table_size = rs.getString( "Value" );
                msg = String.format( "Getting MySQL max_heap_table_size: %s", OLD_max_heap_table_size );
                System.out.println( msg ); plog.show( msg );
                rs.close();
            }
            catch( Exception ex ) {
                msg = String.format( "Main thread (id %02d); LinksMatchManager/main() Exception: %s", mainThreadId, ex.getMessage() );
                System.out.println( msg );
                ex.printStackTrace();
            }

            msg = String.format( "Setting MySQL max_heap_table_size to: %s", max_heap_table_size );
            System.out.println( msg ); plog.show( msg );
            try {
                String query = "SET max_heap_table_size = " + max_heap_table_size;
                System.out.println( query );
                dbconPrematch.createStatement().execute( query );
            }
            catch( Exception ex ) {
                msg = String.format( "Main thread (id %02d); LinksMatchManager/main() Exception: %s", mainThreadId, ex.getMessage() );
                System.out.println( msg );
                ex.printStackTrace();
            }

            // Create a single QueryGenerator object, that contains the input from the match_process table.
            // The input is derived from the 'y' records in the match_process table.
            // dbconMatch: read match_process table from links_match;
            // dbconPrematch: get record counts for the generated queries for chunking the samples
            QueryGenerator queryGen = new QueryGenerator( plog, dbconPrematch, dbconMatch, s1_sample_limit, s2_sample_limit );

            // The InputSet 'is', is the only accessible object from the queryGen object.
            InputSet inputSet = queryGen.is;
            checkInputSet( inputSet );

            // The inputSet contains an ArrayList< QueryGroupSet >, 1 QueryGroupSet per 'y' record from the match_process table
            int isSize = inputSet.getSize();
            plog.show( String.format( "Number of active records from links_match.match_process: %d\n", isSize ) );
            if( isSize == 0 ) { msg = "Nothing to do; Stop.";  System.out.println( msg ); plog.show( msg ); }

            //msg = "Input test only\nEXIT.";
            //System.out.println( msg ); plog.show( msg );
            //System.exit( 0 );

            // The first ls_firstname and ls_familyname, are copied to a MEMORY database table
            // We require that the 'y' match_process lines have identical ls_firstname and ls_familyname,
            // otherwise we might have to copy 6 ls_ tables to memory, which might becomes too much

            String lvs_table_familyname_use = "";
            String lvs_table_firstname_use  = "";

            String freq_table_familyname_use = "";
            String freq_table_firstname_use  = "";

            QueryGroupSet qgs0 = inputSet.get( 0 );
            QuerySet qs0 = qgs0.get( 0 );

            String lvs_table_familyname = qs0.prematch_familyname;
            String lvs_table_firstname  = qs0.prematch_firstname;

            String freq_table_familyname = "freq_familyname";
            String freq_table_firstname  = "freq_firstname";
           //String freq_table_firstname  = "freq_firstname_sx";    // NOT in match_process table?

            System.out.println( "lvs familyname table: " + lvs_table_familyname );
            System.out.println( "lvs firstname  table: " + lvs_table_firstname );

            System.out.println( "freq familyname table: " + freq_table_familyname );
            System.out.println( "freq firstname  table: " + freq_table_firstname );

            if( use_memory_tables )     // use memory tables
            {
                // levenshtein methods should not change during matching run; check before we go
                for( int n_mp = 0; n_mp < isSize; n_mp++ )
                {
                    QueryGroupSet qgs = inputSet.get( n_mp );

                    for( int j = 0; j < qgs.getSize(); j++ )
                    {
                        QuerySet qs = qgs.get( 0 );     // checking the first is enough
                        String lvs_table_familyname_ij = qs.prematch_familyname;
                        String lvs_table_firstname_ij  = qs.prematch_firstname;

                        if( ! lvs_table_familyname_ij.contentEquals( lvs_table_familyname ) ||
                            ! lvs_table_firstname_ij .contentEquals( lvs_table_firstname  ) ) {
                            String err = "Thou shall not mix different Levenshtein methods within a job\nEXIT.";
                            System.out.println( err ); plog.show( err );
                            System.exit( 1 );
                        }
                    }
                }

                System.out.println( "Using Levenshtein memory tables" );

                // Create memory tables as copies of the normal tables
                // create ls_memory tables
                memtables_ls_create( dbconPrematch, lvs_table_firstname, lvs_table_familyname, name_postfix );
                // create freq__memory tables
                memtables_freq_create( dbconPrematch, freq_table_firstname, freq_table_familyname, name_postfix );

                // and now change the names to the actual table names used !
                lvs_table_familyname_use = lvs_table_familyname + name_postfix;
                lvs_table_firstname_use  = lvs_table_firstname  + name_postfix;

                freq_table_familyname_use = freq_table_familyname + name_postfix;
                freq_table_firstname_use  = freq_table_firstname  + name_postfix;
            }
            else            // do not use memory tables
            {
                System.out.println( "Not using memory tables" );
                lvs_table_familyname_use  = lvs_table_familyname;
                lvs_table_firstname_use   = lvs_table_firstname;
                freq_table_familyname_use = freq_table_familyname;
                freq_table_firstname_use  = freq_table_firstname;
            }

            msg = "Before threading";
            elapsedShowMessage( msg, matchStart, System.currentTimeMillis() );

            int skipped_threads = 0;            // zero sample size(s)
            int total_match_threads = 0;        // total number of threads
            for( int n_mp = 0; n_mp < isSize; n_mp++ )
            {
                QueryGroupSet qgs = inputSet.get( n_mp );
                total_match_threads += qgs.getSize();
            }

            // do not keep connections endlessly open
            if( dbconMatch    != null ) { dbconMatch.close(); }
            if( dbconPrematch != null ) { dbconPrematch.close(); }

            // Loop through the records from the match_process table
            for( int n_mp = 0; n_mp < isSize; n_mp++ )
            {
                System.out.println( "" );
                plog.show( "" );

                show_java_memory();   // show some java memory stats

                // The VariantLoader is broken.
                // And we will get the Levenshtein variants for each name of s1 one-by-one
                /*
                // Create a new prematch variants object for every record in match_process table
                VariantLoader vl = new VariantLoader( dbconPrematch, plog );


                plog.show( String.format( "\nmatching record: %d-of-%d\n", i+1, misSize ) );
                if( ! useExactMatch ) { plog.show( "Notice: skipping exact matches" ); }


                int method = mis.is.get( i ).get( 0 ).method;
                if( method == 1 )       // use root names
                {
                    // Load the name sets
                    rootFamilyName = vl.loadRootNames( mis.is.get( i ).get( 0 ).prematch_familyname );
                    //plog.show( String.format( "rootFamilyName size = %d x %d\n", rootFamilyName[0].length, rootFamilyName[1].length ) );

                    rootFirstName =  vl.loadRootNames( mis.is.get( i ).get( 0 ).prematch_firstname );
                    //plog.show( String.format( "rootFirstName size = %d x %d\n", rootFirstName[0].length, rootFirstName[1].length ) );

                }
                else    // use variant names
                {
                    // Load the name sets
                    variantFamilyName = vl.loadNames(
                        mis.is.get( i ).get( 0 ).prematch_familyname,
                        mis.is.get( i ).get( 0 ).prematch_familyname_value );
                    //plog.show( String.format( "variantFamilyName size = %d x %d\n", variantFamilyName[0].length, variantFamilyName[1].length ) );

                    variantFirstName = vl.loadNames(
                        mis.is.get( i ).get( 0 ).prematch_firstname,
                        mis.is.get( i ).get( 0 ).prematch_firstname_value );
                    //plog.show( String.format( "variantFirstName size = %d x %d\n", variantFirstName[0].length, variantFirstName[1].length ) );
                }
                */

                msg = String.format( "Thread id %02d; Match process record %d of %d", mainThreadId, (n_mp + 1), isSize );
                System.out.println( msg );
                plog.show( msg );     // Show user the active record and total

                // The inputSet contains an ArrayList< QueryGroupSet >, 1 QueryGroupSet per 'y' record from the match_process table.
                // Each QueryGroupSet contains an ArrayList< QuerySet >, all QuerySets refer to the same record from the match_process table.

                // A QuerySet contains a row of the match_process table (although not all variables), plus the generated s1 & s2 queries.
                // In case the QueryGroupSet contains more than 1 QuerySet, the s1 & s2 queries are different:
                // they differ in the lower and upper limit of registration_days.

                // Each QueryGroupSet contains an ArrayList< QuerySet >, all QuerySets refer to the same record from the match_process table.
                QueryGroupSet qgs = inputSet.get( n_mp );

                int nthreads_started = 0;
                //int total_match_threads = isSize * qgs.getSize();
                msg = String.format( "Thread id %02d; Number of matching threads to be used: %d", mainThreadId, total_match_threads );
                System.out.println( msg ); plog.show( msg );

                /*
                if( dry_run ) {
                    msg = String.format( "Thread id %02d; DRY RUN: not deleting old matches, not matching!", mainThreadId );
                    System.out.println( msg ); plog.show( msg );
                    System.exit( 1 );
                }
                */

                // delete the previous matches for this QueryGroupSet;
                // get its match_process table id from its first QuerySet.
                // (the QuerySets only differ in registration_days low and high limits)
                int match_process_id = qgs.get( 0 ).id;

                if( dry_run ) {
                    msg = String.format( "Thread id %02d; DRY RUN: not deleting, nor writing matches!", mainThreadId );
                    System.out.println( msg ); plog.show( msg );
                }
                else {
                    if( dbconMatch == null ) {
                        //dbconMatch = DatabaseManager.getConnection( db_host, dbnameMatch, db_user, db_pass );
                        dbconMatch = dsrcMatch.getConnection();
                    }
                    deleteMatches( match_process_id );
                    dbconMatch.close();
                }

                // Loop through the subsamples
                for( int n_qs = 0; n_qs < qgs.getSize(); n_qs++ )
                {
                    QuerySet qs = qgs.get( n_qs );
                    int mp_id = qs.id;
                    msg = String.format( "Thread id %02d; mp_id %d, subsample %d-of-%d", mainThreadId, mp_id, n_qs + 1, qgs.getSize() );
                    System.out.println( msg ); plog.show( msg );

                    showQuerySet( qs );     // show match_process record parameters

                    long qlStart = System.currentTimeMillis();
                    // Notice: SampleLoader becomes a replacement of QueryLoader, but it is not finished.
                    // Create a new instance of the queryLoader. Queryloader loads the s1 & s2 sample data into Vectors.
                    // Its input is a QuerySet and a database connection object.
                    //ql = new QueryLoader( Thread.currentThread().getId(), qs, dbconPrematch );
                    //ql = new QueryLoader( qs, dbconPrematch );
                    //ql = new QueryLoader( plog, qs, db_host, dbnamePrematch, db_user, db_pass );
                    ql = new QueryLoader( plog, qs, dsrcPrematch );

                    msg = String.format( "Thread id %02d; mp_id %d, subsample %d-of-%d; query loader time", mainThreadId, mp_id, n_qs + 1, qgs.getSize() );
                    elapsedShowMessage( msg, qlStart, System.currentTimeMillis() );
                    show_java_memory();   // show some java memory stats

                    /*
                    long sStart = System.currentTimeMillis();
                    // Notice: SampleLoader becomes a replacement of QueryLoader, but it is not finished.
                    //s1 = new SampleLoader( qs, dbconPrematch, 1 );
                    //s2 = new SampleLoader( qs, dbconPrematch, 2 );
                    s1 = new SampleLoader( qs, db_host, dbnamePrematch, db_user, db_pass, 1 );
                    s2 = new SampleLoader( qs, db_host, dbnamePrematch, db_user, db_pass, 2 );
                    msg = String.format( "Thread id %02d; s1_size: %d, s2_size: %d", mainThreadId, s1.id_base.size(), s2.id_base.size() );
                    System.out.println( msg ); plog.show( msg );
                    s1.freeVectors();
                    s2.freeVectors();
                    msg = String.format( "Thread id %02d; mp_id %d, subsample %d-of-%d; samples loading time", mainThreadId, mp_id, n_qs + 1, qgs.getSize() );
                    elapsedShowMessage( msg, sStart, System.currentTimeMillis() );
                    */

                    /*
                    if( debug ) {
                        msg = "SKIPPING MatchAsync !!!";
                        System.out.println( msg ); plog.show( msg );
                        if( 1 == 1 ) { continue; }
                    }
                    */

                    int s1_size = ql.s1_id_base.size();
                    int s2_size = ql.s2_id_base.size();
                    msg = String.format( "Thread id %02d; s1_size: %d, s2_size: %d", mainThreadId, s1_size, s2_size );
                    System.out.println( msg ); plog.show( msg );

                    if( s1_size == 0 || s2_size == 0 ) {
                        skipped_threads++;
                        msg = String.format( "Thread id %02d; ZERO SAMPLE SIZE for s1 and/or s2, skipping this query set\n", mainThreadId );
                        System.out.println( msg ); plog.show( msg );
                        continue;
                    }

                    // Wait until semaphore gives permission
                    int npermits = sem.availablePermits();
                    msg = String.format( "Thread id %02d; Semaphore: # of permits: %d", mainThreadId, npermits );
                    plog.show( msg );

                    while( ! sem.tryAcquire( 0, TimeUnit.SECONDS ) ) {
                        msg = String.format( "Thread id %02d; No permission for new thread: Waiting 60 seconds", mainThreadId );
                        plog.show( msg );
                        Thread.sleep( 60000 );
                    }

                    npermits = sem.availablePermits();
                    msg = String.format( "Thread id %02d; Semaphore: # of permits: %d", mainThreadId, npermits );
                    plog.show( msg );

                    MatchAsync ma;          // Here begins threading
                    NameLvsVariants nameLvsVariants = new NameLvsVariants();    // but no longer used

                    if( qgs.get( n_qs ).method == 1 )
                    {
                        //ma = new MatchAsync( debug, dry_run, plog, sem, n_mp, n_qs, ql, s1, s2, qgs, inputSet, db_host, db_user, db_pass,
                        ma = new MatchAsync( debug, dry_run, plog, sem, n_mp, n_qs, ql, s1, s2, qgs, inputSet, dsrcPrematch, dsrcMatch, dsrcTemp,
                            lvs_table_firstname_use, lvs_table_familyname_use, freq_table_firstname_use, freq_table_familyname_use,
                            rootFirstName, rootFamilyName, nameLvsVariants, true );
                    }
                    else          // method == 0
                    {
                        //ma = new MatchAsync( debug, dry_run, plog, sem, n_mp, n_qs, ql, s1, s2, qgs, inputSet, db_host, db_user, db_pass,
                        ma = new MatchAsync( debug, dry_run, plog, sem, n_mp, n_qs, ql, s1, s2, qgs, inputSet, dsrcPrematch, dsrcMatch, dsrcTemp,
                            lvs_table_firstname_use, lvs_table_familyname_use, freq_table_firstname_use, freq_table_familyname_use,
                            variantFirstName, variantFamilyName, nameLvsVariants );
                    }

                    ma.start();
                    threads.add( ma );

                    long threadId = ma.getId();
                    System.out.println( "" ); plog.show( "" );
                    msg = String.format( "Thread id %02d; Main(): thread id %02d was started", mainThreadId, threadId );
                    System.out.println( msg ); plog.show( msg );

                    nthreads_started++;
                    msg = String.format( "Thread id %02d; Started matching thread # (not id) %d-of-%d (skipped: %d)",
                        mainThreadId, nthreads_started, total_match_threads, skipped_threads );
                    plog.show( msg );

                } // for subsamples
            } // for 'y' records

            // join the threads: main thread must wait for children to finish
            for( MatchAsync ma : threads ) { ma.join(); }

            msg = String.format( "Thread id %02d; Matching Finished.", mainThreadId );
            System.out.println( msg ); plog.show( msg );

            // the memory tables should only be dropped after all threads have finished.
            if( use_memory_tables ) {
                //dbconPrematch = DatabaseManager.getConnection( db_host, dbnamePrematch, db_user, db_pass );
                dbconPrematch = dsrcPrematch.getConnection();
                memtables_drop( dbconPrematch, lvs_table_familyname,  lvs_table_firstname,  name_postfix );
                memtables_drop( dbconPrematch, freq_table_familyname, freq_table_firstname, name_postfix );

                if( dbconMatch != null ) { dbconMatch.close(); }
            }
            else { msg = "skipping memtables_drop()"; System.out.println( msg ); plog.show( msg ); }

            msg = String.format( "Restoring MySQL max_heap_table_size to initial size: %s", OLD_max_heap_table_size );
            System.out.println( msg ); plog.show( msg );
            try {
                String query = "SET max_heap_table_size = " + OLD_max_heap_table_size;
                dbconPrematch = dsrcPrematch.getConnection();
                dbconPrematch.createStatement().execute( query );
                dbconPrematch.close();
            }
            catch( Exception ex ) {
                msg = String.format( "Thread id %02d; LinksMatchManager/main() Exception: %s", mainThreadId, ex.getMessage() );
                System.out.println( msg );
                ex.printStackTrace();
            }

            if( dsrcPrematch != null ) { dsrcPrematch.close(); };
            if( dsrcMatch    != null ) { dsrcMatch.close(); };
            if( dsrcTemp     != null ) { dsrcTemp.close(); };

            String timestamp3 = getTimeStamp2( "yyyy.MM.dd-HH:mm:ss" );
            plog.show( "Matching was started at: " + timestamp2 );
            plog.show( "Matching now stopped at: " + timestamp3 );

        } // try

        catch( Exception ex ) {
            String msg = String.format( "Main thread (id %02d); LinksMatchManager/main() Exception: %s", mainThreadId, ex.getMessage() );
            System.out.println( msg );
            ex.printStackTrace();
        }

    } // main


    /**
     *
     * @param inputSet
     */
    public static void checkInputSet( InputSet inputSet )
    {
        System.out.println( "checkInputSet()" );

        // The inputSet contains an ArrayList< QueryGroupSet >, 1 QueryGroupSet per 'y' record from the match_process table
        // Each QueryGroupSet contains an ArrayList< QuerySet >, all QuerySets refer to the same record from the match_process table.

        // A QuerySet contains a row of the match_process table (although not all variables), plus the generated s1 & s2 queries.
        // In case the QueryGroupSet contains more than 1 QuerySet, the s1 & s2 queries are different:
        // they differ in the lower and upper limit of registration_days.

        int isSize = inputSet.getSize();
        System.out.println( String.format( "Number of matching records from links_match.match_process: %d", isSize ) );
        System.out.println( String.format( "Notice: s1 & s2 record_counts are the total # of records (without the limits and offsets)" ) );

        // Loop through the records from the match_process table
        for( int n_mp = 0; n_mp < isSize; n_mp++ )
        {
            QueryGroupSet qgs = inputSet.get( n_mp );

            int qgsSize = qgs.getSize();
            for( int n_qs = 0; n_qs < qgsSize; n_qs++ )
            {
                QuerySet qs = qgs.get( n_qs );

                if( n_qs == 0 ) {
                    System.out.println( String.format( "\nmatch process id: %d", qs.id ) );
                    System.out.println( String.format( "Number of QuerySets in QueryGroupSet: %d", qgsSize ) );
                }

                String msg = String.format(
                    "QuerySet: %2d, date_range: %d, s1_record_count: %d, s2_record_count: %d, s1_days_low: %d, s1_days_high: %d, s2_days_low: %d, s2_days_high: %d, s1_limit: %d, s1_offset: %d, s2_limit: %d, s2_offset: %d",
                    n_qs, qs.date_range, qs.s1_record_count, qs.s2_record_count, qs.s1_days_low, qs.s1_days_high, qs.s2_days_low, qs.s2_days_high,
                    qs.s1_limit, qs.s1_offset, qs.s2_limit, qs.s2_offset );
                System.out.println( msg );
            }
        }

        // Loop through the records from the match_process table
        for( int n_mp = 0; n_mp < isSize; n_mp++ )
        {
            QueryGroupSet qgs = inputSet.get( n_mp );

            int qgsSize = qgs.getSize();
            for( int n_qs = 0; n_qs < qgsSize; n_qs++ )
            {
                QuerySet qs = qgs.get( n_qs );

                String msg = String.format(
                    "QuerySet: %2d, int_minmax_e: %03d, chk_ego_birth:     %5s, chk_ego_marriage:     %5s, chk_ego_death:     %5s",
                    n_qs, qs.int_minmax_e, qs.chk_ego_birth, qs.chk_ego_marriage, qs.chk_ego_death );
                System.out.println( msg );

                msg = String.format(
                    "QuerySet: %2d, int_minmax_m: %03d, chk_mother_birth:  %5s, chk_mother_marriage:  %5s, chk_mother_death:  %5s",
                    n_qs, qs.int_minmax_m, qs.chk_mother_birth, qs.chk_mother_marriage, qs.chk_mother_death );
                System.out.println( msg );

                msg = String.format(
                    "QuerySet: %2d, int_minmax_f: %03d, chk_father_birth:  %5s, chk_father_marriage:  %5s, chk_father_death:  %5s",
                    n_qs, qs.int_minmax_f, qs.chk_father_birth, qs.chk_father_marriage, qs.chk_father_death );
                System.out.println( msg );

                msg = String.format(
                    "QuerySet: %2d, int_minmax_p: %03d, chk_partner_birth: %5s, chk_partner_marriage: %5s, chk_partner_death: %5s",
                    n_qs, qs.int_minmax_p, qs.chk_partner_birth, qs.chk_partner_marriage, qs.chk_partner_death );
                System.out.println( msg );
            }
        }
        System.out.println( "" );

    } // checkInputSet


    /**
     * @param format
     * @return
     */
    public static String getTimeStamp2( String format ) {
        Calendar cal = Calendar.getInstance();
        SimpleDateFormat sdf = new SimpleDateFormat( format );
        return sdf.format(cal.getTime());
    } // getTimeStamp2

    public static String millisec2hms( long millisec_start, long millisec_stop ) {
        long millisec = millisec_stop - millisec_start;
        long sec = millisec / 1000;

        long hour = sec / 3600;
        long min = sec / 60;
        long rmin = min - 60 * hour;
        long rsec = sec - ( 60 * ( rmin + 60 * hour ) );

        String hms = "";
        if( hour == 0 ) {
            if( rmin == 0 ) {
                double fsec = ((double)millisec) / 1000.0;
                //hms = String.format("[%d sec]", rsec );
                hms = String.format("[%.1f sec]", fsec );
            }
            else { hms = String.format( "[%02d:%02d mm:ss]", rmin, rsec ); }
        }
        else { hms = String.format( "[%02d:%02d:%02d HH:mm:ss]", hour, rmin, rsec ); }

        return hms;
    } // millisec2hms


    /**
     * @param msg_in
     * @param start
     * @param stop
     */
    private static void elapsedShowMessage( String msg_in, long start, long stop )
    {
        String elapsed = millisec2hms( start, stop );
        String msg_out = msg_in + " " + elapsed + " elapsed";
        System.out.println( msg_out);
        try { plog.show( msg_out ); } catch( Exception ex ) { System.out.println( ex.getMessage()); }
    } // elapsedShowMessage


    private static void show_java_memory()
    {
        final int MegaBytes = 10241024;

        long freeMemory  = Runtime.getRuntime().freeMemory()  / MegaBytes;
        long totalMemory = Runtime.getRuntime().totalMemory() / MegaBytes;
        long maxMemory   = Runtime.getRuntime().maxMemory()   / MegaBytes;

        String[] msgs = {
            "used  memory in JVM: " + ( maxMemory - freeMemory ) + " MB",
            "free  memory in JVM: " + freeMemory + " MB",
            "total memory in JVM: " + totalMemory + " MB",     // shows current size of java heap
            "max   memory in JVM: " + maxMemory + " MB"
        };

        for( String msg : msgs )
        {
            try { plog.show( msg ); }
            catch( Exception ex ) { System.out.println( ex.getMessage() ); }
        }
    } // show_java_memory


    private static void memtables_ls_create( Connection dbcon, String table_firstname_src, String table_familyname_src, String name_postfix )
    {
        try
        {
            String msg = "memtables_ls_create()";
            System.out.println( msg ); plog.show( msg );

            // without backticks
            String table_firstname_dst  = table_firstname_src  + name_postfix;
            String table_familyname_dst = table_familyname_src + name_postfix;

            memtable_ls_name( dbcon, table_firstname_src,  table_firstname_dst );
            memtable_ls_name( dbcon, table_familyname_src, table_familyname_dst );
        }
        catch( Exception ex ) {
            String err = "Exception in memtables_ls_create(): " + ex.getMessage();
            System.out.println( err );
            try { plog.show( err ); } catch( Exception ex2 ) { ; }
        }
    } // memtables_ls_create


    private static void memtables_freq_create( Connection dbcon, String table_firstname_src, String table_familyname_src, String name_postfix )
    {
        try
        {
            String msg = "memtables_freq_create()";
            System.out.println( msg ); plog.show( msg );

            // without backticks
            String table_firstname_dst  = table_firstname_src  + name_postfix;
            String table_familyname_dst = table_familyname_src + name_postfix;

            memtable_freq_name( dbcon, table_firstname_src,  table_firstname_dst );
            memtable_freq_name( dbcon, table_familyname_src, table_familyname_dst );
        }
        catch( Exception ex ) {
            String err = "Exception in memtables_freq_create(): " + ex.getMessage();
            System.out.println( err );
            try { plog.show( err ); } catch( Exception ex2 ) { ; }
        }
    } // memtables_freq_create


    private static void memtables_drop( Connection dbcon, String table_firstname_src, String table_familyname_src, String name_postfix )
    {
        try
        {
            String msg = "memtables_drop()" ;
            System.out.println( msg ); plog.show( msg );

            if( name_postfix.isEmpty()  ) {
                msg = "memtables_drop(): empty name_postfix";
                System.out.println( msg ); plog.show( msg );
                return;
            }

            // without backticks
            String table_firstname_dst  = table_firstname_src  + name_postfix;
            String table_familyname_dst = table_familyname_src + name_postfix;

            if( memtable_ls_exists( dbcon, table_firstname_dst ) )  { memtable_drop( dbcon, table_firstname_dst ); }
            if( memtable_ls_exists( dbcon, table_familyname_dst ) ) { memtable_drop( dbcon, table_familyname_dst ); }
        }
        catch( Exception ex ) {
            String err = "Exception in memtables__drop(): " + ex.getMessage();
            System.out.println( err );
            try { plog.show( err ); } catch( Exception ex2 ) { ; }
        }
    } //memtables_drop


    private static void memtable_drop( Connection dbcon, String table_name )
    {
        try {
            String msg = "memtable_drop() " + table_name;
            System.out.println( msg ); plog.show( msg );

            String query = "DROP TABLE " + table_name;

            dbcon.createStatement().execute( query );
        }
        catch( Exception ex ) {
            String err = "Exception in memtables_drop(): " + ex.getMessage();
            System.out.println( err );
            try { plog.show( err ); } catch( Exception ex2 ) { ; }
        }
    } // memtable_drop


    private static boolean memtable_ls_exists( Connection dbcon, String table_name )
    {
        boolean exists = false;

        String query = "SELECT COUNT(*) AS count FROM information_schema.tables " +
            "WHERE table_schema = 'links_prematch' AND table_name = '" + table_name + "'";

        try {
            ResultSet rs = dbcon.createStatement().executeQuery( query );
            while( rs.next() )
            {
                int count = rs.getInt( "count" );
                if( count == 1 ) { exists = true; }
            }
            rs.close();
        }
        catch( Exception ex ) {
            String err = "Exception in memtable_ls_exists(): " + ex.getMessage();
            System.out.println( err );
            try { plog.show( err ); } catch( Exception ex2 ) { ; }
        }

        return exists;
    } // memtable_ls_exists


    private static void memtable_freq_name( Connection dbcon, String src_table, String dst_table )
    {
        if( memtable_ls_exists( dbcon, dst_table ) ) {
            String msg = "memtable_freq_name() deleting previous " + dst_table;
            System.out.println( msg );
            try { plog.show( msg ); } catch( Exception ex ) { ; }

            memtable_drop( dbcon, dst_table );
        }

        String msg = "memtable_freq_name() copying " + src_table + " -> " + dst_table;
        System.out.println( msg );
        try { plog.show( msg ); } catch( Exception ex ) { ; }

        try
        {
            String[] name_queries =
            {
                "CREATE TABLE " + dst_table
                    + " ( "
                    + " `id` int(10) unsigned NOT NULL AUTO_INCREMENT,"
                    + " `name_str` varchar(100) DEFAULT NULL,"
                    + " `frequency` int(10) unsigned DEFAULT NULL,"
                    + " PRIMARY KEY (`id`)"
                    + " )"
                    + " ENGINE = MEMORY DEFAULT CHARSET = utf8 COLLATE = utf8_bin",

                "TRUNCATE TABLE " + dst_table,

                "ALTER TABLE " + dst_table + " DISABLE KEYS",

                "INSERT INTO " + dst_table + " SELECT * FROM " + src_table,

                "ALTER TABLE " + dst_table + " ENABLE KEYS"
            };

            for( String query : name_queries ) { dbcon.createStatement().execute( query ); }
        }
        catch( Exception ex ) {
            String err = ex.getMessage();
            msg = "Exception in memtable_freq_name(): " + err;
            System.out.println( msg );

            try {
                plog.show( msg );
                System.out.println( "EXIT" ); plog.show( "EXIT" );
                System.exit( 1 );       // should not continue; would give wrong matching results.
            }
            catch( Exception ex1 ) {
                System.out.println( "EXIT" );
                System.exit( 1 );
            }
        }
    } // memtable_freq_name


    private static void memtable_ls_name( Connection dbcon, String src_table, String dst_table )
    {
        if( memtable_ls_exists( dbcon, dst_table ) ) {
            String msg = "memtable_ls_name() deleting previous " + dst_table;
            System.out.println( msg );
            try { plog.show( msg ); } catch( Exception ex ) { ; }

            memtable_drop( dbcon, dst_table );
        }

        String msg = "memtable_ls_name() copying " + src_table + " -> " + dst_table;
        System.out.println( msg );
        try { plog.show( msg ); } catch( Exception ex ) { ; }


        // Notice: leave out original keys if we do not need them now
        /*
                    + "  PRIMARY KEY (`id`),"
                    + "  KEY `value` (`value`),"
                    + "  KEY `length_1` (`length_1`),"
                    + "  KEY `length_2` (`length_2`),"
                    + "  KEY `name_str_1` (`name_str_1`),"
                    + "  KEY `name_str_2` (`name_str_2`),"
                    + "  KEY `name_int_1` (`name_int_1`)"
        */

        try
        {
            String[] name_queries =
            {
                "CREATE TABLE " + dst_table
                    + " ( "
                    + " `id` int(10) unsigned NOT NULL AUTO_INCREMENT,"
                    + "  `name_str_1` varchar(100) COLLATE utf8_bin DEFAULT NULL,"
                    + "  `name_str_2` varchar(100) COLLATE utf8_bin DEFAULT NULL,"
                    + "  `length_1` mediumint(8) unsigned DEFAULT NULL,"
                    + "  `length_2` mediumint(8) unsigned DEFAULT NULL,"
                    + "  `name_int_1` int(11) DEFAULT NULL,"
                    + "  `name_int_2` int(11) DEFAULT NULL,"
                    + "  `value` tinyint(3) unsigned DEFAULT NULL,"
                    + "  PRIMARY KEY (`id`),"
                    + "  KEY `name_int_1` (`name_int_1`),"
                    + "  KEY `name_int_2` (`name_int_2`),"
                    + "  KEY `value` (`value`)"
                    + " )"
                    + " ENGINE = MEMORY DEFAULT CHARSET = utf8 COLLATE = utf8_bin",

                "TRUNCATE TABLE " + dst_table,

                "ALTER TABLE " + dst_table + " DISABLE KEYS",

                "INSERT INTO " + dst_table + " SELECT * FROM " + src_table,

                "ALTER TABLE " + dst_table + " ENABLE KEYS"
            };

            for( String query : name_queries ) { dbcon.createStatement().execute( query ); }
        }
        catch( Exception ex ) {
            String err = ex.getMessage();
            msg = "Exception in memtable_ls_name(): " + err;
            System.out.println( msg );

            try {
                plog.show( msg );
                System.out.println( "EXIT" ); plog.show( "EXIT" );
                System.exit( 1 );       // should not continue; would give wrong matching results.
            }
            catch( Exception ex1 ) {
                System.out.println( "EXIT" );
                System.exit( 1 );
            }
        }
    } // memtable_ls_name


    /**
     * Delete previous matches for the given id, before creating new matches
     * @param id_match_process
     */
    private static void deleteMatches( int id_match_process )
    {
        String query = "DELETE FROM matches WHERE id_match_process = " + id_match_process;

        try
        {
            if( dbconMatch == null ) { dbconMatch = dsrcMatch.getConnection(); }

            plog.show( String.format( "Deleting matches for match_process id: %d", id_match_process ) );

            dbconMatch.createStatement().execute( query );
        }
        catch( Exception ex ) {
            System.out.println( query ); try { plog.show( query ); } catch( Exception ex2 ) { ; }

            long threadId = Thread.currentThread().getId();
            String err = String.format( "Thread id %02d; LinksMatchManager/deleteMatches() Exception: %s", threadId, ex.getMessage() );
            System.out.println( err );
            try { plog.show( err ); } catch( Exception ex2 ) { ; }
        }
    } // deleteMatches


    /**
     *
     * @param qs
     */
    private static void showQuerySet( QuerySet qs )
    {
        long threadId = Thread.currentThread().getId();

        try
        {
            plog.show( String.format( "Thread id %02d; match_process settings: for id = %d", threadId, qs.id ) );
            plog.show( String.format( "Thread id %02d; id = %d", threadId, qs.id ) );
            plog.show( String.format( "Thread id %02d; query1 = %s", threadId, qs.s1_query ) );
            plog.show( String.format( "Thread id %02d; query2 = %s", threadId, qs.s2_query ) );

            plog.show( String.format( "Thread id %02d; s1_days_low ............. = %s", threadId, qs.s1_days_low ) );
            plog.show( String.format( "Thread id %02d; s1_days_high ............ = %s", threadId, qs.s1_days_high ) );
            plog.show( String.format( "Thread id %02d; s2_days_low ............. = %s", threadId, qs.s2_days_low ) );
            plog.show( String.format( "Thread id %02d; s2_days_high ............ = %s", threadId, qs.s2_days_high ) );

            plog.show( String.format( "Thread id %02d; use_mother .............. = %s", threadId, qs.use_mother ) );
            plog.show( String.format( "Thread id %02d; use_father .............. = %s", threadId, qs.use_father ) );
            plog.show( String.format( "Thread id %02d; use_partner ............. = %s", threadId, qs.use_partner ) );

            plog.show( String.format( "Thread id %02d; method .................. = %d", threadId, qs.method ) );

            plog.show( String.format( "Thread id %02d; ignore_sex .............. = %s", threadId, qs.ignore_sex ) );
            plog.show( String.format( "Thread id %02d; ignore_minmax ........... = %s", threadId, qs.ignore_minmax ) );
            plog.show( String.format( "Thread id %02d; firstname (method)....... = %d", threadId, qs.firstname_method ) );

            plog.show( String.format( "Thread id %02d; prematch_familyname ..... = %s", threadId, qs.prematch_familyname ) );
            plog.show( String.format( "Thread id %02d; prematch_familyname_value = %d", threadId, qs.lvs_dist_max_familyname ) );
            plog.show( String.format( "Thread id %02d; prematch_firstname ...... = %s", threadId, qs.prematch_firstname ) );
            plog.show( String.format( "Thread id %02d; prematch_firstname_value  = %d", threadId, qs.lvs_dist_max_firstname ) );

            //plog.show( String.format( "Thread id %02d; use_familyname .......... = %s", threadId, qs.use_familyname ) );
            //plog.show( String.format( "Thread id %02d; use_firstname ........... = %s", threadId, qs.use_firstname ) );
            //plog.show( String.format( "Thread id %02d; use_minmax .............  = %s", threadId, qs.use_minmax ) );

            plog.show( String.format( "Thread id %02d; int_familyname_e ........ = %d", threadId, qs.int_familyname_e ) );
            plog.show( String.format( "Thread id %02d; int_familyname_m .......  = %d", threadId, qs.int_familyname_m ) );
            plog.show( String.format( "Thread id %02d; int_familyname_f ........ = %d", threadId, qs.int_familyname_f ) );
            plog.show( String.format( "Thread id %02d; int_familyname_p ........ = %d", threadId, qs.int_familyname_p ) );

            plog.show( String.format( "Thread id %02d; int_firstname_e ......... = %d", threadId, qs.int_firstname_e ) );
            plog.show( String.format( "Thread id %02d; int_firstname_m ......... = %d", threadId, qs.int_firstname_m ) );
            plog.show( String.format( "Thread id %02d; int_firstname_f ......... = %d", threadId, qs.int_firstname_f ) );
            plog.show( String.format( "Thread id %02d; int_firstname_p ......... = %d", threadId, qs.int_firstname_p ) );

            plog.show( String.format( "Thread id %02d; int_minmax_e ............ = %d", threadId, qs.int_minmax_e ) );
            plog.show( String.format( "Thread id %02d; int_minmax_m ............ = %d", threadId, qs.int_minmax_m ) );
            plog.show( String.format( "Thread id %02d; int_minmax_f ............ = %d", threadId, qs.int_minmax_f ) );
            plog.show( String.format( "Thread id %02d; int_minmax_p ............ = %d", threadId, qs.int_minmax_p ) );
        }
        catch( Exception ex ) { ex.getMessage(); }
    } // showQuerySet

}
