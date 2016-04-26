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

import java.sql.Connection;
import java.sql.ResultSet;

import linksmatchmanager.DataSet.InputSet;
import linksmatchmanager.DataSet.QueryGroupSet;
import linksmatchmanager.DataSet.QuerySet;

/**
 * @author Omar Azouguagh
 * @author Fons Laan
 *
 * FL-30-Jun-2014 Imported from OA backup
 * FL-15-Jan-2015 Each thread its own db connectors
 * FL-06-Apr-2015 Latest change
 */

public class MatchMain
{
    // class global vars
    private static boolean debug;

    private static QueryLoader ql;
    private static PrintLogger plog;

    private static Connection dbconPrematch;
    private static Connection dbconMatch;
    private static Connection dbconTemp;

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
        boolean debugrange = false;

        boolean use_memory_tables = true;
        String name_postfix = "_mem";

        if( debugrange ) { use_memory_tables = false; }

        int s1_split_limit = 4;
        long mainThreadId = Thread.currentThread().getId();
        ArrayList< MatchAsync > threads = new ArrayList();

        try
        {
            plog = new PrintLogger( "LMM-" );

            long matchStart = System.currentTimeMillis();
            String timestamp1 = "06-Apr-2016 12:02";
            String timestamp2 = getTimeStamp2( "yyyy.MM.dd-HH:mm:ss" );
            plog.show( "Links Match Manager 2.0 timestamp: " + timestamp1 );
            plog.show( "Start at: " + timestamp2 );

            String version = System.getProperty( "java.version" );
            plog.show( "Java version: " + version );
            System.out.println( "Java version: " + version );

            //String dbg = String.format( "Main thread (id %d); Matching EXIT." );
            //System.out.println( dbg ); plog.show( dbg );
            //System.exit( 0 );

            // Load arguments; check length
            if( args.length != 6 ) {
                String msg ="Invalid argument length " + args.length + ", it should be 6";
                plog.show( msg ); System.out.println( msg );

                msg = "Usage: java -jar LinksMatchManager-2.0.jar <db_url> <db_username> <db_password> <max_threads> <debug>";
                plog.show( msg ); System.out.println( msg );

                return;
            }

            // cmd line args
            String url                 = args[ 0 ];
            String user                = args[ 1 ];
            String pass                = args[ 2 ];
            String max_threads_str     = args[ 3 ];
            String max_heap_table_size = args[ 4 ];
            String debug_str           = args[ 5 ];

            //System.out.println( "debug_str: '" + debug_str + "'" );
            if( debug_str.equals( "true" ) ) { debug = true; }
            else { debug = false; }
            //System.out.println( "debug: '" + debug + "'" );


            String msg = String.format( "db_url: %s, db_username: %s, db_password: %s, max_threads: %s, max_heap_table_size: %s, debug: %s",
                url, user, pass, max_threads_str, max_heap_table_size, debug );
            System.out.println( msg );
            plog.show( msg );

            //Properties properties = Functions.getProperties();  // Read properties file

            plog.show( "Matching process started." );
            int max_threads_simul = Integer.parseInt( max_threads_str );
            msg = String.format( "Max simultaneous active matching threads: %d", max_threads_simul );
            System.out.println( msg ); plog.show( msg );

            //ProcessManager pm = new ProcessManager( max_threads_simul );
            final Semaphore pm = new Semaphore( max_threads_simul, true );

            int ncores = Runtime.getRuntime().availableProcessors();
            msg = String.format( "Available cores: %d", ncores );
            System.out.println( msg ); plog.show( msg );

            int nthreads_active = java.lang.Thread.activeCount();
            msg = String.format( "Currently active threads: %d", nthreads_active );
            System.out.println( msg ); plog.show( msg );

            // Create database connections
            dbconMatch    = General.getConnection( url, "links_match", user, pass );
            dbconPrematch = General.getConnection( url, "links_prematch", user, pass );
            dbconTemp     = General.getConnection( url, "links_temp", user, pass );

            // we now use links_prematch for tmp mem tables, so it must be writable
            //dbconPrematch.setReadOnly( true );                // Set read only

            msg = String.format( "Setting MySQL max_heap_table_size to: %s", max_heap_table_size );
            System.out.println( msg );
            plog.show( msg );
            try
            {
                String query = "SET max_heap_table_size = " + max_heap_table_size;
                dbconPrematch.createStatement().execute( query );
            }
            catch( Exception ex ) { System.out.println( "Exception in main(): " + ex.getMessage() ); }

            // Create a single QueryGenerator object, that contains the input from the match_process table.
            // The input is derived from the 'y' records in the match_process table.
            QueryGenerator queryGen = new QueryGenerator( plog, dbconMatch );

            // The InputSet 'is', is the only accessible object from queryGen
            InputSet inputSet = queryGen.is;
            checkInputSet( inputSet );

            // The inputSet contains an ArrayList< QueryGroupSet >, 1 QueryGroupSet per 'y' record from the match_process table
            int isSize = inputSet.getSize();
            plog.show( String.format( "Number of active records from links_match.match_process: %d\n", isSize ) );
            if( isSize == 0 ) {
                System.out.println( "Nothing to do; Stop." );
                plog.show( "Nothing to do; Stop." );
            }

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

                // not used  here
                //int lvs_dist_familyname = qs0.prematch_familyname_value;
                //int lvs_dist_firstname  = qs0.prematch_firstname_value;
                //System.out.println( "lvs familyname distance: " + lvs_dist_familyname );
                //System.out.println( "lvs firstname  distance: " + lvs_dist_firstname );

                // Create memory tables as copies of the normal tables
                // create ls_memory tables
                memtables_ls_create( lvs_table_firstname, lvs_table_familyname, name_postfix );
                // create freq__memory tables
                memtables_freq_create( freq_table_firstname, freq_table_familyname, name_postfix );

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


            // Loop through the records from the match_process table
            for( int n_mp = 0; n_mp < isSize; n_mp++ )
            {
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

                msg = "Match process record " + (n_mp + 1) + " of " + isSize;
                System.out.println( msg );
                plog.show( msg );     // Show user the active record and total

                // The inputSet contains an ArrayList< QueryGroupSet >, 1 QueryGroupSet per 'y' record from the match_process table.
                // Each QueryGroupSet contains an ArrayList< QuerySet >, all QuerySets refer to the same record from the match_process table.

                // A QuerySet contains a row of the match_process table (although not all variables), plus the generated s1 & s2 queries.
                // In case the QueryGroupSet contains more than 1 QuerySet, the s1 & s2 queries are different:
                // they differ in the lower and upper limit of registration_days.

                // Each QueryGroupSet contains an ArrayList< QuerySet >, all QuerySets refer to the same record from the match_process table.
                QueryGroupSet qgs = inputSet.get( n_mp );

                int num_s1_parts = 0;   // the number of parts into which s1 will be split
                if( isSize * qgs.getSize() > s1_split_limit )       // split s1 if we have have few threads (but not 1 )
                {  num_s1_parts = 1; }   // do not split s1 into separate pieces
                else
                { num_s1_parts = max_threads_simul; }

                if( num_s1_parts == 1 ) {
                    msg = String.format( "sample s1 not split" );
                    System.out.println( msg); plog.show( msg );
                }
                else {
                    msg = String.format( "sample s1 split into %d parts", num_s1_parts );
                    System.out.println( msg); plog.show( msg );
                }

                boolean free_vecs = false;
                if( num_s1_parts == 1 ) { free_vecs = true; }

                int total_match_threads = isSize * qgs.getSize() * num_s1_parts;
                msg = String.format( "number of matching threads to be used: %d", total_match_threads );
                System.out.println( msg ); plog.show( msg );

                // delete the previous matches for this QueryGroupSet;
                // get its match_process table id from its first QuerySet.
                // (the QuerySets only differ in registration_days low and high limits)
                int match_process_id = qgs.get( 0 ).id;
                deleteMatches( match_process_id );

                int range_counter = 0;  // debug
                // Loop through the ranges/subqueries
                for( int n_qs = 0; n_qs < qgs.getSize(); n_qs++ )
                {
                    msg = String.format( "Range %d-of-%d", n_qs + 1, qgs.getSize() );
                    System.out.println( msg ); plog.show( msg );

                    QuerySet qs = qgs.get( n_qs );
                    showQuerySet( qs );

                    long qlStart = System.currentTimeMillis();
                    // Notice: SampleLoader becomes a replacement ofQueryLoader, but it is not finished
                    // Create a new instance of the queryLoader. Queryloader is used to use the queries to load data into the sets.
                    // Its input is a QuerySet and a database connection object.
                    ql = new QueryLoader( Thread.currentThread().getId(), qs, dbconPrematch );
                    msg = String.format( "Range %d-of-%d; query loader time", n_qs+1, qgs.getSize() );
                    elapsedShowMessage( msg, qlStart, System.currentTimeMillis() );

                    int s1_size = ql.s1_id_base.size();
                    msg = String.format( "s1_size: " + s1_size );
                    System.out.println( msg ); plog.show( msg );

                    int s2_size = ql.s2_id_base.size();
                    msg = String.format( "s2_size: " + s2_size );
                    System.out.println( msg ); plog.show( msg );

                    if( s1_size == 0 || s2_size == 0 ) {
                        msg = "ZERO SAMPLE SIZE for s1 and/or s2, skipping this query set\n";
                        System.out.println( msg ); plog.show( msg );
                        continue;
                    }

                    int s1_chunksize = s1_size / num_s1_parts;  // how many records from sample 1 to use in 1 thread
                    int s1_piece;                               // number of s1 records for individual thread
                    int nthreads_started = 0;                   // number of matching threads started

                    int s1part_counter = 0;  // debug

                    //int processCount = pm.getProcessCount();
                    int processCount = pm.availablePermits();
                    plog.show( "processCount: " + processCount );

                    // Loop through the s1 parts
                    for( int s1_ipart = 0; s1_ipart < num_s1_parts; s1_ipart++ )
                    {
                        int s1_npart = s1_ipart + 1;
                        int s1_offset = s1_ipart * s1_chunksize;       // where to start in sample 1

                        if( s1_npart == max_threads_simul ) { s1_piece = s1_size - s1_offset; }
                        else { s1_piece = s1_chunksize;  }

                        msg = String.format( "range: %d, s1 part: %d-of-%d, offset: %d, s1 piece size: %d",
                            n_qs+1, s1_npart, max_threads_simul, s1_offset, s1_piece );
                        System.out.println( msg ); plog.show( msg );

                        if( debugrange ) { continue; }

                        // Wait until process manager gives permission
                        /*
                        while( ! pm.allowProcess() ) {
                         plog.show( "No permission for new thread: Waiting 60 seconds" );
                            Thread.sleep( 60000 );
                        }
                        processCount = pm.addProcess();        // Add a process to process list
                        */
                        while( ! pm.tryAcquire( 0, TimeUnit.SECONDS ) ) {
                            plog.show( "No permission for new thread: Waiting 60 seconds" );
                            Thread.sleep( 60000 );
                        }
                        processCount = pm.availablePermits();
                        plog.show( "processCount: " + processCount );

                        MatchAsync ma;          // Here begins threading

                        if( qgs.get( n_qs ).method == 1 )
                        {
                          //ma = new MatchAsync( debug, free_vecs, pm, n_mp, n_qs, ql, plog, qgs, inputSet, s1_offset, s1_piece, dbconPrematch, dbconMatch, dbconTemp,
                          //    lvs_table_firstname_use, lvs_table_familyname_use, freq_table_firstname_use, freq_table_familyname_use, rootFirstName, rootFamilyName, true );
                            ma = new MatchAsync( debug, free_vecs, pm, n_mp, n_qs, ql, plog, qgs, inputSet, s1_offset, s1_piece, url, user, pass,
                                lvs_table_firstname_use, lvs_table_familyname_use, freq_table_firstname_use, freq_table_familyname_use, rootFirstName, rootFamilyName, true );
                        }
                        else          // method == 0
                        {
                          //ma = new MatchAsync( debug, free_vecs, pm, n_mp, n_qs, ql, plog, qgs, inputSet, s1_offset, s1_piece, dbconPrematch, dbconMatch, dbconTemp,
                          //    lvs_table_firstname_use, lvs_table_familyname_use, freq_table_firstname_use, freq_table_familyname_use, variantFirstName, variantFamilyName );
                            ma = new MatchAsync( debug, free_vecs, pm, n_mp, n_qs, ql, plog, qgs, inputSet, s1_offset, s1_piece, url, user, pass,
                                lvs_table_firstname_use, lvs_table_familyname_use, freq_table_firstname_use, freq_table_familyname_use, variantFirstName, variantFamilyName );
                        }

                        ma.start();
                        threads.add( ma );

                        long threadId = ma.getId();
                        msg = String.format( "\nMain(): thread id %2d was started", threadId );
                        System.out.println( msg ); plog.show( msg );

                        nthreads_started++;
                        plog.show( String.format( "Started matching thread # (not id) %d-of-%d", nthreads_started, max_threads_simul ) );

                        s1part_counter++;
                    } // for s1 parts
                    plog.show( String.format( "%d s1_parts processed", s1part_counter ) ); // debug

                    range_counter++;
                } // for ranges
                plog.show( String.format( "%d ranges processed", range_counter ) ); // debug
            } // for 'y' records

            // join the threads: main thread must wait for children to finish
            for( MatchAsync ma : threads ) { ma.join(); }

            msg = String.format( "Main thread (id %d); Matching Finished.", mainThreadId );
            System.out.println( msg ); plog.show( msg );

            // the memory tables should only be dropped after all threads have finished.
            if( use_memory_tables ) {
                memtables_drop( lvs_table_familyname,  lvs_table_firstname,  name_postfix );
                memtables_drop( freq_table_familyname, freq_table_firstname, name_postfix );
            }
            else { msg = "skipping memtables_drop()"; System.out.println( msg ); plog.show( msg ); }

            dbconPrematch.close();
            dbconMatch.close();
            dbconTemp.close();

            String timestamp3 = getTimeStamp2( "yyyy.MM.dd-HH:mm:ss" );
            plog.show( "Matching was started at: " + timestamp2 );
            plog.show( "Matching now stopped at: " + timestamp3 );

        } // try

        catch( Exception ex )
        { System.out.println( "LinksMatchManager/main() Exception: " + ex.getMessage() ); }

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

                String msg = String.format( "QuerySet :%2d, s1_days_low: %d, 1_days_high: %d, s2_days_low: %d, s2_days_high: %d",
                    n_qs, qs.s1_days_low, qs.s1_days_high, qs.s2_days_low, qs.s2_days_high );
                System.out.println( msg );
            }
        }
    } // heckInputSet


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


    private static void memtables_ls_create( String table_firstname_src, String table_familyname_src, String name_postfix )
    {
        try
        {
            String msg = "memtables_ls_create()";
            System.out.println( msg ); plog.show( msg );

            // without backticks
            String table_firstname_dst  = table_firstname_src  + name_postfix;
            String table_familyname_dst = table_familyname_src + name_postfix;

            memtable_ls_name( table_firstname_src,  table_firstname_dst );
            memtable_ls_name( table_familyname_src, table_familyname_dst );
        }
        catch( Exception ex ) {
            String err = "Exception in memtables_ls_create(): " + ex.getMessage();
            System.out.println( err );
            try { plog.show( err ); } catch( Exception ex2 ) { ; }
        }
    } // memtables_ls_create


    private static void memtables_freq_create( String table_firstname_src, String table_familyname_src, String name_postfix )
    {
        try
        {
            String msg = "memtables_freq_create()";
            System.out.println( msg ); plog.show( msg );

            // without backticks
            String table_firstname_dst  = table_firstname_src  + name_postfix;
            String table_familyname_dst = table_familyname_src + name_postfix;

            memtable_freq_name( table_firstname_src,  table_firstname_dst );
            memtable_freq_name( table_familyname_src, table_familyname_dst );
        }
        catch( Exception ex ) {
            String err = "Exception in memtables_freq_create(): " + ex.getMessage();
            System.out.println( err );
            try { plog.show( err ); } catch( Exception ex2 ) { ; }
        }
    } // memtables_freq_create


    private static void memtables_drop( String table_firstname_src, String table_familyname_src, String name_postfix )
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

            if( memtable_ls_exists( table_firstname_dst ) )  { memtable_drop( table_firstname_dst ); }
            if( memtable_ls_exists( table_familyname_dst ) ) { memtable_drop( table_familyname_dst ); }
        }
        catch( Exception ex ) {
            String err = "Exception in memtables__drop(): " + ex.getMessage();
            System.out.println( err );
            try { plog.show( err ); } catch( Exception ex2 ) { ; }
        }
    } //memtables_drop


    private static void memtable_drop( String table_name )
    {
        try {
            String msg = "memtable_drop() " + table_name;
            System.out.println( msg ); plog.show( msg );

            String query = "DROP TABLE " + table_name;

            dbconPrematch.createStatement().execute( query );
        }
        catch( Exception ex ) {
            String err = "Exception in memtables_drop(): " + ex.getMessage();
            System.out.println( err );
            try { plog.show( err ); } catch( Exception ex2 ) { ; }
        }
    } // memtable_drop


    private static boolean memtable_ls_exists( String table_name )
    {
        boolean exists = false;

        String query = "SELECT COUNT(*) AS count FROM information_schema.tables " +
            "WHERE table_schema = 'links_prematch' AND table_name = '" + table_name + "'";

        try {
            ResultSet rs = dbconPrematch.createStatement().executeQuery( query );
            while( rs.next() )
            {
                int count = rs.getInt( "count" );
                if( count == 1 ) { exists = true; }
            }
        }
        catch( Exception ex ) {
            String err = "Exception in emtable_ls_exists(): " + ex.getMessage();
            System.out.println( err );
            try { plog.show( err ); } catch( Exception ex2 ) { ; }
        }

        return exists;
    } // memtable_ls_exists


    private static void memtable_freq_name( String src_table, String dst_table )
    {
        if( memtable_ls_exists( dst_table ) ) {
            String msg = "memtable_freq_name() deleting previous " + dst_table;
            System.out.println( msg );
            try { plog.show( msg ); } catch( Exception ex ) { ; }

            memtable_drop( dst_table );
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
                    + " PRIMARY KEY (`id`),"
                    + " KEY `name_str` (`name_str`)"
                    + " )"
                    + " ENGINE = MEMORY DEFAULT CHARSET = utf8 COLLATE = utf8_bin",

                "TRUNCATE TABLE " + dst_table,

                "ALTER TABLE " + dst_table + " DISABLE KEYS",

                "INSERT INTO " + dst_table + " SELECT * FROM " + src_table,

                "ALTER TABLE " + dst_table + " ENABLE KEYS"
            };

            for( String query : name_queries ) { dbconPrematch.createStatement().execute( query ); }
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


    private static void memtable_ls_name( String src_table, String dst_table )
    {
        if( memtable_ls_exists( dst_table ) ) {
            String msg = "memtable_ls_name() deleting previous " + dst_table;
            System.out.println( msg );
            try { plog.show( msg ); } catch( Exception ex ) { ; }

            memtable_drop( dst_table );
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
                    + "  KEY `value` (`value`),"
                    + "  KEY `name_int_1` (`name_int_1`)"
                    + " )"
                    + " ENGINE = MEMORY DEFAULT CHARSET = utf8 COLLATE = utf8_bin",

                "TRUNCATE TABLE " + dst_table,

                "ALTER TABLE " + dst_table + " DISABLE KEYS",

                "INSERT INTO " + dst_table + " SELECT * FROM " + src_table,

                "ALTER TABLE " + dst_table + " ENABLE KEYS"
            };

            for( String query : name_queries ) { dbconPrematch.createStatement().execute( query ); }
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
        try {
            plog.show( String.format( "Deleting matches for match_process id: %d", id_match_process ) );
            String query = "DELETE FROM matches WHERE id_match_process = " + id_match_process;

            dbconMatch.createStatement().execute( query );
            dbconMatch.createStatement().close();
        }
        catch( Exception ex ) {
            String err = "LinksMatchManager/deleteMatches() Exception: " + ex.getMessage();
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
        try
        {
            plog.show( "match_process settings:" );
            plog.show( String.format( "id = %d", qs.id ) );
            plog.show( String.format( "query1 = %s", qs.query1 ) );
            plog.show( String.format( "query2 = %s", qs.query2 ) );

            plog.show( String.format( "s1_days_low ............. = %s", qs.s1_days_low ) );
            plog.show( String.format( "s1_days_high ............ = %s", qs.s1_days_high ) );
            plog.show( String.format( "s2_days_low ............. = %s", qs.s2_days_low ) );
            plog.show( String.format( "s2_days_high ............ = %s", qs.s2_days_high ) );

            plog.show( String.format( "use_mother .............. = %s", qs.use_mother ) );
            plog.show( String.format( "use_father .............. = %s", qs.use_father ) );
            plog.show( String.format( "use_partner ............. = %s", qs.use_partner ) );

            plog.show( String.format( "method .................. = %d", qs.method ) );

            plog.show( String.format( "ignore_sex .............. = %s", qs.ignore_sex ) );
            plog.show( String.format( "ignore_minmax ........... = %s", qs.ignore_minmax ) );
            plog.show( String.format( "firstname ............... = %d", qs.firstname ) );

            plog.show( String.format( "prematch_familyname ..... = %s", qs.prematch_familyname ) );
            plog.show( String.format( "prematch_familyname_value = %d", qs.prematch_familyname_value ) );
            plog.show( String.format( "prematch_firstname ...... = %s", qs.prematch_firstname ) );
            plog.show( String.format( "prematch_firstname_value  = %d", qs.prematch_firstname_value ) );

            //plog.show( String.format( "use_familyname .......... = %s", qs.use_familyname ) );
            //plog.show( String.format( "use_firstname ........... = %s", qs.use_firstname ) );
            //plog.show( String.format( "use_minmax .............  = %s", qs.use_minmax ) );

            plog.show( String.format( "int_familyname_e ........ = %d", qs.int_familyname_e ) );
            plog.show( String.format( "int_familyname_m .......  = %d", qs.int_familyname_m ) );
            plog.show( String.format( "int_familyname_f ........ = %d", qs.int_familyname_f ) );
            plog.show( String.format( "int_familyname_p ........ = %d", qs.int_familyname_p ) );

            plog.show( String.format( "int_firstname_e ......... = %d", qs.int_firstname_e ) );
            plog.show( String.format( "int_firstname_m ......... = %d", qs.int_firstname_m ) );
            plog.show( String.format( "int_firstname_f ......... = %d", qs.int_firstname_f ) );
            plog.show( String.format( "int_firstname_p ......... = %d", qs.int_firstname_p ) );

            plog.show( String.format( "int_minmax_e ............ = %d", qs.int_minmax_e ) );
            plog.show( String.format( "int_minmax_m ............ = %d", qs.int_minmax_m ) );
            plog.show( String.format( "int_minmax_f ............ = %d", qs.int_minmax_f ) );
            plog.show( String.format( "int_minmax_p ............ = %d", qs.int_minmax_p ) );
        }
        catch( Exception ex ) { ex.getMessage(); }
    } // showQuerySet

}