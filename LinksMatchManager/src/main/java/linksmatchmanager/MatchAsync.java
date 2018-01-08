package linksmatchmanager;

import java.io.File;
import java.io.FileWriter;
import java.io.PrintStream;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;

import java.sql.Connection;
import java.sql.ResultSet;

import java.util.Collection;
import java.util.concurrent.Semaphore;
import java.util.List;
import java.util.Vector;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Supplier;
import com.google.common.collect.Lists;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Multimaps;
import com.google.common.collect.Multiset;
import com.google.common.collect.Multiset.Entry;
import com.google.common.collect.Collections2;

import linksmatchmanager.DataSet.InputSet;
import linksmatchmanager.DataSet.NameLvsVariants;
import linksmatchmanager.DataSet.NameType;
import linksmatchmanager.DataSet.QueryGroupSet;
import linksmatchmanager.DataSet.QuerySet;
import linksmatchmanager.DatabaseManager;

/**
 * @author Omar Azouguagh
 * @author Fons Laan
 *
 * FL-15-Jan-2015 Each thread its own db connectors
 * FL-25-Jul-2017 Debug run
 * FL-23-Aug-2017 chk_* flags from qs object
 * FL-13-Nov-2017 dbconMatchLocal
 * FL-08-Jan-2018 Math.max(lvs1, lvs2)  maximum, not summing
 *
 * "Vectors are synchronized. Any method that touches the Vector's contents is thread safe.
 * ArrayList, on the other hand, is unsynchronized, making them, therefore, not thread safe."
 *  -> So in MatchAsync we should use Vectors
 */

public class MatchAsync extends Thread
{
    // efficient: static final boolean false blocks will be removed during compilation
    static final boolean match2csv = true;      // collect all matches of current thread in csv file, and write to table in one go

    static final boolean debugfail = false;     // debug match failures
    static final boolean debugfreq = false;     // debug name frequencies

    //boolean debugfail = false;     // debug match failures
    //boolean debugfreq = false;     // debug name frequencies

    boolean debug;
    boolean dry_run;

    Semaphore sem;

    int n_mp;           // match_process 'y' records: 0...
    int n_qs;           // query sets: 0...

    QueryLoader ql;     // contains s1 & s2 samples from links_base
    SampleLoader s1;    // sample 1
    SampleLoader s2;    // sample 2

    PrintLogger plog;

    QueryGroupSet qgs;
    InputSet inputSet;

    String db_host;
    String db_user;
    String db_pass;

    String lvs_table_firstname;
    String lvs_table_familyname;

    String freq_table_firstname;
    String freq_table_familyname;

    int[][] variantFirstName;
    int[][] variantFamilyName;

    int[][] rootFirstName;
    int[][] rootFamilyName;

    boolean isUseRoot = false;      // false for variant

    NameLvsVariants nameLvsVariants;

    Connection dbconPrematch = null;
    Connection dbconMatch = null;
    Connection dbconTemp = null;


    public MatchAsync       // for variant names
    (
        boolean debug,
        boolean dry_run,

        PrintLogger plog,
        Semaphore sem,

        int n_mp,
        int n_qs,

        QueryLoader ql,
        SampleLoader s1,    // sample 1
        SampleLoader s2,    // sample 2

        QueryGroupSet qgs,
        InputSet inputSet,

        String db_host,
        String db_user,
        String db_pass,

        String lvs_table_firstname,
        String lvs_table_familyname,

        String freq_table_firstname,
        String freq_table_familyname,

        int[][] variantFirstName,
        int[][] variantFamilyName,

        NameLvsVariants nameLvsVariants
    )
    {
        this.debug = debug;
        this.dry_run = dry_run;

        this.plog = plog;
        this.sem = sem;

        this.n_mp = n_mp;
        this.n_qs = n_qs;

        this.ql = ql;
        this.s1 = s1;    // sample 1
        this.s2 = s2;    // sample 2

        this.qgs = qgs;
        this.inputSet = inputSet;

        this.db_host = db_host;
        this.db_user = db_user;
        this.db_pass = db_pass;

        this.lvs_table_firstname  = lvs_table_firstname;
        this.lvs_table_familyname = lvs_table_familyname;

        this.freq_table_firstname  = freq_table_firstname;
        this.freq_table_familyname = freq_table_familyname;

        this.variantFirstName  = variantFirstName;
        this.variantFamilyName = variantFamilyName;

        this.nameLvsVariants = nameLvsVariants;

        long threadId = Thread.currentThread().getId();
        System.out.printf( "Thread id %02d; MatchAsync: using variant names (instead of root names)\n", threadId );
    }


    public MatchAsync       // for root names
    (
        boolean debug,
        boolean dry_run,

        PrintLogger plog,
        Semaphore sem,

        int n_mp,
        int n_qs,

        QueryLoader ql,
        SampleLoader s1,    // sample 1
        SampleLoader s2,    // sample 2

        QueryGroupSet qgs,
        InputSet inputSet,

        String db_host,
        String db_user,
        String db_pass,

        String lvs_table_firstname,
        String lvs_table_familyname,

        String freq_table_firstname,
        String freq_table_familyname,

        int[][] rootFirstName,
        int[][] rootFamilyName,

        NameLvsVariants nameLvsVariants,

        boolean root
    )
    {
        this.debug = debug;
        this.dry_run = dry_run;

        this.plog = plog;
        this.sem = sem;

        this.n_mp = n_mp;
        this.n_qs = n_qs;

        this.ql = ql;
        this.s1 = s1;    // sample 1
        this.s2 = s2;    // sample 2

        this.qgs = qgs;
        this.inputSet = inputSet;

        this.db_host = db_host;
        this.db_user = db_user;
        this.db_pass = db_pass;

        this.lvs_table_firstname  = lvs_table_firstname;
        this.lvs_table_familyname = lvs_table_familyname;

        this.freq_table_firstname  = freq_table_firstname;
        this.freq_table_familyname = freq_table_familyname;

        this.rootFirstName  = rootFirstName;
        this.rootFamilyName = rootFamilyName;

        this.nameLvsVariants = nameLvsVariants;

        this.isUseRoot = true;      // true for root

        long threadId = Thread.currentThread().getId();
        System.out.printf( "Thread id %02d; MatchAsync: using root names (instead of variant names)\n", threadId );
    }

    @Override
    public void run()
    {
        // If you want the actual CPU time of the current thread (or indeed, any arbitrary thread) rather than
        // the wall clock time then you can get this via ThreadMXBean. Basically, do this at the start:
        ThreadMXBean threadMXB = ManagementFactory.getThreadMXBean();
        threadMXB.setThreadCpuTimeEnabled( true );

        // in order to show the indexes when an exception occurs, we define copies outside the try/catch
        int s1_idx_cpy  = 0;
        int s2_hit_cpy  = 0;
        int lvs_idx_cpy = 0;

        // count why the matches fail
        long n_int_familyname_e = 0;
        long n_int_familyname_m = 0;
        long n_int_familyname_f = 0;
        long n_int_familyname_p = 0;

        long n_int_firstname_e = 0;
        long n_int_firstname_m = 0;
        long n_int_firstname_f = 0;
        long n_int_firstname_p = 0;

        long n_int_name = 0;    // any name

        long n_minmax = 0;
        long n_sex    = 0;

        long threadId = Thread.currentThread().getId();

        try
        {
            long threadStart = System.currentTimeMillis();      // ? clock time or process time

            long nanoseconds_begin  = ManagementFactory.getThreadMXBean().getThreadCpuTime( Thread.currentThread().getId() );
            long milliseconds_begin = TimeUnit.SECONDS.convert( nanoseconds_begin, TimeUnit.MILLISECONDS );

            String msg = String.format( "\nThread id %02d; MatchAsync/run(): thread started running", threadId );
            System.out.println( msg ); plog.show( msg );

            msg = String.format( "Thread id %02d; process id: %d", threadId, qgs.get( 0 ).id );
            System.out.println( msg ); plog.show( msg );

            msg = String.format( "Thread id %02d; Range %d of %d", threadId, (n_qs + 1), qgs.getSize() );
            System.out.println( msg ); plog.show( msg );

            /*
            int s1_id_registration_chk;
            int s2_id_registration_chk;
            if( debugfail )
            {
                s1_id_registration_chk = 24492739;
                s2_id_registration_chk = 35338341;

                int s1_id_registration_idx = ql.s1_id_registration.indexOf( s1_id_registration_chk );
                int s2_id_registration_idx = ql.s2_id_registration.indexOf( s2_id_registration_chk );

                msg = String.format( "Thread id %02d; s1_id_registration_chk: %d, s1_id_registration_idx: %d",
                    threadId, s1_id_registration_chk, s1_id_registration_idx );
                System.out.println( msg );

                msg = String.format( "Thread id %02d; s2_id_registration_chk: %d, s2_id_registration_idx: %d",
                    threadId, s2_id_registration_chk, s2_id_registration_idx );
                System.out.println( msg );

                if( s1_id_registration_idx == -1 || s2_id_registration_idx == -1  ) {
                    System.out.println( "RETURN\n" );
                    return;
                }
            }
            */

            // database connections
            dbconPrematch = DatabaseManager.getConnection( db_host, "links_prematch", db_user, db_pass );

            String csvFilename = "";
            FileWriter writerMatches = null;
            if( match2csv ) // collect matches in csv file
            {
                csvFilename = "matches_threadId_" + threadId + ".csv";      // Create csv file to collect the matches
                writerMatches = createCsvFileMatches( threadId, csvFilename );
                msg = String.format( "Thread id %02d; Collecting thread matches in CSV file: %s", threadId, csvFilename );
                System.out.println( msg ); plog.show( msg );

                if( debug ) {   // use temp table to collect the matches
                    dbconTemp = DatabaseManager.getConnection( db_host, "links_temp", db_user, db_pass );
                    createTempMatchesTable( threadId );                     // Create temp table to collect the matches
                }
            }
            else    // write matches immediately to matches table
            {
                dbconMatch = DatabaseManager.getConnection( db_host, "links_match", db_user, db_pass );
            }

            int id_match_process = inputSet.get( n_mp ).get( 0 ).id;

            // Get a QuerySet object. This object will contains the data from a match_process table record
            QuerySet qs = qgs.get( n_qs );

            // Levenshtein distances to use to get the variant names
            int lvs_dist_firstname  = qs.lvs_dist_max_firstname;
            int lvs_dist_familyname = qs.lvs_dist_max_familyname;

            msg = String.format( "Thread id %02d; use firstname  levenshtein distance for variants: %d", threadId, lvs_dist_firstname );
            System.out.println( msg ); plog.show( msg );
            msg = String.format( "Thread id %02d; use familyname levenshtein distance for variants: %d", threadId, lvs_dist_familyname );
            System.out.println( msg ); plog.show( msg );

            msg = String.format( "Thread id %02d; based on s1 query:", threadId );
            System.out.println( msg ); plog.show( msg );
            msg = String.format( "Thread id %02d; %s", threadId, qs.s1_query );
            System.out.println( msg ); plog.show( msg );

            msg = String.format( "Thread id %02d, based on s2 query:", threadId );
            System.out.println( msg ); plog.show( msg );
            msg = String.format( "Thread id %02d; %s", threadId, qs.s2_query );
            System.out.println( msg ); plog.show( msg );

            // variant names for s1 name from ls_ table, plus lvs distance
            Vector< Integer > s2_idx_matches   = new Vector< Integer >();   // matches for current s1_idx
            Vector< Integer > lvsVariantsName  = new Vector< Integer >();
            Vector< Integer > lvsDistancesName = new Vector< Integer >();
            String ending_previous = "";
            int name_previous = -1;

            // Loop through set 1
            //msg = String.format( "Thread id %02d; Set 1 size: %d from links_base", threadId, ql.s1_id_base.size() );
            msg = String.format( "Thread id %02d", threadId );
            System.out.println( msg ); plog.show( msg );

            long n_recs   = 0;
            long n_match  = 0;

            int s1_size  = ql.s1_id_base.size();

            int s1_nchunks = 100;       // show progress in s1_nchunks steps
            int s1_chunk = s1_size / s1_nchunks;

            // Previous s1EgoFamilyName, initial is 0. Because the familynames are ordered, the calculation of the potential
            // matches is done once, only the first time.
            int previous_s1EgoFamilyName = 0;

            // Outer loop over the records of the s1 query set
            for( int s1_idx = 0; s1_idx < s1_size; s1_idx++ )
            {
                /*
                int s1_id_base = ql.s1_id_base.get( s1_idx );
                if( s1_id_base == 74760790 ) { debug = debugfreq = debugfail = true; }
                else { debug = debugfreq = debugfail = false; continue; }
                */

                if( debug || debugfreq) {
                    System.out.println( "***< s1_idx >*******************************************************************" );
                    int s1_id_base = ql.s1_id_base.get( s1_idx );
                    int s1_id_registration = ql.s1_id_registration.get( s1_idx );
                    System.out.println( String.format( "s1_idx: %d-of-%d, s1_id_base: %d, s1_id_registration: %d",
                        s1_idx, s1_size-1, s1_id_base, s1_id_registration ) );
                }

                n_recs++;

                s1_idx_cpy = s1_idx;   // copy value in order to display if exception occurs

                if( s1_chunk != 0 && ( ( s1_idx + s1_chunk ) % s1_chunk == 0 ) )        // show progress
                { System.out.println( String.format( "Thread id %02d; records processed: %d-of-%d, total # of matches found: %d", threadId , s1_idx, s1_size, n_match ) ); }

                /*
                if( n_match > 100 ) {
                    System.out.println( "BREAK" );
                    break;
                }
                */

                if( debug || debugfreq ) {
                    System.out.println( String.format( "s1_id_base: %d, s1_id_registration: %d, s1_ego_familyname: %s, s1_ego_firstname1: %s, s1_mother_familyname: %s, s1_mother_firstname1: %s",
                        ql.s1_id_base.get( s1_idx ), ql.s1_id_registration.get( s1_idx ),
                        ql.s1_ego_familyname_str.get( s1_idx ), ql.s1_ego_firstname1_str.get( s1_idx ),
                        ql.s1_mother_familyname_str.get( s1_idx ), ql.s1_mother_firstname1_str.get( s1_idx )
                    ) );
                }

                // get the frequencies of names used for matching, ordered from low to high frequency
                ListMultimap<Integer, String> nameFreqMap = check_frequencies( debugfreq, qs, s1_idx );

                Multiset< Integer> keys = nameFreqMap.keys();
                Collection< String > values = nameFreqMap.values();

                if( debugfreq ) {
                    System.out.println( "nameFreqMap entries: " + nameFreqMap.size() );
                    System.out.println( String.format( "ordered keys: %s, ordered values: %s", keys.toString(), values.toString() ) );
                }

                boolean recheck_firstnames = false;
                if( qs.firstname_method != 2 ) { recheck_firstnames = true; }   // not only firstname1 used

                // Use the first emfp_name (lowest frequency, i.e. index = 0) category to determine the variant names
                Integer freq_key0 = (Integer)  keys.toArray()[ 0 ];
                String emfp_name0 = (String) values.toArray()[ 0 ];

                if( emfp_name0.endsWith( "_firstname" ) )
                {
                    int s1_firstname1 = -1;
                    String s1_firstname1_str = "";

                    if( emfp_name0.equals( "ego_firstname" ) ) {
                        s1_firstname1 = ql.s1_ego_firstname1.get( s1_idx );
                        s1_firstname1_str = ql.s1_ego_firstname1_str.get( s1_idx );
                        if( debugfreq ) { System.out.println( "ego_firstname" ); }
                    }
                    else if( emfp_name0.equals( "mother_firstname" ) ) {
                        s1_firstname1 = ql.s1_mother_firstname1.get( s1_idx );
                        s1_firstname1_str = ql.s1_mother_firstname1_str.get( s1_idx );
                        if( debugfreq ) { System.out.println( "mother_firstname" ); }
                    }
                    else if( emfp_name0.equals( "father_firstname" ) ) {
                        s1_firstname1 = ql.s1_father_firstname1.get( s1_idx );
                      //s1_firstname1_str = ql.s1_father_firstname1_str.get( s1_idx );
                        if( debugfreq ) { System.out.println( "father_firstname" ); }
                    }
                    else if( emfp_name0.equals( "partner_firstname" ) ) {
                        s1_firstname1 = ql.s1_partner_firstname1.get( s1_idx );
                      //s1_firstname1_str = ql.s1_partner_firstname1_str.get( s1_idx );
                        if( debugfreq ) { System.out.println( "partner_firstname" ); }
                    }

                    // if the name or ending changed, get new s1 firstname variants
                    if( ! ( s1_firstname1 == name_previous && ending_previous.equals( "firstname" ) ) )
                    {
                        if( debugfreq ) {
                            System.out.println( "===< new firstname >============================================================" );
                            System.out.println( String.format( "s1_firstname1 = %d, s1_firstname1_str = %s", s1_firstname1, s1_firstname1_str ) );
                            System.out.println( String.format( "match name 0-of-%d: key: %d, name: %s", keys.size(), freq_key0, emfp_name0 ) );
                            System.out.println( "changed firstname: get Levenshtein variants..." );
                        }

                        name_previous = s1_firstname1;
                        ending_previous = "_firstname";
                        lvsVariantsName .clear();          // Empty the previous lists
                        lvsDistancesName.clear();

                        // 1: asymmetric ('single-sided') ls table; SELECT UNION query
                        // 2:  symmetric ('double-sided') ls table; single SELECT
                        getLvsVariants1( s1_firstname1, lvs_table_firstname, qs.lvs_dist_max_firstname, lvsVariantsName, lvsDistancesName );

                        if( debugfreq ) {
                            System.out.printf( "lvs_table: %s, lvs_dist_max: %d, s1_name: %d, count: %d\n",
                                lvs_table_firstname, qs.lvs_dist_max_firstname, s1_firstname1, lvsVariantsName.size() );
                        }
                    }
                }

                if( emfp_name0.endsWith( "_familyname" ) )
                {
                    int s1_familyname = -1;
                    String s1_familyname_str = "";

                    if( emfp_name0.equals( "ego_familyname" ) ) {
                        s1_familyname = ql.s1_ego_familyname.get( s1_idx );
                        s1_familyname_str = ql.s1_ego_familyname_str.get( s1_idx );
                        if( debugfreq ) { System.out.println( "ego_familyname" ); }
                    }
                    else if( emfp_name0.equals( "mother_familyname" ) ) {
                        s1_familyname = ql.s1_mother_familyname.get( s1_idx );
                        s1_familyname_str = ql.s1_mother_familyname_str.get( s1_idx );
                        if( debugfreq ) { System.out.println( "mother_familyname" ); }
                    }
                    else if( emfp_name0.equals( "father_familyname" ) ) {
                        s1_familyname = ql.s1_father_familyname.get( s1_idx );
                      //s1_familyname_str = ql.s1_father_familyname_str.get( s1_idx );
                        if( debugfreq ) { System.out.println( "father_familyname" ); }
                    }
                    else if( emfp_name0.equals( "partner_familyname" ) ) {
                        s1_familyname = ql.s1_partner_familyname.get( s1_idx );
                      //s1_familyname_str = ql.s1_partner_familyname_str.get( s1_idx );
                        if( debugfreq ) { System.out.println( "partner_familyname" ); }
                    }

                    // if the name or ending changed, get new s1 familyname variants
                    if( ! ( s1_familyname == name_previous && ending_previous.equals( "familyname" ) ) )
                    {
                        if( debugfreq ) {
                            System.out.println( "===< new familyname >===========================================================" );
                            System.out.println( String.format( "s1_familyname = %d, s1_familyname_str = %s", s1_familyname, s1_familyname_str ) );
                            System.out.println( String.format( "match name 0-of-%d: key: freq = %d, value: emfp_name = %s", keys.size(), freq_key0, emfp_name0 ) );
                            System.out.println( "changed familyname: get Levenshtein variants..." );
                        }

                        name_previous = s1_familyname;
                        ending_previous = "_familyname";
                        lvsVariantsName .clear();         // Empty the previous lists
                        lvsDistancesName.clear();

                        // 1: asymmetric ('single-sided') ls table; SELECT UNION query
                        // 2:  symmetric ('double-sided') ls table; single SELECT
                        getLvsVariants1( s1_familyname, lvs_table_familyname, qs.lvs_dist_max_familyname, lvsVariantsName, lvsDistancesName );

                        if( debugfreq ) {
                            System.out.printf( "lvs_table: %s, lvs_dist_max: %d, s1_name: %d, # of variants: %d\n",
                                lvs_table_familyname, qs.lvs_dist_max_familyname, s1_familyname, lvsVariantsName.size() );
                        }
                    }
                }

                int n_variant_match = 0;
                s2_idx_matches.clear();

                // Loop over the found variant names
                for( int lvn_idx = 0; lvn_idx < lvsVariantsName.size(); lvn_idx++ )
                {
                    if( debugfreq ) { System.out.println( "---< lvs_idx >------------------------------------------------------------------" ); }
                    if( debug || debugfreq ) { System.out.println( String.format( "variant %d-of-%d", lvn_idx + 1, lvsVariantsName.size() ) ); }

                    int var_name = lvsVariantsName .get( lvn_idx );
                    int var_dist = lvsDistancesName.get( lvn_idx );

                    // search s1 variant name in s2
                    int s2_offset = 0;
                    while( true )   // there may be more hits in s2 of the given s1 variant
                    {
                        String lvs_dist_family_ego = "null";
                        String lvs_dist_family_mot = "null";
                        String lvs_dist_family_fat = "null";
                        String lvs_dist_family_par = "null";

                        String lvs_dist_first_ego = "null";
                        String lvs_dist_first_mot = "null";
                        String lvs_dist_first_fat = "null";
                        String lvs_dist_first_par = "null";

                        int s2_idx = search_s2_variant( debugfreq, emfp_name0, var_name, s2_offset );
                        s2_offset = s2_idx + 1;                 // offset for next search

                        if( s2_idx == -1 ) { break; }           // no more variants
                        else
                        {
                            boolean names_matched = true;       // optimistic

                            if( debug || debugfreq ) {
                                System.out.println( String.format( "s2_id_base: %d, s2_id_registration: %d, s2_ego_familyname: %s, s2_ego_firstname1: %s, s2_mother_familyname: %s, s2_mother_firstname1: %s",
                                    ql.s2_id_base.get( s2_idx ), ql.s2_id_registration.get( s2_idx ),
                                    ql.s2_ego_familyname_str.get( s2_idx ), ql.s2_ego_firstname1_str.get( s2_idx ),
                                    ql.s2_mother_familyname_str.get( s2_idx ), ql.s2_mother_firstname1_str.get( s2_idx )
                                ) );
                            }

                            // compare the used names
                            if( debugfreq ) { System.out.println( "keys.size(): " + keys.size() ); }
                            for( int n = 0; n < keys.size(); n++ )    // other names in increasing frequency order
                            {
                                Integer freq_key = (Integer)  keys.toArray()[ n ];
                                String emfp_name = (String) values.toArray()[ n ];

                                if( debugfreq ) {
                                    System.out.println( "...< names >...................................................................." );
                                    System.out.println( "n: " + n );
                                    System.out.println( String.format( "match name, step %d-of-%d: key: freq = %d, value = emfp_name: %s", n+1, keys.size(), freq_key, emfp_name ) );
                                }

                                int lvs_dist = -1;
                                if( n == 0 )
                                {
                                    lvs_dist = var_dist;
                                    if( debugfreq ) { System.out.println( "lvs_dist: " + lvs_dist ); }
                                    if( emfp_name.endsWith( "_firstname" ) )    // re-check firstnames?
                                    {
                                        if( recheck_firstnames )
                                        { lvs_dist = compare_names_in_pair( debugfreq, s1_idx, s2_idx, emfp_name, qs ); }
                                        //else { continue; }  // No, only firstname1, so using var_dist
                                        // but no "else { continue; }", because we need to set the lvs_dist_first_... string below
                                    }
                                }
                                else    // n > 0: compare names, either firstnames, or familynames
                                {
                                    if( debugfreq ) { System.out.println( "before compare_names_in_pair" ); }
                                    lvs_dist = compare_names_in_pair( debugfreq, s1_idx, s2_idx, emfp_name, qs );
                                }

                                if( lvs_dist == -1 ) {
                                    if( debugfreq ) { System.out.println( String.format( "NO Match %s", emfp_name ) ); }
                                    names_matched = false;
                                    break;
                                }
                                else
                                {
                                    String lvs_dist_str = Integer.toString( lvs_dist );
                                    if( lvs_dist_str == null ) { lvs_dist_str = "null"; }

                                         if( emfp_name.equals( "ego_firstname"     ) ) { lvs_dist_first_ego = lvs_dist_str; }
                                    else if( emfp_name.equals( "mother_firstname"  ) ) { lvs_dist_first_mot = lvs_dist_str; }
                                    else if( emfp_name.equals( "father_firstname"  ) ) { lvs_dist_first_fat = lvs_dist_str; }
                                    else if( emfp_name.equals( "partner_firstname" ) ) { lvs_dist_first_par = lvs_dist_str; }

                                    else if( emfp_name.equals( "ego_familyname"     ) ) { lvs_dist_family_ego = lvs_dist_str; }
                                    else if( emfp_name.equals( "mother_familyname"  ) ) { lvs_dist_family_mot = lvs_dist_str; }
                                    else if( emfp_name.equals( "father_familyname"  ) ) { lvs_dist_family_fat = lvs_dist_str; }
                                    else if( emfp_name.equals( "partner_familyname" ) ) { lvs_dist_family_par = lvs_dist_str; }

                                    if( debugfreq && s2_idx_matches.indexOf( s2_idx ) == -1 ) {
                                        System.out.println( String.format( "names in pair MATCHed %s ( n = %d), s1_idx = %d, s2_idx = %d", emfp_name, n, s1_idx, s2_idx ) );
                                    }
                                }
                            }

                            if( names_matched )   // passed all name pairs comparisons
                            {
                                // Check min max
                                if( ! qs.ignore_minmax )
                                {
                                    if( ! acceptMinMax( qs, s1_idx, s2_idx ) )
                                    {
                                        n_minmax++;
                                        if( debugfail ) { System.out.println( "failed _minmax" ); }
                                        continue;
                                    }
                                    else { if( debugfail ) { System.out.println( "matched _minmax" ); } }
                                }

                                // Check sex
                                if( qs.ignore_sex.equals( "n" ) )
                                {
                                    int s1_sex = ql.s1_sex.get( s1_idx );
                                    int s2_sex = ql.s2_sex.get( s2_idx );

                                    if( ( s1_sex == 1 && s2_sex == 2 ) || ( s1_sex == 2 && s2_sex == 1 ) )  // no match
                                    {
                                        n_sex++;
                                        if( debugfail ) { System.out.println( "failed _sex: s1_sex=" + s1_sex + ", s2_sex=" + s2_sex ); }
                                        continue;
                                    }
                                    else { if( debugfail ) { System.out.println( "matched _sex: s1_sex=" + s1_sex + ", s2_sex=" + s2_sex ); } }
                                }

                                // also passed minmax & gender test
                                if( s2_idx_matches.indexOf( s2_idx ) != -1 )
                                {
                                    System.out.println( "MATCH doublure; EXIT" );
                                    // continue;    // ignore double match (due to firstnames re-check?)
                                    System.exit( 0 );
                                }
                                else
                                {
                                    s2_idx_matches.add( s2_idx );
                                    n_match++;                      // number of matches of this thread
                                    n_variant_match++;

                                    // write to match tables
                                    int id_linksbase_1 = ql.s1_id_base.get( s1_idx );
                                    int id_linksbase_2 = ql.s2_id_base.get( s2_idx );

                                    if( debug || debugfreq ) {
                                        msg = String.format( "MATCH: s1_idx: %d, s2_idx: %d, id_linksbase_1: %d, id_linksbase_2: %d",
                                            s1_idx, s2_idx, id_linksbase_1, id_linksbase_2 );
                                            System.out.println( msg ); plog.show( msg );
                                    }

                                    if( match2csv )
                                    {
                                        // trying to get NULLs for empty lvs in db table via csv (for the query we need the "null")
                                        if( lvs_dist_first_ego == "null" ) { lvs_dist_first_ego = "\\N"; }
                                        if( lvs_dist_first_mot == "null" ) { lvs_dist_first_mot = "\\N"; }
                                        if( lvs_dist_first_fat == "null" ) { lvs_dist_first_fat = "\\N"; }
                                        if( lvs_dist_first_par == "null" ) { lvs_dist_first_par = "\\N"; }

                                        if( lvs_dist_family_ego == "null" ) { lvs_dist_family_ego = "\\N"; }
                                        if( lvs_dist_family_mot == "null" ) { lvs_dist_family_mot = "\\N"; }
                                        if( lvs_dist_family_fat == "null" ) { lvs_dist_family_fat = "\\N"; }
                                        if( lvs_dist_family_par == "null" ) { lvs_dist_family_par = "\\N"; }

                                        String flag_overlink = "\\N";   // not used in matching, but completes the csv record
                                        String flag_quality  = "\\N";   // not used in matching, but completes the csv record
                                        String ids           = "\\N";   // not used in matching, but completes the csv record

                                        // notice: in order to let the \N properperly work, avoid leading or trailing whitespace
                                        String str = String.format( "%d, %d, %d,%s,%s,%s,%s,%s,%s,%s,%s\n",
                                            id_match_process , id_linksbase_1 , id_linksbase_2 ,
                                            lvs_dist_first_ego , lvs_dist_family_ego ,
                                            lvs_dist_first_mot , lvs_dist_family_mot ,
                                            lvs_dist_first_fat , lvs_dist_family_fat ,
                                            lvs_dist_first_par , lvs_dist_family_par );

                                        // String str = String.format( "%d, %d, %d,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n",
                                        // flag_overlink, flag_quality, ids );  // not used in matching
                                        //System.out.println( str );

                                        writerMatches.write( str );
                                    }
                                    else
                                    {
                                        String query = "INSERT INTO matches ( id_match_process , id_linksbase_1 , id_linksbase_2, " +
                                            "value_firstname_ego, value_familyname_ego, " +
                                            "value_firstname_mo , value_familyname_mo , " +
                                            "value_firstname_fa , value_familyname_fa , " +
                                            "value_firstname_pa , value_familyname_pa ) " +
                                            "VALUES ( " + id_match_process + "," + id_linksbase_1 + "," + id_linksbase_2 + "," +
                                            lvs_dist_first_ego + "," + lvs_dist_family_ego + "," +
                                            lvs_dist_first_mot + "," + lvs_dist_family_mot + "," +
                                            lvs_dist_first_fat + "," + lvs_dist_family_fat + "," +
                                            lvs_dist_first_par + "," + lvs_dist_family_par + ")";

                                        if( debug || debugfreq ) {
                                            msg = String.format( "lvn_idx %2d: %s", lvn_idx, query );
                                            System.out.println( msg ); plog.show( msg );
                                        }

                                        if( ! dry_run ) {
                                            dbconMatch.createStatement().execute( query );
                                            dbconMatch.createStatement().close();
                                            dbconMatch = null;
                                        }
                                    }
                                } // insert
                            } // names_matched
                            else { n_int_name++; }
                        } // match block
                    } // while loop
                } // lvs_idx loop

                // display n_variant_match

                nameFreqMap.clear();
                nameFreqMap = null;

                System.out.flush();
                System.err.flush();
            } // s1_idx loop

            if( match2csv ) {
                writerMatches.close();

                boolean removeCsv = true;
                if( debug ) { removeCsv = false; }

                if( debug )
                {
                    if( dbconTemp != null ) {
                        loadCsvFileToTempTable( threadId, csvFilename );
                        dbconTemp.close();
                    }
                }
                else { loadCsvFileToMatchTable( threadId, csvFilename, removeCsv ); }
            }

            sem.release();
            int npermits = sem.availablePermits();
            plog.show( "Semaphore: # of permits: " + npermits );

            msg = String.format( "Thread id %02d; s1 records processed:         %10d", threadId, n_recs );
            System.out.println( msg ); plog.show( msg );

            msg = String.format( "Thread id %02d; Number of matches:            %10d", threadId, n_match );
            System.out.println( msg ); plog.show( msg );

            long n_fail = 0;

            if( n_int_name != 0 ) {
                n_fail += n_int_name;
                msg = String.format( "Thread id %02d; failures n_int_name:          %10d", threadId, n_int_name );
                System.out.println( msg ); plog.show( msg );
            }

            if( n_int_familyname_e != 0 ) {
                n_fail += n_int_familyname_e;
                msg = String.format( "Thread id %02d; failures n_int_familyname_e:  %10d", threadId, n_int_familyname_e );
                System.out.println( msg ); plog.show( msg );
            }

            if( n_int_familyname_m != 0 ) {
                n_fail += n_int_familyname_m;
                msg = String.format( "Thread id %02d; failures n_int_familyname_m:  %10d", threadId, n_int_familyname_m );
                System.out.println( msg ); plog.show( msg );
            }

            if( n_int_familyname_f != 0 ) {
                n_fail += n_int_familyname_f;
                msg = String.format( "Thread id %02d; failures n_int_familyname_f:  %10d", threadId, n_int_familyname_f );
                System.out.println( msg ); plog.show( msg );
            }

            if( n_int_familyname_p != 0 ) {
                n_fail += n_int_familyname_p;
                msg = String.format( "Thread id %02d; failures n_int_familyname_p:  %10d", threadId, n_int_familyname_p );
                System.out.println( msg ); plog.show( msg );
            }

            if( n_int_firstname_e != 0 ) {
                n_fail += n_int_firstname_e;
                msg = String.format( "Thread id %02d; failures n_int_firstname_e:   %10d", threadId, n_int_firstname_e );
                System.out.println( msg ); plog.show( msg );
            }

            if( n_int_firstname_m != 0 ) {
                n_fail += n_int_firstname_m;
                msg = String.format( "Thread id %02d; failures n_int_firstname_m:   %10d", threadId, n_int_firstname_m );
                System.out.println( msg ); plog.show( msg );
            }

            if( n_int_firstname_f != 0 ) {
                n_fail += n_int_firstname_f;
                msg = String.format( "Thread id %02d; failures n_int_firstname_f:   %10d", threadId, n_int_firstname_f );
                System.out.println( msg ); plog.show( msg );
            }

            if( n_int_firstname_p != 0 ) {
                n_fail += n_int_firstname_p;
                msg = String.format( "Thread id %02d; failures n_int_firstname_p:   %10d", threadId, n_int_firstname_p );
                System.out.println( msg ); plog.show( msg );
            }

            if( n_minmax != 0 ) {
                n_fail += n_minmax;
                msg = String.format( "Thread id %02d; failures n_minmax:            %10d", threadId, n_minmax );
                System.out.println( msg ); plog.show( msg );
            }

            if( n_sex != 0 ) {
                n_fail += n_sex;
                msg = String.format( "Thread id %02d; failures n_sex:               %10d", threadId, n_sex );
                System.out.println( msg ); plog.show( msg );
            }

            msg = String.format( "Thread id %02d; total match attempt failures: %10d", threadId, n_fail );
            System.out.println( msg ); plog.show( msg );

            long n_mismatch = n_recs - ( n_fail + n_match );
            if( n_mismatch > 0 ) {
                msg = String.format( "Thread id %02d; missing records: %d ??", threadId, n_mismatch );
                System.out.println( msg ); plog.show( msg );
            }


            msg = String.format( "Thread id %02d; Done: Range %d-of-%d", threadId, (n_qs + 1), qgs.getSize() );
            System.out.println( msg );
            plog.show( msg );

            int nthreads_active = java.lang.Thread.activeCount();
            msg = String.format( "Thread id %02d; MatchAsync/run(): Thread is done (%d active threads remaining)", threadId, nthreads_active );
            System.out.println( msg );
            plog.show( msg );

            msg = String.format( "Thread id %02d; freeing s1 and s2 vectors", threadId );
            System.out.println( msg ); plog.show( msg );
            ql.freeVectors();
            ql = null;
            qs = null;

            if( dbconPrematch != null ) { dbconPrematch.close(); }
            if( dbconMatch    != null ) { dbconMatch.close(); }
            if( dbconTemp     != null ) { dbconTemp.close(); }

            msg = String.format( "Thread id %02d; clock time", threadId );
            elapsedShowMessage( msg, threadStart, System.currentTimeMillis() );

            long cpuTimeNsec  = threadMXB.getCurrentThreadCpuTime();   // elapsed CPU time for current thread in nanoseconds
            long cpuTimeMsec  = TimeUnit.NANOSECONDS.toMillis( cpuTimeNsec );
            msg = String.format( "Thread id %02d; thread time", threadId );
            elapsedShowMessage( msg, 0, cpuTimeMsec );

            //long userTimeNsec = thx.getCurrentThreadUserTime();  // elapsed user time in nanoseconds
            //long userTimeMsec = TimeUnit.NANOSECONDS.toMillis( userTimeNsec );
            //elapsedShowMessage( "user m time elapsed", 0, userTimeMsec );   // user mode part of thread time
        }
        catch( Exception ex1 )
        {
            sem.release();
            String err = String.format( "Thread id %02d; MatchAsync/run(): thread exception: s1_idx_cpy = %d, lvs_idx_cpy = %d, s2_hit_cpy: %d, error = %s",
                threadId, s1_idx_cpy, lvs_idx_cpy, s2_hit_cpy, ex1.getMessage() );
            System.out.println( err );
            ex1.printStackTrace();
            try { plog.show( err ); }
            catch( Exception ex2 ) { ex2.printStackTrace(); }
        }
    } // run


    private boolean existsMatchTempTable( String table_name )
    {
        boolean exists = false;

        String query = "SELECT COUNT(*) AS count FROM information_schema.tables " +
            "WHERE table_schema = 'links_temp' AND table_name = '" + table_name + "'";

        try {
            ResultSet rs = dbconTemp.createStatement().executeQuery( query );
            while( rs.next() )
            {
                int count = rs.getInt( "count" );
                if( count == 1 ) { exists = true; }
            }
            rs.close();
        }
        catch( Exception ex ) {
            String err = "Exception in existsMatchTempTable(): " + ex.getMessage();
            System.out.println( err );
            try { plog.show( err ); } catch( Exception ex2 ) { ; }
        }

        return exists;
    } // existsMatchTempTable


    private void dropTempTable( String tablename ) throws Exception
    {
        String query = "DROP TABLE " + tablename;
        System.out.println( query ); plog.show( query );

        dbconTemp.createStatement().execute( query );

    } // memtable_drop


    /**
     * @throws Exception
     */
    private void createTempMatchesTable( long threadId ) throws Exception
    {
        String tablename = "matches_threadId_" + threadId;

        if( existsMatchTempTable( tablename ) ) {
            String msg = String.format( "Thread id %02d; Deleting previous table %s", threadId, tablename );
            System.out.println( msg ); plog.show( msg );

            dropTempTable( tablename );
        }

        String msg = String.format( "Thread id %02d; Creating %s table", threadId, tablename );
        plog.show( msg ); System.out.println( msg );

        // the temp table does not contain the last 3 columns of the matches table,
        // because they are not used during matching; these are:
        // `flag_overlink`, `flag_quality`, and `ids`.
        String query = "CREATE  TABLE links_temp." + tablename
            + " ("
            + " `id_matches` INT(10) UNSIGNED NOT NULL AUTO_INCREMENT,"
            + " `id_match_process`   INT(10) UNSIGNED NOT NULL,"
            + " `id_linksbase_1`     INT(10) UNSIGNED NOT NULL,"
            + " `id_linksbase_2`     INT(10) UNSIGNED NOT NULL,"
            + " `value_firstname_ego`  TINYINT(4) DEFAULT NULL,"
            + " `value_familyname_ego` TINYINT(4) DEFAULT NULL,"
            + " `value_firstname_mo`   TINYINT(4) DEFAULT NULL,"
            + " `value_familyname_mo`  TINYINT(4) DEFAULT NULL,"
            + " `value_firstname_fa`   TINYINT(4) DEFAULT NULL,"
            + " `value_familyname_fa`  TINYINT(4) DEFAULT NULL,"
            + " `value_firstname_pa`   TINYINT(4) DEFAULT NULL,"
            + " `value_familyname_pa`  TINYINT(4) DEFAULT NULL,"
            + " PRIMARY KEY (`id_matches`),"
            + " KEY `id_match_process` (`id_match_process`),"
            + " KEY `id_linksbase_1` (`id_linksbase_1`),"
            + " KEY `id_linksbase_2` (`id_linksbase_2`)"
            + " )"
            + " ENGINE=MyISAM DEFAULT CHARSET=utf8 COLLATE=utf8_bin;";

        System.out.println( query );
        dbconTemp.createStatement().execute( query );
        dbconTemp.createStatement().close();
    } // createTempFamilynameTable


    /**
     * @throws Exception
     */
    private FileWriter createCsvFileMatches( long threadId, String filename ) throws Exception
    {
        File file = new File( filename );
        if( file.exists() ) {
            String msg = String.format( "Thread id %02d; Deleting file %s", threadId, filename );
            System.out.println( msg ); plog.show( msg );
            file.delete();
        }

        String msg = String.format( "Thread id %02d; Creating %s", threadId, filename );
        plog.show( msg ); System.out.println( msg );

        return new FileWriter( filename );
    } // createCsvFileMatches


     /**
     * @throws Exception
     */
    private void loadCsvFileToTempTable( long threadId, String filename ) throws Exception
    {
        String tablename = "links_temp.matches_threadId_" + threadId;

        String msg = String.format( "Thread id %02d; Loading %s into %s table", threadId, filename, tablename );
        System.out.println( msg ); plog.show( msg );

        String query = "LOAD DATA LOCAL INFILE '" + filename + "'"
            + " INTO TABLE " + tablename
            + " FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n'"
            + "( id_match_process , id_linksbase_1 , id_linksbase_2 ,"
            + " value_firstname_ego , value_familyname_ego ,"
            + " value_firstname_mo , value_familyname_mo ,"
            + " value_firstname_fa , value_familyname_fa ,"
            + " value_firstname_pa , value_familyname_pa );";

        msg = String.format( "Thread id %02d; %s", threadId, query );
        System.out.println( msg ); plog.show( msg );

        dbconTemp.createStatement().execute( query );
        dbconTemp.createStatement().close();
    } // loadCsvFileToTempTable


     /**
     * @throws Exception
     */
    private void loadCsvFileToMatchTable( long threadId, String filenameCsv, boolean removeCsv )
    throws Exception
    {
        String tablename = "links_match.matches";

        String msg = String.format( "Thread id %02d; Loading %s into %s table", threadId, filenameCsv, tablename );
        System.out.println( msg ); plog.show( msg );

        String query = "LOAD DATA LOCAL INFILE '" + filenameCsv + "'"
            + " INTO TABLE " + tablename
            + " FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n'"
            + "("
            + " id_match_process , id_linksbase_1 , id_linksbase_2"
            + " ,value_firstname_ego , value_familyname_ego"
            + " ,value_firstname_mo , value_familyname_mo"
            + " ,value_firstname_fa , value_familyname_fa"
            + " ,value_firstname_pa , value_familyname_pa"
            + " );";

            // + " ,flag_overlink, flag_quality, ids"   // not used in matching

        //System.out.println( query ); plog.show( query );

        // do not keep connection endlessly open
        Connection dbconMatchLocal = DatabaseManager.getConnection( db_host, "links_match", db_user, db_pass );
        dbconMatchLocal.createStatement().execute( query );
        dbconMatchLocal.createStatement().close();
        dbconMatchLocal.close();

        if( removeCsv ) {
            msg = String.format( "Thread id %02d; Deleting file %s", threadId, filenameCsv );
            System.out.println( msg ); plog.show( msg );
            java.io.File file = new java.io.File( filenameCsv );
            file.delete();
        }
    } // loadCsvFileToMatchTable


    public int search_s2_variant( boolean debugfreq, String emfp_name, int var_name, int s2_offset )
    {
        if( debugfreq ) { System.out.println( String.format( "search_s2_variant: %s = %d, start at s2_offset = %d", emfp_name, var_name, s2_offset ) ); }

        int s2_idx = -1;

             if( emfp_name.equals( "ego_firstname"     ) ) { s2_idx = ql.s2_ego_firstname1    .indexOf( var_name, s2_offset ); }
        else if( emfp_name.equals( "mother_firstname"  ) ) { s2_idx = ql.s2_mother_firstname1 .indexOf( var_name, s2_offset ); }
        else if( emfp_name.equals( "father_firstname"  ) ) { s2_idx = ql.s2_father_firstname1 .indexOf( var_name, s2_offset ); }
        else if( emfp_name.equals( "partner_firstname" ) ) { s2_idx = ql.s2_partner_firstname1.indexOf( var_name, s2_offset ); }

        else if( emfp_name.equals( "ego_familyname"     ) ) { s2_idx = ql.s2_ego_familyname    .indexOf( var_name, s2_offset ); }
        else if( emfp_name.equals( "mother_familyname"  ) ) { s2_idx = ql.s2_mother_familyname .indexOf( var_name, s2_offset ); }
        else if( emfp_name.equals( "father_familyname"  ) ) { s2_idx = ql.s2_father_familyname .indexOf( var_name, s2_offset ); }
        else if( emfp_name.equals( "partner_familyname" ) ) { s2_idx = ql.s2_partner_familyname.indexOf( var_name, s2_offset ); }

        if( debugfreq ) {
            if( s2_idx == -1 ) { System.out.println( String.format( "not found: s2_idx = %d", s2_idx ) ); }
            else { System.out.println( String.format( "found: s2_idx = %d", s2_idx ) ); }
        }

        return s2_idx;
    } // search_s2_variant


    public int compare_names_in_pair( boolean debugfreq, int s1_idx, int s2_idx, String emfp_name, QuerySet qs )
    {
        if( debugfreq ) { System.out.println( String.format( "compare_names_in_pair for %s: s1_idx = %d, s2_idx = %d", emfp_name, s1_idx, s2_idx ) ); }

        int lvs_dist = -1;

        if( emfp_name.equals( "ego_firstname" ) ) {
            if( debugfreq ) { System.out.println( "ego_firstname" ); }
            lvs_dist = match_ego_firstname( s1_idx, s2_idx, qs );
        }

        else if( emfp_name.equals( "mother_firstname" ) ) {
            if( debugfreq ) { System.out.println( "mother_firstname" ); }
            lvs_dist = match_mother_firstname( s1_idx, s2_idx, qs );
        }

        else if( emfp_name.equals( "father_firstname" ) ) {
            if( debugfreq ) { System.out.println( "father_firstname" ); }
            lvs_dist = match_father_firstname( s1_idx, s2_idx, qs );
        }

        else if( emfp_name.equals( "partner_firstname" ) ) {
            if( debugfreq ) { System.out.println( "partner_firstname" ); }
            lvs_dist = match_partner_firstname( s1_idx, s2_idx, qs );
        }

        else if( emfp_name.equals( "ego_familyname" ) ) {
            if( debugfreq ) { System.out.println( "ego_familyname" ); }
            lvs_dist = match_ego_familyname( s1_idx, s2_idx, qs );
        }

        else if( emfp_name.equals( "mother_familyname" ) ) {
            if( debugfreq ) { System.out.println( "mother_familyname" ); }
            lvs_dist = match_mother_familyname( s1_idx, s2_idx, qs );
        }

        else if( emfp_name.equals( "father_familyname" ) ) {
            if( debugfreq ) { System.out.println( "father_familyname" ); }
            lvs_dist = match_father_familyname( s1_idx, s2_idx, qs );
        }

        else if( emfp_name.equals( "partner_familyname" ) ) {
            if( debugfreq ) { System.out.println( "partner_familyname" ); }
            lvs_dist = match_partner_familyname( s1_idx, s2_idx, qs );
        }

        return lvs_dist;
    } // compare_names_in_pair


    public int match_ego_familyname( int s1_idx, int s2_idx, QuerySet qs )
    {
        int s1EgoFamName = ql.s1_ego_familyname.get( s1_idx );
        int s2EgoFamName = ql.s2_ego_familyname.get( s2_idx );

        int lvs_dist = isVariant( s1EgoFamName, s2EgoFamName, lvs_table_familyname, qs.lvs_dist_max_familyname, NameType.FAMILYNAME, qs.method );

        return lvs_dist;
    } // match_ego_familyname

    public int match_ego_firstname( int s1_idx, int s2_idx, QuerySet qs )
    {
        int s1EgoFirName1 = ql.s1_ego_firstname1.get( s1_idx );
        int s1EgoFirName2 = ql.s1_ego_firstname2.get( s1_idx );
        int s1EgoFirName3 = ql.s1_ego_firstname3.get( s1_idx );
        int s1EgoFirName4 = ql.s1_ego_firstname4.get( s1_idx );

        int s2EgoFirName1 = ql.s2_ego_firstname1.get( s2_idx );
        int s2EgoFirName2 = ql.s2_ego_firstname2.get( s2_idx );
        int s2EgoFirName3 = ql.s2_ego_firstname3.get( s2_idx );
        int s2EgoFirName4 = ql.s2_ego_firstname4.get( s2_idx );

        int lvs_dist = checkFirstName( qs.firstname_method,
            s1EgoFirName1, s1EgoFirName2, s1EgoFirName3, s1EgoFirName4,
            s2EgoFirName1, s2EgoFirName2, s2EgoFirName3, s2EgoFirName4,
            qs.method, lvs_table_firstname, qs.lvs_dist_max_firstname );

        return lvs_dist;
    } // match_ego_firstname

    public int match_mother_familyname( int s1_idx, int s2_idx, QuerySet qs )
    {
        int s1MotherFamName = ql.s1_mother_familyname.get( s1_idx );
        int s2MotherFamName = ql.s2_mother_familyname.get( s2_idx );

        int lvs_dist = isVariant( s1MotherFamName, s2MotherFamName, lvs_table_familyname, qs.lvs_dist_max_familyname, NameType.FAMILYNAME, qs.method );

        return lvs_dist;
    } // match_mother_familyname

    public int match_mother_firstname( int s1_idx, int s2_idx, QuerySet qs )
    {
        int s1MotherFirName1 = ql.s1_mother_firstname1.get( s1_idx );
        int s1MotherFirName2 = ql.s1_mother_firstname2.get( s1_idx );
        int s1MotherFirName3 = ql.s1_mother_firstname3.get( s1_idx );
        int s1MotherFirName4 = ql.s1_mother_firstname4.get( s1_idx );

        int s2MotherFirName1 = ql.s2_mother_firstname1.get( s2_idx );
        int s2MotherFirName2 = ql.s2_mother_firstname2.get( s2_idx );
        int s2MotherFirName3 = ql.s2_mother_firstname3.get( s2_idx );
        int s2MotherFirName4 = ql.s2_mother_firstname4.get( s2_idx );

        int lvs_dist = checkFirstName( qs.firstname_method,
            s1MotherFirName1, s1MotherFirName2, s1MotherFirName3, s1MotherFirName4,
            s2MotherFirName1, s2MotherFirName2, s2MotherFirName3, s2MotherFirName4,
            qs.method, lvs_table_firstname, qs.lvs_dist_max_firstname );

        return lvs_dist;
    } // match_mother_firstname

    public int match_father_familyname( int s1_idx, int s2_idx, QuerySet qs )
    {
        int s1FatherFamName = ql.s1_father_familyname.get( s1_idx );
        int s2FatherFamName = ql.s2_father_familyname.get( s2_idx );

        int lvs_dist = isVariant( s1FatherFamName, s2FatherFamName, lvs_table_familyname, qs.lvs_dist_max_familyname, NameType.FAMILYNAME, qs.method );

        return lvs_dist;
    } // match_father_familyname

    public int match_father_firstname( int s1_idx, int s2_idx, QuerySet qs )
    {
        int s1FatherFamName  = ql.s1_father_familyname.get( s1_idx );
        int s1FatherFirName1 = ql.s1_father_firstname1.get( s1_idx );
        int s1FatherFirName2 = ql.s1_father_firstname2.get( s1_idx );
        int s1FatherFirName3 = ql.s1_father_firstname3.get( s1_idx );
        int s1FatherFirName4 = ql.s1_father_firstname4.get( s1_idx );

        int s2FatherFirName1 = ql.s2_father_firstname1.get( s2_idx );
        int s2FatherFirName2 = ql.s2_father_firstname2.get( s2_idx );
        int s2FatherFirName3 = ql.s2_father_firstname3.get( s2_idx );
        int s2FatherFirName4 = ql.s2_father_firstname4.get( s2_idx );

        int lvs_dist = checkFirstName( qs.firstname_method,
            s1FatherFirName1, s1FatherFirName2, s1FatherFirName3, s1FatherFirName4,
            s2FatherFirName1, s2FatherFirName2, s2FatherFirName3, s2FatherFirName4,
            qs.method, lvs_table_firstname, qs.lvs_dist_max_firstname );

        return lvs_dist;
    } // match_father_firstname

    public int match_partner_familyname( int s1_idx, int s2_idx, QuerySet qs )
    {
        int s1PartnerFamName = ql.s1_partner_familyname.get( s1_idx );
        int s2PartnerFamName = ql.s2_partner_familyname.get( s2_idx );

        int lvs_dist = isVariant( s1PartnerFamName, s2PartnerFamName, lvs_table_familyname, qs.lvs_dist_max_familyname, NameType.FAMILYNAME, qs.method );

        return lvs_dist;
    } // match_partner_familyname

    public int match_partner_firstname( int s1_idx, int s2_idx, QuerySet qs )
    {
        int s1PartnerFirName1 = ql.s1_partner_firstname1.get( s1_idx );
        int s1PartnerFirName2 = ql.s1_partner_firstname2.get( s1_idx );
        int s1PartnerFirName3 = ql.s1_partner_firstname3.get( s1_idx );
        int s1PartnerFirName4 = ql.s1_partner_firstname4.get( s1_idx );

        int s2PartnerFirName1 = ql.s2_partner_firstname1.get( s2_idx );
        int s2PartnerFirName2 = ql.s2_partner_firstname2.get( s2_idx );
        int s2PartnerFirName3 = ql.s2_partner_firstname3.get( s2_idx );
        int s2PartnerFirName4 = ql.s2_partner_firstname4.get( s2_idx );

        int lvs_dist = checkFirstName( qs.firstname_method,
            s1PartnerFirName1, s1PartnerFirName2, s1PartnerFirName3, s1PartnerFirName4,
            s2PartnerFirName1, s2PartnerFirName2, s2PartnerFirName3, s2PartnerFirName4,
            qs.method, lvs_table_firstname, qs.lvs_dist_max_firstname );

        return lvs_dist;
    } // match_partner_firstname


    /**
     *
     * @param debug
     * @param qs
     * @param s1_idx
     * @return
     */
    public ListMultimap<Integer, String> check_frequencies( boolean debug, QuerySet qs, int s1_idx )
    {
        int s1_id_base = ql.s1_id_base.get( s1_idx );

        String msg = "";
        if( debug ) {
            msg = String.format( "check_frequencies() s1_idx: %d", s1_idx );
            msg += String.format( ", id_base : %d", s1_id_base );
        }

        // the Multimaps.synchronizedListMultimap makes the ListMultimap thread-save,
        // TreeMap and ArrayList are not thread-save, replace with ConcurrentSkipListMap and CopyOnWriteArrayList
        // should they be replaced by concurrent alternatives?
        ListMultimap< Integer, String > nameFreqMap = Multimaps.synchronizedListMultimap    // concurrent wrapper call
        (
            Multimaps.newListMultimap
            (
              //new TreeMap< Integer, Collection< String > >(),                             // not thread-save
                new ConcurrentSkipListMap< Integer, Collection< String > >(),               // concurrent
                new Supplier< List< String > >()
                {
                  //public List< String > get() { return Lists.newArrayList(); }            // not thread-save
                    public List< String > get() { return Lists.newCopyOnWriteArrayList(); } // concurrent
                }
            )
        );

        // Ego (always used)
        if( qs.int_familyname_e > 0 ) {
            int s1EgoFamName = ql.s1_ego_familyname.get( s1_idx );
            int freq_s1EgoFamName = getFrequency( freq_table_familyname, s1EgoFamName );
            nameFreqMap.put( freq_s1EgoFamName, "ego_familyname" );
            if( debug ) { msg += String.format( "\nEgoFamName: %d", freq_s1EgoFamName ); }
        }

        if( qs.int_firstname_e > 0 ) {
            int s1EgoFirName1 = ql.s1_ego_firstname1.get( s1_idx );
            int freq_s1EgoFirName1 = getFrequency( freq_table_firstname, s1EgoFirName1 );
            nameFreqMap.put( freq_s1EgoFirName1, "ego_firstname" );
            if( debug ) { msg += String.format( "\nEgoFirName1: %d", freq_s1EgoFirName1 ); }
        }

        // Mother
        if( qs.use_mother ) {
            if( qs.int_familyname_m > 0 ) {
                int s1MotherFamName = ql.s1_mother_familyname.get( s1_idx );
                int freq_s1MotherFamName = getFrequency( freq_table_familyname, s1MotherFamName );
                nameFreqMap.put( freq_s1MotherFamName, "mother_familyname"  );
                if( debug ) { msg += String.format( "\nMotherFamName: %d", freq_s1MotherFamName ); }
            }

            if( qs.int_firstname_m > 0 ) {
                int s1MotherFirName1 = ql.s1_mother_firstname1.get( s1_idx );
                int freq_s1MotherFirName1 = getFrequency( freq_table_firstname, s1MotherFirName1 );
                nameFreqMap.put( freq_s1MotherFirName1, "mother_firstname" );
                if( debug ) { msg += String.format( "\nMotherFirName1: %d", freq_s1MotherFirName1 ); }
            }
        }

        // Father
        if( qs.use_father ) {
            if( qs.int_familyname_f > 0 ) {
                int s1FatherFamName = ql.s1_father_familyname.get( s1_idx );
                int freq_s1FatherFamName = getFrequency( freq_table_familyname, s1FatherFamName );
                nameFreqMap.put( freq_s1FatherFamName, "father_familyname" );
                if( debug ) { msg += String.format( "\nFatherFamName: %d", freq_s1FatherFamName ); }
            }

            if( qs.int_firstname_f > 0 ) {
                int s1FatherFirName1 = ql.s1_father_firstname1.get( s1_idx );
                int freq_s1FatherFirName1 = getFrequency( freq_table_firstname, s1FatherFirName1 );
                nameFreqMap.put( freq_s1FatherFirName1, "father_firstname" );
                if( debug ) { msg += String.format( "\nFatherFirName1: %d", freq_s1FatherFirName1 ); }
            }
        }

        // Partner
        if( qs.use_partner ) {
            if( qs.int_familyname_p > 0 ) {
                int s1PartnerFamName = ql.s1_partner_familyname.get( s1_idx );
                int freq_s1PartnerFamName = getFrequency( freq_table_familyname, s1PartnerFamName );
                nameFreqMap.put( freq_s1PartnerFamName, "partner_familyname" );
                if( debug ) { msg += String.format( "\nPartnerFamName: %d", freq_s1PartnerFamName ); }
            }

            if( qs.int_firstname_p > 0 ) {
                int s1PartnerFirName1 = ql.s1_partner_firstname1.get( s1_idx );
                int freq_s1PartnerFirName1 = getFrequency( freq_table_firstname, s1PartnerFirName1 );
                nameFreqMap.put( freq_s1PartnerFirName1, "partner_firstname" );
                if( debug ) { msg += String.format( "\nPartnerFirName1: %d", freq_s1PartnerFirName1 ); }
            }
        }

        if( ! msg.isEmpty() ) { System.out.println( msg ); }

        return nameFreqMap;
    } // check_frequencies


    /**
     *
     * @param freq_tablename
     * @param id
     * @return
     */
    public int getFrequency( String freq_tablename, int id )
    {
        int freq = 0;

        try
        {
            String query = "SELECT * FROM links_prematch." + freq_tablename + " WHERE id = " + id + ";";
            ResultSet rs = dbconPrematch.createStatement().executeQuery( query );

            while( rs.next() ) { freq = rs.getInt( "frequency" ); }
            rs.close();
        }
        catch( Exception ex ) {
            System.out.println( "Exception in getFrequency(): " + ex.getMessage() );
            ex.printStackTrace();
        }

        return freq;
    } //


    /**
     * @param freq_tablename
     * @param id_name
     * @param id
     * @return
     */
    public int getFrequencyStr( String freq_tablename, String id_name, int id )
    {
        int freq = 0;

        try
        {
            String query = "SELECT * FROM links_prematch." + freq_tablename + " WHERE id = " + id + ";";
            //System.out.println( id_name + ", query: " + query );
            ResultSet rs = dbconPrematch.createStatement().executeQuery( query );

            while( rs.next() ) {
                freq = rs.getInt( "frequency" );
                /*
                String name = rs.getString( "name_str" );
                String msg = String.format( "getFrequency() name: %s, freq: %d", name, freq );
                System.out.println( msg );
                */
            }
            rs.close();
        }
        catch( Exception ex ) {
            System.out.println( "Exception in getFrequencyStr(): " + ex.getMessage() );
            ex.printStackTrace();
        }

        return freq;
    } // getFrequencyStr


    /**
     * only write positive numbers to db field, else NULL
     *
     * @param lvs_dist
     * @return
     */
    public String intOrNull( int lvs_dist ) {

        String value = "";

        if( lvs_dist != -1 ) { value = Integer.toString( lvs_dist ); }
        else { value = "null"; }

        return value;
    } // intOrNull


    public String millisec2hms( long millisec_start, long millisec_stop ) {
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
    private void elapsedShowMessage( String msg_in, long start, long stop )
    {
        String elapsed = millisec2hms( start, stop );
        String msg_out = msg_in + " " + elapsed + " elapsed";
        System.out.println( msg_out);
        try { plog.show( msg_out ); } catch( Exception ex ) { System.out.println( ex.getMessage()); }
    } // elapsedShowMessage


    /**
     * getLvsVariants1() assuming asymmetric (i.e. single-sized) tables, also containing distance = 0.
     *
     * Query the Levenshtein table lvs_table for name_int_1,
     * to get the various name_int_2's as levenshtein variants for the set s2.
     *
     * @param name_int          // an ego familyname from the set s1
     * @param lvs_table         // ls_ table to use, e.g. ls_familyname
     * @param lvs_dist_max      // max Levenshtein distance
     * @param lvsVariants       // Levenshtein variants of name
     * @param lvsDistances      // Levenshtein distances of the variants
     */
    private void getLvsVariants1( int name_int, String lvs_table, int lvs_dist_max, Vector< Integer > lvsVariants, Vector< Integer > lvsDistances )
    {
        try
        {
            String query = "";
            if( debug ) {
                query  = "( SELECT *, name_int_2 AS name_int, name_str_2 AS name_str, value FROM links_prematch." + lvs_table + " WHERE value <= " + lvs_dist_max + " AND name_int_1 = " + name_int + " ) ";
                query += "UNION ALL ";
                query += "( SELECT *, name_int_1 AS name_int, name_str_1 AS name_str, value FROM links_prematch." + lvs_table + " WHERE value <= " + lvs_dist_max + " AND name_int_2 = " + name_int + " AND value <> 0 ) ";
                query += "ORDER BY name_str;";
            }
            else {
                query  = "( SELECT name_int_2 AS name_int, value FROM links_prematch." + lvs_table + " WHERE value <= " + lvs_dist_max + " AND name_int_1 = " + name_int + ") ";
                query += "UNION ALL ";
                query += "( SELECT name_int_1 AS name_int, value FROM links_prematch." + lvs_table + " WHERE value <= " + lvs_dist_max + " AND name_int_2 = " + name_int + " AND value <> 0 );";
            }

            ResultSet rs = dbconPrematch.createStatement().executeQuery( query );

            if( debug ) {
                String msg = String.format( "getLvsVariants1(): lvs_dist_max = %d, name_int = %d", lvs_dist_max, name_int );
                System.out.println( msg ); plog.show( msg );
                System.out.println( query ); plog.show( query );
            }

            int nrecs = 0;
            while( rs.next() )
            {
                int name_int_rs = rs.getInt( "name_int" );
                int lvs_dist_rs = rs.getInt( "value" );

                if( debug ) {
                    String name_str_1 = rs.getString( "name_str_1" );
                    String name_str_2 = rs.getString( "name_str_2" );

                    String msg = String.format( "variants for name_int = %d, name_str_1 = %s, name_str_2 = %s, lvs_dist: %d", name_int_rs, name_str_1, name_str_2, lvs_dist_rs );
                    System.out.println( msg ); plog.show( msg );
                }

                lvsVariants .add( name_int_rs );
                lvsDistances.add( lvs_dist_rs );

                nrecs++;
            }
            rs.close();

            if( debug && nrecs != 0 ) {
                String msg = String.format( "getLvsVariants1(): # of LvsVariants = %d\n", nrecs );
                System.out.println( msg ); plog.show( msg );
            }
        }
        catch( Exception ex ) {
            System.out.println( "Exception in getLvsVariants1(): " + ex.getMessage() );
            System.out.println( "Abort" );
            System.exit( 1 );
        }
    } // getLvsVariants1


    /**
     * getLvsVariants2() assuming symmetric (i.e. double-sized) tables, also containing distance = 0.
     *
     * Query the Levenshtein table lvs_table for name_int_1,
     * to get the various name_int_2's as levenshtein variants for the set s2.
     *
     * @param name_int          // an ego familyname from the set s1
     * @param lvs_table         // ls_ table to use, e.g. ls_familyname
     * @param lvs_dist_max      // max Levenshtein distance
     * @param lvsVariants       // Levenshtein variants of name
     * @param lvsDistances      // Levenshtein distances of the variants
     */
    private void getLvsVariants2( int name_int, String lvs_table, int lvs_dist_max, Vector< Integer > lvsVariants, Vector< Integer > lvsDistances )
    {
        try
        {
            String query = "SELECT * FROM links_prematch." + lvs_table + " WHERE value <= " + lvs_dist_max + " AND name_int_1 = " + name_int ;
            ResultSet rs = dbconPrematch.createStatement().executeQuery( query );

             if( debug ) {
                String msg = String.format( "getLvsVariants2(): lvs_dist_max = %d, name_int = %d", lvs_dist_max, name_int );
                System.out.println( msg ); plog.show( msg );
                System.out.println( query ); plog.show( query );
             }

            int nrecs = 0;
            while( rs.next() )
            {
                int name_int_2 = rs.getInt( "name_int_2" );
                int lvs_dist   = rs.getInt( "value" );

                if( debug ) {
                    String name_str_1 = rs.getString( "name_str_1" );
                    String name_str_2 = rs.getString( "name_str_2" );

                    if( nrecs == 0 ) {
                        String msg = String.format( "variants for name_str_1 = %s (name_int_1 = %d): ", name_str_1, name_int );
                        System.out.println( msg ); plog.show( msg );
                    }

                    String msg = String.format( "name_str_2: %s (name_int_2: %d), lvs_dist: %d", name_str_2, name_int_2, lvs_dist );
                    System.out.println( msg ); plog.show( msg );
                }

                lvsVariants .add( name_int_2 );
                lvsDistances.add( lvs_dist );

                nrecs++;
            }
            rs.close();

            if( debug && nrecs != 0 ) {
                String msg = String.format( "getLvsVariants2(): # of LvsVariants = %d\n", nrecs );
                System.out.println( msg ); plog.show( msg );
            }

            //System.out.println( String.format( "getLvsVariants2(): # of LvsVariants = %d for name_int: %d\n", nrecs, name_int ) );
        }
        catch( Exception ex ) {
            System.out.println( "Exception in getLvsVariants2(): " + ex.getMessage() );
            System.out.println( "Abort" );
            System.exit( 1 );
        }
    } // getLvsVariants2


     /**
     * Query table ls_firstname or ls_lastname for s2Name in name_int_1 column,
     * to get the name_int_2 as levenshtein variants for the set s2Name.
     * Compare the variants for a match with s1Name
     */
    private int compareLvsNames( int s1Name, int s2Name, String lvs_table, int lvs_dist_max )
    {
        int lvs_dist = -1;      // -1 = no match, otherwise the found distance

        try
        {
            if( debug ) {
                String msg = "compareLvsNames(): s1Name = " + s1Name + ", s2Name = " + s2Name;
                System.out.println( msg ); plog.show( msg );
            }

            // the "ORDER BY value" gives us the smallest lvs distances first
            String query = "";

            // this query assumes the lvs_table is a symmetric (double-sized) lvs table
            //query += "SELECT * FROM links_prematch." + lvs_table + " WHERE value <= " + lvs_dist_max + " AND name_int_1 = " + s2Name + " ORDER BY value";   // old version
            //query += "SELECT name_int_2 AS name_int, value FROM links_prematch." + lvs_table + " WHERE value <= " + lvs_dist_max + " AND name_int_1 = " + s2Name + " ORDER BY value";

            // this query assumes the lvs_table is an asymmetric (single-sized) lvs table
            if( debug ) {
                query += "( SELECT name_int_2 AS name_int, name_str_2 AS name_str, value FROM links_prematch." + lvs_table + " WHERE value <= " + lvs_dist_max + " AND name_int_1 = " + s2Name + " ORDER BY value ) ";
                query += "UNION ALL ";
                query += "( SELECT name_int_1 AS name_int, name_str_1 AS name_str, value FROM links_prematch." + lvs_table + " WHERE value <= " + lvs_dist_max + " AND name_int_2 = " + s2Name + " AND value <> 0 ) ";
                query += "ORDER BY value;";
                System.out.println( query );
            }
            else {
                query += "( SELECT name_int_2 AS name_int, value FROM links_prematch." + lvs_table + " WHERE value <= " + lvs_dist_max + " AND name_int_1 = " + s2Name + " ORDER BY value ) ";
                query += "UNION ALL ";
                query += "( SELECT name_int_1 AS name_int, value FROM links_prematch." + lvs_table + " WHERE value <= " + lvs_dist_max + " AND name_int_2 = " + s2Name + " AND value <> 0 ) ";
                query += "ORDER BY value;";
            }

            ResultSet rs = dbconPrematch.createStatement().executeQuery( query );

            int nrecs = 0;
            while( rs.next() )
            {
                int name_int = rs.getInt( "name_int" );     // new

                int lvs_dist_nrec = rs.getInt( "value" );

                if( debug )
                {
                    String name_str = rs.getString( "name_str" );

                    if( nrecs == 0 ) { System.out.printf( lvs_table + " variant for %s (%d): ", name_str, name_int ); }
                    System.out.printf( "%s (%d) ", name_str, name_int );
                }

                if( s1Name == name_int )        // new
                {
                    lvs_dist = lvs_dist_nrec;
                    if( debug ) { System.out.println( "compareLvsNames(): match" );  }
                    break;
                }

                nrecs++;
            }
            rs.close();

            if( debug ) {
                String msg = "nrecs: " + nrecs;
                System.out.println( msg ); plog.show( msg );
            }
        }
        catch( Exception ex ) {
            System.out.println( "Exception in compareLvsNames: " + ex.getMessage() );
            System.out.println( "Abort" );
            ex.printStackTrace();
            System.exit( 1 );
        }

        if( debug ) { System.out.println( "compareLvsNames(): lvs_dist = " + lvs_dist );  }
        return lvs_dist;
    } // compareLvsNames


    /**
     *
     * @param qs
     * @param s1_idx
     * @param s2_idx
     * @return
     *
     * using file global ql: QueryLoader object containing s1 & s2 samples from links_base
     */
    private boolean acceptMinMax( QuerySet qs, int s1_idx, int s2_idx )
    {
        String msg = "";

        /*
        String str_minmax_e = String.format( "%03d", qs.int_minmax_e );
        String str_minmax_m = String.format( "%03d", qs.int_minmax_m );
        String str_minmax_f = String.format( "%03d", qs.int_minmax_f );
        String str_minmax_p = String.format( "%03d", qs.int_minmax_p );

        int g_flag_idx = 0;       // 1st position for the birth (geboorte) flag
        int h_flag_idx = 1;       // 2nd position for the marriage (huwelijk) flag
        int o_flag_idx = 2;       // 3rd position for the death (overlijden) flag

        boolean chk_ego_birth    = false;
        boolean chk_ego_marriage = false;
        boolean chk_ego_death    = false;

        boolean chk_mother_birth    = false;
        boolean chk_mother_marriage = false;
        boolean chk_mother_death    = false;

        boolean chk_father_birth    = false;
        boolean chk_father_marriage = false;
        boolean chk_father_death    = false;

        boolean chk_partner_birth    = false;
        boolean chk_partner_marriage = false;
        boolean chk_partner_death    = false;

        if( str_minmax_e.substring( g_flag_idx, g_flag_idx + 1 ).equals( "1" ) ) { chk_ego_birth    = true; }       //
        if( str_minmax_e.substring( h_flag_idx, h_flag_idx + 1 ).equals( "1" ) ) { chk_ego_marriage = true; }       //
        if( str_minmax_e.substring( o_flag_idx, o_flag_idx + 1 ).equals( "1" ) ) { chk_ego_death    = true; }       //

        if( str_minmax_m.substring( g_flag_idx, g_flag_idx + 1 ).equals( "1" ) ) { chk_mother_birth    = true; }    //
        if( str_minmax_m.substring( h_flag_idx, h_flag_idx + 1 ).equals( "1" ) ) { chk_mother_marriage = true; }    //
        if( str_minmax_m.substring( o_flag_idx, o_flag_idx + 1 ).equals( "1" ) ) { chk_mother_death    = true; }    //

        if( str_minmax_f.substring( g_flag_idx, g_flag_idx + 1 ).equals( "1" ) ) { chk_father_birth    = true; }    //
        if( str_minmax_f.substring( h_flag_idx, h_flag_idx + 1 ).equals( "1" ) ) { chk_father_marriage = true; }    //
        if( str_minmax_f.substring( o_flag_idx, o_flag_idx + 1 ).equals( "1" ) ) { chk_father_death    = true; }    //

        if( str_minmax_p.substring( g_flag_idx, g_flag_idx + 1 ).equals( "1" ) ) { chk_partner_birth    = true; }   //
        if( str_minmax_p.substring( h_flag_idx, h_flag_idx + 1 ).equals( "1" ) ) { chk_partner_marriage = true; }   //
        if( str_minmax_p.substring( o_flag_idx, o_flag_idx + 1 ).equals( "1" ) ) { chk_partner_death    = true; }   //
        */

        if( debug )
        {
            System.out.printf( "int_minmax_e: %d\n", qs.int_minmax_e );
            System.out.printf( "int_minmax_m: %d\n", qs.int_minmax_m );
            System.out.printf( "int_minmax_f: %d\n", qs.int_minmax_f );
            System.out.printf( "int_minmax_p: %d\n", qs.int_minmax_p );

            System.out.printf( "ego     geb: %5s, huw: %s, ovl: %5s\n", qs.chk_ego_birth,     qs.chk_ego_marriage,     qs.chk_ego_death );
            System.out.printf( "mother  geb: %5s, huw: %s, ovl: %5s\n", qs.chk_mother_birth,  qs.chk_mother_marriage,  qs.chk_mother_death );
            System.out.printf( "father  geb: %5s, huw: %s, ovl: %5s\n", qs.chk_father_birth,  qs.chk_father_marriage,  qs.chk_father_death );
            System.out.printf( "partner geb: %5s, huw: %s, ovl: %5s\n", qs.chk_partner_birth, qs.chk_partner_marriage, qs.chk_partner_death );

            /*
            System.out.printf( "int_minmax_e: %d, str_minmax_e: %s\n", qs.int_minmax_e, str_minmax_e );
            System.out.printf( "int_minmax_m: %d, str_minmax_m: %s\n", qs.int_minmax_m, str_minmax_m );
            System.out.printf( "int_minmax_f: %d, str_minmax_f: %s\n", qs.int_minmax_f, str_minmax_f );
            System.out.printf( "int_minmax_p: %d, str_minmax_p: %s\n", qs.int_minmax_p, str_minmax_p );

            System.out.printf( "ego     geb: %s, huw: %s, ovl: %s\n", chk_ego_birth,     chk_ego_marriage,     chk_ego_death );
            System.out.printf( "mother  geb: %s, huw: %s, ovl: %s\n", chk_mother_birth,  chk_mother_marriage,  chk_mother_death );
            System.out.printf( "father  geb: %s, huw: %s, ovl: %s\n", chk_father_birth,  chk_father_marriage,  chk_father_death );
            System.out.printf( "partner geb: %s, huw: %s, ovl: %s\n", chk_partner_birth, chk_partner_marriage, chk_partner_death );
            */

            if( debugfail ) {
                System.out.printf( "s1_ego_birth min/max:     %d %d\n", ql.s1_ego_birth_min.get( s1_idx ),     ql.s1_ego_birth_max.get( s1_idx ) );
                System.out.printf( "s1_mother_birth min/max:  %d %d\n", ql.s1_mother_birth_min.get( s1_idx ),  ql.s1_mother_birth_max.get( s1_idx ) );
                System.out.printf( "s1_father_birth min/max:  %d %d\n", ql.s1_father_birth_min.get( s1_idx ),  ql.s1_father_birth_max.get( s1_idx ) );
                System.out.printf( "s1_partner_birth min/max: %d %d\n", ql.s1_partner_birth_min.get( s1_idx ), ql.s1_partner_birth_max.get( s1_idx ) );

                System.out.printf( "s1_ego_marriage min/max:     %d %d\n", ql.s1_ego_marriage_min.get( s1_idx ),     ql.s1_ego_marriage_max.get( s1_idx ) );
                System.out.printf( "s1_mother_marriage min/max:  %d %d\n", ql.s1_mother_marriage_min.get( s1_idx ),  ql.s1_mother_marriage_max.get( s1_idx ) );
                System.out.printf( "s1_father_marriage min/max:  %d %d\n", ql.s1_father_marriage_min.get( s1_idx ),  ql.s1_father_marriage_max.get( s1_idx ) );
                System.out.printf( "s1_partner_marriage min/max: %d %d\n", ql.s1_partner_marriage_min.get( s1_idx ), ql.s1_partner_marriage_max.get( s1_idx ) );

                System.out.printf( "s1_ego_death min/max:     %d %d\n", ql.s1_ego_death_min.get( s1_idx ),     ql.s1_ego_death_max.get( s1_idx ) );
                System.out.printf( "s1_mother_death min/max:  %d %d\n", ql.s1_mother_death_min.get( s1_idx ),  ql.s1_mother_death_max.get( s1_idx ) );
                System.out.printf( "s1_father_death min/max:  %d %d\n", ql.s1_father_death_min.get( s1_idx ),  ql.s1_father_death_max.get( s1_idx ) );
                System.out.printf( "s1_partner_death min/max: %d %d\n", ql.s1_partner_death_min.get( s1_idx ), ql.s1_partner_death_max.get( s1_idx ) );

                System.out.printf( "s2_ego_birth min/max:     %d %d\n", ql.s2_ego_birth_min.get( s2_idx ),     ql.s2_ego_birth_max.get( s2_idx ) );
                System.out.printf( "s2_mother_birth min/max:  %d %d\n", ql.s2_mother_birth_min.get( s2_idx ),  ql.s2_mother_birth_max.get( s2_idx ) );
                System.out.printf( "s2_father_birth min/max:  %d %d\n", ql.s2_father_birth_min.get( s2_idx ),  ql.s2_father_birth_max.get( s2_idx ) );
                System.out.printf( "s2_partner_birth min/max: %d %d\n", ql.s2_partner_birth_min.get( s2_idx ), ql.s2_partner_birth_max.get( s2_idx ) );

                System.out.printf( "s2_ego_marriage min/max:     %d %d\n", ql.s2_ego_marriage_min.get( s2_idx ),     ql.s2_ego_marriage_max.get( s2_idx ) );
                System.out.printf( "s2_mother_marriage min/max:  %d %d\n", ql.s2_mother_marriage_min.get( s2_idx ),  ql.s2_mother_marriage_max.get( s2_idx ) );
                System.out.printf( "s2_father_marriage min/max:  %d %d\n", ql.s2_father_marriage_min.get( s2_idx ),  ql.s2_father_marriage_max.get( s2_idx ) );
                System.out.printf( "s2_partner_marriage min/max: %d %d\n", ql.s2_partner_marriage_min.get( s2_idx ), ql.s2_partner_marriage_max.get( s2_idx ) );

                System.out.printf( "s2_ego_death min/max:     %d %d\n", ql.s2_ego_death_min.get( s2_idx ),     ql.s2_ego_death_max.get( s2_idx ) );
                System.out.printf( "s2_mother_death min/max:  %d %d\n", ql.s2_mother_death_min.get( s2_idx ),  ql.s2_mother_death_max.get( s2_idx ) );
                System.out.printf( "s2_father_death min/max:  %d %d\n", ql.s2_father_death_min.get( s2_idx ),  ql.s2_father_death_max.get( s2_idx ) );
                System.out.printf( "s2_partner_death min/max: %d %d\n", ql.s2_partner_death_min.get( s2_idx ), ql.s2_partner_death_max.get( s2_idx ) );
            }
        }

        if( debug ) {
            System.out.printf( "s1_idx = %d, s2_idx = %d\n", s1_idx, s2_idx );
            System.out.printf( "use_minmax ego: %3d\n", qs.int_minmax_e );
        }

        //if( ( qs.int_minmax_e % 2 ) == 1 )
        //if( qs.int_minmax_e == 100 || qs.int_minmax_e == 101 || qs.int_minmax_e == 110 || qs.int_minmax_e == 111 )
        if( qs.chk_ego_birth )
        {
            if( debug ) { System.out.printf( "use_minmax ego birth...\n" ); }

            if( ( firstGreater( ql.s1_ego_birth_min.get( s1_idx ), ql.s2_ego_birth_max.get( s2_idx ) ) ) ||
                ( firstGreater( ql.s2_ego_birth_min.get( s2_idx ), ql.s1_ego_birth_max.get( s1_idx ) ) ) )
            { return false; }
        }

        // if it is a matching between a birth and death registration,
        // we skip the check for ego marriage date range, because in that case it is unreliable.
        boolean skip = false;
        if( (qs.s1_maintype == 1 && qs.s2_maintype == 3) || (qs.s1_maintype == 3 && qs.s2_maintype == 1) )
        { skip = true; }
        // the proper way to skip go marriage date range is by setting the corresponding position to 0:
        // in use_minmax: %0%.%%%.%%%.%%% in the match_priocess table, thst is easy to forget

        if( ! skip )
        {
            //if( qs.int_minmax_e ==  10 || qs.int_minmax_e ==  11 || qs.int_minmax_e == 110 || qs.int_minmax_e == 111 )
            if( qs.chk_ego_marriage )
            {
                if( debug ) { System.out.printf( "use_minmax ego marriage...\n" ); }

                if( ( firstGreater( ql.s1_ego_marriage_min.get( s1_idx ), ql.s2_ego_marriage_max.get( s2_idx ) ) ) ||
                    ( firstGreater( ql.s2_ego_marriage_min.get( s2_idx ), ql.s1_ego_marriage_max.get( s1_idx ) ) ) )
                { return false; }
            }
        }

        //if( qs.int_minmax_e > 99 )
        //if( qs.int_minmax_e ==   1 || qs.int_minmax_e ==  11 || qs.int_minmax_e == 101 || qs.int_minmax_e == 111 )
        if( qs.chk_ego_death )
        {
            if( debug ) { System.out.printf( "use_minmax ego death...\n" ); }

            if( ( firstGreater( ql.s1_ego_death_min.get( s1_idx ), ql.s2_ego_death_max.get( s2_idx ) ) ) ||
                ( firstGreater( ql.s2_ego_death_min.get( s2_idx ), ql.s1_ego_death_max.get( s1_idx ) ) ) )
            { return false; }
        }

        if( qs.use_mother )
        {
            if( debug ) { System.out.printf( "use_minmax mother: %3d\n", qs.int_minmax_m ); }

            //if( ( qs.int_minmax_m % 2 ) == 1 )
            //if( qs.int_minmax_m == 100 || qs.int_minmax_m == 101 || qs.int_minmax_m == 110 || qs.int_minmax_m == 111 )
            if( qs.chk_mother_birth )
            {
                if( ( firstGreater( ql.s1_mother_birth_min.get( s1_idx ), ql.s2_mother_birth_max.get( s2_idx ) ) ) ||
                    ( firstGreater( ql.s2_mother_birth_min.get( s2_idx ), ql.s1_mother_birth_max.get( s1_idx ) ) ) )
                { return false; }
            }

            //if( qs.int_minmax_m ==  10 || qs.int_minmax_m ==  11 || qs.int_minmax_m == 110 || qs.int_minmax_m == 111 )
            if( qs.chk_mother_marriage )
            {
                if( ( firstGreater( ql.s1_mother_marriage_min.get( s1_idx ), ql.s2_mother_marriage_max.get( s2_idx ) ) ) ||
                    ( firstGreater( ql.s2_mother_marriage_min.get( s2_idx ), ql.s1_mother_marriage_max.get( s1_idx ) ) ) )
                { return false; }
            }

            //if( qs.int_minmax_m > 99 )
            //if( qs.int_minmax_m ==   1 || qs.int_minmax_m ==  11 || qs.int_minmax_m == 101 || qs.int_minmax_m == 111 )
            if( qs.chk_mother_death )
            {
                if( ( firstGreater( ql.s1_mother_death_min.get( s1_idx ), ql.s2_mother_death_max.get( s2_idx ) ) ) ||
                    ( firstGreater( ql.s2_mother_death_min.get( s2_idx ), ql.s1_mother_death_max.get( s1_idx ) ) ) )
                { return false; }
            }
        }

        if( qs.use_father )
        {
            if( debug ) { System.out.printf( "use_minmax father: %3d\n", qs.int_minmax_f ); }

            //if( ( qs.int_minmax_f % 2 ) == 1 )    // Omar
            //if( qs.int_minmax_f == 100 || qs.int_minmax_f == 101 || qs.int_minmax_f == 110 || qs.int_minmax_f == 111 )
            if( qs.chk_father_birth )
            {
                if( ( firstGreater( ql.s1_father_birth_min.get( s1_idx ), ql.s2_father_birth_max.get( s2_idx ) ) ) ||
                    ( firstGreater( ql.s2_father_birth_min.get( s2_idx ), ql.s1_father_birth_max.get( s1_idx ) ) ) )
                { return false; }
            }

            //if( qs.int_minmax_f ==  10 || qs.int_minmax_f ==  11 || qs.int_minmax_f == 110 || qs.int_minmax_f == 111 )
            if( qs.chk_father_marriage )
            {
                if( ( firstGreater( ql.s1_father_marriage_min.get( s1_idx ), ql.s2_father_marriage_max.get( s2_idx ) ) ) ||
                    ( firstGreater( ql.s2_father_marriage_min.get( s2_idx ), ql.s1_father_marriage_max.get( s1_idx ) ) ) )
                { return false; }
            }

            //if( qs.int_minmax_f > 99 )
            //if( qs.int_minmax_f ==   1 || qs.int_minmax_f ==  11 || qs.int_minmax_f == 101 || qs.int_minmax_f == 111 )
            if( qs.chk_father_death )
            {
                if( ( firstGreater( ql.s1_father_death_min.get( s1_idx ), ql.s2_father_death_max.get( s2_idx ) ) ) ||
                    ( firstGreater( ql.s2_father_death_min.get( s2_idx ), ql.s1_father_death_max.get( s1_idx ) ) ) )
                { return false; }
            }
        }

        if( qs.use_partner )
        {
            if( debug ) { System.out.printf( "use_minmax partner: %3d\n", qs.int_minmax_p ); }

            //if( ( qs.int_minmax_p % 2 ) == 1 )
            //if( qs.int_minmax_p == 100 || qs.int_minmax_p == 101 || qs.int_minmax_p == 110 || qs.int_minmax_p == 111 )
            if( qs.chk_partner_birth )
            {
                if( ( firstGreater( ql.s1_partner_birth_min.get( s1_idx ), ql.s2_partner_birth_max.get( s2_idx ) ) ) ||
                    ( firstGreater( ql.s2_partner_birth_min.get( s2_idx ), ql.s1_partner_birth_max.get( s1_idx ) ) ) )
                { return false; }
            }

            //if( qs.int_minmax_p ==  10 || qs.int_minmax_p ==  11 || qs.int_minmax_p == 110 || qs.int_minmax_p == 111 )
            if( qs.chk_partner_marriage )
            {
                if( ( firstGreater( ql.s1_partner_marriage_min.get( s1_idx ), ql.s2_partner_marriage_max.get( s2_idx ) ) ) ||
                    ( firstGreater( ql.s2_partner_marriage_min.get( s2_idx ), ql.s1_partner_marriage_max.get( s1_idx ) ) ) )
                { return false; }
            }

            //if( qs.int_minmax_p > 99 )
            //if( qs.int_minmax_p ==   1 || qs.int_minmax_p ==  11 || qs.int_minmax_p == 101 || qs.int_minmax_p == 111 )
            if( qs.chk_partner_death )
            {
                if( ( firstGreater( ql.s1_partner_death_min.get( s1_idx ), ql.s2_partner_death_max.get( s2_idx ) ) ) ||
                    ( firstGreater( ql.s2_partner_death_min.get( s2_idx ), ql.s1_partner_death_max.get( s1_idx ) ) ) )
                { return false; }
            }
        }

        return true;
    } // acceptMinMax


    /**
     *
     * @param first
     * @param second
     * @return
     */
    private boolean firstGreater( int first, int second )
    {
        if( debug ) {
            try { plog.show( "firstGreater()" ); }
            catch( Exception ex ) { System.out.println( ex.getMessage() ); }
        }

        if( first == 0 || second == 0 ) { return false; }

        if( first > second ) { return true; }

        return false;
    } // firstGreater


    /**
     *
     * @param s1Name
     * @param s2Name
     * @param lvs_table
     * @param lvs_dist
     * @param tnt
     * @param method
     * @return
     */
    private int isVariant( int s1Name, int s2Name, String lvs_table, int lvs_dist, NameType tnt, int method )
    {
        // is s2Name a Levenshtein variant of s1Name ?

        if( debug ) {
            String msg = "isVariant() " + s1Name + ", " + s2Name + ", " + lvs_table + ", " + lvs_dist + ", " + tnt + ", " + lvs_dist;
            System.out.println( msg );
            try { plog.show( msg ); }
            catch( Exception ex ) { System.out.println( ex.getMessage() ); }
        }

        if( method == 0 )
        {
            if( tnt == NameType.FAMILYNAME )
            {
                return compareLvsNames( s1Name, s2Name, lvs_table, lvs_dist );

                /*
                if( s1Name == 0 || s2Name == 0 ) { return false; }    // NULL string names
                else if( s1Name == s2Name ) { ; }                     // identical names
                else if( variantFamilyName.length > s1Name && variantFamilyName[ s1Name ] != null && Arrays.binarySearch( variantFamilyName[ s1Name ], s2Name ) > -1) { ; }
                else if( variantFamilyName.length > s2Name && variantFamilyName[ s2Name ] != null && Arrays.binarySearch( variantFamilyName[ s2Name ], s1Name ) > -1) { ; }
                else { return false; }
                */
            }
            else if( tnt == NameType.FIRSTNAME )
            {

                return compareLvsNames( s1Name, s2Name, lvs_table, lvs_dist );

                /*
                if( s1Name == 0 || s2Name == 0 ) { return false; }    // NULL string names
                else if( s1Name == s2Name ) { ; }                     // identical names
                else if( variantFirstName.length > s1Name && variantFirstName[ s1Name ] != null && Arrays.binarySearch( variantFirstName[ s1Name ], s2Name ) > -1) { ; }
                else if( variantFirstName.length > s2Name && variantFirstName[ s2Name ] != null && Arrays.binarySearch( variantFirstName[ s2Name ], s1Name ) > -1) { ; }
                else { return false; }
                */
            }

            return -1;
        }

        else if( method == 1 )
        {
            throw new UnsupportedOperationException();

            /*
            // not implemented in the new way

            if( s1Name == s2Name ) { return true; }

            int[] root1;
            int[] root2;

            if( tnt == NameType.FAMILYNAME )
            {
                if( ( s1Name >= rootFamilyName.length ) || ( ( s2Name >= rootFamilyName.length ) ) ) { return false; }

                root1 = this.rootFamilyName[ s1Name ];
                root2 = this.rootFamilyName[ s2Name ];

            }
            else
            {
                if( ( s1Name >= rootFirstName.length ) || ( ( s2Name >= rootFirstName.length ) ) ) { return false; }

                root1 = this.rootFirstName[ s1Name ];
                root2 = this.rootFirstName[ s2Name ];
            }

            for( int i = 0; i < root1.length; i++ ) {
                for( int j = 0; j < root2.length; j++ ) {
                    if( root1[ i ] == root2[ j ]) {
                        return true;
                    }
                }
                break;
            }
            return false;
            */
        }

        else { return -1; }
    } // isVariant


    /**
     *
     * @param fn_method
     * @param s1Name1
     * @param s1Name2
     * @param s1Name3
     * @param s1Name4
     * @param s2Name1
     * @param s2Name2
     * @param s2Name3
     * @param s2Name4
     * @param method
     * @param lvs_table
     * @param lvs_dist_max
     * @return
     */
    private int checkFirstName
    (
        int fn_method,          // 'firstname' in match_process table: {0,1,2,3,4,5}
        int s1Name1, int s1Name2, int s1Name3, int s1Name4,
        int s2Name1, int s2Name2, int s2Name3, int s2Name4,
        int method,             // 'method' in match_process table: {0,1}
        String lvs_table, int lvs_dist_max
    )
    {
        int lvs_dist = -1;

        if( debug ) {
            System.out.println( "checkFirstName() fn_method: " + fn_method );
            try { plog.show( "checkFirstName() fn_method = " + fn_method ); }
            catch( Exception ex ) { System.out.println( ex.getMessage() ); }
        }

        if( fn_method == 1 )
        {
            // compare firstname1 + firstname2 of s1 & s2, but skip firstname2 comparison if they both empty
            int lvs_dist_name1, lvs_dist_name2 = -1;

            lvs_dist_name1 = isVariant( s1Name1, s2Name1, lvs_table, lvs_dist_max, NameType.FIRSTNAME, method );

            if( s1Name2 == 0 && s2Name2 == 0 ) { lvs_dist = lvs_dist_name1; }   // only using firstname1 comparison
            else
            {
                lvs_dist_name2 = isVariant( s1Name2, s2Name2, lvs_table, lvs_dist_max, NameType.FIRSTNAME, method );

                if( lvs_dist_name1 == -1 || lvs_dist_name2 == -1 ) { lvs_dist = -1; }   // no match
                else { lvs_dist = Math.max( lvs_dist_name1, lvs_dist_name2 ); }
            }
        }

        else if( fn_method == 2 )
        {
            // only compare firstname1 of s1 & s2
            lvs_dist = isVariant( s1Name1, s2Name1, lvs_table, lvs_dist_max, NameType.FIRSTNAME, method );
        }

        else if( fn_method == 3 )
        {
            // also deal with swapped firstnames, accept as match when at least one combination matches

            lvs_dist = isVariant( s1Name1, s2Name1, lvs_table, lvs_dist_max, NameType.FIRSTNAME, method );
            if( lvs_dist >= 0 ) { return lvs_dist; }
            lvs_dist = isVariant( s1Name1, s2Name2, lvs_table, lvs_dist_max, NameType.FIRSTNAME, method );
            if( lvs_dist >= 0 ) { return lvs_dist; }
            lvs_dist = isVariant( s1Name1, s2Name3, lvs_table, lvs_dist_max, NameType.FIRSTNAME, method );
            if( lvs_dist >= 0 ) { return lvs_dist; }
            lvs_dist = isVariant( s1Name1, s2Name4, lvs_table, lvs_dist_max, NameType.FIRSTNAME, method );
            if( lvs_dist >= 0 ) { return lvs_dist; }

            lvs_dist = isVariant( s1Name2, s2Name1, lvs_table, lvs_dist_max, NameType.FIRSTNAME, method );
            if( lvs_dist >= 0 ) { return lvs_dist; }
            lvs_dist = isVariant( s1Name2, s2Name2, lvs_table, lvs_dist_max, NameType.FIRSTNAME, method );
            if( lvs_dist >= 0 ) { return lvs_dist; }
            lvs_dist = isVariant( s1Name2, s2Name3, lvs_table, lvs_dist_max, NameType.FIRSTNAME, method );
            if( lvs_dist >= 0 ) { return lvs_dist; }
            lvs_dist = isVariant( s1Name2, s2Name4, lvs_table, lvs_dist_max, NameType.FIRSTNAME, method );
            if( lvs_dist >= 0 ) { return lvs_dist; }

            lvs_dist = isVariant( s1Name3, s2Name1, lvs_table, lvs_dist_max, NameType.FIRSTNAME, method );
            if( lvs_dist >= 0 ) { return lvs_dist; }
            lvs_dist = isVariant( s1Name3, s2Name2, lvs_table, lvs_dist_max, NameType.FIRSTNAME, method );
            if( lvs_dist >= 0 ) { return lvs_dist; }
            lvs_dist = isVariant( s1Name3, s2Name3, lvs_table, lvs_dist_max, NameType.FIRSTNAME, method );
            if( lvs_dist >= 0 ) { return lvs_dist; }
            lvs_dist = isVariant( s1Name3, s2Name4, lvs_table, lvs_dist_max, NameType.FIRSTNAME, method );
            if( lvs_dist >= 0 ) { return lvs_dist; }

            lvs_dist = isVariant( s1Name4, s2Name1, lvs_table, lvs_dist_max, NameType.FIRSTNAME, method );
            if( lvs_dist >= 0 ) { return lvs_dist; }
            lvs_dist = isVariant( s1Name4, s2Name2, lvs_table, lvs_dist_max, NameType.FIRSTNAME, method );
            if( lvs_dist >= 0 ) { return lvs_dist; }
            lvs_dist = isVariant( s1Name4, s2Name3, lvs_table, lvs_dist_max, NameType.FIRSTNAME, method );
            if( lvs_dist >= 0 ) { return lvs_dist; }
            lvs_dist = isVariant( s1Name4, s2Name4, lvs_table, lvs_dist_max, NameType.FIRSTNAME, method );
            if( lvs_dist >= 0 ) { return lvs_dist; }
        }

        return lvs_dist;
    } // checkFirstName

}

