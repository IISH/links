package linksmatchmanager;

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

import linksmatchmanager.DataSet.InputSet;
import linksmatchmanager.DataSet.NameType;
import linksmatchmanager.DataSet.QueryGroupSet;
import linksmatchmanager.DataSet.QuerySet;


/**
 * @author Omar Azouguagh
 * @author Fons Laan
 *
 * FL-15-Jan-2015 Each thread its own db connectors
 * FL-26-May-2015 Latest change
 *
 * "Vectors are synchronized. Any method that touches the Vector's contents is thread safe.
 * ArrayList, on the other hand, is unsynchronized, making them, therefore, not thread safe."
 *  -> So in MatchAsync we should use Vectors
 */

public class MatchAsync extends Thread
{
    boolean debug;
    boolean free_vecs;

    //ProcessManager pm;
    Semaphore pm;

    int n_mp;           // match_process 'y' records: 0...
    int n_qs;           // query sets: 0...

    QueryLoader ql;     // contains s1 & s2 samples from links_base
    PrintLogger plog;

    QueryGroupSet qgs;
    InputSet inputSet;

    int s1_offset;      // which record to start from sample 1
    int s1_piece;       // how many records from sample 1

    Connection dbconPrematch;
    Connection dbconMatch;

    String url;
    String user;
    String pass;

    String lvs_table_firstname;
    String lvs_table_familyname;

    String freq_table_firstname;
    String freq_table_familyname;

    int[][] variantFirstName;
    int[][] variantFamilyName;

    int[][] rootFirstName;
    int[][] rootFamilyName;

    boolean isUseRoot = false;      // false for variant


    public MatchAsync       // for variant names
    (
        boolean debug,
        boolean free_vecs,

        //ProcessManager pm,
        Semaphore pm,

        int n_mp,
        int n_qs,

        QueryLoader ql,
        PrintLogger plog,

        QueryGroupSet qgs,
        InputSet inputSet,

        int s1_offset,
        int s1_piece,

        //Connection dbconPrematch,
        //Connection dbconMatch,
        String url,
        String user,
        String pass,

        String lvs_table_firstname,
        String lvs_table_familyname,

        String freq_table_firstname,
        String freq_table_familyname,

        int[][] variantFirstName,
        int[][] variantFamilyName
    )
    {
        this.debug = debug;
        this.free_vecs = free_vecs;

        this.pm = pm;

        this.n_mp = n_mp;
        this.n_qs = n_qs;

        this.ql = ql;
        this.plog = plog;

        this.qgs = qgs;
        this.inputSet = inputSet;

        this.s1_offset = s1_offset;
        this.s1_piece  = s1_piece;

        //this.dbconPrematch = dbconPrematch;
        //this.dbconMatch    = dbconMatch;
        //this.dbconTemp     = dbconTemp;
        this.url  = url;
        this.user = user;
        this.pass = pass;

        this.lvs_table_firstname  = lvs_table_firstname;
        this.lvs_table_familyname = lvs_table_familyname;

        this.freq_table_firstname  = freq_table_firstname;
        this.freq_table_familyname = freq_table_familyname;

        this.variantFirstName  = variantFirstName;
        this.variantFamilyName = variantFamilyName;

        System.out.println( "\nMatchAsync: using variant names (instead of root names)" );
    }


    public MatchAsync       // for root names
    (
        boolean debug,
        boolean free_vecs,

        //ProcessManager pm,
        Semaphore pm,

        int n_mp,
        int n_qs,

        QueryLoader ql,
        PrintLogger plog,

        QueryGroupSet qgs,
        InputSet inputSet,

        int s1_offset,
        int s1_piece,

        //Connection dbconPrematch,
        //Connection dbconMatch,
        String url,
        String user,
        String pass,

        String lvs_table_firstname,
        String lvs_table_familyname,

        String freq_table_firstname,
        String freq_table_familyname,

        int[][] rootFirstName,
        int[][] rootFamilyName,

        boolean root
    )
    {
        this.debug = debug;
        this.free_vecs = free_vecs;

        this.pm = pm;

        this.n_mp = n_mp;
        this.n_qs = n_qs;

        this.ql = ql;
        this.plog = plog;

        this.qgs = qgs;
        this.inputSet = inputSet;

        this.s1_offset = s1_offset;
        this.s1_piece = s1_piece;

        //this.dbconPrematch = dbconPrematch;
        //this.dbconMatch    = dbconMatch;
        this.url  = url;
        this.user = user;
        this.pass = pass;

        this.lvs_table_firstname  = lvs_table_firstname;
        this.lvs_table_familyname = lvs_table_familyname;

        this.freq_table_firstname  = freq_table_firstname;
        this.freq_table_familyname = freq_table_familyname;

        this.rootFirstName  = rootFirstName;
        this.rootFamilyName = rootFamilyName;

        this.isUseRoot = true;      // true for root

        System.out.println( "\nMatchAsync: using root names (instead of variant names)" );
    }

    @Override
    public void run()
    {
        // If you want the actual CPU time of the current thread (or indeed, any arbitrary thread) rather than
        // the wall clock time then you can get this via ThreadMXBean. Basically, do this at the start:
        ThreadMXBean threadMXB = ManagementFactory.getThreadMXBean();
        threadMXB.setThreadCpuTimeEnabled( true );

        boolean debugfail = false;       // debug match failures
        boolean debugfreq = true;       // debug name frequencies

        // in order to show the indexes when an exception occurs, we define copies outside the try/catch
        int s1_idx_cpy = 0;
        int lv_idx_cpy = 0;

        // count why the matches fail
        long n_int_familyname_e = 0;
        long n_int_familyname_m = 0;
        long n_int_familyname_f = 0;
        long n_int_familyname_p = 0;

        long n_int_firstname_e = 0;
        long n_int_firstname_m = 0;
        long n_int_firstname_f = 0;
        long n_int_firstname_p = 0;

        long n_minmax = 0;
        long n_sex    = 0;


        try
        {
            long threadStart = System.currentTimeMillis();      // ? clock time or process time

            long nanoseconds_begin  = ManagementFactory.getThreadMXBean().getThreadCpuTime( Thread.currentThread().getId() );
            long milliseconds_begin = TimeUnit.SECONDS.convert( nanoseconds_begin, TimeUnit.MILLISECONDS );

            long threadId = Thread.currentThread().getId();
            String msg = String.format( "\nMatchAsync/run(): thread id %2d running", threadId );
            System.out.println( msg ); plog.show( msg );

            msg = String.format( "Thread id %2d; process id: %d", threadId, qgs.get( 0 ).id );
            System.out.println( msg ); plog.show( msg );

            msg = String.format( "Thread id %2d; Range %d of %d", threadId, (n_qs + 1), qgs.getSize() );
            System.out.println( msg ); plog.show( msg );

            // Create database connections
            dbconMatch    = General.getConnection( url, "links_match",    user, pass );
            dbconPrematch = General.getConnection( url, "links_prematch", user, pass );

            // Get a QuerySet object. This object will contains the data from a match_process table record
            QuerySet qs = qgs.get( n_qs );

            // Levenshtein distances to use to get the variant names
            int lvs_dist_firstname  = qs.prematch_firstname_value;
            int lvs_dist_familyname = qs.prematch_familyname_value;

            msg = String.format( "Thread id %2d; use firstname  levenshtein distance for variants: %d", threadId, lvs_dist_firstname );
            System.out.println( msg ); plog.show( msg );
            msg = String.format( "Thread id %2d; use familyname levenshtein distance for variants: %d", threadId, lvs_dist_familyname );
            System.out.println( msg ); plog.show( msg );


            long threadId_current = Thread.currentThread().getId();

            msg = String.format( "Thread id %2d, based on s1 query:\n%s", threadId_current, qs.s1_query );
            System.out.println( msg ); plog.show( msg );

            msg = String.format( "Thread id %2d, based on s2 query:\n%s", threadId_current, qs.s2_query );
            System.out.println( msg ); plog.show( msg );

            // Previous s1EgoFamilyName, initial is 0. Because the familynames are ordered, the calculation of the potential
            // matches is done once, only the first time.
            int previous_s1Name = 0;

            // variant names for s1 ego familyname from ls_ table, plus lvs distance
            Vector< Integer > lvsVariants  = new Vector< Integer >();
            Vector< Integer > lvsDistances = new Vector< Integer >();

            // all occurrences in the s2 set of the above variants
            Vector< Integer > s2_idx_variants_ego       = new Vector< Integer >();

            Vector< Integer > s2_idx_firstname_ego_lvs  = new Vector< Integer >();
            Vector< Integer > s2_idx_familyname_ego_lvs = new Vector< Integer >();

            Vector< Integer > s2_idx_firstname_mo_lvs   = new Vector< Integer >();
            Vector< Integer > s2_idx_familyname_mo_lvs  = new Vector< Integer >();

            Vector< Integer > s2_idx_firstname_fa_lvs   = new Vector< Integer >();
            Vector< Integer > s2_idx_familyname_fa_lvs  = new Vector< Integer >();

            Vector< Integer > s2_idx_firstname_pa_lvs   = new Vector< Integer >();
            Vector< Integer > s2_idx_familyname_pa_lvs  = new Vector< Integer >();

            // Loop through set 1
            //msg = String.format( "Thread id %2d; Set 1 size: %d from links_base", threadId, ql.s1_id_base.size() );
            msg = String.format( "Thread id %2d; Set 1 offset: %d, piece: %d from links_base", threadId, s1_offset, s1_piece );
            System.out.println( msg );
            plog.show( msg );

            long n_recs   = 0;
            long n_match  = 0;

            int s1_size  = ql.s1_id_base.size();
            int s1_nchunks = 100;
            int s1_chunk = s1_piece / s1_nchunks;

            // Outer loop over the records of the s1 query set
            // individual threads get a portion of s1: start at s1_offset and process s1_piece records
            for( int s1_idx = s1_offset; s1_idx < (s1_offset + s1_piece); s1_idx++ )
            {
                n_recs ++;

                s1_idx_cpy = s1_idx;   // copy value to display if exception occurs

                if( s1_chunk != 0 && ( ( s1_idx + s1_chunk ) % s1_chunk == 0 ) )
                { System.out.println( String.format( "Thread id %2d; records processed: %d-of-%d, matches found: %d", threadId , s1_idx, s1_size, n_match ) ); }
                //{ System.out.println( "Thread id " + threadId + "; records processed: " + s1_idx + ", matches found: " + n_match ); }

                /*
                int id_registration1 = ql.s1_id_registration.get( s1_idx );
                if( id_registration1 == 2555759 ) {
                    System.out.println( "found: id_registration1: " + id_registration1 );
                    debug = true;
                }
                else {
                    continue;
                    debug = false;
                }
                */

                int s1EgoFamName  = ql.s1_ego_familyname.get( s1_idx );
                int s1EgoFirName1 = ql.s1_ego_firstname1.get( s1_idx );
                int s1EgoFirName2 = ql.s1_ego_firstname2.get( s1_idx );
                int s1EgoFirName3 = ql.s1_ego_firstname3.get( s1_idx );
                int s1EgoFirName4 = ql.s1_ego_firstname4.get( s1_idx );

                int s1MotherFamName  = ql.s1_mother_familyname.get( s1_idx );
                int s1MotherFirName1 = ql.s1_mother_firstname1.get( s1_idx );
                int s1MotherFirName2 = ql.s1_mother_firstname2.get( s1_idx );
                int s1MotherFirName3 = ql.s1_mother_firstname3.get( s1_idx );
                int s1MotherFirName4 = ql.s1_mother_firstname4.get( s1_idx );

                int s1FatherFamName  = ql.s1_father_familyname.get( s1_idx );
                int s1FatherFirName1 = ql.s1_father_firstname1.get( s1_idx );
                int s1FatherFirName2 = ql.s1_father_firstname2.get( s1_idx );
                int s1FatherFirName3 = ql.s1_father_firstname3.get( s1_idx );
                int s1FatherFirName4 = ql.s1_father_firstname4.get( s1_idx );

                int s1PartnerFamName  = ql.s1_partner_familyname.get( s1_idx );
                int s1PartnerFirName1 = ql.s1_partner_firstname1.get( s1_idx );
                int s1PartnerFirName2 = ql.s1_partner_firstname2.get( s1_idx );
                int s1PartnerFirName3 = ql.s1_partner_firstname3.get( s1_idx );
                int s1PartnerFirName4 = ql.s1_partner_firstname4.get( s1_idx );

                /*
                //if( n_recs == 7 ) { System.exit( 0 ); }

                // get frequencies of used names, process from low to high frequency
                ListMultimap<Integer, String> nameFreqMap = check_frequencies( debugfreq, qs, s1_idx );

                System.out.println( "" );
                System.out.println( "entries: " + nameFreqMap.size() );
                Collection< String > values = nameFreqMap.values();

                String entries = nameFreqMap.values().toString();
                System.out.println( entries );

                System.out.println( nameFreqMap.keys() );

                for ( String name : values )    // names in frequency order
                {
                    System.out.println( name );
                    if( name == "ego_familyname" ) {
                        System.out.println( "ego_familyname..." );

                    }
                    else if( name == "ego_firstname" ) {
                        System.out.println( "ego_firstname..." );

                    }
                    else if( name == "mother_familyname" ) {
                        System.out.println( "mother_familyname..." );

                    }
                    else if( name == "mother_firstname" ) {
                        System.out.println( "mother_firstname..." );

                    }
                    else if( name == "father_familyname" ) {
                        System.out.println( "father_familyname..." );

                    }
                    else if( name == "father_firstname" ) {
                        System.out.println( "father_firstname..." );

                    }
                    else if( name == "partner_familyname" ) {
                        System.out.println( "partner_familyname..." );

                    }
                    else if( name == "partner_firstname" ) {
                        System.out.println( "partner_firstname..." );

                    }
                    else {
                        System.out.println( "name ??" );
                        System.exit( 1 );
                    }
                }
                */

                // Get ego names of Set 1
                String s1EgoFamNameStr = ql.s1_ego_familyname_str.get( s1_idx );
                String s1EgoFirNameStr = ql.s1_ego_firstname1_str.get( s1_idx );
                if( debug ) { System.out.printf( "s1 ego familyname: %s,  s1 ego firstname1: %s\n", s1EgoFamNameStr, s1EgoFirNameStr ); }


                // If the s1 name or lvs table changes, create a new variant names list, otherwise go on
                // to check the other s1 entries with the same name against this set.

                // we used to start with s1EgoFamName
                int s1Name = s1EgoFamName;
                String s1LvsTable = "lvs_table_familyname";

                if( s1EgoFamName != previous_s1Name )
                {
                    if( debug ) {
                        msg = String.format( "s1EgoFamNameStr: %s", s1EgoFamNameStr );
                        System.out.println( msg ); plog.show( msg );
                    }

                    previous_s1Name = s1EgoFamName;        // Set previous name

                    // Get the variants of name s1EgoFamName; these are names (as ints) from the ls_ table
                    lvsVariants .clear();                           // Empty the lists
                    lvsDistances.clear();

                    // fill the lists lvsVariants and lvsDistances lists for variants of s1EgoFamName
                    getLvsVariants( s1EgoFamName, lvs_table_familyname, lvs_dist_familyname, lvsVariants, lvsDistances );

                    s2_idx_variants_ego      .clear();                  // Empty the lists

                    s2_idx_firstname_ego_lvs .clear();
                    s2_idx_familyname_ego_lvs.clear();

                    s2_idx_firstname_mo_lvs .clear();
                    s2_idx_familyname_mo_lvs.clear();

                    s2_idx_firstname_fa_lvs .clear();
                    s2_idx_familyname_fa_lvs.clear();

                    s2_idx_firstname_pa_lvs .clear();
                    s2_idx_familyname_pa_lvs.clear();

                    for( int lv_idx = 0; lv_idx < lvsVariants.size(); lv_idx++ )
                    {
                        lv_idx_cpy = lv_idx;        // copy for display if exception occurs

                        if( debug ) {
                            msg = String.format( "lv idx: %d-of-%d", lv_idx + 1, lvsVariants.size() );
                            System.out.println( msg );
                            plog.show( msg );
                        }

                        int s2EgoFamName = lvsVariants.get( lv_idx );                   // potential match

                        if( debug ) {
                            msg = String.format( "Find all occurrences of the %d s1EgoFamName variants in s2", lvsVariants.size() );
                            System.out.println( msg ); plog.show( msg );
                        }

                        int offset = 0;
                        while( true )
                        {
                            int s2_idx = ql.s2_ego_familyname.indexOf( s2EgoFamName, offset );      // index in s2 set

                            if( s2_idx == -1 ) { break; }       // no more occurences
                            else                                // we got one
                            {
                                if( debug ) {
                                    String s2EgoFamName_str = ql.s2_ego_familyname_str.get( s2_idx );
                                    int id_base =  ql.s2_id_base.get( s2_idx );
                                    int id_regist = ql.s2_id_registration.get( s2_idx );
                                    msg = String.format( "lvs variant of s1EgoFamName found in s2EgoFamName: %d %s, id_base: %d, id_regist: %d",
                                        s2EgoFamName, s2EgoFamName_str, id_base, id_regist );
                                    System.out.println( msg ); plog.show( msg );
                                }

                                s2_idx_variants_ego      .add( s2_idx );
                                s2_idx_familyname_ego_lvs.add( lvsDistances.get( lv_idx ) );    // Lvs distance from ls_ table

                                // do not yet know the values, but must keep the vectors having equal length
                                s2_idx_firstname_ego_lvs.add( -1 );

                                s2_idx_familyname_mo_lvs.add( -1 );
                                s2_idx_firstname_mo_lvs .add( -1 );

                                s2_idx_familyname_fa_lvs.add( -1 );
                                s2_idx_firstname_fa_lvs .add( -1 );

                                s2_idx_familyname_pa_lvs.add( -1 );
                                s2_idx_firstname_pa_lvs .add( -1 );

                                offset = s2_idx + 1;            // offset for next search
                                if( offset >= ql.s2_ego_familyname.size() ) { break; }  // no more occurences
                            }
                        }
                        if( debug ) {
                            msg = String.format( "s2_idx_variants_ego size: %d", s2_idx_variants_ego.size() );
                            System.out.println( msg ); plog.show( msg );
                        }
                    }
                    //System.out.println( "" );
                }

                // The above variant vectors are only refreshed when a new ego familyname occurs.
                // As long as the ego familyname stays the same, we reuse the variant vectors.
                // We flag a non-match by setting the current s2_idx_variants entry to -1.
                // Because of the reuse, we must use a copy of s2_idx_variants for the -1 setting.
                Vector< Integer > s2_idx_variants_cpy = new Vector( s2_idx_variants_ego );

                if( debug  && s2_idx_variants_ego.size() > 0 ) {
                    for( int lv_idx = 0; lv_idx < s2_idx_variants_ego.size(); lv_idx++ ) {
                        int s2_idx = s2_idx_variants_ego.get( lv_idx );
                        System.out.println( "s2_idx: " + s2_idx );
                    }
                    System.out.println( "" );

                    msg = String.format( "Thread id %2d; potential matches: %d", threadId, s2_idx_variants_ego.size() );
                    System.out.println( msg ); plog.show( msg );

                    msg = String.format( "loop through the %d familynames of s2; exact + variants", s2_idx_variants_cpy.size() );
                    System.out.println( msg ); plog.show( msg );
                }

                for( int lv_idx = 0; lv_idx < s2_idx_variants_cpy.size(); lv_idx++ )
                {
                    lv_idx_cpy = lv_idx;        // copy for display if exception occurs

                    int s2_idx = s2_idx_variants_ego.get( lv_idx );

                    int id_registration2 = ql.s2_id_registration.get( s2_idx );
                    // the debug/test selection is done on the basis of s1_idx; do not skip id_registration2's here

                    // familyname
                    int s2EgoFamName      = ql.s2_ego_familyname    .get( s2_idx );
                    int s2MotherFamName   = ql.s2_mother_familyname .get( s2_idx );
                    int s2FatherFamName   = ql.s2_father_familyname .get( s2_idx );
                    int s2PartnerFamName  = ql.s2_partner_familyname.get( s2_idx );

                    // firstname 1
                    int s2EgoFirName1     = ql.s2_ego_firstname1    .get( s2_idx );
                    int s2MotherFirName1  = ql.s2_mother_firstname1 .get( s2_idx );
                    int s2FatherFirName1  = ql.s2_father_firstname1 .get( s2_idx );
                    int s2PartnerFirName1 = ql.s2_partner_firstname1.get( s2_idx );

                    // firstname 2
                    int s2EgoFirName2     = ql.s2_ego_firstname2    .get( s2_idx );
                    int s2MotherFirName2  = ql.s2_mother_firstname2 .get( s2_idx );
                    int s2FatherFirName2  = ql.s2_father_firstname2 .get( s2_idx );
                    int s2PartnerFirName2 = ql.s2_partner_firstname2.get( s2_idx );

                    // firstname 3
                    int s2EgoFirName3     = ql.s2_ego_firstname3    .get( s2_idx );
                    int s2MotherFirName3  = ql.s2_mother_firstname3 .get( s2_idx );
                    int s2FatherFirName3  = ql.s2_father_firstname3 .get( s2_idx );
                    int s2PartnerFirName3 = ql.s2_partner_firstname3.get( s2_idx );

                    // firstname 4
                    int s2EgoFirName4     = ql.s2_ego_firstname4    .get( s2_idx );
                    int s2MotherFirName4  = ql.s2_mother_firstname4 .get( s2_idx );
                    int s2FatherFirName4  = ql.s2_father_firstname4 .get( s2_idx );
                    int s2PartnerFirName4 = ql.s2_partner_firstname4.get( s2_idx );


                    // 2-of-10 Mother familyname
                    if( qs.use_mother && qs.int_familyname_m > 0 )
                    {
                        int lv_dist = isVariant( s1MotherFamName, s2MotherFamName, lvs_table_familyname, lvs_dist_familyname, NameType.FAMILYNAME, qs.method );

                        if( lv_dist >= 0 )  // match
                        {
                            if( debugfail ) { System.out.println( "matched int_familyname_m" ); }
                            s2_idx_familyname_mo_lvs.set( lv_idx, lv_dist );         // for match table
                        }
                        else
                        {
                            s2_idx_variants_cpy.set( lv_idx, -1 );       // no match
                            n_int_familyname_m++;
                            if( debugfail ) { System.out.println( "failed int_familyname_m" ); }
                            continue;
                        }
                    }

                    // 3-of-10 Partner familyname
                    if( qs.use_partner && qs.int_familyname_p > 0 )
                    {
                        int lv_dist = isVariant( s1PartnerFamName, s2PartnerFamName, lvs_table_familyname, lvs_dist_familyname, NameType.FAMILYNAME, qs.method );

                        if( lv_dist >= 0 )  // match
                        {
                            if( debugfail ) { System.out.println( "matched int_familyname_p" ); }
                            s2_idx_familyname_pa_lvs.set( lv_idx, lv_dist );         // for match table
                        }
                        else
                        {
                            s2_idx_variants_cpy.set( lv_idx, -1 );       // no match
                            n_int_familyname_p++;
                            if( debugfail ) { System.out.println( "failed int_familyname_p" ); }
                            continue;
                        }
                    }

                    // 4-of-10 Ego firstname
                    if( qs.int_firstname_e > 0 ) {
                        int lv_dist = checkFirstName( qs.firstname,
                            s1EgoFirName1, s1EgoFirName2, s1EgoFirName3, s1EgoFirName4,
                            s2EgoFirName1, s2EgoFirName2, s2EgoFirName3, s2EgoFirName4,
                            qs.method, lvs_table_firstname, lvs_dist_firstname );

                        if( lv_dist >= 0 )  // match
                        {
                            if( debugfail ) { System.out.println( "matched int_firstname_e" ); }
                            s2_idx_firstname_ego_lvs.set( lv_idx, lv_dist );         // for match table
                        }
                        else
                        {
                            s2_idx_variants_cpy.set( lv_idx, -1 );              // no match
                            n_int_firstname_e++;
                            if( debugfail ) { System.out.println( "failed int_firstname_e" ); }
                            continue;
                        }
                    }

                    // 5-of-10 Mother firstname
                    if( qs.use_mother && qs.int_firstname_m > 0 )
                    {
                        int lv_dist = checkFirstName( qs.firstname,
                            s1MotherFirName1, s1MotherFirName2, s1MotherFirName3, s1MotherFirName4,
                            s2MotherFirName1, s2MotherFirName2, s2MotherFirName3, s2MotherFirName4,
                            qs.method, lvs_table_firstname, lvs_dist_firstname );

                        if( lv_dist >= 0 )  // match
                        {
                            if( debugfail ) { System.out.println( "matched int_familyname_m" ); }
                            s2_idx_firstname_mo_lvs.set( lv_idx, lv_dist );         // for match table
                        }
                        else
                        {
                            s2_idx_variants_cpy.set( lv_idx, -1 );       // no match
                            n_int_firstname_m++;
                            if( debugfail ) { System.out.println( "failed int_firstname_m" ); }
                            continue;
                        }
                    }

                    // 6-of-10 Father firstname
                    if( qs.use_father && qs.int_firstname_f > 0 )
                    {
                        int lv_dist = checkFirstName( qs.firstname,
                            s1FatherFirName1, s1FatherFirName2, s1FatherFirName3, s1FatherFirName4,
                            s2FatherFirName1, s2FatherFirName2, s2FatherFirName3, s2FatherFirName4,
                            qs.method, lvs_table_firstname, lvs_dist_firstname );

                        if( lv_dist >= 0 )  // match
                        {
                            if( debugfail ) { System.out.println( "matched int_firstname_f" ); }
                            s2_idx_firstname_fa_lvs.set( lv_idx, lv_dist );         // for match table
                        }
                        else
                        {
                            s2_idx_variants_cpy.set( lv_idx, -1 );       // no match
                            n_int_firstname_f++;
                            if( debugfail ) { System.out.println( "failed int_firstname_f" ); }
                            continue;
                        }
                    }

                    // 7-of-10 Partner firstname
                    if( qs.use_partner && qs.int_firstname_p > 0 )
                    {
                        int lv_dist = checkFirstName( qs.firstname,
                            s1PartnerFirName1, s1PartnerFirName2, s1PartnerFirName3, s1PartnerFirName4,
                            s2PartnerFirName1, s2PartnerFirName2, s2PartnerFirName3, s2PartnerFirName4,
                            qs.method, lvs_table_firstname, lvs_dist_firstname );

                        if( lv_dist >= 0 )  // match
                        {
                            if( debugfail ) { System.out.println( "matched int_firstname_p" ); }
                            s2_idx_firstname_pa_lvs.set( lv_idx, lv_dist );         // for match table
                        }
                        else
                        {
                            s2_idx_variants_cpy.set( lv_idx, -1 );       // no match
                            n_int_firstname_p++;
                            if( debugfail ) { System.out.println( "failed int_firstname_p" ); }
                            continue;
                        }
                    }

                    // 8-of-10 Check min max; use new min max
                    if( ! qs.ignore_minmax )
                    {
                        if( ! CheckMinMax( qs, s1_idx, s2_idx ) ) {
                            s2_idx_variants_cpy.set( lv_idx, -1 );           // no match
                            n_minmax++;
                            if( debugfail ) { System.out.println( "failed _minmax" ); }
                            continue;
                        }
                        else { if( debugfail ) { System.out.println( "matched _minmax" ); } }
                    }

                    // 9-of-10 Check sex
                    if( ! qs.ignore_sex )
                    {
                        int s1_sex = ql.s1_sex.get( s1_idx );
                        int s2_sex = ql.s2_sex.get( s2_idx );



                        if( ( s1_sex == 1 && s2_sex == 2 ) || ( s1_sex == 2 && s2_sex == 1 ) )  // no match
                        {
                            s2_idx_variants_cpy.set( lv_idx, -1 );
                            n_sex++;
                            if( debugfail ) { System.out.println( "failed _sex: s1_sex=" + s1_sex + ", s2_sex=" + s2_sex ); }
                            continue;
                        }
                        else { if( debugfail ) { System.out.println( "matched _sex: s1_sex=" + s1_sex + ", s2_sex=" + s2_sex ); } }
                    }

                    // 10-of-10 Father familyname
                    if( qs.use_father && qs.int_familyname_f > 0 )
                    {
                        int lv_dist = isVariant( s1FatherFamName, s2FatherFamName, lvs_table_familyname, lvs_dist_familyname, NameType.FAMILYNAME, qs.method );

                        if( lv_dist >= 0 )  // match
                        {
                            if( debugfail ) { System.out.println( "matched int_familyname_f" ); }
                            s2_idx_familyname_fa_lvs.set( lv_idx, lv_dist );         // for match table
                        }
                        else
                        {
                            s2_idx_variants_cpy.set( lv_idx, -1 );       // no match
                            n_int_familyname_f++;
                            if( debugfail ) { System.out.println( "failed int_familyname_f" ); }
                            continue;
                        }
                    }
                }

                if( debug ) {
                    System.out.println( "s1EgoFamName variants done" );
                    plog.show( "s1EgoFamName variants done" );
                }

                // entries of matchesList that have not been zeroed above imply a match
                for( int l = 0; l < s2_idx_variants_cpy.size(); l++ )
                {
                    int lv_idx = s2_idx_variants_cpy.get( l );
                    if( lv_idx == -1 )        // this variant ego familyname had no match
                    {
                        n_int_familyname_e++;
                        if( debug ) {
                            msg = String.format( "variant %d: no match\n", l );
                            System.out.println( msg );
                            plog.show( msg );
                        }
                    }
                    else
                    {
                        n_match++;

                        int id_match_process = inputSet.get( n_mp ).get( 0 ).id;

                        int id_s1 = ql.s1_id_base.get( s1_idx );
                        int id_s2 = ql.s2_id_base.get( lv_idx );

                        String lvs_dist_first_ego  = intOrNull( s2_idx_firstname_ego_lvs .get( l ) );
                        String lvs_dist_family_ego = intOrNull( s2_idx_familyname_ego_lvs.get( l ) );

                        String lvs_dist_first_mo   = intOrNull( s2_idx_firstname_mo_lvs .get( l ) );
                        String lvs_dist_family_mo  = intOrNull( s2_idx_familyname_mo_lvs.get( l ) );

                        String lvs_dist_first_fa   = intOrNull( s2_idx_firstname_fa_lvs .get( l ) );
                        String lvs_dist_family_fa  = intOrNull( s2_idx_familyname_fa_lvs.get( l ) );

                        String lvs_dist_first_pa   = intOrNull( s2_idx_firstname_pa_lvs .get( l ) );
                        String lvs_dist_family_pa  = intOrNull( s2_idx_familyname_pa_lvs.get( l ) );

                        String query = "INSERT INTO matches ( id_match_process , id_linksbase_1 , id_linksbase_2, " +
                            "value_firstname_ego, value_familyname_ego, " +
                            "value_firstname_mo , value_familyname_mo , " +
                            "value_firstname_fa , value_familyname_fa , " +
                            "value_firstname_pa , value_familyname_pa ) " +
                            "VALUES ( " + id_match_process + "," + id_s1 + "," + id_s2 + "," +
                            lvs_dist_first_ego + "," + lvs_dist_family_ego + "," +
                            lvs_dist_first_mo  + "," + lvs_dist_family_mo  + "," +
                            lvs_dist_first_fa  + "," + lvs_dist_family_fa  + "," +
                            lvs_dist_first_pa  + "," + lvs_dist_family_pa  + ")";

                        if( debug ) {
                            msg = String.format( "variant %d: %s", l, query );
                            System.out.println( msg );
                            plog.show( msg );
                        }

                        dbconMatch.createStatement().execute( query );
                        dbconMatch.createStatement().close();
                    }
                }
                s2_idx_variants_cpy = null;
                System.out.flush();
                System.err.flush();
            }

            //int processCount = pm.removeProcess();
            pm.release();
            int processCount = pm.availablePermits();
            plog.show( "processCount: " + processCount );

            msg = String.format( "Thread id %2d; s1 records processed:         %10d", threadId, n_recs );
            System.out.println( msg ); plog.show( msg );

            msg = String.format( "Thread id %2d; Number of matches:            %10d", threadId, n_match );
            System.out.println( msg ); plog.show( msg );

            long n_fail = 0;

            if( n_int_familyname_e != 0 ) {
                n_fail += n_int_familyname_e;
                msg = String.format( "Thread id %2d; failures n_int_familyname_e:  %10d", threadId, n_int_familyname_e );
                System.out.println( msg ); plog.show( msg );
            }

            if( n_int_familyname_m != 0 ) {
                n_fail += n_int_familyname_m;
                msg = String.format( "Thread id %2d; failures n_int_familyname_m:  %10d", threadId, n_int_familyname_m );
                System.out.println( msg ); plog.show( msg );
            }

            if( n_int_familyname_f != 0 ) {
                n_fail += n_int_familyname_f;
                msg = String.format( "Thread id %2d; failures n_int_familyname_f:  %10d", threadId, n_int_familyname_f );
                System.out.println( msg ); plog.show( msg );
            }

            if( n_int_familyname_p != 0 ) {
                n_fail += n_int_familyname_p;
                msg = String.format( "Thread id %2d; failures n_int_familyname_p:  %10d", threadId, n_int_familyname_p );
                System.out.println( msg ); plog.show( msg );
            }

            if( n_int_firstname_e != 0 ) {
                n_fail += n_int_firstname_e;
                msg = String.format( "Thread id %2d; failures n_int_firstname_e:   %10d", threadId, n_int_firstname_e );
                System.out.println( msg ); plog.show( msg );
            }

            if( n_int_firstname_m != 0 ) {
                n_fail += n_int_firstname_m;
                msg = String.format( "Thread id %2d; failures n_int_firstname_m:   %10d", threadId, n_int_firstname_m );
                System.out.println( msg ); plog.show( msg );
            }

            if( n_int_firstname_f != 0 ) {
                n_fail += n_int_firstname_f;
                msg = String.format( "Thread id %2d; failures n_int_firstname_f:   %10d", threadId, n_int_firstname_f );
                System.out.println( msg ); plog.show( msg );
            }

            if( n_int_firstname_p != 0 ) {
                n_fail += n_int_firstname_p;
                msg = String.format( "Thread id %2d; failures n_int_firstname_p:   %10d", threadId, n_int_firstname_p );
                System.out.println( msg ); plog.show( msg );
            }

            if( n_minmax != 0 ) {
                n_fail += n_minmax;
                msg = String.format( "Thread id %2d; failures n_minmax:            %10d", threadId, n_minmax );
                System.out.println( msg ); plog.show( msg );
            }

            if( n_sex != 0 ) {
                n_fail += n_sex;
                msg = String.format( "Thread id %2d; failures n_sex:               %10d", threadId, n_sex );
                System.out.println( msg ); plog.show( msg );
            }

            msg = String.format( "Thread id %2d; total match attempt failures: %10d", threadId, n_fail );
            System.out.println( msg ); plog.show( msg );

            long n_mismatch = n_recs - ( n_fail + n_match );
            if( n_mismatch > 0 ) {
                msg = String.format( "Thread id %2d; missing records: %d ??", threadId, n_mismatch );
                System.out.println( msg ); plog.show( msg );
            }


            msg = String.format( "Thread id %2d; Done: Range %d-of-%d", threadId, (n_qs + 1), qgs.getSize() );
            System.out.println( msg );
            plog.show( msg );

            int nthreads_active = java.lang.Thread.activeCount();
            msg = String.format( "MatchAsync/run(): thread id %2d is done (%d active threads remaining)", threadId, nthreads_active );
            System.out.println( msg );
            plog.show( msg );

            if( free_vecs ) {
                msg = String.format( "Thread id %2d; freeing s1 and s2 vectors", threadId );
                System.out.println( msg ); plog.show( msg );
                ql.freeVectors();
                ql = null;
                qs = null;
            }
            else {
                msg = "not freeing vectors";
                System.out.println( msg ); plog.show( msg );
            }

            dbconPrematch.close();
            dbconMatch.close();

            msg = String.format( "Thread id %2d; clock time", threadId );
            elapsedShowMessage( msg, threadStart, System.currentTimeMillis() );

            long cpuTimeNsec  = threadMXB.getCurrentThreadCpuTime();   // elapsed CPU time for current thread in nanoseconds
            long cpuTimeMsec  = TimeUnit.NANOSECONDS.toMillis( cpuTimeNsec );
            msg = String.format( "Thread id %2d; thread time", threadId );
            elapsedShowMessage( msg, 0, cpuTimeMsec );

            //long userTimeNsec = thx.getCurrentThreadUserTime();  // elapsed user time in nanoseconds
            //long userTimeMsec = TimeUnit.NANOSECONDS.toMillis( userTimeNsec );
            //elapsedShowMessage( "user m time elapsed", 0, userTimeMsec );   // user mode part of thread time
        }
        catch( Exception ex1 )
        {
            //pm.removeProcess();
            pm.release();

            String err = "MatchAsync/run(): thread error: s1_idx_cpy = " + s1_idx_cpy + ", lv_idx_cpy = " + lv_idx_cpy + ", error = " + ex1.getMessage();
            System.out.println( err );
            ex1.printStackTrace();
            try { plog.show( err ); }
            catch( Exception ex2 ) { ex2.printStackTrace(); }
        }
    } // run


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
            msg = String.format( "\ncheck_frequencies() s1_idx: %d", s1_idx );
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

        System.out.println( msg );

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
        }
        catch( Exception ex ) {
            System.out.println( "Exception in getFrequency(): " + ex.getMessage() );
            ex.printStackTrace( new PrintStream( System.out ) );
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
        }
        catch( Exception ex ) {
            System.out.println( "Exception in getFrequency(): " + ex.getMessage() );
            ex.printStackTrace( new PrintStream( System.out ) );
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
     * Query the table Levenshtein table lvs_table for name_int_1 (i.e. an ego_familyname),
       to get the various name_int_2 as levenshtein variants for the set s2.
     *
     * @param name_int_1        // an ego familyname from the set s1
     * @param lvs_table         // ls_ table to use, e.g. ls_familyname
     * @param lvs_dist_max      // max Levenshtein distance
     * @param lvsVariants       // Levenshtein variants of ego familyname
     * @param lvsDistances      // Levenshtein distances of potential matches
     */
    private void getLvsVariants( int name_int_1, String lvs_table, int lvs_dist_max, Vector< Integer > lvsVariants, Vector< Integer > lvsDistances )
    {
        try
        {
            String query = "SELECT * FROM links_prematch." + lvs_table + " WHERE value <= " + lvs_dist_max + " AND name_int_1 = " + name_int_1 ;
            ResultSet rs = dbconPrematch.createStatement().executeQuery( query );

             if( debug ) {
                String msg = String.format( "getLvsVariants(): lvs_dist_max = %d, name_int_1 = %d", lvs_dist_max, name_int_1 );
                System.out.println( msg ); plog.show( msg );
                System.out.println( query ); plog.show( query );
             }

            int nrecs = 0;
            while( rs.next() )
            {
                //int length_1 = rs.getInt( "length_1" );
                //int length_2 = rs.getInt( "length_2" );

                int name_int_2 = rs.getInt( "name_int_2" );
                int lvs_dist   = rs.getInt( "value" );

                if( debug ) {
                    String name_str_1 = rs.getString( "name_str_1" );
                    String name_str_2 = rs.getString( "name_str_2" );

                    if( nrecs == 0 ) {
                        String msg = String.format( "variants for name_str_1 = %s (name_int_1 = %d): ", name_str_1, name_int_1 );
                        System.out.println( msg ); plog.show( msg );
                    }

                    String msg = String.format( "name_str_2: %s ( name_int_2: %d) ", name_str_2, name_int_2 );
                    System.out.println( msg ); plog.show( msg );
                }

                lvsVariants .add( name_int_2 );
                lvsDistances.add( lvs_dist );

                nrecs++;
            }

            if( debug && nrecs != 0 ) {
                String msg = String.format( "getLvsVariants(): # of LvsVariants = %d\n", nrecs );
                System.out.println( msg ); plog.show( msg );
            }
        }
        catch( Exception ex ) {
            System.out.println( "Exception in getLvsVariants: " + ex.getMessage() );
            System.out.println( "Abort" );
            System.exit( 1 );
        }
    } // getLvsVariants


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
                String msg = "compareLSnames(): s1Name = " + s1Name + ", s2Name = " + s2Name;
                System.out.println( msg ); plog.show( msg );
            }

            // the "ORDER BY value" gives us the smallest lvs distances first
            String query = "SELECT * FROM links_prematch." + lvs_table + " WHERE value <= " + lvs_dist_max + " AND name_int_1 = " + s2Name + " ORDER BY value";
            ResultSet rs = dbconPrematch.createStatement().executeQuery( query );

            int nrecs = 0;
            while( rs.next() )
            {
                //int length_1 = rs.getInt( "length_1" );
                //int length_2 = rs.getInt( "length_2" );

                int name_int_1 = rs.getInt( "name_int_1" );
                int name_int_2 = rs.getInt( "name_int_2" );

                int lvs_dist_nrec = rs.getInt( "value" );

                if( debug ) {
                    String name_str_1 = rs.getString( "name_str_1" );
                    String name_str_2 = rs.getString( "name_str_2" );

                    if( nrecs == 0 ) { System.out.printf( lvs_table + " variant for %s (%d): ", name_str_1, name_int_1 ); }
                    System.out.printf( "%s (%d) ", name_str_2, name_int_2 );
                }

                //potentialMatches  .add( name_int_2 );
                //LvPotentialMatches.add( Ldist );

                if( s1Name == name_int_2 ) {
                    lvs_dist = lvs_dist_nrec;
                    if( debug ) { System.out.println( "compareLvsNames(): match" );  }
                    break;
                }

                nrecs++;
            }
            if( debug ) {
                String msg = "rs nrecs: " + nrecs;
                System.out.println( msg ); plog.show( msg );
            }
        }
        catch( Exception ex ) {
            System.out.println( "Exception in compareLvsNames: " + ex.getMessage() );
            System.out.println( "Abort" );
            System.exit( 1 );
        }

        if( debug ) { System.out.println( "compareLvsNames(): lvs_dist = " + lvs_dist );  }
        return lvs_dist;
    } // compareLvsNames


    /**
     * This was the Omar version
     *
     * Loop through the whole set of ego familynames to get all ids with names
     * that are a Levenshtein variant of this name.
     * Notice: exact matches are also included in this list
     *
     * //@param fn                    // an ego familyname from the set s1
     * //@param potentialMatches      // Levenshtein variants of ego familyname
     * //param LvPotentialMatches     // Levenshtein distances of potential matches
     */
    /*
    private void variantsToList( int fn, Vector< Integer > potentialMatches, Vector< Integer > LvPotentialMatches )
    {
        try
        {
            if( debug ) { plog.show( "variantsToList(): ql.s2_ego_familyname.size = " + ql.s2_ego_familyname.size() ); }

            for( int l = 0; l < ql.s2_ego_familyname.size(); l++ )
            {
                if( onlyExactMatch ) {
                    // Use binary search to Check if this name is variant of
                    if( fn == ql.s2_ego_familyname.get( l ) ) {
                        potentialMatches.add( l );          // Add ID of name to list
                        LvPotentialMatches.add( 0 );
                        continue;                           // exact match, so we are done
                    }
                }

                // Do the search in root names or variant names
                if( isUseRoot )     // root names
                {
                    if( fn >= rootFamilyName.length ) { return; }

                    if( ql.s2_ego_familyname.get( l ) >= rootFamilyName.length ) {
                        continue;
                    }

                    int[] root1 = rootFamilyName[ fn ];
                    int[] root2 = rootFamilyName[ ql.s2_ego_familyname.get( l ) ];

                    for( int i = 0; i < root1.length; i++ ) {
                        for( int j = 0; j < root2.length; j++ ) {
                            if( root1[ i ] == root2[ j ] ) {
                                potentialMatches.add( l );
                                LvPotentialMatches.add( -1 );
                                break;
                            }
                        }
                        break;
                    }
                }
                else        // variant names
                {
                    int large;
                    int small;

                    if( fn > ql.s2_ego_familyname.get( l ) ) {
                        large = fn;
                        small = ql.s2_ego_familyname.get( l );
                    }
                    else {
                        large = ql.s2_ego_familyname.get( l );
                        small = fn;
                    }

                    //if( debug ) { plog.show( "small: " + small + ", large: " + fn ); }
                    //if( debug ) { plog.show( "variantFamilyName.length: " + variantFamilyName.length ); }

                    if( variantFamilyName.length == 0 )
                    { continue; }

                    if( variantFamilyName.length > small   &&
                        variantFamilyName[ small ] != null &&
                        Arrays.binarySearch( variantFamilyName[ small ], large ) > -1
                    )
                    {
                        if( debug ) { plog.show( "add: " + l ); }
                        potentialMatches.add( l );
                        LvPotentialMatches.add( -1 );
                    }
                }
            }
        }
        catch( Exception ex ) {
            System.out.println( "Exception in variantsToList: " + ex.getMessage() );
            System.out.println( "Abort" );
            System.exit( 1 );
        }

    } // variantsToList
    */


    /**
     *
     * @param qs
     * @param s1_idx
     * @param s2_idx
     * @return
     *
     * using file global ql: QueryLoader object containing s1 & s2 samples from links_base
     */
    private boolean CheckMinMax( QuerySet qs, int s1_idx, int s2_idx )
    {
        String msg = "";

        if( debug ) { System.out.printf( "s1_idx = %d, s2_idx = %d\n", s1_idx, s2_idx ); }
        if( debug ) { System.out.printf( "use_minmax ego: %3d\n", qs.int_minmax_e ); }

        if( ( qs.int_minmax_e % 2 ) == 1 )
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
            if( qs.int_minmax_e ==  10 ||
                qs.int_minmax_e ==  11 ||
                qs.int_minmax_e == 110 ||
                qs.int_minmax_e == 111 )
            {
                if( debug ) { System.out.printf( "use_minmax ego marriage...\n" ); }

                if( ( firstGreater( ql.s1_ego_marriage_min.get( s1_idx ), ql.s2_ego_marriage_max.get( s2_idx ) ) ) ||
                    ( firstGreater( ql.s2_ego_marriage_min.get( s2_idx ), ql.s1_ego_marriage_max.get( s1_idx ) ) ) )
                { return false; }
            }
        }

        if( qs.int_minmax_e > 99 )
        {
            if( debug ) { System.out.printf( "use_minmax ego death...\n" ); }

            if( ( firstGreater( ql.s1_ego_death_min.get( s1_idx ), ql.s2_ego_death_max.get( s2_idx ) ) ) ||
                ( firstGreater( ql.s2_ego_death_min.get( s2_idx ), ql.s1_ego_death_max.get( s1_idx ) ) ) )
            { return false; }
        }

        if( qs.use_mother )
        {
            if( debug ) { System.out.printf( "use_minmax mother: %3d\n", qs.int_minmax_m ); }

            if( ( qs.int_minmax_m % 2 ) == 1 )
            {
                if( ( firstGreater( ql.s1_mother_birth_min.get( s1_idx ), ql.s2_mother_birth_max.get( s2_idx ) ) ) ||
                    ( firstGreater( ql.s2_mother_birth_min.get( s2_idx ), ql.s1_mother_birth_max.get( s1_idx ) ) ) )
                { return false; }
            }

            if( qs.int_minmax_m ==  10 ||
                qs.int_minmax_m ==  11 ||
                qs.int_minmax_m == 110 ||
                qs.int_minmax_m == 111 )
            {

                if( ( firstGreater( ql.s1_mother_marriage_min.get( s1_idx ), ql.s2_mother_marriage_max.get( s2_idx ) ) ) ||
                    ( firstGreater( ql.s2_mother_marriage_min.get( s2_idx ), ql.s1_mother_marriage_max.get( s1_idx ) ) ) )
                { return false; }
            }

            if( qs.int_minmax_m > 99 )
            {
                if( ( firstGreater( ql.s1_mother_death_min.get( s1_idx ), ql.s2_mother_death_max.get( s2_idx ) ) ) ||
                    ( firstGreater( ql.s2_mother_death_min.get( s2_idx ), ql.s1_mother_death_max.get( s1_idx ) ) ) )
                { return false; }
            }
        }

        if( qs.use_father )
        {
            if( debug ) {
                msg = String.format( "use_minmax father: %3d", qs.int_minmax_f );
                System.out.println( msg );
                try { plog.show( msg ); }
                catch( Exception ex ) { System.out.println( ex.getMessage() ); }
            }

            if( ( qs.int_minmax_f % 2 ) == 1 )
            {
                if( ( firstGreater( ql.s1_father_birth_min.get( s1_idx ), ql.s2_father_birth_max.get( s2_idx ) ) ) ||
                    ( firstGreater( ql.s2_father_birth_min.get( s2_idx ), ql.s1_father_birth_max.get( s1_idx ) ) ) )
                { return false; }
            }

            if( qs.int_minmax_f ==  10 ||
                qs.int_minmax_f ==  11 ||
                qs.int_minmax_f == 110 ||
                qs.int_minmax_f == 111 )
            {
                if( ( firstGreater( ql.s1_father_marriage_min.get( s1_idx ), ql.s2_father_marriage_max.get( s2_idx ) ) ) ||
                    ( firstGreater( ql.s2_father_marriage_min.get( s2_idx ), ql.s1_father_marriage_max.get( s1_idx ) ) ) )
                { return false; }
            }

            if( qs.int_minmax_f > 99) {
                if( ( firstGreater( ql.s1_father_death_min.get( s1_idx ), ql.s2_father_death_max.get( s2_idx ) ) ) ||
                    ( firstGreater( ql.s2_father_death_min.get( s2_idx ), ql.s1_father_death_max.get( s1_idx ) ) ) )
                { return false; }
            }
        }

        if( qs.use_partner )
        {
            if( debug ) {
                msg = String.format( "use_minmax partner: %3d", qs.int_minmax_p );
                System.out.println( msg );
                try { plog.show( msg ); }
                catch( Exception ex ) { System.out.println( ex.getMessage() ); }
            }

            if( ( qs.int_minmax_p % 2 ) == 1 )
            {
                if( ( firstGreater( ql.s1_partner_birth_min.get( s1_idx ), ql.s2_partner_birth_max.get( s2_idx ) ) ) ||
                    ( firstGreater( ql.s2_partner_birth_min.get( s2_idx ), ql.s1_partner_birth_max.get( s1_idx ) ) ) )
                { return false; }
            }

            if( qs.int_minmax_p ==  10 ||
                qs.int_minmax_p ==  11 ||
                qs.int_minmax_p == 110 ||
                qs.int_minmax_p == 111 )
            {
                if( ( firstGreater( ql.s1_partner_marriage_min.get( s1_idx ), ql.s2_partner_marriage_max.get( s2_idx ) ) ) ||
                    ( firstGreater( ql.s2_partner_marriage_min.get( s2_idx ), ql.s1_partner_marriage_max.get( s1_idx ) ) ) )
                { return false; }
            }

            if( qs.int_minmax_p > 99 )
            {
                if( ( firstGreater( ql.s1_partner_death_min.get( s1_idx ), ql.s2_partner_death_max.get( s2_idx ) ) ) ||
                    ( firstGreater( ql.s2_partner_death_min.get( s2_idx ), ql.s1_partner_death_max.get( s1_idx ) ) ) )
                { return false; }
            }
        }

        return true;
    } // CheckMinMax


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
        //int lvs_dist_name1, lvs_dist_name2, lvs_dist_name3, lvs_dist_name4 = -1;
        int lvs_dist = -1;

        if( debug ) {
            System.out.println( "checkFirstName() fn_method: " + fn_method );
            try { plog.show( "checkFirstName() fn_method = " + fn_method ); }
            catch( Exception ex ) { System.out.println( ex.getMessage() ); }
        }

        /*
        if( fn_method == 1 )
        {
            // compare compare firstname1 + firstname2 + firstname3 + firstname4 of s1 & s2
            int lvs_dist_name1, lvs_dist_name2, lvs_dist_name3, lvs_dist_name4 = -1;

            lvs_dist_name1 = isVariant( s1Name1, s2Name1, lvs_table, lvs_dist_max, NameType.FIRSTNAME, method );
            lvs_dist_name2 = isVariant( s1Name2, s2Name2, lvs_table, lvs_dist_max, NameType.FIRSTNAME, method );
            lvs_dist_name3 = isVariant( s1Name3, s2Name3, lvs_table, lvs_dist_max, NameType.FIRSTNAME, method );
            lvs_dist_name4 = isVariant( s1Name4, s2Name4, lvs_table, lvs_dist_max, NameType.FIRSTNAME, method );

            if( lvs_dist_name1 == -1 || lvs_dist_name2 == -1 || lvs_dist_name3 == -1|| lvs_dist_name4 == -1 ) { retval = -1; }  // no match
            else { retval =  lvs_dist_name1 + lvs_dist_name2 + lvs_dist_name3 + lvs_dist_name4; }
        }
        */

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
                else { lvs_dist = lvs_dist_name1 + lvs_dist_name2; }
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

