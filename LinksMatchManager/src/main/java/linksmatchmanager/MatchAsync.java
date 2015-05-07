package linksmatchmanager;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;

import java.sql.Connection;
import java.sql.ResultSet;

import java.util.concurrent.TimeUnit;
import java.util.Vector;

import linksmatchmanager.DataSet.InputSet;
import linksmatchmanager.DataSet.NameType;
import linksmatchmanager.DataSet.QueryGroupSet;
import linksmatchmanager.DataSet.QuerySet;


/**
 * @author Omar Azouguagh
 * @author Fons Laan
 *
 * FL-01-May-2015 Latest change
 *
 * "Vectors are synchronized. Any method that touches the Vector's contents is thread safe. ArrayList,
 * on the other hand, is unsynchronized, making them, therefore, not thread safe."
 *  -> So in MatchAsync we should use Vectors
 */

public class MatchAsync extends Thread
{
    boolean debug;
    boolean free_vecs;

    ProcessManager pm;

    int n_mp;       // match_process 'y' records: 0...
    int n_qs;       // query sets: 0...

    QueryLoader ql;
    PrintLogger plog;

    QueryGroupSet qgs;
    InputSet inputSet;

    int s1_offset;      // which record to start from sample 1
    int s1_piece;       // how many records from sample 1

    Connection dbconPrematch;
    Connection dbconMatch;
    Connection dbconTemp;

    String lvs_table_firstname;
    String lvs_table_familyname;

    int[][] variantFirstName;
    int[][] variantFamilyName;
    int[][] rootFirstName;
    int[][] rootFamilyName;

    boolean isUseRoot = false;      // false for variant


    public MatchAsync
    (
        boolean debug,
        boolean free_vecs,

        ProcessManager pm,

        int n_mp,
        int n_qs,

        QueryLoader ql,
        PrintLogger plog,

        QueryGroupSet qgs,
        InputSet inputSet,

        int s1_offset,
        int s1_piece,

        Connection dbconPrematch,
        Connection dbconMatch,
        Connection dbconTemp,

        String lvs_table_firstname,
        String lvs_table_familyname,

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
        this.s1_piece = s1_piece;

        this.dbconPrematch = dbconPrematch;
        this.dbconMatch    = dbconMatch;
        this.dbconTemp     = dbconTemp;

        this.lvs_table_firstname  = lvs_table_firstname;
        this.lvs_table_familyname = lvs_table_familyname;

        this.variantFirstName  = variantFirstName;
        this.variantFamilyName = variantFamilyName;

        System.out.println( "MatchAsync: using variant names (instead of root names)" );
    }


    public MatchAsync   // variant has no root parameter
    (
        boolean debug,
        boolean free_vecs,

        ProcessManager pm,

        int n_mp,
        int n_qs,

        QueryLoader ql,
        PrintLogger plog,

        QueryGroupSet qgs,
        InputSet inputSet,

        int s1_offset,
        int s1_piece,

        Connection dbconPrematch,
        Connection dbconMatch,
        Connection dbconTemp,

        String lvs_table_firstname,
        String lvs_table_familyname,

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

        this.dbconPrematch = dbconPrematch;
        this.dbconMatch    = dbconMatch;
        this.dbconTemp     = dbconTemp;

        this.lvs_table_firstname  = lvs_table_firstname;
        this.lvs_table_familyname = lvs_table_familyname;

        this.rootFirstName  = rootFirstName;
        this.rootFamilyName = rootFamilyName;

        this.isUseRoot = true;      // true for root

        System.out.println( "MatchAsync: using root names (instead of variant names)" );
    }

    @Override
    public void run()
    {
        // If you want the actual CPU time of the current thread (or indeed, any arbitrary thread) rather than
        // the wall clock time then you can get this via ThreadMXBean. Basically, do this at the start:
        ThreadMXBean threadMXB = ManagementFactory.getThreadMXBean();
        threadMXB.setThreadCpuTimeEnabled( true );

        boolean debugfail = false;

        // in order to show the indexes when an exception occurs, we define copies outside the try/catch
        int s1_idx_cpy = 0;
        int lv_idx_cpy = 0;

        // count why the matches fail
        long n_sex    = 0;
        long n_minmax = 0;

        long n_int_firstname_e = 0;
        long n_int_firstname_m = 0;
        long n_int_firstname_f = 0;
        long n_int_firstname_p = 0;

        long n_int_familyname_e = 0;
        long n_int_familyname_m = 0;
        long n_int_familyname_f = 0;
        long n_int_familyname_p = 0;


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

            // Get a QuerySet object. This object will contains all data about a certain query/subquery
            QuerySet qs = qgs.get( n_qs );

            // Levenshtein distances to use to get the variant names
            int lvs_dist_firstname  = qs.prematch_firstname_value;
            int lvs_dist_familyname = qs.prematch_familyname_value;

            msg = String.format( "Thread id %2d; use firstname  levenshtein distance for variants: %d", threadId, lvs_dist_firstname );
            System.out.println( msg ); plog.show( msg );
            msg = String.format( "Thread id %2d; use familyname levenshtein distance for variants: %d", threadId, lvs_dist_familyname );
            System.out.println( msg ); plog.show( msg );


            long threadId_current = Thread.currentThread().getId();

            msg = String.format( "Thread id %2d, based on s1 query:\n%s", threadId_current, qs.query1 );
            System.out.println( msg ); plog.show( msg );

            msg = String.format( "Thread id %2d, based on s2 query:\n%s", threadId_current, qs.query2 );
            System.out.println( msg ); plog.show( msg );

            // Create new instance of queryloader. Queryloader is used to use the queries to load data into the sets.
            // Its input is a QuerySet and a database connection object.
            // do this now in main()
            //ql = new QueryLoader( threadId, qs, dbconPrematch );

            // Previous familyname, initial is 0. Because the familynames are ordered, the calculation of the potential
            // matches is done once, only the first time.
            int previousEgoFamilyName = 0;

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

            // individual threads get a portion of s1
            //for( int s1_idx = 0; s1_idx < s1_size; s1_idx++ )
            for( int s1_idx = s1_offset; s1_idx < (s1_offset + s1_piece); s1_idx++ )
            {
                n_recs ++;

                s1_idx_cpy = s1_idx;   // copy for display if exception occurs

                if( s1_chunk != 0 && ( ( s1_idx + s1_chunk ) % s1_chunk == 0 ) )
                { System.out.println( String.format( "Thread id %2d; records processed: %d-of-%d, matches found: %d", threadId , s1_idx, s1_size, n_match ) ); }
                //{ System.out.println( "Thread id " + threadId + "; records processed: " + s1_idx + ", matches found: " + n_match ); }

                //if( s1_idx > s1_offset ) { continue; }                    // DUMMY RUN
                //if( s1_idx > (s1_offset + 10) ) { System.exit( 0 ); }         // DUMMY RUN

                if( debug ) {
                    msg = String.format( "\ns1 idx: %d-of-%d", s1_idx + 1, s1_size );
                    System.out.println( msg );
                    plog.show( msg );
                }

                // Get familyname of Set 1
                int    s1EgoFamName    = ql.s1_ego_familyname    .get( s1_idx );
                String s1EgoFamNameStr = ql.s1_ego_familyname_str.get( s1_idx );
                String s1EgoFirNameStr = ql.s1_ego_firstname1_str.get( s1_idx );
                if( debug ) { System.out.printf( "s1 ego familyname: %s,  s1 ego firstname1: %s\n", s1EgoFamNameStr, s1EgoFirNameStr ); }

                // If the s1 ego familyname changes, create a new variant names list, otherwise go on
                // to check the other s1 entries with the same ego familyname against this set.
                if( s1EgoFamName != previousEgoFamilyName )
                {
                    //System.out.printf("s1 ego familyname: %s ", s1EgoFamNameStr);
                    previousEgoFamilyName = s1EgoFamName;           // Set previous name

                    // Get the variants of name s1EgoFamName; these are names (as ints) from the ls_ table
                    lvsVariants .clear();                           // Empty the lists
                    lvsDistances.clear();

                    // fill the lvsVariants and lvsDistances lists for variants of s1EgoFamName
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

                        int offset = 0;
                        // Find all occurrences of s2EgoFamName in s2
                        while( true )
                        {
                            int s2_idx = ql.s2_ego_familyname.indexOf( s2EgoFamName, offset );      // index in s2 set

                            if( s2_idx == -1 ) { break; }       // no more occurences

                            else                                // we got one
                            {
                                if( debug ) { System.out.println( "s1EgoFamName found in s2: " + s1EgoFamName ); }
                                //String s2_ego_familyname_str = ql.s2_ego_familyname_str.get( s2_idx );
                                //System.out.println( " " + s2_ego_familyname_str);

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

                                // offset for next try
                                offset = s2_idx + 1;            // for next search
                                if( offset >= ql.s2_ego_familyname.size() )
                                { break; }                      // no more occurences
                            }
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
                    System.out.println( msg );
                    plog.show( msg );
                }

                // TODO: improve performance: process from lowest to highest frequency

                // familyname
                //  s1EgoFamName -> from s1_idx above
                int s1MotherFamName  = ql.s1_mother_familyname .get( s1_idx );
                int s1FatherFamName  = ql.s1_father_familyname .get( s1_idx );
                int s1PartnerFamName = ql.s1_partner_familyname.get( s1_idx );

                int freqEgo     = getFrequency( s1EgoFamName );
                int freqMother  = getFrequency( s1MotherFamName );
                int freqFather  = getFrequency( s1FatherFamName );
                int freqPartner = getFrequency( s1PartnerFamName );

                if( debug ) {
                    // show only the used familynames
                    msg = String.format( "Familyname frequencies: Ego: %d", freqEgo );
                    if( qs.use_mother  && qs.int_familyname_m > 0 ) { msg += String.format( ", Mother: %d",  freqMother ); }
                    if( qs.use_father  && qs.int_familyname_f > 0 ) { msg += String.format( ", Father: %d",  freqFather ); }
                    if( qs.use_partner && qs.int_familyname_p > 0 ) { msg += String.format( ", Partner: %d", freqPartner ); }
                    System.out.println( msg );
                }

                // firstname 1
                int s1EgoFirName1     = ql.s1_ego_firstname1    .get( s1_idx );
                int s1MotherFirName1  = ql.s1_mother_firstname1 .get( s1_idx );
                int s1FatherFirName1  = ql.s1_father_firstname1 .get( s1_idx );
                int s1PartnerFirName1 = ql.s1_partner_firstname1.get( s1_idx );

                // firstname 2
                int s1EgoFirName2     = ql.s1_ego_firstname2    .get( s1_idx );
                int s1MotherFirName2  = ql.s1_mother_firstname2 .get( s1_idx );
                int s1FatherFirName2  = ql.s1_father_firstname2 .get( s1_idx );
                int s1PartnerFirName2 = ql.s1_partner_firstname2.get( s1_idx );

                // firstname 3
                int s1EgoFirName3     = ql.s1_ego_firstname3    .get( s1_idx );
                int s1MotherFirName3  = ql.s1_mother_firstname3 .get( s1_idx );
                int s1FatherFirName3  = ql.s1_father_firstname3 .get( s1_idx );
                int s1PartnerFirName3 = ql.s1_partner_firstname3.get( s1_idx );

                // firstname 4
                int s1EgoFirName4     = ql.s1_ego_firstname4    .get( s1_idx );
                int s1MotherFirName4  = ql.s1_mother_firstname4 .get( s1_idx );
                int s1FatherFirName4  = ql.s1_father_firstname4 .get( s1_idx );
                int s1PartnerFirName4 = ql.s1_partner_firstname4.get( s1_idx );

                /*
                if( s1MotherFirName1 == 22823 ) {
                    System.out.println( "null s1MotherFirName1: 22823" );
                    plog.show( "null s1MotherFirName1: 22823" );
                }
                */

                // loop through the familynames of s2; exact + variants
                for( int lv_idx = 0; lv_idx < s2_idx_variants_cpy.size(); lv_idx++ )
                {
                    lv_idx_cpy = lv_idx;        // copy for display if exception occurs

                    int s2_idx = s2_idx_variants_ego.get( lv_idx );

                    if( debug ) {
                        String s2_ego_familyname_str = ql.s2_ego_familyname_str.get( s2_idx );
                        msg = String.format( "s2 idx: %d %s\n", s2_idx, s2_ego_familyname_str );
                        System.out.println( msg );
                        plog.show( msg );
                    }

                    // Check min max; use new min max
                    if( ! qs.ignore_minmax ) {
                        if( ! CheckMinMax( qs, s1_idx, s2_idx ) ) {
                            s2_idx_variants_cpy.set( lv_idx, -1 );           // no match
                            n_minmax++;
                            if( debugfail ) { System.out.println( "failed ignore_minmax" ); }
                            continue;
                        }
                    }

                    // Check sex
                    if( ! qs.ignore_sex ) {
                        int s1s = ql.s1_sex.get( s1_idx );
                        int s2s = ql.s2_sex.get( s2_idx );

                        // Empty sex is denied
                        if( s1s != 0 && s2s != 0 && ( s1s != s2s ) ) {
                            s2_idx_variants_cpy.set( lv_idx, -1 );           // no match
                            n_sex++;
                            if( debugfail ) { System.out.println( "failed ignore_sex" ); }
                            continue;
                        }
                    }

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

                    // Check the firstnames of ego
                    if( qs.int_firstname_e > 0 ) {
                        int lv_dist = checkFirstName( qs.firstname,
                            s1EgoFirName1, s1EgoFirName2, s1EgoFirName3, s1EgoFirName4,
                            s2EgoFirName1, s2EgoFirName2, s2EgoFirName3, s2EgoFirName4,
                            qs.method, lvs_table_firstname, lvs_dist_firstname );
                        if( debugfail ) { System.out.println( "int_firstname_e: lv_dist = " + lv_dist ); }

                        if( lv_dist >= 0 )  // match
                        {
                            if( debugfail ) { System.out.println( "match int_firstname_e" ); }
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

                    if( qs.use_mother ) {
                        if( qs.int_familyname_m > 0 ) {
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

                        if( qs.int_firstname_m > 0 ) {
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
                    }

                    if( qs.use_father ) {
                        if( qs.int_familyname_f > 0 ) {
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

                        if( qs.int_firstname_f > 0 ) {
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
                    }

                    if( qs.use_partner ) {
                        if( qs.int_familyname_p > 0 ) {
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

                        if( qs.int_firstname_p > 0 ) {
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
                    }
                }
                //System.out.println( "variants done" );

                // entries of matchesList that have not been zeroed above imply a match
                for( int l = 0; l < s2_idx_variants_cpy.size(); l++ )
                {
                    int lv_idx = s2_idx_variants_cpy.get( l );
                    if( lv_idx == -1 )        // this variant ego familyname had no match
                    { n_int_familyname_e++; }
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
                            System.out.println( query );
                            plog.show( query );
                        }

                        dbconMatch.createStatement().execute( query );
                        dbconMatch.createStatement().close();
                    }
                }
                s2_idx_variants_cpy = null;
                System.out.flush();
                System.err.flush();
            }

            pm.removeProcess();

            msg = String.format( "Thread id %2d; s1 records processed: %d", threadId, n_recs );
            System.out.println( msg ); plog.show( msg );

            msg = String.format( "Thread id %2d; Number of matches: %d", threadId, n_match );
            System.out.println( msg ); plog.show( msg );

            long n_fail = 0;

            if( n_minmax != 0 ) {
                n_fail += n_minmax;
                msg = String.format( "Thread id %2d; failures n_minmax: %d", threadId, n_minmax );
                System.out.println( msg ); plog.show( msg );
            }

            if( n_sex != 0 ) {
                n_fail += n_sex;
                msg = String.format( "Thread id %2d; failures n_sex: %d", threadId, n_minmax );
                System.out.println( msg ); plog.show( msg );
            }

            if( n_int_familyname_e != 0 ) {
                n_fail += n_int_familyname_e;
                msg = String.format( "Thread id %2d; failures n_int_familyname_e: %d", threadId, n_int_familyname_e );
                System.out.println( msg ); plog.show( msg );
            }

            if( n_int_firstname_e != 0 ) {
                n_fail += n_int_firstname_e;
                msg = String.format( "Thread id %2d; failures n_int_firstname_e: %d", threadId, n_int_firstname_e );
                System.out.println( msg ); plog.show( msg );
            }

            if( n_int_familyname_m != 0 ) {
                n_fail += n_int_familyname_m;
                msg = String.format( "Thread id %2d; failures n_int_familyname_m: %d", threadId, n_int_familyname_m );
                System.out.println( msg ); plog.show( msg );
            }

            if( n_int_firstname_m != 0 ) {
                n_fail += n_int_firstname_m;
                msg = String.format( "Thread id %2d; failures n_int_firstname_m: %d", threadId, n_int_firstname_m );
                System.out.println( msg ); plog.show( msg );
            }

            if( n_int_familyname_f != 0 ) {
                n_fail += n_int_familyname_f;
                msg = String.format( "Thread id %2d; failures n_int_familyname_f: %d", threadId, n_int_familyname_f );
                System.out.println( msg ); plog.show( msg );
            }

            if( n_int_firstname_f != 0 ) {
                n_fail += n_int_firstname_f;
                msg = String.format( "Thread id %2d; failures n_int_firstname_f: %d", threadId, n_int_firstname_f );
                System.out.println( msg ); plog.show( msg );
            }

            if( n_int_familyname_p != 0 ) {
                n_fail += n_int_familyname_p;
                msg = String.format( "Thread id %2d; failures n_int_familyname_p: %d", threadId, n_int_familyname_p );
                System.out.println( msg ); plog.show( msg );
            }

            if( n_int_firstname_p != 0 ) {
                n_fail += n_int_firstname_p;
                msg = String.format( "Thread id %2d; failures n_int_firstname_p: %d", threadId, n_int_firstname_p );
                System.out.println( msg ); plog.show( msg );
            }

            msg = String.format( "Thread id %2d; total match attempt failures: %d", threadId, n_fail );
            System.out.println( msg ); plog.show( msg );

            long n_mismatch = n_recs - ( n_fail + n_match );
            if( n_mismatch > 0 ) {
                msg = String.format( "Thread id %2d; missing records: %d ??", threadId, n_mismatch );
                System.out.println( msg ); plog.show( msg );
            }


            msg = String.format( "Thread id %2d; Done: Range %d of %d", threadId, (n_qs + 1), qgs.getSize() );
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

            elapsedShowMessage( "clock time", threadStart, System.currentTimeMillis() );

            long cpuTimeNsec  = threadMXB.getCurrentThreadCpuTime();   // elapsed CPU time for current thread in nanoseconds
            long cpuTimeMsec  = TimeUnit.NANOSECONDS.toMillis( cpuTimeNsec );
            elapsedShowMessage( "thread time", 0, cpuTimeMsec );

            //long userTimeNsec = thx.getCurrentThreadUserTime();  // elapsed user time in nanoseconds
            //long userTimeMsec = TimeUnit.NANOSECONDS.toMillis( userTimeNsec );
            //elapsedShowMessage( "user m time elapsed", 0, userTimeMsec );   // user mode part of thread time
        }
        catch( Exception ex1 )
        {
            pm.removeProcess();

            String err = "MatchAsync/run(): thread error: s1_idx_cpy = " + s1_idx_cpy + ", lv_idx_cpy = " + lv_idx_cpy + ", error = " + ex1.getMessage();
            System.out.println( err );
            ex1.printStackTrace();
            try { plog.show( err ); }
            catch( Exception ex2 ) { ex2.printStackTrace(); }
        }
    } // run


    /**
     *
     * @param familyname_int
     * @return
     */
    public int getFrequency( int familyname_int )
    {
        int freq = 0;

        try
        {
            String query = "SELECT * FROM links_prematch.freq_familyname_mem WHERE id= " + familyname_int + ";";
            ResultSet rs = dbconPrematch.createStatement().executeQuery( query );

            String name = "";
            while( rs.next() ) {
                freq = rs.getInt( "frequency" );

                /*
                name = rs.getString( "name_str" );
                String msg = String.format( "getFrequency() name: %s, freq: %d", name, freq );
                System.out.println( msg );
                */
            }
        }
        catch( Exception ex ) {
            System.out.println( "Exception in getLvsVariants: " + ex.getMessage() );
            System.out.println( "Abort" );
            System.exit( 1 );
        }

        return freq;
    }


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
    }


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
    }


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
            if( debug ) { plog.show( "variantsToList(): name_int_1 = " + name_int_1 ); }

            String query = "SELECT * FROM links_prematch." + lvs_table + " WHERE value <= " + lvs_dist_max + " AND name_int_1 = " + name_int_1 ;
            ResultSet rs = dbconPrematch.createStatement().executeQuery( query );

            int nrecs = 0;
            while( rs.next() )
            {
                //int length_1 = rs.getInt( "length_1" );
                //int length_2 = rs.getInt( "length_2" );

                int name_int_2 = rs.getInt( "name_int_2" );

                int lvs_dist = rs.getInt( "value" );

                if( debug ) {
                    String name_str_1 = rs.getString( "name_str_1" );
                    String name_str_2 = rs.getString( "name_str_2" );

                    if( nrecs == 0 ) { System.out.printf( "variants for %s (%d): ", name_str_1, name_int_1 ); }
                    System.out.printf( "%s (%d) ", name_str_2, name_int_2 );
                }

                lvsVariants .add( name_int_2 );
                lvsDistances.add( lvs_dist );

                nrecs++;
            }
            if( debug && nrecs != 0 ) { System.out.println( "" ); }
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

    private boolean CheckMinMax( QuerySet qs, int k, int index )
    {
        if( debug )
        {
            String msg = "CheckMinMax() k = " + k + ", index = " + index;
            System.out.println( msg );
            try { plog.show( msg ); }
            catch( Exception ex ) { System.out.println( ex.getMessage() ); }
        }

        if( (qs.int_minmax_e % 2 ) == 1 )
        {
            if( ( firstGreater( ql.s1_ego_birth_min.get( k ),     ql.s2_ego_birth_max.get( index ) ) ) ||
                ( firstGreater( ql.s2_ego_birth_min.get( index ), ql.s1_ego_birth_max.get( k ) ) ) )
            { return false; }
        }

        if( qs.int_minmax_e ==  10 ||
            qs.int_minmax_e ==  11 ||
            qs.int_minmax_e == 110 ||
            qs.int_minmax_e == 111 )
        {
            if( ( firstGreater( ql.s1_ego_marriage_min.get( k ),     ql.s2_ego_marriage_max.get( index ) ) ) ||
                ( firstGreater( ql.s2_ego_marriage_min.get( index ), ql.s1_ego_marriage_max.get( k ) ) ) )
            { return false; }
        }

        if( qs.int_minmax_e > 99 )
        {
            if( ( firstGreater( ql.s1_ego_death_min.get( k ),     ql.s2_ego_death_max.get( index ) ) ) ||
                ( firstGreater( ql.s2_ego_death_min.get( index ), ql.s1_ego_death_max.get( k ) ) ) )
            { return false; }
        }

        if( qs.use_mother )
        {
            if( ( qs.int_minmax_m % 2 ) == 1 )
            {
                if( ( firstGreater( ql.s1_mother_birth_min.get( k ),     ql.s2_mother_birth_max.get( index ) ) ) ||
                    ( firstGreater( ql.s2_mother_birth_min.get( index ), ql.s1_mother_birth_max.get( k ) ) ) )
                { return false; }
            }

            if( qs.int_minmax_m ==  10 ||
                qs.int_minmax_m ==  11 ||
                qs.int_minmax_m == 110 ||
                qs.int_minmax_m == 111 )
            {
                if( ( firstGreater( ql.s1_mother_marriage_min.get( k ),     ql.s2_mother_marriage_max.get( index ) ) ) ||
                    ( firstGreater( ql.s2_mother_marriage_min.get( index ), ql.s1_mother_marriage_max.get( k ) ) ) )
                { return false; }
            }

            if( qs.int_minmax_m > 99 )
            {
                if( ( firstGreater( ql.s1_mother_death_min.get( k ),     ql.s2_mother_death_max.get( index ) ) ) ||
                    ( firstGreater( ql.s2_mother_death_min.get( index ), ql.s1_mother_death_max.get( k ) ) ) )
                { return false; }
            }
        }

        if( qs.use_father )
        {
            if( ( qs.int_minmax_f % 2 ) == 1 )
            {
                if( ( firstGreater( ql.s1_father_birth_min.get( k ),     ql.s2_father_birth_max.get( index ) ) ) ||
                    ( firstGreater( ql.s2_father_birth_min.get( index ), ql.s1_father_birth_max.get( k ) ) ) )
                { return false; }
            }

            if( qs.int_minmax_f ==  10 ||
                qs.int_minmax_f ==  11 ||
                qs.int_minmax_f == 110 ||
                qs.int_minmax_f == 111 )
            {
                if( ( firstGreater( ql.s1_father_marriage_min.get( k ),     ql.s2_father_marriage_max.get( index ) ) ) ||
                    ( firstGreater( ql.s2_father_marriage_min.get( index ), ql.s1_father_marriage_max.get( k ) ) ) )
                { return false; }
            }

            if( qs.int_minmax_f > 99) {
                if( ( firstGreater( ql.s1_father_death_min.get( k ),     ql.s2_father_death_max.get( index ) ) ) ||
                    ( firstGreater( ql.s2_father_death_min.get( index ), ql.s1_father_death_max.get( k ) ) ) )
                { return false; }
            }
        }

        if( qs.use_partner )
        {
            if( ( qs.int_minmax_p % 2 ) == 1 )
            {
                if( ( firstGreater( ql.s1_partner_birth_min.get( k ), ql.s2_partner_birth_max.get( index ) ) ) ||
                    ( firstGreater( ql.s2_partner_birth_min.get( index ), ql.s1_partner_birth_max.get( k ) ) ) )
                { return false; }
            }

            if( qs.int_minmax_p ==  10 ||
                qs.int_minmax_p ==  11 ||
                qs.int_minmax_p == 110 ||
                qs.int_minmax_p == 111 )
            {
                if( ( firstGreater( ql.s1_partner_marriage_min.get( k ),     ql.s2_partner_marriage_max.get( index ) ) ) ||
                    ( firstGreater( ql.s2_partner_marriage_min.get( index ), ql.s1_partner_marriage_max.get( k ) ) ) )
                { return false; }
            }

            if( qs.int_minmax_p > 99 )
            {
                if( ( firstGreater( ql.s1_partner_death_min.get( k ),     ql.s2_partner_death_max.get( index ) ) ) ||
                    ( firstGreater( ql.s2_partner_death_min.get( index ), ql.s1_partner_death_max.get( k ) ) ) )
                { return false; }
            }
        }

        return true;
    } // CheckMinMax


    // private static boolean firstGreater( int first, int second )
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

