package linksmatchmanager;

import java.sql.Connection;
import java.sql.ResultSet;

//import java.util.ArrayList;
import java.util.Vector;

import linksmatchmanager.DataSet.QuerySet;
import linksmatchmanager.DataSet.NameType;
import linksmatchmanager.DataSet.QueryGroupSet;

/**
 * @author Omar Azouguagh
 * @author Fons Laan
 *
 * FL-26-Feb-2015 Latest change
 *
 * "Vectors are synchronized. Any method that touches the Vector's contents is thread safe. ArrayList,
 * on the other hand, is unsynchronized, making them, therefore, not thread safe."
 *  -> So in MatchAsync we should use Vectors
 */

public class MatchAsync extends Thread
{
    boolean debug;

    ProcessManager pm;

    int i;
    int j;

    QueryLoader ql;
    PrintLogger plog;

    QueryGroupSet qgs;
    QueryGenerator mis;

    Connection dbconPrematch;
    Connection dbconMatch;
    Connection dbconTemp;

    int[][] variantFirstName;
    int[][] variantFamilyName;
    int[][] rootFirstName;
    int[][] rootFamilyName;

    boolean isUseRoot = false;      // false for variant


    public MatchAsync
    (
        boolean debug,

        ProcessManager pm,

        int i,
        int j,

        QueryLoader ql,
        PrintLogger plog,

        QueryGroupSet qgs,
        QueryGenerator mis,

        Connection dbconPrematch,
        Connection dbconMatch,
        Connection dbconTemp,

        int[][] variantFirstName,
        int[][] variantFamilyName
    )
    {
        this.debug = debug;

        this.pm = pm;

        this.i = i;
        this.j = j;

        this.ql = ql;
        this.plog = plog;

        this.qgs = qgs;
        this.mis = mis;

        this.dbconPrematch = dbconPrematch;
        this.dbconMatch    = dbconMatch;
        this.dbconTemp     = dbconTemp;

        this.variantFirstName  = variantFirstName;
        this.variantFamilyName = variantFamilyName;

        System.out.println( "MatchAsync: using variant names (instead of root names)" );
    }


    public MatchAsync   // variant has no root parameter
    (
        boolean debug,

        ProcessManager pm,

        int i,
        int j,

        QueryLoader ql,
        PrintLogger plog,

        QueryGroupSet qgs,
        QueryGenerator mis,

        Connection dbconPrematch,
        Connection dbconMatch,
        Connection dbconTemp,

        int[][] rootFirstName,
        int[][] rootFamilyName,

        boolean root
    )
    {
        this.debug = debug;

        this.pm = pm;

        this.i = i;
        this.j = j;

        this.ql = ql;
        this.plog = plog;

        this.qgs = qgs;
        this.mis = mis;

        this.dbconPrematch = dbconPrematch;
        this.dbconMatch    = dbconMatch;
        this.dbconTemp     = dbconTemp;

        this.rootFirstName  = rootFirstName;
        this.rootFamilyName = rootFamilyName;

        this.isUseRoot = true;      // true for root

        System.out.println( "MatchAsync: using root names (instead of variant names)" );
    }

    @Override
    public void run()
    {
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
            long threadId = Thread.currentThread().getId();
            String msg = String.format( "\nMatchAsync/run(): thread id %d running", threadId );
            System.out.println( msg );
            plog.show( msg );

            msg = String.format( "Thread id %d; Range %d of %d", threadId, (j + 1), qgs.getSize() );
            System.out.println( msg );
            plog.show( msg );

            System.out.println( "process id: " + qgs.get( 0 ).id );

            // Get a QuerySet object. This object will contains all data about a certain query/subquery
            QuerySet qs = qgs.get( j );

            msg = "s1 query:\n" + qs.query1;
            System.out.println( msg );
            plog.show( msg );
            msg = "s2 query:\n" + qs.query2;
            System.out.println( msg );
            plog.show( msg );

            String lvs_table_familyname = qgs.get( 0 ).prematch_familyname;
            String lvs_table_firstname  = qgs.get( 0 ).prematch_firstname;

            System.out.println( "lvs familyname table: " + lvs_table_familyname );
            System.out.println( "lvs firstname  table: " + lvs_table_firstname );

            int lvs_dist_familyname = qgs.get( 0 ).prematch_familyname_value;
            int lvs_dist_firstname  = qgs.get( 0 ).prematch_firstname_value;

            System.out.println( "lvs familyname dist: " + lvs_dist_familyname );
            System.out.println( "lvs firstname  dist: " + lvs_dist_firstname );

            // Create memory tables to hold the ls_* tables
            // need much more heap for ls_ tables
            long max_heap_table_size = ( 512 + 64 + 32 ) * 1024 * 1024;    // default is 16 GB: 16 * 1024 * 1024
            System.out.println( "max_heap_table_size: " + max_heap_table_size );
            String table_firstname_src  = "ls_firstname";
            String table_familyname_src = "ls_familyname";
            String name_postfix = "_mem";
            memtables_create( threadId, max_heap_table_size, table_firstname_src, table_familyname_src, name_postfix );

            // and now change the names to the actual table names used !
            lvs_table_familyname += name_postfix;
            lvs_table_firstname  += name_postfix;


            // Create new instance of queryloader. Queryloader is used to use the queries to load data into the sets.
            // Its input is a QuerySet and a database connection object.
            ql = new QueryLoader( threadId, qs, dbconPrematch );

            // Previous familyname, initial is 0. Because the familynames are ordered, the calculation of the potential
            // matches is done once, only the first time.
            int previousEgoFamilyName = 0;

            // variant names for s1 ego familyname from ls_ table, plus lvs distance
            Vector< Integer > lvsVariants  = new Vector< Integer >();
            Vector< Integer > lvsDistances = new Vector< Integer >();

            // all occurrences in the s2 set of the above variants
            Vector< Integer > s2_idx_variants       = new Vector< Integer >();
            Vector< Integer > s2_idx_firstname_lvs  = new Vector< Integer >();
            Vector< Integer > s2_idx_familyname_lvs = new Vector< Integer >();

            // Loop through set 1
            msg = String.format( "Thread id %d; Set 1 size: %d from links_base", threadId, ql.s1_id_base.size() );
            System.out.println( msg );
            plog.show( msg );

            long n_recs   = 0;
            long n_match  = 0;

            int s1_size  = ql.s1_id_base.size();
            int s1_chunk = s1_size / 20;

            for( int s1_idx = 0; s1_idx < s1_size; s1_idx++ )
            {
                //int ids1 = ql.s1_id_base.get( s1_idx );
                //if( ids1 != 88685 ) { continue; }

                /*
                if( n_recs > 10 ) {
                    System.out.println( "EXIT" );
                    break;
                }
                */

                n_recs ++;

                s1_idx_cpy = s1_idx;   // copy for display if exception occurs

                if( ( s1_idx + s1_chunk ) % s1_chunk == 0 )
                { System.out.println( "Thread id " + threadId + "; records processed: " + s1_idx + ", matches found: " + n_match ); }

                if( debug ) {
                    msg = String.format( "\ns1 idx: %d-of-%d", s1_idx + 1, s1_size );
                    System.out.println( msg );
                    plog.show( msg );
                }

                // Get familyname of Set 1
                int s1EgoFamName = ql.s1_ego_familyname    .get(s1_idx);

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

                    s2_idx_variants      .clear();                  // Empty the lists
                    s2_idx_firstname_lvs .clear();
                    s2_idx_familyname_lvs.clear();

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

                                s2_idx_variants      .add( s2_idx );
                                s2_idx_familyname_lvs.add( lvsDistances.get( lv_idx ) );    // Lvs distance from ls_ table
                                s2_idx_firstname_lvs .add( -1 );        // do not yet know the value, but must extend the array

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
                Vector< Integer > s2_idx_variants_cpy = new Vector( s2_idx_variants );

                if( debug  && s2_idx_variants.size() > 0 ) {
                    for( int lv_idx = 0; lv_idx < s2_idx_variants.size(); lv_idx++ ) {
                        int s2_idx = s2_idx_variants.get( lv_idx );
                        System.out.println( "s2_idx: " + s2_idx );
                    }
                    System.out.println( "" );

                    msg = String.format( "Thread id %d; potential matches: %d", threadId, s2_idx_variants.size() );
                    System.out.println( msg );
                    plog.show( msg );
                }

                // TODO: improve performance: process from lowest to highest frequency

                // familyname
                //  s1EgoFamName -> from s1_idx above
                int s1MotherFamName  = ql.s1_mother_familyname .get( s1_idx );
                int s1FatherFamName  = ql.s1_father_familyname .get( s1_idx );
                int s1PartnerFamName = ql.s1_partner_familyname.get( s1_idx );

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

                // loop through the familynames of s2; exact + variants
                for( int lv_idx = 0; lv_idx < s2_idx_variants_cpy.size(); lv_idx++ )
                {
                    lv_idx_cpy = lv_idx;        // copy for display if exception occurs

                    int s2_idx = s2_idx_variants.get( lv_idx );

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
                            s2_idx_firstname_lvs.set( lv_idx, lv_dist );         // for match table
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
                            if( -1 == isVariant( s1MotherFamName, s2MotherFamName, lvs_table_familyname, lvs_dist_familyname, NameType.FAMILYNAME, qs.method ) )
                            {
                                s2_idx_variants_cpy.set( lv_idx, -1 );       // no match
                                n_int_familyname_m++;
                                if( debugfail ) { System.out.println( "failed int_familyname_m" ); }
                                continue;
                            }
                        }

                        if( qs.int_firstname_m > 0 ) {
                            if( -1 == checkFirstName( qs.firstname,
                                s1MotherFirName1, s1MotherFirName2, s1MotherFirName3, s1MotherFirName4,
                                s2MotherFirName1, s2MotherFirName2, s2MotherFirName3, s2MotherFirName4,
                                qs.method, lvs_table_firstname, lvs_dist_firstname ) )
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
                            if( -1 == isVariant( s1FatherFamName, s2FatherFamName, lvs_table_familyname, lvs_dist_familyname, NameType.FAMILYNAME, qs.method ) )
                            {
                                s2_idx_variants_cpy.set( lv_idx, -1 );       // no match
                                n_int_familyname_f++;
                                if( debugfail ) { System.out.println( "failed int_familyname_f" ); }
                                continue;
                            }
                        }

                        if( qs.int_firstname_f > 0 ) {
                            if( -1 == checkFirstName( qs.firstname,
                                s1FatherFirName1, s1FatherFirName2, s1FatherFirName3, s1FatherFirName4,
                                s2FatherFirName1, s2FatherFirName2, s2FatherFirName3, s2FatherFirName4,
                                qs.method, lvs_table_firstname, lvs_dist_firstname ) )
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
                            if( -1 == isVariant( s1PartnerFamName, s2PartnerFamName, lvs_table_familyname, lvs_dist_familyname, NameType.FAMILYNAME, qs.method ) )
                            {
                                s2_idx_variants_cpy.set( lv_idx, -1 );       // no match
                                n_int_familyname_p++;
                                if( debugfail ) { System.out.println( "failed int_familyname_p" ); }
                                continue;
                            }
                        }

                        if( qs.int_firstname_p > 0 ) {
                            if( -1 == checkFirstName( qs.firstname,
                                s1PartnerFirName1, s1PartnerFirName2, s1PartnerFirName3, s1PartnerFirName4,
                                s2PartnerFirName1, s2PartnerFirName2, s2PartnerFirName3, s2PartnerFirName4,
                                qs.method, lvs_table_firstname, lvs_dist_firstname ) )
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

                        int id_match_process = mis.is.get( i ).get( 0 ).id;

                        int id_s1 = ql.s1_id_base.get( s1_idx );
                        int id_s2 = ql.s2_id_base.get( lv_idx );

                        int lvs_dist_first  = s2_idx_firstname_lvs .get( l );
                        int lvs_dist_family = s2_idx_familyname_lvs.get( l );

                        String query = "INSERT INTO matches ( id_match_process , id_linksbase_1 , id_linksbase_2, value_firstname, value_familyname ) " +
                            "VALUES ( " + id_match_process + "," + id_s1 + "," + id_s2 + "," + lvs_dist_first+ "," + lvs_dist_family + ")";

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

            // remove the memory tables
            // we could gain some speed by using the same mem tables for all threads, but via
            // match_process they could specify different tables for different threads: normal, _first, _strict.
            memtables_drop( threadId, table_firstname_src, table_familyname_src, name_postfix );

            msg = String.format( "Thread id %d; s1 records processed: %d", threadId, n_recs );
            System.out.println( msg );
            plog.show( msg );

            msg = String.format( "Thread id %d; Number of matches: %d", threadId, n_match );
            System.out.println( msg ); plog.show( msg );


            long n_fail = 0;

            if( n_minmax != 0 ) {
                n_fail += n_minmax;
                msg = String.format( "Thread id %d; failures n_minmax: %d", threadId, n_minmax );
                System.out.println( msg ); plog.show( msg );
            }

            if( n_sex != 0 ) {
                n_fail += n_sex;
                msg = String.format( "Thread id %d; failures n_sex: %d", threadId, n_minmax );
                System.out.println( msg ); plog.show( msg );
            }

            if( n_int_familyname_e != 0 ) {
                n_fail += n_int_familyname_e;
                msg = String.format( "Thread id %d; failures n_int_familyname_e: %d", threadId, n_int_familyname_e );
                System.out.println( msg ); plog.show( msg );
            }

            if( n_int_firstname_e != 0 ) {
                n_fail += n_int_firstname_e;
                msg = String.format( "Thread id %d; failures n_int_firstname_e: %d", threadId, n_int_firstname_e );
                System.out.println( msg ); plog.show( msg );
            }

            if( n_int_familyname_m != 0 ) {
                n_fail += n_int_familyname_m;
                msg = String.format( "Thread id %d; failures n_int_familyname_m: %d", threadId, n_int_familyname_m );
                System.out.println( msg ); plog.show( msg );
            }

            if( n_int_firstname_m != 0 ) {
                n_fail += n_int_firstname_m;
                msg = String.format( "Thread id %d; failures n_int_firstname_m: %d", threadId, n_int_firstname_m );
                System.out.println( msg ); plog.show( msg );
            }

            if( n_int_familyname_f != 0 ) {
                n_fail += n_int_familyname_f;
                msg = String.format( "Thread id %d; failures n_int_familyname_f: %d", threadId, n_int_familyname_f );
                System.out.println( msg ); plog.show( msg );
            }

            if( n_int_firstname_f != 0 ) {
                n_fail += n_int_firstname_f;
                msg = String.format( "Thread id %d; failures n_int_firstname_f: %d", threadId, n_int_firstname_f );
                System.out.println( msg ); plog.show( msg );
            }

            if( n_int_familyname_p != 0 ) {
                n_fail += n_int_familyname_p;
                msg = String.format( "Thread id %d; failures n_int_familyname_p: %d", threadId, n_int_familyname_p );
                System.out.println( msg ); plog.show( msg );
            }

            if( n_int_firstname_p != 0 ) {
                n_fail += n_int_firstname_p;
                msg = String.format( "Thread id %d; failures n_int_firstname_p: %d", threadId, n_int_firstname_p );
                System.out.println( msg ); plog.show( msg );
            }

            msg = String.format( "Thread id %d; total match attempt failures: %d", threadId, n_fail );
            System.out.println( msg ); plog.show( msg );

            long n_mismatch = n_recs - ( n_fail + n_match );
            if( n_mismatch > 0 ) {
                msg = String.format( "Thread id %d; missing records: %d ??", threadId, n_mismatch );
                System.out.println( msg ); plog.show( msg );
            }


            msg = String.format( "Thread id %d; Done: Range %d of %d", threadId, (j + 1), qgs.getSize() );
            System.out.println( msg );
            plog.show( msg );

            msg = String.format( "MatchAsync/run(): thread id %d is done", threadId );
            System.out.println( msg );
            plog.show( msg );
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


    private void memtables_create( long threadId, long max_heap_table_size, String table_firstname_src, String table_familyname_src, String name_postfix )
    {
        System.out.println( "Thread id "+ threadId + "; memtables_create()" );

        try
        {
            String query = "SET max_heap_table_size = " + max_heap_table_size;
            dbconPrematch.createStatement().execute( query );

            String table_firstname_dst  = "`" + table_firstname_src  + name_postfix + "`";
            String table_familyname_dst = "`" + table_familyname_src + name_postfix + "`";

            memtable_ls_name( threadId, table_firstname_src, table_firstname_dst );

            memtable_ls_name( threadId, table_familyname_src, table_familyname_dst );
        }
        catch( Exception ex ) { System.out.println( "Exception in memtables_create(): " + ex.getMessage() ); }
    } // memtables_create


    private void memtables_drop( long threadId, String table_firstname_src, String table_familyname_src, String name_postfix )
    {
        System.out.println( "Thread id "+ threadId + "; memtables_drop()" );

        try
        {
            String table_firstname_dst  = "`" + table_firstname_src  + name_postfix + "`";
            String table_familyname_dst = "`" + table_familyname_src + name_postfix + "`";

            String query = "DROP TABLE " + table_firstname_dst;
            dbconPrematch.createStatement().execute( query );

            query = "DROP TABLE " + table_familyname_dst;
            dbconPrematch.createStatement().execute( query );
        }
        catch( Exception ex ) { System.out.println( "Exception in memtables_drop(): " + ex.getMessage() ); }
    } // memtables_drop


    private void memtable_ls_name( long threadId, String src_table, String dst_table )
    {
        System.out.println( "Thread id "+ threadId + "; memtable_ls_name() copying " + src_table + " -> " + dst_table );

        try
        {
            String[] name_queries =
            {
                "DROP TABLE IF EXISTS " + dst_table,

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
                    + "  KEY `length_1` (`length_1`),"
                    + "  KEY `length_2` (`length_2`),"
                    + "  KEY `name_1` (`name_str_1`),"
                    + "  KEY `name_2` (`name_str_2`),"
                    + "  KEY `n_int_1` (`name_int_1`)"
                    + " )"
                    + " ENGINE = MEMORY DEFAULT CHARSET = utf8 COLLATE = utf8_bin",

                "ALTER TABLE " + dst_table + " DISABLE KEYS",

                "INSERT INTO " + dst_table + " SELECT * FROM " + src_table,

                "ALTER TABLE " + dst_table + " ENABLE KEYS"
            };

            for( String query : name_queries ) { dbconPrematch.createStatement().execute( query ); }
        }
        catch( Exception ex ) { System.out.println( "Exception in memtable_ls_name(): " + ex.getMessage() ); }
    } // memtable_ls_name


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
        // but..., we first check for an exact match
        if( s1Name == s2Name ) { return 0; }

        int lvs_dist = -1;      // -1 = no match, otherwise the found distance

        try
        {
            if( debug ) {
                String msg = "compareLSnames(): s1Name = " + s1Name + ", s2Name = " + s2Name;
                System.out.println( msg );
                plog.show( msg );
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
            if( debug ) { System.out.println( "" ); }
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
        int lvs_dist = -1;

        if( debug ) {
            System.out.println( "checkFirstName() fn_method: " + fn_method );
            try { plog.show( "checkFirstName() fn_method = " + fn_method ); }
            catch( Exception ex ) { System.out.println( ex.getMessage() ); }
        }

        if( fn_method == 1 )
        {
            throw new UnsupportedOperationException();
            /*
            if( ! isVariant( s1Name1, s2Name1, lvs_table, lvs_dist_max, NameType.FIRSTNAME, method ) ||
                ! isVariant( s1Name2, s2Name2, lvs_table, lvs_dist_max, NameType.FIRSTNAME, method ) ||
                ! isVariant( s1Name3, s2Name3, lvs_table, lvs_dist_max, NameType.FIRSTNAME, method ) ||
                ! isVariant( s1Name4, s2Name4, lvs_table, lvs_dist_max, NameType.FIRSTNAME, method ) )
            { return false; }
            */
        }
        else if( fn_method == 2 )
        {
            // only compare firstname1 of s1 & s2
            lvs_dist = isVariant( s1Name1, s2Name1, lvs_table, lvs_dist_max, NameType.FIRSTNAME, method );
        }
        else if( fn_method == 3 )
        {
            throw new UnsupportedOperationException();
            /*
            if( ! isVariant( s1Name1, s2Name1, lvs_table, lvs_dist_max, NameType.FIRSTNAME, method ) ||
                ! isVariant( s1Name2, s2Name2, lvs_table, lvs_dist_max, NameType.FIRSTNAME, method ) )
            { return false; }
            */
        }
        else if( fn_method == 4 )
        {
            throw new UnsupportedOperationException();
            /*
            if( ! isVariant( s1Name1, s2Name1, lvs_table, lvs_dist_max, NameType.FIRSTNAME, method ) ||
                ! isVariant( s1Name2, s2Name2, lvs_table, lvs_dist_max, NameType.FIRSTNAME, method ) ||
                ! isVariant( s1Name3, s2Name3, lvs_table, lvs_dist_max, NameType.FIRSTNAME, method ) )
            { return false; }
            */
        }
        else if( fn_method == 5 )
        {
            throw new UnsupportedOperationException();
            /*
            if( isVariant( s1Name1, s2Name1, lvs_table, lvs_dist_max, NameType.FIRSTNAME, method ) ||
                isVariant( s1Name1, s2Name2, lvs_table, lvs_dist_max, NameType.FIRSTNAME, method ) ||
                isVariant( s1Name1, s2Name3, lvs_table, lvs_dist_max, NameType.FIRSTNAME, method ) ||
                isVariant( s1Name1, s2Name4, lvs_table, lvs_dist_max, NameType.FIRSTNAME, method ) ||
                isVariant( s2Name1, s1Name2, lvs_table, lvs_dist_max, NameType.FIRSTNAME, method ) ||
                isVariant( s2Name1, s1Name3, lvs_table, lvs_dist_max, NameType.FIRSTNAME, method ) ||
                isVariant( s2Name1, s1Name4, lvs_table, lvs_dist_max, NameType.FIRSTNAME, method ) )
            { ; }
            else { return false; }
            */
        }

        return lvs_dist;
    } // checkFirstName

}

