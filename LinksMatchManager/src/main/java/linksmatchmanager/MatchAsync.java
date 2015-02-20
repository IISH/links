package linksmatchmanager;

import java.sql.Connection;
import java.sql.ResultSet;

import java.util.Arrays;
import java.util.ArrayList;

import linksmatchmanager.DataSet.QuerySet;
import linksmatchmanager.DataSet.NameType;
import linksmatchmanager.DataSet.QueryGroupSet;

/**
 * @author Omar Azouguagh
 * @author Fons Laan
 *
 * FL-20-Feb-2015 Latest change
 */

public class MatchAsync extends Thread
{
    boolean debug;

    boolean doExactMatch;
    boolean doLevyMatch;

    ProcessManager pm;

    int i;
    int j;

    QueryLoader ql;
    PrintLogger plog;

    QueryGroupSet qgs;
    QueryGenerator mis;

    Connection dbconPrematch;
    Connection dbconMatch;

    int[][] variantFirstName;
    int[][] variantFamilyName;
    int[][] rootFirstName;
    int[][] rootFamilyName;

    boolean isUseRoot = false;      // false for variant


    public MatchAsync
    (
        boolean debug,

        boolean doExactMatch,
        boolean doLevyMatch,

        ProcessManager pm,

        int i,
        int j,

        QueryLoader ql,
        PrintLogger plog,

        QueryGroupSet qgs,
        QueryGenerator mis,

        Connection dbconPrematch,
        Connection dbconMatch,

        int[][] variantFirstName,
        int[][] variantFamilyName
    )
    {
        this.debug = debug;

        this.doExactMatch = doExactMatch;
        this.doLevyMatch  = doLevyMatch;

        this.pm = pm;

        this.i = i;
        this.j = j;

        this.ql = ql;
        this.plog = plog;

        this.qgs = qgs;
        this.mis = mis;

        this.dbconPrematch = dbconPrematch;
        this.dbconMatch    = dbconMatch;

        this.variantFirstName  = variantFirstName;
        this.variantFamilyName = variantFamilyName;

        System.out.println( "MatchAsync: using variant names (instead of root names)" );
    }


    public MatchAsync   // variant has no root parameter
    (
        boolean debug,

        boolean doExactMatch,
        boolean doLevyMatch,

        ProcessManager pm,

        int i,
        int j,

        QueryLoader ql,
        PrintLogger plog,

        QueryGroupSet qgs,
        QueryGenerator mis,

        Connection dbconPrematch,
        Connection dbconMatch,

        int[][] rootFirstName,
        int[][] rootFamilyName,

        boolean root
    )
    {
        this.debug = debug;

        this.doExactMatch = doExactMatch;
        this.doLevyMatch  = doLevyMatch;

        this.pm = pm;

        this.i = i;
        this.j = j;

        this.ql = ql;
        this.plog = plog;

        this.qgs = qgs;
        this.mis = mis;

        this.dbconPrematch = dbconPrematch;
        this.dbconMatch = dbconMatch;

        this.rootFirstName  = rootFirstName;
        this.rootFamilyName = rootFamilyName;

        this.isUseRoot = true;      // true for root

        System.out.println( "MatchAsync: using root names (instead of variant names)" );
    }

    @Override
    public void run()
    {
        // in order to show the indexes when an exception occurs, we define copies outside the try/catch
        int s1_idx_cpy = 0;
        int lv_idx_cpy = 0;

        boolean debugfail = false;
        // count why the matches fail
        int n_sex    = 0;
        int n_minmax = 0;

        int n_int_firstname_e = 0;  // never used
        int n_int_firstname_m = 0;
        int n_int_firstname_f = 0;
        int n_int_firstname_p = 0;

        int n_int_familyname_e = 0;
        int n_int_familyname_m = 0;
        int n_int_familyname_f = 0;
        int n_int_familyname_p = 0;


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

            String lvs_familyname = qgs.get( 0 ).prematch_familyname;
            String lvs_firstname  = qgs.get( 0 ).prematch_firstname;

            System.out.println( "prematch_familyname table: " + lvs_familyname );
            System.out.println( "prematch_firstname table: "  + lvs_firstname );

            int lvs_dist_familyname = qgs.get( 0 ).prematch_familyname_value;
            int lvs_dist_firstname  = qgs.get( 0 ).prematch_firstname_value;

            System.out.println( "prematch_familyname_value: " + lvs_dist_familyname );
            System.out.println( "prematch_firstname_value: "  + lvs_dist_firstname );

            if( doExactMatch ) { System.out.println( "[also] searching for exact matches"); }
            else               { System.out.println( "NOT searching for exact matches"); }

            if( doLevyMatch )  { System.out.println( "[also] searching for levenshtein matches"); }
            else               { System.out.println( "NOT searching for levenshtein matches"); }

            // Create new instance of queryloader. Queryloader is used to use the queries to load data into the sets.
            // Its input is a QuerySet and a database connection object.
            ql = new QueryLoader( qs, dbconPrematch );

            // Last familyname, initial is 0. Because the familynames are ordered, the calculation of the potential
            // matches is done once, only the first time.
            int previousEgoFamilyName = 0;

            ArrayList< Integer > lvsVariants  = new ArrayList< Integer >();
            ArrayList< Integer > lvsDistances = new ArrayList< Integer >();

            // almost the same as the previous pair, but may include s2_idx exact match
            // lv_idx -> s2EgoFamName -> s2_idx
            ArrayList< Integer > s2_idx_variants     = new ArrayList< Integer >();
            ArrayList< Integer > s2_idx_variants_lvs = new ArrayList< Integer >();

            // Loop through set 1
            msg = String.format( "Thread id %d; Set 1 size: %d from links_base", threadId, ql.s1_id_base.size() );
            System.out.println( msg );
            plog.show( msg );

            int n_recs    = 0;
            int n_match   = 0;
            int s1_size  = ql.s1_id_base.size();
            int s1_chunk = s1_size / 20;

            //for( int k = 0; k < ql.s1_id_base.size(); k++ )
            for( int s1_idx = 0; s1_idx < s1_size; s1_idx++ )
            {
                n_recs ++;

                /*
                if( s1_idx > 10 ) {
                    System.out.println( "EXIT in MatchAsync/run()" );
                    System.exit( 0 );
                }
                */

                s1_idx_cpy = s1_idx;   // copy for display if exception occurs

                if( ( s1_idx + s1_chunk ) % s1_chunk == 0 )
                { System.out.println( "records processed: " + s1_idx + ", matches found: " + n_match ); }

                if( debug ) {
                    msg = String.format( "\ns1 idx: %d-of-%d", s1_idx + 1, s1_size );
                    System.out.println( msg );
                    plog.show( msg );
                }

                // Get familyname of Set 1
                String s1EgoFamNameStr = ql.s1_ego_familyname_str.get( s1_idx );
                int s1EgoFamName = ql.s1_ego_familyname.get( s1_idx );
                //System.out.printf( "s1 ego familyname: %s ", s1EgoFamNameStr );

                // If the familyname changes, create a new potentialMatches list,
                // otherwise go on to check the other values from the s1 set for this s1_idx

                if( s1EgoFamName != previousEgoFamilyName )
                {
                    //System.out.printf("s1 ego familyname: %s ", s1EgoFamNameStr);
                    previousEgoFamilyName = s1EgoFamName;                  // Set previous name

                    // Get the variants of name s1EgoFamName; these are names (as ints) from the ls_ table
                    // Create list with potential matches for a s1 ego familyname
                    lvsVariants .clear();                           // Empty the lists
                    lvsDistances.clear();

                    String Lvs_table = lvs_familyname;
                    int Lvs_dist     = lvs_dist_familyname;

                    // fill the lvsVariants and lvsDistances lists for variants of s1EgoFamName
                    getLvsVariants( s1EgoFamName, Lvs_table, Lvs_dist, lvsVariants, lvsDistances );

                    s2_idx_variants    .clear();                    // Empty the lists
                    s2_idx_variants_lvs.clear();

                    if( doExactMatch ) {
                        // also include a potential exact match of s1EgoFamName in s2 !
                        int s2_idx = ql.s2_ego_familyname.indexOf( s1EgoFamName );  // index in s2 set

                        if( s2_idx == -1 ) {
                            n_int_familyname_e++;
                            if( debugfail ) { System.out.println( "failed int_familyname_e" ); }
                        }
                        else { if( debug ) { System.out.println( "s1EgoFamName found in s2: " + s1EgoFamName ); }

                            //String s2_ego_familyname_str = ql.s2_ego_familyname_str.get( s2_idx );
                            //System.out.println( ": (" + s2_ego_familyname_str + ")");

                            s2_idx_variants    .add( s2_idx );
                            s2_idx_variants_lvs.add( 0 );       // exact match
                        }
                    }

                    if( doLevyMatch ) {
                        for( int lv_idx = 0; lv_idx < lvsVariants.size(); lv_idx++ )
                        {
                            lv_idx_cpy = lv_idx;        // copy for display if exception occurs

                            if( debug ) {
                                msg = String.format( "lv idx: %d-of-%d", lv_idx + 1, lvsVariants.size() );
                                System.out.println( msg );
                                plog.show( msg );
                            }

                            int s2EgoFamName = lvsVariants.get( lv_idx );                   // potential match
                            int s2_idx = ql.s2_ego_familyname.indexOf( s2EgoFamName );      // index in s2 set
                            if( s2_idx != -1 ) {
                                if( debug ) { System.out.println( "s1EgoFamName found in s2: " + s1EgoFamName ); }

                                //String s2_ego_familyname_str = ql.s2_ego_familyname_str.get( s2_idx );
                                //System.out.println( " " + s2_ego_familyname_str);

                                s2_idx_variants    .add( s2_idx );
                                s2_idx_variants_lvs.add( lvsDistances.get( lv_idx ) );      // Lvs distance from ls_ table
                            }
                        }
                    }

                    //System.out.println( "" );
                }

                if( debug  && s2_idx_variants.size() > 0 ) {
                    msg = String.format( "Thread id %d; potential matches: %d", threadId, s2_idx_variants.size() );
                    System.out.println( msg );
                    plog.show( msg );
                }

                // Copy to working copy, zeroing non-matches as we proceed
                ArrayList< Integer > matchesList = new ArrayList< Integer >();
                matchesList.addAll( s2_idx_variants );

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

                // loop through the familynames of s2; exact and/or variants
                for( int lv_idx = 0; lv_idx < matchesList.size(); lv_idx++ )
                {
                    lv_idx_cpy = lv_idx;        // copy for display if exception occurs

                    int s2_idx = matchesList.get( lv_idx );

                    if( debug ) {
                        String s2_ego_familyname_str = ql.s2_ego_familyname_str.get( s2_idx );
                        msg = String.format( "s2 idx: %d %s\n", s2_idx, s2_ego_familyname_str );
                        System.out.println( msg );
                        plog.show( msg );
                    }

                    // Check min max; use new min max
                    if( ! qs.ignore_minmax ) {
                        if( ! CheckMinMax( qs, s1_idx, s2_idx ) ) {
                            matchesList.set( lv_idx, -1 );           // no match
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
                            matchesList.set( lv_idx, -1 );           // no match
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
                        if( ! checkFirstName( qs.firstname,
                            s1EgoFirName1, s1EgoFirName2, s1EgoFirName3, s1EgoFirName4,
                            s2EgoFirName1, s2EgoFirName2, s2EgoFirName3, s2EgoFirName4,
                            qs.method, lvs_firstname, lvs_dist_firstname ) )
                        {
                            matchesList.set( lv_idx, -1 );           // no match
                            n_int_firstname_e++;
                            if( debugfail ) { System.out.println( "failed int_firstname_e" ); }
                            continue;
                        }
                    }

                    if( qs.use_mother ) {
                        if( qs.int_familyname_m > 0 ) {
                            if( ! isVariant( s1MotherFamName, s2MotherFamName, lvs_familyname, lvs_dist_familyname, NameType.FAMILYNAME, qs.method ) )
                            {
                                matchesList.set( lv_idx, -1 );       // no match
                                n_int_familyname_m++;
                                if( debugfail ) { System.out.println( "failed int_familyname_m" ); }
                                continue;
                            }
                        }

                        if( qs.int_firstname_m > 0 ) {
                            if( ! checkFirstName( qs.firstname,
                                s1MotherFirName1, s1MotherFirName2, s1MotherFirName3, s1MotherFirName4,
                                s2MotherFirName1, s2MotherFirName2, s2MotherFirName3, s2MotherFirName4,
                                qs.method, lvs_firstname, lvs_dist_firstname ) )
                            {
                                matchesList.set( lv_idx, -1 );       // no match
                                n_int_firstname_m++;
                                if( debugfail ) { System.out.println( "failed int_firstname_m" ); }
                                continue;
                            }
                        }
                    }

                    if( qs.use_father ) {
                        if( qs.int_familyname_f > 0 ) {
                            if( ! isVariant( s1FatherFamName, s2FatherFamName, lvs_familyname, lvs_dist_familyname, NameType.FAMILYNAME, qs.method ) )
                            {
                                matchesList.set( lv_idx, -1 );       // no match
                                n_int_familyname_f++;
                                if( debugfail ) { System.out.println( "failed int_familyname_f" ); }
                                continue;
                            }
                        }

                        if( qs.int_firstname_f > 0 ) {
                            if( ! checkFirstName( qs.firstname,
                                s1FatherFirName1, s1FatherFirName2, s1FatherFirName3, s1FatherFirName4,
                                s2FatherFirName1, s2FatherFirName2, s2FatherFirName3, s2FatherFirName4,
                                qs.method, lvs_firstname, lvs_dist_firstname ) )
                            {
                                matchesList.set( lv_idx, -1 );       // no match
                                n_int_firstname_f++;
                                if( debugfail ) { System.out.println( "failed int_firstname_f" ); }
                                continue;
                            }
                        }
                    }

                    if( qs.use_partner ) {
                        if( qs.int_familyname_p > 0 ) {
                            if( ! isVariant( s1PartnerFamName, s2PartnerFamName, lvs_familyname, lvs_dist_familyname, NameType.FAMILYNAME, qs.method ) )
                            {
                                matchesList.set( lv_idx, -1 );       // no match
                                n_int_familyname_p++;
                                if( debugfail ) { System.out.println( "failed int_familyname_p" ); }
                                continue;
                            }
                        }

                        if( qs.int_firstname_p > 0 ) {
                            if( ! checkFirstName( qs.firstname,
                                s1PartnerFirName1, s1PartnerFirName2, s1PartnerFirName3, s1PartnerFirName4,
                                s2PartnerFirName1, s2PartnerFirName2, s2PartnerFirName3, s2PartnerFirName4,
                                qs.method, lvs_firstname, lvs_dist_firstname ) )
                            {
                                matchesList.set( lv_idx, -1 );       // no match
                                n_int_firstname_p++;
                                if( debugfail ) { System.out.println( "failed int_firstname_p" ); }
                                continue;
                            }
                        }
                    }
                }
                //System.out.println( "variants done" );

                // entries of matchesList that have not been zeroed above imply a match
                for( int l = 0; l < matchesList.size(); l++ )
                {
                    if( matchesList.get( l ) != -1 ) {
                        n_match++;

                        int id_match_process = mis.is.get( i ).get( 0 ).id;

                        int id_s1 = ql.s1_id_base.get( s1_idx );
                        int id_s2 = ql.s2_id_base.get( matchesList.get( l ) );

                        int Lvs_dist = s2_idx_variants_lvs.get( l );

                        String query = "INSERT INTO matches ( id_match_process , id_linksbase_1 , id_linksbase_2, value_familyname ) " +
                            "VALUES ( " + id_match_process + "," + id_s1 + "," + id_s2 + "," + Lvs_dist + ")";

                        if( debug ) {
                            System.out.println( query );
                            plog.show( query );
                        }

                        dbconMatch.createStatement().execute( query );
                        dbconMatch.createStatement().close();
                    }
                }
                matchesList.clear();
            }

            pm.removeProcess();

            msg = String.format( "s1 records processed: %d", n_recs );
            System.out.println( msg );
            plog.show( msg );

            msg = String.format( "Number of matches: %d", n_match );
            System.out.println( msg ); plog.show( msg );


            int n_fail = 0;

            if( n_sex != 0 ) {
                n_fail += n_sex;
                msg = String.format( "failures n_sex: %d", n_minmax );
                System.out.println( msg ); plog.show( msg );
            }

            if( n_minmax != 0 ) {
                n_fail += n_minmax;
                msg = String.format( "failures n_minmax: %d", n_minmax );
                System.out.println( msg ); plog.show( msg );
            }

            if( n_int_familyname_e != 0 ) {
                n_fail += n_int_familyname_e;
                msg = String.format( "failures n_int_familyname_e: %d", n_int_familyname_e );
                System.out.println( msg ); plog.show( msg );
            }

            if( n_int_familyname_p != 0 ) {
                n_fail += n_int_familyname_p;
                msg = String.format( "failures n_int_familyname_p: %d", n_int_familyname_p );
                System.out.println( msg ); plog.show( msg );
            }

            msg = String.format( "total failures: %d", n_fail );
            System.out.println( msg ); plog.show( msg );

            int n_mismatch = n_recs - ( n_fail + n_match );
            if( n_mismatch > 0 ) {
                msg = String.format( "missing records: %d ??", n_mismatch );
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
    private void getLvsVariants( int name_int_1, String lvs_table, int lvs_dist_max, ArrayList< Integer > lvsVariants, ArrayList< Integer > lvsDistances )
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
     * Query table ls_firstname or ls_lastname for s1Name in name_int_1 column,
     * to get the name_int_2 as levenshtein variants for the set s2.
     * Compare the variants for a match with s2Name
     */
    //private void variantsFirstnameToList( int ego_firstname, ArrayList< Integer > potentialMatches, ArrayList< Integer > LvPotentialMatches )
    private boolean compareLSnames( int s1Name, int s2Name, String lsv_table, int lvs_dist )
    {
        // but..., we first check for an exact match
        if( s1Name == s2Name ) { return true; }


        boolean match = false;
        try
        {
            if( debug ) {
                String msg = "compareLSnames(): s1Name = " + s1Name + ", s2Name = " + s2Name;
                System.out.println( msg );
                plog.show( msg );
            }

            String query = "SELECT * FROM links_prematch." + lsv_table + " WHERE value <= " + lvs_dist + " AND name_int_1 = " + s1Name ;
            ResultSet rs = dbconPrematch.createStatement().executeQuery( query );

            int nrecs = 0;
            while( rs.next() )
            {
                //int length_1 = rs.getInt( "length_1" );
                //int length_2 = rs.getInt( "length_2" );

                int name_int_1 = rs.getInt( "name_int_1" );
                int name_int_2 = rs.getInt( "name_int_2" );

                //int Ldist = rs.getInt( "value" );

                if( debug ) {
                    String name_str_1 = rs.getString( "name_str_1" );
                    String name_str_2 = rs.getString( "name_str_2" );

                    if( nrecs == 0 ) { System.out.printf( "variants for %s (%d): ", name_str_1, name_int_1 ); }
                    System.out.printf( "%s (%d) ", name_str_2, name_int_2 );
                }

                //potentialMatches  .add( name_int_2 );
                //LvPotentialMatches.add( Ldist );

                if( s2Name == name_int_2 ) {
                    match = true;
                    if( debug ) { System.out.println( "variantsToList(): match" );  }
                    break;
                }

                nrecs++;
            }
            if( debug ) { System.out.println( "" ); }
        }
        catch( Exception ex ) {
            System.out.println( "Exception in compareLSnames: " + ex.getMessage() );
            System.out.println( "Abort" );
            System.exit( 1 );
        }

        return match;
    } // compareLSnames


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
    private void variantsToList( int fn, ArrayList< Integer > potentialMatches, ArrayList< Integer > LvPotentialMatches )
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
        if( debug ) {
            try { plog.show( "CheckMinMax()" ); }
            catch( Exception ex ) { System.out.println( ex.getMessage() ); }
        }

        if ((qs.int_minmax_e % 2) == 1) {
            if ((firstGreater(ql.s1_ego_birth_min.get(k),
                    ql.s2_ego_birth_max.get(index)))
                    || (firstGreater(ql.s2_ego_birth_min.get(index),
                    ql.s1_ego_birth_max.get(k)))) {
                return false;
            }
        }
        if (qs.int_minmax_e == 10
                || qs.int_minmax_e == 11
                || qs.int_minmax_e == 110
                || qs.int_minmax_e == 111) {
            if ((firstGreater(ql.s1_ego_marriage_min.get(k),
                    ql.s2_ego_marriage_max.get(index)))
                    || (firstGreater(ql.s2_ego_marriage_min.get(index),
                    ql.s1_ego_marriage_max.get(k)))) {
                return false;
            }
        }
        if (qs.int_minmax_e > 99) {
            if ((firstGreater(ql.s1_ego_death_min.get(k),
                    ql.s2_ego_death_max.get(index)))
                    || (firstGreater(ql.s2_ego_death_min.get(index),
                    ql.s1_ego_death_max.get(k)))) {
                return false;
            }
        }
        if (qs.use_mother) {
            if ((qs.int_minmax_m % 2) == 1) {
                if ((firstGreater(ql.s1_mother_birth_min.get(k),
                        ql.s2_mother_birth_max.get(index)))
                        || (firstGreater(ql.s2_mother_birth_min.get(index),
                        ql.s1_mother_birth_max.get(k)))) {
                    return false;
                }
            }
            if (qs.int_minmax_m == 10
                    || qs.int_minmax_m == 11
                    || qs.int_minmax_m == 110
                    || qs.int_minmax_m == 111) {
                if ((firstGreater(ql.s1_mother_marriage_min.get(k),
                        ql.s2_mother_marriage_max.get(index)))
                        || (firstGreater(ql.s2_mother_marriage_min.get(index),
                        ql.s1_mother_marriage_max.get(k)))) {
                    return false;
                }
            }
            if (qs.int_minmax_m > 99) {
                if ((firstGreater(ql.s1_mother_death_min.get(k),
                        ql.s2_mother_death_max.get(index)))
                        || (firstGreater(ql.s2_mother_death_min.get(index),
                        ql.s1_mother_death_max.get(k)))) {
                    return false;
                }
            }
        }
        if (qs.use_father) {
            if ((qs.int_minmax_f % 2) == 1) {
                if ((firstGreater(ql.s1_father_birth_min.get(k),
                        ql.s2_father_birth_max.get(index)))
                        || (firstGreater(ql.s2_father_birth_min.get(index),
                        ql.s1_father_birth_max.get(k)))) {
                    return false;
                }
            }
            if (qs.int_minmax_f == 10
                    || qs.int_minmax_f == 11
                    || qs.int_minmax_f == 110
                    || qs.int_minmax_f == 111) {
                if ((firstGreater(ql.s1_father_marriage_min.get(k),
                        ql.s2_father_marriage_max.get(index)))
                        || (firstGreater(ql.s2_father_marriage_min.get(index),
                        ql.s1_father_marriage_max.get(k)))) {
                    return false;
                }
            }
            if (qs.int_minmax_f > 99) {
                if ((firstGreater(ql.s1_father_death_min.get(k),
                        ql.s2_father_death_max.get(index)))
                        || (firstGreater(ql.s2_father_death_min.get(index),
                        ql.s1_father_death_max.get(k)))) {
                    return false;
                }
            }
        }
        if (qs.use_partner) {
            if ((qs.int_minmax_p % 2) == 1) {
                if ((firstGreater(ql.s1_partner_birth_min.get(k),
                        ql.s2_partner_birth_max.get(index)))
                        || (firstGreater(ql.s2_partner_birth_min.get(index),
                        ql.s1_partner_birth_max.get(k)))) {
                    return false;
                }
            }
            if (qs.int_minmax_p == 10
                    || qs.int_minmax_p == 11
                    || qs.int_minmax_p == 110
                    || qs.int_minmax_p == 111) {
                if ((firstGreater(ql.s1_partner_marriage_min.get(k),
                        ql.s2_partner_marriage_max.get(index)))
                        || (firstGreater(ql.s2_partner_marriage_min.get(index),
                        ql.s1_partner_marriage_max.get(k)))) {
                    return false;
                }
            }
            if (qs.int_minmax_p > 99) {
                if ((firstGreater(ql.s1_partner_death_min.get(k),
                        ql.s2_partner_death_max.get(index)))
                        || (firstGreater(ql.s2_partner_death_min.get(index),
                        ql.s1_partner_death_max.get(k)))) {
                    return false;
                }
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

        if (first == 0 || second == 0) {
            return false;
        }
        if (first > second) {
            return true;
        }
        return false;
    } // firstGreater


    private boolean isVariant( int s1Name, int s2Name, String lvs_table, int lvs_dist, NameType tnt, int method )
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
                return compareLSnames( s1Name, s2Name, lvs_table, lvs_dist );

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

                return compareLSnames( s1Name, s2Name, lvs_table, lvs_dist );

                /*
                if( s1Name == 0 || s2Name == 0 ) { return false; }    // NULL string names
                else if( s1Name == s2Name ) { ; }                     // identical names
                else if( variantFirstName.length > s1Name && variantFirstName[ s1Name ] != null && Arrays.binarySearch( variantFirstName[ s1Name ], s2Name ) > -1) { ; }
                else if( variantFirstName.length > s2Name && variantFirstName[ s2Name ] != null && Arrays.binarySearch( variantFirstName[ s2Name ], s1Name ) > -1) { ; }
                else { return false; }
                */
            }

            return true;
        }

        else if( method == 1 )
        {
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
        }

        else { return false; }
    } // isVariant


    private boolean checkFirstName
    (
        int fn_method,          // 'firstname' in match_process table: {0,1,2,3,4,5}
        int s1Name1, int s1Name2, int s1Name3, int s1Name4,
        int s2Name1, int s2Name2, int s2Name3, int s2Name4,
        int method,             // 'method' in match_process table: {0,1}
        String lvs_table, int lvs_dist
        )
    {
        if( debug ) {
            System.out.println( "checkFirstName() fn_method: " + fn_method );
            try { plog.show( "checkFirstName() fn_method = " + fn_method ); }
            catch( Exception ex ) { System.out.println( ex.getMessage() ); }
        }

        if( fn_method == 1 )
        {
            if( ! isVariant( s1Name1, s2Name1, lvs_table, lvs_dist, NameType.FIRSTNAME, method ) ||
                ! isVariant( s1Name2, s2Name2, lvs_table, lvs_dist, NameType.FIRSTNAME, method ) ||
                ! isVariant( s1Name3, s2Name3, lvs_table, lvs_dist, NameType.FIRSTNAME, method ) ||
                ! isVariant( s1Name4, s2Name4, lvs_table, lvs_dist, NameType.FIRSTNAME, method ) )
            { return false; }
        }
        else if( fn_method == 2 )
        {
            if( ! isVariant( s1Name1, s2Name1, lvs_table, lvs_dist, NameType.FIRSTNAME, method ) )
            { return false; }
        }
        else if( fn_method == 3 )
        {
            if( ! isVariant( s1Name1, s2Name1, lvs_table, lvs_dist, NameType.FIRSTNAME, method ) ||
                ! isVariant( s1Name2, s2Name2, lvs_table, lvs_dist, NameType.FIRSTNAME, method ) )
            { return false; }
        }
        else if( fn_method == 4 )
        {
            if( ! isVariant( s1Name1, s2Name1, lvs_table, lvs_dist, NameType.FIRSTNAME, method ) ||
                ! isVariant( s1Name2, s2Name2, lvs_table, lvs_dist, NameType.FIRSTNAME, method ) ||
                ! isVariant( s1Name3, s2Name3, lvs_table, lvs_dist, NameType.FIRSTNAME, method ) )
            { return false; }
        }
        else if( fn_method == 5 )
        {
            if( isVariant( s1Name1, s2Name1, lvs_table, lvs_dist, NameType.FIRSTNAME, method ) ||
                isVariant( s1Name1, s2Name2, lvs_table, lvs_dist, NameType.FIRSTNAME, method ) ||
                isVariant( s1Name1, s2Name3, lvs_table, lvs_dist, NameType.FIRSTNAME, method ) ||
                isVariant( s1Name1, s2Name4, lvs_table, lvs_dist, NameType.FIRSTNAME, method ) ||
                isVariant( s2Name1, s1Name2, lvs_table, lvs_dist, NameType.FIRSTNAME, method ) ||
                isVariant( s2Name1, s1Name3, lvs_table, lvs_dist, NameType.FIRSTNAME, method ) ||
                isVariant( s2Name1, s1Name4, lvs_table, lvs_dist, NameType.FIRSTNAME, method ) )
            { ; }
            else { return false; }
        }
        return true;
    } // checkFirstName

}

