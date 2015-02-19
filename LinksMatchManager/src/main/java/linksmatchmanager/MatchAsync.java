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
 * FL-19-Feb-2015 Latest change
 */

public class MatchAsync extends Thread
{
    boolean debug;
    boolean useExactMatch;

    ProcessManager pm;

    int i;
    int j;

    QueryLoader ql;
    PrintLogger plog;

    QueryGroupSet qgs;
    QueryGenerator mis;

    Connection conPrematch;
    Connection conMatch;

    int[][] variantFirstName;
    int[][] variantFamilyName;
    int[][] rootFirstName;
    int[][] rootFamilyName;

    boolean isUseRoot = false;      // false for variant


    public MatchAsync
    (
        boolean debug,
        boolean useExactMatch,

        ProcessManager pm,

        int i,
        int j,

        QueryLoader ql,
        PrintLogger plog,

        QueryGroupSet qgs,
        QueryGenerator mis,

        Connection conPrematch,
        Connection conMatch,

        int[][] variantFirstName,
        int[][] variantFamilyName
    )
    {
        this.debug = debug;
        this.useExactMatch = useExactMatch;

        this.pm = pm;

        this.i = i;
        this.j = j;

        this.ql = ql;
        this.plog = plog;

        this.qgs = qgs;
        this.mis = mis;

        this.conPrematch = conPrematch;
        this.conMatch    = conMatch;

        this.variantFirstName  = variantFirstName;
        this.variantFamilyName = variantFamilyName;

        System.out.println( "MatchAsync: using variant names (instead of root names)" );
    }


    public MatchAsync   // variant has no root parameter
    (
        boolean debug,
        boolean useExactMatch,

        ProcessManager pm,

        int i,
        int j,

        QueryLoader ql,
        PrintLogger plog,

        QueryGroupSet qgs,
        QueryGenerator mis,

        Connection conPrematch,
        Connection conMatch,

        int[][] rootFirstName,
        int[][] rootFamilyName,

        boolean root
    )
    {
        this.debug = debug;
        this.useExactMatch = useExactMatch;

        this.pm = pm;

        this.i = i;
        this.j = j;

        this.ql = ql;
        this.plog = plog;

        this.qgs = qgs;
        this.mis = mis;

        this.conPrematch = conPrematch;
        this.conMatch = conMatch;

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

            // Create new instance of queryloader. Queryloader is used to use the queries to load data into the sets.
            // Its input is a QuerySet and a database connection object.
            ql = new QueryLoader( qs, conPrematch );

            // Last familyname, initial is 0. Because the familynames are ordered, the calculation of the potential
            // matches is done once, only the first time.
            int lastFamilyName = 0;

            // Create list with potential matches for a familyname
            ArrayList< Integer >   potentialMatches = new ArrayList< Integer >();
            ArrayList< Integer > LvPotentialMatches = new ArrayList< Integer>();

            // Loop through set 1
            msg = String.format( "Thread id %d; Set 1 size: %d", threadId, ql.s1_id_base.size() );
            System.out.println( msg );
            plog.show( msg );

            int nmatch = 0;
            int s1_size = ql.s1_id_base.size();
            int s1_chunk = s1_size / 20;

            //for( int k = 0; k < ql.s1_id_base.size(); k++ )
            for( int s1_idx = 0; s1_idx < ql.s1_id_base.size(); s1_idx++ )
            {
                /*
                if( s1_idx > 10 ) {
                    System.out.println( "EXIT in MatchAsync/run()" );
                    System.exit( 0 );
                }
                */

                s1_idx_cpy = s1_idx;   // copy for display if exception occurs

                if( ( s1_idx + s1_chunk ) % s1_chunk == 0 )
                { System.out.println( "records processed: " + s1_idx + ", matches found: " + nmatch ); }

                if( debug ) {
                    msg = String.format( "\ns1 idx: %d-of-%d", s1_idx + 1, s1_size );
                    System.out.println( msg );
                    plog.show( msg );
                }


                // The order of the steps has to be dynamic
                // Now it starts with the check of the familyname

                // Get familyname of Set 1
                int s1EgoFamName = ql.s1_ego_familyname.get( s1_idx );

                // If the familyname changes, create a new potentialMatches list, otherwise go on
                if( s1EgoFamName != lastFamilyName )
                {
                    lastFamilyName = s1EgoFamName;                      // Set previous name

                    potentialMatches.clear();                           // Empty the list
                    LvPotentialMatches.clear();

                    // Load the potential matches;
                    // these are names (as ints) from the ls_ table
                    variantsToList( s1EgoFamName, "ls_familyname", potentialMatches, LvPotentialMatches );
                }


                if( debug  && potentialMatches.size() > 0 ) {
                    msg = String.format( "Thread id %d; potential matches: %d", threadId, potentialMatches.size() );
                    System.out.println( msg );
                    plog.show( msg );
                }

                // Copy the existing variantList to working copy, zeroing non-matches as we proceed
                ArrayList< Integer > matchesList = new ArrayList< Integer >();
                matchesList.addAll( potentialMatches );

                // TODO: improve performance: process from lowest to highest frequency

                // familyname
                //  s1EgoFamName -> already loaded
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

                // loop through the Levenshtein variants
                for( int lv_idx = 0; lv_idx < potentialMatches.size(); lv_idx++ )
                {
                    lv_idx_cpy = lv_idx;        // copy for display if exception occurs

                    if( debug ) {
                        msg = String.format( "lv idx: %d-of-%d", lv_idx + 1, potentialMatches.size() );
                        System.out.println( msg );
                        plog.show( msg );
                    }

                    int s2EgoFamName = potentialMatches.get( lv_idx );          // potential match
                    int s2_idx = ql.s2_ego_familyname.indexOf( s2EgoFamName );  // index in s2 set

                    if( s2_idx == -1 ) {
                        if( debug ) { System.out.println( "s1 variant not in s2: " + s2EgoFamName ); }
                        matchesList.set( lv_idx, 0 );               // no match
                        continue;                                   // next potential match
                    }
                    else { matchesList.set( lv_idx, s2_idx ); }     // replace Lv int from ls_ with index from s2 set

                    if( debug ) {
                        msg = String.format( "s2 idx: %d", s2_idx );
                        System.out.println( msg );
                        plog.show( msg );
                    }

                    // Check min max; use new min max
                    if( ! qs.ignore_minmax ) {
                        if( ! CheckMinMax( qs, s1_idx, s2_idx ) ) {
                            matchesList.set( lv_idx, 0 );           // no match
                            if( debug ) { System.out.println( "failed ignore_minmax" ); }
                            continue;
                        }
                    }

                    // Check sex
                    if( ! qs.ignore_sex ) {
                        int s1s = ql.s1_sex.get( s1_idx );
                        int s2s = ql.s2_sex.get( s2_idx );

                        // Empty sex is denied
                        if( s1s != 0 && s2s != 0 && ( s1s != s2s ) ) {
                            matchesList.set( lv_idx, 0 );           // no match
                            if( debug ) { System.out.println( "failed ignore_sex" ); }
                            continue;
                        }
                    }

                    // familyname
                    //  s2EgoFamName -> already loaded
                    int s2MotherFamName  = ql.s2_mother_familyname .get( s2_idx );
                    int s2FatherFamName  = ql.s2_father_familyname .get( s2_idx );
                    int s2PartnerFamName = ql.s2_partner_familyname.get( s2_idx );

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
                            qs.method ) )
                        {
                            matchesList.set( lv_idx, 0 );           // no match
                            if( debug ) { System.out.println( "failed int_firstname_e" ); }
                            continue;
                        }
                    }

                    if( qs.use_mother ) {
                        if( qs.int_familyname_m > 0 ) {
                            if( ! isVariant( s1MotherFamName, s2MotherFamName, NameType.FAMILYNAME, qs.method ) )
                            {
                                matchesList.set( lv_idx, 0 );       // no match
                                if( debug ) { System.out.println( "failed int_familyname_m" ); }
                                continue;
                            }
                        }

                        if( qs.int_firstname_m > 0 ) {
                            if( ! checkFirstName( qs.firstname,
                                s1MotherFirName1, s1MotherFirName2, s1MotherFirName3, s1MotherFirName4,
                                s2MotherFirName1, s2MotherFirName2, s2MotherFirName3, s2MotherFirName4,
                                qs.method ) )
                            {
                                matchesList.set( lv_idx, 0 );       // no match
                                if( debug ) { System.out.println( "failed int_firstname_m" ); }
                                continue;
                            }
                        }
                    }

                    if( qs.use_father ) {
                        if( qs.int_familyname_f > 0 ) {
                            if( ! isVariant( s1FatherFamName, s2FatherFamName, NameType.FAMILYNAME, qs.method ) )
                            {
                                matchesList.set( lv_idx, 0 );       // no match
                                if( debug ) { System.out.println( "failed int_familyname_f" ); }
                                continue;
                            }
                        }

                        if( qs.int_firstname_f > 0 ) {
                            if( ! checkFirstName( qs.firstname,
                                s1FatherFirName1, s1FatherFirName2, s1FatherFirName3, s1FatherFirName4,
                                s2FatherFirName1, s2FatherFirName2, s2FatherFirName3, s2FatherFirName4,
                                qs.method ) )
                            {
                                matchesList.set( lv_idx, 0 );       // no match
                                if( debug ) { System.out.println( "failed int_firstname_f" ); }
                                continue;
                            }
                        }
                    }

                    if( qs.use_partner ) {
                        if( qs.int_familyname_p > 0 ) {
                            if( ! isVariant( s1PartnerFamName, s2PartnerFamName, NameType.FAMILYNAME, qs.method ) )
                            {
                                matchesList.set( lv_idx, 0 );       // no match
                                if( debug ) { System.out.println( "failed int_familyname_p" ); }
                                continue;
                            }
                        }

                        if( qs.int_firstname_p > 0 ) {
                            if( ! checkFirstName( qs.firstname,
                                s1PartnerFirName1, s1PartnerFirName2, s1PartnerFirName3, s1PartnerFirName4,
                                s2PartnerFirName1, s2PartnerFirName2, s2PartnerFirName3, s2PartnerFirName4,
                                qs.method ) )
                            {
                                matchesList.set( lv_idx, 0 );       // no match
                                if( debug ) { System.out.println( "failed int_firstname_p" ); }
                                continue;
                            }
                        }
                    }
                }
                //System.out.println( "variants done" );

                // entries of matchesList that have not been zeroed above imply a match
                for( int l = 0; l < matchesList.size(); l++ )
                {
                    if( matchesList.get( l ) != 0 ) {
                        nmatch++;

                        int id_s1 = ql.s1_id_base.get( s1_idx );
                        int id_s2 = ql.s2_id_base.get( matchesList.get( l ) );

                        String query = "INSERT INTO matches ( id_match_process , id_linksbase_1 , id_linksbase_2 ) " +
                            "VALUES ( " + mis.is.get(i).get(0).id + "," + id_s1 + "," + id_s2 + ")";

                        if( debug ) {
                            System.out.println( query );
                            plog.show( query );
                        }

                        conMatch.createStatement().execute( query );
                        conMatch.createStatement().close();
                    }
                }
                matchesList.clear();
            }

            pm.removeProcess();

            msg = String.format( "Number of matches: %d", nmatch );
            System.out.println( msg );
            plog.show( msg );

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
     * Query table ls_familyname for ego_familyname in name_int_1,
       to get the name_int_2 as levenshtein variants for the set s2.
     *
     * @param ego_familyname        // an ego familyname from the set s1
     * @param potentialMatches      // Levenshtein variants of ego familyname
     * @param LvPotentialMatches    // Levenshtein distances of potential matches
     */
    private void variantsToList( int ego_familyname, String ls_table, ArrayList< Integer > potentialMatches, ArrayList< Integer > LvPotentialMatches )
    {
        String url = "localhost";
        String usr = "links";
        String pwd = "mslinks";

        int Ldist = 1;

        try
        {
            if( debug ) { plog.show( "variantsToList(): ego_familyname = " + ego_familyname ); }

            Connection con = General.getConnection( url, "links_prematch", usr, pwd );
            con.setReadOnly( true );

            String query = "SELECT * FROM links_prematch." + ls_table + " WHERE value = 1 AND name_int_1 = " + ego_familyname ;
            ResultSet rs = con.createStatement().executeQuery( query );

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

                potentialMatches  .add( name_int_2 );
                LvPotentialMatches.add( Ldist );

                nrecs++;
            }
            if( debug && nrecs != 0 ) { System.out.println( "" ); }

            con.createStatement().close();
            con.close();
            con = null;
        }
        catch( Exception ex ) {
            System.out.println( "Exception in variantsToList: " + ex.getMessage() );
            System.out.println( "Abort" );
            System.exit( 1 );
        }
    } // variantsToList


     /**
     * Query table ls_firstname or ls_lastname for s1Name in name_int_1 column,
     * to get the name_int_2 as levenshtein variants for the set s2.
     * Compare the variants for a match with s2Name
     */
    //private void variantsFirstnameToList( int ego_firstname, ArrayList< Integer > potentialMatches, ArrayList< Integer > LvPotentialMatches )
    private boolean compareLSnames( int s1Name, int s2Name, String ls_table, int Ldist )
    {
        String url = "localhost";
        String usr = "links";
        String pwd = "mslinks";

        boolean match = false;

        try
        {
            if( debug ) {
                String msg = "compareLSnames(): s1Name = " + s1Name + ", s2Name = " + s2Name;
                System.out.println( msg );
                plog.show( msg );
            }

            Connection con = General.getConnection( url, "links_prematch", usr, pwd );
            con.setReadOnly( true );

            String query = "SELECT * FROM links_prematch." + ls_table + " WHERE value <= " + Ldist + " AND name_int_1 = " + s1Name ;
            ResultSet rs = con.createStatement().executeQuery( query );

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

            con.createStatement().close();
            con.close();
            con = null;
        }
        catch( Exception ex ) {
            System.out.println( "Exception in variantsToList: " + ex.getMessage() );
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
                if( useExactMatch ) {
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


    private boolean isVariant( int s1Name, int s2Name, NameType tnt, int method )
    {
        int Ldist = 1;      // should become a parameter

        // is s2Name a Levenshtein variant of s1Name ?

        if( debug ) {
            String msg = "isVariant() " + s1Name + ", " + s2Name + ", " + tnt + ", " + Ldist;
            System.out.println( msg );
            try { plog.show( msg ); }
            catch( Exception ex ) { System.out.println( ex.getMessage() ); }
        }

        if( method == 0 )
        {
            if( tnt == NameType.FAMILYNAME )
            {
                return compareLSnames( s1Name, s2Name, "ls_familyname", Ldist );

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

                return compareLSnames( s1Name, s2Name, "ls_firstname", Ldist );

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
        int method              // 'method' in match_process table: {0,1}
    )
    {
        if( debug ) {
            System.out.println( "checkFirstName() fn_method: " + fn_method );
            try { plog.show( "checkFirstName() fn_method = " + fn_method ); }
            catch( Exception ex ) { System.out.println( ex.getMessage() ); }
        }

        if( fn_method == 1 )
        {
            if( ! isVariant( s1Name1, s2Name1, NameType.FIRSTNAME, method ) ||
                ! isVariant( s1Name2, s2Name2, NameType.FIRSTNAME, method ) ||
                ! isVariant( s1Name3, s2Name3, NameType.FIRSTNAME, method ) ||
                ! isVariant( s1Name4, s2Name4, NameType.FIRSTNAME, method ) )
            { return false; }
        }
        else if( fn_method == 2 )
        {
            if( ! isVariant( s1Name1, s2Name1, NameType.FIRSTNAME, method ) )
            { return false; }
        }
        else if( fn_method == 3 )
        {
            if( ! isVariant( s1Name1, s2Name1, NameType.FIRSTNAME, method ) ||
                ! isVariant( s1Name2, s2Name2, NameType.FIRSTNAME, method ) )
            { return false; }
        }
        else if( fn_method == 4 )
        {
            if( ! isVariant( s1Name1, s2Name1, NameType.FIRSTNAME, method ) ||
                ! isVariant( s1Name2, s2Name2, NameType.FIRSTNAME, method ) ||
                ! isVariant( s1Name3, s2Name3, NameType.FIRSTNAME, method ) )
            { return false; }
        }
        else if( fn_method == 5 )
        {
            if( isVariant( s1Name1, s2Name1, NameType.FIRSTNAME, method ) ||
                isVariant( s1Name1, s2Name2, NameType.FIRSTNAME, method ) ||
                isVariant( s1Name1, s2Name3, NameType.FIRSTNAME, method ) ||
                isVariant( s1Name1, s2Name4, NameType.FIRSTNAME, method ) ||
                isVariant( s2Name1, s1Name2, NameType.FIRSTNAME, method ) ||
                isVariant( s2Name1, s1Name3, NameType.FIRSTNAME, method ) ||
                isVariant( s2Name1, s1Name4, NameType.FIRSTNAME, method ) )
            { ; }
            else { return false; }
        }
        return true;
    } // checkFirstName

}

