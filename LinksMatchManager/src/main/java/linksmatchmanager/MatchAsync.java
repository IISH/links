package linksmatchmanager;

import java.sql.Connection;

import java.util.Arrays;
import java.util.ArrayList;

import linksmatchmanager.DataSet.QuerySet;
import linksmatchmanager.DataSet.NameType;
import linksmatchmanager.DataSet.QueryGroupSet;

/**
 * @author Omar Azouguagh
 * @author Fons Laan
 *
 * <p/>
 * FL-11-Dec-2014 Latest change
 */

public class MatchAsync extends Thread
{
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

    boolean isUseRoot = false;


    public MatchAsync
    (
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
        this.pm = pm;

        this.i = i;
        this.j = j;

        this.ql = ql;
        this.plog = plog;

        this.qgs = qgs;
        this.mis = mis;

        this.conPrematch = conPrematch;
        this.conMatch = conMatch;

        this.variantFirstName = variantFirstName;
        this.variantFamilyName = variantFamilyName;
    }


    public MatchAsync(
        ProcessManager pm,
        int i,
        int j,
        QueryLoader ql,
        PrintLogger log,
        QueryGroupSet qgs,
        QueryGenerator mis,
        Connection conPrematch,
        Connection conMatch,
        int[][] rootFirstName,
        int[][] rootFamilyName,

        boolean root
    )
    {
        this.pm = pm;
        this.i = i;
        this.j = j;
        this.ql = ql;
        this.plog = plog;
        this.qgs = qgs;
        this.mis = mis;
        this.conPrematch = conPrematch;
        this.conMatch = conMatch;
        this.rootFirstName = rootFirstName;
        this.rootFamilyName = rootFamilyName;

        isUseRoot = true;
    }

    @Override
    public void run()
    {
        int ka = 0;
        int ix = 0;

        try
        {
            long threadId = Thread.currentThread().getId();
            String msg = String.format( "\nMatchAsync/run(): thread id %d running", threadId );
            System.out.println( msg );
            plog.show( msg );

            msg = String.format( "Thread id %d; Range %d of %d", threadId, (j + 1), qgs.getSize() );
            System.out.println( msg );
            plog.show( msg );

            // Get a QuerySet object. This object contains all data about a certain query/subquery
            QuerySet qs = qgs.get( j );

            // Create new instance of queryloader. Queryloader is used to use the queries to load data into the sets.
            // It input is a QuerySet and a database connection object.
            ql = new QueryLoader( qs, conPrematch );

            // Last familyname, initial is 0. Because the familynames are ordered the calculation of the potential
            // matches is done once, only the first time.
            int lastFamilyName = 0;

            // Create list with potential matches for a familynames
            ArrayList<Integer> potentialMatches = new ArrayList<Integer>();

            // Loop through set 1
            msg = String.format( "Thread id %d; Set 1 size: %d", threadId, ql.s1_id_base.size() );
            System.out.println( msg );
            plog.show(msg);

            int nmatch = 0;

            for( int k = 0; k < ql.s1_id_base.size(); k++ )
            {
                ka = k;
                ix = 0;

                // The order of the steps has to be dynamic
                // Now it starts with the check of the familyname

                // Get familyname of Set 1
                int s1EgoFamName = ql.s1_ego_familyname.get( k );

                // If the familyname is not the same, then build up the list. Otherwise go on
                if( s1EgoFamName != lastFamilyName )
                {
                    lastFamilyName = s1EgoFamName;                      // Set previous name

                    potentialMatches.clear();                           // Empty the list

                    variantsToList( s1EgoFamName, potentialMatches );   // Load the potential variants
                }

                // Copy the existing variantList to working copy
                ArrayList<Integer> tempVarList = new ArrayList<Integer>();

                tempVarList.addAll( potentialMatches );

                // Get the lowest to greatest frequency of the names
                // TODO: make it dynamic

                // Now test the names with the lowest frequency
                // s1EgoFamName -> Already loaded
                int s1MotherFamName  = ql.s1_mother_familyname.get( k );
                int s1FatherFamName  = ql.s1_father_familyname.get( k );
                int s1PartnerFamName = ql.s1_partner_familyname.get( k );

                // First name 1
                int s1EgoFirName1     = ql.s1_ego_firstname1.get( k );
                int s1MotherFirName1  = ql.s1_mother_firstname1.get( k );
                int s1FatherFirName1  = ql.s1_father_firstname1.get( k );
                int s1PartnerFirName1 = ql.s1_partner_firstname1.get( k );

                // First name 2
                int s1EgoFirName2     = ql.s1_ego_firstname2.get( k );
                int s1MotherFirName2  = ql.s1_mother_firstname2.get( k );
                int s1FatherFirName2  = ql.s1_father_firstname2.get( k );
                int s1PartnerFirName2 = ql.s1_partner_firstname2.get( k );

                // first name 3
                int s1EgoFirName3     = ql.s1_ego_firstname3.get( k );
                int s1MotherFirName3  = ql.s1_mother_firstname3.get( k );
                int s1FatherFirName3  = ql.s1_father_firstname3.get( k );
                int s1PartnerFirName3 = ql.s1_partner_firstname3.get( k );

                // first name 4
                int s1EgoFirName4     = ql.s1_ego_firstname4.get( k );
                int s1MotherFirName4  = ql.s1_mother_firstname4.get( k );
                int s1FatherFirName4  = ql.s1_father_firstname4.get( k );
                int s1PartnerFirName4 = ql.s1_partner_firstname4.get( k );

                // loop through set 2
                // Visit only the id in variantList
                for( int idIndex = 0; idIndex < tempVarList.size(); idIndex++ )
                {
                    ix = idIndex;

                    if( k == 3 && ix == 0 ) { int g = 0; }

                    int index = tempVarList.get( idIndex );

                    // Check min max; use new min max
                    if( !qs.ignore_minmax ) {
                        if( !CheckMinMax( qs, k, index ) ) {
                            tempVarList.set( idIndex, 0 );    // Go to next person in Set 2
                            continue;
                        }
                    }

                    // Check sex
                    if( !qs.ignore_sex ) {
                        int s1s = ql.s1_sex.get( k );
                        int s2s = ql.s2_sex.get( index );
                        // Empty sex is denied
                        if( s1s != 0 && s2s != 0 && ( s1s != s2s ) ) {
                            tempVarList.set( idIndex, 0 );
                            continue;
                        }
                    }

                    // Get all names
                    //  s2EgoFamName already used
                    int s2MotherFamName  = ql.s2_mother_familyname.get( index );
                    int s2FatherFamName  = ql.s2_father_familyname.get( index );
                    int s2PartnerFamName = ql.s2_partner_familyname.get( index );

                    // First name 1
                    int s2EgoFirName1     = ql.s2_ego_firstname1.get( index );
                    int s2MotherFirName1  = ql.s2_mother_firstname1.get( index );
                    int s2FatherFirName1  = ql.s2_father_firstname1.get( index );
                    int s2PartnerFirName1 = ql.s2_partner_firstname1.get( index );

                    // First name 2
                    int s2EgoFirName2     = ql.s2_ego_firstname2.get( index );
                    int s2MotherFirName2  = ql.s2_mother_firstname2.get( index );
                    int s2FatherFirName2  = ql.s2_father_firstname2.get( index );
                    int s2PartnerFirName2 = ql.s2_partner_firstname2.get( index );

                    // first name 3
                    int s2EgoFirName3     = ql.s2_ego_firstname3.get( index );
                    int s2MotherFirName3  = ql.s2_mother_firstname3.get( index );
                    int s2FatherFirName3  = ql.s2_father_firstname3.get( index );
                    int s2PartnerFirName3 = ql.s2_partner_firstname3.get( index );

                    // first name 4
                    int s2EgoFirName4     = ql.s2_ego_firstname4.get( index );
                    int s2MotherFirName4  = ql.s2_mother_firstname4.get( index );
                    int s2FatherFirName4  = ql.s2_father_firstname4.get( index );
                    int s2PartnerFirName4 = ql.s2_partner_firstname4.get( index );

                    // Check the firstnames of ego
                    if( qs.int_firstname_e > 0 ) {
                        if( !checkFirstName( qs.firstname,
                                s1EgoFirName1, s1EgoFirName2, s1EgoFirName3, s1EgoFirName4,
                                s2EgoFirName1, s2EgoFirName2, s2EgoFirName3, s2EgoFirName4,
                                qs.method ) ) {
                            tempVarList.set( idIndex, 0 );
                            continue;
                        }
                    }

                    if( qs.use_mother ) {
                        if( qs.int_familyname_m > 0 ) {
                            if( !isVariant( s1MotherFamName, s2MotherFamName, NameType.FAMILYNAME, qs.method ) ) {
                                tempVarList.set( idIndex, 0 ); // set 0
                                continue;
                            }
                        }
                        if( qs.int_firstname_m > 0 ) {
                            if ( !checkFirstName( qs.firstname,
                                    s1MotherFirName1, s1MotherFirName2, s1MotherFirName3, s1MotherFirName4,
                                    s2MotherFirName1, s2MotherFirName2, s2MotherFirName3, s2MotherFirName4,
                                    qs.method ) ) {
                                tempVarList.set( idIndex, 0 );
                                continue;
                            }
                        }
                    }

                    if( qs.use_father ) {
                        if( qs.int_familyname_f > 0 ) {
                            if( !isVariant(s1FatherFamName, s2FatherFamName, NameType.FAMILYNAME, qs.method ) ) {
                                tempVarList.set( idIndex, 0 ); // set 0
                                continue;
                            }
                        }
                        if( qs.int_firstname_f > 0 ) {
                            if( !checkFirstName( qs.firstname,
                                    s1FatherFirName1, s1FatherFirName2, s1FatherFirName3, s1FatherFirName4,
                                    s2FatherFirName1, s2FatherFirName2, s2FatherFirName3, s2FatherFirName4,
                                    qs.method ) ) {
                                tempVarList.set( idIndex, 0 );
                                continue;
                            }
                        }
                    }

                    if( qs.use_partner ) {
                        if( qs.int_familyname_p > 0 ) {
                            if( !isVariant(s1PartnerFamName, s2PartnerFamName, NameType.FAMILYNAME, qs.method ) ) {
                                tempVarList.set( idIndex, 0 ); // set 0
                                continue;
                            }
                        }
                        if( qs.int_firstname_p > 0 ) {
                            if( !checkFirstName( qs.firstname,
                                    s1PartnerFirName1, s1PartnerFirName2, s1PartnerFirName3, s1PartnerFirName4,
                                    s2PartnerFirName1, s2PartnerFirName2, s2PartnerFirName3, s2PartnerFirName4,
                                    qs.method ) ) {
                                tempVarList.set( idIndex, 0 );
                                continue;
                            }
                        }
                    }
                }

                for( int l = 0; l < tempVarList.size(); l++ )
                {
                    if( tempVarList.get( l ) != 0 ) {
                        nmatch++;

                        int id_s1 = ql.s1_id_base.get( k );
                        int id_s2 = ql.s2_id_base.get( tempVarList.get( l ) );

                        String query = "INSERT INTO matches ( id_match_process , id_linksbase_1 , id_linksbase_2 ) VALUES (" + mis.is.get(i).get(0).id + "," + id_s1 + "," + id_s2 + ")";
                        //System.out.println( query );
                        //plog.show(query);

                        conMatch.createStatement().execute( query );
                        conMatch.createStatement().close();
                    }
                }
                tempVarList.clear();
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
        catch( Exception ex1 ) {
            pm.removeProcess();

            try { plog.show( "Thread Error: where= " + ka + "-----" + ix + " error=" + ex1.getMessage() ); }
            catch( Exception ex2 ) { ex2.printStackTrace(); }
        }
    }


    /**
     * 
     * @param fn
     * @param potentialMatches 
     */
    private void variantsToList(int fn,
            ArrayList<Integer> potentialMatches) {

        /**
         * Loop through whole set to getall id with 
         * names which are a variant of this name
         */
        for (int l = 0; l < ql.s2_ego_familyname.size(); l++) {

            // Use binary search to Check if this name is variant of
            if (fn == ql.s2_ego_familyname.get(l)) {
                // Add ID of name to list
                potentialMatches.add(l);
                continue;
            }

            // Do the search
            if (isUseRoot) {

                if (fn >= rootFamilyName.length) {
                    return;
                }

                if (ql.s2_ego_familyname.get(l) >= rootFamilyName.length) {
                    continue;
                }

                int[] root1 = rootFamilyName[fn];
                int[] root2 = rootFamilyName[ql.s2_ego_familyname.get(l)];

                for (int i = 0; i < root1.length; i++) {
                    for (int j = 0; j < root2.length; j++) {
                        if (root1[i] == root2[j]) {
                            potentialMatches.add(l);
                            break;
                        }
                    }
                    break;
                }
            } else {

                int large;
                int small;

                if (fn > ql.s2_ego_familyname.get(l)) {
                    large = fn;
                    small = ql.s2_ego_familyname.get(l);
                } else {
                    large = ql.s2_ego_familyname.get(l);
                    small = fn;
                }

                if (variantFamilyName[small] != null
                        && (Arrays.binarySearch(variantFamilyName[small], large) > -1)) {
                    potentialMatches.add(l);
                }
            }
        }
    }


    private boolean CheckMinMax(QuerySet qs, int k, int index) {
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
    }


    private static boolean firstGreater(int first, int second) {

        if (first == 0 || second == 0) {
            return false;
        }
        if (first > second) {
            return true;
        }
        return false;
    }


    private boolean isVariant(int basicName, int seconName, NameType tnt, int method) {
        if (method == 0) {
            if (tnt == NameType.FAMILYNAME) {
                if (basicName == 0 || seconName == 0) {
                    return false;
                } else if (basicName == seconName) {
                } else if (variantFamilyName.length > basicName && variantFamilyName[basicName] != null && Arrays.binarySearch(variantFamilyName[basicName], seconName) > -1) {
                } else if (variantFamilyName.length > seconName && variantFamilyName[seconName] != null && Arrays.binarySearch(variantFamilyName[seconName], basicName) > -1) {
                } else {
                    return false;
                }

            } else if (tnt == NameType.FIRSTNAME) {
                if (basicName == 0 || seconName == 0) {
                    return false;
                } else if (basicName == seconName) {
                } else if (variantFirstName.length > basicName && variantFirstName[basicName] != null && Arrays.binarySearch(variantFirstName[basicName], seconName) > -1) {
                } else if (variantFirstName.length > seconName && variantFirstName[seconName] != null && Arrays.binarySearch(variantFirstName[seconName], basicName) > -1) {
                } else {
                    return false;
                }
            }

            return true;
        } else if (method == 1) {

            if (basicName == seconName) {
                return true;
            }

            int[] root1;
            int[] root2;

            if (tnt == NameType.FAMILYNAME) {

                if ((basicName >= rootFamilyName.length) || ((seconName >= rootFamilyName.length))) {
                    return false;
                }

                root1 = this.rootFamilyName[basicName];
                root2 = this.rootFamilyName[seconName];

            } else {

                if ((basicName >= rootFirstName.length) || ((seconName >= rootFirstName.length))) {
                    return false;
                }

                root1 = this.rootFirstName[basicName];
                root2 = this.rootFirstName[seconName];
            }

            for (int i = 0; i < root1.length; i++) {
                for (int j = 0; j < root2.length; j++) {
                    if (root1[i] == root2[j]) {
                        return true;
                    }
                }
                break;
            }
            return false;
        } else {
            return false;
        }
    }


    private boolean checkFirstName(int fn,
            int s1Name1, int s1Name2, int s1Name3, int s1Name4,
            int s2Name1, int s2Name2, int s2Name3, int s2Name4,
            int method) {

        if (fn == 1) {
            if (!isVariant(s1Name1, s2Name1, NameType.FIRSTNAME, method)
                    || !isVariant(s1Name2, s2Name2, NameType.FIRSTNAME, method)
                    || !isVariant(s1Name3, s2Name3, NameType.FIRSTNAME, method)
                    || !isVariant(s1Name4, s2Name4, NameType.FIRSTNAME, method)) {
                return false;
            }
        } else if (fn == 2) {
            if (!isVariant(s1Name1, s2Name1, NameType.FIRSTNAME, method)) {
                return false;
            }
        } else if (fn == 3) {
            if (!isVariant(s1Name1, s2Name1, NameType.FIRSTNAME, method)
                    || !isVariant(s1Name2, s2Name2, NameType.FIRSTNAME, method)) {
                return false;
            }
        } else if (fn == 4) {
            if (!isVariant(fn, s2Name1, NameType.FIRSTNAME, method)
                    || !isVariant(s1Name2, s2Name2, NameType.FIRSTNAME, method)
                    || !isVariant(s1Name3, s2Name3, NameType.FIRSTNAME, method)) {
                return false;
            }
        } else if (fn == 5) {
            if (isVariant(s1Name1, s2Name1, NameType.FIRSTNAME, method)
                    || isVariant(s1Name1, s2Name2, NameType.FIRSTNAME, method)
                    || isVariant(s1Name1, s2Name3, NameType.FIRSTNAME, method)
                    || isVariant(s1Name1, s2Name4, NameType.FIRSTNAME, method)
                    || isVariant(s2Name1, s1Name2, NameType.FIRSTNAME, method)
                    || isVariant(s2Name1, s1Name3, NameType.FIRSTNAME, method)
                    || isVariant(s2Name1, s1Name4, NameType.FIRSTNAME, method)) {
            } else {
                return false;
            }
        }
        return true;
    }
}
