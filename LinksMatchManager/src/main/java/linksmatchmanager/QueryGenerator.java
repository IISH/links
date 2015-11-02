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

import java.sql.*;
import java.util.*;

import linksmatchmanager.DataSet.QuerySet;
import linksmatchmanager.DataSet.QueryGroupSet;
import linksmatchmanager.DataSet.InputSet;

/**
 * This Class generates queries that can be used to get sets to match
 * @author Omar Azouguagh
 * @author Fons Laan
 *
 * FL-30-Jun-2014 Imported from OA backup
 * FL-13-Feb-2015 Do not retrieve NULL names from links_base
 * FL-02-Nov-2015 Add maintype to QuerySet
 * FL-02-Nov-2015 Latest change
 */
public class QueryGenerator
{
    public InputSet is;     // this variable can be used outside the class

    private boolean debug = false;
    private PrintLogger plog;
    private ResultSet rs;

    /**
     * Constructor
     * @param plog
     * @param dbconMatch
     * @throws Exception
     *
     * There is just one QueryGenerator containing all input variables, plus resultSet from match_process.
     * So it could/should have been a singleton object.
     */
    public QueryGenerator( PrintLogger plog, Connection dbconMatch ) throws Exception
    {
        this.plog = plog;

        // Get all records and fields from the match_process table
        rs = dbconMatch.createStatement().executeQuery( "SELECT * FROM match_process ORDER BY id" );

        is = new InputSet();

        setToArray();   // fill 1 or more QueryGroupSets, and add them to the InputSet 'is'
    }


    /**
     * @return String with al the generated queries  
     */
    public String printToString()
    {
        String returnValue = "";

        // count is
        returnValue += "Number of records to match: [" + is.getSize() + "]" + "\r\n";

        for (int i = 0; i < is.getSize(); i++) {

            QueryGroupSet iQgs = is.get(i);

            returnValue += "\r\n" + "\t" + "Record no [" + (i + 1) + "] contains [" + iQgs.getSize() + "] query pairs" + "\r\n";

            for (int j = 0; j < iQgs.getSize(); j++) {

                QuerySet iQs = iQgs.get(j);

                returnValue += "\t" + "\t" + "Query Pair no [" + (j + 1) + "] :" + "\r\n";

                returnValue += "\t" + "\t" + "Query 1: " + iQs.query1 + "\r\n";

                returnValue += "\t" + "\t" + "Query 2: " + iQs.query2 + "\r\n";
            }
        }
        return returnValue;
    }


    /**
     * Creates an array of queries
     * @throws Exception When a database operation fails
     */
    private void setToArray() throws Exception
    {
        if( debug ) { System.out.println( "QueryGenerator/setToArray()" ); }

        int nline   = 0;
        int nline_y = 0;
        int nline_n = 0;

        // Process the records from the match_process table
        while( rs.next() )
        {
            nline++;
            String match = rs.getString( "match" );

            if( ! match.equalsIgnoreCase( "y" ) ) {
                nline_n++;
                continue;       // skip records that have 'match' set to 'n'
            }
            nline_y++;

            // get all the fields from the table match_process
            int id = rs.getInt( "id" );
            if( debug ) { System.out.println( "match_process id: " + id ); }

            int s1_maintype = rs.getInt( "s1_maintype" );
            int s2_maintype = rs.getInt( "s2_maintype" );

            String s1_type = rs.getString( "s1_type" ) != null ? rs.getString( "s1_type" ) : "";
            String s2_type = rs.getString( "s2_type" ) != null ? rs.getString( "s2_type" ) : "";

            String s1_role_ego = rs.getString( "s1_role_ego" ) != null ? rs.getString( "s1_role_ego" ) : "";
            String s2_role_ego = rs.getString( "s2_role_ego" ) != null ? rs.getString( "s2_role_ego" ) : "";

            int s1_startyear = rs.getInt( "s1_startyear" );
            int s1_range     = rs.getInt( "s1_range" );
            int s1_endyear   = rs.getInt( "s1_endyear" );
            int s2_startyear = rs.getInt( "s2_startyear" );
            int s2_range     = rs.getInt( "s2_range" );

            if( debug ) {
                System.out.println( "s1_startyear: " + s1_startyear );
                System.out.println( "s2_startyear: " + s2_startyear );
                System.out.println( "s1_endyear: " + s1_endyear );
                System.out.println( "s1_range: " + s1_range );
                System.out.println( "s2_range: " + s2_range );
            }

            String s1_source = rs.getString( "s1_source" ) != null ? rs.getString( "s1_source" ) : "";
            String s2_source = rs.getString( "s2_source" ) != null ? rs.getString( "s2_source" ) : "";

            int method = rs.getInt( "method" );
            String ignore_sex = rs.getString( "ignore_sex" ) != null ? rs.getString( "ignore_sex" ) : "";

            // set to "n" below
            // String ignore_minmax = rs.getString( "ignore_minmax" ) != null ? rs.getString( "ignore_minmax" ) : "";

            int firstname = rs.getInt( "firstname" );
            String prematch_familyname    = rs.getString( "prematch_familyname" ) != null ? rs.getString( "prematch_familyname" ) : "";
            int prematch_familyname_value = rs.getInt( "prematch_familyname_value" );
            String prematch_firstname     = rs.getString( "prematch_firstname" ) != null ? rs.getString( "prematch_firstname" ) : "";
            int prematch_firstname_value  = rs.getInt( "prematch_firstname_value" );

            //new fields
            String use_familyname = rs.getString( "use_familyname" ) != null ? rs.getString( "use_familyname" ) : "";
            String use_firstname  = rs.getString( "use_firstname" )  != null ? rs.getString( "use_firstname" )  : "";
            String use_minmax     = rs.getString( "use_minmax" )     != null ? rs.getString( "use_minmax" )     : "";

            // divide to strings
            int int_familyname_e = Integer.parseInt( use_familyname.substring( 0, 1 ) );
            int int_familyname_m = Integer.parseInt( use_familyname.substring( 1, 2 ) );
            int int_familyname_f = Integer.parseInt( use_familyname.substring( 2, 3 ) );
            int int_familyname_p = Integer.parseInt( use_familyname.substring( 3, 4 ) );

            int int_firstname_e = Integer.parseInt(use_firstname.substring( 0, 1 ) );
            int int_firstname_m = Integer.parseInt(use_firstname.substring( 1, 2 ) );
            int int_firstname_f = Integer.parseInt(use_firstname.substring( 2, 3 ) );
            int int_firstname_p = Integer.parseInt(use_firstname.substring( 3, 4 ) );

            int int_minmax_e = Integer.parseInt( use_minmax.split( "\\." )[ 0 ] );
            int int_minmax_m = Integer.parseInt( use_minmax.split( "\\." )[ 1 ] );
            int int_minmax_f = Integer.parseInt( use_minmax.split( "\\." )[ 2 ] );
            int int_minmax_p = Integer.parseInt( use_minmax.split( "\\." )[ 3 ] );

            String use_mother  = "n";
            String use_father  = "n";
            String use_partner = "n";

            String ignore_minmax = "n";

            // use ego is always true
            if( int_familyname_m > 0 || int_firstname_m > 0 || int_minmax_m > 0 ) {
                use_mother = "y";
            }

            if( int_familyname_f > 0 || int_firstname_f > 0 || int_minmax_f > 0 ) {
                use_father = "y";
            }

            if( int_familyname_p > 0 || int_firstname_p > 0 || int_minmax_p > 0 ) {
                use_partner = "y";
            }

            // use min max
            if( (int_minmax_e + int_minmax_m + int_minmax_f + int_minmax_p ) == 0 ) {
                ignore_minmax = "y";
            }

            // create group
            QueryGroupSet qgs = new QueryGroupSet();

            boolean loop = true;
            int counter = 0;


            // Do extra checks for empty end and start years
            boolean once = false;
            if( s1_range == 0 ) {
                s1_range = s1_endyear - s1_startyear;
                once = true;
            }

            // FL-02-Mar-2015
            // If 'once' is true, the while loop is executed only once, and the InputSet will contain just
            // one QueryGroupSet. With a given s1_range > 0 from the match_process table, multiple QueryGroupSets
            // will be generated. The variables in those QueryGroupSets will be mostly the same; the difference
            // lies in the values for s1_days, s2_days and s1_range.
            while( loop )
            {
                // collect the above variables in a QuerySet object
                QuerySet qs = new QuerySet();

                qs.s1_maintype = s1_maintype;
                qs.s2_maintype = s2_maintype;

                qs.int_familyname_e = int_familyname_e;
                qs.int_familyname_m = int_familyname_m;
                qs.int_familyname_f = int_familyname_f;
                qs.int_familyname_p = int_familyname_p;

                qs.int_firstname_e = int_firstname_e;
                qs.int_firstname_m = int_firstname_m;
                qs.int_firstname_f = int_firstname_f;
                qs.int_firstname_p = int_firstname_p;

                qs.int_minmax_e = int_minmax_e;
                qs.int_minmax_m = int_minmax_m;
                qs.int_minmax_f = int_minmax_f;
                qs.int_minmax_p = int_minmax_p;

                // booleans
                qs.use_mother    = use_mother   .equalsIgnoreCase( "y" ) ? true : false;
                qs.use_father    = use_father   .equalsIgnoreCase( "y" ) ? true : false;
                qs.use_partner   = use_partner  .equalsIgnoreCase( "y" ) ? true : false;
                qs.ignore_minmax = ignore_minmax.equalsIgnoreCase( "y" ) ? true : false;
                qs.ignore_sex    = ignore_sex   .equalsIgnoreCase( "y" ) ? true : false;

                qs.firstname = firstname;

                qs.id = id;

                qs.method = method;

                qs.prematch_familyname       = prematch_familyname;
                qs.prematch_familyname_value = prematch_familyname_value;
                qs.prematch_firstname        = prematch_firstname;
                qs.prematch_firstname_value  = prematch_firstname_value;

                //new
                qs.use_mother  = use_mother .equalsIgnoreCase( "y" ) ? true : false;
                qs.use_father  = use_father .equalsIgnoreCase( "y" ) ? true : false;
                qs.use_partner = use_partner.equalsIgnoreCase( "y" ) ? true : false;

                // Initial part of query to get the data from links_base
                qs.query1 = getSelectQuery( qs.use_mother, qs.use_father, qs.use_partner, qs.ignore_minmax, qs.firstname );
                qs.query2 = qs.query1;

                if( !qs.ignore_sex ) {
                    qs.query1 += ", ego_sex ";
                    qs.query2 += ", ego_sex ";
                }

                // FROM
                qs.query1 += "FROM links_base ";
                qs.query2 += "FROM links_base ";

                // FL-13-Feb-2015, suppress empty names, if used
                String notzero = "";
                if( int_familyname_e == 1 ) { notzero += "WHERE ego_familyname <> 0 ";  }

                if( int_familyname_m == 1 ) {
                    if( notzero.isEmpty() ) { notzero += "WHERE mother_familyname <> 0 "; }
                    else                    { notzero +=   "AND mother_familyname <> 0 "; }
                }

                if( int_familyname_f == 1 ) {
                    if( notzero.isEmpty() ) { notzero += "WHERE father_familyname <> 0 "; }
                    else                    { notzero +=   "AND father_familyname <> 0 "; }
                }

                if( int_familyname_p == 1 ) {
                    if( notzero.isEmpty() ) { notzero += "WHERE partner_familyname <> 0 "; }
                    else                    { notzero +=   "AND partner_familyname <> 0 "; }
                }


                // for the time being, only firstname1
                if( int_firstname_e == 1 ) {
                    if( notzero.isEmpty() ) { notzero += "WHERE ego_firstname1 <> 0 ";  }
                    else                    { notzero +=   "AND ego_firstname1 <> 0 ";  }
                }

                if( int_firstname_m == 1 ) {
                    if( notzero.isEmpty() ) { notzero += "WHERE mother_firstname1 <> 0 "; }
                    else                    { notzero +=   "AND mother_firstname1 <> 0 "; }
                }

                if( int_firstname_f == 1 ) {
                    if( notzero.isEmpty() ) { notzero += "WHERE father_firstname1 <> 0 "; }
                    else                    { notzero +=   "AND father_firstname1 <> 0 "; }
                }

                if( int_firstname_p == 1 ) {
                    if( notzero.isEmpty() ) { notzero += "WHERE partner_firstname1 <> 0 "; }
                    else                    { notzero +=   "AND partner_firstname1 <> 0 "; }
                }


                qs.query1 += notzero;
                qs.query2 += notzero;


                // AND
                if( ! s1_role_ego.isEmpty()
                    || ! s2_role_ego.isEmpty()
                    ||   s1_maintype != 0
                    ||   s2_maintype != 0
                    || ! s1_source.isEmpty()
                    || ! s2_source.isEmpty()
                    ||   s1_startyear != 0
                )
                {
                    if( notzero.isEmpty() ) {
                        qs.query1 += "WHERE ";
                        qs.query2 += "WHERE ";
                    }
                    else {
                        qs.query1 += "AND ";
                        qs.query2 += "AND ";
                    }
                }

                // ego role
                //if (s1_role_ego != 0) {
                //    qs.query1 += "ego_role = " + s1_role_ego + " AND ";
                //}
                //if (s2_role_ego != 0) {
                //    qs.query2 += "ego_role = " + s2_role_ego + " AND ";
                //}

                // Ego s1 role
                if( !s1_role_ego.isEmpty() )
                {
                    // where string 
                    String s1_role_ego_where = "";

                    if( s1_role_ego.contains( "," ) )
                    {
                        s1_role_ego_where = "(";

                        for( String s : s1_role_ego.split( "," ) ) {
                            s1_role_ego_where += "OR ego_role = " + s + " ";
                        }

                        // replace first OR
                        s1_role_ego_where = s1_role_ego_where.replaceFirst( "OR", "" );

                        s1_role_ego_where += ")";

                    }
                    else { s1_role_ego_where += "ego_role = " + s1_role_ego; }

                    qs.query1 += s1_role_ego_where + " AND ";
                }

                // Ego s2 role
                if( !s2_role_ego.isEmpty() )
                {
                    // where string 
                    String s2_role_ego_where = "";

                    if( s2_role_ego.contains( "," ) )
                    {

                        s2_role_ego_where = "(";

                        for( String s : s2_role_ego.split( "," ) ) {
                            s2_role_ego_where += "OR ego_role = " + s + " ";
                        }

                        // replace first OR
                        s2_role_ego_where = s2_role_ego_where.replaceFirst( "OR", "" );

                        s2_role_ego_where += ")";

                    }
                    else { s2_role_ego_where += "ego_role = " + s2_role_ego; }

                    qs.query2 += s2_role_ego_where + " AND ";
                }

                // Main type
                if( s1_maintype != 0 ) {
                    qs.query1 += "registration_maintype = " + s1_maintype + " AND ";
                }
                if( s2_maintype != 0 ) {
                    qs.query2 += "registration_maintype = " + s2_maintype + " AND ";
                }

                // Type
                if( !s1_type.isEmpty() ) {
                    qs.query1 += "registration_type = '" + s1_type + "' AND ";
                }
                if( !s2_type.isEmpty() ) {
                    qs.query2 += "registration_type = '" + s2_type + "' AND ";
                }

                // ID source
                if( !s1_source.isEmpty() && !s1_source.equals( "0" ) )
                {
                    // where string 
                    String s1_source_where = "";

                    if( s1_source.contains( "," ) ) {
                        s1_source_where = "(";
                        for( String s : s1_source.split( "," ) ) {
                            s1_source_where += "OR id_source = " + s + " ";
                        }

                        // replace first OR
                        s1_source_where = s1_source_where.replaceFirst( "OR", "" );

                        s1_source_where += ") ";
                    }
                    else { s1_source_where += " id_source = " + s1_source; }

                    qs.query1 += s1_source_where + " AND ";
                }

                // ID source
                if( !s2_source.isEmpty() && !s2_source.equals( "0" ) )
                {
                    // where string 
                    String s2_source_where = "";

                    if( s2_source.contains( "," ) ) {

                        s2_source_where = "(";

                        for( String s : s2_source.split( "," ) ) {
                            s2_source_where += "OR id_source = " + s + " ";
                        }

                        // replace first OR
                        s2_source_where = s2_source_where.replaceFirst( "OR", "" );

                        s2_source_where += ") ";
                    }
                    else { s2_source_where += " id_source = " + s2_source; }

                    qs.query2 += s2_source_where + " AND ";
                }

                // begin registration days
                if( s1_startyear != 0 ) {
                    int s1_days_low = daysSinceBegin( s1_startyear + (counter * s1_range), 1, 1 );
                    int s2_days_low = daysSinceBegin( s2_startyear + (counter * s1_range), 1, 1 );

                    if( s1_days_low > 0 ) {
                        qs.query1 += "registration_days >= " + s1_days_low + " AND ";
                        qs.s1_days_low = s1_days_low;
                        if( debug ) { System.out.println( String.format( "counter: %d, s1 registration_days >= %d", counter, s1_days_low ) ); }
                    }

                    if( s2_days_low > 0 ) {
                        qs.query2 += "registration_days >= " + s2_days_low + " AND ";
                        qs.s2_days_low = s2_days_low;
                        if( debug ) { System.out.println( String.format( "counter: %d, s2 registration_days >= %d", counter, s2_days_low ) ); }
                    }
                }

                // end registration days
                int s1_days_high = 0;
                int s2_days_high = 0;

                if( s1_startyear > 0 )
                {
                    if( once ) {
                        s1_days_high = daysSinceBegin( s1_endyear, 12, 31 );
                    } else {
                        s1_days_high = daysSinceBegin( s1_startyear + ( (counter + 1) * s1_range ), 1, 1 ) - 1;
                    }

                    if( s1_days_high >= daysSinceBegin( s1_endyear, 12, 31 ) ) {
                         s1_days_high = daysSinceBegin( s1_endyear, 12, 31 );

                        loop = false;
                    }

                    if( s1_days_high > 0 ) {
                        qs.query1 += "registration_days <= " + s1_days_high + " AND ";
                        qs.s1_days_high = s1_days_high;
                        if( debug ) { System.out.println( String.format( "counter: %d, s1 registration_days <= %d", counter, s1_days_high ) ); }
                    }

                    if( s2_range > 0 ) {
                        if( once ) {
                            s2_days_high = daysSinceBegin( s2_startyear + s2_range + s1_range, 1, 1 );
                        } else {
                            s2_days_high = daysSinceBegin( s2_startyear + s2_range + ( counter * s1_range ), 1, 1 );
                        }

                        if( s2_days_high > 0 ) {
                            qs.query2 += "registration_days <= " + s2_days_high + " AND ";
                            qs.s2_days_high = s2_days_high;
                            if( debug ) { System.out.println( String.format( "counter: %d, s2 registration_days <= %d", counter, s2_days_high ) ); }
                        }
                    }
                }

                // ignore_sex



                // clean
                if( qs.query1.endsWith( " AND " ) ) {
                    qs.query1 = qs.query1.substring( 0, (qs.query1.length() - 4) );
                }

                if( qs.query2.endsWith( " AND " ) ) {
                    qs.query2 = qs.query2.substring( 0, (qs.query2.length() - 4) );
                }

                // Order
                qs.query1 += "ORDER BY ego_familyname LIMIT 0,100000000";
                qs.query2 += "ORDER BY ego_familyname LIMIT 0,100000000";

                qgs.add( qs );      // add the QuerySet to the QueryGroupSet
                counter++;
                
                if( once ) { loop = false; }
            }

            is.add( qgs );          // add the QueryGroupSet to the InputSet
        }

        String msg = String.format( "match_process lines: %s, using %d, ignoring %d", nline, nline_y, nline_n );
        System.out.println( msg );
        plog.show( msg );
    }


    /**
     * Initial part of query to get the data from links_base
     *
     * @param use_mother
     * @param use_father
     * @param use_partner
     * @param ignore_minmax
     * @param numberofFirstnames
     * @return
     */
    private String getSelectQuery( boolean use_mother, boolean use_father, boolean use_partner, boolean ignore_minmax, int numberofFirstnames )
    {
        String query;

        query = "SELECT id_base , registration_days , ego_familyname_str , ego_firstname1_str , ego_familyname ";

        if( !ignore_minmax ) {
            query += ", ego_birth_min , ego_birth_max , ego_marriage_min , ego_marriage_max , ego_death_min , ego_death_max ";
        }

        switch( numberofFirstnames )
        {
            case 1:
                query += ", ego_firstname1 , ego_firstname2 , ego_firstname3 , ego_firstname4 ";
                break;
            case 2:
                query += ", ego_firstname1 ";
                break;
            case 3:
                query += ", ego_firstname1 , ego_firstname2 ";
                break;
            case 4:
                query += ", ego_firstname1 , ego_firstname2 , ego_firstname3 ";
                break;
            case 5:
                query += ", ego_firstname1 , ego_firstname2 , ego_firstname3 , ego_firstname4 ";
                break;
        }

        if( use_mother )
        {
            query += ", mother_familyname ";

            if( !ignore_minmax ) {
                query += ", mother_birth_min , mother_birth_max , mother_marriage_min , mother_marriage_max , mother_death_min , mother_death_max ";
            }

            switch( numberofFirstnames )
            {
                case 1:
                    query += ", mother_firstname1 , mother_firstname2 , mother_firstname3 , mother_firstname4 ";
                    break;
                case 2:
                    query += ", mother_firstname1 ";
                    break;
                case 3:
                    query += ", mother_firstname1 , mother_firstname2 ";
                    break;
                case 4:
                    query += ", mother_firstname1 , mother_firstname2 , mother_firstname3 ";
                    break;
                case 5:
                    query += ", mother_firstname1 , mother_firstname2 , mother_firstname3 , mother_firstname4 ";
                    break;
            }

        }

        if( use_father )
        {
            query += ", father_familyname ";

            if( !ignore_minmax ) {
                query += ", father_birth_min , father_birth_max , father_marriage_min , father_marriage_max , father_death_min , father_death_max ";
            }

            switch( numberofFirstnames )
            {
                case 1:
                    query += ", father_firstname1 , father_firstname2 , father_firstname3 , father_firstname4 ";
                    break;
                case 2:
                    query += ", father_firstname1 ";
                    break;
                case 3:
                    query += ", father_firstname1 , father_firstname2 ";
                    break;
                case 4:
                    query += ", father_firstname1 , father_firstname2 , father_firstname3 ";
                    break;
                case 5:
                    query += ", father_firstname1 , father_firstname2 , father_firstname3 , father_firstname4 ";
                    break;
            }
        }

        if( use_partner )
        {
            query += ", partner_familyname ";

            if( !ignore_minmax ) {
                query += ", partner_birth_min , partner_birth_max , partner_marriage_min , partner_marriage_max , partner_death_min , partner_death_max ";

            }

            switch( numberofFirstnames )
            {
                case 1:
                    query += ", partner_firstname1 , partner_firstname2 , partner_firstname3 , partner_firstname4 ";
                    break;
                case 2:
                    query += ", partner_firstname1 ";
                    break;
                case 3:
                    query += ", partner_firstname1 , partner_firstname2 ";
                    break;
                case 4:
                    query += ", partner_firstname1 , partner_firstname2 , partner_firstname3 ";
                    break;
                case 5:
                    query += ", partner_firstname1 , partner_firstname2 , partner_firstname3 , partner_firstname4 ";
                    break;
            }

        }

        return query;
    }


    /**
     * Calculate the number of days since january 1st of year 1 (There is no year zero)
     *
     * @param year
     * @param month
     * @param day
     * @return 
     */
    public int daysSinceBegin( int year, int month, int day )
    {
        Calendar cal1 = new GregorianCalendar();
        Calendar cal2 = new GregorianCalendar();

        cal1.set( 1, 1, 1 );                // first of january in year one

        cal2.set(year, month, day);         // given date

        java.util.Date d1 = cal1.getTime();
        java.util.Date d2 = cal2.getTime();

        return (int) ( (d2.getTime() - d1.getTime() ) / (1000 * 60 * 60 * 24) );
    }

}
