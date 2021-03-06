package viewmatrix;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;

/**
 *
 * @author Omar Azouguagh
 * @author Fons Laan
 *
 * input:  links_match.match_view
 * output: links_match.matrix
 *
 * FL-09-Dec-2014 Latest change
 */
public class ViewMatrix
{
    private static Connection dbc_links_match;

    private static int id_match;
    private static int firstname;
    private static char[] use_familyname;
    private static char[] use_firstname;

    //private static PrintLogger plog;


    /**
     * @param args the command line arguments
     */
    public static void main( String[] args )
    {
        processArgs( args );

        getParams();


        try
        {


            /*
            String use_familyname_temp = "1001";
            String use_firstname_temp  = "1001";
            int firstname = 2;

            char[] use_familyname = use_familyname_temp.toCharArray();
            char[] use_firstname  = use_firstname_temp.toCharArray();
            */


            String querySelect = ""
                + "SELECT "
                + "ego_familyname_value, "
                + "ego_firstname1_value, "
                + "ego_firstname2_value, "
                + "ego_firstname3_value, "
                + "ego_firstname4_value, "
                + "mother_familyname_value, "
                + "mother_firstname1_value, "
                + "mother_firstname2_value, "
                + "mother_firstname3_value, "
                + "mother_firstname4_value, "
                + "father_familyname_value, "
                + "father_firstname1_value, "
                + "father_firstname2_value, "
                + "father_firstname3_value, "
                + "father_firstname4_value, "
                + "partner_familyname_value, "
                + "partner_firstname1_value, "
                + "partner_firstname2_value, "
                + "partner_firstname3_value, "
                + "partner_firstname4_value "
                + "FROM "
                + "match_view";

            System.out.println( querySelect );

            ResultSet rs = dbc_links_match.createStatement().executeQuery( querySelect );

            // allocate
            int[] lv    = new int[ 80 ];
            int[] lv_0  = new int[ 80 ];
            int[] lv_1  = new int[ 80 ];
            int[] lv_2  = new int[ 80 ];
            int[] lv_3  = new int[ 80 ];
            int[] lv_4  = new int[ 80 ];
            int[] lv_5  = new int[ 80 ];
            int[] lv_6  = new int[ 80 ];
            int[] lv_7  = new int[ 80 ];
            int[] lv_8  = new int[ 80 ];
            int[] lv_9  = new int[ 80 ];
            int[] lv_10 = new int[ 80 ];
            int[] lv_11 = new int[ 80 ];
            int[] lv_12 = new int[ 80 ];
            int[] lv_13 = new int[ 80 ];
            int[] lv_14 = new int[ 80 ];
            int[] lv_15 = new int[ 80 ];
            int[] lv_16 = new int[ 80 ];
            int[] lv_17 = new int[ 80 ];
            int[] lv_18 = new int[ 80 ];
            int[] lv_19 = new int[ 80 ];
            int[] lv_20 = new int[ 80] ;

            // initialize
            for( int i = 0; i < 80; i++ )
            {
                lv[ i ]    = 0;
                lv_0[ i ]  = 0;
                lv_1[ i ]  = 0;
                lv_2[ i ]  = 0;
                lv_3[ i ]  = 0;
                lv_4[ i ]  = 0;
                lv_5[ i ]  = 0;
                lv_6[ i ]  = 0;
                lv_7[ i ]  = 0;
                lv_8[ i ]  = 0;
                lv_9[ i ]  = 0;
                lv_10[ i ] = 0;
                lv_11[ i ] = 0;
                lv_12[ i ] = 0;
                lv_13[ i ] = 0;
                lv_14[ i ] = 0;
                lv_15[ i ] = 0;
                lv_16[ i ] = 0;
                lv_17[ i ] = 0;
                lv_18[ i ] = 0;
                lv_19[ i ] = 0;
                lv_20[ i ] = 0;
            }

            int nhits = 0;
            while( rs.next() )
            {
                nhits++;

                // load into
                String[] sa = new String[ 20 ];

                // fill with empty
                for( int i = 0; i < 20; i++ ) { sa[ i ] = ""; }

                // familynames
                if( use_familyname[ 0 ] == '1' ) { // ego
                    sa[ 0 ] = rs.getString( "ego_familyname_value" ) != null ? rs.getString( "ego_familyname_value" ) : "";
                }

                if( use_familyname[ 1 ] == '1' ) { // moeder
                    sa[ 5 ] = rs.getString( "mother_familyname_value" ) != null ? rs.getString( "mother_familyname_value" ) : "";
                }

                if( use_familyname[ 2 ] == '1' ) { // vader
                    sa[ 10 ] = rs.getString( "father_familyname_value" ) != null ? rs.getString( "father_familyname_value" ) : "";
                }

                if( use_familyname[ 3 ] == '1' ) { // partner
                    sa[ 15 ] = rs.getString( "partner_familyname_value" ) != null ? rs.getString( "partner_familyname_value" ) : "";
                }


                if( use_firstname[ 0 ] == '1' ) { // ego
                    sa[ 1 ] = rs.getString( "ego_firstname1_value" ) != null ? rs.getString( "ego_firstname1_value" ) : "";

                    if( firstname == 1 || firstname > 2 ) {
                        sa[ 2 ] = rs.getString( "ego_firstname2_value" ) != null ? rs.getString( "ego_firstname2_value" ) : "";
                    }

                    if( firstname == 1 || firstname > 3 ) {
                        sa[ 3 ] = rs.getString( "ego_firstname3_value" ) != null ? rs.getString( "ego_firstname3_value" ) : "";
                    }

                    if( firstname == 1 || firstname > 4 ) {
                        sa[ 4 ] = rs.getString( "ego_firstname4_value" ) != null ? rs.getString( "ego_firstname4_value" ) : "";
                    }
                }


                if( use_firstname[ 1 ] == '1' ) { // mother
                    sa[ 6 ] = rs.getString( "mother_firstname1_value" ) != null ? rs.getString( "mother_firstname1_value" ) : "";

                    if( firstname == 1 || firstname > 2 ) {
                        sa[ 7 ] = rs.getString( "mother_firstname2_value" ) != null ? rs.getString( "mother_firstname2_value" ) : "";
                    }

                    if( firstname == 1 || firstname > 3 ) {
                        sa[ 8 ] = rs.getString( "mother_firstname3_value" ) != null ? rs.getString( "mother_firstname3_value" ) : "";
                    }

                    if( firstname == 1 || firstname > 4 ) {
                        sa[ 9 ] = rs.getString( "mother_firstname4_value" ) != null ? rs.getString( "mother_firstname4_value" ) : "";
                    }
                }


                if( use_firstname[ 2 ] == '1' ) { // father
                    sa[ 11 ] = rs.getString( "father_firstname1_value" ) != null ? rs.getString( "father_firstname1_value" ) : "";

                    if( firstname == 1 || firstname > 2 ) {
                        sa[ 12 ] = rs.getString( "father_firstname2_value" ) != null ? rs.getString( "father_firstname2_value" ) : "";
                    }

                    if( firstname == 1 || firstname > 3 ) {
                        sa[ 13 ] = rs.getString( "father_firstname3_value" ) != null ? rs.getString( "father_firstname3_value" ) : "";
                    }

                    if( firstname == 1 || firstname > 4 ) {
                        sa[ 14 ] = rs.getString( "father_firstname4_value" ) != null ? rs.getString( "father_firstname4_value" ) : "";
                    }
                }


                if( use_firstname[ 3 ] == '1' ) { // partner
                    sa[ 16 ] = rs.getString( "partner_firstname1_value" ) != null ? rs.getString( "partner_firstname1_value" ) : "";

                    if( firstname == 1 || firstname > 2 ) {
                        sa[ 17 ] = rs.getString( "partner_firstname2_value" ) != null ? rs.getString( "partner_firstname2_value" ) : "";
                    }

                    if( firstname == 1 || firstname > 3 ) {
                        sa[ 18 ] = rs.getString( "partner_firstname3_value" ) != null ? rs.getString( "partner_firstname3_value" ) : "";
                    }

                    if( firstname == 1 || firstname > 4 ) {
                        sa[ 19 ] = rs.getString( "partner_firstname4_value" ) != null ? rs.getString( "partner_firstname4_value" ) : "";
                    }
                }

                int lv_temp = 0;
                int exact = 0;

                for( String s : sa ) {
                    if( !s.isEmpty() ) {
                        lv_temp += Integer.parseInt( s );

                        if( Integer.parseInt( s ) == 0 ) { exact++; }
                    }
                }

                lv[ lv_temp ]++;

                switch( exact ) {
                    case 1:
                        lv_1[ lv_temp ]++;
                        break;
                    case 2:
                        lv_2[ lv_temp ]++;
                        break;
                    case 3:
                        lv_3[ lv_temp ]++;
                        break;
                    case 4:
                        lv_4[ lv_temp ]++;
                        break;
                    case 5:
                        lv_5[ lv_temp ]++;
                        break;
                    case 6:
                        lv_6[ lv_temp ]++;
                        break;
                    case 7:
                        lv_7[ lv_temp ]++;
                        break;
                    case 8:
                        lv_8[ lv_temp ]++;
                        break;
                    case 9:
                        lv_9[ lv_temp ]++;
                        break;
                    case 10:
                        lv_10[ lv_temp ]++;
                        break;
                    case 11:
                        lv_11[ lv_temp ]++;
                        break;
                    case 12:
                        lv_12[ lv_temp ]++;
                        break;
                    case 13:
                        lv_13[ lv_temp ]++;
                        break;
                    case 14:
                        lv_14[ lv_temp ]++;
                        break;
                    case 15:
                        lv_15[ lv_temp ]++;
                        break;
                    case 16:
                        lv_16[ lv_temp ]++;
                        break;
                    case 17:
                        lv_17[ lv_temp ]++;
                        break;
                    case 18:
                        lv_18[ lv_temp ]++;
                        break;
                    case 19:
                        lv_19[ lv_temp ]++;
                        break;
                    case 20:
                        lv_20[ lv_temp ]++;
                        break;
                    default:
                        lv_0[ lv_temp ]++;
                        break;
                }
            }

            System.out.println( "nhits = " + nhits );

            String queryDelete = "TRUNCATE TABLE matrix";      // delete everything from table
            System.out.println( queryDelete );
            dbc_links_match.createStatement().execute( queryDelete  );

            for( int i = 0; i < 80; i++ )
            {
                String queryInsert = "INSERT INTO matrix( lv,n,e0,e1,e2,e3,e4,e5,e6,e7,e8,e9,e10,e11,e12,e13,e14,e15,e16,e17,e18,e19,e20 ) VALUES( "
                    + i + ","
                    + lv[ i ] + ","
                    + lv_0[ i ] + ","
                    + lv_1[ i ] + ","
                    + lv_2[ i ] + ","
                    + lv_3[ i ] + ","
                    + lv_4[ i ] + ","
                    + lv_5[ i ] + ","
                    + lv_6[ i ] + ","
                    + lv_7[ i ] + ","
                    + lv_8[ i ] + ","
                    + lv_9[ i ] + ","
                    + lv_10[ i ] + ","
                    + lv_11[ i ] + ","
                    + lv_12[ i ] + ","
                    + lv_13[ i ] + ","
                    + lv_14[ i ] + ","
                    + lv_15[ i ] + ","
                    + lv_16[ i ] + ","
                    + lv_17[ i ] + ","
                    + lv_18[ i ] + ","
                    + lv_19[ i ] + ","
                    + lv_20[ i ] + " );";

                System.out.println( queryInsert );
                dbc_links_match.createStatement().execute( queryInsert );
            }
        }
        catch( Exception ex ) { System.out.println( ex.getMessage()); }
    }


    private static void processArgs( String[] args )
    {
        // Load arguments; check length
        if( args.length != 4 ) {
            System.out.println( "Invalid argument length, it should be 4" );
            System.out.println( "Usage: java -jar ViewMatrix-2.0.jar <db_url> <db_username> <db_password> <id_match>" );

            System.exit( 0 );
        }

        String db_url   = args[ 0 ];
        String db_user  = args[ 1 ];
        String db_pass  = args[ 2 ];
        String matchStr = args[ 3 ];

        id_match = Integer.parseInt( matchStr );

        System.out.println( String.format( "db_url:   %s", db_url ) );
        System.out.println( String.format( "db_user:  %s", db_user ) );
        System.out.println( String.format( "db_pass:  %s", db_pass ) );
        System.out.println( String.format( "id_match: %d", id_match ) );

        String db_name    = "links_match";
        //String driver     = "org.gjt.mm.mysql.Driver";
        String db_longUrl = "jdbc:mysql://" + db_url + "/" + db_name + "?dontTrackOpenResources=true";

        try {
            //Class.forName( driver );
            dbc_links_match = DriverManager.getConnection( db_longUrl, db_user, db_pass );
        }
        catch( Exception ex ) { System.out.println( ex.getMessage()); }
    }


    private static void getParams()
    {
        /*
        // hard-coded text values from Omar
        String use_familyname_temp = "1001";
        String use_firstname_temp  = "1001";
        int firstname = 2;

        char[] use_familyname = use_familyname_temp.toCharArray();
        char[] use_firstname  = use_firstname_temp.toCharArray();
        */

        String querySelect = ""
            + "SELECT "
            + "id, "
            + "firstname, "
            + "use_familyname, "
            + "use_firstname "
            + "FROM "
            + "match_process";

        System.out.println( querySelect );

        try {
            ResultSet rs = dbc_links_match.createStatement().executeQuery( querySelect );

            while( rs.next() )
            {
                int id = rs.getInt( "id" );

                if( id == id_match ) {
                    firstname = rs.getInt( "firstname" );

                    String use_familynameStr = rs.getString( "use_familyname" );
                    String use_firstnameStr  = rs.getString( "use_firstname" );

                    use_familyname = use_familynameStr.toCharArray();
                    use_firstname  = use_firstnameStr.toCharArray();

                    System.out.println( String.format( "firstname: %d",firstname ) );
                    System.out.println( String.format( "use_familyname: %s", use_familynameStr ) );
                    System.out.println( String.format( "use_firstname:  %s", use_firstnameStr ) );

                    break;
                }
            }
        }
        catch( Exception ex ) { System.out.println( ex.getMessage()); }
    }
}
