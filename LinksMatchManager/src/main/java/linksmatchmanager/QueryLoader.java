package linksmatchmanager;

import java.sql.Connection;
import java.sql.ResultSet;

import java.util.Vector;

import linksmatchmanager.DataSet.QuerySet;

/**
 * @author Omar Azouguagh
 * @author Fons Laan
 *
 * <p/>
 * FL-30-Jun-2014 Imported from OA backup
 * FL-30-Apr-2015 Free vectors
 * FL-05-Nov-2015 Latest change
 *
 * TODO It would be better to have a SampleLoader, separate for s1 and s2
 */
public class QueryLoader
{
    private Connection dbconPrematch;

    private boolean use_mother;
    private boolean use_father;
    private boolean use_partner;
    private boolean ignore_sex;
    private boolean ignore_minmax;

    private int firstname;

    private ResultSet set1;
    private ResultSet set2;

    // Set variables
    public Vector< Integer > s1_id_base              = new Vector< Integer >();
    public Vector< Integer > s1_registration_days    = new Vector< Integer >();

    public Vector< String > s1_ego_familyname_str    = new Vector< String >();
    public Vector< String > s1_ego_firstname1_str    = new Vector< String >();

    public Vector< Integer > s1_ego_familyname       = new Vector< Integer >();
    public Vector< Integer > s1_ego_firstname1       = new Vector< Integer >();
    public Vector< Integer > s1_ego_firstname2       = new Vector< Integer >();
    public Vector< Integer > s1_ego_firstname3       = new Vector< Integer >();
    public Vector< Integer > s1_ego_firstname4       = new Vector< Integer >();
    public Vector< Integer > s1_ego_birth_min        = new Vector< Integer >();
    public Vector< Integer > s1_ego_birth_max        = new Vector< Integer >();
    public Vector< Integer > s1_ego_marriage_min     = new Vector< Integer >();
    public Vector< Integer > s1_ego_marriage_max     = new Vector< Integer >();
    public Vector< Integer > s1_ego_death_min        = new Vector< Integer >();
    public Vector< Integer > s1_ego_death_max        = new Vector< Integer >();

    public Vector< Integer > s1_sex                  = new Vector< Integer >();

    public Vector< Integer > s1_mother_familyname    = new Vector< Integer >();
    public Vector< Integer > s1_mother_firstname1    = new Vector< Integer >();
    public Vector< Integer > s1_mother_firstname2    = new Vector< Integer >();
    public Vector< Integer > s1_mother_firstname3    = new Vector< Integer >();
    public Vector< Integer > s1_mother_firstname4    = new Vector< Integer >();
    public Vector< Integer > s1_mother_birth_min     = new Vector< Integer >();
    public Vector< Integer > s1_mother_birth_max     = new Vector< Integer >();
    public Vector< Integer > s1_mother_marriage_min  = new Vector< Integer >();
    public Vector< Integer > s1_mother_marriage_max  = new Vector< Integer >();
    public Vector< Integer > s1_mother_death_min     = new Vector< Integer >();
    public Vector< Integer > s1_mother_death_max     = new Vector< Integer >();

    public Vector< Integer > s1_father_familyname    = new Vector< Integer >();
    public Vector< Integer > s1_father_firstname1    = new Vector< Integer >();
    public Vector< Integer > s1_father_firstname2    = new Vector< Integer >();
    public Vector< Integer > s1_father_firstname3    = new Vector< Integer >();
    public Vector< Integer > s1_father_firstname4    = new Vector< Integer >();
    public Vector< Integer > s1_father_birth_min     = new Vector< Integer >();
    public Vector< Integer > s1_father_birth_max     = new Vector< Integer >();
    public Vector< Integer > s1_father_marriage_min  = new Vector< Integer >();
    public Vector< Integer > s1_father_marriage_max  = new Vector< Integer >();
    public Vector< Integer > s1_father_death_min     = new Vector< Integer >();
    public Vector< Integer > s1_father_death_max     = new Vector< Integer >();

    public Vector< Integer > s1_partner_familyname   = new Vector< Integer >();
    public Vector< Integer > s1_partner_firstname1   = new Vector< Integer >();
    public Vector< Integer > s1_partner_firstname2   = new Vector< Integer >();
    public Vector< Integer > s1_partner_firstname3   = new Vector< Integer >();
    public Vector< Integer > s1_partner_firstname4   = new Vector< Integer >();
    public Vector< Integer > s1_partner_birth_min    = new Vector< Integer >();
    public Vector< Integer > s1_partner_birth_max    = new Vector< Integer >();
    public Vector< Integer > s1_partner_marriage_min = new Vector< Integer >();
    public Vector< Integer > s1_partner_marriage_max = new Vector< Integer >();
    public Vector< Integer > s1_partner_death_min    = new Vector< Integer >();
    public Vector< Integer > s1_partner_death_max    = new Vector< Integer >();


    public Vector< Integer > s2_id_base              = new Vector< Integer >();
    public Vector< Integer > s2_registration_days    = new Vector< Integer >();

    public Vector< String > s2_ego_familyname_str    = new Vector< String >();
    public Vector< String > s2_ego_firstname1_str    = new Vector< String >();

    public Vector< Integer > s2_ego_familyname       = new Vector< Integer >();
    public Vector< Integer > s2_ego_firstname1       = new Vector< Integer >();
    public Vector< Integer > s2_ego_firstname2       = new Vector< Integer >();
    public Vector< Integer > s2_ego_firstname3       = new Vector< Integer >();
    public Vector< Integer > s2_ego_firstname4       = new Vector< Integer >();
    public Vector< Integer > s2_ego_birth_min        = new Vector< Integer >();
    public Vector< Integer > s2_ego_birth_max        = new Vector< Integer >();
    public Vector< Integer > s2_ego_marriage_min     = new Vector< Integer >();
    public Vector< Integer > s2_ego_marriage_max     = new Vector< Integer >();
    public Vector< Integer > s2_ego_death_min        = new Vector< Integer >();
    public Vector< Integer > s2_ego_death_max        = new Vector< Integer >();
    public Vector< Integer > s2_sex                  = new Vector< Integer >();

    public Vector< Integer > s2_mother_familyname    = new Vector< Integer >();
    public Vector< Integer > s2_mother_firstname1    = new Vector< Integer >();
    public Vector< Integer > s2_mother_firstname2    = new Vector< Integer >();
    public Vector< Integer > s2_mother_firstname3    = new Vector< Integer >();
    public Vector< Integer > s2_mother_firstname4    = new Vector< Integer >();
    public Vector< Integer > s2_mother_birth_min     = new Vector< Integer >();
    public Vector< Integer > s2_mother_birth_max     = new Vector< Integer >();
    public Vector< Integer > s2_mother_marriage_min  = new Vector< Integer >();
    public Vector< Integer > s2_mother_marriage_max  = new Vector< Integer >();
    public Vector< Integer > s2_mother_death_min     = new Vector< Integer >();
    public Vector< Integer > s2_mother_death_max     = new Vector< Integer >();

    public Vector< Integer > s2_father_familyname    = new Vector< Integer >();
    public Vector< Integer > s2_father_firstname1    = new Vector< Integer >();
    public Vector< Integer > s2_father_firstname2    = new Vector< Integer >();
    public Vector< Integer > s2_father_firstname3    = new Vector< Integer >();
    public Vector< Integer > s2_father_firstname4    = new Vector< Integer >();
    public Vector< Integer > s2_father_birth_min     = new Vector< Integer >();
    public Vector< Integer > s2_father_birth_max     = new Vector< Integer >();
    public Vector< Integer > s2_father_marriage_min  = new Vector< Integer >();
    public Vector< Integer > s2_father_marriage_max  = new Vector< Integer >();
    public Vector< Integer > s2_father_death_min     = new Vector< Integer >();
    public Vector< Integer > s2_father_death_max     = new Vector< Integer >();

    public Vector< Integer > s2_partner_familyname   = new Vector< Integer >();
    public Vector< Integer > s2_partner_firstname1   = new Vector< Integer >();
    public Vector< Integer > s2_partner_firstname2   = new Vector< Integer >();
    public Vector< Integer > s2_partner_firstname3   = new Vector< Integer >();
    public Vector< Integer > s2_partner_firstname4   = new Vector< Integer >();
    public Vector< Integer > s2_partner_birth_min    = new Vector< Integer >();
    public Vector< Integer > s2_partner_birth_max    = new Vector< Integer >();
    public Vector< Integer > s2_partner_marriage_min = new Vector< Integer >();
    public Vector< Integer > s2_partner_marriage_max = new Vector< Integer >();
    public Vector< Integer > s2_partner_death_min    = new Vector< Integer >();
    public Vector< Integer > s2_partner_death_max    = new Vector< Integer >();


    /**
     * 
     * @param qs
     * @param dbconPrematch
     */
    public QueryLoader( long threadId, QuerySet qs, Connection dbconPrematch ) throws Exception
    {
        this.use_mother    = qs.use_mother;
        this.use_father    = qs.use_father;
        this.use_partner   = qs.use_partner;
        this.firstname     = qs.firstname;
        this.ignore_sex    = qs.ignore_sex;
        this.ignore_minmax = qs.ignore_minmax;


        // get set 1 from links_base
        System.out.println( "Thread id " + threadId + "; retrieving set 1 from links_base..." );
        System.out.println( qs.query1 );
        set1 = dbconPrematch.createStatement().executeQuery( qs.query1 );

        // get set 2 from links_base
        System.out.println( "Thread id " + threadId + "; retrieving set 2 from links_base..." );
        System.out.println( qs.query2 );

        set2 = dbconPrematch.createStatement().executeQuery( qs.query2 );
        //set2 = dbconPrematch.createStatement().executeQuery( qs.query1 );     // only for matching TEST !

        fillArrays();

        this.dbconPrematch = dbconPrematch;
    }


    private void fillArrays() throws Exception
    {
        // Do set 1
        while( set1.next() )
        {
            // Vars to use, global
            int var_s1_id_base = 0;
            int var_s1_registration_days = 0;

            String var_s1_ego_familyname_str = "";
            String var_s1_ego_firstname1_str = "";

            int var_s1_ego_familyname   = 0;
            int var_s1_ego_firstname1   = 0;
            int var_s1_ego_firstname2   = 0;
            int var_s1_ego_firstname3   = 0;
            int var_s1_ego_firstname4   = 0;
            int var_s1_ego_birth_min    = 0;
            int var_s1_ego_birth_max    = 0;
            int var_s1_ego_marriage_min = 0;
            int var_s1_ego_marriage_max = 0;
            int var_s1_ego_death_min    = 0;
            int var_s1_ego_death_max    = 0;

            int var_s1_sex = 0;
            int var_s1_mother_familyname   = 0;
            int var_s1_mother_firstname1   = 0;
            int var_s1_mother_firstname2   = 0;
            int var_s1_mother_firstname3   = 0;
            int var_s1_mother_firstname4   = 0;
            int var_s1_mother_birth_min    = 0;
            int var_s1_mother_birth_max    = 0;
            int var_s1_mother_marriage_min = 0;
            int var_s1_mother_marriage_max = 0;
            int var_s1_mother_death_min    = 0;
            int var_s1_mother_death_max    = 0;

            int var_s1_father_familyname   = 0;
            int var_s1_father_firstname1   = 0;
            int var_s1_father_firstname2   = 0;
            int var_s1_father_firstname3   = 0;
            int var_s1_father_firstname4   = 0;
            int var_s1_father_birth_min    = 0;
            int var_s1_father_birth_max    = 0;
            int var_s1_father_marriage_min = 0;
            int var_s1_father_marriage_max = 0;
            int var_s1_father_death_min    = 0;
            int var_s1_father_death_max    = 0;

            int var_s1_partner_familyname   = 0;
            int var_s1_partner_firstname1   = 0;
            int var_s1_partner_firstname2   = 0;
            int var_s1_partner_firstname3   = 0;
            int var_s1_partner_firstname4   = 0;
            int var_s1_partner_birth_min    = 0;
            int var_s1_partner_birth_max    = 0;
            int var_s1_partner_marriage_min = 0;
            int var_s1_partner_marriage_max = 0;
            int var_s1_partner_death_min    = 0;
            int var_s1_partner_death_max    = 0;

            // Get all vars from table
            var_s1_id_base = set1.getInt( "id_base" );
            var_s1_registration_days = set1.getInt( "registration_days" );

            // Ego
            // Familyname
            var_s1_ego_familyname     = set1.getInt(    "ego_familyname" );
            var_s1_ego_familyname_str = set1.getString( "ego_familyname_str" );
            var_s1_ego_firstname1_str = set1.getString( "ego_firstname1_str" );

            // First Names ego
            switch( firstname )     // firstname method:
            {
                case 1:
                    var_s1_ego_firstname1 = set1.getInt( "ego_firstname1" );
                    var_s1_ego_firstname2 = set1.getInt( "ego_firstname2" );
                    var_s1_ego_firstname3 = set1.getInt( "ego_firstname3" );
                    var_s1_ego_firstname3 = set1.getInt( "ego_firstname4" );
                    break;
                case 2:
                    var_s1_ego_firstname1 = set1.getInt( "ego_firstname1" );
                    break;
                case 3:
                    var_s1_ego_firstname1 = set1.getInt( "ego_firstname1" );
                    var_s1_ego_firstname2 = set1.getInt( "ego_firstname2" );
                    break;
                case 4:
                    var_s1_ego_firstname1 = set1.getInt( "ego_firstname1" );
                    var_s1_ego_firstname2 = set1.getInt( "ego_firstname2" );
                    var_s1_ego_firstname3 = set1.getInt( "ego_firstname3" );
                    break;
                case 5:
                    var_s1_ego_firstname1 = set1.getInt( "ego_firstname1" );
                    var_s1_ego_firstname2 = set1.getInt( "ego_firstname2" );
                    var_s1_ego_firstname3 = set1.getInt( "ego_firstname3" );
                    var_s1_ego_firstname3 = set1.getInt( "ego_firstname4" );
                    break;
            }

            if( ! ignore_minmax )
            {
                var_s1_ego_birth_min    = set1.getInt( "ego_birth_min" );
                var_s1_ego_birth_max    = set1.getInt( "ego_birth_max" );
                var_s1_ego_marriage_min = set1.getInt( "ego_marriage_min" );
                var_s1_ego_marriage_max = set1.getInt( "ego_marriage_max" );
                var_s1_ego_death_min    = set1.getInt( "ego_death_min" );
                var_s1_ego_death_max    = set1.getInt( "ego_death_max" );
            }

            if( use_mother )
            {
                // Family name
                var_s1_mother_familyname = set1.getInt( "mother_familyname" );

                // First name
                switch( firstname )
                {
                    case 1:
                        var_s1_mother_firstname1 = set1.getInt( "mother_firstname1" );
                        var_s1_mother_firstname2 = set1.getInt( "mother_firstname2" );
                        var_s1_mother_firstname3 = set1.getInt( "mother_firstname3" );
                        var_s1_mother_firstname3 = set1.getInt( "mother_firstname4" );
                        break;
                    case 2:
                        var_s1_mother_firstname1 = set1.getInt( "mother_firstname1" );
                        break;
                    case 3:
                        var_s1_mother_firstname1 = set1.getInt( "mother_firstname1" );
                        var_s1_mother_firstname2 = set1.getInt( "mother_firstname2" );
                        break;
                    case 4:
                        var_s1_mother_firstname1 = set1.getInt( "mother_firstname1" );
                        var_s1_mother_firstname2 = set1.getInt( "mother_firstname2" );
                        var_s1_mother_firstname3 = set1.getInt( "mother_firstname3" );
                        break;
                    case 5:
                        var_s1_mother_firstname1 = set1.getInt( "mother_firstname1" );
                        var_s1_mother_firstname2 = set1.getInt( "mother_firstname2" );
                        var_s1_mother_firstname3 = set1.getInt( "mother_firstname3" );
                        var_s1_mother_firstname3 = set1.getInt( "mother_firstname4" );
                        break;
                }

                if( ! ignore_minmax )
                {
                    var_s1_mother_birth_min    = set1.getInt( "mother_birth_min" );
                    var_s1_mother_birth_max    = set1.getInt( "mother_birth_max" );
                    var_s1_mother_marriage_min = set1.getInt( "mother_marriage_min" );
                    var_s1_mother_marriage_max = set1.getInt( "mother_marriage_max" );
                    var_s1_mother_death_min    = set1.getInt( "mother_death_min" );
                    var_s1_mother_death_max    = set1.getInt( "mother_death_max" );
                }
            }

            if( use_father )
            {
                // Family Name
                var_s1_father_familyname = set1.getInt( "father_familyname" );

                // First Names
                switch( firstname )
                {
                    case 1:
                        var_s1_father_firstname1 = set1.getInt( "father_firstname1" );
                        var_s1_father_firstname2 = set1.getInt( "father_firstname2" );
                        var_s1_father_firstname3 = set1.getInt( "father_firstname3" );
                        var_s1_father_firstname3 = set1.getInt( "father_firstname4" );
                        break;
                    case 2:
                        var_s1_father_firstname1 = set1.getInt( "father_firstname1" );
                        break;
                    case 3:
                        var_s1_father_firstname1 = set1.getInt( "father_firstname1" );
                        var_s1_father_firstname2 = set1.getInt( "father_firstname2" );
                        break;
                    case 4:
                        var_s1_father_firstname1 = set1.getInt("father_firstname1");
                        var_s1_father_firstname2 = set1.getInt("father_firstname2");
                        var_s1_father_firstname3 = set1.getInt("father_firstname3");
                        break;
                    case 5:
                        var_s1_father_firstname1 = set1.getInt( "father_firstname1" );
                        var_s1_father_firstname2 = set1.getInt( "father_firstname2" );
                        var_s1_father_firstname3 = set1.getInt( "father_firstname3" );
                        var_s1_father_firstname3 = set1.getInt( "father_firstname4" );
                        break;
                }

                if( ! ignore_minmax )
                {
                    var_s1_father_birth_min    = set1.getInt( "father_birth_min" );
                    var_s1_father_birth_max    = set1.getInt( "father_birth_max" );
                    var_s1_father_marriage_min = set1.getInt( "father_marriage_min" );
                    var_s1_father_marriage_max = set1.getInt( "father_marriage_max" );
                    var_s1_father_death_min    = set1.getInt( "father_death_min" );
                    var_s1_father_death_max    = set1.getInt( "father_death_max" );
                }
            }

            if( use_partner )
            {
                // Family Name
                var_s1_partner_familyname = set1.getInt( "partner_familyname" );

                // First Names
                switch( firstname )
                {
                    case 1:
                        var_s1_partner_firstname1 = set1.getInt( "partner_firstname1" );
                        var_s1_partner_firstname2 = set1.getInt( "partner_firstname2" );
                        var_s1_partner_firstname3 = set1.getInt( "partner_firstname3" );
                        var_s1_partner_firstname3 = set1.getInt( "partner_firstname4" );
                        break;
                    case 2:
                        var_s1_partner_firstname1 = set1.getInt( "partner_firstname1" );
                        break;
                    case 3:
                        var_s1_partner_firstname1 = set1.getInt( "partner_firstname1" );
                        var_s1_partner_firstname2 = set1.getInt( "partner_firstname2" );
                        break;
                    case 4:
                        var_s1_partner_firstname1 = set1.getInt( "partner_firstname1" );
                        var_s1_partner_firstname2 = set1.getInt( "partner_firstname2" );
                        var_s1_partner_firstname3 = set1.getInt( "partner_firstname3" );
                        break;
                    case 5:
                        var_s1_partner_firstname1 = set1.getInt( "partner_firstname1" );
                        var_s1_partner_firstname2 = set1.getInt( "partner_firstname2" );
                        var_s1_partner_firstname3 = set1.getInt( "partner_firstname3" );
                        var_s1_partner_firstname3 = set1.getInt( "partner_firstname4" );
                        break;
                }

                if( !ignore_minmax )
                {
                    var_s1_partner_birth_min    = set1.getInt( "partner_birth_min" );
                    var_s1_partner_birth_max    = set1.getInt( "partner_birth_max" );
                    var_s1_partner_marriage_min = set1.getInt( "partner_marriage_min" );
                    var_s1_partner_marriage_max = set1.getInt( "partner_marriage_max" );
                    var_s1_partner_death_min    = set1.getInt( "partner_death_min" );
                    var_s1_partner_death_max    = set1.getInt( "partner_death_max" );
                }
            }

            // convert sex to int
            if( !ignore_sex )
            {
                String s = set1.getString( "ego_sex" );

                if( s.equalsIgnoreCase( "v" ) )      { var_s1_sex = 1; }
                else if( s.equalsIgnoreCase( "m" ) ) { var_s1_sex = 2; }
                else                                 { var_s1_sex = 0; }
            }

            // fill the arraylists
            s1_id_base          .add( var_s1_id_base );
            s1_registration_days.add( var_s1_registration_days );

            s1_ego_familyname_str.add( var_s1_ego_familyname_str );
            s1_ego_firstname1_str.add( var_s1_ego_firstname1_str );

            s1_ego_familyname  .add( var_s1_ego_familyname );
            s1_ego_firstname1  .add( var_s1_ego_firstname1 );
            s1_ego_firstname2  .add( var_s1_ego_firstname2 );
            s1_ego_firstname3  .add( var_s1_ego_firstname3 );
            s1_ego_firstname4  .add( var_s1_ego_firstname4 );
            s1_ego_birth_min   .add( var_s1_ego_birth_min );
            s1_ego_birth_max   .add( var_s1_ego_birth_max );
            s1_ego_marriage_min.add( var_s1_ego_marriage_min );
            s1_ego_marriage_max.add( var_s1_ego_marriage_max );
            s1_ego_death_min   .add( var_s1_ego_death_min );
            s1_ego_death_max   .add( var_s1_ego_death_max );

            s1_sex.add(var_s1_sex);

            s1_mother_familyname  .add( var_s1_mother_familyname );
            s1_mother_firstname1  .add( var_s1_mother_firstname1 );
            s1_mother_firstname2  .add( var_s1_mother_firstname2 );
            s1_mother_firstname3  .add( var_s1_mother_firstname3 );
            s1_mother_firstname4  .add( var_s1_mother_firstname4 );
            s1_mother_birth_min   .add( var_s1_mother_birth_min );
            s1_mother_birth_max   .add( var_s1_mother_birth_max );
            s1_mother_marriage_min.add( var_s1_mother_marriage_min );
            s1_mother_marriage_max.add( var_s1_mother_marriage_max );
            s1_mother_death_min   .add( var_s1_mother_death_min );
            s1_mother_death_max   .add( var_s1_mother_death_max );

            s1_father_familyname  .add( var_s1_father_familyname );
            s1_father_firstname1  .add( var_s1_father_firstname1 );
            s1_father_firstname2  .add( var_s1_father_firstname2 );
            s1_father_firstname3  .add( var_s1_father_firstname3 );
            s1_father_firstname4  .add( var_s1_father_firstname4) ;
            s1_father_birth_min   .add( var_s1_father_birth_min );
            s1_father_birth_max   .add( var_s1_father_birth_max );
            s1_father_marriage_min.add( var_s1_father_marriage_min );
            s1_father_marriage_max.add( var_s1_father_marriage_max );
            s1_father_death_min   .add( var_s1_father_death_min );
            s1_father_death_max   .add( var_s1_father_death_max );
            s1_partner_familyname .add( var_s1_partner_familyname );

            s1_partner_firstname1  .add( var_s1_partner_firstname1 );
            s1_partner_firstname2  .add( var_s1_partner_firstname2 );
            s1_partner_firstname3  .add( var_s1_partner_firstname3 );
            s1_partner_firstname4  .add( var_s1_partner_firstname4 );
            s1_partner_birth_min   .add( var_s1_partner_birth_min );
            s1_partner_birth_max   .add( var_s1_partner_birth_max );
            s1_partner_marriage_min.add( var_s1_partner_marriage_min );
            s1_partner_marriage_max.add( var_s1_partner_marriage_max );
            s1_partner_death_min  .add(  var_s1_partner_death_min );
            s1_partner_death_max  .add(  var_s1_partner_death_max );
        }


        // Do set 2
        while( set2.next() )
        {
            int var_s2_id_base = 0;
            int var_s2_registration_days = 0;

            String var_s2_ego_familyname_str = "";
            String var_s2_ego_firstname1_str = "";

            int var_s2_ego_familyname   = 0;
            int var_s2_ego_firstname1   = 0;
            int var_s2_ego_firstname2   = 0;
            int var_s2_ego_firstname3   = 0;
            int var_s2_ego_firstname4   = 0;
            int var_s2_ego_birth_min    = 0;
            int var_s2_ego_birth_max    = 0;
            int var_s2_ego_marriage_min = 0;
            int var_s2_ego_marriage_max = 0;
            int var_s2_ego_death_min    = 0;
            int var_s2_ego_death_max    = 0;

            int var_s2_sex = 0;

            int var_s2_mother_familyname   = 0;
            int var_s2_mother_firstname1   = 0;
            int var_s2_mother_firstname2   = 0;
            int var_s2_mother_firstname3   = 0;
            int var_s2_mother_firstname4   = 0;
            int var_s2_mother_birth_min    = 0;
            int var_s2_mother_birth_max    = 0;
            int var_s2_mother_marriage_min = 0;
            int var_s2_mother_marriage_max = 0;
            int var_s2_mother_death_min    = 0;
            int var_s2_mother_death_max    = 0;

            int var_s2_father_familyname   = 0;
            int var_s2_father_firstname1   = 0;
            int var_s2_father_firstname2   = 0;
            int var_s2_father_firstname3   = 0;
            int var_s2_father_firstname4   = 0;
            int var_s2_father_birth_min    = 0;
            int var_s2_father_birth_max    = 0;
            int var_s2_father_marriage_min = 0;
            int var_s2_father_marriage_max = 0;
            int var_s2_father_death_min    = 0;
            int var_s2_father_death_max    = 0;

            int var_s2_partner_familyname   = 0;
            int var_s2_partner_firstname1   = 0;
            int var_s2_partner_firstname2   = 0;
            int var_s2_partner_firstname3   = 0;
            int var_s2_partner_firstname4   = 0;
            int var_s2_partner_birth_min    = 0;
            int var_s2_partner_birth_max    = 0;
            int var_s2_partner_marriage_min = 0;
            int var_s2_partner_marriage_max = 0;
            int var_s2_partner_death_min    = 0;
            int var_s2_partner_death_max    = 0;

            // Get all vars from table
            var_s2_id_base = set2.getInt( "id_base" );
            var_s2_registration_days = set2.getInt( "registration_days" );

            // Ego
            // familyname
            var_s2_ego_familyname     = set2.getInt(    "ego_familyname" );
            var_s2_ego_familyname_str = set2.getString( "ego_familyname_str" );
            var_s2_ego_firstname1_str = set2.getString( "ego_firstname1_str" );

            // First name
            switch( firstname )     // firstname method:
            {
                case 1:
                    var_s2_ego_firstname1 = set2.getInt( "ego_firstname1" );
                    var_s2_ego_firstname2 = set2.getInt( "ego_firstname2" );
                    var_s2_ego_firstname3 = set2.getInt( "ego_firstname3" );
                    var_s2_ego_firstname3 = set2.getInt( "ego_firstname4" );
                    break;
                case 2:
                    var_s2_ego_firstname1 = set2.getInt( "ego_firstname1" );
                    break;
                case 3:
                    var_s2_ego_firstname1 = set2.getInt( "ego_firstname1" );
                    var_s2_ego_firstname2 = set2.getInt( "ego_firstname2" );
                    break;
                case 4:
                    var_s2_ego_firstname1 = set2.getInt( "ego_firstname1" );
                    var_s2_ego_firstname2 = set2.getInt( "ego_firstname2" );
                    var_s2_ego_firstname3 = set2.getInt( "ego_firstname3" );
                    break;
                case 5:
                    var_s2_ego_firstname1 = set2.getInt( "ego_firstname1" );
                    var_s2_ego_firstname2 = set2.getInt( "ego_firstname2" );
                    var_s2_ego_firstname3 = set2.getInt( "ego_firstname3" );
                    var_s2_ego_firstname3 = set2.getInt( "ego_firstname4" );
                    break;
            }

            if( !ignore_minmax )
            {
                var_s2_ego_birth_min    = set2.getInt( "ego_birth_min" );
                var_s2_ego_birth_max    = set2.getInt( "ego_birth_max" );
                var_s2_ego_marriage_min = set2.getInt( "ego_marriage_min" );
                var_s2_ego_marriage_max = set2.getInt( "ego_marriage_max" );
                var_s2_ego_death_min    = set2.getInt( "ego_death_min" );
                var_s2_ego_death_max    = set2.getInt( "ego_death_max" );
            }

            if( use_mother )
            {
                // Family Name
                var_s2_mother_familyname = set2.getInt( "mother_familyname" );

                // First Names
                switch( firstname )
                {
                    case 1:
                        var_s2_mother_firstname1 = set2.getInt( "mother_firstname1" );
                        var_s2_mother_firstname2 = set2.getInt( "mother_firstname2" ) ;
                        var_s2_mother_firstname3 = set2.getInt( "mother_firstname3" );
                        var_s2_mother_firstname3 = set2.getInt( "mother_firstname4" );
                        break;
                    case 2:
                        var_s2_mother_firstname1 = set2.getInt( "mother_firstname1" );
                        break;
                    case 3:
                        var_s2_mother_firstname1 = set2.getInt( "mother_firstname1" );
                        var_s2_mother_firstname2 = set2.getInt( "mother_firstname2" );
                        break;
                    case 4:
                        var_s2_mother_firstname1 = set2.getInt( "mother_firstname1" );
                        var_s2_mother_firstname2 = set2.getInt( "mother_firstname2" );
                        var_s2_mother_firstname3 = set2.getInt( "mother_firstname3" );
                        break;
                    case 5:
                        var_s2_mother_firstname1 = set2.getInt( "mother_firstname1" );
                        var_s2_mother_firstname2 = set2.getInt( "mother_firstname2" );
                        var_s2_mother_firstname3 = set2.getInt( "mother_firstname3" );
                        var_s2_mother_firstname3 = set2.getInt( "mother_firstname4" );
                        break;
                }

                if( !ignore_minmax )
                {
                    var_s2_mother_birth_min    = set2.getInt( "mother_birth_min" );
                    var_s2_mother_birth_max    = set2.getInt( "mother_birth_max" );
                    var_s2_mother_marriage_min = set2.getInt( "mother_marriage_min" );
                    var_s2_mother_marriage_max = set2.getInt( "mother_marriage_max" );
                    var_s2_mother_death_min    = set2.getInt( "mother_death_min" );
                    var_s2_mother_death_max    = set2.getInt( "mother_death_max" );
                }
            }

            if( use_father )
            {
                // Family Name
                var_s2_father_familyname = set2.getInt( "father_familyname" );

                // First Names
                switch( firstname )
                {
                    case 1:
                        var_s2_father_firstname1 = set2.getInt( "father_firstname1" );
                        var_s2_father_firstname2 = set2.getInt( "father_firstname2" );
                        var_s2_father_firstname3 = set2.getInt( "father_firstname3" );
                        var_s2_father_firstname3 = set2.getInt( "father_firstname4" );
                        break;
                    case 2:
                        var_s2_father_firstname1 = set2.getInt( "father_firstname1" );
                        break;
                    case 3:
                        var_s2_father_firstname1 = set2.getInt( "father_firstname1" );
                        var_s2_father_firstname2 = set2.getInt( "father_firstname2" );
                        break;
                    case 4:
                        var_s2_father_firstname1 = set2.getInt( "father_firstname1" );
                        var_s2_father_firstname2 = set2.getInt( "father_firstname2" );
                        var_s2_father_firstname3 = set2.getInt( "father_firstname3" );
                        break;
                    case 5:
                        var_s2_father_firstname1 = set2.getInt( "father_firstname1" );
                        var_s2_father_firstname2 = set2.getInt( "father_firstname2" );
                        var_s2_father_firstname3 = set2.getInt( "father_firstname3" );
                        var_s2_father_firstname3 = set2.getInt( "father_firstname4" );
                        break;
                }

                if( !ignore_minmax )
                {
                    var_s2_father_birth_min    = set2.getInt( "father_birth_min" );
                    var_s2_father_birth_max    = set2.getInt( "father_birth_max" );
                    var_s2_father_marriage_min = set2.getInt( "father_marriage_min" );
                    var_s2_father_marriage_max = set2.getInt( "father_marriage_max" );
                    var_s2_father_death_min    = set2.getInt( "father_death_min" );
                    var_s2_father_death_max    = set2.getInt( "father_death_max" );
                }
            }

            if( use_partner )
            {
                // Family Name
                var_s2_partner_familyname = set2.getInt( "partner_familyname" );

                // First Names
                switch (firstname) {
                    case 1:
                        var_s2_partner_firstname1 = set2.getInt( "partner_firstname1" );
                        var_s2_partner_firstname2 = set2.getInt( "partner_firstname2" );
                        var_s2_partner_firstname3 = set2.getInt( "partner_firstname3" );
                        var_s2_partner_firstname3 = set2.getInt( "partner_firstname4" );
                        break;
                    case 2:
                        var_s2_partner_firstname1 = set2.getInt( "partner_firstname1" );
                        break;
                    case 3:
                        var_s2_partner_firstname1 = set2.getInt( "partner_firstname1" );
                        var_s2_partner_firstname2 = set2.getInt( "partner_firstname2" );
                        break;
                    case 4:
                        var_s2_partner_firstname1 = set2.getInt( "partner_firstname1" );
                        var_s2_partner_firstname2 = set2.getInt( "partner_firstname2" );
                        var_s2_partner_firstname3 = set2.getInt( "partner_firstname3" );
                        break;
                    case 5:
                        var_s2_partner_firstname1 = set2.getInt( "partner_firstname1" );
                        var_s2_partner_firstname2 = set2.getInt( "partner_firstname2" );
                        var_s2_partner_firstname3 = set2.getInt( "partner_firstname3" );
                        var_s2_partner_firstname3 = set2.getInt( "partner_firstname4" );
                        break;
                }

                if( !ignore_minmax )
                {
                    var_s2_partner_birth_min    = set2.getInt( "partner_birth_min" );
                    var_s2_partner_birth_max    = set2.getInt( "partner_birth_max" );
                    var_s2_partner_marriage_min = set2.getInt( "partner_marriage_min" );
                    var_s2_partner_marriage_max = set2.getInt( "partner_marriage_max" );
                    var_s2_partner_death_min    = set2.getInt( "partner_death_min" );
                    var_s2_partner_death_max    = set2.getInt( "partner_death_max" );
                }
            }

            //convert sex to int
            if( !ignore_sex )
            {
                String s = set2.getString( "ego_sex" );

                if( s.equalsIgnoreCase( "v" ) )      { var_s2_sex = 1; }
                else if( s.equalsIgnoreCase( "m" ) ) { var_s2_sex = 2; }
                else                                 { var_s2_sex = 0; }
            }

            //fill the arraylists
            s2_id_base.add( var_s2_id_base );
            s2_registration_days.add( var_s2_registration_days );

            s2_ego_familyname_str.add( var_s2_ego_familyname_str );
            s2_ego_firstname1_str.add( var_s2_ego_firstname1_str );

            s2_ego_familyname  .add( var_s2_ego_familyname );
            s2_ego_firstname1  .add( var_s2_ego_firstname1 );
            s2_ego_firstname2  .add( var_s2_ego_firstname2 );
            s2_ego_firstname3  .add( var_s2_ego_firstname3) ;
            s2_ego_firstname4  .add( var_s2_ego_firstname4 );
            s2_ego_birth_min   .add( var_s2_ego_birth_min );
            s2_ego_birth_max   .add( var_s2_ego_birth_max );
            s2_ego_marriage_min.add( var_s2_ego_marriage_min );
            s2_ego_marriage_max.add( var_s2_ego_marriage_max );
            s2_ego_death_min   .add( var_s2_ego_death_min );
            s2_ego_death_max   .add( var_s2_ego_death_max );

            s2_sex.add( var_s2_sex );

            s2_mother_familyname  .add( var_s2_mother_familyname );
            s2_mother_firstname1  .add( var_s2_mother_firstname1 );
            s2_mother_firstname2  .add( var_s2_mother_firstname2 );
            s2_mother_firstname3  .add( var_s2_mother_firstname3 );
            s2_mother_firstname4  .add( var_s2_mother_firstname4 );
            s2_mother_birth_min   .add( var_s2_mother_birth_min );
            s2_mother_birth_max   .add( var_s2_mother_birth_max );
            s2_mother_marriage_min.add( var_s2_mother_marriage_min );
            s2_mother_marriage_max.add( var_s2_mother_marriage_max );
            s2_mother_death_min   .add( var_s2_mother_death_min );
            s2_mother_death_max   .add( var_s2_mother_death_max );

            s2_father_familyname  .add( var_s2_father_familyname );
            s2_father_firstname1  .add( var_s2_father_firstname1 );
            s2_father_firstname2  .add( var_s2_father_firstname2 );
            s2_father_firstname3  .add( var_s2_father_firstname3 );
            s2_father_firstname4  .add( var_s2_father_firstname4 );
            s2_father_birth_min   .add( var_s2_father_birth_min );
            s2_father_birth_max   .add( var_s2_father_birth_max );
            s2_father_marriage_min.add( var_s2_father_marriage_min );
            s2_father_marriage_max.add( var_s2_father_marriage_max );
            s2_father_death_min   .add( var_s2_father_death_min );
            s2_father_death_max   .add( var_s2_father_death_max );

            s2_partner_familyname  .add( var_s2_partner_familyname );
            s2_partner_firstname1  .add( var_s2_partner_firstname1 );
            s2_partner_firstname2  .add( var_s2_partner_firstname2 );
            s2_partner_firstname3  .add( var_s2_partner_firstname3 );
            s2_partner_firstname4  .add( var_s2_partner_firstname4 );
            s2_partner_birth_min   .add( var_s2_partner_birth_min );
            s2_partner_birth_max   .add( var_s2_partner_birth_max );
            s2_partner_marriage_min.add( var_s2_partner_marriage_min );
            s2_partner_marriage_max.add( var_s2_partner_marriage_max );
            s2_partner_death_min   .add( var_s2_partner_death_min );
            s2_partner_death_max   .add( var_s2_partner_death_max );
        }

        // Freeing memory
        set1.close();
        set2.close();

        set1 = null;
        set2 = null;
    }


    public void freeVectors() throws Exception
    {
        s1_id_base              .clear(); s1_id_base = null;
        s1_registration_days    .clear(); s1_registration_days    = null;

        s1_ego_familyname_str   .clear(); s1_ego_familyname_str   = null;
        s1_ego_firstname1_str   .clear(); s1_ego_firstname1_str   = null;

        s1_ego_familyname       .clear(); s1_ego_familyname       = null;
        s1_ego_firstname1       .clear(); s1_ego_firstname1       = null;
        s1_ego_firstname2       .clear(); s1_ego_firstname2       = null;
        s1_ego_firstname3       .clear(); s1_ego_firstname3       = null;
        s1_ego_firstname4       .clear(); s1_ego_firstname4       = null;
        s1_ego_birth_min        .clear(); s1_ego_birth_min        = null;
        s1_ego_birth_max        .clear(); s1_ego_birth_max        = null;
        s1_ego_marriage_min     .clear(); s1_ego_marriage_min     = null;
        s1_ego_marriage_max     .clear(); s1_ego_marriage_max     = null;
        s1_ego_death_min        .clear(); s1_ego_death_min        = null;
        s1_ego_death_max        .clear(); s1_ego_death_max        = null;

        s1_sex                  .clear(); s1_sex                  = null;

        s1_mother_familyname    .clear(); s1_mother_familyname    = null;
        s1_mother_firstname1    .clear(); s1_mother_firstname1    = null;
        s1_mother_firstname2    .clear(); s1_mother_firstname2    = null;
        s1_mother_firstname3    .clear(); s1_mother_firstname3    = null;
        s1_mother_firstname4    .clear(); s1_mother_firstname4    = null;
        s1_mother_birth_min     .clear(); s1_mother_birth_min     = null;
        s1_mother_birth_max     .clear(); s1_mother_birth_max     = null;
        s1_mother_marriage_min  .clear(); s1_mother_marriage_min  = null;
        s1_mother_marriage_max  .clear(); s1_mother_marriage_max  = null;
        s1_mother_death_min     .clear(); s1_mother_death_min     = null;
        s1_mother_death_max     .clear(); s1_mother_death_max     = null;

        s1_father_familyname    .clear(); s1_father_familyname    = null;
        s1_father_firstname1    .clear(); s1_father_firstname1    = null;
        s1_father_firstname2    .clear(); s1_father_firstname2    = null;
        s1_father_firstname3    .clear(); s1_father_firstname3    = null;
        s1_father_firstname4    .clear(); s1_father_firstname4    = null;
        s1_father_birth_min     .clear(); s1_father_birth_min     = null;
        s1_father_birth_max     .clear(); s1_father_birth_max     = null;
        s1_father_marriage_min  .clear(); s1_father_marriage_min  = null;
        s1_father_marriage_max  .clear(); s1_father_marriage_max  = null;
        s1_father_death_min     .clear(); s1_father_death_min     = null;
        s1_father_death_max     .clear(); s1_father_death_max     = null;

        s1_partner_familyname   .clear(); s1_partner_familyname   = null;
        s1_partner_firstname1   .clear(); s1_partner_firstname1   = null;
        s1_partner_firstname2   .clear(); s1_partner_firstname2   = null;
        s1_partner_firstname3   .clear(); s1_partner_firstname3   = null;
        s1_partner_firstname4   .clear(); s1_partner_firstname4   = null;
        s1_partner_birth_min    .clear(); s1_partner_birth_min    = null;
        s1_partner_birth_max    .clear(); s1_partner_birth_max    = null;
        s1_partner_marriage_min .clear(); s1_partner_marriage_min = null;
        s1_partner_marriage_max .clear(); s1_partner_marriage_max = null;
        s1_partner_death_min    .clear(); s1_partner_death_min    = null;
        s1_partner_death_max    .clear(); s1_partner_death_max    = null;


        s2_id_base              .clear(); s2_id_base = null;
        s2_registration_days    .clear(); s2_registration_days    = null;

        s2_ego_familyname_str   .clear(); s2_ego_familyname_str   = null;
        s2_ego_firstname1_str   .clear(); s2_ego_firstname1_str   = null;

        s2_ego_familyname       .clear(); s2_ego_familyname       = null;
        s2_ego_firstname1       .clear(); s2_ego_firstname1       = null;
        s2_ego_firstname2       .clear(); s2_ego_firstname2       = null;
        s2_ego_firstname3       .clear(); s2_ego_firstname3       = null;
        s2_ego_firstname4       .clear(); s2_ego_firstname4       = null;
        s2_ego_birth_min        .clear(); s2_ego_birth_min        = null;
        s2_ego_birth_max        .clear(); s2_ego_birth_max        = null;
        s2_ego_marriage_min     .clear(); s2_ego_marriage_min     = null;
        s2_ego_marriage_max     .clear(); s2_ego_marriage_max     = null;
        s2_ego_death_min        .clear(); s2_ego_death_min        = null;
        s2_ego_death_max        .clear(); s2_ego_death_max        = null;

        s2_sex                  .clear(); s2_sex                  = null;

        s2_mother_familyname    .clear(); s2_mother_familyname    = null;
        s2_mother_firstname1    .clear(); s2_mother_firstname1    = null;
        s2_mother_firstname2    .clear(); s2_mother_firstname2    = null;
        s2_mother_firstname3    .clear(); s2_mother_firstname3    = null;
        s2_mother_firstname4    .clear(); s2_mother_firstname4    = null;
        s2_mother_birth_min     .clear(); s2_mother_birth_min     = null;
        s2_mother_birth_max     .clear(); s2_mother_birth_max     = null;
        s2_mother_marriage_min  .clear(); s2_mother_marriage_min  = null;
        s2_mother_marriage_max  .clear(); s2_mother_marriage_max  = null;
        s2_mother_death_min     .clear(); s2_mother_death_min     = null;
        s2_mother_death_max     .clear(); s2_mother_death_max     = null;

        s2_father_familyname    .clear(); s2_father_familyname    = null;
        s2_father_firstname1    .clear(); s2_father_firstname1    = null;
        s2_father_firstname2    .clear(); s2_father_firstname2    = null;
        s2_father_firstname3    .clear(); s2_father_firstname3    = null;
        s2_father_firstname4    .clear(); s2_father_firstname4    = null;
        s2_father_birth_min     .clear(); s2_father_birth_min     = null;
        s2_father_birth_max     .clear(); s2_father_birth_max     = null;
        s2_father_marriage_min  .clear(); s2_father_marriage_min  = null;
        s2_father_marriage_max  .clear(); s2_father_marriage_max  = null;
        s2_father_death_min     .clear(); s2_father_death_min     = null;
        s2_father_death_max     .clear(); s2_father_death_max     = null;

        s2_partner_familyname   .clear(); s2_partner_familyname   = null;
        s2_partner_firstname1   .clear(); s2_partner_firstname1   = null;
        s2_partner_firstname2   .clear(); s2_partner_firstname2   = null;
        s2_partner_firstname3   .clear(); s2_partner_firstname3   = null;
        s2_partner_firstname4   .clear(); s2_partner_firstname4   = null;
        s2_partner_birth_min    .clear(); s2_partner_birth_min    = null;
        s2_partner_birth_max    .clear(); s2_partner_birth_max    = null;
        s2_partner_marriage_min .clear(); s2_partner_marriage_min = null;
        s2_partner_marriage_max .clear(); s2_partner_marriage_max = null;
        s2_partner_death_min    .clear(); s2_partner_death_min    = null;
        s2_partner_death_max    .clear(); s2_partner_death_max    = null;
    }
}
