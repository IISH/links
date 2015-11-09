package linksmatchmanager;

import java.sql.Connection;
import java.sql.ResultSet;

import java.util.Vector;

import linksmatchmanager.DataSet.QuerySet;

/**
 * @author Fons Laan
 *
 * <p/>
 * FL-09-Nov-2015 Created
 * FL-09-Nov-2015 Latest change
 *
 * Replacement of QueryLoader:
 * QueryLoader combines the s1 & s2 samples, we prefer to keep them separately
 *
 * TODO incomplete, untested
 */
public class SampleLoader
{
    private Connection dbconPrematch;

    private boolean use_mother;
    private boolean use_father;
    private boolean use_partner;
    private boolean ignore_sex;
    private boolean ignore_minmax;

    private int firstname;

  //private ResultSet set1;
  //private ResultSet set2;
    private ResultSet rs;

    public int set_no;
    public String query;

    // Set variables
    public Vector< Integer > id_base              = new Vector< Integer >();
    public Vector< Integer > registration_days    = new Vector< Integer >();

    public Vector< String > ego_familyname_str    = new Vector< String >();
    public Vector< String > ego_firstname1_str    = new Vector< String >();

    public Vector< Integer > ego_familyname       = new Vector< Integer >();
    public Vector< Integer > ego_firstname1       = new Vector< Integer >();
    public Vector< Integer > ego_firstname2       = new Vector< Integer >();
    public Vector< Integer > ego_firstname3       = new Vector< Integer >();
    public Vector< Integer > ego_firstname4       = new Vector< Integer >();
    public Vector< Integer > ego_birth_min        = new Vector< Integer >();
    public Vector< Integer > ego_birth_max        = new Vector< Integer >();
    public Vector< Integer > ego_marriage_min     = new Vector< Integer >();
    public Vector< Integer > ego_marriage_max     = new Vector< Integer >();
    public Vector< Integer > ego_death_min        = new Vector< Integer >();
    public Vector< Integer > ego_death_max        = new Vector< Integer >();

    public Vector< Integer > sex                  = new Vector< Integer >();

    public Vector< Integer > mother_familyname    = new Vector< Integer >();
    public Vector< Integer > mother_firstname1    = new Vector< Integer >();
    public Vector< Integer > mother_firstname2    = new Vector< Integer >();
    public Vector< Integer > mother_firstname3    = new Vector< Integer >();
    public Vector< Integer > mother_firstname4    = new Vector< Integer >();
    public Vector< Integer > mother_birth_min     = new Vector< Integer >();
    public Vector< Integer > mother_birth_max     = new Vector< Integer >();
    public Vector< Integer > mother_marriage_min  = new Vector< Integer >();
    public Vector< Integer > mother_marriage_max  = new Vector< Integer >();
    public Vector< Integer > mother_death_min     = new Vector< Integer >();
    public Vector< Integer > mother_death_max     = new Vector< Integer >();

    public Vector< Integer > father_familyname    = new Vector< Integer >();
    public Vector< Integer > father_firstname1    = new Vector< Integer >();
    public Vector< Integer > father_firstname2    = new Vector< Integer >();
    public Vector< Integer > father_firstname3    = new Vector< Integer >();
    public Vector< Integer > father_firstname4    = new Vector< Integer >();
    public Vector< Integer > father_birth_min     = new Vector< Integer >();
    public Vector< Integer > father_birth_max     = new Vector< Integer >();
    public Vector< Integer > father_marriage_min  = new Vector< Integer >();
    public Vector< Integer > father_marriage_max  = new Vector< Integer >();
    public Vector< Integer > father_death_min     = new Vector< Integer >();
    public Vector< Integer > father_death_max     = new Vector< Integer >();

    public Vector< Integer > partner_familyname   = new Vector< Integer >();
    public Vector< Integer > partner_firstname1   = new Vector< Integer >();
    public Vector< Integer > partner_firstname2   = new Vector< Integer >();
    public Vector< Integer > partner_firstname3   = new Vector< Integer >();
    public Vector< Integer > partner_firstname4   = new Vector< Integer >();
    public Vector< Integer > partner_birth_min    = new Vector< Integer >();
    public Vector< Integer > partner_birth_max    = new Vector< Integer >();
    public Vector< Integer > partner_marriage_min = new Vector< Integer >();
    public Vector< Integer > partner_marriage_max = new Vector< Integer >();
    public Vector< Integer > partner_death_min    = new Vector< Integer >();
    public Vector< Integer > partner_death_max    = new Vector< Integer >();


    /**
     *
     * @param qs
     * @param dbconPrematch
     */
    public SampleLoader( long threadId, QuerySet qs, Connection dbconPrematch, int set_no ) throws Exception
    {
        this.use_mother    = qs.use_mother;
        this.use_father    = qs.use_father;
        this.use_partner   = qs.use_partner;
        this.firstname     = qs.firstname;
        this.ignore_sex    = qs.ignore_sex;
        this.ignore_minmax = qs.ignore_minmax;

        this.set_no = set_no;
        rs = null;
        if( set_no == 1 ) {
            // get set 1 from links_base
            query = qs.query1;
            System.out.println( "Thread id " + threadId + "; retrieving set 1 from links_base..." );
            System.out.println( query );
            rs = dbconPrematch.createStatement().executeQuery( query );
        }
        else if( set_no == 2 ) {
            // get set 2 from links_base
            query = qs.query2;
            System.out.println( "Thread id " + threadId + "; retrieving set 2 from links_base..." );
            System.out.println( query );

            rs = dbconPrematch.createStatement().executeQuery( query );
            //set2 = dbconPrematch.createStatement().executeQuery( qs.query1 );     // only for matching TEST !
        }

        fillArrays();

        this.dbconPrematch = dbconPrematch;
    }


    private void fillArrays() throws Exception
    {
        while( rs.next() )
        {
            // Vars to use, global
            int var_id_base = 0;
            int var_registration_days = 0;

            String var_ego_familyname_str = "";
            String var_ego_firstname1_str = "";

            int var_ego_familyname   = 0;
            int var_ego_firstname1   = 0;
            int var_ego_firstname2   = 0;
            int var_ego_firstname3   = 0;
            int var_ego_firstname4   = 0;
            int var_ego_birth_min    = 0;
            int var_ego_birth_max    = 0;
            int var_ego_marriage_min = 0;
            int var_ego_marriage_max = 0;
            int var_ego_death_min    = 0;
            int var_ego_death_max    = 0;

            int var_sex = 0;
            int var_mother_familyname   = 0;
            int var_mother_firstname1   = 0;
            int var_mother_firstname2   = 0;
            int var_mother_firstname3   = 0;
            int var_mother_firstname4   = 0;
            int var_mother_birth_min    = 0;
            int var_mother_birth_max    = 0;
            int var_mother_marriage_min = 0;
            int var_mother_marriage_max = 0;
            int var_mother_death_min    = 0;
            int var_mother_death_max    = 0;

            int var_father_familyname   = 0;
            int var_father_firstname1   = 0;
            int var_father_firstname2   = 0;
            int var_father_firstname3   = 0;
            int var_father_firstname4   = 0;
            int var_father_birth_min    = 0;
            int var_father_birth_max    = 0;
            int var_father_marriage_min = 0;
            int var_father_marriage_max = 0;
            int var_father_death_min    = 0;
            int var_father_death_max    = 0;

            int var_partner_familyname   = 0;
            int var_partner_firstname1   = 0;
            int var_partner_firstname2   = 0;
            int var_partner_firstname3   = 0;
            int var_partner_firstname4   = 0;
            int var_partner_birth_min    = 0;
            int var_partner_birth_max    = 0;
            int var_partner_marriage_min = 0;
            int var_partner_marriage_max = 0;
            int var_partner_death_min    = 0;
            int var_partner_death_max    = 0;

            // Get all vars from table
            var_id_base = rs.getInt( "id_base" );
            var_registration_days = rs.getInt( "registration_days" );

            // Ego
            // Familyname
            var_ego_familyname     = rs.getInt(    "ego_familyname" );
            var_ego_familyname_str = rs.getString( "ego_familyname_str" );
            var_ego_firstname1_str = rs.getString( "ego_firstname1_str" );

            // First Names ego
            switch( firstname )     // firstname method:
            {
                case 1:
                    var_ego_firstname1 = rs.getInt( "ego_firstname1" );
                    var_ego_firstname2 = rs.getInt( "ego_firstname2" );
                    var_ego_firstname3 = rs.getInt( "ego_firstname3" );
                    var_ego_firstname3 = rs.getInt( "ego_firstname4" );
                    break;
                case 2:
                    var_ego_firstname1 = rs.getInt( "ego_firstname1" );
                    break;
                case 3:
                    var_ego_firstname1 = rs.getInt( "ego_firstname1" );
                    var_ego_firstname2 = rs.getInt( "ego_firstname2" );
                    break;
                case 4:
                    var_ego_firstname1 = rs.getInt( "ego_firstname1" );
                    var_ego_firstname2 = rs.getInt( "ego_firstname2" );
                    var_ego_firstname3 = rs.getInt( "ego_firstname3" );
                    break;
                case 5:
                    var_ego_firstname1 = rs.getInt( "ego_firstname1" );
                    var_ego_firstname2 = rs.getInt( "ego_firstname2" );
                    var_ego_firstname3 = rs.getInt( "ego_firstname3" );
                    var_ego_firstname3 = rs.getInt( "ego_firstname4" );
                    break;
            }

            if( ! ignore_minmax )
            {
                var_ego_birth_min    = rs.getInt( "ego_birth_min" );
                var_ego_birth_max    = rs.getInt( "ego_birth_max" );
                var_ego_marriage_min = rs.getInt( "ego_marriage_min" );
                var_ego_marriage_max = rs.getInt( "ego_marriage_max" );
                var_ego_death_min    = rs.getInt( "ego_death_min" );
                var_ego_death_max    = rs.getInt( "ego_death_max" );
            }

            if( use_mother )
            {
                // Family name
                var_mother_familyname = rs.getInt( "mother_familyname" );

                // First name
                switch( firstname )
                {
                    case 1:
                        var_mother_firstname1 = rs.getInt( "mother_firstname1" );
                        var_mother_firstname2 = rs.getInt( "mother_firstname2" );
                        var_mother_firstname3 = rs.getInt( "mother_firstname3" );
                        var_mother_firstname3 = rs.getInt( "mother_firstname4" );
                        break;
                    case 2:
                        var_mother_firstname1 = rs.getInt( "mother_firstname1" );
                        break;
                    case 3:
                        var_mother_firstname1 = rs.getInt( "mother_firstname1" );
                        var_mother_firstname2 = rs.getInt( "mother_firstname2" );
                        break;
                    case 4:
                        var_mother_firstname1 = rs.getInt( "mother_firstname1" );
                        var_mother_firstname2 = rs.getInt( "mother_firstname2" );
                        var_mother_firstname3 = rs.getInt( "mother_firstname3" );
                        break;
                    case 5:
                        var_mother_firstname1 = rs.getInt( "mother_firstname1" );
                        var_mother_firstname2 = rs.getInt( "mother_firstname2" );
                        var_mother_firstname3 = rs.getInt( "mother_firstname3" );
                        var_mother_firstname3 = rs.getInt( "mother_firstname4" );
                        break;
                }

                if( ! ignore_minmax )
                {
                    var_mother_birth_min    = rs.getInt( "mother_birth_min" );
                    var_mother_birth_max    = rs.getInt( "mother_birth_max" );
                    var_mother_marriage_min = rs.getInt( "mother_marriage_min" );
                    var_mother_marriage_max = rs.getInt( "mother_marriage_max" );
                    var_mother_death_min    = rs.getInt( "mother_death_min" );
                    var_mother_death_max    = rs.getInt( "mother_death_max" );
                }
            }

            if( use_father )
            {
                // Family Name
                var_father_familyname = rs.getInt( "father_familyname" );

                // First Names
                switch( firstname )
                {
                    case 1:
                        var_father_firstname1 = rs.getInt( "father_firstname1" );
                        var_father_firstname2 = rs.getInt( "father_firstname2" );
                        var_father_firstname3 = rs.getInt( "father_firstname3" );
                        var_father_firstname3 = rs.getInt( "father_firstname4" );
                        break;
                    case 2:
                        var_father_firstname1 = rs.getInt( "father_firstname1" );
                        break;
                    case 3:
                        var_father_firstname1 = rs.getInt( "father_firstname1" );
                        var_father_firstname2 = rs.getInt( "father_firstname2" );
                        break;
                    case 4:
                        var_father_firstname1 = rs.getInt("father_firstname1");
                        var_father_firstname2 = rs.getInt("father_firstname2");
                        var_father_firstname3 = rs.getInt("father_firstname3");
                        break;
                    case 5:
                        var_father_firstname1 = rs.getInt( "father_firstname1" );
                        var_father_firstname2 = rs.getInt( "father_firstname2" );
                        var_father_firstname3 = rs.getInt( "father_firstname3" );
                        var_father_firstname3 = rs.getInt( "father_firstname4" );
                        break;
                }

                if( ! ignore_minmax )
                {
                    var_father_birth_min    = rs.getInt( "father_birth_min" );
                    var_father_birth_max    = rs.getInt( "father_birth_max" );
                    var_father_marriage_min = rs.getInt( "father_marriage_min" );
                    var_father_marriage_max = rs.getInt( "father_marriage_max" );
                    var_father_death_min    = rs.getInt( "father_death_min" );
                    var_father_death_max    = rs.getInt( "father_death_max" );
                }
            }

            if( use_partner )
            {
                // Family Name
                var_partner_familyname = rs.getInt( "partner_familyname" );

                // First Names
                switch( firstname )
                {
                    case 1:
                        var_partner_firstname1 = rs.getInt( "partner_firstname1" );
                        var_partner_firstname2 = rs.getInt( "partner_firstname2" );
                        var_partner_firstname3 = rs.getInt( "partner_firstname3" );
                        var_partner_firstname3 = rs.getInt( "partner_firstname4" );
                        break;
                    case 2:
                        var_partner_firstname1 = rs.getInt( "partner_firstname1" );
                        break;
                    case 3:
                        var_partner_firstname1 = rs.getInt( "partner_firstname1" );
                        var_partner_firstname2 = rs.getInt( "partner_firstname2" );
                        break;
                    case 4:
                        var_partner_firstname1 = rs.getInt( "partner_firstname1" );
                        var_partner_firstname2 = rs.getInt( "partner_firstname2" );
                        var_partner_firstname3 = rs.getInt( "partner_firstname3" );
                        break;
                    case 5:
                        var_partner_firstname1 = rs.getInt( "partner_firstname1" );
                        var_partner_firstname2 = rs.getInt( "partner_firstname2" );
                        var_partner_firstname3 = rs.getInt( "partner_firstname3" );
                        var_partner_firstname3 = rs.getInt( "partner_firstname4" );
                        break;
                }

                if( !ignore_minmax )
                {
                    var_partner_birth_min    = rs.getInt( "partner_birth_min" );
                    var_partner_birth_max    = rs.getInt( "partner_birth_max" );
                    var_partner_marriage_min = rs.getInt( "partner_marriage_min" );
                    var_partner_marriage_max = rs.getInt( "partner_marriage_max" );
                    var_partner_death_min    = rs.getInt( "partner_death_min" );
                    var_partner_death_max    = rs.getInt( "partner_death_max" );
                }
            }

            // convert sex to int
            if( !ignore_sex )
            {
                String s = rs.getString( "ego_sex" );

                if( s.equalsIgnoreCase( "v" ) )      { var_sex = 1; }
                else if( s.equalsIgnoreCase( "m" ) ) { var_sex = 2; }
                else                                 { var_sex = 0; }
            }

            // fill the arraylists
            id_base          .add( var_id_base );
            registration_days.add( var_registration_days );

            ego_familyname_str.add( var_ego_familyname_str );
            ego_firstname1_str.add( var_ego_firstname1_str );

            ego_familyname  .add( var_ego_familyname );
            ego_firstname1  .add( var_ego_firstname1 );
            ego_firstname2  .add( var_ego_firstname2 );
            ego_firstname3  .add( var_ego_firstname3 );
            ego_firstname4  .add( var_ego_firstname4 );
            ego_birth_min   .add( var_ego_birth_min );
            ego_birth_max   .add( var_ego_birth_max );
            ego_marriage_min.add( var_ego_marriage_min );
            ego_marriage_max.add( var_ego_marriage_max );
            ego_death_min   .add( var_ego_death_min );
            ego_death_max   .add( var_ego_death_max );

            sex.add(var_sex);

            mother_familyname  .add( var_mother_familyname );
            mother_firstname1  .add( var_mother_firstname1 );
            mother_firstname2  .add( var_mother_firstname2 );
            mother_firstname3  .add( var_mother_firstname3 );
            mother_firstname4  .add( var_mother_firstname4 );
            mother_birth_min   .add( var_mother_birth_min );
            mother_birth_max   .add( var_mother_birth_max );
            mother_marriage_min.add( var_mother_marriage_min );
            mother_marriage_max.add( var_mother_marriage_max );
            mother_death_min   .add( var_mother_death_min );
            mother_death_max   .add( var_mother_death_max );

            father_familyname  .add( var_father_familyname );
            father_firstname1  .add( var_father_firstname1 );
            father_firstname2  .add( var_father_firstname2 );
            father_firstname3  .add( var_father_firstname3 );
            father_firstname4  .add( var_father_firstname4) ;
            father_birth_min   .add( var_father_birth_min );
            father_birth_max   .add( var_father_birth_max );
            father_marriage_min.add( var_father_marriage_min );
            father_marriage_max.add( var_father_marriage_max );
            father_death_min   .add( var_father_death_min );
            father_death_max   .add( var_father_death_max );
            partner_familyname .add( var_partner_familyname );

            partner_firstname1  .add( var_partner_firstname1 );
            partner_firstname2  .add( var_partner_firstname2 );
            partner_firstname3  .add( var_partner_firstname3 );
            partner_firstname4  .add( var_partner_firstname4 );
            partner_birth_min   .add( var_partner_birth_min );
            partner_birth_max   .add( var_partner_birth_max );
            partner_marriage_min.add( var_partner_marriage_min );
            partner_marriage_max.add( var_partner_marriage_max );
            partner_death_min  .add(  var_partner_death_min );
            partner_death_max  .add(  var_partner_death_max );
        }

        /*
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
        */
        // Freeing memory
        rs.close();
        rs = null;

        //set2.close();
        //set2 = null;
    }


    public void freeVectors() throws Exception
    {
        id_base              .clear(); id_base = null;
        registration_days    .clear(); registration_days    = null;

        ego_familyname_str   .clear(); ego_familyname_str   = null;
        ego_firstname1_str   .clear(); ego_firstname1_str   = null;

        ego_familyname       .clear(); ego_familyname       = null;
        ego_firstname1       .clear(); ego_firstname1       = null;
        ego_firstname2       .clear(); ego_firstname2       = null;
        ego_firstname3       .clear(); ego_firstname3       = null;
        ego_firstname4       .clear(); ego_firstname4       = null;
        ego_birth_min        .clear(); ego_birth_min        = null;
        ego_birth_max        .clear(); ego_birth_max        = null;
        ego_marriage_min     .clear(); ego_marriage_min     = null;
        ego_marriage_max     .clear(); ego_marriage_max     = null;
        ego_death_min        .clear(); ego_death_min        = null;
        ego_death_max        .clear(); ego_death_max        = null;

        sex                  .clear(); sex                  = null;

        mother_familyname    .clear(); mother_familyname    = null;
        mother_firstname1    .clear(); mother_firstname1    = null;
        mother_firstname2    .clear(); mother_firstname2    = null;
        mother_firstname3    .clear(); mother_firstname3    = null;
        mother_firstname4    .clear(); mother_firstname4    = null;
        mother_birth_min     .clear(); mother_birth_min     = null;
        mother_birth_max     .clear(); mother_birth_max     = null;
        mother_marriage_min  .clear(); mother_marriage_min  = null;
        mother_marriage_max  .clear(); mother_marriage_max  = null;
        mother_death_min     .clear(); mother_death_min     = null;
        mother_death_max     .clear(); mother_death_max     = null;

        father_familyname    .clear(); father_familyname    = null;
        father_firstname1    .clear(); father_firstname1    = null;
        father_firstname2    .clear(); father_firstname2    = null;
        father_firstname3    .clear(); father_firstname3    = null;
        father_firstname4    .clear(); father_firstname4    = null;
        father_birth_min     .clear(); father_birth_min     = null;
        father_birth_max     .clear(); father_birth_max     = null;
        father_marriage_min  .clear(); father_marriage_min  = null;
        father_marriage_max  .clear(); father_marriage_max  = null;
        father_death_min     .clear(); father_death_min     = null;
        father_death_max     .clear(); father_death_max     = null;

        partner_familyname   .clear(); partner_familyname   = null;
        partner_firstname1   .clear(); partner_firstname1   = null;
        partner_firstname2   .clear(); partner_firstname2   = null;
        partner_firstname3   .clear(); partner_firstname3   = null;
        partner_firstname4   .clear(); partner_firstname4   = null;
        partner_birth_min    .clear(); partner_birth_min    = null;
        partner_birth_max    .clear(); partner_birth_max    = null;
        partner_marriage_min .clear(); partner_marriage_min = null;
        partner_marriage_max .clear(); partner_marriage_max = null;
        partner_death_min    .clear(); partner_death_min    = null;
        partner_death_max    .clear(); partner_death_max    = null;

        /*
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
        */
    }
}