package linksmatchmanager;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

import java.util.Vector;

import com.zaxxer.hikari.HikariDataSource;
//import linksmatchmanager.DatabaseManager;
import linksmatchmanager.DataSet.QuerySet;

/**
 * @author Fons Laan
 *
 * <p/>
 * FL-09-Nov-2015 Created
 * FL-22-Mar-2016 Sex: f, m, u
 * FL-03-Jan-2018 Local db connection, no longer as function parameters (connections timeouts)
 * FL-04-Mar-2019 HikariCPDataSource
 * FL-04-Mar-2019 SampleLoader not finished? What has to be done?
 * FL-11-Mar-2019 HikariDataSource
 *
 * Replacement of QueryLoader:
 * QueryLoader combines the s1 & s2 samples. SampleLoader keeps them separate,
 * because in general they need not be updated together all the time.
 */
public class SampleLoader
{
    PrintLogger plog;
    private Connection dbconPrematch;

    private boolean use_mother;
    private boolean use_father;
    private boolean use_partner;
    private boolean ignore_minmax;

    private String ignore_sex;

    private int firstname_method;

    public int sample_no = 0;
    public String query = "";
    public int record_count = 0;

    // Create Vectors for the links_base columns
    private boolean have_vectors = true;

    public Vector< Integer > id_base              = new Vector< Integer >();
    public Vector< Integer > id_registration      = new Vector< Integer >();
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

    public Vector< String > mother_familyname_str = new Vector< String >();
    public Vector< String > mother_firstname1_str = new Vector< String >();

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


    public static String millisec2hms( long millisec_start, long millisec_stop )
    {
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
    private static void elapsedShowMessage( String msg_in, long start, long stop )
    {
        String elapsed = millisec2hms( start, stop );
        String msg_out = msg_in + " " + elapsed + " elapsed";
        System.out.println( msg_out);
    } // elapsedShowMessage


    /**
     * @param qs
     * @param dsrcPrematch
     * @param sample_no
     * @throws Exception
     */
    public SampleLoader( PrintLogger plog, QuerySet qs, HikariDataSource dsrcPrematch, int sample_no )
    throws Exception
    {
        long threadId = Thread.currentThread().getId();
        this.plog = plog;
        this.use_mother       = qs.use_mother;
        this.use_father       = qs.use_father;
        this.use_partner      = qs.use_partner;
        this.firstname_method = qs.firstname_method;
        this.ignore_sex       = qs.ignore_sex;
        this.ignore_minmax    = qs.ignore_minmax;

        this.sample_no     = sample_no;

        // get sample from links_base
             if( sample_no == 1 ) { query = qs.s1_query; }
        else if( sample_no == 2 ) { query = qs.s2_query; }
        else {
            System.out.printf( "Thread id %d; SampleLoader() sample_no must be 1 or 2\n", threadId );
            return;
        }

        long start = System.currentTimeMillis();
        System.out.printf( "Thread id %02d; SampleLoader() retrieving sample %d...\n", threadId, sample_no );
        System.out.printf( "Thread id %02d; %s\n", threadId, query );

        try
        {
            dbconPrematch = dsrcPrematch.getConnection();

            try( PreparedStatement ps = dbconPrematch.prepareStatement( query ) )
            {
                try( ResultSet rs = ps.executeQuery() )
                {
                    String msg = String.format( "Thread id %02d; retrieved sample from links_base " , threadId );
                    elapsedShowMessage( msg, start, System.currentTimeMillis() );

                    System.out.printf( "Thread id %02d; filling the s1 vectors...\n", threadId );
                    fillArrays( rs );
                }
            }
        }
        catch( Exception ex ) {
            String msg = String.format( "Thread id %02d; SampleLoader() Exception: %s", threadId, ex.getMessage() );
            System.out.println( msg ); plog.show( msg );
            ex.printStackTrace( System.out );
            return;
        }
        finally
        {
            dbconPrematch.close();
            dbconPrematch = null;
        }

        System.out.printf( "Thread id %02d; SampleLoader() done\n", threadId );
    } // SampleLoader


    /**
     * @throws Exception
     */
    private void fillArrays( ResultSet rs ) throws Exception
    {
        long threadId = Thread.currentThread().getId();

        System.out.printf( "Thread id %02d; SampleLoader/fillArrays() sample_no: %s\n", threadId, sample_no );

        while( rs.next() )          // fetch records from ResultSet
        {
            record_count++;

            // initialise variables (not all are filled from the db)
            int var_id_base           = 0;
            int var_id_registration   = 0;
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

            String var_mother_familyname_str = "";
            String var_mother_firstname1_str = "";

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
            var_id_base           = rs.getInt( "id_base" );
            var_id_registration   = rs.getInt( "id_registration" );
            var_registration_days = rs.getInt( "registration_days" );

            // Ego
            // Familyname
            var_ego_familyname     = rs.getInt(    "ego_familyname" );
            var_ego_familyname_str = rs.getString( "ego_familyname_str" );
            var_ego_firstname1_str = rs.getString( "ego_firstname1_str" );

            switch( firstname_method )      // firstname method for ego
            {
                case 1:
                    var_ego_firstname1 = rs.getInt( "ego_firstname1" );
                    var_ego_firstname2 = rs.getInt( "ego_firstname2" );
                    var_ego_firstname3 = rs.getInt( "ego_firstname3" );
                    var_ego_firstname4 = rs.getInt( "ego_firstname4" );
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
                    var_ego_firstname4 = rs.getInt( "ego_firstname4" );
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
                // Familyname
                var_mother_familyname     = rs.getInt( "mother_familyname" );
                var_mother_familyname_str = rs.getString( "mother_familyname_str" );
                var_mother_firstname1_str = rs.getString( "mother_firstname1_str" );

                // Firstname
                switch( firstname_method )      // firstname method for mother
                {
                    case 1:
                        var_mother_firstname1 = rs.getInt( "mother_firstname1" );
                        var_mother_firstname2 = rs.getInt( "mother_firstname2" );
                        var_mother_firstname3 = rs.getInt( "mother_firstname3" );
                        var_mother_firstname4 = rs.getInt( "mother_firstname4" );
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
                        var_mother_firstname4 = rs.getInt( "mother_firstname4" );
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
                // FamilyName
                var_father_familyname = rs.getInt( "father_familyname" );

                // FirstNames
                switch( firstname_method )      // firstname method for father
                {
                    case 1:
                        var_father_firstname1 = rs.getInt( "father_firstname1" );
                        var_father_firstname2 = rs.getInt( "father_firstname2" );
                        var_father_firstname3 = rs.getInt( "father_firstname3" );
                        var_father_firstname4 = rs.getInt( "father_firstname4" );
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
                        var_father_firstname4 = rs.getInt( "father_firstname4" );
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
                // FamilyName
                var_partner_familyname = rs.getInt( "partner_familyname" );

                // FirstNames
                switch( firstname_method )      // firstname method for partner
                {
                    case 1:
                        var_partner_firstname1 = rs.getInt( "partner_firstname1" );
                        var_partner_firstname2 = rs.getInt( "partner_firstname2" );
                        var_partner_firstname3 = rs.getInt( "partner_firstname3" );
                        var_partner_firstname4 = rs.getInt( "partner_firstname4" );
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
                        var_partner_firstname4 = rs.getInt( "partner_firstname4" );
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
            if( ! ignore_sex.equals( "y" ) )
            {
                String s = rs.getString( "ego_sex" );

                     if( s.equals( "f" ) ) { var_sex = 1; }    // female
                else if( s.equals( "m" ) ) { var_sex = 2; }    // male
                else                       { var_sex = 0; }    // 'u' = unknown

                //System.out.println( "id_registration: " + var_id_registration + ", sex: " + s + ", var_sex: " + var_sex  );
            }

            // fill the Vectors
            id_base          .add( var_id_base );
            id_registration  .add( var_id_registration );
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

            sex.add( var_sex );

            mother_familyname_str.add( var_mother_familyname_str );
            mother_firstname1_str.add( var_mother_firstname1_str );

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

        System.out.println( String.format( "Thread id %02d; SampleLoader/fillArrays() sample record_count: %d", threadId, record_count ) );
    } // fillArrays


    /**
     * @throws Exception
     */
    public void freeVectors() throws Exception
    {
        long threadId = Thread.currentThread().getId();

        if( ! have_vectors ) { System.out.printf( "Thread id %02d; SampleLoader/freeVectors(): sample_no %d already freed?\n", threadId, sample_no ); }
        else
        {
            System.out.printf( "Thread id %02d; SampleLoader/freeVectors() sample_no: %s\n", threadId, sample_no );

            id_base              .clear(); id_base              = null;
            id_registration      .clear(); id_registration      = null;
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

            mother_familyname_str.clear(); mother_familyname_str= null;
            mother_firstname1_str.clear(); mother_firstname1_str= null;

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

            have_vectors = false;
        }
    } // freeVectors
}
