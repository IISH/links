package linksmatchmanager;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

import java.util.Vector;

import com.zaxxer.hikari.HikariDataSource;
//import linksmatchmanager.DatabaseManager;

import linksmatchmanager.DataSet.QuerySet;

/**
 * @author Omar Azouguagh
 * @author Fons Laan
 *
 * <p/>
 * FL-30-Jun-2014 Imported from OA backup
 * FL-30-Apr-2015 Free vectors
 * FL-04-Jan-2018 Local db connection, no longer as function parameters (connections timeouts)
 * FL-05-Jan-2018 Split fillArrays()
 * FL-02-Oct-2018 Added s1_id_persist_registration & s2_id_persist_registration to the vector zoo
 * FL-12-Mar-2019 HikariDataSource
 * FL-29-Apr-2019 Using PreparedStatement
 * FL-03-Dec-2019 Latest change
 * FL-27-Feb-2020 Added s1_id_person_o & s2_id_person_o to the vector zoo
 *
 * See SampleLoader for a variant that keeps s1 and s2 separate.
 */
public class QueryLoader
{
    private boolean use_mother;
    private boolean use_father;
    private boolean use_partner;
    private boolean ignore_minmax;

    //private boolean ignore_sex;
    private String ignore_sex;

    private int firstname_method;

    PrintLogger plog;

    // Set variables
    public Vector< Integer > s1_id_base                = new Vector< Integer >();
    public Vector< Integer > s1_id_registration        = new Vector< Integer >();
    public Vector< Integer > s1_registration_days      = new Vector< Integer >();

    public Vector< String > s1_id_persist_registration = new Vector< String >();
    public Vector< Integer > s1_id_person_o            = new Vector< Integer >();
    public Vector< String > s1_ego_familyname_str      = new Vector< String >();
    public Vector< String > s1_ego_firstname1_str      = new Vector< String >();

    public Vector< Integer > s1_ego_familyname         = new Vector< Integer >();
    public Vector< Integer > s1_ego_firstname1         = new Vector< Integer >();
    public Vector< Integer > s1_ego_firstname2         = new Vector< Integer >();
    public Vector< Integer > s1_ego_firstname3         = new Vector< Integer >();
    public Vector< Integer > s1_ego_firstname4         = new Vector< Integer >();
    public Vector< Integer > s1_ego_birth_min          = new Vector< Integer >();
    public Vector< Integer > s1_ego_birth_max          = new Vector< Integer >();
    public Vector< Integer > s1_ego_marriage_min       = new Vector< Integer >();
    public Vector< Integer > s1_ego_marriage_max       = new Vector< Integer >();
    public Vector< Integer > s1_ego_death_min          = new Vector< Integer >();
    public Vector< Integer > s1_ego_death_max          = new Vector< Integer >();

    public Vector< Integer > s1_sex                    = new Vector< Integer >();

    public Vector< String > s1_mother_familyname_str   = new Vector< String >();
    public Vector< String > s1_mother_firstname1_str   = new Vector< String >();

    public Vector< Integer > s1_mother_familyname      = new Vector< Integer >();
    public Vector< Integer > s1_mother_firstname1      = new Vector< Integer >();
    public Vector< Integer > s1_mother_firstname2      = new Vector< Integer >();
    public Vector< Integer > s1_mother_firstname3      = new Vector< Integer >();
    public Vector< Integer > s1_mother_firstname4      = new Vector< Integer >();
    public Vector< Integer > s1_mother_birth_min       = new Vector< Integer >();
    public Vector< Integer > s1_mother_birth_max       = new Vector< Integer >();
    public Vector< Integer > s1_mother_marriage_min    = new Vector< Integer >();
    public Vector< Integer > s1_mother_marriage_max    = new Vector< Integer >();
    public Vector< Integer > s1_mother_death_min       = new Vector< Integer >();
    public Vector< Integer > s1_mother_death_max       = new Vector< Integer >();

    public Vector< Integer > s1_father_familyname      = new Vector< Integer >();
    public Vector< Integer > s1_father_firstname1      = new Vector< Integer >();
    public Vector< Integer > s1_father_firstname2      = new Vector< Integer >();
    public Vector< Integer > s1_father_firstname3      = new Vector< Integer >();
    public Vector< Integer > s1_father_firstname4      = new Vector< Integer >();
    public Vector< Integer > s1_father_birth_min       = new Vector< Integer >();
    public Vector< Integer > s1_father_birth_max       = new Vector< Integer >();
    public Vector< Integer > s1_father_marriage_min    = new Vector< Integer >();
    public Vector< Integer > s1_father_marriage_max    = new Vector< Integer >();
    public Vector< Integer > s1_father_death_min       = new Vector< Integer >();
    public Vector< Integer > s1_father_death_max       = new Vector< Integer >();

    public Vector< Integer > s1_partner_familyname     = new Vector< Integer >();
    public Vector< Integer > s1_partner_firstname1     = new Vector< Integer >();
    public Vector< Integer > s1_partner_firstname2     = new Vector< Integer >();
    public Vector< Integer > s1_partner_firstname3     = new Vector< Integer >();
    public Vector< Integer > s1_partner_firstname4     = new Vector< Integer >();
    public Vector< Integer > s1_partner_birth_min      = new Vector< Integer >();
    public Vector< Integer > s1_partner_birth_max      = new Vector< Integer >();
    public Vector< Integer > s1_partner_marriage_min   = new Vector< Integer >();
    public Vector< Integer > s1_partner_marriage_max   = new Vector< Integer >();
    public Vector< Integer > s1_partner_death_min      = new Vector< Integer >();
    public Vector< Integer > s1_partner_death_max      = new Vector< Integer >();


    public Vector< Integer > s2_id_base                = new Vector< Integer >();
    public Vector< Integer > s2_id_registration        = new Vector< Integer >();
    public Vector< Integer > s2_registration_days      = new Vector< Integer >();

    public Vector< String > s2_id_persist_registration = new Vector< String >();
    public Vector< Integer > s2_id_person_o            = new Vector< Integer >();
    public Vector< String > s2_ego_familyname_str      = new Vector< String >();
    public Vector< String > s2_ego_firstname1_str      = new Vector< String >();

    public Vector< Integer > s2_ego_familyname         = new Vector< Integer >();
    public Vector< Integer > s2_ego_firstname1         = new Vector< Integer >();
    public Vector< Integer > s2_ego_firstname2         = new Vector< Integer >();
    public Vector< Integer > s2_ego_firstname3         = new Vector< Integer >();
    public Vector< Integer > s2_ego_firstname4         = new Vector< Integer >();
    public Vector< Integer > s2_ego_birth_min          = new Vector< Integer >();
    public Vector< Integer > s2_ego_birth_max          = new Vector< Integer >();
    public Vector< Integer > s2_ego_marriage_min       = new Vector< Integer >();
    public Vector< Integer > s2_ego_marriage_max       = new Vector< Integer >();
    public Vector< Integer > s2_ego_death_min          = new Vector< Integer >();
    public Vector< Integer > s2_ego_death_max          = new Vector< Integer >();

    public Vector< Integer > s2_sex                    = new Vector< Integer >();

    public Vector< String > s2_mother_familyname_str   = new Vector< String >();
    public Vector< String > s2_mother_firstname1_str   = new Vector< String >();

    public Vector< Integer > s2_mother_familyname      = new Vector< Integer >();
    public Vector< Integer > s2_mother_firstname1      = new Vector< Integer >();
    public Vector< Integer > s2_mother_firstname2      = new Vector< Integer >();
    public Vector< Integer > s2_mother_firstname3      = new Vector< Integer >();
    public Vector< Integer > s2_mother_firstname4      = new Vector< Integer >();
    public Vector< Integer > s2_mother_birth_min       = new Vector< Integer >();
    public Vector< Integer > s2_mother_birth_max       = new Vector< Integer >();
    public Vector< Integer > s2_mother_marriage_min    = new Vector< Integer >();
    public Vector< Integer > s2_mother_marriage_max    = new Vector< Integer >();
    public Vector< Integer > s2_mother_death_min       = new Vector< Integer >();
    public Vector< Integer > s2_mother_death_max       = new Vector< Integer >();

    public Vector< Integer > s2_father_familyname      = new Vector< Integer >();
    public Vector< Integer > s2_father_firstname1      = new Vector< Integer >();
    public Vector< Integer > s2_father_firstname2      = new Vector< Integer >();
    public Vector< Integer > s2_father_firstname3      = new Vector< Integer >();
    public Vector< Integer > s2_father_firstname4      = new Vector< Integer >();
    public Vector< Integer > s2_father_birth_min       = new Vector< Integer >();
    public Vector< Integer > s2_father_birth_max       = new Vector< Integer >();
    public Vector< Integer > s2_father_marriage_min    = new Vector< Integer >();
    public Vector< Integer > s2_father_marriage_max    = new Vector< Integer >();
    public Vector< Integer > s2_father_death_min       = new Vector< Integer >();
    public Vector< Integer > s2_father_death_max       = new Vector< Integer >();

    public Vector< Integer > s2_partner_familyname     = new Vector< Integer >();
    public Vector< Integer > s2_partner_firstname1     = new Vector< Integer >();
    public Vector< Integer > s2_partner_firstname2     = new Vector< Integer >();
    public Vector< Integer > s2_partner_firstname3     = new Vector< Integer >();
    public Vector< Integer > s2_partner_firstname4     = new Vector< Integer >();
    public Vector< Integer > s2_partner_birth_min      = new Vector< Integer >();
    public Vector< Integer > s2_partner_birth_max      = new Vector< Integer >();
    public Vector< Integer > s2_partner_marriage_min   = new Vector< Integer >();
    public Vector< Integer > s2_partner_marriage_max   = new Vector< Integer >();
    public Vector< Integer > s2_partner_death_min      = new Vector< Integer >();
    public Vector< Integer > s2_partner_death_max      = new Vector< Integer >();


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
     * @param plog
     * @param qs
     * @param dsrcPrematch
     */
    //public QueryLoader( PrintLogger plog, QuerySet qs, String db_url, String db_name, String db_user, String db_pass )
    public QueryLoader( PrintLogger plog, QuerySet qs, HikariDataSource dsrcPrematch )
    throws Exception
    {
        this.plog = plog;

        this.use_mother       = qs.use_mother;
        this.use_father       = qs.use_father;
        this.use_partner      = qs.use_partner;
        this.firstname_method = qs.firstname_method;
        this.ignore_sex       = qs.ignore_sex;
        this.ignore_minmax    = qs.ignore_minmax;

        long threadId = Thread.currentThread().getId();

        System.out.printf( "Thread id %02d; QueryLoader()\n", threadId  );
        Connection dbconPrematch = null;

        // get set 1 from links_base
        long start = System.currentTimeMillis();
        System.out.printf( "Thread id %02d; retrieving sample 1 from links_base...\n", threadId );
        System.out.printf( "Thread id %02d; %s\n", threadId, qs.s1_query );

        try
        {
            dbconPrematch = dsrcPrematch.getConnection();

            try( PreparedStatement pstmt1 = dbconPrematch.prepareStatement( qs.s1_query ) )
            {
                try( ResultSet rs1 = pstmt1.executeQuery() )
                {
                    String msg = String.format( "Thread id %02d; retrieved sample 1 from links_base " , threadId );
                    elapsedShowMessage( msg, start, System.currentTimeMillis() );

                    System.out.printf( "Thread id %02d; filling the s1 vectors...\n", threadId );
                    fillArrays_rs1( rs1 );
                }
            }
        }
        catch( Exception ex )
        {
            String msg = String.format( "Thread id %02d; QueryLoader() sample 1 Exception: %s", threadId, ex.getMessage() );
            System.out.println( msg ); plog.show( msg );
            ex.printStackTrace( System.out );
            return;
        }
        finally { dbconPrematch.close(); }

        // get set 2 from links_base
        start = System.currentTimeMillis();
        System.out.printf( "Thread id %02d; retrieving sample 2 from links_base...\n", threadId );
        System.out.printf( "Thread id %02d; %s\n", threadId, qs.s2_query );

        try
        {
            dbconPrematch = dsrcPrematch.getConnection();

            try( PreparedStatement pstmt2 = dbconPrematch.prepareStatement( qs.s2_query ) )
            {
                try( ResultSet rs2 = pstmt2.executeQuery() )
                {
                    String msg = String.format( "Thread id %02d; retrieved sample 2 from links_base " , threadId );
                    elapsedShowMessage( msg, start, System.currentTimeMillis() );

                    System.out.printf( "Thread id %02d; filling the s2 vectors...\n", threadId );
                    fillArrays_rs2( rs2 );
                }
            }
        }
        catch( Exception ex )
        {
            String msg = String.format( "Thread id %02d; QueryLoader() sample 2 Exception: %s", threadId, ex.getMessage() );
            System.out.println( msg ); plog.show( msg );
            ex.printStackTrace( System.out );
            return;
        }
        finally { dbconPrematch.close(); }

        System.out.printf( "Thread id %02d; QueryLoader() done\n", threadId );
    }


    private void fillArrays_rs1( ResultSet rs1 ) throws Exception
    {
        long threadId = Thread.currentThread().getId();

        int s1_record_count = 0;
        while( rs1.next() )
        {
            s1_record_count++;

            // Vars to use, global
            int var_s1_id_base = 0;
            int var_s1_id_registration = 0;
            int var_s1_registration_days = 0;

            String var_s1_id_persist_registration = "";
            int var_s1_id_person_o = 0;

            String var_s1_ego_familyname_str = "";
            String var_s1_ego_firstname1_str = "";

            int var_s1_ego_familyname = 0;
            int var_s1_ego_firstname1 = 0;
            int var_s1_ego_firstname2 = 0;
            int var_s1_ego_firstname3 = 0;
            int var_s1_ego_firstname4 = 0;
            int var_s1_ego_birth_min = 0;
            int var_s1_ego_birth_max = 0;
            int var_s1_ego_marriage_min = 0;
            int var_s1_ego_marriage_max = 0;
            int var_s1_ego_death_min = 0;
            int var_s1_ego_death_max = 0;

            int var_s1_sex = 0;

            String var_s1_mother_familyname_str = "";
            String var_s1_mother_firstname1_str = "";

            int var_s1_mother_familyname = 0;
            int var_s1_mother_firstname1 = 0;
            int var_s1_mother_firstname2 = 0;
            int var_s1_mother_firstname3 = 0;
            int var_s1_mother_firstname4 = 0;
            int var_s1_mother_birth_min = 0;
            int var_s1_mother_birth_max = 0;
            int var_s1_mother_marriage_min = 0;
            int var_s1_mother_marriage_max = 0;
            int var_s1_mother_death_min = 0;
            int var_s1_mother_death_max = 0;

            int var_s1_father_familyname = 0;
            int var_s1_father_firstname1 = 0;
            int var_s1_father_firstname2 = 0;
            int var_s1_father_firstname3 = 0;
            int var_s1_father_firstname4 = 0;
            int var_s1_father_birth_min = 0;
            int var_s1_father_birth_max = 0;
            int var_s1_father_marriage_min = 0;
            int var_s1_father_marriage_max = 0;
            int var_s1_father_death_min = 0;
            int var_s1_father_death_max = 0;

            int var_s1_partner_familyname = 0;
            int var_s1_partner_firstname1 = 0;
            int var_s1_partner_firstname2 = 0;
            int var_s1_partner_firstname3 = 0;
            int var_s1_partner_firstname4 = 0;
            int var_s1_partner_birth_min = 0;
            int var_s1_partner_birth_max = 0;
            int var_s1_partner_marriage_min = 0;
            int var_s1_partner_marriage_max = 0;
            int var_s1_partner_death_min = 0;
            int var_s1_partner_death_max = 0;

            // Get all vars from table links_base
            var_s1_id_base                 = rs1.getInt("id_base");
            var_s1_id_registration         = rs1.getInt("id_registration");
            var_s1_id_persist_registration = rs1.getString("id_persist_registration");
            var_s1_id_person_o             = rs1.getInt("id_person_o");
            var_s1_registration_days       = rs1.getInt("registration_days");

            // Ego
            // Familyname
            var_s1_ego_familyname = rs1.getInt("ego_familyname");
            var_s1_ego_familyname_str = rs1.getString("ego_familyname_str");
            var_s1_ego_firstname1_str = rs1.getString("ego_firstname1_str");

            // First Names ego
            switch (firstname_method)     // firstname matching method:
            {
                case 1:
                    var_s1_ego_firstname1 = rs1.getInt("ego_firstname1");
                    var_s1_ego_firstname2 = rs1.getInt("ego_firstname2");
                    var_s1_ego_firstname3 = rs1.getInt("ego_firstname3");
                    var_s1_ego_firstname4 = rs1.getInt("ego_firstname4");
                    break;
                case 2:
                    var_s1_ego_firstname1 = rs1.getInt("ego_firstname1");
                    break;
                case 3:
                    var_s1_ego_firstname1 = rs1.getInt("ego_firstname1");
                    var_s1_ego_firstname2 = rs1.getInt("ego_firstname2");
                    break;
                case 4:
                    var_s1_ego_firstname1 = rs1.getInt("ego_firstname1");
                    var_s1_ego_firstname2 = rs1.getInt("ego_firstname2");
                    var_s1_ego_firstname3 = rs1.getInt("ego_firstname3");
                    break;
                case 5:
                    var_s1_ego_firstname1 = rs1.getInt("ego_firstname1");
                    var_s1_ego_firstname2 = rs1.getInt("ego_firstname2");
                    var_s1_ego_firstname3 = rs1.getInt("ego_firstname3");
                    var_s1_ego_firstname4 = rs1.getInt("ego_firstname4");
                    break;
            }

            if (!ignore_minmax) {
                var_s1_ego_birth_min = rs1.getInt("ego_birth_min");
                var_s1_ego_birth_max = rs1.getInt("ego_birth_max");
                var_s1_ego_marriage_min = rs1.getInt("ego_marriage_min");
                var_s1_ego_marriage_max = rs1.getInt("ego_marriage_max");
                var_s1_ego_death_min = rs1.getInt("ego_death_min");
                var_s1_ego_death_max = rs1.getInt("ego_death_max");
            }

            if (use_mother) {
                // Family name
                var_s1_mother_familyname = rs1.getInt("mother_familyname");
                var_s1_mother_familyname_str = rs1.getString("mother_familyname_str");
                var_s1_mother_firstname1_str = rs1.getString("mother_firstname1_str");

                // First name
                switch (firstname_method)      // firstname matching method:
                {
                    case 1:
                        var_s1_mother_firstname1 = rs1.getInt("mother_firstname1");
                        var_s1_mother_firstname2 = rs1.getInt("mother_firstname2");
                        var_s1_mother_firstname3 = rs1.getInt("mother_firstname3");
                        var_s1_mother_firstname4 = rs1.getInt("mother_firstname4");
                        break;
                    case 2:
                        var_s1_mother_firstname1 = rs1.getInt("mother_firstname1");
                        break;
                    case 3:
                        var_s1_mother_firstname1 = rs1.getInt("mother_firstname1");
                        var_s1_mother_firstname2 = rs1.getInt("mother_firstname2");
                        break;
                    case 4:
                        var_s1_mother_firstname1 = rs1.getInt("mother_firstname1");
                        var_s1_mother_firstname2 = rs1.getInt("mother_firstname2");
                        var_s1_mother_firstname3 = rs1.getInt("mother_firstname3");
                        break;
                    case 5:
                        var_s1_mother_firstname1 = rs1.getInt("mother_firstname1");
                        var_s1_mother_firstname2 = rs1.getInt("mother_firstname2");
                        var_s1_mother_firstname3 = rs1.getInt("mother_firstname3");
                        var_s1_mother_firstname4 = rs1.getInt("mother_firstname4");
                        break;
                }

                if (!ignore_minmax) {
                    var_s1_mother_birth_min = rs1.getInt("mother_birth_min");
                    var_s1_mother_birth_max = rs1.getInt("mother_birth_max");
                    var_s1_mother_marriage_min = rs1.getInt("mother_marriage_min");
                    var_s1_mother_marriage_max = rs1.getInt("mother_marriage_max");
                    var_s1_mother_death_min = rs1.getInt("mother_death_min");
                    var_s1_mother_death_max = rs1.getInt("mother_death_max");
                }
            }

            if (use_father) {
                // Family Name
                var_s1_father_familyname = rs1.getInt("father_familyname");

                // First Names
                switch (firstname_method)      // firstname matching method:
                {
                    case 1:
                        var_s1_father_firstname1 = rs1.getInt("father_firstname1");
                        var_s1_father_firstname2 = rs1.getInt("father_firstname2");
                        var_s1_father_firstname3 = rs1.getInt("father_firstname3");
                        var_s1_father_firstname4 = rs1.getInt("father_firstname4");
                        break;
                    case 2:
                        var_s1_father_firstname1 = rs1.getInt("father_firstname1");
                        break;
                    case 3:
                        var_s1_father_firstname1 = rs1.getInt("father_firstname1");
                        var_s1_father_firstname2 = rs1.getInt("father_firstname2");
                        break;
                    case 4:
                        var_s1_father_firstname1 = rs1.getInt("father_firstname1");
                        var_s1_father_firstname2 = rs1.getInt("father_firstname2");
                        var_s1_father_firstname3 = rs1.getInt("father_firstname3");
                        break;
                    case 5:
                        var_s1_father_firstname1 = rs1.getInt("father_firstname1");
                        var_s1_father_firstname2 = rs1.getInt("father_firstname2");
                        var_s1_father_firstname3 = rs1.getInt("father_firstname3");
                        var_s1_father_firstname4 = rs1.getInt("father_firstname4");
                        break;
                }

                if (!ignore_minmax) {
                    var_s1_father_birth_min = rs1.getInt("father_birth_min");
                    var_s1_father_birth_max = rs1.getInt("father_birth_max");
                    var_s1_father_marriage_min = rs1.getInt("father_marriage_min");
                    var_s1_father_marriage_max = rs1.getInt("father_marriage_max");
                    var_s1_father_death_min = rs1.getInt("father_death_min");
                    var_s1_father_death_max = rs1.getInt("father_death_max");
                }
            }

            if (use_partner) {
                // Family Name
                var_s1_partner_familyname = rs1.getInt("partner_familyname");

                // First Names
                switch (firstname_method)      // firstname matching method:
                {
                    case 1:
                        var_s1_partner_firstname1 = rs1.getInt("partner_firstname1");
                        var_s1_partner_firstname2 = rs1.getInt("partner_firstname2");
                        var_s1_partner_firstname3 = rs1.getInt("partner_firstname3");
                        var_s1_partner_firstname4 = rs1.getInt("partner_firstname4");
                        break;
                    case 2:
                        var_s1_partner_firstname1 = rs1.getInt("partner_firstname1");
                        break;
                    case 3:
                        var_s1_partner_firstname1 = rs1.getInt("partner_firstname1");
                        var_s1_partner_firstname2 = rs1.getInt("partner_firstname2");
                        break;
                    case 4:
                        var_s1_partner_firstname1 = rs1.getInt("partner_firstname1");
                        var_s1_partner_firstname2 = rs1.getInt("partner_firstname2");
                        var_s1_partner_firstname3 = rs1.getInt("partner_firstname3");
                        break;
                    case 5:
                        var_s1_partner_firstname1 = rs1.getInt("partner_firstname1");
                        var_s1_partner_firstname2 = rs1.getInt("partner_firstname2");
                        var_s1_partner_firstname3 = rs1.getInt("partner_firstname3");
                        var_s1_partner_firstname4 = rs1.getInt("partner_firstname4");
                        break;
                }

                if (!ignore_minmax) {
                    var_s1_partner_birth_min = rs1.getInt("partner_birth_min");
                    var_s1_partner_birth_max = rs1.getInt("partner_birth_max");
                    var_s1_partner_marriage_min = rs1.getInt("partner_marriage_min");
                    var_s1_partner_marriage_max = rs1.getInt("partner_marriage_max");
                    var_s1_partner_death_min = rs1.getInt("partner_death_min");
                    var_s1_partner_death_max = rs1.getInt("partner_death_max");
                }
            }

            // convert sex to int
            if (!ignore_sex.equals("y")) {
                String s = rs1.getString("ego_sex");

                if (s.equals("f")) {
                    var_s1_sex = 1;
                } else if (s.equals("m")) {
                    var_s1_sex = 2;
                } else {
                    var_s1_sex = 0;
                }
            }

            // fill the arraylists
            s1_id_base                .add( var_s1_id_base );
            s1_id_registration        .add( var_s1_id_registration );
            s1_id_persist_registration.add( var_s1_id_persist_registration );
            s1_id_person_o            .add( var_s1_id_person_o );
            s1_registration_days      .add( var_s1_registration_days );

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
            s1_ego_death_min   .add(var_s1_ego_death_min);
            s1_ego_death_max   .add(var_s1_ego_death_max);

            s1_sex.add( var_s1_sex );

            s1_mother_familyname_str.add( var_s1_mother_familyname_str );
            s1_mother_firstname1_str.add( var_s1_mother_firstname1_str );

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
            s1_father_firstname4  .add( var_s1_father_firstname4 );
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
            s1_partner_death_min   .add( var_s1_partner_death_min );
            s1_partner_death_max   .add( var_s1_partner_death_max );
        }

        System.out.printf( String.format( "Thread id %02d; s1_record_count: %d\n", threadId, s1_record_count ) );
    } // fillArrays_rs1()


    private void fillArrays_rs2( ResultSet rs2 ) throws Exception
    {
        long threadId = Thread.currentThread().getId();

        int s2_record_count = 0;
        while( rs2.next() )
        {
            s2_record_count++;

            int var_s2_id_base           = 0;
            int var_s2_id_registration   = 0;
            int var_s2_registration_days = 0;

            String var_s2_id_persist_registration = "";
            int var_s2_id_person_o = 0;

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

            String var_s2_mother_familyname_str = "";
            String var_s2_mother_firstname1_str = "";

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
            var_s2_id_base                 = rs2.getInt( "id_base" );
            var_s2_id_registration         = rs2.getInt( "id_registration" );
            var_s2_id_persist_registration = rs2.getString("id_persist_registration");
            var_s2_id_person_o             = rs2.getInt("id_person_o");
            var_s2_registration_days       = rs2.getInt( "registration_days" );

            // Ego
            // familyname
            var_s2_ego_familyname     = rs2.getInt(    "ego_familyname" );
            var_s2_ego_familyname_str = rs2.getString( "ego_familyname_str" );
            var_s2_ego_firstname1_str = rs2.getString( "ego_firstname1_str" );

            // First name
            switch( firstname_method )      // firstname matching method:
            {
                case 1:
                    var_s2_ego_firstname1 = rs2.getInt( "ego_firstname1" );
                    var_s2_ego_firstname2 = rs2.getInt( "ego_firstname2" );
                    var_s2_ego_firstname3 = rs2.getInt( "ego_firstname3" );
                    var_s2_ego_firstname4 = rs2.getInt( "ego_firstname4" );
                    break;
                case 2:
                    var_s2_ego_firstname1 = rs2.getInt( "ego_firstname1" );
                    break;
                case 3:
                    var_s2_ego_firstname1 = rs2.getInt( "ego_firstname1" );
                    var_s2_ego_firstname2 = rs2.getInt( "ego_firstname2" );
                    break;
                case 4:
                    var_s2_ego_firstname1 = rs2.getInt( "ego_firstname1" );
                    var_s2_ego_firstname2 = rs2.getInt( "ego_firstname2" );
                    var_s2_ego_firstname3 = rs2.getInt( "ego_firstname3" );
                    break;
                case 5:
                    var_s2_ego_firstname1 = rs2.getInt( "ego_firstname1" );
                    var_s2_ego_firstname2 = rs2.getInt( "ego_firstname2" );
                    var_s2_ego_firstname3 = rs2.getInt( "ego_firstname3" );
                    var_s2_ego_firstname4 = rs2.getInt( "ego_firstname4" );
                    break;
            }

            if( !ignore_minmax )
            {
                var_s2_ego_birth_min    = rs2.getInt( "ego_birth_min" );
                var_s2_ego_birth_max    = rs2.getInt( "ego_birth_max" );
                var_s2_ego_marriage_min = rs2.getInt( "ego_marriage_min" );
                var_s2_ego_marriage_max = rs2.getInt( "ego_marriage_max" );
                var_s2_ego_death_min    = rs2.getInt( "ego_death_min" );
                var_s2_ego_death_max    = rs2.getInt( "ego_death_max" );
            }

            if( use_mother )
            {
                // Family Name
                var_s2_mother_familyname     = rs2.getInt(    "mother_familyname" );
                var_s2_mother_familyname_str = rs2.getString( "mother_familyname_str" );
                var_s2_mother_firstname1_str = rs2.getString( "mother_firstname1_str" );

                // First Names
                switch( firstname_method )      // firstname matching method:
                {
                    case 1:
                        var_s2_mother_firstname1 = rs2.getInt( "mother_firstname1" );
                        var_s2_mother_firstname2 = rs2.getInt( "mother_firstname2" ) ;
                        var_s2_mother_firstname3 = rs2.getInt( "mother_firstname3" );
                        var_s2_mother_firstname4 = rs2.getInt( "mother_firstname4" );
                        break;
                    case 2:
                        var_s2_mother_firstname1 = rs2.getInt( "mother_firstname1" );
                        break;
                    case 3:
                        var_s2_mother_firstname1 = rs2.getInt( "mother_firstname1" );
                        var_s2_mother_firstname2 = rs2.getInt( "mother_firstname2" );
                        break;
                    case 4:
                        var_s2_mother_firstname1 = rs2.getInt( "mother_firstname1" );
                        var_s2_mother_firstname2 = rs2.getInt( "mother_firstname2" );
                        var_s2_mother_firstname3 = rs2.getInt( "mother_firstname3" );
                        break;
                    case 5:
                        var_s2_mother_firstname1 = rs2.getInt( "mother_firstname1" );
                        var_s2_mother_firstname2 = rs2.getInt( "mother_firstname2" );
                        var_s2_mother_firstname3 = rs2.getInt( "mother_firstname3" );
                        var_s2_mother_firstname4 = rs2.getInt( "mother_firstname4" );
                        break;
                }

                if( !ignore_minmax )
                {
                    var_s2_mother_birth_min    = rs2.getInt( "mother_birth_min" );
                    var_s2_mother_birth_max    = rs2.getInt( "mother_birth_max" );
                    var_s2_mother_marriage_min = rs2.getInt( "mother_marriage_min" );
                    var_s2_mother_marriage_max = rs2.getInt( "mother_marriage_max" );
                    var_s2_mother_death_min    = rs2.getInt( "mother_death_min" );
                    var_s2_mother_death_max    = rs2.getInt( "mother_death_max" );
                }
            }

            if( use_father )
            {
                // Family Name
                var_s2_father_familyname = rs2.getInt( "father_familyname" );

                // First Names
                switch( firstname_method )      // firstname matching method:
                {
                    case 1:
                        var_s2_father_firstname1 = rs2.getInt( "father_firstname1" );
                        var_s2_father_firstname2 = rs2.getInt( "father_firstname2" );
                        var_s2_father_firstname3 = rs2.getInt( "father_firstname3" );
                        var_s2_father_firstname4 = rs2.getInt( "father_firstname4" );
                        break;
                    case 2:
                        var_s2_father_firstname1 = rs2.getInt( "father_firstname1" );
                        break;
                    case 3:
                        var_s2_father_firstname1 = rs2.getInt( "father_firstname1" );
                        var_s2_father_firstname2 = rs2.getInt( "father_firstname2" );
                        break;
                    case 4:
                        var_s2_father_firstname1 = rs2.getInt( "father_firstname1" );
                        var_s2_father_firstname2 = rs2.getInt( "father_firstname2" );
                        var_s2_father_firstname3 = rs2.getInt( "father_firstname3" );
                        break;
                    case 5:
                        var_s2_father_firstname1 = rs2.getInt( "father_firstname1" );
                        var_s2_father_firstname2 = rs2.getInt( "father_firstname2" );
                        var_s2_father_firstname3 = rs2.getInt( "father_firstname3" );
                        var_s2_father_firstname4 = rs2.getInt( "father_firstname4" );
                        break;
                }

                if( !ignore_minmax )
                {
                    var_s2_father_birth_min    = rs2.getInt( "father_birth_min" );
                    var_s2_father_birth_max    = rs2.getInt( "father_birth_max" );
                    var_s2_father_marriage_min = rs2.getInt( "father_marriage_min" );
                    var_s2_father_marriage_max = rs2.getInt( "father_marriage_max" );
                    var_s2_father_death_min    = rs2.getInt( "father_death_min" );
                    var_s2_father_death_max    = rs2.getInt( "father_death_max" );
                }
            }

            if( use_partner )
            {
                // Family Name
                var_s2_partner_familyname = rs2.getInt( "partner_familyname" );

                // First Names
                switch( firstname_method )      // firstname matching method:
                {
                    case 1:
                        var_s2_partner_firstname1 = rs2.getInt( "partner_firstname1" );
                        var_s2_partner_firstname2 = rs2.getInt( "partner_firstname2" );
                        var_s2_partner_firstname3 = rs2.getInt( "partner_firstname3" );
                        var_s2_partner_firstname4 = rs2.getInt( "partner_firstname4" );
                        break;
                    case 2:
                        var_s2_partner_firstname1 = rs2.getInt( "partner_firstname1" );
                        break;
                    case 3:
                        var_s2_partner_firstname1 = rs2.getInt( "partner_firstname1" );
                        var_s2_partner_firstname2 = rs2.getInt( "partner_firstname2" );
                        break;
                    case 4:
                        var_s2_partner_firstname1 = rs2.getInt( "partner_firstname1" );
                        var_s2_partner_firstname2 = rs2.getInt( "partner_firstname2" );
                        var_s2_partner_firstname3 = rs2.getInt( "partner_firstname3" );
                        break;
                    case 5:
                        var_s2_partner_firstname1 = rs2.getInt( "partner_firstname1" );
                        var_s2_partner_firstname2 = rs2.getInt( "partner_firstname2" );
                        var_s2_partner_firstname3 = rs2.getInt( "partner_firstname3" );
                        var_s2_partner_firstname4 = rs2.getInt( "partner_firstname4" );
                        break;
                }

                if( !ignore_minmax )
                {
                    var_s2_partner_birth_min    = rs2.getInt( "partner_birth_min" );
                    var_s2_partner_birth_max    = rs2.getInt( "partner_birth_max" );
                    var_s2_partner_marriage_min = rs2.getInt( "partner_marriage_min" );
                    var_s2_partner_marriage_max = rs2.getInt( "partner_marriage_max" );
                    var_s2_partner_death_min    = rs2.getInt( "partner_death_min" );
                    var_s2_partner_death_max    = rs2.getInt( "partner_death_max" );
                }
            }

            //convert sex to int
            if( ! ignore_sex.equals( "y" ) )
            {
                String s = rs2.getString( "ego_sex" );

                     if( s.equals( "f" ) ) { var_s2_sex = 1; }
                else if( s.equals( "m" ) ) { var_s2_sex = 2; }
                else                       { var_s2_sex = 0; }
            }

            //fill the arraylists
            s2_id_base                .add( var_s2_id_base );
            s2_id_registration        .add( var_s2_id_registration );
            s2_id_persist_registration.add( var_s2_id_persist_registration );
            s2_id_person_o            .add( var_s2_id_person_o );
            s2_registration_days      .add( var_s2_registration_days );

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

            s2_mother_familyname_str.add( var_s2_mother_familyname_str );
            s2_mother_firstname1_str.add( var_s2_mother_firstname1_str );

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

        System.out.printf( String.format( "Thread id %02d; s2_record_count: %d\n", threadId, s2_record_count ) );
    } // fillArrays_rs2()


    public void freeVectors_rs1() throws Exception
    {
        s1_id_base              .clear(); s1_id_base              = null;
        s1_id_registration      .clear(); s1_id_registration      = null;
        s1_registration_days    .clear(); s1_registration_days    = null;

        s1_id_persist_registration.clear(); s1_id_persist_registration = null;
        s1_id_person_o            .clear(); s1_id_person_o             = null;
        s1_ego_familyname_str     .clear(); s1_ego_familyname_str      = null;
        s1_ego_firstname1_str     .clear(); s1_ego_firstname1_str      = null;

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

        s1_mother_familyname_str.clear(); s1_mother_familyname_str= null;
        s1_mother_firstname1_str.clear(); s1_mother_firstname1_str= null;

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
    } // freeVectors_rs1()


    public void freeVectors_rs2() throws Exception
        {
        s2_id_base              .clear(); s2_id_base = null;
        s2_id_registration      .clear(); s2_id_registration      = null;
        s2_registration_days    .clear(); s2_registration_days    = null;

        s2_id_persist_registration.clear(); s2_id_persist_registration = null;
        s2_id_person_o            .clear(); s2_id_person_o             = null;
        s2_ego_familyname_str     .clear(); s2_ego_familyname_str      = null;
        s2_ego_firstname1_str     .clear(); s2_ego_firstname1_str      = null;

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

        s2_mother_familyname_str.clear(); s2_mother_familyname_str= null;
        s2_mother_firstname1_str.clear(); s2_mother_firstname1_str= null;

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
    } // freeVectors_rs2()
}
