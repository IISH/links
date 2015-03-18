package linksids;

import java.io.FileInputStream;
import java.io.InputStream;
import java.io.IOException;

import java.sql.ResultSet;
import java.sql.SQLException;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Properties;

import com.mysql.jdbc.Connection;
import com.mysql.jdbc.PreparedStatement;
import com.mysql.jdbc.ResultSetMetaData;
import com.mysql.jdbc.Statement;

import com.google.common.base.Strings;


/**
 * @author Cor Munnik
 * @author Fons Laan
 *
 * <p/>
 * FL-21-Jan-2014 Imported from CM
 * FL-18-Mar-2015 Latest change, changes by CM
 */
public class LinksIDS
{
    //public static intMAX_LIST_SIZE = 10000;   // the db INSERT became too big, but you can also adapt the 'max_allowed_packet' MySQL parameter.
    public static int MAX_LIST_SIZE = 5000;

    public static String url_links_general  = null;
    public static String url_links_prematch = null;
    public static String url_links_match    = null;
    public static String url_links_ids      = null;

    private static Connection connection = null;
    private static Statement statement = null;
    private static PreparedStatement preparedStatement = null;
    private static ResultSet resultSet = null;

    //private static LinkedBlockingQueue< String >             qe  = new LinkedBlockingQueue<String>(1024);
    private static LinkedBlockingQueue< ArrayList< Person > >  qe  = new LinkedBlockingQueue<ArrayList<Person>>( 1024 );
    private static LinkedBlockingQueue< ArrayList< String > >  qe2 = new LinkedBlockingQueue<ArrayList<String>>( 1024 );

    private static String insertStatement    = "";
    private static ArrayList<Thread> threads = new ArrayList<Thread>();
    private static int numberOfThreads       = 12;
    private static Integer personsWritten    = 0;
    public static int phase                  = 0;
    
    static class Pair {
        int x;
        int y;
    }
    
    static HashMap< Pair,    Integer > relations  = null;
    static HashMap< Integer, Integer > locations  = null;
    static HashMap< Integer, Integer > locNo2Id_C = new HashMap<Integer, Integer>();

    /*
    private static String sp01 = "%s";
    private static String sp02 = sp01 + sp01;
    private static String sp03 = sp02 + sp02;
    private static String sp04 = sp03 + sp03;
    private static String sp05 = sp04 + sp04;
    private static String sp06 = sp05 + sp05;
    private static String sp07 = sp06 + sp06;
    private static String sp08 = sp07 + sp07;
    private static String sp09 = sp08 + sp08;
    private static String sp10 = sp09 + sp09;
    private static String sp11 = sp10 + sp10;
    private static String sp12 = sp11 + sp11;   //  2048
    private static String sp13 = sp12 + sp12;   //  4096
    private static String sp14 = sp13 + sp13;   //  8192
    private static String sp15 = sp14 + sp14;   // 16384
    */

    private static String sp15 = Strings.repeat( "%s", 16384 );
    //private static String sp15 = Strings.repeat( "%s", MAX_LIST_SIZE );     //

    private static ArrayList< String >  iList = new ArrayList<String>();
    private static ArrayList< String > iiList = new ArrayList<String>();
    private static ArrayList< String >  cList = new ArrayList<String>();
    private static ArrayList< String > ccList = new ArrayList<String>();
    private static ArrayList< String > icList = new ArrayList<String>();


    public static Properties getProperties()
    {
        Properties properties = new Properties();
        InputStream input = null;

        String propertiesPath = System.getProperty( "properties.path" );
        if( propertiesPath == null ) {
            System.out.println( "No properties file." );
            return properties;
        }
        else { System.out.println( "properties path: " + propertiesPath ); }

        try
        {
            System.out.println( "gettings properties.path" );

            input = new FileInputStream(propertiesPath);

            if( input == null ) {
                System.out.println( "Cannot read: " + propertiesPath + "." );
                return properties;
            }
            System.out.println( "properties file: " + propertiesPath );

            properties.load( input );

        }
        catch( IOException ex ) { ex.printStackTrace(); }
        finally
        {
            if( input != null ) {
                try { input.close(); }
                catch( IOException ex ) { ex.printStackTrace(); }
            }
        }

        return properties;
    }


    public static void loadProperties( Properties properties )
    {
        String ref_url  = properties.getProperty( "mysql_hsnref_hosturl" );
        String ref_user = properties.getProperty( "mysql_hsnref_username" );
        String ref_pass = properties.getProperty( "mysql_hsnref_password" );
        String ref_db   = properties.getProperty( "mysql_hsnref_dbname" );

        System.out.println( "mysql_hsnref_hosturl:\t"  + ref_url );
        System.out.println( "mysql_hsnref_username:\t" + ref_user );
        System.out.println( "mysql_hsnref_password:\t" + ref_pass );
        System.out.println( "mysql_hsnref_dbname:\t"   + ref_db );

        url_links_general = "//" + ref_url + "/" + ref_db + "?user=" + ref_user + "&password=" + ref_pass;
        System.out.println( url_links_general );

        String url  = properties.getProperty( "mysql_links_hosturl" );
        String user = properties.getProperty( "mysql_links_username" );
        String pass = properties.getProperty( "mysql_links_password" );

        System.out.println( "mysql_links_hosturl:\t"  + url );
        System.out.println( "mysql_links_username:\t" + user );
        System.out.println( "mysql_links_password:\t" + pass );

        url_links_prematch = "//" + url + "/" + "links_prematch" + "?user=" + user + "&password=" + pass;
        url_links_match    = "//" + url + "/" + "links_match" + "?user=" + user + "&password=" + pass;
        url_links_ids      = "//" + url + "/" + "links_ids" + "?user=" + user + "&password=" + pass;

        System.out.println( url_links_prematch );
        System.out.println( url_links_match );
        System.out.println( url_links_ids );
    }


    public static void main( String[] args )
    {
        int c = 0;
        int pageSize = 1000 * 1000;

        // Load arguments; check length
        if( args.length != 2 ) {
            System.out.println( "Invalid argument length, it should be 2" );
            System.out.println( "Usage: java -jar LinksIDS-2.0.jar <method> <debug>" );

            return;
        }

        // cmd line args
        String method = args[ 0 ];
        String debug_str = args[ 1 ];
        boolean debug = false;
        if( debug_str.equals( "true" ) ) { debug = true; }

        System.out.printf( "method: %s\n", method );
        System.out.printf( "debug:  %s\n", debug );

        Properties properties = getProperties();    // read properties file
        loadProperties( properties );               // get individual properties

        Statement statement = null;
        try
        {
            System.out.println( "LinksIDS/main() Started" );

            connection = Utils.connect( url_links_ids );

            System.out.println( "LinksIDS/main: Creating or truncating IDS tables" );
            Utils.executeQ( connection, "SET SESSION sql_mode=''" );            // enable backslash escape
            Utils.createIDSTables( connection );

            populateContext( debug );
            
            Contxt2.initializeContext( connection );
            
            statement = ( Statement ) connection.createStatement();
        }
        catch( Exception ex )
        {
            System.out.println( "Exception:\n" + ex.getMessage() );
            ex.printStackTrace();
            Utils.closeConnection( connection );
            System.exit( 1 );
        }


        if( method.equals( "numbers" ) || method.equals( "both" ) )
        {
            // phase-1: create the person numbers
            try { PersonNumber.personNumber( debug, url_links_match, url_links_ids ); }
            catch( Exception ex )
            {
                System.out.println( "Exception:\n" + ex.getMessage() );
                ex.printStackTrace();
                Utils.closeConnection( connection );
                System.exit( 1 );
            }
        }
        else { System.out.println( "skipping PersonNumber" ); }


        if( ! ( method.equals( "tables" ) || method.equals( "both" ) ) ) {
            System.out.println( "skipping other IDS tables" );
            return;
        }


        // fill the other IDS tables
        try
        {
            int previousPersonNumber = -1;

            //ArrayList<Person> persons  = new ArrayList<Person>();
            ArrayList<String []> persons = new ArrayList<String []>();
            HashMap<String, Integer> h = null;

            //String q = "SELECT * FROM links_match.personNumbers as N, links_cleaned.person_c as P where N.id_person = P.id_person order by person_number";

            // SELECT * from links_match.personNumbers as N,  links_cleaned.person_c as P, links_cleaned.registration_c as R where N.id_person = P.id_person and P.id_registration =  R.id_registration and (person_number > 0 and   person_number <=  100000) order by person_number
            
            
            System.out.println( "LinksIDS/main: Processing Persons" );
            // TODO: use highest id_person instead of 100*1000*1000
            outer: for( int a = 0; a < 100*1000*1000; a += pageSize )
            {
                //String q = "SELECT * from links_ids.personNumbers as N,  links_cleaned.person_c as P, links_cleaned.registration_c as R  " +
                //        " where N.id_person = P.id_person and P.id_registration =  R.id_registration " +
                //        " and (person_number > " + a + " and " +    "  person_number <=  " + (a + pageSize)  + ")" + 
                //        " order by person_number";

                String query = "SELECT" +
                    " N.person_number," +
                    " P.firstname, " +
                    " P.familyname, " +
                    " P.prefix, " +
                    " P.sex, " +
                    " P.birth_day, " +
                    " P.birth_month, " +
                    " P.birth_year, " +
                    " P.birth_day_min, " +
                    " P.birth_month_min, " +
                    " P.birth_year_min, " +
                    " P.birth_day_max, " +
                    " P.birth_month_max, " +
                    " P.birth_year_max, " +
                    " P.birth_location, " +
                    " P.death_day, " +
                    " P.death_month, " +
                    " P.death_year, " +
                    " P.death_location, " +
                    " P.role, " +
                    " P.stillbirth, " +
                    " P.occupation, " +
                    " P.religion, " +
                    " P.civil_status, " +
                    " P.living_location, " +
                    " R.id_orig_registration, " +
                    " R.id_source, " +
                    " R.registration_day, " +
                    " R.registration_month, " +
                    " R.registration_year, " +
                    " R.registration_maintype, " +
                    " R.registration_type, " +
                    " R.registration_seq, " +
                    " R.registration_location_no" +
                    " FROM" +
                    " links_ids.personNumbers as N,  " +
                    " links_cleaned.person_c as P, " +
                    " links_cleaned.registration_c as R " +
                    " WHERE " +
                    " N.id_person = P.id_person and" +
                    " P.id_registration =  R.id_registration and " +
                    " R.id_source !=  10 and " +                        // 10 = HSNRP0002, the 'anchor'
                    " N.person_number >   " + a + " and " +
                    " N.person_number <=  " + (a + pageSize)  +
                    " ORDER BY N.person_number, R.registration_maintype";

                if( debug ) { System.out.println( "Scanning person_number range [" + a + ", " + (a + pageSize - 1) + "]" ); }

                statement.executeQuery( query );
                resultSet = statement.getResultSet();
                
                if(h == null){
                    h = new HashMap<String, Integer>();
                
                    ResultSetMetaData meta = (ResultSetMetaData) resultSet.getMetaData();
                
                    for(int i = 1; i <= meta.getColumnCount(); i++){
                        //System.out.println(meta.getColumnName(i));
                        if(meta.getColumnName(i) != null)
                            h.put(meta.getColumnName(i), i);
                    }
                }
                
                int Id_C = 0;
                while( resultSet.next() )
                {
                    //System.out.println("In loop");
                    c++;
                    //System.out.println("c = " + c);
                    if( resultSet.getInt( "person_number" ) != previousPersonNumber )
                    {
                        if( persons.size() != 0 )
                        {
                            if( debug && c % MAX_LIST_SIZE == 0 ) { System.out.println("---> processed " + c + " person appearances"); }

                            //if(c > 1000 * 1000){                                
                                //System.exit(8);
                            //}
                                
                            writeIndividual(h, persons);
                        }

                        persons.clear();                
                        add(resultSet, h, persons);
                        previousPersonNumber = resultSet.getInt("person_number");

                    }
                    else{
                        add(resultSet, h, persons);

                    }
                }
                //resultSet.close ();

                if(persons.size()  != 0){
                    writeIndividual(h, persons);    
                    persons.clear();
                }

                if( debug ) { System.out.println("---> processed " + c + " person appearances"); }
            }

            //if(1==1) System.exit(0);
            //s.close ();
            //Utils.closeConnection(connection);
        }
        catch( Exception ex )
        {
            System.out.println( "Exception:\n" + ex.getMessage() );
            ex.printStackTrace();
            Utils.closeConnection( connection );
            System.exit( 1 );
        }
        
        flushIndiv(connection);

        //Contxt.save(connection);
        //Utils.closeConnection(connection);        
        
        //System.out.println("Program ended");
        //if(1==1) System.exit(0);
        
        relations = new HashMap<Pair, Integer>();
        

        ArrayList<Integer> personNumbers = new ArrayList<Integer>();
        ArrayList<Integer> roles         = new ArrayList<Integer>();
        ArrayList<String>  stillborns    = new ArrayList<String>();
        
        c = 0;
        System.out.println( "LinksIDS/main: Processing Registrations" );

        for( int a = 0; a < 100*1000*1000; a += pageSize )
        {
            try
            {
                String q = "SELECT" +
                        " R.*," +
                        " N.person_number," +
                        " P.role," +
                        " P.stillbirth," +
                        " P.living_location" +
                        " FROM" +
                        " links_ids.personNumbers AS N," +
                        " links_cleaned.person_c AS P," +
                        " links_cleaned.registration_c AS R" +
                        " WHERE" +
                        " R.id_source != 10 AND" +                      // 10 = HSNRP0002, the 'anchor'
                        " R.id_registration = P.id_registration AND" +
                        " P.id_person = N.id_person AND" +
                        " (R.id_registration > " +  a + " AND" +
                        " R.id_registration <= " + (a + pageSize)  + ")" +
                        " ORDER BY R.id_registration";

                if( debug ) {
                    System.out.println( q );
                    System.out.println( "Scanning id_registration range [" + a + ", " + (a + pageSize - 1) + "]" );
                }

                Statement s = (Statement) connection.createStatement ();
                s.executeQuery(q);
                resultSet = s.getResultSet ();

                // 1 Child 
                // 2 Mother
                // 3 Father
                // 4 Bride
                // 5 Mother Bride
                // 6 Father Bride
                // 7 Groom
                // 8 Mother Groom
                // 9 Father Groom 
                //10 Deceased
                //11 Partner
                //12 Witness
                
                String [] rls = {"", 
                        "Child", 
                        "Mother",
                        "Father",
                        "Bride",
                        "Mother Bride",
                        "Father Bride",
                        "Groom",
                        "Mother Groom",                         
                        "Father Groom",  
                        "Deceased",
                        "Partner",
                        "Witness"
                };

                String [] certType = {"", "B", "M", "D"};
                String regType = null;

                int previousRegistrationNumber = -1;

                // Columns of registration_c
                int id_registration            = 0;
                int id_source                  = 0;
                int id_persist_source          = 0;
                String id_persist_registration = null;
                int id_orig_registration       = 0;
                int registration_maintype      = 0;
                String registration_type       = null;
                String extract                 = null;
                int registration_location_no   = 0;
                int registration_spec          = 0;
                String registration_church     = null;
                int registration_day           = 0;
                int registration_month         = 0;
                int registration_year          = 0;
                String registration_seq        = null;
                String remarks                 = null;
                int person_number              = 0;
                int role                       = 0;
                String stillborn               = null;

                int Id_C = 0;

                while( resultSet.next() )
                {
                    if( resultSet.getInt( "id_registration" ) != previousRegistrationNumber )
                    {
                        if(personNumbers.size() > 0)
                            writeIndivIndiv(personNumbers, roles, stillborns, registration_day, registration_month, registration_year, registration_maintype);

                        // Add context for this source

                        registration_maintype = resultSet.getInt("registration_maintype");
                        regType = "" + registration_maintype;
                        if(registration_maintype < certType.length)
                            regType = certType[registration_maintype];

                        if(resultSet.getInt("registration_location_no") != 0)
                                Id_C = Contxt2.addCertificate(connection, cList, ccList, 
                                regType,
                                locNo2Id_C.get(resultSet.getInt("registration_location_no")), 
                                resultSet.getInt("registration_year"), 
                                resultSet.getString("registration_seq"));

                        
                        previousRegistrationNumber = resultSet.getInt("id_registration");

                        // Save all registration_c columns

                        id_source                     =  resultSet.getInt("id_source");
                        id_persist_source             =  resultSet.getInt("id_persist_source");
                        id_persist_registration     =  resultSet.getString("id_persist_registration");
                        id_orig_registration         =  resultSet.getInt("id_orig_registration");
                        registration_maintype         =  resultSet.getInt("registration_maintype");
                        //registration_type             =  resultSet.getString("registration_type");
                        //extract                     =  resultSet.getString("extract");
                        //registration_location_no     =  resultSet.getInt("registration_location_no");
                        //registration_spec             =  resultSet.getInt("id_registration_spec");
                        //registration_church         =  resultSet.getString("registration_church");
                        registration_day             =  resultSet.getInt("registration_day");
                        registration_month             =  resultSet.getInt("registration_month");
                        registration_year             =  resultSet.getInt("registration_year");
                        registration_seq             =  resultSet.getString("registration_seq");
                        //remarks                     =  resultSet.getString("remarks");
                        person_number                  =  resultSet.getInt("person_number");
                        role                         =  resultSet.getInt("role");
                        stillborn                       =  resultSet.getString("stillbirth");
                        
                        personNumbers.clear();
                        roles.clear();
                        stillborns.clear();
                    }

                    if (Id_C != 0){
                        
                        
                        int roleIndex = resultSet.getInt("role");
                        String rol = "" + roleIndex;
                        if(roleIndex < rls.length)
                            rol = rls[roleIndex];
                        
                        addIndivContext(connection, resultSet.getInt("person_number" ), Id_C,  regType, rol, "Event", "Exact", registration_day, registration_month, registration_year);
                    }

                    c++;

                    if( debug && c % MAX_LIST_SIZE == 0 ) { System.out.println("---> processed " + c + " person appearances (2)"); }

                    personNumbers.add(resultSet.getInt("person_number"));
                    roles.add(resultSet.getInt("role"));
                    stillborns.add(resultSet.getString("stillbirth"));
                }

                if(personNumbers.size() > 0){
                    writeIndivIndiv(personNumbers, roles, stillborns, registration_day, registration_month, registration_year, registration_maintype);
                    personNumbers.clear();
                    roles.clear();
                }
                 
                if( debug ) { System.out.println("---> processed " + c + " person appearances"); }

            }
            catch( Exception ex )
            {
                System.out.println( "Exception:\n" + ex.getMessage() );
                ex.printStackTrace();
                Utils.closeConnection( connection );
                System.exit( 1 );
            }        
        }
        
        flushIndivContext( connection );
        flushIndivIndiv( connection );
        Contxt2.saveContext( connection, cList, ccList );

        System.out.println( "LinksIDS/main() ended." );
    } // main


    private static void writeIndividual( HashMap<String, Integer> h, ArrayList<String []> persons )
    {
        //System.out.println( "writeIndividual()" );
        
        int birth_day       = 0;
        int birth_month     = 0;
        int birth_year      = 0;
        int birth_day_min   = 0;
        int birth_month_min = 0;
        int birth_year_min  = 0;
        int birth_day_max   = 0;
        int birth_month_max = 0;
        int birth_year_max  = 0;
        
        boolean birthDay = false;

        int death_day       = 0;
        int death_month     = 0;
        int death_year      = 0;
        int death_day_min   = 0;
        int death_month_min = 0;
        int death_year_min  = 0;
        int death_day_max   = 0;
        int death_month_max = 0;
        int death_year_max  = 0;
        
        boolean deathDay = false;

        
        boolean fn = false;
        boolean ln = false;
        boolean pf = false;
        boolean sx = false;
        boolean bl = false;
        boolean dl = false;
        
        int person_number = 0;
        int registration_maintype = 0;
        int registration_day = 0;
        int registration_month = 0;
        int registration_year = 0;
        String registration_seq = null;

        String stillborn = null;

        for( int i = 0; i < persons.size(); i++ )
        {
            String columnName = "person_number";
            if(h.get(columnName) != null && persons.get(i)[h.get(columnName)] != null) person_number = new Integer (persons.get(i)[h.get(columnName)]);

            columnName = "registration_maintype";
            if(h.get(columnName) != null && persons.get(i)[h.get(columnName)] != null) registration_maintype = new Integer (persons.get(i)[h.get(columnName)]);

            columnName = "registration_day";
            if(h.get(columnName) != null && persons.get(i)[h.get(columnName)] != null) registration_day = new Integer (persons.get(i)[h.get(columnName)]);

            columnName = "registration_month";
            if(h.get(columnName) != null && persons.get(i)[h.get(columnName)] != null) registration_month = new Integer (persons.get(i)[h.get(columnName)]);

            columnName = "registration_year";
            if(h.get(columnName) != null && persons.get(i)[h.get(columnName)] != null) registration_year = new Integer (persons.get(i)[h.get(columnName)]);
            
            columnName = "registration_seq";
            if(h.get(columnName) != null && persons.get(i)[h.get(columnName)] != null) registration_seq = new String (persons.get(i)[h.get(columnName)]);
            
            if( birthDay == false )
            {
                columnName = "birth_day";
                if(h.get(columnName) != null && persons.get(i)[h.get(columnName)] != null) birth_day = new Integer (persons.get(i)[h.get(columnName)]);

                columnName = "birth_month";
                if(h.get(columnName) != null && persons.get(i)[h.get(columnName)] != null) birth_month = new Integer (persons.get(i)[h.get(columnName)]);

                columnName = "birth_year";
                if(h.get(columnName) != null && persons.get(i)[h.get(columnName)] != null) birth_year = new Integer (persons.get(i)[h.get(columnName)]);

                columnName = "birth_day_min";
                if(h.get(columnName) != null && persons.get(i)[h.get(columnName)] != null) birth_day_min = new Integer (persons.get(i)[h.get(columnName)]);

                columnName = "birth_month_min";
                if(h.get(columnName) != null && persons.get(i)[h.get(columnName)] != null) birth_month_min = new Integer (persons.get(i)[h.get(columnName)]);

                columnName = "birth_year_min";
                if(h.get(columnName) != null && persons.get(i)[h.get(columnName)] != null) birth_year_min = new Integer (persons.get(i)[h.get(columnName)]);

                columnName = "birth_day_max";
                if(h.get(columnName) != null && persons.get(i)[h.get(columnName)] != null) birth_day_max = new Integer (persons.get(i)[h.get(columnName)]);

                columnName = "birth_month_max";
                if(h.get(columnName) != null && persons.get(i)[h.get(columnName)] != null) birth_month_max = new Integer (persons.get(i)[h.get(columnName)]);

                columnName = "birth_year_max";
                if(h.get(columnName) != null && persons.get(i)[h.get(columnName)] != null) birth_year_max = new Integer (persons.get(i)[h.get(columnName)]);

                columnName = "stillbirth";
                if(h.get(columnName) != null && persons.get(i)[h.get(columnName)] != null) stillborn = new String (persons.get(i)[h.get(columnName)]);

                String birthdate = "BIRTH_DATE";
                if(stillborn != null && stillborn.trim().equals("1")) birthdate = "STILLBIRTH_DATE";

                if( birth_year != 0 || birth_year_min != 0 || birth_year_max != 0 )
                {
                    if( birth_year != 0 )
                    {
                        birth_day_min   = 0;
                        birth_month_min = 0;
                        birth_year_min  = 0;
                        birth_day_max   = 0;
                        birth_month_max = 0;
                        birth_year_max  = 0;
                    }

                    addIndiv( connection, person_number, "" + registration_maintype, birthdate, null,  0, "Declared", "Exact", birth_day, birth_month, birth_year,
                        birth_day_min, birth_month_min, birth_year_min,  birth_day_max, birth_month_max, birth_year_max );

                    birthDay = true;
                }
            }
            
            if( bl == false )
            {
                columnName = "birth_location";
                if(h.get(columnName) != null && persons.get(i)[h.get(columnName)] != null){
                    Integer bll = new Integer(persons.get(i)[h.get(columnName)]);
                    Integer Id_C = locNo2Id_C.get(bll);
                    System.out.println("birth_location = " + persons.get(i)[h.get(columnName)] + " Id_C = " + Id_C);
                    if(Id_C != null && Id_C != 0){
                        addIndiv(connection, person_number, "" + registration_maintype, "BIRTH_PLACE",   null, Id_C, "Declared", "Exact", 
                                birth_day, birth_month, birth_year,
                                birth_day_min, birth_month_min, birth_year_min,  birth_day_max, birth_month_max, birth_year_max);

                        bl = true;
                    }
                }
            }

            if( fn == false )
            {
                columnName = "firstname";
                if( h.get(columnName) != null && persons.get(i)[h.get(columnName)] != null && persons.get(i)[h.get(columnName)].trim().length() > 0 )
                {
                    addIndiv( connection, person_number, "" + registration_maintype, "FIRST_NAME", persons.get(i)[h.get(columnName)].trim(), 0, "Declared", "Exact",
                        birth_day, birth_month, birth_year, birth_day_min, birth_month_min, birth_year_min,  birth_day_max, birth_month_max, birth_year_max );

                    fn = true;
                }
            }

            if( ln == false )
            {
                columnName = "familyname";
                if( h.get(columnName) != null && persons.get(i)[h.get(columnName)] != null && persons.get(i)[h.get(columnName)].trim().length() > 0 )
                {
                    addIndiv( connection, person_number, "" + registration_maintype, "LAST_NAME", persons.get(i)[h.get(columnName)].trim(), 0, "Declared", "Exact",
                        birth_day, birth_month, birth_year, birth_day_min, birth_month_min, birth_year_min,  birth_day_max, birth_month_max, birth_year_max );

                    ln = true;
                }
            }

            if( pf == false )
            {
                columnName = "prefix";
                if( h.get(columnName) != null && persons.get(i)[h.get(columnName)] != null && persons.get(i)[h.get(columnName)].trim().length() > 0 )
                {
                    addIndiv( connection, person_number, "" + registration_maintype, "PREFIX_LAST_NAME", persons.get(i)[h.get(columnName)].trim(), 0, "Declared", "Exact",
                        birth_day, birth_month, birth_year, birth_day_min, birth_month_min, birth_year_min,  birth_day_max, birth_month_max, birth_year_max );

                    pf = true;
                }
            }
            
            if( sx == false )
            {
                columnName = "sex";
                if( h.get(columnName) != null && persons.get(i)[h.get(columnName)] != null && persons.get(i)[h.get(columnName)].trim().length() > 0 )
                {
                    addIndiv( connection, person_number, "" + registration_maintype, "SEX", persons.get(i)[h.get(columnName)].trim(), 0, "Declared", "Exact",
                        birth_day, birth_month, birth_year, birth_day_min, birth_month_min, birth_year_min,  birth_day_max, birth_month_max, birth_year_max );

                    sx = true;
                }
            }

            if( deathDay == false )
            {
                
                columnName = "death_day";
                if(h.get(columnName) != null && persons.get(i)[h.get(columnName)] != null) death_day = new Integer (persons.get(i)[h.get(columnName)]);

                columnName = "death_month";
                if(h.get(columnName) != null && persons.get(i)[h.get(columnName)] != null) death_month = new Integer (persons.get(i)[h.get(columnName)]);

                columnName = "death_year";
                if(h.get(columnName) != null && persons.get(i)[h.get(columnName)] != null) death_year = new Integer (persons.get(i)[h.get(columnName)]);

                columnName = "death_day_min";
                if(h.get(columnName) != null && persons.get(i)[h.get(columnName)] != null) death_day_min = new Integer (persons.get(i)[h.get(columnName)]);

                columnName = "death_month_min";
                if(h.get(columnName) != null && persons.get(i)[h.get(columnName)] != null) death_month_min = new Integer (persons.get(i)[h.get(columnName)]);

                columnName = "death_year_min";
                if(h.get(columnName) != null && persons.get(i)[h.get(columnName)] != null) death_year_min = new Integer (persons.get(i)[h.get(columnName)]);

                columnName = "death_day_max";
                if(h.get(columnName) != null && persons.get(i)[h.get(columnName)] != null) death_day_max = new Integer (persons.get(i)[h.get(columnName)]);

                columnName = "death_month_max";
                if(h.get(columnName) != null && persons.get(i)[h.get(columnName)] != null) death_month_max = new Integer (persons.get(i)[h.get(columnName)]);

                columnName = "death_year_max";
                if(h.get(columnName) != null && persons.get(i)[h.get(columnName)] != null) death_year_max = new Integer (persons.get(i)[h.get(columnName)]);

                if( death_year != 0 || death_year_min != 0 || death_year_max != 0 )
                {
                    if( death_year != 0 )
                    {
                        death_day_min   = 0;
                        death_month_min = 0;
                        death_year_min  = 0;
                        death_day_max   = 0;
                        death_month_max = 0;
                        death_year_max  = 0;
                    }

                    addIndiv( connection, person_number, "" + registration_maintype, "DEATH_DATE",   null, 0, "Declared", "Exact", death_day, death_month, death_year,
                        death_day_min, death_month_min, death_year_min, death_day_max, death_month_max, death_year_max );

                    deathDay = true;
                }
            }
            
            if( dl == false )
            {
                columnName = "death_location";
                if( h.get(columnName) != null && persons.get(i)[h.get(columnName)] != null )
                {
                    Integer pn = new Integer(persons.get(i)[h.get(columnName)]);
                    Integer Id_C = locNo2Id_C.get(pn);
                    if( Id_C != null && Id_C != 0 )
                    {
                        addIndiv( connection, person_number, "" + registration_maintype, "DEATH_PLACE",   null, Id_C, "Declared", "Exact",
                            death_day, death_month, death_year, death_day_min, death_month_min, death_year_min, death_day_max, death_month_max, death_year_max );

                        dl = true;
                    }
                }
            }

            columnName = "occupation";
            if(h.get(columnName) != null && persons.get(i)[h.get(columnName)] != null && persons.get(i)[h.get(columnName)].trim().length() > 0)
                addIndiv(connection, person_number, "" + registration_maintype, "OCCUPATION_STANDARD",   persons.get(i)[h.get(columnName)].trim(), 0, "Declared", "Exact", registration_day, registration_month, registration_year,0,0,0,0,0,0);

            columnName = "religion";
            if(h.get(columnName) != null && persons.get(i)[h.get(columnName)] != null && persons.get(i)[h.get(columnName)].trim().length() > 0)
                addIndiv(connection, person_number, "" + registration_maintype, "RELIGION_STANDARD",   persons.get(i)[h.get(columnName)].trim(), 0, "Declared", "Exact", registration_day, registration_month, registration_year,0,0,0,0,0,0);

            columnName = "civil_status";
            if(h.get(columnName) != null && persons.get(i)[h.get(columnName)] != null && persons.get(i)[h.get(columnName)].trim().length() > 0)
                addIndiv(connection, person_number, "" + registration_maintype, "CIVIL_STATUS",   persons.get(i)[h.get(columnName)].trim(), 0, "Declared", "Exact", registration_day, registration_month, registration_year,0,0,0,0,0,0);


            columnName = "living_location";
            if(h.get(columnName) != null && persons.get(i)[h.get(columnName)] != null && persons.get(i)[h.get(columnName)].trim().length() > 0){
                Integer livingLocation = new Integer(persons.get(i)[h.get(columnName)]); 
                Integer Id_C_l = locNo2Id_C.get(livingLocation);                        
                addIndivContext(connection, person_number, Id_C_l,  "" + registration_maintype, "LIVING_LOCATION", "Event", "Exact", registration_day, registration_month, registration_year);

            }

            // Special processing for HSN Start
            int id_source = 0;
            int id_person_o = 0;

            columnName = " P.id_source";
            if( h.get( columnName ) != null && persons.get( i )[ h.get( columnName ) ] != null ) { id_source = new Integer ( persons.get( i )[ h.get( columnName ) ] ); }

            if( id_source == 10 )
            {
                columnName = " P.id_person_o";
                if( h.get( columnName ) != null && persons.get( i )[ h.get( columnName ) ] != null ) { id_person_o = new Integer ( persons.get( i )[ h.get( columnName ) ] ); }

                if( id_person_o > 0 ) {
                    addIndiv( connection, person_number, "" + registration_maintype, "HSN_IDENTIFIER",   "" +
                        id_person_o, 0, "Declared", "Exact", registration_day, registration_month, registration_year, 0, 0, 0, 0, 0, 0 ); }
            }
            // Special processing for HSN End
        }
    } // writeIndividual
     
     
    private static void add( ResultSet r, HashMap<String, Integer> h, ArrayList<String []> persons )
    {
        //System.out.println( "add()" );

        String [] row = new String[h.size() + 1];
        for(int i = 1; i <= h.size(); i++)
            try {
                if(r.getObject(i) != null){
                    row[i] = r.getObject(i).toString(); 
                    if(row[i].length() > 0 && row[i].substring(row[i].length() -1).equalsIgnoreCase("\\"))  // Hack: remove ending \, gives sql error
                        row[i] = row[i].substring(0, row[i].length() -1);
                    row[i] = row[i].replaceAll("\"", "\\\\\"");
                    //row[i] = row[i].replaceAll("\\", "\\\\");
                    //System.out.println(row[i]);
                }
            } catch (SQLException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        persons.add(row);
    } // add


    public static void addIndiv( Connection connection, int Id_I, String source, String type, String value, int Id_C,
            String dateType, String estimation, int day, int month, int year, int min_day, int min_month, int min_year,int max_day, int max_month, int max_year )
    {
        //System.out.println( "addIndiv()" );

        String t = String.format("(\"%d\",\"%s\",\"%s\",\"%s\",\"%s\", \"%d\", \"%s\",\"%s\",\"%d\",\"%d\",\"%d\",\"%d\",\"%d\",\"%d\",\"%d\",\"%d\",\"%d\"),",  
                                    Id_I, "LINKS", source, type, value, Id_C, dateType, estimation, day, month, year, min_day, min_month, min_year, max_day, max_month,  max_year);

        iList.add(t);
        
         if( iList.size() >= MAX_LIST_SIZE )
             flushIndiv( connection );
    } // addIndiv


    public static void addIndivIndiv(Connection connection, int Id_I_1, int Id_I_2, int source, String relation, 
            String dateType, String estimation, int day, int month, int year)
    {
        //System.out.println( "addIndivIndiv()" );

        String t = String.format("(\"%d\",\"%d\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%d\",\"%d\",\"%d\"),",  
                                    Id_I_1,  Id_I_2, "LINKS", "" + source, relation, dateType, estimation, day, month, year);

        iiList.add(t);
         if( iiList.size() > MAX_LIST_SIZE )
             flushIndivIndiv( connection );
    } // addIndivIndiv


    public static void addIndivContext( Connection connection, int Id_I, int Id_C, String source, String relation,
            String dateType, String estimation, int day, int month, int year )
    {
        //System.out.println( "addIndivContext()" );

        String t = String.format("(\"%d\",\"%s\",\"%d\",\"%s\",\"%s\",\"%s\",\"%s\",\"%d\",\"%d\",\"%d\"),",  
                                    Id_I, "LINKS", Id_C, source, relation, dateType, estimation, day, month, year);

        icList.add(t);
        
         if( icList.size() > MAX_LIST_SIZE )
             flushIndivContext( connection );
    } // addIndivContext


    public static void addContext( Connection connection, int Id_C, String type, String value )
    {
        //System.out.println( "addContext()" );

        String t = String.format("(\"%d\",\"%s\",\"%s\", \"%s\"),",  
                                    Id_C, "LINKS", type, value);
        
        //System.out.println(t);
        cList.add(t);
        
         if( cList.size() > MAX_LIST_SIZE )
             flushContext( connection );
    } // addContext


    public static void addContextContext( Connection connection, int Id_C_1, int Id_C_2, String relation )
    {
        //System.out.println( "addContextContext()" );

        String t = String.format("(\"%d\",\"%d\",\"%s\",\"%s\"),",  
                                    Id_C_1, Id_C_2, "LINKS", relation);
        
        //System.out.println(t);
        ccList.add(t);
        
         if( ccList.size() > MAX_LIST_SIZE )
             flushContextContext( connection );
    } // addContextContext

    
    public static void flushIndiv( Connection connection )
    {
        System.out.println( "flushIndiv() iList.size: " + iList.size() );

        if( iList.size() == 0 )
            return;
        
        String s = String.format( sp15.substring(0, 2 * iList.size()), iList.toArray() );

        iList.clear();
        s = s.substring( 0, s.length() -1 );

        String u = "insert into links_ids.individual (Id_I, Id_D, Source, Type, Value, Id_C, date_type, estimation, day, month, year, Start_day, Start_month, Start_year, End_day, End_month, End_year) values" + s;
        //System.out.println(u.substring(0, 120));

        //String u = "insert into links_ids.context (Id_C, Id_D, Source, Type, Value, date_type, estimation, day, month, year) values" + s;
            
           Utils.executeQ( connection, u );
    } // flushIndiv


    public static void flushIndivIndiv( Connection connection )
    {
        System.out.println( "flushIndivIndiv() iiList.size: " + iiList.size() );

        if( iiList.size() == 0 )
            return;
        
        String s = String.format( sp15.substring(0, 2 * iiList.size()), iiList.toArray() );
        iiList.clear();

        s = s.substring( 0, s.length() -1 );
        String u = "insert into links_ids.indiv_indiv (Id_I_1, Id_I_2, Id_D, Source, relation, date_type, estimation, day, month, year) values" + s;
            
           Utils.executeQ( connection, u );
    } // flushIndivIndiv
    
    
    public static void flushIndivContext( Connection connection )
    {
        System.out.println( "flushIndivContext()" );

        if(icList.size() == 0)
            return;
        
        String s = String.format(sp15.substring(0, 2 * icList.size()), icList.toArray());
        icList.clear();
        s = s.substring(0, s.length() -1);
        String u = "insert into links_ids.indiv_context (Id_I, Id_D, Id_C, Source, relation, date_type, estimation, day, month, year) values" + s;
            
        //System.out.println(u.substring(0, 120));
           Utils.executeQ(connection, u);
    } // flushIndivContext


    public static void flushContext( Connection connection )
    {
        System.out.println( "flushContext()" );

        if(cList.size() == 0)
            return;
        
        String s = String.format(sp15.substring(0, 2 * cList.size()), cList.toArray());
        //System.out.println(s);
        cList.clear();
        s = s.substring(0, s.length() -1);
        String u = "insert into links_ids.context (Id_C, Id_D, type, value) values" + s;
            
        //System.out.println(u.substring(0, 120));
           Utils.executeQ(connection, u);
    } // flushContext


    public static void flushContextContext( Connection connection )
    {
        System.out.println( "flushContextContext()" );

        if(ccList.size() == 0)
            return;
        
        String s = String.format(sp15.substring(0, 2 * ccList.size()), ccList.toArray());
        //System.out.println(s);
        ccList.clear();
        s = s.substring(0, s.length() -1);
        String u = "insert into links_ids.context_context (Id_C_1, Id_C_2, Id_D, relation) values" + s;
            
        //System.out.println(u.substring(0, 120));
           Utils.executeQ(connection, u);
    } // flushContextContext

        
    static void writeIndivIndiv( ArrayList<Integer> personNumbers, ArrayList<Integer> roles, ArrayList<String> stillborns,
            int registration_day, int registration_month, int registration_year, int source )
    {
        //System.out.println( "writeIndivIndiv()" );

        // 1 Child 
        // 2 Mother
        // 3 Father
        // 4 Bride
        // 5 Mother Bride
        // 6 Father Bride
        // 7 Groom
        // 8 Mother Groom
        // 9 Father Groom
        //10 Deceased
        //11 Partner
        //12 Witness
        
        // Add INDIV_INDIV
        
        //System.out.println("Number of kept relations: " + relations.size());
        
        int [] x = new int[13];
        
        boolean stillborn = false;
        
        int pn = 0;
        for(int i = 0; i < roles.size(); i++){
            
            switch(roles.get(i)){
            
            case 1:  
                x[1] = personNumbers.get(i);
                if(stillborns.get(i) != null && stillborns.get(i).trim().equals("1")) stillborn = true;
                break;
                
            case 2:  
                x[2] = personNumbers.get(i);
                break;
                
            case 3:  
                x[3] = personNumbers.get(i);
                break;
                
            case 4:  
                x[4] = personNumbers.get(i);
                break;
                
            case 5:  
                x[5] = personNumbers.get(i);
                break;
                
            case 6:  
                x[6] = personNumbers.get(i);
                break;
                
            case 7:  
                x[7] = personNumbers.get(i);
                break;
                
            case 8:  
                x[8] = personNumbers.get(i);
                break;
                
            case 9:  
                x[9] = personNumbers.get(i);
                break;
                
            case 10: 
                x[10] = personNumbers.get(i);
                break;
                
            case 11:  
                x[11] = personNumbers.get(i);
                break;
                
            case 12:  
                x[12] = personNumbers.get(i);
                break;
            
            }
        }

        
        if(x[1] != 0){
            //addIi(x[1], x[2], "Child and Mother", source);
            //addIi(x[1], x[3], "Child and Father", source);
            
            String rel = "Child";
            if(stillborn) rel = "Stillbirth-child";
            
            addIi(x[1], x[2], rel, source);
            addIi(x[1], x[3], rel, source);
        }

        if(x[2] != 0){
            //addIi(x[2], x[1], "Mother and Child", source);
            //addIi(x[2], x[10], "Mother and Child", source);
            addIi(x[2], x[1], "Mother", source);
            addIi(x[2], x[10], "Mother", source);
            
        }

        if(x[3] != 0){
            //addIi(x[3], x[1], "Father and Child", source);
            //addIi(x[3], x[10], "Father and Child", source);
            addIi(x[3], x[1], "Father", source);
            addIi(x[3], x[10], "Father", source);
            
        }

        if(x[4] != 0){
            //addIi(x[4], x[5], "Child and Mother", source);
            //addIi(x[4], x[6], "Child and Father", source);
            //addIi(x[4], x[7], "Bride and Groom", source);
            //addIi(x[4], x[8], "Daughter-in-law and Mother-in-law", source);
            //addIi(x[4], x[9], "Daughter-in-law and Father-in-law", source);
            addIi(x[4], x[5], "Child", source);
            addIi(x[4], x[6], "Child", source);
            addIi(x[4], x[7], "Bride", source);
            addIi(x[4], x[8], "Daughter-in-law", source);
            addIi(x[4], x[9], "Daughter-in-law", source);
        }

        if(x[5] != 0){
            //addIi(x[5], x[4], "Mother and Child", source);
            //addIi(x[5], x[7], "Mother-in-law and Son-in-law", source);
            addIi(x[5], x[4], "Mother", source);
            addIi(x[5], x[7], "Mother-in-law", source);
        }

        if(x[6] != 0){
            //addIi(x[6], x[4], "Father and Child", source);
            //addIi(x[6], x[7], "Father-in-law and Son-in-law", source);
            addIi(x[6], x[4], "Father", source);
            addIi(x[6], x[7], "Father-in-law", source);
        }

        if(x[7] != 0){
            //addIi(x[7], x[8], "Child and Mother", source);
            //addIi(x[7], x[9], "Child and Father", source);
            //addIi(x[7], x[4], "Groom and Bride", source);
            //addIi(x[7], x[5], "Son-in-law and Mother-in-law", source);
            //addIi(x[7], x[6], "Son-in-law and Father-in-law", source);
            addIi(x[7], x[8], "Child", source);
            addIi(x[7], x[9], "Child", source);
            addIi(x[7], x[4], "Groom", source);
            addIi(x[7], x[5], "Son-in-law", source);
            addIi(x[7], x[6], "Son-in-law", source);
        }

        if(x[8] != 0){
            //addIi(x[8], x[7], "Mother and Child", source);
            //addIi(x[8], x[4], "Mother-in-law and Daughter-in-law", source);
            addIi(x[8], x[7], "Mother", source);
            addIi(x[8], x[4], "Mother-in-law", source);
        }

        if(x[9] != 0){
            //addIi(x[9], x[7], "Father and Child", source);
            //addIi(x[9], x[4], "Father-in-law and Daughter-in-law", source);
            addIi(x[9], x[7], "Father", source);
            addIi(x[9], x[4], "Father-in-law", source);
        }

        if(x[10] != 0){
            //addIi(x[10], x[2], "Child and Mother", source);
            //addIi(x[10], x[3], "Child and Father", source);
            //addIi(x[10], x[11], "Spouse and spouse", source);
            addIi(x[10], x[2], "Child", source);
            addIi(x[10], x[3], "Child", source);
            addIi(x[10], x[11], "Spouse", source);
        }

        if(x[11] != 0){
            //addIi(x[11], x[10], "Spouse and spouse", source);
            addIi(x[11], x[10], "Spouse", source);
        }
    } // writeIndivIndiv


    static void addIi(int personNumber1, int personNumber2, String relation, int source)
    {
        if(personNumber1 == 0 || personNumber2 == 0) return;
        
        Pair p = new Pair();
        p.x = personNumber1;
        p.y = personNumber2;
        
        if(!relations.containsKey(p)){            
            relations.put(p, -1);
            addIndivIndiv(connection, personNumber1, personNumber2, source, relation, "Event", "Exact", 0, 0, 0);
        }
        
    } // addIi


    static void populateContext( boolean debug )
    {
        System.out.println( "LinksIDS/populateContext()" );

        Connection connection_ref = null;
        try
        {
            Statement s = (Statement) connection.createStatement ();
            s.executeQuery( "select count(*) as c from links_ids.context" );
            resultSet = s.getResultSet();

            //System.exit(8);
            //if(1==1) return;
            int c = 0;
            while( resultSet.next() == true ) {
                c = resultSet.getInt( "c" );
                break;
            }
            
            //if(c > 0) return;  // There are already entries in context, so it is not the first time, so we don't populate
            
            //System.out.println("Here");
            Utils.executeQ( connection, "truncate links_ids.context" );             // clear context
            Utils.executeQ( connection, "truncate links_ids.context_context" );     // clear context_context
            
            connection_ref = Utils.connect( url_links_general );  // this is on the reference server
            Statement t = (Statement) connection_ref.createStatement ();
            
            t.executeQuery( "select * from links_general.ref_location group by location_no order by " +
                    "country, region, province,  municipality, location" );
            resultSet = t.getResultSet ();        
            
            /*
            while (resultSet.next()){
                if(1==1) continue;
                
                System.out.println(resultSet.getString("original") + "   " + 
                                    resultSet.getInt("location_no") + "   " +
                                    resultSet.getString("country") + "   " +
                                   resultSet.getString("region")  + "   " +
                                   resultSet.getString("province")  + "   " +
                                   resultSet.getString("municipality")  + "   " +
                                   resultSet.getString("location"));
                
                System.out.println(String.format("%6d           %20s       %20s      %20s      %20s     %20s", 
                        resultSet.getInt("location_no"),
                        resultSet.getString("country"),
                        resultSet.getString("region"),
                        resultSet.getString("province"),
                        resultSet.getString("municipality"), 
                        resultSet.getString("location")
                        ));
            }
            */

            String country      = "";
            String region       = "";
            String province     = "";
            String municipality = "";
            String locality     = "";
            
            int Id_C_CurrentCountry      = -1;
            int Id_C_CurrentRegion       = -1;
            int Id_C_CurrentProvince     = -1;
            int Id_C_CurrentMunicipality = -1;
            int Id_C_CurrentLocality     = -1;
            
            int Id_C = 0;
            String x = null;
            
            while( resultSet.next() )
            {
                if( resultSet.getString("country") != null && !resultSet.getString("country").equals(country) &&
                        !resultSet.getString("country").equals("Onbekend") )
                {
                    country = resultSet.getString("country");
                    addContext(connection, ++Id_C, "NAME", country);
                    addContext(connection,   Id_C, "LEVEL", "Country");
                    Id_C_CurrentCountry = Id_C;
                    Id_C_CurrentRegion       = -1;
                    Id_C_CurrentProvince     = -1;
                    Id_C_CurrentMunicipality = -1;
                    Id_C_CurrentLocality     = -1;                    
                }
                else
                    if(resultSet.getString("country") == null || resultSet.getString("country") == "Onbekend") continue;
                
                    
                if(resultSet.getString("region") != null && !resultSet.getString("region").equals(region)
                        && !resultSet.getString("region").equals("Onbekend")){
                    region = resultSet.getString("region");
                    addContext(connection, ++Id_C, "NAME", region);
                    addContext(connection,   Id_C, "LEVEL", "Region");
                    Id_C_CurrentRegion = Id_C;
                    Id_C_CurrentProvince     = -1;
                    Id_C_CurrentMunicipality = -1;
                    Id_C_CurrentLocality     = -1;                    
                    addContextContext(connection,   Id_C, Id_C_CurrentCountry, "Region and Country");
                }
                else
                    if(resultSet.getString("region") == null || resultSet.getString("region") == "Onbekend"){
                        region = "";
                        Id_C_CurrentRegion = -1;
                    }

                if(resultSet.getString("province") != null && !resultSet.getString("province").equals(province) && 
                        !resultSet.getString("province").equals("Onbekend")){
                    province = resultSet.getString("province");
                    addContext(connection, ++Id_C, "NAME", province);
                    addContext(connection,   Id_C, "LEVEL", "Province");
                    Id_C_CurrentProvince = Id_C;
                    Id_C_CurrentMunicipality = -1;
                    Id_C_CurrentLocality     = -1;                    
                    
                    int Id_C_Temp = Id_C_CurrentRegion;
                    x = "Region";
                    if(Id_C_Temp == -1){
                        Id_C_Temp = Id_C_CurrentCountry;
                        x = "Country";
                    }
                    addContextContext(connection,   Id_C, Id_C_Temp, "Province and " + x);
                }
                else
                    if(resultSet.getString("province") == null || resultSet.getString("province") == "Onbekend"){
                        province = "";
                        Id_C_CurrentProvince = -1;
                    }

                
                if(resultSet.getString("municipality") != null && !resultSet.getString("municipality").equals(municipality)
                        && !resultSet.getString("municipality").equals("Onbekend")){
                    municipality = resultSet.getString("municipality");
                    addContext(connection, ++Id_C, "NAME", municipality);
                    addContext(connection,   Id_C, "LEVEL", "Municipality");
                    Id_C_CurrentMunicipality = Id_C;
                    Id_C_CurrentLocality     = -1;                    
                    
                    int Id_C_Temp = Id_C_CurrentProvince;
                    x = "Province";
                    if(Id_C_Temp == -1){
                        Id_C_Temp = Id_C_CurrentRegion;
                        x = "Region";
                    }
                    if(Id_C_Temp == -1){
                        Id_C_Temp = Id_C_CurrentCountry;
                        x = "Country";
                    }
                    addContextContext(connection,   Id_C, Id_C_Temp, "Municipality and " + x);
                }
                else
                    if(resultSet.getString("municipality") == null || resultSet.getString("municipality") == "Onbekend" ){
                        municipality = "";
                        Id_C_CurrentMunicipality = -1;
                    }

                if(resultSet.getString("location") != null && !resultSet.getString("location").equals(locality)
                        && !resultSet.getString("location").equals("Onbekend")){
                    locality = resultSet.getString("location");
                    addContext(connection, ++Id_C, "NAME", locality);
                    addContext(connection,   Id_C, "LEVEL", "Locality");
                    Id_C_CurrentLocality = Id_C;
                    
                    int Id_C_Temp = Id_C_CurrentMunicipality;
                    x = "Municipality";
                    if(Id_C_Temp == -1){
                        Id_C_Temp = Id_C_CurrentProvince;
                        x = "Province";
                    }
                    if(Id_C_Temp == -1){
                        Id_C_Temp = Id_C_CurrentRegion;
                        x = "Region";
                    }
                    if(Id_C_Temp == -1){
                        Id_C_Temp = Id_C_CurrentCountry;
                        x = "Country";
                    }
                    addContextContext(connection,   Id_C, Id_C_Temp, "Locality and " + x);
                }
                else
                    if(resultSet.getString("location") == null || resultSet.getString("location") == "Onbekend"){
                        locality = "";
                        Id_C_CurrentLocality = -1;
                    }
                
                locNo2Id_C.put( (resultSet.getInt("location_no") ), Id_C ); // To find it back later

                if( debug ) { System.out.println( "Location " + resultSet.getInt("location_no") + " has Id_C " + Id_C ); }
            }
        }
        catch( Exception ex )
        {
            System.out.println( "Exception:\n" + ex.getMessage() );
            ex.printStackTrace();
            Utils.closeConnection( connection );
            System.exit( 1 );
        }    
        
        flushContext( connection );
        flushContextContext( connection );
        Utils.closeConnection( connection_ref );
    } // populateContext

}


   