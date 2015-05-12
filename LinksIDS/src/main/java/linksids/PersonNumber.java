package linksids;

import java.sql.ResultSet;
import java.sql.SQLException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map.Entry;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

//import java.sql.Connection;             // general, for MySQL, PostgreSQL, ...
import java.sql.Statement;              // general, for MySQL, PostgreSQL, ...

import com.mysql.jdbc.Connection;     // MySQL specific
//import com.mysql.jdbc.Statement;      // MySQL specific

import com.google.common.base.Strings;


/**
 * @author Cor Munnik
 * @author Fons Laan
 *
 * <p/>
 * FL-21-Jan-2014 Imported from CM
 * FL-03-Feb-2015 Also use partner in personNumber (from KM + CM)
 * FL-12-May-2015 Latest change
 *
 */
public class PersonNumber implements Runnable
{
    public static int MAX_LIST_SIZE = 16384;

    private static String url_links_ids = null;

    static String                                insStmt              = null;
    static int                                   max_id_person        = 100 * 1000 * 1000;
    static ArrayList< Integer >                  onlySelf             = new ArrayList< Integer >();
    static ArrayList< Integer >[]                aliases              = null;
    static BlockingQueue< ArrayList< Integer > > queue                = new LinkedBlockingQueue< ArrayList< Integer > >( 1024 );
    static Integer                               personNumbersWritten = new Integer( 0 );
    private static ArrayList<Integer>            pLStop               = new ArrayList<Integer>();
    static final int                             numberOfThreads      = 5;
    static final int                             accepted_Levenshtein = 0;

    /*
    private static String sp01                         = "%s";                
    private static String sp02                         = sp01 + sp01;             
    private static String sp03                         = sp02 + sp02;             
    private static String sp04                         = sp03 + sp03;             
    private static String sp05                         = sp04 + sp04;             
    private static String sp06                         = sp05 + sp05;             
    private static String sp07                         = sp06 + sp06;             
    private static String sp08                         = sp07 + sp07;             
    private static String sp09                         = sp08 + sp08;             
    private static String sp10                         = sp09 + sp09; // 512  
    private static String sp11                         = sp10 + sp10; // 1024 
    private static String sp12                         = sp11 + sp11; // 2048 
    private static String sp13                         = sp12 + sp12; // 4096 
    private static String sp14                         = sp13 + sp13; // 8192 
    private static String sp15                         = sp14 + sp14; // 16384 
    private static String sp16                         = sp15 + sp15; // 32768 
    private static String sp17                         = sp16 + sp16; // 65536
    */

    //private static String sp15 = Strings.repeat( "%s", 16384 );
    private static String sp15 = Strings.repeat( "%s", MAX_LIST_SIZE );


    /**
     * Constructor
     */
    public PersonNumber( String url_links_ids ) {
        this.url_links_ids = url_links_ids;
    }


    public void run()
    {
        Connection connection = Utils.connect( url_links_ids );
        String name = Thread.currentThread().getName();
        
        while( true )
        {
            ArrayList< Integer > values = null;

            try {
                values = queue.take();

            }
            catch( InterruptedException ex )
            {
                System.out.println( "Exception:\n" + ex.getMessage() );
                ex.printStackTrace();
                Utils.closeConnection( connection );
                System.exit( -1 );
            }

            if( values == pLStop )
            {
                Utils.commitConnection( connection );
                Utils.closeConnection( connection );
                return;
            }

            write( values, connection );
            //Utils.commitConnection(connection);
            
            synchronized( personNumbersWritten ) {
                //personNumbersWritten += 16384;
                personNumbersWritten += values.size();
                System.out.println( "Written " + personNumbersWritten + " person numbers" );
            }
        }
    } // run


    public static void personNumber( boolean debug, String url_links_match, String url_links_ids )
    {
        System.out.println( "PersonNumber/personNumber()" );
        System.out.println( "numberOfThreads: " + numberOfThreads );

        Connection connection = Utils.connect( url_links_ids );
        initDB( debug, connection );
        
        connection = Utils.connect( url_links_match );

        System.out.println("Reading matches");
        
        int totalCount = 0;
        int effectiveCount = 0;
        int pageSize = 1 * 1000 * 1000;

        for( int i = 0; i < 100 * 1000 * 1000; i += pageSize )
        {
            try {
                java.sql.Statement statement = connection.createStatement();
                //String select = "select X.ego_id, X.mother_id, X.father_id, Y.ego_id, Y.mother_id, Y.father_id" +
                //              " from links_match.matches, links_base.links_base as X,  links_base.links_base as Y " +
                //              " where X.id_base = id_linksbase_1 and " +
                //              "       Y.id_base = id_linksbase_2" +
                //              " limit " + i + ","  +  pageSize;
                String select = "select M.id_matches, " +
                        " X.ego_id,     Y.ego_id,    " +
                        " X.mother_id,  Y.mother_id,  M.value_familyname_mo," +
                        " X.father_id,  Y.father_id,  M.value_familyname_fa," +
                        " X.partner_id, Y.partner_id, M.value_familyname_pa" +
                        " from "
                        + " links_match.matches as M, "
                        + " links_prematch.links_base as X, "
                        + " links_prematch.links_base as Y " +
                        " where " +
                        " M.ids = 'y' and " +
                        " X.id_base = id_linksbase_1 and " +
                        " Y.id_base = id_linksbase_2 and " +
                        " M.id_matches >= " + i + "  and " +
                        " M.id_matches <  " + (i + pageSize) ;

                System.out.println(select);

                ResultSet r = statement.executeQuery(select);
                int count = 0;


                while (r.next()) {

                    //System.out.println("match = "+ r.getInt("M.id_matches")) ;

                    String fies = r.getString("M.value_firstname_ego");
                    int    fie  = r.getInt   ("M.value_firstname_ego");
                    String faes = r.getString("M.value_familyname_ego");
                    int    fae  = r.getInt   ("M.value_familyname_ego");


                    if(fies != null && faes != null && fie <= accepted_Levenshtein && fae <= accepted_Levenshtein){

                        int x = r.getInt("X.ego_id");
                        int y = r.getInt("Y.ego_id");
                        if(x != 0 && y != 0)
                            effectiveCount += add(x, y);
                    }



                    String fims = r.getString("M.value_firstname_mo");
                    int    fim  = r.getInt   ("M.value_firstname_mo");
                    String fams = r.getString("M.value_familyname_mo");
                    int    fam  = r.getInt   ("M.value_familyname_mo");



                    if(fims != null && fams != null && fim <= accepted_Levenshtein && fam <= accepted_Levenshtein){


                        int x = r.getInt("X.mother_id");
                        int y = r.getInt("Y.mother_id");

                        if(x != 0 && y != 0)
                            effectiveCount += add(x, y);
                    }

                    String fifs = r.getString("M.value_firstname_fa");
                    int    fif  = r.getInt   ("M.value_firstname_fa");
                    String fafs = r.getString("M.value_familyname_fa");
                    int    faf  = r.getInt   ("M.value_familyname_fa");


                    if(fifs != null && fafs != null && fif <= accepted_Levenshtein && faf <= accepted_Levenshtein){

                        int x = r.getInt("X.father_id");
                        int y = r.getInt("Y.father_id");

                        if(x != 0 && y != 0)
                            effectiveCount += add(x, y);

                    }

                    String fips = r.getString("M.value_firstname_pa");
                    int    fip  = r.getInt   ("M.value_firstname_pa");
                    String faps = r.getString("M.value_familyname_pa");
                    int    fap  = r.getInt   ("M.value_familyname_pa");



                    if(fips != null && faps != null && fip <= accepted_Levenshtein && fap <= accepted_Levenshtein){

                        int x = r.getInt("X.partner_id");
                        int y = r.getInt("Y.partner_id");

                        if(x != 0 && y != 0)
                            effectiveCount += add(x, y);

                    }

                    count++;
                    if(count % 1000 == 0)
                        System.out.println("Read " + count + " matches");

                }

                totalCount += count;
                System.out.println("Read total " + totalCount + " matches");


            }
            catch( SQLException ex )
            {
                System.out.println( "Exception:\n" + ex.getMessage() );
                ex.printStackTrace();
                Utils.closeConnection( connection );
                System.exit( -1 );
            }
        }
        
        System.out.println( "Read " + totalCount + " matches" );
        System.out.println( "" + effectiveCount + " person numbers changed" );
        
        // write person numbers to table
        
        //if(1==1) System.exit(0);
        
        //Utils.executeQI(connection, "drop index nr on personNumbers");
        
        ArrayList< Thread > a = new ArrayList< Thread >();
        
        for( int i = 0; i < numberOfThreads; i++ )
        {
            Thread p = new Thread( new PersonNumber( url_links_ids ) );
            p.start();
            a.add( p );
        }
        
        int count = 0;
        
        ArrayList< Integer > i = new ArrayList< Integer >();

        // for (Entry<Integer, Integer> entry : personNumbers.entrySet()) {
        
        for( int j = 0; j < aliases.length; j++ )
        {
            if( aliases[ j ] == null ) continue;

            count++;

            i.add( j );
            if( aliases[ j ] == onlySelf ) { i.add( j ); }
            else { i.add( aliases[ j ].get( 0 ) ); }

            //if( count % 16384  == 0 )
            if( count % MAX_LIST_SIZE  == 0 )
            {
                try { queue.put( i ); }
                catch (InterruptedException ex )
                {
                    System.out.println( "Exception:\n" + ex.getMessage() );
                    ex.printStackTrace();
                    Utils.closeConnection( connection );
                    System.exit( -1 );
                }
                i = new ArrayList< Integer >();
            }
        }

        if( i.size() > 0 )
        {
            try { queue.put(i); }
            catch( InterruptedException ex )
            {
                System.out.println( "Exception:\n" + ex.getMessage() );
                ex.printStackTrace();
                Utils.closeConnection( connection );
                System.exit( -1 );
            }
            //executeQ(connection, insStmt);
        }

        try {
            for( int j = 0; j < numberOfThreads; j++ )
            { queue.put( pLStop ); }
        }
        catch( InterruptedException ex )
        {
            System.out.println( "Exception:\n" + ex.getMessage() );
            ex.printStackTrace();
            Utils.closeConnection(connection);
            System.exit( -1 );
        }
        
        try {
            for( int j = 0; j < numberOfThreads; j++ )
            { a.get(j).join(); }
        }
        catch( InterruptedException ex1 )
        {
            System.out.println( "Exception:\n" + ex1.getMessage() );
            ex1.printStackTrace();
            Utils.closeConnection(connection);
            System.exit( -1 );
        }

        System.out.println( "Written " + count + " person numbers" );

        Utils.executeQ( connection, "create index nr on links_ids.personNumbers(person_number)" );
        Utils.closeConnection( connection );

        System.out.println("PersonNumber/personNumber() Finished");
    } // personNumber


    private static void write( ArrayList<Integer> iL, Connection connection )
    {
        System.out.println( "PersonNumber/write()" );
        
        String insStmt = "insert into personNumbers values ";
        
        ArrayList<String> a = new ArrayList<String>();
        for(int i = 0; i < iL.size(); i += 2){
            a.add(String.format("(%d, %d),", iL.get(i), iL.get(i + 1)));
        }

        
        String s = "";
        //if(a.size() %  16384 == 0)
        if( a.size() %  MAX_LIST_SIZE == 0 )
        {
            insStmt = String.format(sp15, a.toArray());
        }
        else
        {
            String fmt = "";
            for(int i = 0; i < a.size(); i++)
                fmt += "%s";
            insStmt = String.format(fmt, a.toArray());
        }

        insStmt = insStmt.substring(0,  insStmt.length() - 1);
        String insStmt2 = "insert into personNumbers values" + insStmt;
        //System.out.println(insStmt2);
        Utils.executeQ(connection, insStmt2);
    } // write
    
    
    private static int add( int x, int y )
    {
        if( x >= max_id_person || y >= max_id_person ) return 0;

        if( x == 0 || y == 0 ) return 0;

        if( aliases[ x ] != onlySelf && ( aliases[ x ] == aliases[ y ] ) ) return 0;

        if( aliases[ x ].size() > 100 ) return 0;

        if( aliases[ y ].size() > 100 ) return 0;

        ArrayList< Integer > h = null;
        if( aliases[ x ] == onlySelf ) {
            h = new ArrayList< Integer >();
            aliases[ x ] = h;
            h.add( x );
        }

        if( aliases[ y ] == onlySelf ) {
            aliases[ y ] = aliases[ x ];
            aliases[ x ].add( y );
        }
        else {
            for( Integer y1: aliases[ y ] ) {
                aliases[ y1 ] = aliases[ x ];
                aliases[ x ].add( y1 );
            }
        }
        return 1;
    } // add


    private static void initDB( boolean debug, Connection connection )
    {
        System.out.println( "PersonNumber/initDB()" );

        try
        {
            // java.sql.Statement statement = connection.createStatement();

            // Next two statements only first time
            
            createTable( connection );
            initializePersonNumbers( connection );
            
            int highest_ID_Person = getHighestID_Person( connection );
            aliases = new ArrayList[ highest_ID_Person + 1 ];
            ArrayList< Integer > h = null;

            int prevPersonNumber = - 1;
            //java.sql.Statement statement1 = connection.createStatement();
            
            Utils.executeQI( connection, "create index nr on personNumbers(person_number)" );

            System.out.println( "Reading person numbers" );
            
            int count = 0;
            int pageSize = 1 * 1000 * 1000;

            for( int i = 0; i < max_id_person; i += pageSize )
            {
                String select = "select id_person, person_number from personNumbers where person_number > " + i + " and person_number <= " +  (i + pageSize) + " group by id_person order by person_number";

                if( debug ) { System.out.println( select ); }

                //ResultSet r = statement.executeQuery(select);
                ResultSet r = connection.createStatement().executeQuery( select );


                while( r.next() )
                {
                    count++;
                    if(r.getInt("person_number") != prevPersonNumber){

                        //personNumberToP_IDs.put(prevPersonNumber, h);

                        if(h == null){
                            h = new ArrayList<Integer>();
                        }
                        else{
                            if(h.size() == 1){
                                aliases[prevPersonNumber] = onlySelf;
                            }
                            else{
                                for(Integer y1: h)
                                    aliases[y1] = h;

                            }
                            h = new ArrayList<Integer>();
                        }
                        prevPersonNumber = r.getInt("person_number");
                    }

                    //int id_person     = r.getInt("id_person");
                    //int person_number = r.getInt("person_number");

                    h.add(r.getInt("id_person"));
                }

                if( h != null ) {
                    if( h.size() == 1 ) {
                        aliases[ prevPersonNumber ] = onlySelf;
                    }
                    else {
                        for( Integer y1: h )
                            aliases[ y1 ] = h;
                    }
                    h = null;
                }

                r.close();
                connection.createStatement().close();
            }

            System.out.println("Read " + count + " person numbers");
            
            // Copy s to s_save and truncate s

            Utils.executeQI(connection, "drop table personNumbers_save ");
            Utils.executeQ(connection, "rename table personNumbers to personNumbers_save ");
            createTable(connection);    // recreate the table
            
            //s = "drop index nr on personNumbers ";
            //System.out.println(s);
            //connection.createStatement().execute(s);
        }
        catch( SQLException ex )
        {
            System.out.println( "Exception:\n" + ex.getMessage() );
            ex.printStackTrace();
            Utils.closeConnection( connection );
            System.exit( -1 );
        }
    } // initDB


    private static void print( HashMap<Integer, HashSet<Integer>> hm )
    {
        
        for (Entry<Integer, HashSet<Integer>> entry : hm.entrySet()) {
            Integer key = entry.getKey();
            System.out.println("" + key);
            HashSet<Integer> h = (HashSet<Integer>) entry.getValue();
            if(h == null){
                System.out.println("   null");
                continue;
            }
            
            for(Integer i: h){
                System.out.println("   " + i);
                
            }
        }
    } // print


    private static int getHighestID_Person( Connection connection )
    {
        System.out.println( "PersonNumber/getHighestID_Person()" );

        ResultSet r = null;
        try {
            r = connection.createStatement().executeQuery("select max(id_person) as m FROM links_ids.personNumbers");
            while (r.next()) {
                System.out.println("Highest id_person = " + r.getInt("m"));
                return(r.getInt("m"));
            }
            r.close();
            //connection.createStatement().close();
        }
        catch( SQLException ex )
        {
            System.out.println( "Exception:\n" + ex.getMessage() );
            ex.printStackTrace();
            Utils.closeConnection( connection );
            System.exit( -1 );
        }
        
        return -1;
    } // getHighestID_Person


    private static void createTable( Connection connection )
    {
        try
        {
            java.sql.Statement statement = connection.createStatement();

            
            Utils.executeQI(connection, "drop table personNumbers");
            Utils.executeQ(connection, "create table personNumbers (id_person int, person_number int)");
            //String indexNr = "create index nr on links_IDS.personNumbers(person_number)";

            //s = "create unique index i on links_match.personNumbers(id_person)";
            //System.out.println(s);
            //statement.execute(s);
            

            
            connection.commit();
        }
        catch( SQLException ex )
        {
            System.out.println( "Exception:\n" + ex.getMessage() );
            ex.printStackTrace();
            Utils.closeConnection( connection );
            System.exit( -1 );
        }
    } // createTable


    private static void initializePersonNumbers( Connection connection )
    {
        Utils.executeQ(connection, "insert into links_ids.personNumbers (select id_person, id_person from links_cleaned.person_c where id_person <= " + max_id_person + ")");

    } // initializePersonNumbers

}