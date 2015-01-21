package linksids;

import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.w3c.dom.stylesheets.LinkStyle;

import com.mysql.jdbc.Connection;
import com.mysql.jdbc.PreparedStatement;
import com.mysql.jdbc.Statement;
// SELECT * FROM links_match.personNumbers as X , links_cleaned.person_c as Y where X.id_person = Y.id_person order by person_number
public class PersonNumber implements Runnable {

    //static HashMap<Integer, HashSet<Integer>>   personNumberToP_IDs     = new HashMap<Integer, HashSet<Integer>>();  
    //static HashMap<Integer, Integer>            personNumbers = new HashMap<Integer, Integer>();
    static String                               insStmt   = null;
    static int []                               personNumber = null;
    static HashSet<Integer> []                  id_person = null;
    static HashSet<Integer>                     onlySelf = new HashSet<Integer>();
    static int                                  max_id_person = 100 * 1000 * 1000;
    
    
    static BlockingQueue<ArrayList<Integer>>    queue = new LinkedBlockingQueue<ArrayList<Integer>>(1024);
    static Integer                              personNumbersWritten = new Integer(0);
    private static ArrayList<Integer>            pLStop         = new ArrayList<Integer>(); 

    static final int                            numberOfThreads =  5;
    
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
    
    public void run()
    {
        Connection connection = Utils.connect(Constants.links_ids);
        String name = Thread.currentThread().getName();
        
        while(true){
            
            ArrayList<Integer> values = null;
            try {
                 values = queue.take();
            } catch (InterruptedException e) {
                e.printStackTrace();
                Utils.closeConnection(connection);
                System.exit(-1);
            }
            if(values == pLStop){
                Utils.commitConnection(connection);
                Utils.closeConnection(connection);
                return;
            }

            write(values, connection);
            //Utils.commitConnection(connection);
            
            synchronized (personNumbersWritten) {
                personNumbersWritten += 16384;
                System.out.println("Written " + personNumbersWritten + " person numbers");
            }
        }
    }    

    public static void personNumber(){
        
        System.out.println("Start");
        
        Connection connection = Utils.connect(Constants.links_ids);
        initDB(connection);
        
        connection = Utils.connect(Constants.links_match);

        System.out.println("Reading matches");
        
        int totalCount = 0;
        int effectiveCount = 0;
        int pageSize = 1 * 1000 * 1000;
        for(int i = 0; i < 100 * 1000 * 1000; i += pageSize){
            try {
                java.sql.Statement statement = connection.createStatement();
                //String select = "select X.ego_id, X.mother_id, X.father_id, Y.ego_id, Y.mother_id, Y.father_id" +
                //        " from links_match.matches, links_base.links_base as X,  links_base.links_base as Y " +
                //        " where X.id_base = id_linksbase_1 and " +
                //        "       Y.id_base = id_linksbase_2" + 
                //        " limit " + i + ","  +  pageSize;
                String select = "select X.ego_id, X.mother_id, X.father_id, Y.ego_id, Y.mother_id, Y.father_id" +
                        " from links_match.matches as M, links_prematch.links_base as X,  links_prematch.links_base as Y " +
                        " where X.id_base = id_linksbase_1 and " +
                        "       Y.id_base = id_linksbase_2 and " + 
                        "       M.id_matches >= " + i + " and " + 
                        "       M.id_matches < " + (i + pageSize) ;
                
                //System.out.println(select);

                ResultSet r = statement.executeQuery(select);
                int count = 0;
                
                
                while (r.next()) {

                    int x = r.getInt("X.ego_id");
                    int y = r.getInt("Y.ego_id");
                    if(x != 0 && y != 0) 
                        effectiveCount += add(x, y);

                    x = r.getInt("X.mother_id");
                    y = r.getInt("Y.mother_id"); 

                    if(x != 0 && y != 0) 
                        effectiveCount += add(x, y);

                    x = r.getInt("X.father_id");
                    y = r.getInt("Y.father_id");

                    if(x != 0 && y != 0) 
                        effectiveCount += add(x, y);

                    count++;          
                    //System.out.println("Read " + count + " matches");
                }

                totalCount += count;
                System.out.println("Read " + totalCount + " matches");

            } catch (SQLException e) {

                System.out.println(e.getMessage());
                Utils.closeConnection(connection);
                System.exit(-1);
            }
        }
        
        System.out.println("Read " + totalCount + " matches");
        System.out.println("" + effectiveCount + " person numbers changed");
        
        // write person numbers to table
        
        //if(1==1) System.exit(0);
        
        //Utils.executeQI(connection, "drop index nr on personNumbers");
        
        ArrayList<Thread> a = new ArrayList<Thread>();
        
        for(int i = 0; i < numberOfThreads; i++){
            Thread p = new Thread(new PersonNumber());
            p.start();
            a.add(p);
        }
        
        int count = 0;
        
        ArrayList<Integer> i = new ArrayList<Integer>();

//        for (Entry<Integer, Integer> entry : personNumbers.entrySet()) {
        
        for (int j = 0; j < personNumber.length; j++) {
            
            
            //System.out.println("Adding");
            
            if(personNumber[j] == 0) continue;
            count++;
            
            i.add(j);
            i.add(personNumber[j]);
            if(count % 16384  == 0){
                try {
                    queue.put(i);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                    Utils.closeConnection(connection);
                    System.exit(-1);
                }
                i = new ArrayList<Integer>();
            }
        }
        if(i.size() > 0){
            try {
                queue.put(i);
            } catch (InterruptedException e) {
                Utils.closeConnection(connection);
                e.printStackTrace();
                System.exit(-1);
            }
            //executeQ(connection, insStmt);
        }

        try {
            for(int j = 0; j < numberOfThreads; j++)
                queue.put(pLStop);
            
        } catch (InterruptedException e) {
            Utils.closeConnection(connection);
            e.printStackTrace();
            System.exit(-1);
        }
        
        try {
            for(int j = 0; j < numberOfThreads; j++)
                a.get(j).join();
        } catch (InterruptedException e1) {
            Utils.closeConnection(connection);
            e1.printStackTrace();
            System.exit(-1);
        }

        System.out.println("Written " + count + " person numbers");

        Utils.executeQ(connection, "create index nr on links_ids.personNumbers(person_number)");
        


        Utils.closeConnection(connection);
        System.out.println("\nFinished");
    }
    
    private static void write(ArrayList<Integer> iL, Connection connection){
        
        //System.out.println("Write output table");    
        
        String insStmt = "insert into personNumbers values ";
        
        ArrayList<String> a = new ArrayList<String>();
        for(int i = 0; i < iL.size(); i += 2){
            a.add(String.format("(%d, %d),", iL.get(i), iL.get(i + 1)));
        }

        
        String s = "";
        if(a.size() %  16384 == 0)
            insStmt = String.format(sp15, a.toArray());
        else{
            
            String fmt = "";
            for(int i = 0; i < a.size(); i++)
                fmt += "%s";
            insStmt = String.format(fmt, a.toArray());
        }

        insStmt = insStmt.substring(0,  insStmt.length() - 1);
        String insStmt2 = "insert into personNumbers values" + insStmt;
        //System.out.println(insStmt2);
        Utils.executeQ(connection, insStmt2);
        
        
        
    }
    
    
    private static int add(int x, int y){
        
        if(x >= max_id_person  || y >= max_id_person) return(0);
        if(x == 0      || y == 0 ) return(0);

        
        if(personNumber[x] == personNumber[y]) return(0);

        
        if(personNumber[x] == 0 || personNumber[y] == 0){
            
            System.out.println("Unknown id_person in links_base ");
            return(0);
        }
        

       if(id_person[personNumber[x]] == null){
           System.out.println("id_person[personNumber[x]] = 0 voor x");
           System.exit(0);
       }
       if(id_person[personNumber[y]] == null){
           System.out.println("id_person[personNumber[y]] = 0 voor y");
           System.exit(0);
       }
        
       HashSet<Integer> h = null;
       
        if(id_person[personNumber[x]] == onlySelf){
            h = new HashSet<Integer>();
            id_person[personNumber[x]] = h;
        }

       int pny = personNumber[y];
       
        for(Integer i: id_person[pny]){
            personNumber[i] = personNumber[x];
            id_person[personNumber[x]].add(i);
        }
        personNumber[pny] =  personNumber[x]; // This is the implicit "Self". It must now be added explicitly
        id_person[personNumber[x]].add(pny);

        
        id_person[pny] = null;

        return(1); 
    }
    
    private static void initDB(Connection connection){
        
        try {

            // java.sql.Statement statement = connection.createStatement();

            // Next two statements only first time
            
            createTable(connection);
            initializePersonNumbers(connection);
            
            int highest_ID_Person = getHighestID_Person(connection);
            id_person = new HashSet[highest_ID_Person + 1]; 
            personNumber = new int[highest_ID_Person + 1]; 
            
            
            HashSet<Integer> h = null;
            int prevPersonNumber = - 1;
            //java.sql.Statement statement1 = connection.createStatement();
            
            Utils.executeQI(connection, "create index nr on personNumbers(person_number)");

            System.out.println("Reading person numbers");
            
            int count = 0;
            int pageSize = 1 * 1000 * 1000;
            for(int i = 0; i < max_id_person; i += pageSize){
                String select = "select id_person, person_number from personNumbers where person_number > " + i + " and person_number <= " +  (i + pageSize) + " group by id_person order by person_number"; 
                System.out.println(select);
                //ResultSet r = statement.executeQuery(select);
                ResultSet r = connection.createStatement().executeQuery(select);
                
                while (r.next()) {
                    
                    count++;
                    if(r.getInt("person_number") != prevPersonNumber){
                        if(h != null){
                            //personNumberToP_IDs.put(prevPersonNumber, h);
                            if(h.size() == 0){
                                id_person[prevPersonNumber] = onlySelf;
                            }
                            else{
                                id_person[prevPersonNumber] = h;
                                h = new HashSet<Integer>();
                            }
                        }
                        else
                            h = new HashSet<Integer>();
                        prevPersonNumber = r.getInt("person_number");
                    }
                    
                    int id_person     = r.getInt("id_person");
                    int person_number = r.getInt("person_number");
                    
                    if(person_number != id_person)
                        h.add(r.getInt("id_person"));
                    personNumber[id_person] = person_number;
                    
                }
                if(h != null){
                    if(h.size() == 0){
                        id_person[prevPersonNumber] = onlySelf;
                    }
                    else{
                        id_person[prevPersonNumber] = h;
                    }
                    h = null;
                        
                }
                r.close();
                connection.createStatement().close();
            }

            System.out.println("Read   " + count + " person numbers");
            
            // Copy s to s_save and truncate s

            Utils.executeQI(connection, "drop table personNumbers_save ");
            Utils.executeQ(connection, "rename table personNumbers to personNumbers_save ");
            createTable(connection);    // recreate the table
            
            //s = "drop index nr on personNumbers ";
            //System.out.println(s);
            //connection.createStatement().execute(s);
        }
        catch (SQLException e) {
            e.printStackTrace();
            Utils.closeConnection(connection);
            System.exit(-1);
        }
    }

    private static void print(HashMap<Integer, HashSet<Integer>> hm){
        
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
    }
    
    private static int getHighestID_Person(Connection connection){
        
        System.out.println("Identifying highest id_person");
        ResultSet r = null;
        try {
            r = connection.createStatement().executeQuery("select max(id_person) as m FROM links_ids.personNumbers");
            while (r.next()) {
                System.out.println("Highest id_person = " + r.getInt("m"));
                return(r.getInt("m"));
            }
            r.close();
            //connection.createStatement().close();
        } catch (SQLException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            System.exit(0);
        }
        
        return -1;
        
    }
    
    private static void createTable(Connection connection){
        try {
            
            java.sql.Statement statement = connection.createStatement();

            
            Utils.executeQI(connection, "drop table personNumbers");
            Utils.executeQ(connection, "create table personNumbers (id_person int, person_number int)");
            //String indexNr = "create index nr on links_IDS.personNumbers(person_number)";

            //s = "create unique index i on links_match.personNumbers(id_person)";
            //System.out.println(s);
            //statement.execute(s);
            

            
            connection.commit();
        }  catch (SQLException e) {
                e.printStackTrace();
                Utils.closeConnection(connection);
                System.exit(-1);
            
        }
        
    }
    
    private static void initializePersonNumbers(Connection connection){
        
        Utils.executeQ(connection, "insert into links_ids.personNumbers (select id_person, id_person from links_cleaned.person_c_2014_05_07 where id_person <= " + max_id_person + ")");

    }
}