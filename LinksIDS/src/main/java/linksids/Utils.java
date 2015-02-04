package linksids;

import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;

import com.mysql.jdbc.Connection;
import com.mysql.jdbc.Statement;


/**
 * @author Cor Munnik
 * @author Fons Laan
 *
 * <p/>
 * FL-21-Jan-2014 Imported from CM
 * FL-04-Feb-2015 Latest change
 */
public class Utils 
{
    public static int MAX_LIST_SIZE = 5000;

    static ArrayList< INDIVIDUAL >        iL = new ArrayList<INDIVIDUAL>();
    static ArrayList< CONTEXT >          iCL = new ArrayList<CONTEXT>();
    static ArrayList< CONTEXT_CONTEXT > iCCL = new ArrayList<CONTEXT_CONTEXT>();

    static int Id_C;
    static int Old_id_C;


    public static void addContext(Connection connection, CONTEXT context)
    {
        iCL.add(context);
        //if(iCL.size() >= 1000)
        if( iCL.size() >= MAX_LIST_SIZE )
        {
            writeCList(connection);
            iCL.clear();
        }
        
    }


    public static void addContextContext(Connection connection, CONTEXT_CONTEXT cc)
    {
        iCCL.add(cc);
        //if(iCCL.size() >= 1000)
        if( iCCL.size() >= MAX_LIST_SIZE )
        {
            writeCCList(connection);
            iCCL.clear();
        }
        
    }

    /**
     * 
     * This routine gets the context element of a Municipality 
     * It returns an array (length = 3) with either:
     *   
     *   Country - Province - Municipality or
     *   Country - null     - Municipality 
     *
     * @param ce
     * @return
     */
    public static String [] getLocationHierarchy(ContextElement ce)
    {
         String[] s = new String[3];
         int j = 0;
         while(ce != null){
             for(int i = 0; i < ce.getTypes().size(); i++)
                 if(ce.getTypes().get(i).equals("NAME")){                     
                     s[2-j++] = ce.getValues().get(i);
                     break;
                 }
             ce = ce.getParent();
         }
         
         if(s[0] == null){ // this means that there was only 1 level above ce (Country), not 2 (Country and Province), so we correct
             s[0] = s[1];
             s[1] = null;
         }

         return s;
        
    }


    private static void writeCList(Connection connection)
    {
        String insertStatement =
                "insert into links_ids.context (Id_D, Id_C, Source, Type, Value, date_type, estimation, day, month, year) values(\"";
        
        for(CONTEXT c: iCL){
            
            // Front part
            
            insertStatement += c.getId_D();
            insertStatement += "\", \"";
            
            insertStatement += c.getId_C();
            insertStatement += "\", \"";
            
            insertStatement += c.getSource();
            insertStatement += "\", \"";
            
            insertStatement += c.getType();
            insertStatement += "\", \"";
            
            insertStatement += c.getValue();
            insertStatement += "\", \"";            
                        
            
            // Timestamp part

            insertStatement += c.getDate_type();
            insertStatement += "\", \"";
            
            insertStatement += c.getEstimation();
            insertStatement += "\", \"";
            
            insertStatement += c.getDay();
            insertStatement += "\", \"";
            
            insertStatement += c.getMonth();
            insertStatement += "\", \"";
            
            insertStatement += c.getYear();
            insertStatement += "\"), (\"";
            
        }
        
        insertStatement = insertStatement.substring(0, insertStatement.length() - 4);
        
        //Utils.executeQ(connection, insertStatement);
        
        try {
            Statement s = (Statement) connection.createStatement ();
            //System.out.println(insertStatement.substring(0,250));
            System.out.println(" Inserted number of lines: " + s.executeUpdate(insertStatement));
        } catch (SQLException e) {
            // TODO Auto-generated catch block
            Utils.closeConnection(connection);
            e.printStackTrace();
        }
        
    }
    
    private static void writeCCList(Connection connection)
    {
        String insertStatement =
                "insert into links_ids.context_context (Id_D, Id_C_1, Id_C_2, Source, relation, date_type, estimation, day, month, year) values(\"";
        
        for(CONTEXT_CONTEXT cc: iCCL){
            
            // Front part
            
            insertStatement += cc.getId_D();
            insertStatement += "\", \"";
            
            insertStatement += cc.getId_C_1();
            insertStatement += "\", \"";
            
            insertStatement += cc.getId_C_2();
            insertStatement += "\", \"";
            
            insertStatement += cc.getSource();
            insertStatement += "\", \"";
            
            insertStatement += cc.getRelation();
            insertStatement += "\", \"";
            
            
            // Timestamp part

            insertStatement += cc.getDate_type();
            insertStatement += "\", \"";
            
            insertStatement += cc.getEstimation();
            insertStatement += "\", \"";
            
            insertStatement += cc.getDay();
            insertStatement += "\", \"";
            
            insertStatement += cc.getMonth();
            insertStatement += "\", \"";
            
            insertStatement += cc.getYear();
            insertStatement += "\"), (\"";
            
        }
        
        insertStatement = insertStatement.substring(0, insertStatement.length() - 4);
        
        try {
            Statement s = (Statement) connection.createStatement ();
            //System.out.println(insertStatement);
            s.execute(insertStatement);
        } catch (SQLException e) {
            // TODO Auto-generated catch block
            Utils.closeConnection(connection);
            e.printStackTrace();
        }
        
    }
    
    public static void createIDSTables(Connection connection)
    {
        try {
            Statement s = (Statement) connection.createStatement ();
            
            s.execute("use links_ids");
            
            String createStatement = CreateIDSTables.INDIVIDUAL;
            s.execute(createStatement);
            createStatement = CreateIDSTables.INDIVIDUAL_TRUNCATE;
            s.execute(createStatement);
            
            createStatement = CreateIDSTables.INDIV_INDIV;
            s.execute(createStatement);
            createStatement = CreateIDSTables.INDIV_INDIV_TRUNCATE;
            s.execute(createStatement);
            
            createStatement = CreateIDSTables.INDIV_CONTEXT;
            s.execute(createStatement);
            createStatement = CreateIDSTables.INDIV_CONTEXT_TRUNCATE;
            s.execute(createStatement);
            
            createStatement = CreateIDSTables.CONTEXT;
            s.execute(createStatement);
            createStatement = CreateIDSTables.CONTEXT_TRUNCATE;
            s.execute(createStatement);
            
            createStatement = CreateIDSTables.CONTEXT_CONTEXT;
            s.execute(createStatement);
            createStatement = CreateIDSTables.CONTEXT_CONTEXT_TRUNCATE;
            s.execute(createStatement);

        } catch (SQLException e) {
            // TODO Auto-generated catch block
            Utils.closeConnection(connection);
            e.printStackTrace();
        }
    }


    public static Connection connect(String dataBase)
    {
        Connection c = null;
        
        try {
            Class.forName("com.mysql.jdbc.Driver");
            c = (Connection) DriverManager.getConnection("jdbc:mysql:" + dataBase);
            c.setAutoCommit(false); 

        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            System.exit(-1);
        
        }
         catch (SQLException e) {
                e.printStackTrace();
                System.exit(-1);
        }
            
        return c;
    }


    public static void closeConnection(Connection connection)
    {
        try {
            connection.close();
        }
         catch (SQLException e) {
                e.printStackTrace();
                System.exit(-1);
        }
    }


    public static void commitConnection(Connection connection)
    {
        try {
            connection.commit();
        }
         catch (SQLException e) {
                closeConnection(connection);
                e.printStackTrace();
                System.exit(-1);
        }
    }    


    public static void executeQN(Connection connection, String s)
    {
        //System.out.println("executeQN: " + s);
        
        try {
            java.sql.Statement statement = connection.createStatement();

            statement.execute(s);
            //connection.commit();
        }  catch (SQLException e) {
            System.out.println(s);
            System.out.println("-->" + e.getErrorCode());
                e.printStackTrace();
                Utils.closeConnection(connection);
                System.exit(-1);
        }
    }


    public static void executeQ(Connection connection, String s)
    {
        //System.out.println("executeQ: " + s);

        try {
            java.sql.Statement statement = connection.createStatement();
            
            //System.out.println(s);

            statement.execute(s);
            //connection.commit();
        }  catch (SQLException e) {
            System.out.println(s);
            System.out.println("-->" + e.getErrorCode());
                e.printStackTrace();
                Utils.closeConnection(connection);
                System.exit(-1);
        }
    }


    public static void executeQI(Connection connection, String s)
    {
        //System.out.println("executeQI: " + s);

        try {
            java.sql.Statement statement = connection.createStatement();
            //System.out.println("+++-->" + s);

            statement.execute(s);
            //connection.commit();
        }  catch (SQLException e) {
            ;
        }
    }

    
    public static int getOld_id_C()
    {
        return Old_id_C;
    }


    public static void setOld_id_C(int old_id_C)
    {
        Old_id_C = old_id_C;
    }


    public static int getId_C()
    {
        
        //int x = 1/0;
    
        return Id_C++;
    }

    public static void setId_C(int id_C)
    {
        Id_C = id_C;
    }

}

