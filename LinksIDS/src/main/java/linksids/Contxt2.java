package linksids;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import com.mysql.jdbc.Connection;
import com.mysql.jdbc.Statement;

import com.google.common.base.Strings;

/**
 * @author Cor Munnik
 * @author Fons Laan
 *
 * <p/>
 * FL-21-Jan-2014 Imported from CM
 * FL-12-May-2015 Latest change
 */
public class Contxt2
{
    static HashMap<String, Integer> placeNameToId_C = new HashMap<String, Integer>();
    static int highestContext_ID = 0;

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
    private static String sp10                         = sp09 + sp09;  
    private static String sp11                         = sp10 + sp10;  
    private static String sp12                         = sp11 + sp11; // 2048  
    private static String sp13                         = sp12 + sp12; // 4096  
    private static String sp14                         = sp13 + sp13; // 8192  
    private static String sp15                         = sp14 + sp14; //16384
    */

    private static String sp15 = Strings.repeat( "%s", 16384 );


    public static void initializeContext( Connection connection )
    {
        System.out.println( "Contxt2/initializeContext()" );

        Statement s = null;
        ResultSet resultSet = null;

        try {
            s = (Statement) connection.createStatement ();
            s.executeQuery("select max(Id_C) as m from links_ids.context");  // Only municipalitie names
            resultSet = s.getResultSet();


            int prev_Id_C = -1;
            while (resultSet.next ()){
                setHighestContext_ID(resultSet.getInt("m"));
                break;
            }

            resultSet.close();

            System.out.println("Highest Id_C = " + getHighestContext_ID());

        } catch (SQLException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }


        }

    /*
    public static void  loadContext(Connection connection){

        Statement s = null;
        ResultSet resultSet = null;


        try {
            s = (Statement) connection.createStatement ();
            s.executeQuery("select * from links_ids.context where Type = 'NAME' and Id_C < 10000");  // Only municipalitie names
            resultSet = s.getResultSet();


            int prev_Id_C = -1;
            while (resultSet.next ())
                placeNameToId_C.put(resultSet.getString("Value"), resultSet.getInt("Id_C"));

            resultSet.close();

        } catch (SQLException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        System.out.println("delete from links_ids.context where Id_C > 100012");
        Utils.executeQ(connection, "delete from links_ids.context where Id_C > 100012");
        System.out.println("delete from links_ids.context_context where Id_C_1 > 100012 or Id_C_2 > 100012");

        Utils.executeQ(connection, "delete from links_ids.context_context where Id_C_1 > 100012 or Id_C_2 > 100012");

        highestContext_ID = 100012;

        System.out.println("Highest Id_C = " + highestContext_ID);
    }

    */

    public static void saveContext(Connection connection, ArrayList<String> cL, ArrayList<String> ccL){

        flushContext(cL, connection);
        flushContextContext(ccL, connection);
        System.out.println("Highest Id_C = " + highestContext_ID);

    }


    public static int addCertificate(Connection connection, ArrayList<String> cL, ArrayList<String> ccL, String source, int Id_C_registration_location,
        int yearCertificate, int month, int day, String sequenceNumberCertificate)
    {
        int Id_C_1 = getNewContextID();

        addContext(cL, connection, Id_C_1, source, "LEVEL", "Source", "Event", "Exact", day, month, yearCertificate);
        addContext(cL, connection, Id_C_1, source, "NAME", source, "Event", "Exact", day, month, yearCertificate);
        addContext(cL, connection, Id_C_1, source, "PERIOD", "" + yearCertificate, "Event", "Exact", day, month, yearCertificate);
        addContext(cL, connection, Id_C_1, source, "SEQUENCE_NUMBER", sequenceNumberCertificate, "Event", "Exact", day, month, yearCertificate);

        addContextContext(ccL, connection, Id_C_1, Id_C_registration_location, source, "Source and Municipality",  null, null,  yearCertificate, month, day);

        return Id_C_1;
    }


    public static void addContext(ArrayList<String> cL, Connection connection, int Id_C, String source, String type, String value,
            String dateType, String estimation, int day, int month, int year){

        String t = String.format("(\"%d\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%d\",\"%d\",\"%d\"),",
                                    Id_C, "LINKS", source, type, value, dateType, estimation, day, month, year);


        cL.add(t);

         if(cL.size() > 10000)
             flushContext(cL, connection);


    }

    public static void addContextContext(ArrayList<String> ccL, Connection connection, int Id_C_1, int Id_C_2, String source, String relation,
            String dateType, String estimation, int day, int month, int year){

        String t = String.format("(\"%d\",\"%d\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%d\",\"%d\",\"%d\"),",
                                    Id_C_1, Id_C_2, "LINKS", source, relation, dateType, estimation, day, month, year);


        ccL.add(t);

         if(ccL.size() > 10000)
             flushContextContext(ccL, connection);


    }

    public static void flushContext(ArrayList<String> lL, Connection connection){

        if(lL.size() == 0)
            return;

        String s = String.format(sp15.substring(0, 2 * lL.size()), lL.toArray());
        lL.clear();
        s = s.substring(0, s.length() -1);
        String u = "insert into links_ids.context (Id_C, Id_D, Source, Type, Value, date_type, estimation, day, month, year) values" + s;

        //System.out.println(s.substring(0, 120));
        Utils.executeQ(connection, u);
    }

    public static void flushContextContext(ArrayList<String> lL, Connection connection){

        if(lL.size() == 0)
            return;

        String s = String.format(sp15.substring(0, 2 * lL.size()), lL.toArray());
        lL.clear();
        s = s.substring(0, s.length() -1);
        String u = "insert into links_ids.context_context (Id_C_1, Id_C_2, Id_D, Source, relation, date_type, estimation, day, month, year) values" + s;
        //System.out.println(u.substring(0, 120));
        Utils.executeQ(connection, u);
    }

    public static int getNewContextID(){

        return ++highestContext_ID;
    }

    public static int getHighestContext_ID() {
        return highestContext_ID;
    }

    public static void setHighestContext_ID(int highestContext_ID) {
        Contxt2.highestContext_ID = highestContext_ID;
    }

}

