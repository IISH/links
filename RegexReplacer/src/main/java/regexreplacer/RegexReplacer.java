package regexreplacer;
import java.sql.*;
import java.util.*;

public class RegexReplacer {

    private static Connection con;
    private static PrintLogger log;

    /**
     * 
     * @param args 
     */
    public static void main(String[] args) {

        try {
            // Load logging
            log = new PrintLogger();

            /* Information about matching software */
            log.show("RegexReplacer v 0.1");
            log.show("For more information about this");
            log.show("software please contact: oaz@iisg.nl");
            log.show("All rights reserved: IISG -2012");
            log.show("------------------------------------");

            /* Load arguments */
            // check length
            if (args.length != 1) {
                log.show("Invalid argument length, it should be 1");
                log.show("please use the following patern to start this sofware");
                log.show("regexreplacer.jar file");

                // Stop program
                return;
            }

            // Read file
            ArrayList<String> al = General.FileToArray(args[0]);

            // Loop through load settings
            String url      = al.get(0);
            String database = al.get(1);
            String user     = al.get(2);
            String pass     = al.get(3);
            String table    = al.get(4);

            // Remove this
            for (int i = 0; i < 5; i++) {
                al.remove(0);
            }

            // Make connection
            con = General.getConnection(url, database, user, pass);
            
            // Clear all
            con.createStatement().executeUpdate("UPDATE " + table + " SET clean = NULL;");
            
            ResultSet rsBeg = con.createStatement().executeQuery("SELECT COUNT(*) FROM " + table + ";");
            rsBeg.first();
            int totaal = rsBeg.getInt(1);

            ResultSet rs = con.createStatement().executeQuery("SELECT id, original FROM " + table + ";");

            int counter = 0;
            int step = 1000;

            while (rs.next()) {

                counter++; 
                
                if (counter == step) {
                    step += 1000;
                    System.out.println( counter + " of " + totaal );
                }

                // get row
                String id = rs.getString("id");
                String original = rs.getString("original") != null ? rs.getString("original") : "";
                
                if(original.isEmpty()){
                    continue;
                }
                
                original = original.toLowerCase();

                original = original.replaceAll(":", "");
                String cleaned = original;

                for (String s : al) {

                    if (s.equalsIgnoreCase("end")) {
                        break; // Breek for loop af
                    }

                    // Ga door
                    String regex = s.split(":")[0];
                    String replacement = s.split(":").length == 2 ? s.split(":")[1] : "";
                    
                    // Replace with expression
                    cleaned = cleaned.replaceAll(regex, replacement); 
                }

                // Set clean, if there is something cleaned
                if (!original.equals(cleaned)) {
                    con.createStatement().executeUpdate("UPDATE " + table + " SET clean = '" + General.prepareForMysql(cleaned) + "' WHERE id = " + id + ";");
                }
            }
            
            System.out.println("Done...");
            
            ResultSet rsTot = con.createStatement().executeQuery("SELECT COUNT(*) FROM " + table + " WHERE clean is not null;");
            rsTot.first();
            int cleaned = rsTot.getInt(1);
            
            System.out.println(cleaned + " of " + totaal + " cleaned. Proces done.");
            
        } catch (Exception e) {
            System.out.print(e.getMessage());
        }
    }
}
