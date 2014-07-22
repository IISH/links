package viewsummarizer;

import java.sql.Connection;
import java.sql.ResultSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Set;
import java.util.Map;
import java.io.File;
import java.io.FileWriter;

/**
 * text text text
 * @author oaz
 */
public class ViewSummarizer {

    private static Connection con;
    private static PrintLogger log;
    private static HashMap hm = new HashMap();

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {

        // Load logging
        log = new PrintLogger();

        /* Information about matching software */
        try {
            log.show("ViewSummarizer v 0.1");
            log.show("For more information about this");
            log.show("software please contact: oaz@iisg.nl");
            log.show("All rights reserved: IISG -2013");
            log.show("------------------------------------");

            /* Load arguments */
            // check length
            if (args.length != 7) {

                log.show("Invalid argument length, it should be 7");
                log.show("please use the following patern to start this sofware");
                log.show("java -jar ViewSummarizer.jar url name user pass tpl qry output");

                // Stop program
                return;
            }
        } catch (Exception e) {
            System.out.println("");
            e.printStackTrace();
            return;
        }

        // Set 7 args
        String url = args[0];
        String name = args[1];
        String user = args[2];
        String pass = args[3];
        String tpl = args[4];
        String qry = args[5];
        String output = args[6];

        // Create connection
        try {
            con = General.getConnection(url, name, user, pass);
        } catch (Exception e) {
            System.out.println("Conneceiton Error - ErrorMessage: " + e.getMessage());
            return;
        }

        // read template, it will be done with replace...
        String templateFile = General.fileToString(tpl);

        addFile(qry);

        // loop through hm to fire queries
        Set set = hm.entrySet();

        // Get an iterator
        Iterator i = set.iterator();

        System.out.println("There are " + hm.size() + " Queries");
        
        int counter = 0;
        // Display elements
        while (i.hasNext()) {
            
            counter++;
            
            System.out.println("Query " + counter + " of " + hm.size());

            Map.Entry me = (Map.Entry) i.next();

            String key = me.getKey() + "";
            String value = me.getValue() + "";

            // Check is key exists in template
            if (templateFile.contains("{" + key + "}")) {

                String st = "";

                try {

                    // waar ben ik me bezig
                    ResultSet rs = con.createStatement().executeQuery(value);
                    rs.first();
                    st = rs.getString(1);
                } catch (Exception e) {
                    System.out.println("Query error - query: " + value + " - Error message: " + e.getMessage());
                    continue;
                }

                // get result, only is there is a query result
                if (st != null) {
                    templateFile = templateFile.replaceAll("\\{" + key + "\\}", st);
                }
            }
        }

        // Write template to output
        try {
            
            System.out.println("Writing tamplate to output...");

            File newTextFile = new File(output);
            FileWriter fileWriter = new FileWriter(newTextFile);
            fileWriter.write(templateFile);
            fileWriter.close();
        } catch (Exception e) {
            System.out.println("Output error - Error message: " + e.getMessage());
        }

// done
        System.out.println("Done...");
        
    }

    /**
     * 
     * @param path 
     */
    private static void addFile(String path) {

        // Read queryfile
        String queryFile = General.fileToString(path);

        // Put everything into map
        String[] queryFileArray = queryFile.split(";");

        // loop through file
        for (String s : queryFileArray) {
            if (!s.isEmpty()) {
                addLine(s);
            }
        }
    }

    /**
     * 
     * @param line 
     */
    private static void addLine(String line) {

        // Split first
        String[] splitted = line.split("::");

        // Contains file
        if (splitted[0].replaceAll("\r", "").replaceAll("\n", "").equals("file")) {
            addFile(splitted[1].replaceAll("\r", "").replaceAll("\n", ""));
        }
        if (splitted[0].replaceAll("\r", "").replaceAll("\n", "").equals("loop")) {
            // do something
            addFile(splitted[1].replaceAll("\r", "").replaceAll("\n", ""));
            
            // add something to this text
            
            // use 
        }
        else { //
            hm.put(splitted[0].replaceAll("\r", "").replaceAll("\n", ""), splitted[1].replaceAll("\r", "").replaceAll("\n", ""));
        }
    }
}
