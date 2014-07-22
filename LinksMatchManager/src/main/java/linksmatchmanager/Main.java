/*
Copyright (C) IISH (www.iisg.nl)

This program is free software; you can redistribute it and/or modify
it under the terms of version 3.0 of the GNU General Public License as
published by the Free Software Foundation.


This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program; if not, write to the Free Software
Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
 */
package linksmatchmanager;

import java.sql.*;
//import java.util.Arrays;
//import java.util.ArrayList;
//import linksmatchmanager.DataSet.QuerySet;
//import linksmatchmanager.DataSet.NameType;
import linksmatchmanager.DataSet.QueryGroupSet;

/**
 * This is the Main class 
 * of the MatchManager Process
 * @author oaz
 */
public class Main {

    // Global vars
    private static QueryLoader ql;
    private static PrintLogger log;
    private static Connection conBase;
    private static Connection conMatch;
    private static int[][] variantFamilyName;
    private static int[][] variantFirstName;
    // private static String[] rootFamilyName;
    // private static String[] rootFirstName;
    
    private static int[][] rootFamilyName;
    private static int[][] rootFirstName;

    /**
     * Main method of the Match Manager software
     * @param args It is mandatory to use three arguments which are
     * the url, the user and the pass of the database server 
     */
    public static void main(String[] args) {

        // DEBUG info
        String ww = "";

        try {
            // Load logging
            log = new PrintLogger();

            /* Information about matching software */
            log.show("Links Match Manager v 0.1 BETA");
            log.show("For more information about this");
            log.show("software please contact: oaz");
            log.show("All rights reserved: IISG -2012");
            log.show("------------------------------------");

            /* Load arguments */
            // check length
            if (args.length != 4) {
                log.show("Invalid argument length, it should be 4");
                log.show("please use the following patern to start this sofware");
                log.show("linksmatchmanager.jar url user pass maxThreads (without '-')");

                // Stop program
                return;
            }

            // Load instances
            String url  = args[0];
            String user = args[1];
            String pass = args[2];
            String max  = args[3];

            /* Match process begins */
            log.show("Matching process started.");

            ProcessManager pm = new ProcessManager(Integer.parseInt(max));

            /* Create database conections*/
            conMatch = General.getConnection(url, "links_match", user, pass);
            conBase = General.getConnection(url, "links_base", user, pass);

            // Set read only
            conBase.setReadOnly(true);

            /** 
             * Run Query Generator to generate queries
             * as input we use the rocords from the 
             * match_process table in the links_match db
             */
            QueryGenerator mis = new QueryGenerator(conMatch);

            // TEST LINE: Print queries to check 
            // System.out.println(mis.printToString());

            /* Frequency In this stadium we do not use the frequencies */
            // log.show("Loading Frequency tables");


            // 
            // Create instance
            // FrequencyLoader fl = new FrequencyLoader(url, user, pass);

            // Load the frequencies
            // fl.load();

            /**
             * Loop through records in match_process
             */
            for (int i = 0; i < mis.is.getSize(); i++) {

                /**
                 * Create a new prematch variants object 
                 * for every record in match_process table 
                 */
                VariantLoader lv = new VariantLoader(url, user, pass);

                if (mis.is.get(i).get(0).method == 1) {

                    // Load the name sets
                    //rootFamilyName =
                    //      lv.loadRootNames(mis.is.get(i).get(0).prematch_familyname);

                    //rootFirstName = 
                     //       lv.loadRootNames(mis.is.get(i).get(0).prematch_firstname);
                    
                    rootFamilyName =
                            lv.loadRootNames(mis.is.get(i).get(0).prematch_familyname);

                    rootFirstName = 
                            lv.loadRootNames(mis.is.get(i).get(0).prematch_firstname);

                } else { // 0
                    // Load the name sets
                    variantFamilyName =
                            lv.loadNames(mis.is.get(i).get(0).prematch_familyname,
                            mis.is.get(i).get(0).prematch_familyname_value);

                    variantFirstName =
                            lv.loadNames(mis.is.get(i).get(0).prematch_firstname,
                            mis.is.get(i).get(0).prematch_firstname_value);
                }
                // Show user the active record and total
                log.show("Record " + (i + 1) + " of " + mis.is.getSize());

                /**
                 * Get a QueryGroupSet object which 
                 * contains a arraylist of objects
                 * every object contains information
                 * about the subqueries
                 */
                QueryGroupSet qgs = mis.is.get(i);

                // Loop through ranges/subqueries
                for (int j = 0; j < qgs.getSize(); j++) {

                    // Wait until processmanager gives permission
                    while (!pm.allowProcess()) {
                        log.show("No permission for new thread: Waiting 60 seconds");
                        Thread.sleep(60000);
                    }

                    // Add process to process list
                    pm.addProcess();

                    MatchAsync ma;

                    // Here begins threading
                    if (qgs.get(0).method == 1) {
                        ma = new MatchAsync(pm, i, j, ql, log, qgs, mis, conBase, conMatch, rootFirstName, rootFamilyName, true);
                    } else { // 0
                        ma = new MatchAsync(pm, i, j, ql, log, qgs, mis, conBase, conMatch, variantFirstName, variantFamilyName);
                    }

                    // inform user
                    log.show("ADD TO THREAD LIST: Range " + (j + 1) + " of " + qgs.getSize());

                    // start
                    ma.start();

                }
            }
        } catch (Exception e) {
            System.out.println("LinksMatchManager Error: " + e.getMessage() + "WW: " + ww);
        }
    }
}