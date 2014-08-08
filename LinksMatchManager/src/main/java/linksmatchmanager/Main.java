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

import java.sql.Connection;
import java.util.Properties;

//import general.Functions;
//import linksutils.*;
import linksmatchmanager.DataSet.QueryGroupSet;


/**
 * @author Omar Azouguagh
 * @author Fons Laan
 *
 * <p/>
 * FL-30-Jun-2014 Imported from OA backup
 * FL-04-Aug-2014 Latest change
}
 */

public class Main
{
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
    public static void main(String[] args)
    {

        try {
            log = new PrintLogger();
            log.show( "Links Match Manager 2.0" );

            // Load arguments; check length
            if( args.length != 4 ) {
                log.show( "Invalid argument length, it should be 4" );
                log.show( "Please starts the Match Manager as follows:" );
                log.show( "java -jar LinksMatchManager-2.0.jar <db_url> <db_username> <db_password> <max_threads>" );

                return;     // Stop program
            }

            // Load instances
            String url  = args[ 0 ];
            String user = args[ 1 ];
            String pass = args[ 2 ];
            String max  = args[ 3 ];

            //Properties properties = Functions.getProperties();  // Read properties file


            log.show( "Matching process started." );
            ProcessManager pm = new ProcessManager( Integer.parseInt( max ) );

            /* Create database connections*/
            conMatch = General.getConnection( url, "links_match", user, pass );
            conBase  = General.getConnection( url, "links_base",  user, pass );

            conBase.setReadOnly( true );                    // Set read only

            /** 
             * Run Query Generator to generate queries.
             * As input we use the records from the match_process table in the links_match db
             */
            QueryGenerator mis = new QueryGenerator( conMatch );

            // TEST LINE: Print queries to check 
            // System.out.println(mis.printToString());

            /* Frequency In this stadium we do not use the frequencies */
            // log.show("Loading Frequency tables");

            // 
            // Create instance
            // FrequencyLoader fl = new FrequencyLoader(url, user, pass);

            // Load the frequencies
            // fl.load();


            int misSize = mis.is.getSize();
            System.out.printf( "size: %s\n", misSize  );

            /**
             * Loop through records in match_process
             */
            for( int i = 0; i < mis.is.getSize(); i++ )
            {
                System.out.printf( "i: %d\n", i );

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
            log.show( "Matching process ended." );

        } catch( Exception ex ) {
            System.out.println( "LinksMatchManager Error: " + ex.getMessage() );
        }
    }
}