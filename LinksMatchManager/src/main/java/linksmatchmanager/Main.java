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

import java.util.ArrayList;
import java.util.List;
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
 * FL-01-Dec-2014 Latest change
 */

public class Main
{
    // Global vars
    private static QueryLoader ql;
    private static PrintLogger plog;
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
    public static void main( String[] args )
    {
        try {
            plog = new PrintLogger();
            plog.show( "Links Match Manager 2.0" );

            // Load arguments; check length
            if( args.length != 4 ) {
                plog.show( "Invalid argument length, it should be 4" );
                plog.show( "Usage: java -jar LinksMatchManager-2.0.jar <db_url> <db_username> <db_password> <max_threads>" );

                return;
            }

            // cmd line args
            String url  = args[ 0 ];
            String user = args[ 1 ];
            String pass = args[ 2 ];
            String max  = args[ 3 ];

            String msg = String.format( "db_url: %s, db_username: %s, db_password: %s, max_threads: %s", url, user, pass, max );
            System.out.println( msg );
            plog.show( msg );

            //Properties properties = Functions.getProperties();  // Read properties file

            plog.show( "Matching process started." );
            ProcessManager pm = new ProcessManager( Integer.parseInt( max ) );

            int ncores = Runtime.getRuntime().availableProcessors();
            msg = String.format( "Available cores: %d", ncores );
            System.out.println( msg );
            plog.show( msg );

            int nthreads = java.lang.Thread.activeCount();
            msg = String.format( "Active threads: %d", nthreads );
            System.out.println( msg );
            plog.show( msg );

            /* Create database connections*/
            conMatch = General.getConnection( url, "links_match", user, pass );
            conBase  = General.getConnection( url, "links_base",  user, pass );

            conBase.setReadOnly( true );                    // Set read only

            String query = "TRUNCATE table matches";        // delete previous matches
            System.out.println( query );
            plog.show( query );
            conMatch.createStatement().execute( query );

            /** 
             * Run Query Generator to generate queries.
             * As input we use the records from the match_process table in the links_match db
             */
            QueryGenerator mis = new QueryGenerator( plog, conMatch );

            // TEST LINE: Print queries to check 
            // System.out.println(mis.printToString());

            // KM-01-Dec-2014: frequencies not used for matching,
            // but used for automatic matching based on Levenshtein
            /* Frequency In this stadium we do not use the frequencies */
            // log.show("Loading Frequency tables");

            // 
            // Create instance
            // FrequencyLoader fl = new FrequencyLoader(url, user, pass);

            // Load the frequencies
            // fl.load();


            int misSize = mis.is.getSize();
            plog.show( String.format( "Number of matching records from links_match.match_process: %d\n", misSize ) );

            // Loop through records in match_process
            for( int i = 0; i < mis.is.getSize(); i++ )
            {
                // Create a new prematch variants object for every record in match_process table
                VariantLoader vl = new VariantLoader( url, user, pass );

                int method = mis.is.get( i ).get( 0 ).method;
                plog.show( String.format( "matching record: %d-of-%d, method = %d\n", i+1, misSize, method ) );

                if( method == 1 )
                {
                    // Load the name sets
                    rootFamilyName = vl.loadRootNames( mis.is.get( i ).get( 0 ).prematch_familyname );
                    //log.show( String.format( "rootFamilyName size = %d x %d\n", rootFamilyName[0].length, rootFamilyName[1].length ) );

                    rootFirstName =  vl.loadRootNames( mis.is.get( i ).get( 0 ).prematch_firstname );
                    //log.show( String.format( "rootFirstName size = %d x %d\n", rootFirstName[0].length, rootFirstName[1].length ) );

                }
                else    // method = 0
                {
                    // Load the name sets
                    variantFamilyName = vl.loadNames(
                        mis.is.get( i ).get( 0 ).prematch_familyname,
                        mis.is.get( i ).get( 0 ).prematch_familyname_value );
                    //log.show( String.format( "variantFamilyName size = %d x %d\n", variantFamilyName[0].length, variantFamilyName[1].length ) );

                    variantFirstName = vl.loadNames(
                        mis.is.get( i ).get( 0 ).prematch_firstname,
                        mis.is.get( i ).get( 0 ).prematch_firstname_value );
                    //log.show( String.format( "variantFirstName size = %d x %d\n", variantFirstName[0].length, variantFirstName[1].length ) );
                }

                // Show user the active record and total
                plog.show( "Record " + (i + 1) + " of " + mis.is.getSize() );

                /**
                 * Get a QueryGroupSet object which contains a arraylist of objects
                 * every object contains information about the subqueries
                 */
                QueryGroupSet qgs = mis.is.get( i );

                //List< MatchAsync > threads = new ArrayList< MatchAsync >();

                // Loop through ranges/subqueries
                for( int j = 0; j < qgs.getSize(); j++ )
                {
                    // Wait until process manager gives permission
                    while( !pm.allowProcess() ) {
                        plog.show( "No permission for new thread: Waiting 60 seconds" );
                        Thread.sleep( 60000 );
                    }

                    // Add process to process list
                    pm.addProcess();

                    MatchAsync ma;
                    // Here begins threading
                    if( qgs.get( 0 ).method == 1 ) {
                        ma = new MatchAsync( pm, i, j, ql, plog, qgs, mis, conBase, conMatch, rootFirstName, rootFamilyName, true );
                    } else { // 0
                        ma = new MatchAsync( pm, i, j, ql, plog, qgs, mis, conBase, conMatch, variantFirstName, variantFamilyName );
                    }

                    plog.show( "Add to thread list: Range " + (j + 1) + " of " + qgs.getSize() );

                    ma.start();
                    //ma.join();        // blocks parent thread?
                }
            }
            System.out.println( "Matching process ended." );
            plog.show( "Matching process ended." );

        } catch( Exception ex ) {
            System.out.println( "LinksMatchManager Error: " + ex.getMessage() );
        }
    }
}