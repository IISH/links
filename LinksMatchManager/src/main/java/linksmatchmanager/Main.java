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

import linksmatchmanager.DataSet.InputSet;
import linksmatchmanager.DataSet.QueryGroupSet;
import linksmatchmanager.DataSet.QuerySet;

/**
 * @author Omar Azouguagh
 * @author Fons Laan
 *
 * FL-30-Jun-2014 Imported from OA backup
 * FL-23-Feb-2015 Latest change
 */

public class Main
{
    // class global vars
    private static boolean debug;

    private static QueryLoader ql;
    private static PrintLogger plog;

    private static Connection dbconPrematch;
    private static Connection dbconMatch;
    private static Connection dbconTemp;

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
            plog = new PrintLogger( "LMM-" );
            plog.show( "Links Match Manager 2.0" );

            // Load arguments; check length
            if( args.length != 6 ) {
                plog.show( "Invalid argument length, it should be 6" );
                plog.show( "Usage: java -jar LinksMatchManager-2.0.jar <db_url> <db_username> <db_password> <max_threads> <debug>" );

                return;
            }

            // cmd line args
            String url                 = args[ 0 ];
            String user                = args[ 1 ];
            String pass                = args[ 2 ];
            String threads             = args[ 3 ];
            String max_heap_table_size = args[ 4 ];
            String debug_str           = args[ 5 ];

            if( debug_str.equals( "true" ) ) { debug = true; }
            else { debug = false; }

            String msg = String.format( "db_url: %s, db_username: %s, db_password: %s, max_threads: %s, max_heap_table_size: %s, debug: %s",
                url, user, pass, threads, max_heap_table_size, debug );
            System.out.println( msg );
            plog.show( msg );

            //Properties properties = Functions.getProperties();  // Read properties file

            plog.show( "Matching process started." );
            ProcessManager pm = new ProcessManager( Integer.parseInt( threads ) );

            int ncores = Runtime.getRuntime().availableProcessors();
            msg = String.format( "Available cores: %d", ncores );
            System.out.println( msg );
            plog.show( msg );

            int nthreads = java.lang.Thread.activeCount();
            msg = String.format( "Active threads: %d", nthreads );
            System.out.println( msg );
            plog.show( msg );

            /* Create database connections*/
            dbconMatch    = General.getConnection( url, "links_match", user, pass );
            dbconPrematch = General.getConnection( url, "links_prematch", user, pass );
            dbconTemp     = General.getConnection( url, "links_temp", user, pass );

            // we now use links_prematch for tmp mem tables, so it must be writable
            //dbconPrematch.setReadOnly( true );                // Set read only

            msg = String.format( "Setting MySQL max_heap_table_size to: %s", max_heap_table_size );
            System.out.println( msg );
            plog.show( msg );
            try
            {
                String query = "SET max_heap_table_size = " + max_heap_table_size;
                dbconPrematch.createStatement().execute( query );
            }
            catch( Exception ex ) { System.out.println( "Exception in main(): " + ex.getMessage() ); }

            // Create a single QueryGenerator object, that contains the input from the match_process table.
            // The input is derived from the 'y' records in the match_process table.
            QueryGenerator queryGen = new QueryGenerator( plog, dbconMatch );

            // The InputSet 'is', is the only accessible object from queryGen
            InputSet inputSet = queryGen.is;

            int isSize = inputSet.getSize();
            plog.show( String.format( "Number of matching records from links_match.match_process: %d\n", isSize ) );
            if( isSize == 0 ) {
                System.out.println( "Nothing to do; Stop." );
                plog.show( "Nothing to do; Stop." );
            }

            // Loop through the records from the match_process table
            for( int i = 0; i < isSize; i++ )
            {
                // The VariantLoader is broken.
                // And we will get the Levenshtein variants for each name of s1 one-by-one
                /*
                // Create a new prematch variants object for every record in match_process table
                VariantLoader vl = new VariantLoader( dbconPrematch, plog );


                plog.show( String.format( "\nmatching record: %d-of-%d\n", i+1, misSize ) );
                if( ! useExactMatch ) { plog.show( "Notice: skipping exact matches" ); }


                int method = mis.is.get( i ).get( 0 ).method;
                if( method == 1 )       // use root names
                {
                    // Load the name sets
                    rootFamilyName = vl.loadRootNames( mis.is.get( i ).get( 0 ).prematch_familyname );
                    //plog.show( String.format( "rootFamilyName size = %d x %d\n", rootFamilyName[0].length, rootFamilyName[1].length ) );

                    rootFirstName =  vl.loadRootNames( mis.is.get( i ).get( 0 ).prematch_firstname );
                    //plog.show( String.format( "rootFirstName size = %d x %d\n", rootFirstName[0].length, rootFirstName[1].length ) );

                }
                else    // use variant names
                {
                    // Load the name sets
                    variantFamilyName = vl.loadNames(
                        mis.is.get( i ).get( 0 ).prematch_familyname,
                        mis.is.get( i ).get( 0 ).prematch_familyname_value );
                    //plog.show( String.format( "variantFamilyName size = %d x %d\n", variantFamilyName[0].length, variantFamilyName[1].length ) );

                    variantFirstName = vl.loadNames(
                        mis.is.get( i ).get( 0 ).prematch_firstname,
                        mis.is.get( i ).get( 0 ).prematch_firstname_value );
                    //plog.show( String.format( "variantFirstName size = %d x %d\n", variantFirstName[0].length, variantFirstName[1].length ) );
                }
                */


                plog.show( "Record " + (i + 1) + " of " + isSize );     // Show user the active record and total

                // The inputSet contains an ArrayList< QueryGroupSet >
                // Each QueryGroupSet contains an ArrayList< QuerySet >
                // A QuerySet contains a row of the match_process table, plus the generated s1 & s2 queries.
                QueryGroupSet qgs = inputSet.get( i );

                // TODO Remember the first ls_firstname and ls_familyname, they go to a memory copy
                // If the ls_firstname and ls_familyname do not change, we keep on providing the
                // memory tables to matchAsync, otherwise they have to use the original tables.


                String lvs_table_familyname = qgs.get( 0 ).prematch_familyname;
                String lvs_table_firstname  = qgs.get( 0 ).prematch_firstname;

                System.out.println( "lvs familyname table: " + lvs_table_familyname );
                System.out.println( "lvs firstname  table: " + lvs_table_firstname );

                int lvs_dist_familyname = qgs.get( 0 ).prematch_familyname_value;
                int lvs_dist_firstname  = qgs.get( 0 ).prematch_firstname_value;

                System.out.println( "lvs familyname dist: " + lvs_dist_familyname );
                System.out.println( "lvs firstname  dist: " + lvs_dist_firstname );

                // Create memory tables to hold the ls_* tables
                String table_firstname_src  = "ls_firstname";
                String table_familyname_src = "ls_familyname";
                String name_postfix = "_mem";

                // creates table_firstname_mem & table_familyname_mem
                memtables_create( table_firstname_src, table_familyname_src, name_postfix );

                // and now change the names to the actual table names used !
                String lvs_table_familyname_use = lvs_table_familyname + name_postfix;
                String lvs_table_firstname_use  = lvs_table_firstname  + name_postfix;


                // The qgs QueryGroupSet is an ArrayList< QuerySet >
                // Loop through ranges/subqueries
                for( int j = 0; j < qgs.getSize(); j++ )
                {
                    QuerySet qs = qgs.get( 0 );

                    deleteMatches( qs.id ); // delete previous matches before creating new ones
                    showQuerySet( qs );

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
                        ma = new MatchAsync( debug, pm, i, j, ql, plog, qgs, inputSet, dbconPrematch, dbconMatch, dbconTemp,
                                lvs_table_firstname_use, lvs_table_familyname_use, rootFirstName, rootFamilyName, true );
                    }
                    else { // 0
                        ma = new MatchAsync( debug, pm, i, j, ql, plog, qgs, inputSet, dbconPrematch, dbconMatch, dbconTemp,
                                lvs_table_firstname_use, lvs_table_familyname_use, variantFirstName, variantFamilyName );
                    }

                    plog.show( "Add to thread list: Range " + (j + 1) + " of " + qgs.getSize() );

                    ma.start();
                    //ma.join();        // blocks parent thread?
                }

                // the tables should only be dropped after all threads have finished.
                //memtables_drop( table_firstname_src, table_familyname_src, name_postfix );

            }
        }
        catch( Exception ex ) { System.out.println( "LinksMatchManager/main() Exeption: " + ex.getMessage() ); }
    } // main


    private static void memtables_create( String table_firstname_src, String table_familyname_src, String name_postfix )
    {
        System.out.println( "memtables_create()" );

        try
        {
            //String table_firstname_dst  = "`" + table_firstname_src  + name_postfix + "`";
            //String table_familyname_dst = "`" + table_familyname_src + name_postfix + "`";

            // without backticks
            String table_firstname_dst  = table_firstname_src  + name_postfix;
            String table_familyname_dst = table_familyname_src + name_postfix;

            memtable_ls_name( table_firstname_src, table_firstname_dst );

            memtable_ls_name( table_familyname_src, table_familyname_dst );
        }
        catch( Exception ex ) { System.out.println( "Exception in memtables_create(): " + ex.getMessage() ); }
    } // memtables_create


    private static void memtables_drop( String table_firstname_src, String table_familyname_src, String name_postfix )
    {
        System.out.println( "memtables_drop()" );

        try
        {
            //String table_firstname_dst  = "`" + table_firstname_src  + name_postfix + "`";
            //String table_familyname_dst = "`" + table_familyname_src + name_postfix + "`";

            // without backticks
            String table_firstname_dst  = table_firstname_src  + name_postfix;
            String table_familyname_dst = table_familyname_src + name_postfix;

            String query = "DROP TABLE " + table_firstname_dst;
            dbconPrematch.createStatement().execute( query );

            query = "DROP TABLE " + table_familyname_dst;
            dbconPrematch.createStatement().execute( query );
        }
        catch( Exception ex ) { System.out.println( "Exception in memtables_drop(): " + ex.getMessage() ); }
    } // memtables_drop


    private static void memtable_ls_name( String src_table, String dst_table )
    {
        System.out.println( "memtable_ls_name() copying " + src_table + " -> " + dst_table );

        // Notice: leave out original keys if we do not need them now
        /*
                    + "  PRIMARY KEY (`id`),"
                    + "  KEY `value` (`value`),"
                    + "  KEY `length_1` (`length_1`),"
                    + "  KEY `length_2` (`length_2`),"
                    + "  KEY `name_str_1` (`name_str_1`),"
                    + "  KEY `name_str_2` (`name_str_2`),"
                    + "  KEY `name_int_1` (`name_int_1`)"
        */

        try
        {
            String[] name_queries =
            {
                "CREATE TABLE " + dst_table
                    + " ( "
                    + " `id` int(10) unsigned NOT NULL AUTO_INCREMENT,"
                    + "  `name_str_1` varchar(100) COLLATE utf8_bin DEFAULT NULL,"
                    + "  `name_str_2` varchar(100) COLLATE utf8_bin DEFAULT NULL,"
                    + "  `length_1` mediumint(8) unsigned DEFAULT NULL,"
                    + "  `length_2` mediumint(8) unsigned DEFAULT NULL,"
                    + "  `name_int_1` int(11) DEFAULT NULL,"
                    + "  `name_int_2` int(11) DEFAULT NULL,"
                    + "  `value` tinyint(3) unsigned DEFAULT NULL,"
                    + "  PRIMARY KEY (`id`),"
                    + "  KEY `value` (`value`),"
                    + "  KEY `name_int_1` (`name_int_1`)"
                    + " )"
                    + " ENGINE = MEMORY DEFAULT CHARSET = utf8 COLLATE = utf8_bin",

                "TRUNCATE TABLE " + dst_table,

                "ALTER TABLE " + dst_table + " DISABLE KEYS",

                "INSERT INTO " + dst_table + " SELECT * FROM " + src_table,

                "ALTER TABLE " + dst_table + " ENABLE KEYS"
            };

            for( String query : name_queries ) { dbconPrematch.createStatement().execute( query ); }
        }
        catch( Exception ex ) {
            String err = ex.getMessage();
            String msg = "Exception in memtable_ls_name(): " + err;
            System.out.println( msg );

            try {
                 plog.show( msg );
                if( err.equals( "The table '" + dst_table + "' is full" ) ) {
                    System.out.println( "EXIT" ); plog.show( "EXIT" );
                    System.exit( 1 );       // should not continue; would give wrong matching results.
                }
            }
            catch( Exception ex1 ) {
                System.out.println( "EXIT" );
                System.exit( 1 );
            }
        }
    } // memtable_ls_name


    /**
     * Delete previous matches for the given id, before creating new matches
     * @param id_match_process
     */
    private static void deleteMatches( int id_match_process )
    {
        try {
            plog.show( String.format( "Deleting matches for match_process id: %d", id_match_process ) );
            String query = "DELETE FROM matches WHERE id_match_process = " + id_match_process;

            dbconMatch.createStatement().execute( query );
            dbconMatch.createStatement().close();
        }
        catch( Exception ex ) { System.out.println( "LinksMatchManager/deleteMatches() Exception: " + ex.getMessage() ); }
    } // deleteMatches


    /**
     *
     * @param qs
     */
    private static void showQuerySet( QuerySet qs )
    {
        try
        {
            plog.show( "match_process values:" );
            plog.show( String.format( "id = %d", qs.id ) );
            plog.show( String.format( "query1 = %s", qs.query1 ) );
            plog.show( String.format( "query2 = %s", qs.query2 ) );

            plog.show( String.format( "use_mother .............. = %s", qs.use_mother ) );
            plog.show( String.format( "use_father .............. = %s", qs.use_father ) );
            plog.show( String.format( "use_partner ............. = %s", qs.use_partner ) );

            plog.show( String.format( "method .................. = %d", qs.method ) );

            plog.show( String.format( "ignore_sex .............. = %s", qs.ignore_sex ) );
            plog.show( String.format( "ignore_minmax ........... = %s", qs.ignore_minmax ) );
            plog.show( String.format( "firstname ............... = %d", qs.firstname ) );

            plog.show( String.format( "prematch_familyname ..... = %s", qs.prematch_familyname ) );
            plog.show( String.format( "prematch_familyname_value = %d", qs.prematch_familyname_value ) );
            plog.show( String.format( "prematch_firstname ...... = %s", qs.prematch_firstname ) );
            plog.show( String.format( "prematch_firstname_value  = %d", qs.prematch_firstname_value ) );

            //plog.show( String.format( "use_familyname .......... = %s", qs.use_familyname ) );
            //plog.show( String.format( "use_firstname ........... = %s", qs.use_firstname ) );
            //plog.show( String.format( "use_minmax .............  = %s", qs.use_minmax ) );

            plog.show( String.format( "int_familyname_e ........ = %d", qs.int_familyname_e ) );
            plog.show( String.format( "int_familyname_m .......  = %d", qs.int_familyname_m ) );
            plog.show( String.format( "int_familyname_f ........ = %d", qs.int_familyname_f ) );
            plog.show( String.format( "int_familyname_p ........ = %d", qs.int_familyname_p ) );

            plog.show( String.format( "int_firstname_e ......... = %d", qs.int_firstname_e ) );
            plog.show( String.format( "int_firstname_m ......... = %d", qs.int_firstname_m ) );
            plog.show( String.format( "int_firstname_f ......... = %d", qs.int_firstname_f ) );
            plog.show( String.format( "int_firstname_p ......... = %d", qs.int_firstname_p ) );

            plog.show( String.format( "int_minmax_e ............ = %d", qs.int_minmax_e ) );
            plog.show( String.format( "int_minmax_m ............ = %d", qs.int_minmax_m ) );
            plog.show( String.format( "int_minmax_f ............ = %d", qs.int_minmax_f ) );
            plog.show( String.format( "int_minmax_p ............ = %d", qs.int_minmax_p ) );
        }
        catch( Exception ex ) { ex.getMessage(); }
    } // showQuerySet

}
