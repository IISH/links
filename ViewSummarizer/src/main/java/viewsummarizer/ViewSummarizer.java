package viewsummarizer;

import java.io.File;
import java.io.FileWriter;

import java.sql.Connection;
import java.sql.ResultSet;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Set;
import java.util.Map;

/**
 * @author Omar Azouguagh
 * @author Fons Laan
 *
 * FL-07-Jan-2014 Latest change
 */
public class ViewSummarizer
{
    private static Connection db_conn;
    private static PrintLogger plog;
    private static HashMap hm = new HashMap();
    private static String templateFile;

    private static String date;
    private static String id_match_process;

    private static String s1_maintype,  s2_maintype;
    private static String s1_role_ego,  s2_role_ego;
    private static String s1_source,    s2_source;
    private static String s1_startyear, s2_startyear;
    private static String s1_range,     s2_range;
    private static String s1_endyear;

    private static String method;
    private static String ignore_sex;
    private static String firstname;
    private static String prematch_firstname;
    private static String prematch_familyname;
    private static String prematch_firstname_value;
    private static String prematch_familyname_value;
    private static String use_firstname;
    private static String use_familyname;

    private static String s1_potential, s2_potential;
    private static String s1_realized,  s2_realized;
    private static String s1_unique,    s2_unique;
    private static String s1_once,      s2_once;
    private static String s1_more,      s2_more;
    private static String s1_without,   s2_without;


    /**
     * @param args command line arguments
     */
    public static void main( String[] args )
    {
        try
        {
            plog = new PrintLogger();

            String msg = "Links View Summarizer 2.0";
            plog.show( msg );

            if( args.length != 8 ) {
                System.out.println( "Invalid argument length, it should be 10" );
                System.out.println( "Usage: java -jar ViewSummarizer.jar <db_url> <db_name> <db_user> <db_pass> <template> <queries> <output> <id_match_process>" );

                return;
            }
        }
        catch( Exception ex ) {
            System.out.println( ex.getMessage() );
            return;
        }

        // Set 7 args
        String db_url     = args[ 0 ];
        String db_name    = args[ 1 ];
        String db_user    = args[ 2 ];
        String db_pass    = args[ 3 ];
        String template   = args[ 4 ];
        String queries    = args[ 5 ];
        String output     = args[ 6 ];
        String id_process = args[ 7 ];

        // Create connection
        try {
            plog.show( String.format( "cmd line parameters: %s %s %s %s %s %s %s %s",
                db_url, db_name, db_user, db_pass, template, queries, output, id_process ) );
            db_conn = General.getConnection( db_url, db_name, db_user, db_pass );
        }
        catch( Exception ex ) {
            System.out.println( "Connection Error: " + ex.getMessage() );
            return;
        }

        // put the id_process used in the name of the html output file for clarity
        output = output.replaceAll( "%", id_process );
        try { plog.show( String.format( "Output file will be: %s", output ) ); }
        catch( Exception ex ) { System.out.println( ex.getMessage() ); }

        // read template file to be used for queries output
        try { plog.show( String.format( "Reading template file: %s", template ) ); }
        catch( Exception ex ) { System.out.println( ex.getMessage() ); }
        templateFile = General.fileToString( template );

        // get variables from match_process table for the given id_proces
        readMatchProcess( id_process );


        // 'queries' contains a few SQL queries, and a few files (filenames) that contain more SQL queries
        //try { plog.show( String.format( "Reading query file: %s", queries ) ); }
        //catch( Exception ex ) { System.out.println( ex.getMessage() ); }
        //readFile( queries );                // fills HashMap hm with key/value pairs

        /*
        fillHMap( id_process );             // fills HashMap hm with key/value pairs

        Set set = hm.entrySet();            // HashMap -> Set
        System.out.println( "There are " + hm.size() + " queries" );
        
        int counter = 0;
        Iterator i = set.iterator();        // Get an iterator
        while( i.hasNext() )        // loop through hm to fire queries
        {
            counter++;
            System.out.printf( "%d ", counter );

            Map.Entry me = (Map.Entry) i.next();

            String key   = me.getKey()   + "";
            String value = me.getValue() + "";

            // Check if key exists in template
            if( templateFile.contains( "{" + key + "}" ) )
            {
                String st = "";

                try {
                    ResultSet rs = db_conn.createStatement().executeQuery( value );
                    rs.first();
                    st = rs.getString( 1 );
                }
                catch( Exception ex ) {
                    System.out.println( "Query error - query: " + value + " - Error message: " + ex.getMessage() );
                    continue;
                }

                System.out.printf( "resp: %s, key: %s, query: %s\n", st, key, value );

                // replace template variable
                if( st == null ) { templateFile = templateFile.replaceAll( "\\{" + key + "\\}", " " ); }
                else { templateFile = templateFile.replaceAll( "\\{" + key + "\\}", st ); }

            }
        }
        System.out.println( "" );
        */

        // Write template to output
        try {
            System.out.printf( "Writing template %s to output %s...\n", template, output );

            File newTextFile = new File( output );
            FileWriter fileWriter = new FileWriter( newTextFile );
            fileWriter.write( templateFile );
            fileWriter.close();
        }
        catch( Exception ex ) {
            System.out.println( "Output error - Error message: " + ex.getMessage() );
        }

        System.out.println( "Done." );
    }


     /**
     *
     */
    private static String executeQuery( String varname, String query )
    {
        String result = "";

        try {
            ResultSet rs = db_conn.createStatement().executeQuery( query );
            rs.first();
            result = rs.getString( 1 );
        }
        catch( Exception ex ) {
            System.out.println( "Query error: " + query + " - Error message: " + ex.getMessage() );
        }

        System.out.printf( "varname: %s, result: %s, query: %s\n", varname, result, query );

        replaceInTemplate( varname, result );

        return result;
    }


     /**
     *
     */
    private static void replaceInTemplate( String varname, String result )
    {
        // replace template variable
        if( result == null ) { templateFile = templateFile.replaceAll( "\\{" + varname + "\\}", " " ); }
        else { templateFile = templateFile.replaceAll( "\\{" + varname + "\\}", result ); }
    }


    /**
     *
     */
    private static void readMatchProcess( String id_process )
    {
        String query = "";

        query = "SELECT NOW()";
        date = executeQuery( "date", query );

        query = "SELECT id FROM links_match.match_process WHERE id = " + id_process;
        id_match_process = executeQuery( "id_match_process", query );


        // s1 & s2 table values
        query = "SELECT s1_maintype FROM links_match.match_process WHERE id = " + id_process;
        s1_maintype =  executeQuery( "s1_maintype", query );
        query = "SELECT s2_maintype FROM links_match.match_process WHERE id = " + id_process;
        s2_maintype = executeQuery( "s2_maintype", query );

        query = "SELECT s1_role_ego FROM links_match.match_process WHERE id = " + id_process;
        s1_role_ego = executeQuery( "s1_role_ego", query );
        query = "SELECT s2_role_ego FROM links_match.match_process WHERE id = " + id_process;
        s2_role_ego = executeQuery( "s2_role_ego", query );

        query = "SELECT s1_source FROM links_match.match_process WHERE id = " + id_process;
        s1_source = executeQuery( "s1_source", query );
        query = "SELECT s2_source FROM links_match.match_process WHERE id = " + id_process;
        s2_source = executeQuery( "s2_source", query );

        query = "SELECT s1_startyear FROM links_match.match_process WHERE id = " + id_process;
        s1_startyear = executeQuery( "s1_startyear", query );
        query = "SELECT s2_startyear FROM links_match.match_process WHERE id = " + id_process;
        s2_startyear = executeQuery( "s2_startyear", query );

        query = "SELECT s1_range FROM links_match.match_process WHERE id = " + id_process;
        s1_range = executeQuery( "s1_range", query );
        query = "SELECT s2_range FROM links_match.match_process WHERE id = " + id_process;
        s2_range = executeQuery( "s2_range", query );

        query = "SELECT s1_endyear FROM links_match.match_process WHERE id = " + id_process;
        s1_endyear = executeQuery( "s1_endyear", query );


        // single table values
        query = "SELECT method FROM links_match.match_process WHERE id = " + id_process;
        method = executeQuery( "method", query );

        query = "SELECT ignore_sex FROM links_match.match_process WHERE id = " + id_process;
        ignore_sex = executeQuery( "ignore_sex", query );

        query = "SELECT firstname FROM links_match.match_process WHERE id = " + id_process;
        firstname = executeQuery( "firstname", query );

        query = "SELECT prematch_firstname FROM links_match.match_process WHERE id = " + id_process;
        prematch_firstname = executeQuery( "prematch_firstname", query );

        query = "SELECT prematch_familyname FROM links_match.match_process WHERE id = " + id_process;
        prematch_familyname = executeQuery( "prematch_familyname", query );

        query = "SELECT prematch_firstname_value FROM links_match.match_process WHERE id = " + id_process;
        prematch_firstname_value = executeQuery( "prematch_firstname_value", query );

        query = "SELECT prematch_familyname_value FROM links_match.match_process WHERE id = " + id_process;
        prematch_familyname_value = executeQuery( "prematch_familyname_value", query );

        query = "SELECT use_firstname FROM links_match.match_process WHERE id = " + id_process;
        use_firstname = executeQuery( "use_firstname", query );

        query = "SELECT use_familyname FROM links_match.match_process WHERE id = " + id_process;
        use_familyname = executeQuery( "use_familyname", query );


        System.out.printf( "date: %s\n", date );
        System.out.printf( "id_match_process: %s\n", id_match_process );

        // s1 & s2 table values
        System.out.printf( "s1_maintype:  %s, s2_maintype:  %s\n", s1_maintype,  s2_maintype );
        System.out.printf( "s1_role_ego:  %s, s2_role_ego:  %s\n", s1_role_ego,  s2_role_ego );
        System.out.printf( "s1_source:    %s, s2_source:    %s\n", s1_source,    s2_source );
        System.out.printf( "s1_startyear: %s, s2_startyear: %s\n", s1_startyear, s2_startyear );
        System.out.printf( "s1_range:     %s, s2_range:     %s\n", s1_range,     s2_range );
        System.out.printf( "s1_endyear:   %s\n", s1_endyear );

        // single table values
        System.out.printf( "method:                    %s\n", method );
        System.out.printf( "ignore_sex:                %s\n", ignore_sex );
        System.out.printf( "firstname:                 %s\n", firstname );
        System.out.printf( "prematch_firstname:        %s\n", prematch_firstname );
        System.out.printf( "prematch_familyname:       %s\n", prematch_familyname );
        System.out.printf( "prematch_firstname_value:  %s\n", prematch_firstname_value );
        System.out.printf( "prematch_familyname_value: %s\n", prematch_familyname_value );
        System.out.printf( "use_firstname:             %s\n", use_firstname );
        System.out.printf( "use_familyname:            %s\n", use_familyname );


        query = "SELECT COUNT(*) FROM links_cleaned.registration_c WHERE id_source = " + s1_source + " AND registration_maintype = " + s1_maintype;
        s1_potential = executeQuery( "s1_potential", query );

        query = "SELECT COUNT(*) FROM links_cleaned.registration_c WHERE id_source = " + s2_source + " AND registration_maintype = " + s2_maintype;
        s2_potential = executeQuery( "s2_potential", query );


        query = "SELECT COUNT(*) FROM links_match.matches WHERE id_match_process = " + id_process;
        s1_realized = executeQuery( "s1_realized", query );

        query = "SELECT COUNT(*) FROM links_match.matches WHERE id_match_process = " + id_process;
        s2_realized = executeQuery( "s2_realized", query );


        query = "SELECT COUNT( DISTINCT( id_linksbase_1 ) ) FROM links_match.matches WHERE id_match_process = " + id_process;
        s1_unique = executeQuery( "s1_unique", query );

        query = "SELECT COUNT( DISTINCT( id_linksbase_2 ) ) FROM links_match.matches WHERE id_match_process = " + id_process;
        s2_unique = executeQuery( "s2_unique", query );


        //query = "SELECT count(*) FROM ( SELECT COUNT( id_linksbase_1 ) AS s1_cnt FROM links_match.matches GROUP BY id_linksbase_1 HAVING s1_cnt = 1 ) AS T";
        query = "SELECT count(*) FROM ( SELECT COUNT( id_linksbase_1 ) AS s1_cnt FROM links_match.matches WHERE id_match_process = " + id_process + " GROUP BY id_linksbase_1 HAVING s1_cnt = 1 ) AS T";
        s1_once = executeQuery( "s1_once", query );

        //query = "SELECT count(*) FROM ( SELECT COUNT( id_linksbase_2 ) AS s2_cnt FROM links_match.matches GROUP BY id_linksbase_2 HAVING s2_cnt = 1 ) AS T";
        query = "SELECT count(*) FROM ( SELECT COUNT( id_linksbase_2 ) AS s2_cnt FROM links_match.matches WHERE id_match_process = " + id_process + " GROUP BY id_linksbase_2 HAVING s2_cnt = 1 ) AS T";
        s2_once = executeQuery( "s2_once", query );


        int s1_unique_int = 0;
        if( ! s1_unique.isEmpty() ) { s1_unique_int = Integer.parseInt( s1_unique ); }

        int s2_unique_int = 0;
        if( ! s2_unique.isEmpty() ) { s2_unique_int = Integer.parseInt( s2_unique ); }

        int s1_once_int = 0;
        if( ! s1_once.isEmpty() ) { s1_once_int = Integer.parseInt( s1_once ); }

        int s2_once_int = 0;
        if( ! s2_once.isEmpty() ) { s2_once_int = Integer.parseInt( s2_once ); }

        int s1_potential_int = 0;
        if( ! s1_potential.isEmpty() ) { s1_potential_int = Integer.parseInt( s1_potential ); }

        int s2_potential_int = 0;
        if( ! s2_potential.isEmpty() ) { s2_potential_int = Integer.parseInt( s2_potential ); }

        s1_more = Integer.toString( s1_unique_int  - s1_once_int );
        s2_more = Integer.toString( s2_unique_int  - s2_once_int );

        replaceInTemplate( "s1_more", s1_more );
        replaceInTemplate( "s2_more", s2_more );

        s1_without = Integer.toString( s1_potential_int - s1_unique_int );
        s2_without = Integer.toString( s2_potential_int - s2_unique_int );

        replaceInTemplate( "s1_without", s1_without );
        replaceInTemplate( "s2_without", s2_without );


        /*
        key = "s1_more";
        value = "SELECT count(*) FROM ( SELECT COUNT( s1_id_linksbase ) AS s1_aantal FROM links_match.match_view GROUP BY s1_id_linksbase HAVING s1_aantal > 1 )";
        hm.put( key, value );

        key = "s2_more";
        value = "SELECT count(*) FROM ( SELECT COUNT( s2_id_linksbase ) AS s2_aantal FROM links_match.match_view GROUP BY s2_id_linksbase HAVING s2_aantal > 1 )";
        hm.put( key, value );


        key = "s1_without";
        value = "SELECT (SELECT COUNT(*) FROM links_prematch.links_base WHERE registration_maintype = 2 AND (id_source = " + s1_source + " OR id_source = " + s2_source + ") AND ego_role = (SELECT s1_role_ego FROM links_match.match_view LIMIT 0,1)) - (SELECT COUNT( DISTINCT( s1_id_linksbase) ) FROM links_match.match_view)";
        hm.put( key, value );

        key = "s2_without";
        value = "SELECT (SELECT COUNT(*) FROM links_prematch.links_base WHERE registration_maintype = 2 AND (id_source = " + s1_source + " OR id_source = " + s2_source + ") AND ego_role = (SELECT s2_role_ego FROM links_match.match_view LIMIT 0,1)) - (SELECT COUNT( DISTINCT( s2_id_linksbase) ) FROM links_match.match_view)";
        hm.put( key, value );
        */

    }


    /**
     * @param id_process
     */
    private static void fillHMap( String id_process )
    {
        String key   = "";
        String value = "";

        /*
        key = "date";
        value = "SELECT NOW()";
        hm.put( key, value );

        key = "id_match_process";
        value = "SELECT id FROM links_match.match_process WHERE id = " + id_process;
        hm.put( key, value );


        // s1 & s2 table
        key = "s1_maintype";
        String s1_maintype = "SELECT s1_maintype FROM links_match.match_process WHERE id = " + id_process;
        hm.put( key, s1_maintype );

        key = "s2_maintype";
        String s2_maintype = "SELECT s2_maintype FROM links_match.match_process WHERE id = " + id_process;
        hm.put( key, s2_maintype );


        key = "s1_role_ego";
        String s1_role_ego = "SELECT s1_role_ego FROM links_match.match_process WHERE id = " + id_process;
        hm.put( key, s1_role_ego );

        key = "s2_role_ego";
        String s2_role_ego = "SELECT s2_role_ego FROM links_match.match_process WHERE id = " + id_process;
        hm.put( key, s2_role_ego );


        key = "s1_source";
        String s1_source = "SELECT s1_source FROM links_match.match_process WHERE id = " + id_process;
        hm.put( key, s1_source );

        key = "s2_source";
        String s2_source = "SELECT s2_source FROM links_match.match_process WHERE id = " + id_process;
        hm.put( key, s2_source );


        key = "s1_startyear";
        String s1_startyear = "SELECT s1_startyear FROM links_match.match_process WHERE id = " + id_process;
        hm.put( key, s1_startyear );

        key = "s2_startyear";
        String s2_startyear = "SELECT s2_startyear FROM links_match.match_process WHERE id = " + id_process;
        hm.put( key, s2_startyear );


        key = "s1_range";
        String s1_range = "SELECT s1_range FROM links_match.match_process WHERE id = " + id_process;
        hm.put( key, s1_range );

        key = "s2_range";
        String s2_range = "SELECT s2_range FROM links_match.match_process WHERE id = " + id_process;
        hm.put( key, s2_range );


        key = "s1_endyear";
        String s1_endyear = "SELECT s1_endyear FROM links_match.match_process WHERE id = " + id_process;
        hm.put( key, s1_endyear );


        // single values table
        key = "method";
        String method = "SELECT method FROM links_match.match_process WHERE id = " + id_process;
        hm.put( key, method );

        key = "ignore_sex";
        String ignore_sex = "SELECT ignore_sex FROM links_match.match_process WHERE id = " + id_process;
        hm.put( key, ignore_sex );

        key = "firstname";
        String firstname = "SELECT firstname FROM links_match.match_process WHERE id = " + id_process;
        hm.put( key, firstname );

        key = "prematch_firstname";
        String prematch_firstname = "SELECT prematch_firstname FROM links_match.match_process WHERE id = " + id_process;
        hm.put( key, prematch_firstname );

        key = "prematch_familyname";
        String prematch_familyname = "SELECT prematch_familyname FROM links_match.match_process WHERE id = " + id_process;
        hm.put( key, prematch_familyname );

        key = "prematch_firstname_value";
        String prematch_firstname_value = "SELECT prematch_firstname_value FROM links_match.match_process WHERE id = " + id_process;
        hm.put( key, prematch_firstname_value );

        key = "prematch_familyname_value";
        String prematch_familyname_value = "SELECT prematch_familyname_value FROM links_match.match_process WHERE id = " + id_process;
        hm.put( key, prematch_familyname_value );

        key = "use_firstname";
        String use_firstname = "SELECT use_firstname FROM links_match.match_process WHERE id = " + id_process;
        hm.put( key, use_firstname );

        key = "use_familyname";
        String use_familyname = "SELECT use_familyname FROM links_match.match_process WHERE id = " + id_process;
        hm.put( key, use_familyname );
        */


        key = "s1_potential";
        //value = "SELECT COUNT(*) FROM links_prematch.links_base WHERE registration_maintype = 2 AND (id_source = 111 OR id_source = 113 ) AND ego_role = (SELECT s1_role_ego FROM links_match.match_view LIMIT 0,1)";
        //value = "SELECT COUNT(*) FROM links_prematch.links_base WHERE registration_maintype = 2 AND (id_source = " + s1_source + " OR id_source = " + s2_source + ") AND ego_role = (SELECT s1_role_ego FROM links_match.match_view LIMIT 0,1)";
        value = "SELECT COUNT(*) FROM links_cleaned.registration_c WHERE id_source = " + s1_source + " AND registration_maintype = " + s1_maintype;
        hm.put( key, value );

        key = "s2_potential";
        //value = "SELECT COUNT(*) FROM links_prematch.links_base WHERE registration_maintype = 2 AND (id_source = 111 OR id_source = 113 ) AND ego_role = (SELECT s2_role_ego FROM links_match.match_view LIMIT 0,1)";
        //value = "SELECT COUNT(*) FROM links_prematch.links_base WHERE registration_maintype = 2 AND (id_source = " + s1_source + " OR id_source = " + s2_source + ") AND ego_role = (SELECT s2_role_ego FROM links_match.match_view LIMIT 0,1)";
        value = "SELECT COUNT(*) FROM links_cleaned.registration_c WHERE id_source = " + s2_source + " AND registration_maintype = " + s2_maintype;
        hm.put( key, value );


        key = "s1_realized";
        value = "SELECT COUNT(*) FROM links_match.matches WHERE id_match_process = " + id_process;
        hm.put( key, value );

        key = "s2_realized";
        value = "SELECT COUNT(*) FROM links_match.matches WHERE id_match_process = " + id_process;
        hm.put( key, value );


        key = "s1_unique";
        value = "SELECT COUNT( DISTINCT( s1_id_linksbase) ) FROM links_match.matches WHERE id_match_process = " + id_process;
        hm.put( key, value );

        key = "s2_unique";
        value = "SELECT COUNT( DISTINCT( s2_id_linksbase) ) FROM links_match.matches WHERE id_match_process = " + id_process;
        hm.put( key, value );


        key = "s1_once";
        value = "SELECT count(*) FROM ( SELECT COUNT( s1_id_linksbase ) AS s1_aantal FROM links_match.matches GROUP BY s1_id_linksbase HAVING s1_aantal = 1 )";
        hm.put( key, value );

        key = "s2_once";
        value = "SELECT count(*) FROM ( SELECT COUNT( s2_id_linksbase ) AS s2_aantal FROM links_match.matches GROUP BY s2_id_linksbase HAVING s2_aantal = 1 )";
        hm.put( key, value );


        key = "s1_more";
        value = "SELECT count(*) FROM ( SELECT COUNT( s1_id_linksbase ) AS s1_aantal FROM links_match.match_view GROUP BY s1_id_linksbase HAVING s1_aantal > 1 )";
        hm.put( key, value );

        key = "s2_more";
        value = "SELECT count(*) FROM ( SELECT COUNT( s2_id_linksbase ) AS s2_aantal FROM links_match.match_view GROUP BY s2_id_linksbase HAVING s2_aantal > 1 )";
        hm.put( key, value );


        key = "s1_without";
        //value = "SELECT (SELECT COUNT(*) FROM links_prematch.links_base WHERE registration_maintype = 2 AND (id_source = 111 OR id_source = 113 ) AND ego_role = (SELECT s1_role_ego FROM links_match.match_view LIMIT 0,1)) - (SELECT COUNT( DISTINCT( s1_id_linksbase) ) FROM links_match.match_view)";
        value = "SELECT (SELECT COUNT(*) FROM links_prematch.links_base WHERE registration_maintype = 2 AND (id_source = " + s1_source + " OR id_source = " + s2_source + ") AND ego_role = (SELECT s1_role_ego FROM links_match.match_view LIMIT 0,1)) - (SELECT COUNT( DISTINCT( s1_id_linksbase) ) FROM links_match.match_view)";
        hm.put( key, value );

        key = "s2_without";
        //value = "SELECT (SELECT COUNT(*) FROM links_prematch.links_base WHERE registration_maintype = 2 AND (id_source = 111 OR id_source = 113 ) AND ego_role = (SELECT s2_role_ego FROM links_match.match_view LIMIT 0,1)) - (SELECT COUNT( DISTINCT( s2_id_linksbase) ) FROM links_match.match_view)";
        value = "SELECT (SELECT COUNT(*) FROM links_prematch.links_base WHERE registration_maintype = 2 AND (id_source = " + s1_source + " OR id_source = " + s2_source + ") AND ego_role = (SELECT s2_role_ego FROM links_match.match_view LIMIT 0,1)) - (SELECT COUNT( DISTINCT( s2_id_linksbase) ) FROM links_match.match_view)";
        hm.put( key, value );



    } // fillHMap


    /**
     * 
     * @param path 
     */
    private static void readFile( String path )
    {
        try {
            plog.show( " ");
            plog.show( String.format( "readFile(): %s", path ) );
        }
        catch( Exception ex ) { System.out.println( ex.getMessage() ); }

        // Read queryfile
        String queryFile = General.fileToString( path );

        String[] queryFileArray = queryFile.split( ";" );

        int nquery = 0;
        int nfile  = 0;
        for( String s : queryFileArray ) {
            s = s.trim();
            if( !s.isEmpty() ) {
                if( s.startsWith( "query" ) ) {
                    nquery++;
                    try { plog.show( String.format( "query %d: |%s|", nquery, s ) ); }
                    catch( Exception ex ) { System.out.println( ex.getMessage() ); }
                }
                else if( s.startsWith( "file" ) ) {
                    nfile++;
                    try { plog.show( String.format( "file %d: |%s|", nfile, s ) ); }
                    catch( Exception ex ) { System.out.println( ex.getMessage() ); }
                }

                addLine( s );
            }
        }
    }


    /**
     * 
     * @param line 
     */
    private static void addLine( String line )
    {
        //try { plog.show( String.format( "line: %s", line ) ); }
        //catch( Exception ex ) { System.out.println( ex.getMessage() ); }

        // Split first
        String[] splitted = line.split( "::" );

        if( splitted.length != 2 ) {
            try { plog.show( String.format( "splitted length: %d", splitted.length ) ); }
            catch( Exception ex ) { System.out.println( ex.getMessage() ); }
        }

        // Contains file
        if( splitted[ 0 ].replaceAll( "\r", "" ).replaceAll( "\n", "" ).equals( "file" ) ) {
            readFile( splitted[ 1 ].replaceAll( "\r", "" ).replaceAll( "\n", "" ) );
        }

        if( splitted[ 0 ].replaceAll( "\r", "" ).replaceAll( "\n", "" ).equals( "loop" ) ) {
            // do something
            readFile( splitted[ 1 ].replaceAll( "\r", "" ).replaceAll( "\n", "" ) );
            
            // add something to this text
            
            // use 
        }
        else {
            //hm.put( splitted[ 0 ].replaceAll( "\r", "" ).replaceAll( "\n", "" ), splitted[ 1 ].replaceAll( "\r", "" ).replaceAll( "\n", "" ) );
            String key   = splitted[ 0 ].replaceAll( "\r", "" ).replaceAll( "\n", "" );
            String value = splitted[ 1 ].replaceAll( "\r", "" ).replaceAll( "\n", "" );

            hm.put( key, value );
        }
    }
}
