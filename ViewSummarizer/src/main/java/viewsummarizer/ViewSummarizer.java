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
 * FL-03-Dec-2014 Latest change
 */
public class ViewSummarizer {

    private static Connection db_conn;
    private static PrintLogger plog;
    private static HashMap hm = new HashMap();

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

            if( args.length != 7 ) {
                System.out.println( "Invalid argument length, it should be 7" );
                System.out.println( "Usage: java -jar ViewSummarizer.jar <db_url> <db_name> <db_user> <db_pass> <template> <queries> <output>" );

                return;
            }
        }
        catch( Exception ex ) {
            System.out.println( ex.getMessage() );
            return;
        }

        // Set 7 args
        String db_url   = args[ 0 ];
        String db_name  = args[ 1 ];
        String db_user  = args[ 2 ];
        String db_pass  = args[ 3 ];
        String template = args[ 4 ];
        String queries  = args[ 5 ];
        String output   = args[ 6 ];

        // Create connection
        try {
            plog.show( String.format( "cmd line parameters: %s %s %s %s %s %s %s", db_url, db_name, db_user, db_pass, template, queries, output ) );
            db_conn = General.getConnection( db_url, db_name, db_user, db_pass );
        }
        catch( Exception ex ) {
            System.out.println( "Connection Error: " + ex.getMessage() );
            return;
        }

        try { plog.show( String.format( "Reading template file: %s", template ) ); }
        catch( Exception ex ) { System.out.println( ex.getMessage() ); }
        String templateFile = General.fileToString( template );

        try { plog.show( String.format( "Reading query file: %s", queries ) ); }
        catch( Exception ex ) { System.out.println( ex.getMessage() ); }
        addFile( queries );

        // loop through hm to fire queries
        Set set = hm.entrySet();

        // Get an iterator
        Iterator i = set.iterator();

        System.out.println( "There are " + hm.size() + " queries" );
        
        int counter = 0;
        // Display elements
        while( i.hasNext() )
        {
            counter++;
            
            System.out.println( "Query " + counter + " of " + hm.size() );

            Map.Entry me = (Map.Entry) i.next();

            String key = me.getKey() + "";
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

                // get result, only if there is a query result
                if( st != null ) {
                    templateFile = templateFile.replaceAll( "\\{" + key + "\\}", st );
                }
            }
        }

        // Write template to output
        try {
            System.out.println( "Writing template to output..." );

            File newTextFile = new File( output );
            FileWriter fileWriter = new FileWriter( newTextFile );
            fileWriter.write( templateFile );
            fileWriter.close();
        }
        catch( Exception ex ) {
            System.out.println("Output error - Error message: " + ex.getMessage() );
        }

        System.out.println( "Done." );
        
    }


    /**
     * 
     * @param path 
     */
    private static void addFile( String path )
    {
        try { plog.show( String.format( "addFile(): %s", path ) ); }
        catch( Exception ex ) { System.out.println( ex.getMessage() ); }

        // Read queryfile
        String queryFile = General.fileToString( path );

        String[] queryFileArray = queryFile.split( ";" );

        int ns = 0;
        for( String s : queryFileArray ) {
            s = s.trim();
            if( !s.isEmpty() ) {
                ns++;
                try { plog.show( String.format( "query %d: |%s|", ns, s ) ); }
                catch( Exception ex ) { System.out.println( ex.getMessage() ); }
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
        try { plog.show( String.format( "line: %s", line ) ); }
        catch( Exception ex ) { System.out.println( ex.getMessage() ); }

        // Split first
        String[] splitted = line.split( "::" );

        try { plog.show( String.format( "splitted length: %d", splitted.length ) ); }
        catch( Exception ex ) { System.out.println( ex.getMessage() ); }

        // Contains file
        if( splitted[ 0 ].replaceAll( "\r", "" ).replaceAll( "\n", "" ).equals( "file" ) ) {
            addFile( splitted[ 1 ].replaceAll( "\r", "" ).replaceAll( "\n", "" ) );
        }

        if( splitted[ 0 ].replaceAll( "\r", "" ).replaceAll( "\n", "" ).equals( "loop" ) ) {
            // do something
            addFile( splitted[ 1 ].replaceAll( "\r", "" ).replaceAll( "\n", "" ) );
            
            // add something to this text
            
            // use 
        }
        else {
            hm.put( splitted[ 0 ].replaceAll( "\r", "" ).replaceAll( "\n", "" ), splitted[ 1 ].replaceAll( "\r", "" ).replaceAll( "\n", "" ) );
        }
    }
}
