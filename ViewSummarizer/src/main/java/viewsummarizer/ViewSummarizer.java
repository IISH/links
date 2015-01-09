package viewsummarizer;

import java.io.File;
import java.io.FileWriter;

import java.sql.Connection;
import java.sql.ResultSet;

import java.util.HashMap;


/**
 * @author Omar Azouguagh
 * @author Fons Laan
 *
 * FL-09-Jan-2014 Latest change
 */
public class ViewSummarizer
{
    private static Connection db_conn;
    private static PrintLogger plog;
    private static String templateFile;

    private static String date;
    private static String id_match_process;

    private static String s1_maintype,  s2_maintype;
    private static String s1_type,      s2_type;
    private static String s1_role_ego,  s2_role_ego;
    private static String s1_source,    s2_source;
    private static String s1_range,     s2_range;
    private static String s1_startyear, s2_startyear;
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
    private static String use_minmax;

    private static String s1_potential, s2_potential;
    private static String s1_realized,  s2_realized;
    private static String s1_unique,    s2_unique;
    private static String s1_once,      s2_once;
    private static String s1_twice,     s2_twice;
    private static String s1_thrice,    s2_thrice;
    private static String s1_frice,     s2_frice;
    private static String s1_more,      s2_more;
    private static String s1_without,   s2_without;
    private static String s1_allcnts,   s2_allcnts;

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

            if( args.length != 5 ) {
                System.out.println( "Invalid argument length, it should be 5" );
                System.out.println( "Usage: java -jar ViewSummarizer.jar <db_url> <db_name> <db_user> <db_pass> <id_match_process>" );

                return;
            }
        }
        catch( Exception ex ) {
            System.out.println( ex.getMessage() );
            return;
        }

        // Set 5 args
        String db_url     = args[ 0 ];
        String db_name    = args[ 1 ];
        String db_user    = args[ 2 ];
        String db_pass    = args[ 3 ];
        String id_process = args[ 4 ];

        String template   = "LVS-template.html";
        String output     = "output/LVS-%.html";


        // Create connection
        try {
            plog.show( String.format( "cmd line parameters: %s %s %s %s %s",
                db_url, db_name, db_user, db_pass, id_process ) );
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
        String display = "";

        try {
            ResultSet rs = db_conn.createStatement().executeQuery( query );
            rs.first();
            result = rs.getString( 1 );
        }
        catch( Exception ex ) {
            System.out.println( "Query error: " + query + " - Error message: " + ex.getMessage() );
        }

        System.out.printf( "varname: %s, result: %s, query: %s\n", varname, result, query );

        if( varname.equals( "s1_source" ) || varname.equals( "s2_source" ) )
        {
            display = result;

                 if( result.equals( "211" ) ) { display += " = Groningen"; }
            else if( result.equals( "212" ) ) { display += " = Fri_Tresoar"; }
            else if( result.equals( "213" ) ) { display += " = Drenthe"; }
            else if( result.equals( "214" ) ) { display += " = Overijssel"; }
            else if( result.equals( "215" ) ) { display += " = Gelderland"; }
            else if( result.equals( "216" ) ) { display += " = Utrecht"; }
            else if( result.equals( "217" ) ) { display += " = N-H_Haarlem"; }
            else if( result.equals( "218" ) ) { display += " = Z-H_Nat-Archief"; }
            else if( result.equals( "220" ) ) { display += " = NBr_BHIC"; }
            else if( result.equals( "221" ) ) { display += " = Limburg"; }
            else if( result.equals( "222" ) ) { display += " = Flevoland"; }
            else if( result.equals( "223" ) ) { display += " = Z-H_Rotterdam"; }
            else if( result.equals( "224" ) ) { display += " = NBr_Breda"; }
            else if( result.equals( "225" ) ) { display += " = Zeeland"; }
            else if( result.equals( "226" ) ) { display += " = NBr_Eindhoven"; }
            else if( result.equals( "227" ) ) { display += " = Utr_Eemland"; }
            else if( result.equals( "228" ) ) { display += " = Leeuwarden"; }
            else if( result.equals( "229" ) ) { display += " = N-H_Alkmaar"; }
            else if( result.equals( "230" ) ) { display += " = Ned-Antillen"; }
            else if( result.equals( "231" ) ) { display += " = Z-H_Oegstgeest"; }
            else if( result.equals( "232" ) ) { display += " = Z-H_Dordrecht"; }
            else if( result.equals( "233" ) ) { display += " = Z-H_Voorne"; }
            else if( result.equals( "234" ) ) { display += " = Z-H_Goeree"; }
            else if( result.equals( "235" ) ) { display += " = Z-H_Rijnstreek"; }
            else if( result.equals( "236" ) ) { display += " = Z-H_Midden-Holland"; }
            else if( result.equals( "237" ) ) { display += " = Z-H_Vlaardingen"; }
            else if( result.equals( "238" ) ) { display += " = Z-H-Midden"; }
            else if( result.equals( "239" ) ) { display += " = Z-H_Gorinchem"; }
            else if( result.equals( "240" ) ) { display += " = Z-H_Westland"; }
            else if( result.equals( "241" ) ) { display += " = Z-H_Leidschendam"; }
            else if( result.equals( "242" ) ) { display += " = Z-H_Wassenaar"; }
        }

        else if( varname.equals( "s1_maintype" ) || varname.equals( "s2_maintype" ) )
        {
            display = result;
                 if( result.equals( "1" ) ) { display += " = Birth"; }
            else if( result.equals( "2" ) ) { display += " = Marriage"; }
            else if( result.equals( "3" ) ) { display += " = Death"; }
        }

        else if( varname.equals( "s1_role_ego" ) || varname.equals( "s2_role_ego" ) )
        {
            // the result may be a comma separated list
            //String[] results = result.split( "," );
            String[] results = result.split( "\\s*,\\s*" );     // and 'strip'
            System.out.printf( "result: %s, len results: %d\n", result, results.length );

            display = result;
            for( int i = 0; i < results.length; i++ )
            {
                String r = results[ i ];
                //System.out.printf( "%d, %s\n", i, r );
                //display += r;

                if( i == 0 ) { display += " = "; }
                else { display += ", "; }

                     if( r.equals(  "1" ) ) { display += "Child"; }
                else if( r.equals(  "2" ) ) { display += "Mother"; }
                else if( r.equals(  "3" ) ) { display += "Father"; }
                else if( r.equals(  "4" ) ) { display += "Bride"; }
                else if( r.equals(  "5" ) ) { display += "Mother bride"; }
                else if( r.equals(  "6" ) ) { display += "Father bride"; }
                else if( r.equals(  "7" ) ) { display += "Bridegroom"; }
                else if( r.equals(  "8" ) ) { display += "Mother bridegroom"; }
                else if( r.equals(  "9" ) ) { display += "Father bridegroom"; }
                else if( r.equals( "10" ) ) { display += "Deceased"; }
                else if( r.equals( "11" ) ) { display += "Partner"; }
            }
            if( display.isEmpty() ) { display = result; }
        }

        else { display = result; }

        replaceInTemplate( varname, display );

        return result;
    }


     /**
     * @param
     */
    private static String collectQuery( String varname, String query )
    {
        String result = "";
        String display = "";

        try {
            ResultSet rs = db_conn.createStatement().executeQuery( query );

            int count = 0;
            while( rs.next() ) {
                count += 1;
                int i = rs.getInt( 1 );
                int c = rs.getInt( 2 );
                //System.out.printf( "i: %d, c: %s", i, c );

                if( ! display.isEmpty() ) { display += ", "; }
                if( (count + 10) % 10 == 0 ) { display += "<br>"; }
                display += String.format( "%dx: %d", i, c );
            }

        }
        catch( Exception ex ) {
            System.out.println( "Query error: " + query + " - Error message: " + ex.getMessage() );
        }

        System.out.println( display );
        replaceInTemplate( varname, display );

        return "";
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

        query = "SELECT s1_type FROM links_match.match_process WHERE id = " + id_process;
        s1_type =  executeQuery( "s1_type", query );
        query = "SELECT s2_type FROM links_match.match_process WHERE id = " + id_process;
        s2_type = executeQuery( "s2_type", query );

        query = "SELECT s1_role_ego FROM links_match.match_process WHERE id = " + id_process;
        s1_role_ego = executeQuery( "s1_role_ego", query );
        query = "SELECT s2_role_ego FROM links_match.match_process WHERE id = " + id_process;
        s2_role_ego = executeQuery( "s2_role_ego", query );

        query = "SELECT s1_source FROM links_match.match_process WHERE id = " + id_process;
        s1_source = executeQuery( "s1_source", query );
        query = "SELECT s2_source FROM links_match.match_process WHERE id = " + id_process;
        s2_source = executeQuery( "s2_source", query );

        query = "SELECT s1_range FROM links_match.match_process WHERE id = " + id_process;
        s1_range = executeQuery( "s1_range", query );
        query = "SELECT s2_range FROM links_match.match_process WHERE id = " + id_process;
        s2_range = executeQuery( "s2_range", query );

        query = "SELECT s1_startyear FROM links_match.match_process WHERE id = " + id_process;
        s1_startyear = executeQuery( "s1_startyear", query );
        query = "SELECT s2_startyear FROM links_match.match_process WHERE id = " + id_process;
        s2_startyear = executeQuery( "s2_startyear", query );

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

        query = "SELECT use_minmax FROM links_match.match_process WHERE id = " + id_process;
        use_minmax = executeQuery( "use_minmax", query );

        /*
        System.out.printf( "date: %s\n", date );
        System.out.printf( "id_match_process: %s\n", id_match_process );

        // s1 & s2 table values
        System.out.printf( "s1_maintype:  %s, s2_maintype:  %s\n", s1_maintype,  s2_maintype );
        System.out.printf( "s1_type:      %s, s2_type:      %s\n", s1_type,      s2_type );
        System.out.printf( "s1_role_ego:  %s, s2_role_ego:  %s\n", s1_role_ego,  s2_role_ego );
        System.out.printf( "s1_source:    %s, s2_source:    %s\n", s1_source,    s2_source );
        System.out.printf( "s1_range:     %s, s2_range:     %s\n", s1_range,     s2_range );
        System.out.printf( "s1_startyear: %s, s2_startyear: %s\n", s1_startyear, s2_startyear );
        System.out.printf( "s1_endyear:   %s\n", s1_endyear );

        // single table values
        System.out.printf( "method: .................. %s\n", method );
        System.out.printf( "ignore_sex: .............. %s\n", ignore_sex );
        System.out.printf( "firstname: ............... %s\n", firstname );
        System.out.printf( "prematch_firstname: ...... %s\n", prematch_firstname );
        System.out.printf( "prematch_familyname: ..... %s\n", prematch_familyname );
        System.out.printf( "prematch_firstname_value:  %s\n", prematch_firstname_value );
        System.out.printf( "prematch_familyname_value: %s\n", prematch_familyname_value );
        System.out.printf( "use_firstname:             %s\n", use_firstname );
        System.out.printf( "use_familyname: .......... %s\n", use_familyname );
        System.out.printf( "use_minmax: .............. %s\n", use_minmax );
        */

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


        query = "SELECT COUNT(*) FROM ( SELECT COUNT( id_linksbase_1 ) AS s1_cnt FROM links_match.matches WHERE id_match_process = " + id_process + " GROUP BY id_linksbase_1 HAVING s1_cnt = 1 ) AS t";
        s1_once = executeQuery( "s1_once", query );

        query = "SELECT COUNT(*) FROM ( SELECT COUNT( id_linksbase_2 ) AS s2_cnt FROM links_match.matches WHERE id_match_process = " + id_process + " GROUP BY id_linksbase_2 HAVING s2_cnt = 1 ) AS t";
        s2_once = executeQuery( "s2_once", query );


        query = "SELECT COUNT(*) FROM ( SELECT COUNT( id_linksbase_1 ) AS s1_cnt FROM links_match.matches WHERE id_match_process = " + id_process + " GROUP BY id_linksbase_1 HAVING s1_cnt = 2 ) AS t";
        s1_twice = executeQuery( "s1_twice", query );

        query = "SELECT COUNT(*) FROM ( SELECT COUNT( id_linksbase_2 ) AS s2_cnt FROM links_match.matches WHERE id_match_process = " + id_process + " GROUP BY id_linksbase_2 HAVING s2_cnt = 2 ) AS t";
        s2_twice = executeQuery( "s2_twice", query );


        query = "SELECT COUNT(*) FROM ( SELECT COUNT( id_linksbase_1 ) AS s1_cnt FROM links_match.matches WHERE id_match_process = " + id_process + " GROUP BY id_linksbase_1 HAVING s1_cnt = 3 ) AS t";
        s1_thrice = executeQuery( "s1_thrice", query );

        query = "SELECT COUNT(*) FROM ( SELECT COUNT( id_linksbase_2 ) AS s2_cnt FROM links_match.matches WHERE id_match_process = " + id_process + " GROUP BY id_linksbase_2 HAVING s2_cnt = 3 ) AS t";
        s2_thrice = executeQuery( "s2_thrice", query );


        query = "SELECT COUNT(*) FROM ( SELECT COUNT( id_linksbase_1 ) AS s1_cnt FROM links_match.matches WHERE id_match_process = " + id_process + " GROUP BY id_linksbase_1 HAVING s1_cnt = 4 ) AS t";
        s1_frice = executeQuery( "s1_frice", query );

        query = "SELECT COUNT(*) FROM ( SELECT COUNT( id_linksbase_2 ) AS s2_cnt FROM links_match.matches WHERE id_match_process = " + id_process + " GROUP BY id_linksbase_2 HAVING s2_cnt = 4 ) AS t";
        s2_frice = executeQuery( "s2_frice", query );


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


        // all individual counts for 1x, 2x, 3x, etc occurrences
        query = "SELECT s1_cnt, COUNT(*) FROM ( SELECT id_linksbase_1, COUNT(*) AS s1_cnt FROM links_match.matches WHERE id_match_process = " + id_process + " GROUP BY id_linksbase_1 ) AS t GROUP BY s1_cnt";
        s1_allcnts = collectQuery("s1_allcnts", query);

        query = "SELECT s2_cnt, COUNT(*) FROM ( SELECT id_linksbase_2, COUNT(*) AS s2_cnt FROM links_match.matches WHERE id_match_process = " + id_process + " GROUP BY id_linksbase_2 ) AS t GROUP BY s2_cnt";
        s2_allcnts = collectQuery("s2_allcnts", query);
    }

}
