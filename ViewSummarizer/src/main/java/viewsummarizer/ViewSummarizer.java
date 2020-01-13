package viewsummarizer;

import java.io.File;
import java.io.FileWriter;

import java.sql.Connection;
import java.sql.ResultSet;

import java.util.ArrayList;

import java.util.HashMap;


/**
 * @author Omar Azouguagh
 * @author Fons Laan
 *
 * FL-29-Feb-2016
 * FL-11-Sep-2017 debug, and template: [=3-4] => [=3-6]
 */
public class ViewSummarizer
{
    private static boolean debug = true;

    private static Connection db_conn;
    private static PrintLogger plog;
    private static String templateFile;

    private static String date;
    private static String id_match_process;

    private static String s1_factor,    s2_factor;
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
        //System.out.println( "main()" );

        try
        {
            plog = new PrintLogger();

            String msg = "Links View Summarizer 2.0";
            plog.show( msg );

            if( args.length < 5 ) {
                System.out.println( "Invalid argument length, it should be 5 or more" );
                System.out.println( "Usage: java -jar ViewSummarizer.jar <db_url> <db_name> <db_user> <db_pass> <hostname> [<id_match_process>]" );

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
        String db_table   = args[ 2 ];      // i.e. matches table
        String db_user    = args[ 3 ];
        String db_pass    = args[ 4 ];
        String hostname   = args[ 5 ];

        String ids_str = "";
        for( int i = 6; i < args.length; i++ ) {
            if( ids_str.length() > 0 ) { ids_str += " "; }
            ids_str += args[ i ];
        }

        if( debug ) { System.out.println( "Hostname: " + hostname ); }

        String template = "LVS-template.html";
        String output_  = "links-vs.html/LVS-%.html";


        // Create connection
        try {
            db_conn = General.getConnection( db_url, db_name, db_user, db_pass );
        }
        catch( Exception ex ) {
            System.out.println( "Connection Error: " + ex.getMessage() );
            return;
        }

        // without ids_str, we process all entries of match_process that have 'match' set to 'y'
        int[] ids = getIdProcessList( ids_str );

        for( int i = 0; i < ids.length; i++ )
        {
            String id_process = "" + ids[ i ];
            if( debug ) { System.out.println( "id_process: " + id_process ); }

            try {
                plog.show( String.format( "parameters: %s %s %s %s %s %s",
                    db_url, db_name, db_table, db_user, db_pass, id_process ) );
            }
            catch( Exception ex ) {
                System.out.println( "Exception: " + ex.getMessage() );
                return;
            }

            // put the id_process used in the name of the html output file for clarity
            String output = output_.replaceAll( "%", id_process );
            try { plog.show( String.format( "Output file will be: %s", output ) ); }
            catch( Exception ex ) { System.out.println( ex.getMessage() ); }

            // read template file to be used for queries output
            try { plog.show( String.format( "Reading template file: %s", template ) ); }
            catch( Exception ex ) { System.out.println( ex.getMessage() ); }
            templateFile = General.fileToString( template );

            replaceInTemplate( "hostname", hostname );

            // get variables from match_process table for the given id_process
            if( ! readMatchProcess( id_process, db_table ) ) { return; }  // id not found in the process table

            // Write template to output
            try {
                System.out.printf( "Writing template %s to output %s\n", template, output );

                File newTextFile = new File( output );
                FileWriter fileWriter = new FileWriter( newTextFile );
                fileWriter.write( templateFile );
                fileWriter.close();
            }
            catch( Exception ex ) {
                System.out.println( "Output error - Error message: " + ex.getMessage() );
            }
        }

        System.out.println( "Done." );
    } // main


     /**
     *
     */
    private static int[] getIdProcessList( String ids_str )
    {
        System.out.println( "getIdProcessList(), sources: " + ids_str );

        if( ! ids_str.isEmpty() ) {
            String[] parts = ids_str.split( " " );

            int[] idsInt = new int[ parts.length ];
            for( int i = 0 ; i < parts.length; i++ ) {
                idsInt[ i ] = Integer.parseInt( parts[ i ] );
            }
            return idsInt;
        }

        String query = "SELECT * FROM match_process ORDER BY id;";

        ArrayList< String > ids = new ArrayList();

        try {
            ResultSet rs = db_conn.createStatement().executeQuery( query );

            int count = 0;
            while( rs.next() ) {
                String id    = rs.getString( "id" );
                String match = rs.getString( "match" );
                if( match.equals( "y") ) {
                    if( debug ) { System.out.printf( "%d: %s %s\n", count, id, match ); }
                    count += 1;
                    ids.add( id );
                }
            }
            if( debug ) { System.out.printf( "count: %s\n", count ); }
        }
        catch( Exception ex ) {
            System.out.println( "Exception: " + query + " - Error message: " + ex.getMessage() );
        }

        int[] idsInt = new int[ ids.size() ];
        int i = 0;
        for( String id : ids ) {
            //System.out.println( id );
            idsInt[ i ] = Integer.parseInt( id );
            i += 1;
        }

        return idsInt;
     } // getIdProcessList


     /**
     *
     */
    private static String executeQuery( String varname, String query )
    {
        //System.out.println( "executeQuery()" );

        String result = "";
        String display = "";

        try {
            ResultSet rs = db_conn.createStatement().executeQuery( query );
            rs.first();
            result = rs.getString( 1 );
            if( result == null ) { result = ""; }
        }
        catch( Exception ex ) { System.out.println( "Exception: " + query + " - Error message: " + ex.getMessage() ); }

        if( debug ) { System.out.printf( "varname: %s, result: %s, query: %s\n", varname, result, query ); }

        if( varname.equals( "date" ) ) {
            if( result.length() > 16 ) { display = result.substring( 0, 16 ); } // date + hh:mm is enough
            else { display = result;}
        }

        else if( varname.equals( "s1_source" ) || varname.equals( "s2_source" ) )
        {
            display = result;

                 if( result.equals( "" ) )    { display += " = alles"; }
            else if( result.equals( "211" ) ) { display += " = Groningen"; }
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
            else { display += " = onbekend archief"; }
        }

        else if( varname.equals( "s1_maintype" ) || varname.equals( "s2_maintype" ) )
        {
            display = result;
                 if( result.equals( "1" ) ) { display += " = Birth"; }
            else if( result.equals( "2" ) ) { display += " = Marriage"; }
            else if( result.equals( "3" ) ) { display += " = Death"; }
            else { display += " = Unknown"; }
        }

        else if( varname.equals( "s1_type" ) || varname.equals( "s2_type" ) )
        {
            display = result;
                 if( result.equals( "g" ) ) { display += " = geboorte"; }
            else if( result.equals( "h" ) ) { display += " = huwelijk"; }
            else if( result.equals( "s" ) ) { display += " = scheiding"; }
            else if( result.equals( "o" ) ) { display += " = overlijden"; }
            else { display += " = onbekend"; }
        }

        else if( varname.equals( "s1_role_ego" ) || varname.equals( "s2_role_ego" ) )
        {
            // the result may be a comma separated list
            //String[] results = result.split( "," );
            String[] results = result.split( "\\s*,\\s*" );     // and 'strip'
            if( debug ) { System.out.printf( "result: %s, len results: %d\n", result, results.length ); }

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
                else { display += "Alien"; }
            }
            if( display.isEmpty() ) { display = result; }
        }

        else if( varname.equals( "firstname" ) )
        {
            display = result;
                 if( result.equals( "1" ) ) { display += " = firstname_1_2"; }
            else if( result.equals( "2" ) ) { display += " = firstname_1"; }
            else if( result.equals( "3" ) ) { display += " = firstname_1-or-2-or-3-or-4"; }
            else { display += " = unknown method"; }
        }

        else if( varname.equals( "s1_potential" ) ) {
            if( s1_factor.equals( "2" ) ) {             // double count the registrations (marriage & divorce)
                int count = 2 * Integer.parseInt( result );
                display = result = Integer.toString( count );
                System.out.printf( "s1_potential: %s -> %s\n", result, display );
            }
            else { display = result; }                  // birth & death: no change
        }

        else if( varname.equals( "s2_potential" ) ) {
            if( s2_factor.equals( "2" ) ) {             // double count the registrations (marriage & divorce)
                int count = 2 * Integer.parseInt( result );
                display = result = Integer.toString( count );
                System.out.printf( "s2_potential: %s -> %s\n", result, display );
            }
            else { display = result; }                  // birth & death: no change
        }

        else { display = result; }

        replaceInTemplate( varname, display );

        return result;
    } // executeQuery


     /**
     * @param
     */
    private static String collectQuery( String varname, String query )
    {
        //System.out.println( "collectQuery()" );

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
        catch( Exception ex ) { System.out.println( "Exception: " + query + " - Error message: " + ex.getMessage() ); }

        if( debug ) { System.out.println( display ); }
        replaceInTemplate( varname, display );

        return display;
    } // collectQuery


     /**
     * @param
     */
    private static void allcntsToTable( String s1_allcnts, String s2_allcnts )
    {
        //System.out.println( "allcntsToTable()" );

        if( s1_allcnts.isEmpty() && s2_allcnts.isEmpty() ) {
            System.out.println( "No counts." );
            return;
        }

        String s1_counts[] = s1_allcnts.split( "," );
        String s2_counts[] = s2_allcnts.split( "," );

        if( debug ) {
            System.out.printf( "%d counts: '%s'\n", s1_counts.length, s1_allcnts );
            System.out.printf( "%d counts: '%s'\n", s2_counts.length, s2_allcnts );
        }

        String trows = "";  // contents for template variable
        int rows = 9;       // max num of rows for (variable) explicit matching occurrences
        int nrow = 6;       // starting row for (variable) explicit matching occurrences

        for( int r = 0; r < rows; r++ )
        {
            String s1_cnt = "0";
            String s2_cnt = "0";

            int times = r + 1;
            String times_str = Integer.toString( times );

            for( int s = 0; s <  s1_counts.length; s++ )
            {
                String entry = s1_counts[ s ];
                //System.out.println( "entry: " + entry );
                String counts[] = entry.split( ":" );
                String scnt = counts[ 0 ].replace( "x", "").replace( "<br>", "").trim();
                //System.out.println( "scnt: " + scnt );
                int i = Integer.parseInt( scnt );
                if( i == times ) {
                    s1_cnt = counts[ 1 ];
                    break;
                }
            }

            for( int s = 0; s <  s2_counts.length; s++ )
            {
                String entry = s2_counts[ s ];
                //System.out.println( "entry: " + entry );
                String counts[] = entry.split( ":" );
                String scnt = counts[ 0 ].replace( "x", "").replace( "<br>", "").trim();
                //System.out.println( "scnt: " + scnt );
                int i = Integer.parseInt( scnt );
                if( i == times ) {
                    s2_cnt = counts[ 1 ];
                    break;
                }
            }

            // do not add table row if both counts are zero
            if( ! ( s1_cnt.equals( "0" ) &&  s2_cnt.equals( "0" ) )  ) {
                String trow = ""
                    + "    <tr>\n"
                    + "        <td align=\"right\"><b>" + Integer.toString( nrow ) + "</b></td>\n"
                    + "        <td>Registrations that matched " + times_str + " x</td>\n"
                    + "        <td align=\"right\">" + s1_cnt + "</td>\n"
                    + "        <td align=\"right\">" + s2_cnt + "</td>\n"
                    + "    </tr>\n";

                trows += trow;
                nrow++;
            }
        }

        replaceInTemplate( "trows", trows );
    } // allcntsToTable


     /**
     *
     */
    private static void replaceInTemplate( String varname, String result )
    {
        //System.out.printf( "varname: %s, result: %s\n", varname, result );

        // replace template variable
        if( result == null ) { templateFile = templateFile.replaceAll( "\\{" + varname + "\\}", " " ); }
        else { templateFile = templateFile.replaceAll( "\\{" + varname + "\\}", result ); }
    } // replaceInTemplate


    /**
     *
     */
    private static boolean readMatchProcess( String id_process, String table_matches )
    {
        //System.out.println( "readMatchProcess()" );

        String query = "";
        query = "SELECT NOW()";
        date = executeQuery( "date", query );

        query = "SELECT id FROM links_match.match_process WHERE id = " + id_process;
        id_match_process = executeQuery( "id_match_process", query );
        if( id_match_process.isEmpty() ) {
            System.out.println( "id_match_process not found in table links_match.match_process." );
            System.out.printf( "No output html file was generated for id_match_process %s\n.", id_process );
            return false;
        }

        // s1 & s2 table values
        query = "SELECT s1_maintype FROM links_match.match_process WHERE id = " + id_process;
        s1_maintype =  executeQuery( "s1_maintype", query );
        query = "SELECT s2_maintype FROM links_match.match_process WHERE id = " + id_process;
        s2_maintype = executeQuery( "s2_maintype", query );

        query = "SELECT s1_type FROM links_match.match_process WHERE id = " + id_process;
        s1_type =  executeQuery( "s1_type", query );
        query = "SELECT s2_type FROM links_match.match_process WHERE id = " + id_process;
        s2_type = executeQuery( "s2_type", query );

        if( s1_type.equals( "h" ) || s1_type.equals( "s" ) ) { s1_factor = "2"; }
        else { s1_factor = "1"; }

        if( s2_type.equals( "h" ) || s2_type.equals( "s" ) ) { s2_factor = "2"; }
        else { s2_factor = "1"; }

        replaceInTemplate( "s1_factor", s1_factor );
        replaceInTemplate( "s2_factor", s2_factor );

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

        if( s1_source.isEmpty() )
        { query = "SELECT COUNT(*) FROM links_cleaned.registration_c WHERE registration_maintype = " + s1_maintype;  }
        else
        {
            query = "SELECT COUNT(*) FROM links_cleaned.registration_c WHERE ";
            if( s1_source.contains( "," ) )
            {
                String s1_source_where = "(";
                for( String s : s1_source.split( "," ) ) {
                    s1_source_where += "OR id_source = " + s + " ";
                }
                s1_source_where = s1_source_where.replaceFirst( "OR", "" );     // remove first OR
                s1_source_where += ") ";
                query += s1_source_where;
            }
            else { query += "id_source = " + s1_source + " "; }

            query += "AND registration_maintype = " + s1_maintype;
        }
        s1_potential = executeQuery( "s1_potential", query );

        if( s2_source.isEmpty() ) { query = "SELECT COUNT(*) FROM links_cleaned.registration_c WHERE registration_maintype = " + s2_maintype; }
        else
        {
            query = "SELECT COUNT(*) FROM links_cleaned.registration_c WHERE ";
            if( s2_source.contains( "," ) )
            {
                String s2_source_where = "(";
                for( String s : s2_source.split( "," ) ) {
                    s2_source_where += "OR id_source = " + s + " ";
                }
                s2_source_where = s2_source_where.replaceFirst( "OR", "" );     // remove first OR
                s2_source_where += ") ";
                query += s2_source_where;
            }
            else { query += "id_source = " + s2_source + " "; }

            query += "AND registration_maintype = " + s2_maintype;
        }
        s2_potential = executeQuery( "s2_potential", query );


        query = String.format( "SELECT COUNT(*) FROM links_match.%s WHERE id_match_process = %s", table_matches, id_process );
        s1_realized = executeQuery( "s1_realized", query );

        query = String.format( "SELECT COUNT(*) FROM links_match.%s WHERE id_match_process = %s", table_matches, id_process );
        s2_realized = executeQuery( "s2_realized", query );


        query = String.format( "SELECT COUNT( DISTINCT( id_linksbase_1 ) ) FROM links_match.%s WHERE id_match_process = %s", table_matches, id_process );
        s1_unique = executeQuery( "s1_unique", query );

        query = String.format( "SELECT COUNT( DISTINCT( id_linksbase_2 ) ) FROM links_match.%s WHERE id_match_process = %s", table_matches, id_process );
        s2_unique = executeQuery( "s2_unique", query );


        query = String.format( "SELECT COUNT(*) FROM ( SELECT COUNT( id_linksbase_1 ) AS s1_cnt FROM links_match.%s WHERE id_match_process = %s GROUP BY id_linksbase_1 HAVING s1_cnt = 1 ) AS t", table_matches, id_process );
        s1_once = executeQuery( "s1_once", query );

        query = String.format( "SELECT COUNT(*) FROM ( SELECT COUNT( id_linksbase_2 ) AS s2_cnt FROM links_match.%s WHERE id_match_process = %s GROUP BY id_linksbase_2 HAVING s2_cnt = 1 ) AS t", table_matches, id_process );
        s2_once = executeQuery( "s2_once", query );


        query = String.format( "SELECT COUNT(*) FROM ( SELECT COUNT( id_linksbase_1 ) AS s1_cnt FROM links_match.%s WHERE id_match_process = %s GROUP BY id_linksbase_1 HAVING s1_cnt = 2 ) AS t", table_matches, id_process );
        s1_twice = executeQuery( "s1_twice", query );

        query = String.format( "SELECT COUNT(*) FROM ( SELECT COUNT( id_linksbase_2 ) AS s2_cnt FROM links_match.%s WHERE id_match_process = %s GROUP BY id_linksbase_2 HAVING s2_cnt = 2 ) AS t", table_matches, id_process );
        s2_twice = executeQuery( "s2_twice", query );


        query = String.format( "SELECT COUNT(*) FROM ( SELECT COUNT( id_linksbase_1 ) AS s1_cnt FROM links_match.%s WHERE id_match_process = %s GROUP BY id_linksbase_1 HAVING s1_cnt = 3 ) AS t", table_matches, id_process );
        s1_thrice = executeQuery( "s1_thrice", query );

        query = String.format( "SELECT COUNT(*) FROM ( SELECT COUNT( id_linksbase_2 ) AS s2_cnt FROM links_match.%s WHERE id_match_process = %s GROUP BY id_linksbase_2 HAVING s2_cnt = 3 ) AS t", table_matches, id_process );
        s2_thrice = executeQuery( "s2_thrice", query );


        query = String.format( "SELECT COUNT(*) FROM ( SELECT COUNT( id_linksbase_1 ) AS s1_cnt FROM links_match.%s WHERE id_match_process = %s GROUP BY id_linksbase_1 HAVING s1_cnt = 4 ) AS t", table_matches, id_process );
        s1_frice = executeQuery( "s1_frice", query );

        query = String.format( "SELECT COUNT(*) FROM ( SELECT COUNT( id_linksbase_2 ) AS s2_cnt FROM links_match.%s WHERE id_match_process = %s GROUP BY id_linksbase_2 HAVING s2_cnt = 4 ) AS t", table_matches, id_process );
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

        int s1_more_int = s1_unique_int  - s1_once_int;
        int s2_more_int = s2_unique_int  - s2_once_int;

        int s1_without_int = s1_potential_int  - s1_unique_int;
        int s2_without_int = s2_potential_int  - s2_unique_int;

        System.out.printf( "s1_potential_int: .................. %d\n", s1_potential_int );
        System.out.printf( "s2_potential_int: .................. %d\n", s2_potential_int );
        System.out.printf( "s1_unique_int: ..................... %d\n", s1_unique_int );
        System.out.printf( "s2_unique_int: ..................... %d\n", s2_unique_int );
        System.out.printf( "s1_once_int: ....................... %d\n", s1_once_int );
        System.out.printf( "s2_once_int: ....................... %d\n", s2_once_int );

        System.out.printf( "s1_more_int = unique - once: ....... %d\n", s1_more_int );
        System.out.printf( "s2_more_int = unique - once: ....... %d\n", s2_more_int );

        System.out.printf( "s1_without_int = potential - unique: %d\n", s1_without_int );
        System.out.printf( "s2_without_int = potential - unique: %d\n", s2_without_int );

        //s1_more = Integer.toString( s1_unique_int  - s1_once_int );
        //s2_more = Integer.toString( s2_unique_int  - s2_once_int );
        s1_more = Integer.toString( s1_more_int );
        s2_more = Integer.toString( s2_more_int );

        replaceInTemplate( "s1_more", s1_more );
        replaceInTemplate( "s2_more", s2_more );

        //s1_without = Integer.toString( s1_potential_int - s1_unique_int );
        //s2_without = Integer.toString( s2_potential_int - s2_unique_int );
        s1_without = Integer.toString( s1_without_int );
        s2_without = Integer.toString( s2_without_int );

        replaceInTemplate( "s1_without", s1_without );
        replaceInTemplate( "s2_without", s2_without );


        // all individual counts for 1x, 2x, 3x, etc occurrences
        query = String.format( "SELECT s1_cnt, COUNT(*) FROM ( SELECT id_linksbase_1, COUNT(*) AS s1_cnt FROM links_match.%s WHERE id_match_process = %s GROUP BY id_linksbase_1 ) AS t GROUP BY s1_cnt", table_matches, id_process );
        s1_allcnts = collectQuery("s1_allcnts", query);

        query = String.format( "SELECT s2_cnt, COUNT(*) FROM ( SELECT id_linksbase_2, COUNT(*) AS s2_cnt FROM links_match.%s WHERE id_match_process = %s GROUP BY id_linksbase_2 ) AS t GROUP BY s2_cnt", table_matches, id_process );
        s2_allcnts = collectQuery("s2_allcnts", query);

        allcntsToTable( s1_allcnts, s2_allcnts );

        return true;
    } // readMatchProcess

}
