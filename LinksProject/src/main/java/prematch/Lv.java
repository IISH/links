package prematch;

import java.io.FileWriter;
import java.io.PrintStream;

import java.sql.ResultSet;
import java.util.ArrayList;

import javax.swing.JTextArea;
import javax.swing.JTextField;

import connectors.MySqlConnector;
import general.Functions;
import general.PrintLogger;

/**
 * @author Omar Azouguagh
 * @author Fons Laan
 *
 * <p/>
 * FL-30-Jun-2014 Imported from OA backup
 * FL-15-Jan-2015 Also want Levenshtein value 0 (together with 1,2,3,4)
 * FL-17-Feb-2015 Add names as integers to the ls_* tables
 * FL-17-Feb-2015 Latest change
 */
public class Lv extends Thread
{
    private boolean debug = false;

    private MySqlConnector db_conn = null;
    private String db_name;
    private String db_table;
    private boolean strict;
    private boolean also_exact_matches;

    private JTextField outputLine;
    private JTextArea  outputArea;

    private PrintLogger plog;

    /**
     * Constructor
     *
     * @param debug
     * @param db_conn
     * @param db_name
     * @param db_table
     * @param strict
     * @param also_exact_matches
     * @param outputLine
     * @param outputArea
     * @param plog
     */
    public Lv
    (
        boolean debug,
        MySqlConnector db_conn,
        String db_name,
        String db_table,
        boolean strict,
        boolean also_exact_matches,
        JTextField outputLine,
        JTextArea outputArea,
        PrintLogger plog
    )
    {
        this.debug                = debug;
        this.db_conn              = db_conn;
        this.db_name              = db_name;                // e.g. links_prematch
        this.db_table             = db_table;               // e.g. freq_firstnames or freq_familyname
        this.strict               = strict;
        this.also_exact_matches   = also_exact_matches;
        this.outputLine           = outputLine;
        this.outputArea           = outputArea;
        this.plog                  = plog;
    }


    /**
     *
     */
    @Override
    public void run()
    {
        long start = System.currentTimeMillis();

        String msg = "Levenshtein " + db_table + " done, strict = " + strict;
        showMessage( msg + "..", false, true );

        showMessage( "Including exact matches: " + also_exact_matches, false, true );

        String csvname = "LS-" + db_table + "-strict=" + strict + ".csv";
        showMessage( "Output filename: " + csvname, false, true );

        int nline = 0;

        try
        {
            String query = "SELECT id, name FROM " + db_name + "." + db_table;
            if( debug ) { showMessage( query, false, true ); }

            ResultSet rs = null;
            try { rs = db_conn.runQueryWithResult( query ); }
            catch( Exception ex ) {
                System.out.println( query );
                showMessage( query, false, true );

                System.out.println( ex.getMessage() );
                showMessage( ex.getMessage(), false, true );
                return;
            }

            ArrayList<Integer> id  = new ArrayList<Integer>();
            ArrayList<String> name = new ArrayList<String>();

            while( rs.next() ) {
                id.add( rs.getInt( "id" ) );
                name.add( rs.getString( "name" ) );
            }

            showMessage( "table " + db_table + " loaded", false, true );
            
            int size = name.size();

            FileWriter csvwriter = new FileWriter( csvname );

            // timing
            long timeExpand = 0;
            long begintime = System.currentTimeMillis();

            int step = 1000;
            int stepheight = step;

            // process all names
            for( int i = 0; i < size ; i++ )
            {
                int id1 = id.get(i);
                int id2 = 0;

                String name_1 = name.get(i);
                String name_2 = "";

                //int begin = i+1;                          // Omar
                int begin = i;                              // starting at i: also gives Levenshtein 0 values
                if( ! also_exact_matches ) { begin++; }     // this prevents names being identical, i.e. Levenshtein value > 0

                for( int j = begin; j < name.size() ; j++ )
                {
                    id2   = id.get( j );
                    name_2 = name.get(j);

                    int a = name_1.length();
                    int b = name_2.length();
                    
                    int smallest = (a < b) ?  a : b;

                    int ld = 4;

                    int basic;
                    int extra;

                    if( a == smallest ) {
                        basic = a;
                        extra = b;
                    }
                    else {
                        basic = b;
                        extra = a;
                    }

                    int diff = extra - basic;

                    if( diff > 4 ) { continue; }            // check difference

                    if( strict )
                    {
                        if( basic <  6 && diff > 1 ) { continue; }

                        if( basic <  9 && diff > 2 ) { continue; }          // 3 4 5

                        if( basic < 12 && diff > 3 ) { continue; }
                    }
                    else
                    {
                        if( basic < 3 && diff > 1 ) { continue; }

                        if( basic < 6 && diff > 2 ) { continue; }           // 3 4 5

                        if( basic < 9 && diff > 3 ) { continue; }
                    }

                    ld = levenshtein( name_1, name_2 );             // levenshtein distance

                    if( ld > 4 ) { continue; }                      // too high, -> not in CSV

                    if( strict )
                    {
                        if( basic <  6 && ld > 1 ) { continue; }

                        if( basic <  9 && ld > 2 ) { continue; }

                        if( basic < 12 && ld > 3 ) { continue; }
                    }
                    else    // levenshtein + length
                    {
                        if( basic <  3 && ld > 1 ) { continue; }

                        if( basic <  6 && ld > 2 ) { continue; }

                        if( basic < 10 && ld > 3 ) { continue; }
                    }

                    // Write to CSV
                    //String line = id1 + "," + id2 + "," + name_1 + "," + name_2 + "," + ld + "\r\n";    // old
                    //String line = name_1 + "," + name_2 + "," + smallest + "," + ld + "\r\n";
                    String line = name_1 + "," + name_2 + "," + a + ","+ b + "," + ld + "\r\n";
                    //if( debug ) { System.out.println( line ); }

                    nline ++;

                    try {  csvwriter.write( line ); }
                    catch( Exception ex ) { showMessage( "Levenshtein Error: " + ex.getMessage(), false, true ); }
                }

                // show progress
                if( i == stepheight ){
                    showMessage( "LV name " + i + " of " + size, true, true );
                    stepheight += step;
                }
            }

            showMessage( "", true, true );      // clear
            csvwriter.close();
            if(debug ) { showMessage( nline + " records written to CSV file", false, true ); }

            // elapsed
            timeExpand = System.currentTimeMillis() - begintime;
            int iTimeEx = (int)( timeExpand / 1000 );
        }
        catch( Exception ex ) { showMessage( "Levenshtein Error: " + ex.getMessage(), false, true ); }

        String ls_table = "";
        if( db_table.equals( "freq_firstnames" ) ) {
            if( strict ) { ls_table = "ls_firstname_strict"; }
            else { ls_table = "ls_firstname"; }
        }
        else if( db_table.equals( "freq_familyname" ) ) {
            if( strict ) { ls_table = "ls_familyname_strict"; }
            else { ls_table = "ls_familyname"; }
        }
        else {
            showMessage( "Levenshtein Error: table name: " + db_table + " ?", false, true );
            return;
        }

        try { loadCsvLsToTable( debug, db_conn, db_name, csvname, ls_table ); }
        catch( Exception ex ) { showMessage( "Levenshtein Error: " + ex.getMessage(), false, true ); }

        try { createLsFirstTable( debug, db_conn, db_name, ls_table ); }
        catch( Exception ex ) { showMessage( "Levenshtein Error: " + ex.getMessage(), false, true ); }


        elapsedShowMessage( msg, start, System.currentTimeMillis() );
        showMessage_nl();
    }


    /**
     *
     * @param s
     * @param t
     * @return
     */
    public static int levenshtein( String s, String t  )
    {
        int n = s.length();     // length of s
        int m = t.length();     // length of t

        int p[] = new int[n+1];     //'previous' cost array, horizontally
        int d[] = new int[n+1];     // cost array, horizontally
        int _d[];       //placeholder to assist in swapping p and d

        // indexes into strings s and t
        int i;      // iterates through s
        int j;      // iterates through t

        char t_j; // jth character of t

        int cost; // cost

        for( i = 0; i<=n; i++ ) {
            p[i] = i;
        }

        for( j = 1; j<=m; j++ ) {
            t_j = t.charAt(j-1);
            d[0] = j;

            for( i=1; i<=n; i++ ) {
                cost = s.charAt(i-1)==t_j ? 0 : 1;
                d[i] = Math.min( Math.min( d[i-1]+1, p[i]+1 ),  p[i-1]+cost );
            }

            _d = p;
            p = d;
            d = _d;
        }

        return p[ n ];
    }


    /**
     * @throws Exception
     */
    private void loadCsvLsToTable( boolean debug, MySqlConnector db_conn, String db_name, String csvname, String db_table )
    throws Exception
    {
        long start = System.currentTimeMillis();
        String msg = "Loading CSV LV data into LV table";
        showMessage( msg + "...", false, true );

        String query = "TRUNCATE TABLE `" + db_name + "`.`" + db_table + "`";
        if(debug ) { showMessage( query, false, true ); }
        db_conn.runQuery( query );

        query = "LOAD DATA LOCAL INFILE '" + csvname + "'"
            + " INTO TABLE `" + db_name + "`.`" + db_table + "`"
            + " FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n'"
            + " ( `name_1` , `name_2`, `length_1`, `length_2`, `value` );";

        if(debug ) { showMessage( query, false, true ); }
        db_conn.runQuery( query );

        elapsedShowMessage( msg, start, System.currentTimeMillis() );

    } // loadCsvLsToTable


        /**
     * @throws Exception
     */
    private void createLsFirstTable( boolean debug, MySqlConnector db_conn, String db_name, String ls_table )
    throws Exception
    {
        if( ls_table.equals( "ls_firstname" ) || ls_table.equals( "ls_familyname" ) )
        {
            long start = System.currentTimeMillis();
            String msg = "Filling table " + ls_table;
            showMessage( msg + "...", false, true );

            // copy the table to '_first', and delete records that differ in 1st char of names
            String ls_table_first = ls_table + "_first";
            String[] queries =
            {
                "DROP TABLE IF EXISTS " + ls_table_first,
                "CREATE TABLE " + ls_table_first + " LIKE " + ls_table + ";",
                "ALTER TABLE "  + ls_table_first + " DISABLE KEYS;",
                "INSERT INTO "  + ls_table_first + " SELECT * FROM " + ls_table + ";",
                "ALTER TABLE "  + ls_table_first + " ENABLE KEYS;",
                "DELETE FROM "  + ls_table_first + " WHERE SUBSTRING( `name_1`,1,1 ) <> SUBSTRING( `name_2`,1,1 );"
            };

            for( String query : queries ) {
                if( debug ) { showMessage( query, false, true ); }
                db_conn.runQuery( query );
            }

            String query = "SELECT COUNT(*) AS count FROM " + ls_table_first;
            try {
                ResultSet rs = db_conn.runQueryWithResult( query );
                while( rs.next() ) {
                    int count = rs.getInt( "count" );
                    showMessage( count + " records in " + ls_table_first, false , true);
                }
            }
            catch( Exception ex ) { showMessage( ex.getMessage(), false, true ); }
        }
        else
        { showMessage( "createLsFirstTable, skipping: " + ls_table, false, true ); }

    } // createLsFirstTable


    public static String stopWatch( int seconds )
    {
        int minutes = seconds / 60;
        int restsec = seconds % 60;
        int uren    = minutes / 60;
        int restmin = minutes % 60;

        String urenText     = "";
        String minutenText  = "";
        String secondenText = "";

        if( uren < 10 )    urenText     = "0";
        if( restmin < 10 ) minutenText  = "0";
        if( restsec < 10 ) secondenText = "0";

        return urenText + uren + ":" + minutenText + restmin + ":" + secondenText + restsec;
    }


    /**
     * @param logText
     * @param isMinOnly
     * @param newLine
     */
    private void showMessage( String logText, boolean isMinOnly, boolean newLine )
    {
        outputLine.setText( logText );

        if( !isMinOnly ) {
            String newLineToken = "";
            if( newLine ) {
                newLineToken = "\r\n";
            }

            outputArea.append( logText + newLineToken );
            //System.out.printf( "%s%s", logText, newLineToken );
            try { plog.show( logText ); }
            catch( Exception ex ) {
                System.out.println( ex.getMessage() );
                ex.printStackTrace( new PrintStream( System.out ) );
            }
        }
    } // showMessage


    /**
     * @param msg
     * @param start
     * @param stop
     */
    private void elapsedShowMessage( String msg, long start, long stop )
    {
        String elapsed = Functions.millisec2hms( start, stop );
        showMessage( msg + " " + elapsed, false, true );
    } // elapsedShowMessage


    /**
     */
    private void showMessage_nl()
    {
        String newLineToken = "\r\n";

        outputArea.append( newLineToken );

        try { plog.show( "" ); }
        catch( Exception ex ) {
            System.out.println( ex.getMessage() );
            ex.printStackTrace( new PrintStream( System.out ) );
        }
    } // showMessage_nl

}