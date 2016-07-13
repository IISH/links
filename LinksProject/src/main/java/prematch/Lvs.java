package prematch;

import connectors.MySqlConnector;
import general.Functions;
import general.PrintLogger;

import javax.swing.*;
import java.io.FileWriter;
import java.io.PrintStream;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;

/**
 * @author Omar Azouguagh
 * @author Fons Laan
 *
 * The current version of creating the ls tables is: asymmetric (i.e. half-sized, also containing distance = 0.
 *
 * The Levenshtein tables are used for the matching phase.
 * For a given input string, its levenshtein variants are extracted from the ls table for matching.
 *
 * Filling the ls tables can be done in 2 ways: asymmetric, or symmetric. The symmetric ones have double the
 * number records.
 * The table are filled by a nested double loop over the names of a frequency table:
 * outer loop: i = 0...N
 * inner loop: j = 0...N    symmetric
 * inner loop: j = i...N    asymmetric
 *
 * Getting all variants from a symmetric tables is trivial, e.g.:
 * SELECT name_str_2 AS name_str, value FROM links_prematch.ls_familyname WHERE name_str_1 = 'elfrink';
 *
 * Applying the same query to an asymmetric table give you only (on average) half the number of available variants.
 * For getting all variants you can do:
 *
 * ( SELECT name_str_2 AS name_str, value FROM links_prematch.ls_familyname WHERE name_str_1 = 'elfrink' )
 *   UNION ALL
 * ( SELECT name_str_1 AS name_str, value FROM links_prematch.ls_familyname WHERE name_str_2 = 'elfrink' AND value <> 0 )
 * ORDER BY name_str;
 *
 * the "value <> 0" is needed if you are creating a table that also contains distance = 0 for the strings.
 *
 * FL-24-May-2016 Copied from Lv.java; goal: direct computation of the variants: normal, strict, first.
 * FL-13-Jul-2016 Latest change
 */
public class Lvs extends Thread
{
    private boolean debug = false;

    private MySqlConnector db_conn = null;
    private String db_name;
    private String freq_table;
    private String lvs_type;
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
     * @param freq_table
     * @param lvs_type
     * @param also_exact_matches
     * @param outputLine
     * @param outputArea
     * @param plog
     */
    public Lvs
    (
            boolean debug,
            MySqlConnector db_conn,
            String db_name,
            String freq_table,
            String lvs_type,
            boolean also_exact_matches,
            JTextField outputLine,
            JTextArea outputArea,
            PrintLogger plog
    )
    {
        this.debug              = debug;
        this.db_conn            = db_conn;
        this.db_name            = db_name;                  // e.g. links_prematch
        this.freq_table         = freq_table;               // e.g. freq_firstname or freq_familyname
        this.lvs_type           = lvs_type;                 // "normal", "strict", "first"
        this.also_exact_matches = also_exact_matches;       // i.e. lvs distance = 0
        this.outputLine         = outputLine;
        this.outputArea         = outputArea;
        this.plog               = plog;
    }


    /**
     *
     */
    @Override
    public void run()
    {
        // If you want the actual CPU time of the current thread (or indeed, any arbitrary thread) rather than
        // the wall clock time then you can get this via ThreadMXBean. Basically, do this at the start:
        ThreadMXBean threadMXB = ManagementFactory.getThreadMXBean();
        threadMXB.setThreadCpuTimeEnabled( true );

        long start = System.currentTimeMillis();
        long threadId = Thread.currentThread().getId();

        String msg = String.format( "thread (id %d); Lvs/run() using freq table: %s, lvs type: %s", threadId, freq_table, lvs_type );
        showMessage( msg, false, true );

        msg = String.format( "thread (id %d); Lvs/run() Including exact matches: %s", threadId, also_exact_matches );
        showMessage( msg, false, true );

        String csv_name = "LS-" + freq_table + "-type=" + lvs_type + ".csv";
        msg = String.format( "thread (id %d); Lvs/run() Output filename: %s", threadId, csv_name );
        showMessage( msg, false, true );

        String lvs_table = "";


        if( lvs_type.equals( "strict" ) )
        {
                 if( freq_table.equals( "freq_firstname"  ) ) { lvs_table = "ls_firstname_strict"; }
            else if( freq_table.equals( "freq_familyname" ) ) { lvs_table = "ls_familyname_strict"; }
            else {
                msg = String.format( "thread (id %d); Lvs/run() Error: freq table name: %s ?", threadId, freq_table );
                showMessage( msg, false, true );
                return;
            }
            do_lvs_strict( debug, threadId, db_conn, db_name, freq_table, csv_name );    // create csv file
        }

        else if( lvs_type.equals( "normal" ) )
        {
                 if( freq_table.equals( "freq_firstname"  ) ) { lvs_table = "ls_firstname"; }
            else if( freq_table.equals( "freq_familyname" ) ) { lvs_table = "ls_familyname"; }
            else {
                msg = String.format( "thread (id %d); Lvs/run() Error: freq table name: %s ?", threadId, freq_table );
                showMessage( msg, false, true );
                return;
            }
            do_lvs_normal( debug, threadId, db_conn, db_name, freq_table, csv_name );    // create csv file
        }

        else if( lvs_type.equals( "first"  ) )
        {
                 if( freq_table.equals( "freq_firstname"  ) ) { lvs_table = "ls_firstname_first"; }
            else if( freq_table.equals( "freq_familyname" ) ) { lvs_table = "ls_familyname_first"; }
            else {
                msg = String.format( "thread (id %d); Lvs/run() Error: freq table name: %s ?", threadId, freq_table );
                showMessage( msg, false, true );
                return;
            }
            do_lvs_first( debug, threadId, db_conn, db_name, freq_table, csv_name );    // create csv file
        }

        else {
            msg = String.format( "thread (id %d); Lvs/run() Error: lvs type: %s ?", threadId, lvs_type );
            showMessage( msg, false, true );
            return;
        }

        msg = String.format( "thread (id %d); Lvs/run() Creating lvs table name: %s", threadId, lvs_table );
        showMessage( msg, false, true );

        /*
        The old Levenshtein table creation used 4 threads, 2 for firstname, 2 for familyname;
        1. firstname    strict = true   tables: ls_firstname_strict
        2. firstname    strict = false  tables: ls_firstname and ls_firstname_first
        3. familyname   strict = true   tables: ls_familyname_strict
        4. familyname   strict = false  tables: ls_familyname and ls_familyname_first

        apart from the first-/family distinctions, it splits in 2 methods:
        a. _first starts with copy from _normal
           _first requires the first character to be the same for input & lvs variant: all others were deleted from the lvs_table.
           _normal only needs lvs=0,1,2; 3&4 were deleted from the normal lvs_table.
        b. _strict only needs lvs=0,1,2; 3&4 were deleted from the strict lvs_table.

        so the new methods with 6 threads will be:
        _strict: strict method, lvs=0,1,2
        _normal: normal method, lvs=0,1,2
        _first:  normal method, lvs=0,1,2,3,4; check for first char = identical
        */

        // csv file has been created above; copy csv file -> lvs table
        try { loadCsvLsToTable( debug, db_conn, db_name, csv_name, lvs_table ); }
        catch( Exception ex ) { showMessage( "Lvs/run() Error: " + ex.getMessage(), false, true ); }

        // removing csv file
        msg = String.format( "thread (id %d); Lvs/run() Removing file %s", threadId, csv_name );
        showMessage( msg + "...", false, true );

        java.io.File file = new java.io.File( csv_name );
        file.delete();

        msg = String.format( "thread (id %d); Lvs/run() Finished.", threadId );
        elapsedShowMessage( msg, start, System.currentTimeMillis() );

        long cpuTimeNsec  = threadMXB.getCurrentThreadCpuTime();   // elapsed CPU time for current thread in nanoseconds
        long cpuTimeMsec  = TimeUnit.NANOSECONDS.toMillis( cpuTimeNsec );

        msg = String.format( "thread (id %d); Lvs/run() Thread time", threadId );
        elapsedShowMessage( msg, 0, cpuTimeMsec );

        showMessage_nl();
    } // run



    public void do_lvs_strict( boolean debug, long threadId, MySqlConnector db_conn, String db_name, String freq_table, String csv_name )
    {
        // strict: strict method, lvsd=0,1,2
        int lvsd_max = 2;

        int n_csv_line = 0;
        try
        {
            String query = "SELECT id, name_str FROM " + db_name + "." + freq_table;
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

            ArrayList< Integer > id      = new ArrayList< Integer >();
            ArrayList< String > name_str = new ArrayList< String >();

            while( rs.next() )
            {
                id.add( rs.getInt( "id" ) );
                name_str.add( rs.getString( "name_str" ) );
            }

            int size = id.size();
            //showMessage( "table " + db_table + " loaded, records: " + size, false, true );
            String msg = String.format( "thread (id %d); do_lvs_strict() Table %s loaded, records: %d", threadId, freq_table, size );
            showMessage( msg, false, true );

            FileWriter csvwriter = new FileWriter( csv_name );

            // timing
            long timeExpand = 0;
            long begintime = System.currentTimeMillis();

            int step = 1000;
            int stepheight = step;

            // process all names
            for( int i = 0; i < size; i++ )
            {
                String name_str_1 = name_str.get( i );
                int name_int_1 = id.get( i );

                int begin = i;                                  // starting at i: also gives Levenshtein 0 values
                if( ! also_exact_matches ) { begin++; }         // this prevents names being identical, i.e. Levenshtein value > 0

                //for( int j = begin; j < size; j++ )  // "asymmetric"; contents of _1 & _2 columns not identical
                for( int j = 0; j < size; j++ )      // "symmetric" ls_tables; doubles the number of records
                {
                    String name_str_2 = name_str.get( j );
                    int name_int_2 = id.get( j );

                    int len_1 = name_str_1.length();
                    int len_2 = name_str_2.length();

                    int len_smallest = (len_1 < len_2) ?  len_1 : len_2;

                    int len_small;
                    int len_great;

                    if( len_1 == len_smallest ) {
                        len_small = len_1;
                        len_great = len_2;
                    }
                    else {
                        len_small = len_2;
                        len_great = len_1;
                    }

                    int len_diff = len_great - len_small;


                    // the length difference imposes a lower bound on the levenshtein distance,
                    // which is used to discard name pairs is the distance is too big.
                    if( len_diff > lvsd_max ) { continue; }             // implies ld > 4

                    // lvs_type.equals( "strict" )
                    if( len_small ==  1                    && len_diff > 0 ) { continue; }

                    if( len_small >=  2 && len_small <=  5 && len_diff > 1 ) { continue; }

                    if( len_small >=  6 && len_small <=  8 && len_diff > 2 ) { continue; }

                    if( len_small >=  9 && len_small <= 11 && len_diff > 3 ) { continue; }

                    if( len_small >= 12                    && len_diff > 4 ) { continue; }

                    // in order to discard additional name pairs we use the actual levenshtein distance
                    int ld = levenshtein( name_str_1, name_str_2 );     // levenshtein distance

                    if( ld > lvsd_max ) { continue; }                   // distance too high

                    // lvs_type.equals( "strict" )
                    if( len_small ==  1                    && ld > 0 ) { continue; }

                    if( len_small >=  2 && len_small <=  5 && ld > 1 ) { continue; }

                    if( len_small >=  6 && len_small <=  8 && ld > 2 ) { continue; }

                    if( len_small >=  9 && len_small <= 11 && ld > 3 ) { continue; }

                    if( len_small >= 12                    && ld > 4 ) { continue; }


                    // Write to CSV
                    String line = name_str_1 + "," + name_str_2 + "," + len_1 + ","+ len_2 + "," + name_int_1 + "," + name_int_2 + "," + ld + "\r\n";
                    //if( debug ) { System.out.println( line ); }

                    n_csv_line ++;

                    try {  csvwriter.write( line ); }
                    catch( Exception ex ) {
                        msg = String.format( "thread (id %d); do_lvs_strict() Error: %s", threadId, ex.getMessage() );
                        showMessage( msg, false, true );
                    }
                }

                // show progress
                if( i == stepheight ) {
                    msg = String.format( "thread (id %d); do_lvs_strict() LV name %d-of-%d", threadId, i, size );
                    showMessage( msg, true, true );
                    stepheight += step;
                }
            }

            showMessage( "", true, true );      // clear
            csvwriter.close();
            if( debug ) { showMessage( "do_lvs_strict()" + n_csv_line + " Records written to CSV file", false, true ); }

            // elapsed
            timeExpand = System.currentTimeMillis() - begintime;
            int iTimeEx = (int)( timeExpand / 1000 );
        }
        catch( Exception ex ) { showMessage( "do_lvs_strict() Error: " + ex.getMessage(), false, true ); }

    } // do_lvs_strict


    public void do_lvs_normal( boolean debug, long threadId, MySqlConnector db_conn, String db_name, String freq_table, String csv_name  )
    {
        // normal: normal method, lvsd=0,1,2
        int lvsd_max = 2;

        int n_csv_line = 0;
        try
        {
            String query = "SELECT id, name_str FROM " + db_name + "." + freq_table;
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

            ArrayList< Integer > id      = new ArrayList< Integer >();
            ArrayList< String > name_str = new ArrayList< String >();

            while( rs.next() )
            {
                id.add( rs.getInt( "id" ) );
                name_str.add( rs.getString( "name_str" ) );
            }

            int size = id.size();
            //showMessage( "table " + db_table + " loaded, records: " + size, false, true );
            String msg = String.format( "thread (id %d); do_lvs_normal() Table %s loaded, records: %d", threadId, freq_table, size );
            showMessage( msg, false, true );

            FileWriter csvwriter = new FileWriter( csv_name );

            // timing
            long timeExpand = 0;
            long begintime = System.currentTimeMillis();

            int step = 1000;
            int stepheight = step;

            // process all names
            for( int i = 0; i < size; i++ )
            {
                String name_str_1 = name_str.get( i );
                int name_int_1 = id.get( i );

                int begin = i;                              // starting at i: also gives Levenshtein 0 values
                if( ! also_exact_matches ) { begin++; }     // this prevents names being identical, i.e. Levenshtein value > 0

                //for( int j = begin; j < size; j++ )  // "asymmetric"; contents of _1 & _2 columns not identical
                for( int j = 0; j < size; j++ )      // "symmetric" ls_tables; doubles the number of records
                {
                    String name_str_2 = name_str.get( j );
                    int name_int_2 = id.get( j );

                    int len_1 = name_str_1.length();
                    int len_2 = name_str_2.length();

                    int len_smallest = (len_1 < len_2) ?  len_1 : len_2;

                    int len_small;
                    int len_great;

                    if( len_1 == len_smallest ) {
                        len_small = len_1;
                        len_great = len_2;
                    }
                    else {
                        len_small = len_2;
                        len_great = len_1;
                    }

                    int len_diff = len_great - len_small;


                    // the length difference imposes a lower bound on the levenshtein distance,
                    // which is used to discard name pairs is the distance is too big.
                    if( len_diff > lvsd_max ) { continue; }             // implies ld > 4

                    // "normal" & "first"
                    if( len_small == 1                   && len_diff > 0 ) { continue; }

                    if( len_small >= 2 && len_small <= 4 && len_diff > 1 ) { continue; }

                    if( len_small >= 5 && len_small <= 7 && len_diff > 2 ) { continue; }

                    if( len_small == 8                   && len_diff > 3 ) { continue; }

                    if( len_small >= 9                   && len_diff > 4 ) { continue; }

                    // in order to discard additional name pairs we use the actual levenshtein distance
                    int ld = levenshtein( name_str_1, name_str_2 );     // levenshtein distance

                    if( ld > lvsd_max ) { continue; }                   // distance too high

                    // "normal" & "first"
                    if( len_small == 1                   && ld > 0 ) { continue; }

                    if( len_small >= 2 && len_small <= 4 && ld > 1 ) { continue; }

                    if( len_small >= 5 && len_small <= 7 && ld > 2 ) { continue; }

                    if( len_small == 8                   && ld > 3 ) { continue; }

                    if( len_small >= 9                   && ld > 4 ) { continue; }


                    // Write to CSV
                    String line = name_str_1 + "," + name_str_2 + "," + len_1 + ","+ len_2 + "," + name_int_1 + "," + name_int_2 + "," + ld + "\r\n";
                    //if( debug ) { System.out.println( line ); }

                    n_csv_line ++;

                    try {  csvwriter.write( line ); }
                    catch( Exception ex ) {
                        msg = String.format( "thread (id %d); do_lvs_normal() Error: %s", threadId, ex.getMessage() );
                        showMessage( msg, false, true );
                    }
                }

                // show progress
                if( i == stepheight ) {
                    msg = String.format( "thread (id %d); do_lvs_normal() LV name %d-of-%d", threadId, i, size );
                    showMessage( msg, true, true );
                    stepheight += step;
                }
            }

            showMessage( "", true, true );      // clear
            csvwriter.close();
            if( debug ) { showMessage( "do_lvs_normal()" + n_csv_line + " Records written to CSV file", false, true ); }

            // elapsed
            timeExpand = System.currentTimeMillis() - begintime;
            int iTimeEx = (int)( timeExpand / 1000 );
        }
        catch( Exception ex ) { showMessage( "do_lvs_normal() Error: " + ex.getMessage(), false, true ); }

    } // do_lvs_normal


    public void do_lvs_first( boolean debug, long threadId, MySqlConnector db_conn, String db_name, String freq_table, String csv_name  )
    {
        // first: normal method, lvs=0,1,2,3,4; check for first char = identical
        int lvsd_max = 4;

        int n_csv_line = 0;
        try
        {
            String query = "SELECT id, name_str FROM " + db_name + "." + freq_table;
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

            ArrayList< Integer > id      = new ArrayList< Integer >();
            ArrayList< String > name_str = new ArrayList< String >();

            while( rs.next() )
            {
                id.add( rs.getInt( "id" ) );
                name_str.add( rs.getString( "name_str" ) );
            }

            int size = id.size();
            //showMessage( "table " + db_table + " loaded, records: " + size, false, true );
            String msg = String.format( "thread (id %d); do_lvs_first() Table %s loaded, records: %d", threadId, freq_table, size );
            showMessage( msg, false, true );

            FileWriter csvwriter = new FileWriter( csv_name );

            // timing
            long timeExpand = 0;
            long begintime = System.currentTimeMillis();

            int step = 1000;
            int stepheight = step;

            // process all names
            for( int i = 0; i < size; i++ )
            {
                String name_str_1 = name_str.get( i );
                int name_int_1 = id.get( i );

                int begin = i;                              // starting at i: also gives Levenshtein 0 values
                if( ! also_exact_matches ) { begin++; }     // this prevents names being identical, i.e. Levenshtein value > 0

                //for( int j = begin; j < size; j++ )  // "asymmetric"; contents of _1 & _2 columns not identical
                for( int j = 0; j < size; j++ )      // "symmetric" ls_tables; doubles the number of records
                {
                    String name_str_2 = name_str.get( j );
                    int name_int_2 = id.get( j );

                    // require that first characters are identical
                    if( name_str_1.charAt( 0 ) != name_str_2.charAt( 0 ) ) { continue; }

                    int len_1 = name_str_1.length();
                    int len_2 = name_str_2.length();

                    int len_smallest = (len_1 < len_2) ?  len_1 : len_2;

                    int len_small;
                    int len_great;

                    if( len_1 == len_smallest ) {
                        len_small = len_1;
                        len_great = len_2;
                    }
                    else {
                        len_small = len_2;
                        len_great = len_1;
                    }

                    int len_diff = len_great - len_small;


                    // the length difference imposes a lower bound on the levenshtein distance,
                    // which is used to discard name pairs is the distance is too big.
                    if( len_diff > lvsd_max ) { continue; }             // implies ld > 4

                    // "normal" & "first"
                    if( len_small == 1                   && len_diff > 0 ) { continue; }

                    if( len_small >= 2 && len_small <= 4 && len_diff > 1 ) { continue; }

                    if( len_small >= 5 && len_small <= 7 && len_diff > 2 ) { continue; }

                    if( len_small == 8                   && len_diff > 3 ) { continue; }

                    if( len_small >= 9                   && len_diff > 4 ) { continue; }

                    // in order to discard additional name pairs we use the actual levenshtein distance
                    int ld = levenshtein( name_str_1, name_str_2 );     // levenshtein distance

                    if( ld > lvsd_max ) { continue; }                   // distance too high

                    // "normal" & "first"
                    if( len_small == 1                   && ld > 0 ) { continue; }

                    if( len_small >= 2 && len_small <= 4 && ld > 1 ) { continue; }

                    if( len_small >= 5 && len_small <= 7 && ld > 2 ) { continue; }

                    if( len_small == 8                   && ld > 3 ) { continue; }

                    if( len_small >= 9                   && ld > 4 ) { continue; }


                    // Write to CSV
                    String line = name_str_1 + "," + name_str_2 + "," + len_1 + ","+ len_2 + "," + name_int_1 + "," + name_int_2 + "," + ld + "\r\n";
                    //if( debug ) { System.out.println( line ); }

                    n_csv_line ++;

                    try {  csvwriter.write( line ); }
                    catch( Exception ex ) {
                        msg = String.format( "thread (id %d); do_lvs_first() Error: %s", threadId, ex.getMessage() );
                        showMessage( msg, false, true );
                    }
                }

                // show progress
                if( i == stepheight ) {
                    msg = String.format( "thread (id %d); do_lvs_first() LV name %d-of-%d", threadId, i, size );
                    showMessage( msg, true, true );
                    stepheight += step;
                }
            }

            showMessage( "", true, true );      // clear
            csvwriter.close();
            if( debug ) { showMessage( "do_lvs_first()" + n_csv_line + " records written to CSV file", false, true ); }

            // elapsed
            timeExpand = System.currentTimeMillis() - begintime;
            int iTimeEx = (int)( timeExpand / 1000 );
        }
        catch( Exception ex ) { showMessage( "do_lvs_first() Error: " + ex.getMessage(), false, true ); }

    } // do_lvs_first



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
    private void loadCsvLsToTable( boolean debug, MySqlConnector db_conn, String db_name, String csv_name, String lvs_table )
    throws Exception
    {
        long threadId = Thread.currentThread().getId();

        long start = System.currentTimeMillis();
        String msg = String.format( "thread (id %d); LoadCsvLsToTable(): Loading CSV LV data into LV table: %s", threadId, lvs_table );
        showMessage( msg + "...", false, true );

        String query = "TRUNCATE TABLE `" + db_name + "`.`" + lvs_table + "`";
        if( debug ) { showMessage( query, false, true ); }
        db_conn.runQuery( query );

        query = "LOAD DATA LOCAL INFILE '" + csv_name + "'"
            + " INTO TABLE `" + db_name + "`.`" + lvs_table + "`"
            + " FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n'"
            + " ( `name_str_1` , `name_str_2`, `length_1`, `length_2`, `name_int_1` , `name_int_2`, `value` );";

        if( debug ) { showMessage( query, false, true ); }
        db_conn.runQuery( query );

        elapsedShowMessage( msg, start, System.currentTimeMillis() );
    } // loadCsvLsToTable


    /**
     * @param debug
     * @param db_conn
     * @param db_name
     * @param ls_table
     * @throws Exception
     *
     * Create the _first variant from the normal ls table,
     * so skip if the _strict variant is provided.
     * After the _first variant is created, delete 3 & 4 distance entries from the input table
     */
    private void createLsFirstTable( boolean debug, MySqlConnector db_conn, String db_name, String ls_table )
    throws Exception
    {
        long threadId = Thread.currentThread().getId();

        // the _first variants are only created from the normal ls tables, not from the _strict ones.
        if( ls_table.equals( "ls_firstname" ) || ls_table.equals( "ls_familyname" ) )
        {
            // copy the table to '_first', and delete records that differ in 1st char of names
            String ls_table_first = ls_table + "_first";

            long start = System.currentTimeMillis();
            String msg = String.format( "thread (id %d); createLsFirstTable(): Filling table: %s", threadId, ls_table_first );
            showMessage( msg, false, true );

            String[] queries =
            {
                "DROP TABLE IF EXISTS " + ls_table_first,
                "CREATE TABLE " + ls_table_first + " LIKE " + ls_table + ";",
                "ALTER TABLE "  + ls_table_first + " DISABLE KEYS;",
                "INSERT INTO "  + ls_table_first + " SELECT * FROM " + ls_table + ";",
                "ALTER TABLE "  + ls_table_first + " ENABLE KEYS;",
                "DELETE FROM "  + ls_table_first + " WHERE SUBSTRING( `name_str_1`,1,1 ) <> SUBSTRING( `name_str_2`,1,1 );",
                "DELETE FROM "  + ls_table + " WHERE value = 3 OR value = 4;"
            };

            for( String query : queries )
            {
                msg = String.format( "thread (id %d); %s", query );
                showMessage( msg, false, true );
                db_conn.runQuery( query );
            }

            String query = "SELECT COUNT(*) AS count FROM " + ls_table_first;
            try {
                ResultSet rs = db_conn.runQueryWithResult( query );
                while( rs.next() ) {
                    int count = rs.getInt( "count" );
                    msg = String.format( "thread (id %d); %d records in %s", threadId, count, ls_table_first );
                    showMessage( msg, false , true );
                }
            }
            catch( Exception ex ) { showMessage( ex.getMessage(), false, true ); }
        }

        else
        {
            // ls_table is ls_firstname_strict or ls_familyname_strict: remove lvs = 3 & 4
            String msg = String.format( "thread (id %d); createLsFirstTable(): Deleting Lvs 3 and 4 from table: %s", threadId, ls_table );
            showMessage(msg, false, true );

            String query = "DELETE FROM " + ls_table + " WHERE value = 3 OR value = 4;";
            db_conn.runQuery( query );
        }

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