package prematch;

import java.io.FileWriter;
import java.io.PrintStream;

import java.sql.ResultSet;
import java.util.ArrayList;

import javax.swing.JTextArea;
import javax.swing.JTextField;

import connectors.MySqlConnector;
import general.PrintLogger;

/**
 * @author Omar Azouguagh
 * @author Fons Laan
 *
 * <p/>
 * FL-30-Jun-2014 Imported from OA backup
 * FL-24-Nov-2014 Latest change
 */
public class Lv extends Thread
{
    private boolean debug = false;

    private MySqlConnector db_conn = null;
    private String db_name;
    private String db_table;
    private boolean strict;

    private JTextField outputLine;
    private JTextArea outputArea;
    private PrintLogger plog;

    /**
     * Constructor
     *
     * @param debug
     * @param db_conn
     * @param db_name
     * @param db_table
     * @param strict
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
        JTextField outputLine,
        JTextArea outputArea,
        PrintLogger plog
    )
    {
        this.debug      = debug;
        this.db_conn    = db_conn;
        this.db_name    = db_name;
        this.db_table   = db_table;
        this.strict     = strict;
        this.outputLine = outputLine;
        this.outputArea = outputArea;
        this.plog       = plog;
    }


    /**
     *
     */
    @Override
    public void run()
    {
        // Run query on Database
        try
        {

            showMessage( "prematch.Lv/run() for table " + db_table + " with strict = " + strict, false, true );

            ResultSet rs = null;
            //String query = "SELECT id, name FROM " + db_table;
            String query = "SELECT id, name FROM " + db_name + "." + db_table;
            if( debug ) { showMessage( query, false, true ); }

            try {
                rs = db_conn.runQueryWithResult( query );
            }
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

            String filename = "LV-" + db_table + "-strict=" + strict + ".csv";
            showMessage( "Output filename: " + filename, false, true );
            FileWriter csvwriter = new FileWriter( filename );

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

                String naam1 = name.get(i);
                String naam2 = "";

                int begin = i+1;

                for( int j = begin; j < name.size() ; j++ )
                {
                    id2   = id.get( j );
                    naam2 = name.get(j);

                    int a = naam1.length();
                    int b = naam2.length();
                    
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

                        if( basic <  9 && diff > 2 ) { continue; }           // 3 4 5

                        if( basic < 12 && diff > 3 ) { continue; }
                    }
                    else
                    {
                        if( basic < 3 && diff > 1 ) { continue; }

                        if( basic < 6 && diff > 2 ) { continue; }           // 3 4 5

                        if( basic < 9 && diff > 3 ) { continue; }
                    }

                    ld = levenshtein( naam1, naam2 );           // levenshtein

                    if( ld > 4 ) { continue; }                  // check distance

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
                    String line = id1 + "," + id2 + "," + naam1 + "," + naam2 + "," + ld + "\r\n";

                    try {  csvwriter.write( line ); }
                    catch( Exception ex ) { showMessage( "Levenshtein Error: " + ex.getMessage(), false, true ); }
                }

                // show progress
                if( i == stepheight ){
                    showMessage( "LV name " + i + " of " + size, true, true );
                    stepheight += step;
                }
            }

            showMessage( "", true, true );
            csvwriter.close();

            // elapsed
            timeExpand = System.currentTimeMillis() - begintime;
            int iTimeEx = (int)( timeExpand / 1000 );

            showMessage( "Levenshtein " + db_table + " done, strict = " + strict + "; elapsed: " + stopWatch( iTimeEx ), false, true );

        }

        catch( Exception ex ) { showMessage( "Levenshtein Error: " + ex.getMessage(), false, true ); }
    }


    /**
     *
     * @param s
     * @param t
     * @return
     */
    public static int levenshtein(  String s, String t  )
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

}