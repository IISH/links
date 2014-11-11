package prematch;

import java.io.FileWriter;
import java.sql.ResultSet;
import java.util.ArrayList;

import javax.swing.JTextArea;
import javax.swing.JTextField;

import connectors.MySqlConnector;

/**
 * @author Omar Azouguagh
 * @author Fons Laan
 *
 * <p/>
 * FL-30-Jun-2014 Imported from OA backup
 * FL-30-Jul-2014 Latest change
 */
public class Lv extends Thread
{
    private MySqlConnector db_conn = null;
    private String db_table;
    private boolean strict;
    private JTextField tbOutput;
    private JTextArea taInfo;

    /**
     * Constructor
     *
     * @param db_conn
     * @param db_table
     * @param strict
     * @param tbOutput
     * @param taInfo
     */
    public Lv
    (
        MySqlConnector db_conn,
        String db_table,
        boolean strict,
        JTextField tbOutput,
        JTextArea taInfo
    )
    {
        this.db_conn = db_conn;
        this.db_table = db_table;
        this.strict = strict;
        this.tbOutput = tbOutput;
        this.taInfo = taInfo;
    }


    /**
     *
     */
    @Override
    public void run(){

        // Run query on Database
        try
        {
            System.out.println( "prematch.Lv.run()" );
            System.out.println( db_conn );

            ResultSet rs = null;
            String query = "SELECT id, name FROM " + db_table;
            try {
                rs = db_conn.runQueryWithResult( query );
            }
            catch( Exception ex ) {
                System.out.println( query );
                taInfo.append( query + "\r\n" );

                System.out.println( ex.getMessage() );
                taInfo.append( ex.getMessage() + "\r\n" );
                return;
            }

            ArrayList<Integer> id  = new ArrayList<Integer>();
            ArrayList<String> name = new ArrayList<String>();

            while( rs.next() ) {
                id.add( rs.getInt( "id" ) );
                name.add( rs.getString( "name" ) );
            }

            taInfo.append( db_table + "LOADED...\r\n" );
            
            int size = name.size();

            FileWriter writer = new FileWriter( strict + "_" + db_table + ".csv" );

            // Hulp variabelen
            long timeExpand = 0;
            long begintime = System.currentTimeMillis();

            //tellerstappen
            int stap = 1000;
            int staphoogte = stap;

            // loopen door alle namen heen
            for( int i = 0; i < size ; i++ ){

                int id1 = id.get(i);
                String naam1 = name.get(i);
                int id2 = 0;
                String naam2 = "";

                int begin = i+1;
                for( int j = begin; j < name.size() ; j++ ){

                    id2 = id.get(j);
                    naam2 = name.get(j);

                    int a = naam1.length();
                    int b = naam2.length();
                    
                    int smallest = (a < b) ?  a : b;

                    int ld = 4;

                    int basic;
                    int extra;

                    if(a == smallest){
                        basic = a;
                        extra = b;
                    }
                    else{
                        basic = b;
                        extra = a;
                    }

                    int verschil = extra - basic;

                    // lengte bepaling
                    if( verschil > 4 ){
                        continue;
                    }

                    if(strict){
                        if( basic < 6 && verschil > 1 ){
                            continue;
                        }

                        // 3 4 5
                        if( basic < 9 && verschil > 2 ){
                            continue;
                        }

                        if( basic < 12 && verschil > 3 ){
                            continue;
                        }
                    }
                    else{
                        if( basic < 3 && verschil > 1 ){
                            continue;
                        }

                        // 3 4 5
                        if( basic < 6 && verschil > 2 ){
                            continue;
                        }

                        if( basic < 9 && verschil > 3 ){
                            continue;
                        }
                    }

                    // levenshtein
                    ld = levenshtein( naam1, naam2 );

                    // check afstand
                    if( ld > 4 ){
                        continue;
                    }

                    if(strict){
                        if( basic < 6 && ld > 1 ){
                            continue;
                        }

                        if( basic < 9 && ld > 2 ){
                            continue;
                        }

                        if( basic < 12 && ld > 3 ){
                            continue;
                        }
                    }
                    else{
                        // levenshtein icm met lengte
                        if( basic < 3 && ld > 1 ){
                            continue;
                        }

                        if( basic < 6 && ld > 2 ){
                            continue;
                        }

                        if( basic < 10 && ld > 3 ){
                            continue;
                        }
                    }

                    // Write to table
                    String regel = id1 + "," + id2 + "," + naam1 + "," + naam2 + "," + ld + "\r\n";

                    try{
                            
                        writer.write(regel);

                    }
                    catch( Exception e ){
                        taInfo.append("ERROR: " + e.getMessage() + "...\r\n");
                    }
                }

                // gebruiker op de hoogte brengen
                if( i == staphoogte ){
                    tbOutput.setText( i + " of " + size );
                    staphoogte += stap;
                }
            }
            writer.close();

            // Tijd berekenen
            timeExpand = System.currentTimeMillis() - begintime;
            int iTimeEx = (int)(timeExpand / 1000);

            taInfo.append("DONE; TIME: " + stopWatch(iTimeEx));

        }

        catch (Exception e) {
            taInfo.append("ERROR: " + e.getMessage() + "...\r\n");
        } 

    }


    /**
     *
     * @param s
     * @param t
     * @return
     */
    public static int levenshtein(String s, String t) {

        int n = s.length(); // length of s
        int m = t.length(); // length of t

        int p[] = new int[n+1]; //'previous' cost array, horizontally
        int d[] = new int[n+1]; // cost array, horizontally
        int _d[]; //placeholder to assist in swapping p and d

        // indexes into strings s and t
        int i; // iterates through s
        int j; // iterates through t

        char t_j; // jth character of t

        int cost; // cost

        for (i = 0; i<=n; i++) {
            p[i] = i;
        }

        for (j = 1; j<=m; j++) {
            t_j = t.charAt(j-1);
            d[0] = j;

            for (i=1; i<=n; i++) {
                cost = s.charAt(i-1)==t_j ? 0 : 1;
                d[i] = Math.min(Math.min(d[i-1]+1, p[i]+1),  p[i-1]+cost);
            }

            _d = p;
            p = d;
            d = _d;
        }

        return p[n];
    }


    public static String stopWatch(int seconds){
        int minutes = seconds / 60;
        int restsec = seconds % 60;
        int uren = minutes / 60;
        int restmin = minutes % 60;

        String urenText = "";
        String minutenText = "";
        String secondenText = "";

        if(uren < 10 ) urenText = "0";
        if(restmin < 10 ) minutenText = "0";
        if(restsec < 10 ) secondenText = "0";

        return urenText + uren + ":" + minutenText + restmin + ":" + secondenText + restsec;
    }

}