package prematch;

import java.sql.*;
import javax.swing.JTextArea;
import javax.swing.JTextField;
import java.util.ArrayList;
import modulemain.LinksSpecific;
import connectors.*;

/**
 *
 * @author oaz
 */
public class VariantLs {

    private JTextArea taInfo;
    private JTextField tbOutput;
    private String table;

    /**
     *
     * @param taInfo
     * @param tbOutput
     */
    public VariantLs(JTextArea taInfo, JTextField tbOutput, String table) {
        this.taInfo = taInfo;
        this.tbOutput = tbOutput;
        this.table = table;

    }

    /**
     *
     */
    public void computeVariants() {

        // Run query on Database
        try {

            // ls function


            MySqlConnector con = new MySqlConnector("hebe", "links_frequency", "linksbeta", "betalinks");

            ResultSet rsA = con.runQueryWithResult("SELECT id, name_2 FROM " + table + "_a");
            ResultSet rsB = con.runQueryWithResult("SELECT name_1, name_2, first_char FROM " + table + "_b ORDER BY first_char ASC, frequency DESC");

            // move rsB to arrayLists because of speed
            ArrayList<String> alBname_1 = new ArrayList<String>();
            ArrayList<String> alBname_2 = new ArrayList<String>();
            ArrayList<String> alBfirst_char = new ArrayList<String>();


            while (rsB.next()) {
                alBname_1.add(rsB.getString("name_1"));
                alBname_2.add(rsB.getString("name_2"));
                alBfirst_char.add(rsB.getString("first_char"));
            }

            // Get begin and end positions
            int firstA = alBfirst_char.indexOf("a");
            int firstB = alBfirst_char.indexOf("b");
            int firstD = alBfirst_char.indexOf("d");
            int firstE = alBfirst_char.indexOf("e");
            int firstF = alBfirst_char.indexOf("f");
            int firstG = alBfirst_char.indexOf("g");
            int firstH = alBfirst_char.indexOf("h");
            int firstK = alBfirst_char.indexOf("k");
            int firstL = alBfirst_char.indexOf("l");
            int firstM = alBfirst_char.indexOf("m");
            int firstN = alBfirst_char.indexOf("n");
            int firstO = alBfirst_char.indexOf("o");
            int firstP = alBfirst_char.indexOf("p");
            int firstQ = alBfirst_char.indexOf("q");
            int firstR = alBfirst_char.indexOf("r");
            int firstT = alBfirst_char.indexOf("t");
            int firstU = alBfirst_char.indexOf("u");
            int firstV = alBfirst_char.indexOf("v");
            int firstW = alBfirst_char.indexOf("w");
            int firstX = alBfirst_char.indexOf("x");
            int firstY = alBfirst_char.indexOf("y");
            int firstZ = alBfirst_char.indexOf("z");

            int counter = 0;

            // loop through A
            while (rsA.next()) {

                // counter
                counter++;
                tbOutput.setText("Name nr: " + counter + " of TOTAL // GET TOTAL");

                //get id and name
                int id = rsA.getInt("id");
                String nameA2 = rsA.getString("name_2");

                // for every name in a loop through b

                // first find index

                // get substring
                char nameA2FirstLetter = nameA2.substring(0, 1).toCharArray()[0];

                int beginIndex = 0;
                int endIndex = alBfirst_char.size();

                // set first and last occurence
                switch (nameA2FirstLetter) {
                    case 'a':
                        beginIndex = firstA;
                        endIndex = firstB;
                        break;
                    case 'b':
                        beginIndex = firstB;
                        endIndex = firstD;
                        break;
                    case 'd':
                        beginIndex = firstD;
                        endIndex = firstE;
                        break;
                    case 'e':
                        beginIndex = firstE;
                        endIndex = firstF;
                        break;
                    case 'f':
                        beginIndex = firstF;
                        endIndex = firstG;
                        break;
                    case 'g':
                        beginIndex = firstG;
                        endIndex = firstH;
                        break;
                    case 'h':
                        beginIndex = firstH;
                        endIndex = firstK;
                        break;
                    case 'k':
                        beginIndex = firstK;
                        endIndex = firstL;
                        break;
                    case 'l':
                        beginIndex = firstL;
                        endIndex = firstM;
                        break;
                    case 'm':
                        beginIndex = firstM;
                        endIndex = firstN;
                        break;
                    case 'n':
                        beginIndex = firstN;
                        endIndex = firstO;
                        break;
                    case 'o':
                        beginIndex = firstO;
                        endIndex = firstP;
                        break;
                    case 'p':
                        beginIndex = firstP;
                        endIndex = firstQ;
                        break;
                    case 'q':
                        beginIndex = firstQ;
                        endIndex = firstR;
                        break;
                    case 'r':
                        beginIndex = firstR;
                        endIndex = firstT;
                        break;
                    case 't':
                        beginIndex = firstT;
                        endIndex = firstU;
                        break;
                    case 'u':
                        beginIndex = firstU;
                        endIndex = firstV;
                        break;
                    case 'v':
                        beginIndex = firstV;
                        endIndex = firstW;
                        break;
                    case 'w':
                        beginIndex = firstW;
                        endIndex = firstX;
                        break;
                    case 'x':
                        beginIndex = firstX;
                        endIndex = firstY;
                        break;
                    case 'y':
                        beginIndex = firstY;
                        endIndex = firstZ;
                        break;
                    case 'z':
                        beginIndex = firstZ;
                        endIndex = alBfirst_char.size();
                        break;
                }

                // loop through B
                for (int i = beginIndex; i < endIndex ; i++ ) {  
 
                    String nameB1 = alBname_1.get(i);
                    String nameB2 = alBname_2.get(i);
                    String firstChar = alBfirst_char.get(i);

                    // get size difference
                    int length = nameA2.length() - nameB2.length();

                    // make it absolute/positive
                    length = (length < 0 ? -length : length);

                    // check length and evt. continue
                    if (length > 1) {
                        continue;
                    }

                    // length difference is 0 or 1, lv 0 or 1 is possible
                    int ld = levenshtein(nameA2, nameB2);

                    // check levensthein
                    if (ld > 1) {
                        continue;
                    }

                    // set highest frequency
                    con.runQuery("UPDATE " + table + "_a SET name_b = '" + LinksSpecific.funcPrepareForMysql(nameB1) + "' WHERE id = " + id);

                    break;

                }
            }
            
            rsA.close();
            rsA = null;
            
            rsB.close();
            rsB = null;

        } catch (Exception e) {
            taInfo.setText(e.getMessage());
        }

    }

    /**
     *
     * @param s
     * @param t
     * @return
     */
    private static int levenshtein(String s, String t) {

        int n = s.length(); // length of s
        int m = t.length(); // length of t

        int p[] = new int[n + 1]; //'previous' cost array, horizontally
        int d[] = new int[n + 1]; // cost array, horizontally
        int _d[]; //placeholder to assist in swapping p and d

        // indexes into strings s and t
        int i; // iterates through s
        int j; // iterates through t

        char t_j; // jth character of t

        int cost; // cost

        for (i = 0; i <= n; i++) {
            p[i] = i;
        }

        for (j = 1; j <= m; j++) {
            t_j = t.charAt(j - 1);
            d[0] = j;

            for (i = 1; i <= n; i++) {
                cost = s.charAt(i - 1) == t_j ? 0 : 1;
                d[i] = Math.min(Math.min(d[i - 1] + 1, p[i] + 1), p[i - 1] + cost);
            }

            _d = p;
            p = d;
            d = _d;
        }

        return p[n];
    }

    private static String stopWatch(int seconds) {
        int minutes = seconds / 60;
        int restsec = seconds % 60;
        int uren = minutes / 60;
        int restmin = minutes % 60;

        String urenText = "";
        String minutenText = "";
        String secondenText = "";

        if (uren < 10) {
            urenText = "0";
        }
        if (restmin < 10) {
            minutenText = "0";
        }
        if (restsec < 10) {
            secondenText = "0";
        }

        return urenText + uren + ":" + minutenText + restmin + ":" + secondenText + restsec;
    }
}