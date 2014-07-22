package moduleMain;

import connectors.*;
import java.sql.*;
import javax.swing.JTextArea;
import javax.swing.JTextField;
import java.io.*;
import general.Functions;

public class LinksPrematch extends Thread {

    private boolean a;
    private boolean b;
    private boolean c;
    private boolean d;
    private boolean e;
    private JTextArea t;
    private JTextField ti;
    private MySqlConnector conCleaned;
    private MySqlConnector conPrematch;
    private MySqlConnector conTemp;
    private MySqlConnector conBase;
    private MySqlConnector conFrequency;
    private java.io.FileWriter writerFirstname;
    private boolean defaultConst = false;

    public LinksPrematch(JTextArea t, JTextField ti, boolean a, boolean b, boolean c, boolean d, boolean e)
            throws Exception {

        this.a = a;
        this.b = b;
        this.c = c;
        this.d = d;
        this.e = e;
        this.t = t;
        this.ti = ti;

        conCleaned =    new MySqlConnector("127.0.0.1", "links_cleaned",     "linksdev", "devlinks");
        conPrematch =   new MySqlConnector("127.0.0.1", "links_prematch",    "linksdev", "devlinks");
        conTemp =       new MySqlConnector("127.0.0.1", "links_temp",        "linksdev", "devlinks");
        conBase =       new MySqlConnector("127.0.0.1", "links_base",        "linksdev", "devlinks");
        conFrequency =  new MySqlConnector("127.0.0.1", "links_frequency",   "linksdev", "devlinks");
    }

    /**
     * Constructor
     * @throws Exception 
     */
    public LinksPrematch(JTextArea t, JTextField ti) throws Exception {
        conCleaned =    new MySqlConnector("127.0.0.1", "links_cleaned",     "linksdev", "devlinks");
        conPrematch =   new MySqlConnector("127.0.0.1", "links_prematch",    "linksdev", "devlinks");
       conTemp =       new MySqlConnector("127.0.0.1", "links_temp",        "linksdev", "devlinks");
        conBase =       new MySqlConnector("127.0.0.1", "links_base",        "linksdev", "devlinks");
        conFrequency =  new MySqlConnector("127.0.0.1", "links_frequency",   "linksdev", "devlinks");

        defaultConst = true;

        this.t = t;
        this.ti = ti;

    }

    /**
     *
     */
    @Override
    public void run() {
        
        t.append(Functions.now("yyyy.MM.dd G 'at' hh:mm:ss z"));

        try {
            if (a) {
                t.append("Splitting names...");
                {
                    doSplitName();
                }
                t.append("Splitting names...OK");
            }
            if (b) {
                t.append("Creating Unique name tables...");
                {
                    doUniqueNameTables();
                }
                t.append("Creating Unique name tables...OK");
            }
            if (c) {
                t.append("Computing Levenshtein...");
                {
                    doLevenshtein();
                }
                t.append("Computing Levenshtein...OK");
            }
            if (d) {
                t.append("Converting Names to Numbers...");
                {
                    doToNumber();
                }
                t.append("Converting Names to Numbers...OK");
            }
            if (e) {
                t.append("Creating Base Table...");
                {
                    doCreateBaseTable();
                }
                t.append("Creating Base Table...OK");
            }
            
            t.append(Functions.now("yyyy.MM.dd G 'at' hh:mm:ss z"));

            this.stop();

        } catch (Exception e) {
            t.append(e.getMessage());
        }

    }

    /**
     * 
     * @throws Exception 
     */
    public void doSplitName() throws Exception {

        String query = "SELECT id_person , firstname FROM person_c WHERE firstname is not null AND firstname <> ''";

        ResultSet rsFirstName = conCleaned.runQueryWithResult(query);

        createTempFirstname();
        createTempFirstnameFile();

        while (rsFirstName.next()) {

            int id_person = rsFirstName.getInt("id_person");
            String firstname = rsFirstName.getString("firstname");

            String[] fn = firstname.split(" ", 4);

            String p0 = "";
            String p1 = "";
            String p2 = "";
            String p3 = "";

            if (fn.length > 0) {
                p0 = fn[0];

                if (fn.length > 1) {
                    p1 = fn[1];

                    if (fn.length > 2) {
                        p2 = fn[2];

                        if (fn.length > 3) {
                            p3 = fn[3];
                        }
                    }
                }
            }

            String q = id_person + "," + p0 + "," + p1 + "," + p2 + "," + p3;

            writerFirstname.write(q + "\n");
        }

        rsFirstName.close();
        rsFirstName = null;

        loadFirstnameToTable();
        updateFirstnameToPersonC();
        removeFirstnameFile();
        removeFirstnameTable();

    }

    /**
     *
     */
    public void doUniqueNameTables() throws Exception {


        if (!defaultConst) {
            t.append("Creating unique tables...");
        }

        dropTableFrequency();

        // Execute queries
        conFrequency.runQuery(LinksSpecific.getSqlQuery("FrequencyTables/FrequencyTables_q01"));
        conFrequency.runQuery(LinksSpecific.getSqlQuery("FrequencyTables/FrequencyTables_q02"));
        conFrequency.runQuery(LinksSpecific.getSqlQuery("FrequencyTables/FrequencyTables_q03"));
        conFrequency.runQuery(LinksSpecific.getSqlQuery("FrequencyTables/FrequencyTables_q04"));
        conFrequency.runQuery(LinksSpecific.getSqlQuery("FrequencyTables/FrequencyTables_q05"));
        conFrequency.runQuery(LinksSpecific.getSqlQuery("FrequencyTables/FrequencyTables_q06"));
        conFrequency.runQuery(LinksSpecific.getSqlQuery("FrequencyTables/FrequencyTables_q07"));
        conFrequency.runQuery(LinksSpecific.getSqlQuery("FrequencyTables/FrequencyTables_q08"));
        conFrequency.runQuery(LinksSpecific.getSqlQuery("FrequencyTables/FrequencyTables_q09"));
        conFrequency.runQuery(LinksSpecific.getSqlQuery("FrequencyTables/FrequencyTables_q10"));
        conFrequency.runQuery(LinksSpecific.getSqlQuery("FrequencyTables/FrequencyTables_q11"));
        conFrequency.runQuery(LinksSpecific.getSqlQuery("FrequencyTables/FrequencyTables_q12"));
        conFrequency.runQuery(LinksSpecific.getSqlQuery("FrequencyTables/FrequencyTables_q13"));
        conFrequency.runQuery(LinksSpecific.getSqlQuery("FrequencyTables/FrequencyTables_q14"));
        conFrequency.runQuery(LinksSpecific.getSqlQuery("FrequencyTables/FrequencyTables_q15"));
        conFrequency.runQuery(LinksSpecific.getSqlQuery("FrequencyTables/FrequencyTables_q16"));
        conFrequency.runQuery(LinksSpecific.getSqlQuery("FrequencyTables/FrequencyTables_q17"));
        conFrequency.runQuery(LinksSpecific.getSqlQuery("FrequencyTables/FrequencyTables_q18"));
        conFrequency.runQuery(LinksSpecific.getSqlQuery("FrequencyTables/FrequencyTables_q19"));
        conFrequency.runQuery(LinksSpecific.getSqlQuery("FrequencyTables/FrequencyTables_q20"));
        conFrequency.runQuery(LinksSpecific.getSqlQuery("FrequencyTables/FrequencyTables_q21"));
        conFrequency.runQuery(LinksSpecific.getSqlQuery("FrequencyTables/FrequencyTables_q22"));
        conFrequency.runQuery(LinksSpecific.getSqlQuery("FrequencyTables/FrequencyTables_q23"));
        conFrequency.runQuery(LinksSpecific.getSqlQuery("FrequencyTables/FrequencyTables_q24"));
        conFrequency.runQuery(LinksSpecific.getSqlQuery("FrequencyTables/FrequencyTables_q25"));
        conFrequency.runQuery(LinksSpecific.getSqlQuery("FrequencyTables/FrequencyTables_q26"));
        conFrequency.runQuery(LinksSpecific.getSqlQuery("FrequencyTables/FrequencyTables_q27"));
        conFrequency.runQuery(LinksSpecific.getSqlQuery("FrequencyTables/FrequencyTables_q28"));

        if (!defaultConst) {
            t.append("OK" + "\r\n");
        }

    }

    /**
     *
     */
    public void doUniqueNameTablesTemp() throws Exception {

        dropTableFrequency();

        // Execute queries
        conFrequency.runQuery(LinksSpecific.getSqlQuery("FrequencyTablesTemp/FrequencyTablesTemp_q01"));
        conFrequency.runQuery(LinksSpecific.getSqlQuery("FrequencyTablesTemp/FrequencyTablesTemp_q02"));
        conFrequency.runQuery(LinksSpecific.getSqlQuery("FrequencyTablesTemp/FrequencyTablesTemp_q03"));
        conFrequency.runQuery(LinksSpecific.getSqlQuery("FrequencyTablesTemp/FrequencyTablesTemp_q04"));
        conFrequency.runQuery(LinksSpecific.getSqlQuery("FrequencyTablesTemp/FrequencyTablesTemp_q05"));
        conFrequency.runQuery(LinksSpecific.getSqlQuery("FrequencyTablesTemp/FrequencyTablesTemp_q06"));
        conFrequency.runQuery(LinksSpecific.getSqlQuery("FrequencyTablesTemp/FrequencyTablesTemp_q07"));
        conFrequency.runQuery(LinksSpecific.getSqlQuery("FrequencyTablesTemp/FrequencyTablesTemp_q08"));
        conFrequency.runQuery(LinksSpecific.getSqlQuery("FrequencyTablesTemp/FrequencyTablesTemp_q09"));
        conFrequency.runQuery(LinksSpecific.getSqlQuery("FrequencyTablesTemp/FrequencyTablesTemp_q10"));
        conFrequency.runQuery(LinksSpecific.getSqlQuery("FrequencyTablesTemp/FrequencyTablesTemp_q11"));
        conFrequency.runQuery(LinksSpecific.getSqlQuery("FrequencyTablesTemp/FrequencyTablesTemp_q12"));
        conFrequency.runQuery(LinksSpecific.getSqlQuery("FrequencyTablesTemp/FrequencyTablesTemp_q13"));
        conFrequency.runQuery(LinksSpecific.getSqlQuery("FrequencyTablesTemp/FrequencyTablesTemp_q14"));
        conFrequency.runQuery(LinksSpecific.getSqlQuery("FrequencyTablesTemp/FrequencyTablesTemp_q15"));
        conFrequency.runQuery(LinksSpecific.getSqlQuery("FrequencyTablesTemp/FrequencyTablesTemp_q16"));
        conFrequency.runQuery(LinksSpecific.getSqlQuery("FrequencyTablesTemp/FrequencyTablesTemp_q17"));
        conFrequency.runQuery(LinksSpecific.getSqlQuery("FrequencyTablesTemp/FrequencyTablesTemp_q18"));


    }

    private void dropTableFrequency() throws Exception {

        String qDropFrequency = "DROP SCHEMA links_frequency ;";

        String qCreateFrequency = "CREATE SCHEMA `links_frequency` DEFAULT CHARACTER SET latin1 COLLATE latin1_general_ci ;";

        // run queries
        conFrequency.runQuery(qDropFrequency);
        conFrequency.runQuery(qCreateFrequency);
    }

    /**
     * 
     */
    public void doBasicName() throws Exception {

        // run preparing queries
        t.append("01" + "\r\n");
        String s01 = LinksSpecific.getSqlQuery("SetVariants/SetVariants_q01");
        String[] a01 = s01.split(";");

        for (int i = 0; i < a01.length; i++) {
            conFrequency.runQuery(a01[i]);
        }

        t.append("02" + "\r\n");
        String s02 = LinksSpecific.getSqlQuery("SetVariants/SetVariants_q02");
        String[] a02 = s02.split(";");

        for (int i = 0; i < a02.length; i++) {
            conFrequency.runQuery(a02[i]);
        }

        t.append("03" + "\r\n");
        String s03 = LinksSpecific.getSqlQuery("SetVariants/SetVariants_q03");
        String[] a03 = s03.split(";");

        for (int i = 0; i < a03.length; i++) {
            conFrequency.runQuery(a03[i]);
        }

        t.append("First 3 SQL statements done, beginning with LV" + "\r\n");

        // Run the variants
        prematch.VariantLs vlFam = new prematch.VariantLs(t, ti, "familyname");
        prematch.VariantLs vlFir = new prematch.VariantLs(t, ti, "firstname");

        vlFam.computeVariants();
        vlFir.computeVariants();

        t.append("LV DONE" + "\r\n");

        String s04 = LinksSpecific.getSqlQuery("SetVariants/SetVariants_q04");
        String[] a04 = s04.split(";");

        for (int i = 0; i < a04.length; i++) {
            conFrequency.runQuery(a04[i]);
        }

        t.append("04" + "\r\n");
    }

    /**
     * 
     * @throws Exception 
     */
    public void doToNumber() throws Exception {

        // Create Runtime Object
        Runtime runtime = Runtime.getRuntime();
        int exitValue = 0;

        conFrequency.runQuery(LinksSpecific.getSqlQuery("NameToNumber/NameToNumber_q01"));
        conFrequency.runQuery(LinksSpecific.getSqlQuery("NameToNumber/NameToNumber_q02"));
        conFrequency.runQuery(LinksSpecific.getSqlQuery("NameToNumber/NameToNumber_q03"));
        conFrequency.runQuery(LinksSpecific.getSqlQuery("NameToNumber/NameToNumber_q04"));
        conFrequency.runQuery(LinksSpecific.getSqlQuery("NameToNumber/NameToNumber_q05"));

        /* Creating name files
        Process process = runtime.exec(new String[]{"/bin/bash", "-c", "./latest.pl familyname familyname"});
        exitValue = process.waitFor();
        t.append("Exitcode0 = " + exitValue + "\r\n");

        process = runtime.exec(new String[]{"/bin/bash", "-c", "./latest.pl firstname firstname1"});
        exitValue = process.waitFor();
        t.append("Exitcode1 = " + exitValue + "\r\n");

        process = runtime.exec(new String[]{"/bin/bash", "-c", "./latest.pl firstname firstname2"});
        exitValue = process.waitFor();
        t.append("Exitcode2 = " + exitValue + "\r\n");

        process = runtime.exec(new String[]{"/bin/bash", "-c", "./latest.pl firstname firstname3"});
        exitValue = process.waitFor();
        t.append("Exitcode3 = " + exitValue + "\r\n");

        process = runtime.exec(new String[]{"/bin/bash", "-c", "./latest.pl firstname firstname4"});
        exitValue = process.waitFor();
        t.append("Exitcode4 = " + exitValue + "\r\n");

        // run File
        Process process = runtime.exec(new String[]{"/bin/bash", "-c", "mysql links_cleaned --user=linksdev --password=devlinks < updates0.sql"});
        exitValue = process.waitFor();
        t.append("Exitcode_1 = " + exitValue + "\r\n"); */
        /////
        //Process process = runtime.exec(new String[]{"/bin/bash", "-c", "/etc/init.d/mysqld restart"});
        //exitValue = process.waitFor();
        //t.append("restart = " + exitValue + "\r\n");
        
        //process = runtime.exec(new String[]{"/bin/bash", "-c", "mysqlcheck --user=linksdev --password=devlinks --auto-repair -c -o links_cleaned"});
        //exitValue = process.waitFor();
        //t.append("optimize = " + exitValue + "\r\n");
        
//        conFrequency.runQuery(LinksSpecific.getSqlQuery("NameToNumber/NameToNumber_q02"));


//        process = runtime.exec(new String[]{"/bin/bash", "-c", "mysql links_cleaned --user=linksdev --password=devlinks < updates1.sql"});
//        exitValue = process.waitFor();
//        t.append("Exitcode_1 = " + exitValue + "\r\n");

        
        
//        process = runtime.exec(new String[]{"/bin/bash", "-c", "/etc/init.d/mysqld restart"});
//        exitValue = process.waitFor();
//        t.append("restart = " + exitValue + "\r\n");
        
//        process = runtime.exec(new String[]{"/bin/bash", "-c", "mysqlcheck --user=linksdev --password=devlinks --auto-repair -c -o links_cleaned"});
//        exitValue = process.waitFor();
//        t.append("optimize = " + exitValue + "\r\n");
        
//        conFrequency.runQuery(LinksSpecific.getSqlQuery("NameToNumber/NameToNumber_q03"));
        
//        process = runtime.exec(new String[]{"/bin/bash", "-c", "mysql links_cleaned --user=linksdev --password=devlinks < updates2.sql"});
//        exitValue = process.waitFor();
//        t.append("Exitcode_2 = " + exitValue + "\r\n");

        
        
//        process = runtime.exec(new String[]{"/bin/bash", "-c", "/etc/init.d/mysqld restart"});
//        exitValue = process.waitFor();
//        t.append("restart = " + exitValue + "\r\n");
        
//        process = runtime.exec(new String[]{"/bin/bash", "-c", "mysqlcheck --user=linksdev --password=devlinks --auto-repair -c -o links_cleaned"});
//        exitValue = process.waitFor();
//        t.append("optimize = " + exitValue + "\r\n");
        
//        conFrequency.runQuery(LinksSpecific.getSqlQuery("NameToNumber/NameToNumber_q04"));
//        conFrequency.runQuery(LinksSpecific.getSqlQuery("NameToNumber/NameToNumber_q05"));

        
        //        process = runtime.exec(new String[]{"/bin/bash", "-c", "mysql links_cleaned --user=linksdev --password=devlinks < updates3.sql"});
        //        exitValue = process.waitFor();
        //        t.append("Exitcode_3 = " + exitValue + "\r\n");
        //        
        //        process = runtime.exec(new String[]{"/bin/bash", "-c", "mysql links_cleaned --user=linksdev --password=devlinks < updates4.sql"});
        //        exitValue = process.waitFor();
        //        t.append("Exitcode_4 = " + exitValue + "\r\n");

    }

    /**
     * 
     * @throws Exception 
     */
    public void doLevenshtein() throws Exception {

        prematch.Lv lv1 = new prematch.Lv(t, ti, "firstname", true);
        prematch.Lv lv2 = new prematch.Lv(t, ti, "firstname", false);
        prematch.Lv lv3 = new prematch.Lv(t, ti, "familyname", true);
        prematch.Lv lv4 = new prematch.Lv(t, ti, "familyname", false);

        lv1.start();
        lv2.start();
        lv3.start();
        lv4.start();
    }

    /**
     * 
     * @throws Exception 
     */
    public void doCreateBaseTable() throws Exception {

        if (!defaultConst) {
            t.append("Creating LINKS_BASE tables...");
        }

        {
            // TODO, make it shorter
            t.append("Running query 1...\r\n");
            conBase.runQuery(LinksSpecific.getSqlQuery("FillBaseTable/FillBaseTable_q01"));
            t.append("Running query 2...\r\n");
            conBase.runQuery(LinksSpecific.getSqlQuery("FillBaseTable/FillBaseTable_q02"));
            t.append("Running query 3...\r\n");
            conBase.runQuery(LinksSpecific.getSqlQuery("FillBaseTable/FillBaseTable_q03"));
            t.append("Running query 4...\r\n");
            conBase.runQuery(LinksSpecific.getSqlQuery("FillBaseTable/FillBaseTable_q04"));
            t.append("Running query 5...\r\n");
            conBase.runQuery(LinksSpecific.getSqlQuery("FillBaseTable/FillBaseTable_q05"));
            t.append("Running query 6...\r\n");
            conBase.runQuery(LinksSpecific.getSqlQuery("FillBaseTable/FillBaseTable_q06"));
            t.append("Running query 7...\r\n");
            conBase.runQuery(LinksSpecific.getSqlQuery("FillBaseTable/FillBaseTable_q07"));
            t.append("Running query 8...\r\n");
            conBase.runQuery(LinksSpecific.getSqlQuery("FillBaseTable/FillBaseTable_q08"));
            t.append("Running query 9...\r\n");
            conBase.runQuery(LinksSpecific.getSqlQuery("FillBaseTable/FillBaseTable_q09"));
            t.append("Running query 10...\r\n");
            conBase.runQuery(LinksSpecific.getSqlQuery("FillBaseTable/FillBaseTable_q10"));
            t.append("Running query 11...\r\n");
            conBase.runQuery(LinksSpecific.getSqlQuery("FillBaseTable/FillBaseTable_q11"));
            t.append("Running query 12...\r\n");
            conBase.runQuery(LinksSpecific.getSqlQuery("FillBaseTable/FillBaseTable_q12"));
            t.append("Running query 13...\r\n");
            conBase.runQuery(LinksSpecific.getSqlQuery("FillBaseTable/FillBaseTable_q13"));
            t.append("Running query 14...\r\n");
            conBase.runQuery(LinksSpecific.getSqlQuery("FillBaseTable/FillBaseTable_q14"));
            t.append("Running query 15...\r\n");
            conBase.runQuery(LinksSpecific.getSqlQuery("FillBaseTable/FillBaseTable_q15"));
            t.append("Running query 16...\r\n");
            conBase.runQuery(LinksSpecific.getSqlQuery("FillBaseTable/FillBaseTable_q16"));
            t.append("Running query 17...\r\n");
            conBase.runQuery(LinksSpecific.getSqlQuery("FillBaseTable/FillBaseTable_q17"));
            t.append("Running query 18...\r\n");
            conBase.runQuery(LinksSpecific.getSqlQuery("FillBaseTable/FillBaseTable_q18"));
            t.append("Running query 19...\r\n");
            conBase.runQuery(LinksSpecific.getSqlQuery("FillBaseTable/FillBaseTable_q19"));
            t.append("Running query 20...\r\n");
            conBase.runQuery(LinksSpecific.getSqlQuery("FillBaseTable/FillBaseTable_q20"));
            t.append("Running query 21...\r\n");
            conBase.runQuery(LinksSpecific.getSqlQuery("FillBaseTable/FillBaseTable_q21"));
            t.append("Running query 22...\r\n");
            conBase.runQuery(LinksSpecific.getSqlQuery("FillBaseTable/FillBaseTable_q22"));
            t.append("Running query 23...\r\n");
            conBase.runQuery(LinksSpecific.getSqlQuery("FillBaseTable/FillBaseTable_q23"));
            t.append("Running query 24...\r\n");
            conBase.runQuery(LinksSpecific.getSqlQuery("FillBaseTable/FillBaseTable_q24"));
            t.append("Running query 25...\r\n");
            conBase.runQuery(LinksSpecific.getSqlQuery("FillBaseTable/FillBaseTable_q25"));
            t.append("Running query 26...\r\n");
            conBase.runQuery(LinksSpecific.getSqlQuery("FillBaseTable/FillBaseTable_q26"));
            t.append("Running query 27...\r\n");
            conBase.runQuery(LinksSpecific.getSqlQuery("FillBaseTable/FillBaseTable_q27"));
            t.append("Running query 28...\r\n");
            conBase.runQuery(LinksSpecific.getSqlQuery("FillBaseTable/FillBaseTable_q28"));
            t.append("Running query 29...\r\n");
            conBase.runQuery(LinksSpecific.getSqlQuery("FillBaseTable/FillBaseTable_q29"));
            t.append("Running query 30...\r\n");
            conBase.runQuery(LinksSpecific.getSqlQuery("FillBaseTable/FillBaseTable_q30"));
            t.append("Running query 31...\r\n");
            conBase.runQuery(LinksSpecific.getSqlQuery("FillBaseTable/FillBaseTable_q31"));
            t.append("Running query 32...\r\n");
            conBase.runQuery(LinksSpecific.getSqlQuery("FillBaseTable/FillBaseTable_q32"));
            t.append("Running query 33...\r\n");
            conBase.runQuery(LinksSpecific.getSqlQuery("FillBaseTable/FillBaseTable_q33"));
            
//            t.append("Running query 41...\r\n");
//            conBase.runQuery(LinksSpecific.getSqlQuery("FillBaseTable/FillBaseTable_q41"));
//            t.append("Running query 42...\r\n");
//            conBase.runQuery(LinksSpecific.getSqlQuery("FillBaseTable/FillBaseTable_q42"));
//            t.append("Running query 43...\r\n");
//            conBase.runQuery(LinksSpecific.getSqlQuery("FillBaseTable/FillBaseTable_q43"));
//            t.append("Running query 44...\r\n");
//            conBase.runQuery(LinksSpecific.getSqlQuery("FillBaseTable/FillBaseTable_q44"));
//            t.append("Running query 45...\r\n");
//            conBase.runQuery(LinksSpecific.getSqlQuery("FillBaseTable/FillBaseTable_q45"));
//            t.append("Running query 46...\r\n");
//            conBase.runQuery(LinksSpecific.getSqlQuery("FillBaseTable/FillBaseTable_q46"));
//            t.append("Running query 47...\r\n");
//            conBase.runQuery(LinksSpecific.getSqlQuery("FillBaseTable/FillBaseTable_q47"));
        }

        if (!defaultConst) {
            t.append(".OK");
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

    /**
     *
     * @param seconds
     * @return
     */
    public static String stopWatch(int seconds) {
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

    /**
     *
     * @throws Exception
     */
    private void createTempFirstname() throws Exception {

        String c = "CREATE  TABLE links_temp.firstname_t_split ("
                + " person_id INT UNSIGNED NOT NULL AUTO_INCREMENT ,"
                + " firstname1 VARCHAR(30) NULL ,"
                + " firstname2 VARCHAR(30) NULL ,"
                + " firstname3 VARCHAR(30) NULL ,"
                + " firstname4 VARCHAR(30) NULL ,"
                + " PRIMARY KEY (person_id) );";

        conTemp.runQuery(c);
    }

    /**
     *
     * @throws Exception
     */
    private void createTempFirstnameFile() throws Exception {
        writerFirstname = new java.io.FileWriter("firstname_t_split.csv");
    }

    /**
     *
     * @throws Exception
     */
    private void loadFirstnameToTable() throws Exception {
        if (!defaultConst) {
            t.append("Loading CSV data into temp table...");
        }
        {
            String query = "LOAD DATA LOCAL INFILE 'firstname_t_split.csv' INTO TABLE firstname_t_split FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' ( person_id , firstname1 , firstname2 , firstname3 , firstname4 );";
            conTemp.runQuery(query);
        }
        if (!defaultConst) {
            t.append("OK. \r\n");
        }
    }

    /**
     *
     */
    private void updateFirstnameToPersonC() throws Exception {
        if (!defaultConst) {
            t.append("Moving first names from temp table to person_c...");
        }


        String query = "UPDATE links_cleaned.person_c, links_temp.firstname_t_split"
                + " SET "
                + " links_cleaned.person_c.firstname1 = links_temp.firstname_t_split.firstname1 ,"
                + " links_cleaned.person_c.firstname2 = links_temp.firstname_t_split.firstname2 ,"
                + " links_cleaned.person_c.firstname3 = links_temp.firstname_t_split.firstname3 ,"
                + " links_cleaned.person_c.firstname4 = links_temp.firstname_t_split.firstname4"
                + " WHERE links_cleaned.person_c.id_person = links_temp.firstname_t_split.person_id;";

        conTemp.runQuery(query);

        if (!defaultConst) {
            t.append("OK. \r\n");
        }
    }

    /**
     *
     * @throws Exception
     */
    public void removeFirstnameFile() throws Exception {
        File file = new File("firstname_t_split.csv");
        file.delete();
    }

    /**
     *
     * @throws Exception
     */
    public void removeFirstnameTable() throws Exception {
        String query = "DROP TABLE firstname_t_split;";
        conTemp.runQuery(query);
    }
}
