package moduleMain;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

import java.lang.reflect.Method;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Properties;
import java.util.Queue;
import java.util.Set;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.swing.JTextArea;
import javax.swing.JTextField;

import dataSet.ActRoleSet;
import dataSet.Ages;
import dataSet.ArrayListNonCase;
import dataSet.DateYearMonthDaySet;
import dataSet.DevinedMinMaxDatumSet;
import dataSet.DoSet;
import dataSet.MarriageYearPersonsSet;
import dataSet.MinMaxDateSet;
import dataSet.MinMaxYearSet;
import dataSet.PersonC;
import dataSet.RegistrationC;
import dataSet.RelationSet;
import dataSet.TableToArraysSet;

import connectors.MySqlConnector;
import enumDefinitions.TableType;
import enumDefinitions.TimeType;
import linksManager.ManagerGui;
//import moduleMain.LinksSpecific.*;


/**
 * @author Omar Azouguagh
 * @author Fons Laan
 *
 * <p/>
 * FL-30-Jun-2014 Imported from OA backup
 * FL-23-Jul-2014 Read properties file
 * FL-23-Jul-2014 Latest change
 */
public class LinksCleaned extends Thread
{
    /**
     * Table to Array Sets for the
     */
    private TableToArraysSet ttalOccupation;
    private TableToArraysSet ttalFirstname;
    private TableToArraysSet ttalFamilyname;
    private TableToArraysSet ttalRegistration;
    private TableToArraysSet ttalLocation;
    private TableToArraysSet ttalStatusSex;
    private TableToArraysSet ttalPrepiece;
    private TableToArraysSet ttalSuffix;
    private TableToArraysSet ttalAlias;
    private TableToArraysSet ttalReport;

    private JTextField tbLOLClatestOutput;
    private JTextArea taLOLCoutput;

    private String bronFilter = "";
    private String sourceFilter = "";
    private String bronFilterCleanPers = "";
    private String bronFilterOrigineelPers = "";
    private String bronFilterCleanReg = "";
    private String bronFilterOrigineelReg = "";

    private MySqlConnector conLog;
    private MySqlConnector conCleaned;
    private MySqlConnector conGeneral;
    private MySqlConnector conOriginal;
    private MySqlConnector conTemp;
    private MySqlConnector conOr;
    private Runtime r = Runtime.getRuntime();
    private String tempTableName;
    private DoSet dos;
    private final static String SC_I = "o";
    private final static String SC_X = "x";
    private final static String SC_N = "n";
    private final static String SC_Y = "y";
    // old links_base
    private ArrayList<Integer> hpChildRegistration = new ArrayList<Integer>();
    private ArrayList<Integer> hpChildAge = new ArrayList<Integer>();
    private ArrayList<Integer> hpChildMonth = new ArrayList<Integer>();
    private ArrayList<Integer> hpChildWeek = new ArrayList<Integer>();
    private ArrayList<Integer> hpChildDay = new ArrayList<Integer>();
    private ArrayList<Integer> hpBrideRegistration = new ArrayList<Integer>();
    private ArrayList<Integer> hpBrideAge = new ArrayList<Integer>();
    private ArrayList<Integer> hpBrideMonth = new ArrayList<Integer>();
    private ArrayList<Integer> hpBrideWeek = new ArrayList<Integer>();
    private ArrayList<Integer> hpBrideDay = new ArrayList<Integer>();
    private ArrayList<Integer> hpGroomRegistration = new ArrayList<Integer>();
    private ArrayList<Integer> hpGroomAge = new ArrayList<Integer>();
    private ArrayList<Integer> hpGroomMonth = new ArrayList<Integer>();
    private ArrayList<Integer> hpGroomWeek = new ArrayList<Integer>();
    private ArrayList<Integer> hpGroomDay = new ArrayList<Integer>();
    private ArrayList<Integer> hpDeceasedRegistration = new ArrayList<Integer>();
    private ArrayList<Integer> hpDeceasedAge = new ArrayList<Integer>();
    private ArrayList<Integer> hpDeceasedMonth = new ArrayList<Integer>();
    private ArrayList<Integer> hpDeceasedWeek = new ArrayList<Integer>();
    private ArrayList<Integer> hpDeceasedDay = new ArrayList<Integer>();

    private java.io.FileWriter writerFamilyname;
    private java.io.FileWriter writerFirstname;
    private ManagerGui mg;

    private String ref_url  = "";       // reference db access
    private String ref_user = "";
    private String ref_pass = "";

    private int teller = 0;
    private int bronNr;

    private String url  = "";           // links db's access
    private String user = "";
    private String pass = "";

    private int[] sources = { 10, 225 };

    /**
     * Constructor
     *
     * @param ref_url
     * @param ref_user
     * @param ref_pass
     * @param bronNr
     * @param url
     * @param user
     * @param pass
     * @param tbLOLClatestOutput
     * @param taLOLCoutput
     * @param dos
     * @param mg
     */
    public LinksCleaned(
            String ref_url,
            String ref_user,
            String ref_pass,
            int bronNr,
            String url,
            String user,
            String pass,
            JTextField tbLOLClatestOutput,
            JTextArea taLOLCoutput,
            DoSet dos,
            ManagerGui mg)
    {
        this.ref_url = ref_url;
        this.ref_user = ref_user;
        this.ref_pass = ref_pass;
        this.bronNr = bronNr;
        this.url = url;
        this.user = user;
        this.pass = pass;
        this.tbLOLClatestOutput = tbLOLClatestOutput;
        this.taLOLCoutput = taLOLCoutput;
        this.dos = dos;
        this.mg = mg;

        String timestamp = LinksSpecific.getTimeStamp2( "hh:mm:ss" );
        System.out.println( timestamp + "  linksCleaned()" );

        System.out.println( "mysql_hsnref_hosturl:\t"  + ref_url );
        System.out.println( "mysql_hsnref_username:\t" + ref_user );
        System.out.println( "mysql_hsnref_password:\t" + ref_pass );
    }

    @Override
    /**
     * Begin
     */
    public void run()
    {
        System.out.println( "LinksCleaned/run()" );

        try {

            // Vars
            String ts = "";                                 // timestamp
            long timeExpand = 0;
            long begintime = System.currentTimeMillis();
            tempTableName = LinksSpecific.getTimeStamp();

            clearTextFields();                              // Clear output text fields on form
            connectToDatabases();                           // Connect to Databases
            createLogTable();                               // Create log table

            if( bronNr != 0 ) { setSourceFilters(); }       // Set source filters

            if( dos.isDoRenewData() ) { funcRenewData(); }  // Renew Data in Cleaned

            // Load reports
            ts = LinksSpecific.getTimeStamp2( "hh:mm:ss" );
            System.out.println( ts + " Loading report table" );
            funcShowMessage( "Loading report table", false, false );
            {
                ttalReport = new TableToArraysSet( conGeneral, conOr, "", "report" );
            }
            funcShowMessage( ".", false, true );


            // basic names TEMP
            if( dos.isDoPreBasicNames() )
            {
                ts = LinksSpecific.getTimeStamp2( "hh:mm:ss" );
                System.out.println( ts + " dos.isDoPreBasicNames" );

                // load the ref tables
                ts = LinksSpecific.getTimeStamp2( "hh:mm:ss" );
                System.out.println( ts + " Loading reftabel(s)" );
                funcShowMessage( "Loading reftabel(s): " + "firstname/familyname/prepiece/suffix", false, false );
                {
                    // ttalFirstname = new TableToArraysSet(conGeneral, "original", "firstname");
                    // ttalFamilyname = new TableToArraysSet(conGeneral, "original", "familyname");
                    ttalPrepiece = new TableToArraysSet(conGeneral, conOr, "original", "prepiece");
                    ttalSuffix = new TableToArraysSet(conGeneral, conOr, "original", "suffix");
                    ttalAlias = new TableToArraysSet(conGeneral, conOr, "original", "alias");
                }
                funcShowMessage( ".", false, true );

                // run name functions
                //removeFirstnameTable();
                createTempFirstname();
                createTempFirstnameFile();
                ttalFirstname = new TableToArraysSet(conGeneral, conOr, "original", "firstname");
                runMethod("funcStandardFirstname");
                ttalFirstname.updateTable();
                ttalFirstname.free();
                writerFirstname.close();
                loadFirstnameToTable();
                updateFirstnameToPersonC();
                removeFirstnameFile();
                removeFirstnameTable();

                //removeFamilynameTable();
                createTempFamilyname();
                createTempFamilynameFile();
                ttalFamilyname = new TableToArraysSet(conGeneral, conOr, "original", "familyname");
                runMethod("funcStandardFamilyname");
                ttalFamilyname.updateTable();
                ttalFamilyname.free();
                writerFamilyname.close();
                loadFamilynameToTable();
                updateFamilynameToPersonC();
                removeFamilynameFile();
                removeFamilynameTable();

                // Update other tables
                ttalPrepiece.updateTable();
                ttalSuffix.updateTable();
                ttalPrepiece.free();
                ttalSuffix.free();


                ts = LinksSpecific.getTimeStamp2( "hh:mm:ss" );
                System.out.println( ts + " Converting names to lowercase" );
                funcShowMessage( "Converting names to lowercase", false, false );
                {
                    String qLower = "UPDATE links_cleaned.person_c SET firstname = LOWER(firstname),  familyname = LOWER(familyname);";
                    conCleaned.runQuery(qLower);
                }
                funcShowMessage( ".", false, true );


                //
                LinksPrematch lpm = new LinksPrematch(taLOLCoutput, tbLOLClatestOutput);

                // temp
                ts = LinksSpecific.getTimeStamp2( "hh:mm:ss" );
                funcShowMessage( "Splitting names", false, false );
                {
                    lpm.doSplitName();
                }
                funcShowMessage( ".", false, true );

                ts = LinksSpecific.getTimeStamp2( "hh:mm:ss" );
                funcShowMessage( "Creating unique name tables", false, false );
                {
                    lpm.doUniqueNameTablesTemp();
                }

                ts = LinksSpecific.getTimeStamp2( "hh:mm:ss" );
                funcShowMessage( "Basic names tables", false, false );
                {
                    lpm.doBasicName();
                }
            }


            // Names Section
            if( dos.isDoNames() )
            {
                ts = LinksSpecific.getTimeStamp2( "hh:mm:ss" );
                System.out.println( ts + " dos.isDoNames" );

                // Loading reference tables
                ts = LinksSpecific.getTimeStamp2( "hh:mm:ss" );
                funcShowMessage( "Loading name reference tables", false, false );
                {
                    ttalPrepiece = new TableToArraysSet(conGeneral, conOr, "original", "prepiece");
                    ttalSuffix = new TableToArraysSet(conGeneral, conOr, "original", "suffix");
                    ttalAlias = new TableToArraysSet(conGeneral, conOr, "original", "alias");
                }
                funcShowMessage( ".", false, true );

                // First name
                //removeFirstnameTable();
                createTempFirstname();
                createTempFirstnameFile();
                String IndexField = "original";
                String tableName = "firstname";
                ts = LinksSpecific.getTimeStamp2( "hh:mm:ss" );
                funcShowMessage( "TableToArraysSet: " + IndexField + ", " + tableName, false, false );
                ttalFirstname = new TableToArraysSet(conGeneral, conOr, "original", "firstname");
                funcShowMessage( ".", false, true );

                runMethod("funcStandardFirstname");
                ttalFirstname.updateTable();
                ttalFirstname.free();
                writerFirstname.close();
                loadFirstnameToTable();
                updateFirstnameToPersonC();
                removeFirstnameFile();
                removeFirstnameTable();

                // Family name
                //removeFamilynameTable();
                createTempFamilyname();
                createTempFamilynameFile();
                tableName = "familyname";
                ts = LinksSpecific.getTimeStamp2( "hh:mm:ss" );
                funcShowMessage( "TableToArraysSet: " + IndexField + ", " + tableName, false, false );
                ttalFamilyname = new TableToArraysSet(conGeneral, conOr, "original", "familyname");
                funcShowMessage( ".", false, true );

                runMethod("funcStandardFamilyname");
                ttalFamilyname.updateTable();
                ttalFamilyname.free();
                writerFamilyname.close();
                loadFamilynameToTable();
                updateFamilynameToPersonC();
                removeFamilynameFile();
                removeFamilynameTable();

                // Delete empty records
                funcDeleteRows();

                // Names to lowercase
                ts = LinksSpecific.getTimeStamp2( "hh:mm:ss" );
                funcShowMessage( "Converting names to lowercase", false, false );
                {
                    String qLower = "UPDATE links_cleaned.person_c SET firstname = LOWER(firstname),  familyname = LOWER(familyname);";
                    conCleaned.runQuery(qLower);
                }
                funcShowMessage( ".", false, true );

                // Run prepiece
                runMethod("funcStandardPrepiece");

                // Run suffix
                runMethod("funcStandardSuffix");

                // Update reference
                ts = LinksSpecific.getTimeStamp2( "hh:mm:ss" );
                funcShowMessage( "Updating names reference tables...", false, false );
                {
                    ttalPrepiece.updateTable();
                    ttalSuffix.updateTable();
                    ttalAlias.updateTable();
                }
                funcShowMessage( ".", false, true );
            }

            if( 1 == 1 ) { System.out.println( "ABORT." ); return; }

            // Remarks
            if (dos.isDoRemarks())
            {
                ts = LinksSpecific.getTimeStamp2( "hh:mm:ss" );
                System.out.println( ts + " dos.isDoRemarks" );

                // load al refs used by remarks Parser
                funcShowMessage( "Loading reftabel(s): " + "location/occupation" + "...", false, false );
                {
                    //ttalLocation = new TableToArraysSet(conGeneral, "original", "location");
                    //ttalOccupation = new TableToArraysSet(conGeneral, "original", "occupation");
                }
                funcShowMessage( ".", false, true );

                runMethod("scanRemarks");

                funcShowMessage( "Updating reftabel(s): " + "location/occupation" + "...", false, false );
                {
                    //ttalLocation.updateTable();
                    //ttalOccupation.updateTable();
                }
                funcShowMessage( ".", false, true );
            }

            // All location functions,
            if (dos.isDoLocations())
            {
                ts = LinksSpecific.getTimeStamp2( "hh:mm:ss" );
                System.out.println( ts + " dos.isDoLocations" );

                funcShowMessage( "Loading reftabel(s): " + "ref_location" + "...", false, false );
                {
                    ttalLocation = new TableToArraysSet(conGeneral, conOr, "original", "location");
                }
                funcShowMessage( ".", false, true );

                runMethod("funcStandardRegistrationLocation");
                runMethod("funcStandardBirthLocation");
                runMethod("funcStandardMarLocation");
                runMethod("funcStandardDeathLocation");

                funcShowMessage( "Updating reftabel(s): " + "ref_location" + "...", false, false );
                {
                    ttalLocation.updateTable();
                }
                funcShowMessage( ".", false, true );
            }

            // AGE YEAR
            if (dos.isDoAgeYear()) {
                runMethod("funcStandardYearAge");
            }

            // Status Sex
            if (dos.isDoStatusSex())
            {
                funcShowMessage( "Loading reftabel(s): " + "status_sex" + "...", false, false );
                {
                    ttalStatusSex = new TableToArraysSet(conGeneral, conOr, "original", "status_sex");
                }
                funcShowMessage( ".", false, true );

                runMethod("funcStandardSex");
                runMethod("funcStandardStatusSex");

                funcShowMessage( "Updating reftabel(s): " + "status_sex" + "...", false, false );
                {
                    ttalStatusSex.updateTable();
                }
                funcShowMessage( ".", false, true );
            }

            // registration Type
            if (dos.isDoType()) {
                runMethod("funcStandardType");
            }


            // ROLE
            if (dos.isDoRole()) {
                funcShowMessage( "Running funcStandardRole on all sources...", false, false );
                funcStandardRole();
                funcShowMessage( ".", false, true );
            }

            // DATE FUNCTIONS

            // Run date queries
            if (dos.isDoDates())
            {
                funcShowMessage( "Running Date functions on all sources...", false, false );
                {
                    // Clean dates
                    runMethod("funcStandardRegistrationDate");

                    // Clean
                    funcStandardDate("birth");
                    funcStandardDate("mar");
                    funcStandardDate("death");

                    // Fill empty dates with register dates
                    funcFlagBirthDate();
                    funcFlagMarriageDate();
                    funcFlagDeathDate();

                    funcStandardFlaggedDate("birth");
                    funcStandardFlaggedDate("mar");
                    funcStandardFlaggedDate("death");

                    funcMinMaxCorrectDate();

                    funcCompleteMinMaxBirth();
                    funcCompleteMinMaxMar();

                    funcSetcomplete();

                    // extra function to correct registration data
                    String q1 = "UPDATE links_cleaned.registration_c AS r, links_cleaned.person_c AS p SET r.registration_date = p.birth_date WHERE r.registration_date IS NULL AND r.id_registration = p.id_registration AND r.registration_maintype = 1 AND p.role = 1;";
                    String q2 = "UPDATE links_cleaned.registration_c AS r, links_cleaned.person_c AS p SET r.registration_date = p.mar_date WHERE r.registration_date IS NULL AND r.id_registration = p.id_registration AND r.registration_maintype = 2 AND p.role = 4;";
                    String q3 = "UPDATE links_cleaned.registration_c AS r, links_cleaned.person_c AS p SET r.registration_date = p.mar_date WHERE r.registration_date IS NULL AND r.id_registration = p.id_registration AND r.registration_maintype = 2 AND p.role = 7;";
                    String q4 = "UPDATE links_cleaned.registration_c AS r, links_cleaned.person_c AS p SET r.registration_date = p.death_date WHERE r.registration_date IS NULL AND r.id_registration = p.id_registration AND r.registration_maintype = 3 AND p.role = 10;";
                    String q5 = "UPDATE links_cleaned.registration_c AS r, links_cleaned.person_c AS p SET r.registration_date = p.death_date WHERE r.registration_date IS NULL AND r.id_registration = p.id_registration AND r.registration_maintype = 7 AND p.role = 10;";

                    conCleaned.runQuery(q1);
                    conCleaned.runQuery(q2);
                    conCleaned.runQuery(q3);
                    conCleaned.runQuery(q4);
                    conCleaned.runQuery(q5);

                }
                funcShowMessage( ".", false, true );
            }

            if (dos.isDoSequence()) {
                runMethod("funcStandardSequence");
            }

            if (dos.isDoRelation()) {
                runMethod("funcRelation");
            }

            // funMinMaxDateMain
            if (dos.isDoMinMaxDate()) {
                if (bronFilter.isEmpty()) {
                    for (int i : sources) {
                        funcShowMessage( "Running funMinMaxDateMain for source: " + i + "...", false, false );
                        {
                            funcFillMinMaxArrays("" + i);
                            funMinMaxDateMain("" + i);
                        }
                        funcShowMessage( ".", false, true );
                    }
                } else {
                    funcShowMessage( "Running funMinMaxDateMain...", false, false );
                    {
                        funcFillMinMaxArrays("" + this.bronNr);
                        funMinMaxDateMain("");
                    }
                    funcShowMessage( ".", false, true );
                }
            }

            // funcMinMaxMarriageYear
            if (dos.isDoMinMaxMarriage()) {
                try {
                    // loading ref
                    ResultSet refMinMaxMarriageYear = conGeneral.runQueryWithResult("SELECT * FROM ref_minmax_marriageyear");

                    if (bronFilter.isEmpty()) {

                        for (int i : sources) {

                            funcShowMessage( "Running funcMinMaxMarriageYear for source: " + i + "...", false, false );
                            {
                                funcMinMaxMarriageYear(funcSetMarriageYear(i + ""), refMinMaxMarriageYear);
                            }
                            funcShowMessage( ".", false, true );
                        }

                    } else {
                        funcShowMessage( "Running funcMinMaxMarriageYear...", false, false );
                        {
                            funcMinMaxMarriageYear(funcSetMarriageYear(this.bronNr + ""), refMinMaxMarriageYear);
                        }
                        funcShowMessage( ".", false, true );
                    }
                } catch (Exception e) {
                    funcShowMessage( "An error occured while running Min max Marriage date, properly ref_minmax_marriageyear error: " + e.getMessage(), false, true );
                }
            }

            if (dos.isDoPartsToFullDate()) {
                funcShowMessage( "Running func Part to Date on all sources...", false, false );
                funcPartsToDate();
                funcShowMessage( ".", false, true );
            }

            if (dos.isDoDaysSinceBegin()) {
                funcShowMessage( "Running func Days since begin on all sources...", false, false );
                funcDaysSinceBegin();
                funcShowMessage( ".", false, true );
            }

            if (dos.isDoPostTasks()) {
                funcShowMessage( "Running func post tasks all sources...", false, false );
                funcPostTasks();
                funcShowMessage( ".", false, true );
            }

            // Close connections
            conOriginal.close();
            conLog.close();
            conCleaned.close();
            conGeneral.close();
            conTemp.close();


            if (dos.isDoPrematch()) {
                funcShowMessage( "Running PREMATCH...", false, false );
                mg.firePrematch();
                funcShowMessage( ".", false, true );
            }

            // Total time
            timeExpand = System.currentTimeMillis() - begintime;
            int iTimeEx = (int) (timeExpand / 1000);

            funcShowMessage( "Conversion from Original to Cleaned is done; Total time: " + LinksSpecific.stopWatch(iTimeEx), false, true );
        } catch (Exception ex) {
            funcShowMessage( "Error: " + ex.getMessage(), false, true );
        }
    }

    /**
     * @param MethodName
     * @throws Exception
     */
    private void runMethod(String MethodName) throws Exception {

        Class[] partypes = new Class[1];
        Object[] argList = new Object[1];

        partypes[0] = String.class;

        // source 1 by 1
        if (bronFilter.isEmpty())
        {
            for (int i : sources) {
                String ts = LinksSpecific.getTimeStamp2( "hh:mm:ss" );
                funcShowMessage( ts + " Running " + MethodName + " for source: " + i + "...", false, false );

                argList[0] = i + "";
                Method m = this.getClass().getMethod(MethodName, partypes);

                // Call method
                m.invoke(this, argList);

                funcShowMessage( ".", false, true );
            }
        } else
        {
            String ts = LinksSpecific.getTimeStamp2( "hh:mm:ss" );
            funcShowMessage( ts + "Running " + MethodName + "...", false, false );

            argList[0] = "";
            Method m = this.getClass().getMethod(MethodName, partypes);

            // Call method
            m.invoke(this, argList);

            funcShowMessage( ".", false, true );
        }
    }

    /**
     * REMARKS SECTION
     * This section is not translated to English
     *
     *
     *
     *
     *
     *
     *
     *
     *
     */
    /**
     * @param rs
     * @param rsScanStrings
     * @return
     * @throws Exception
     */
    private HashMap functieParseRemarks(ResultSet rs, ResultSet rsScanStrings) throws Exception {

        // Hashmap voor de overgebleven
        HashMap cache = new HashMap();

        // Counter
        teller = 0;

        // Stappen instellen
        int step = 10000;

        /**
         * Door de opmerkingen heen lopen
         */
        while (rs.next()) {
            teller++;

//            if(teller>634646){
//                int iets = 0;
//                iets++;
//            }
//            else{
//                continue;
//            }

            // Get Opmerking
            String id_registratie = rs.getString("id_registration");
            String registratie_hoofdtype = rs.getString("registration_maintype");
            String bron = rs.getString("id_source");
            String opmerking = rs.getString("remarks");

            // Controleren of de opmerking null is
            if (opmerking == null) {
                continue;
            }

            // Controleren of de opmerking leeg is
            if (opmerking.isEmpty()) {
                continue;
            }

            /**
             * Controleren of de opmerking al in de cache voorkomt
             */
            if (cache.containsKey(registratie_hoofdtype + ":" + opmerking)) {
                Object o = cache.get(registratie_hoofdtype + ":" + opmerking).toString();
                int oToInt = Integer.parseInt(o.toString());
                int newValue = oToInt + 1;
                cache.put(registratie_hoofdtype + ":" + opmerking, newValue);
            } else {
                // TODO: Skip werkt niet helemaal goed
                int skipGroup = 0;


                /**
                 * Gebruiker op de hoogte stellen
                 */
                if (teller > step) {
                    funcShowMessage( (teller - 1) + "", true, true );
                    step += 10000;

                    // Clean memory
                    if (((teller - 1) % 50000) == 0) {
                        funcShowMessage( "Cleaning unused memory...", true, false );
                        r.gc();
                        funcShowMessage( "DONE!", true, true );
                    }
                }

                // Set matchfound boolean
                boolean matchFound = false;

                /**
                 * Opmerking strippen aan de hand van de tabel
                 * We lopen hier door alle regexen uit de tabel
                 */
                while (rsScanStrings.next()) {

                    // Er is net iets gevonden
                    if (matchFound) {

                        // Controleer of deze al in de cache zit
                        if (cache.containsKey(registratie_hoofdtype + ":" + opmerking)) {
                            break;
                        }

                        // Reset matchfound
                        matchFound = false;
                    }

                    // Haal regex uit de tabel
                    int aktenummer =
                            Integer.parseInt(rsScanStrings.getString("maintype"));

                    int groepnummer =
                            Integer.parseInt(rsScanStrings.getString("group_no"));

                    String scan_string = rsScanStrings.getString("scan_string");
                    String scan_waarde = rsScanStrings.getString("scan_value");
                    int role = rsScanStrings.getInt("role");
                    String veld = rsScanStrings.getString("field");

                    // Controleren of er iets verwijderd moet worden
                    // 99 betekent verwijderen
                    if (groepnummer == 99) {

                        opmerking = opmerking.replaceAll(scan_string, "");

                        continue;

                    }

                    // Controleren of dit de juiste groep is
                    if ((aktenummer != Integer.parseInt(registratie_hoofdtype))
                            && (aktenummer != 0)) {
                        continue;
                    }

                    /**
                     * Reguliere expressie gaat gebruikt worden
                     */
                    Pattern regex = Pattern.compile(scan_string);

                    // Voer expressie uit op de opmerking
                    Matcher m = regex.matcher(opmerking);

                    /**
                     * Controleer of er iets gevonden is
                     */
                    if (m.find()) {

                        // Controleer of deze groep overgeslagen moet worden
                        skipGroup = groepnummer;

                        // Opmerking strippen
                        opmerking = opmerking.replaceAll(scan_string, "");

                        // Destilleer het benodigde stukje op uit de opmerking
                        String currentPart = m.group();

                        HashMap insertValues = new HashMap();

                        /**
                         * We controleren of de gebruiker een functie aanroept,
                         * of een expressie wil uitvoeren
                         */
                        if (scan_waarde.toLowerCase().contains("functieopmerkingen")) {

                            // regex veld bevat een functie
                            // Nu zoeken we uit om welke functie het gaat

                            if (scan_waarde.toLowerCase().contains("functieopmerkingenwaardenadubbelepunt")) {
                                String tempValue = LinksSpecific.functieOpmerkingenWaardeNaDubbelePunt(currentPart);
                                insertValues.put(veld, tempValue);
                            } else if (scan_waarde.toLowerCase().contains("functieopmerkingenberoep")) {
                                String tempValue = functieOpmerkingenBeroep(id_registratie, bron, currentPart.toLowerCase());
                                insertValues.put(veld, tempValue);
                            } else if (scan_waarde.toLowerCase().contains("functieopmerkingenlocatie")) {
                                String tempValue = functieOpmerkingenLocatie(currentPart);
                                insertValues.put(veld, tempValue);
                            } else if (scan_waarde.toLowerCase().contains("functieopmerkingenlocatieendatum")) {
                                insertValues = functieOpmerkingenLocatieEnDatum(currentPart);
                            } else if (scan_waarde.toLowerCase().contains("functieopmerkingengebdatoverl")) {
                                insertValues = functieOpmerkingenGebDatOverl(currentPart);
                            } else if (scan_waarde.toLowerCase().contains("functieopmerkingenlevenloos")) {
                                insertValues.put(veld, "ja");
                            } else if (scan_waarde.toLowerCase().contains("functieopmerkingentelocatie")) { // no prob
                                String tempValue = functieOpmerkingenTeLocatie(currentPart);
                                insertValues.put(veld, tempValue);
                            } else if (scan_waarde.toLowerCase().contains("functieopmerkingenoudbijna")) { // no prob
                                insertValues.put(veld, "1");
                            } else if (scan_waarde.toLowerCase().contains("functieopmerkingeneldersoverleden")) { // no prob

                                // Update register.extract
                                String query = "UPDATE registration_c"
                                        + " SET registration_c.extract = 'y'"
                                        + " WHERE registration_c.id_registration = " + id_registratie;

                                // Voer query uit
                                conCleaned.runQuery(query);

                                // Clean datatype
                                insertValues.clear();
                            } // Extract met locatie
                            else if (scan_waarde.toLowerCase().contains("functieopmerkingenextract")) { // no prob

                                // Haal locatie op
                                String location = currentPart.replaceAll("(Extract|[U|u]ittreksel)[ ]*overlijdensregister[ ]*", "");

                                // Clean locatie
                                String cleanLocation = LinksSpecific.funcCleanSides(location);

                                // Schone locatie
                                String returnedLocation = functieVeldLocatie(cleanLocation);

                                // Maak query
                                String query = "UPDATE registration_c"
                                        + " SET registration_c.extract = 'y'"
                                        + " WHERE registration_c.id_registration = " + id_registratie;

                                // Voer query uit
                                conCleaned.runQuery(query);

                                // Geef locatie door
                                insertValues.put("death_location", returnedLocation);
                            } // Leeftijd jaar
                            else if (scan_waarde.toLowerCase().contains("functieopmerkingenleeftijdjaar")) {
                                Pattern regexval = Pattern.compile("[0-9]+");
                                Matcher mval = regexval.matcher(currentPart);

                                if (mval.find()) {
                                    // Er is een leeftijd gevonden

                                    int leeftijd = 0;

                                    try {
                                        leeftijd = Integer.parseInt(mval.group());
                                    } catch (Exception we) {
                                        // We doen hier niets mee omdat het nooit zal gebeuren
                                        // dat de expressie [0-9]+ een nietgetal zal opleveren
                                        // We hebben wel een catch voor de 0% uitzondering
                                    }

                                    // Geldige leeftijd valt tussen de 0 en 115
                                    if ((leeftijd > 0) && (leeftijd < 115)) {
                                        insertValues.put(veld, leeftijd);
                                    } else {
                                        // TODO: MELDING, ongeldige leeftijd

                                        // Leeg de dataset
                                        insertValues.clear();
                                    }
                                }

                                // Er is niets gevonden
                                insertValues.clear();
                            } else if (scan_waarde.toLowerCase().contains("functieopmerkingengeboreninjaartal")) {
                                String jaartal = currentPart.replaceAll("Geboren[ ]*in[ ]*", "");

                                int intJaartal = 0;

                                try {
                                    intJaartal = Integer.parseInt(jaartal);
                                } catch (Exception qa) {
                                    // We doen hier niets mee omdat het nooit zal gebeuren
                                    // dat de expressie [0-9]+ een nietgetal zal opleveren
                                    // We hebben wel een catch voor de 0% uitzondering
                                }

                                // Controleren op geldigheid jaartal
                                if ((intJaartal > 1680)
                                        && (intJaartal < 1960)) {

                                    insertValues.put(veld, intJaartal);
                                } else {

                                    // TODO: MELDING ongeldig jaartal
                                    insertValues.clear();

                                }
                            }
                        } else if (scan_waarde.toLowerCase().contains("functieopmerkingenwoonplaats")) {
                            String location = currentPart.replaceAll("[W|w]oonpl[a]?[a]?[t]?[s]?[ ]*", "");

                            String cleanLocation = LinksSpecific.funcCleanSides(location);

                            String returnedLocation = functieVeldLocatie(cleanLocation);

                            insertValues.put(veld, returnedLocation);
                        } else if (scan_waarde.toLowerCase().contains("functieopmerkingenweduw")) {
                            insertValues.put(veld, "verweduwd");
                        } else if (scan_waarde.toLowerCase().contains("functieopmerkingengeboortelocatiehuw")) {
                            String location = currentPart.replaceAll("[G|g]eboortepl[a]?[a]?[t]?[s]?[ ]*bruid[ ]+", "").replaceAll("[G|g]eboortepl[a]?[a]?[t]?[s]?[ ]*bruidegom[ ]+", "");

                            String cleanLocation = LinksSpecific.funcCleanSides(location);

                            String returnedLocation = functieVeldLocatie(cleanLocation);

                            insertValues.put(veld, returnedLocation);
                        } else if (scan_waarde.toLowerCase().contains("functieopmerkingenleeftijduren")) {
                            Pattern regexval = Pattern.compile("[0-9]+");
                            Matcher mval = regexval.matcher(currentPart);

                            if (mval.find()) {
                                int days = 0;

                                try {
                                    days = Integer.parseInt(mval.group());
                                } catch (Exception we) {
                                    // We doen hier niets mee omdat het nooit zal gebeuren
                                    // dat de expressie [0-9]+ een nietgetal zal opleveren
                                    // We hebben wel een catch voor de 0% uitzondering
                                }

                                if ((days > 0) && (days < 36)) {
                                    insertValues.put(veld, "1");
                                } else {
                                    // TODO: Onduidelijke melding

                                    insertValues.clear();
                                }
                            }

                            insertValues.clear();
                        } else if (scan_waarde.toLowerCase().contains("functieopmerkingendoopplaats")) { // no prob
                            String tempValue = functieOpmerkingenTeLocatie(currentPart);
                            insertValues.put(veld, tempValue);
                        } /**
                         * Het gaat om ene expressie en geen functie
                         */
                        else {

                            // Expressie wordt uitgevoerd
                            Pattern regexval = Pattern.compile(scan_waarde);
                            Matcher mval = regexval.matcher(currentPart);

                            // Als er wat gevonden, wordt dit toegevoegd
                            if (mval.find()) {

                                insertValues.put(veld, mval.group());

                            }

                        }

                        /**
                         * Verwerkingsfase:
                         * Controleer of er iets te verwerken valt
                         */
                        if (!insertValues.isEmpty()) {

                            // Maak een iterator aan om door de set te loopen
                            Set keySet = insertValues.keySet();
                            Iterator keySetIterator = keySet.iterator();

                            // Itereer door de dataset heen
                            while (keySetIterator.hasNext()) {

                                Object key = keySetIterator.next();
                                Object value = insertValues.get(key);

                                // Controleer op null
                                String valueQ = "";

                                if (value != null) {

                                    valueQ =
                                            LinksSpecific.funcPrepareForMysql(value.toString());

                                }

                                // Maak query aan
                                String query = "UPDATE person_c"
                                        + " SET person_c." + key.toString() + " = '" + valueQ + "'"
                                        + " WHERE person_c.id_registration = " + id_registratie
                                        + " AND person_c.role = " + role;

                                // Voer query uit
                                conCleaned.runQuery(query);
                            }

                            // Zet flag op true
                            // Nu wordt er eerst in cache gekeken
                            // alvorens er verder wordt gestript
                            matchFound = true;
                        }
                    }
                }

                // De resultset Iterator weer terugzetten
                rsScanStrings.beforeFirst();

                /**
                 * We zijn door de opmerking heen
                 * Controleer of de 'rest' opmerking ana de cache toegevoegd wordt
                 */
                if (cache.containsKey(registratie_hoofdtype + ":" + opmerking)) {

                    int newValue = Integer.parseInt(
                            cache.get(registratie_hoofdtype + ":" + opmerking).toString()) + 1;

                    cache.put(registratie_hoofdtype + ":" + opmerking, newValue);
                } else {

                    cache.put(registratie_hoofdtype + ":" + opmerking, 1);

                }

            }

        }

        return cache;

    }

    private String functieOpmerkingenBeroep(String id_registratie, String bron, String value) throws Exception {

        String beroep = LinksSpecific.functieOpmerkingenWaardeNaDubbelePunt(value);

        // verwerken, gaat via een generieke methode, ergens anders hebben we geen waarde na opmerking
        String nieuweBeroep = functieVeldBeroep(id_registratie, bron, beroep);

        // return event. bewerkte beroep
        return nieuweBeroep;
    }

    private String functieOpmerkingenTeLocatie(String currentPart) {
        String[] locationRaw = currentPart.split("te");

        // to prevent nullpoint exception
        if (locationRaw.length > 1) {
            String location = locationRaw[1];
            String cleanLocation = LinksSpecific.funcCleanSides(location);
            return cleanLocation;
        } else {
            return "";
        }
    }

    private String functieOpmerkingenLocatie(String value) throws Exception {
        String locatie = LinksSpecific.functieOpmerkingenWaardeNaDubbelePunt(value);

        // verwerken, gaat via een generieke methode, ergens anders hebben we geen waarde na opmerking
        String nieuweLocatie = functieVeldLocatie(locatie);

        // return event. bewerkte beroep
        return nieuweLocatie;
    }

    private HashMap functieOpmerkingenGebDatOverl(String currentPart) {
        String cleanValue = currentPart.replaceAll("[G|g]eboren", "");

        String cleanDate = LinksSpecific.funcCleanSides(cleanValue);

        // create hashmap to put values into
        HashMap values = new HashMap();

        values.put("birth_date", cleanDate);

        // date has to be devided
        DateYearMonthDaySet dymd = LinksSpecific.devideCheckDate(cleanDate);

        //add location
        values.put("birth_day", dymd.getDay());
        values.put("birth_month", dymd.getMonth());
        values.put("birth_year", dymd.getYear());

        // TODO: Verwerk logbestanden
        if (!dymd.getReportDay().isEmpty()) {
        }
        if (!dymd.getReportMonth().isEmpty()) {
        }
        if (!dymd.getReportYear().isEmpty()) {
        }

        //return
        return values;
    }

    private HashMap functieOpmerkingenLocatieEnDatum(String currentPart) throws Exception {
        String cleanValue = LinksSpecific.functieOpmerkingenWaardeNaDubbelePunt(currentPart);

        // functie datum scheiding
        String[] devidedValueDate = devideValueDate(cleanValue).split("&");

        HashMap values = new HashMap();

        //add location, via verwijzing
        values.put("birth_location", functieVeldLocatie(LinksSpecific.funcCleanSides(devidedValueDate[0])));


        String date = LinksSpecific.funcCleanSides(devidedValueDate[1]);

        values.put("birth_date", date);

        // date has to be devided
        DateYearMonthDaySet dymd = LinksSpecific.devideCheckDate(date);

        //add location
        values.put("birth_day", dymd.getDay());
        values.put("birth_month", dymd.getMonth());
        values.put("birth_year", dymd.getYear());

        // TODO: Verwerk logbestanden
        if (!dymd.getReportDay().isEmpty()) {
        }
        if (!dymd.getReportMonth().isEmpty()) {
        }
        if (!dymd.getReportYear().isEmpty()) {
        }

        //return
        return values;
    }

    // Verwerk in de lijst, en geef evt. ander beroep terug
    private String functieVeldBeroep(String id_registratie, String id_bron, String beroepTemp) throws Exception {
        if (beroepTemp != null) {

            // String beroep = beroepTemp.toLowerCase();
            String beroep = beroepTemp;

            if (ttalOccupation.originalExists(beroep)) {
                String nieuwCode = ttalOccupation.getStandardCodeByOriginal(beroep);

                if (nieuwCode == null ? SC_X == null : (nieuwCode.equals(SC_X))) {
                    // melding 41
                    try {
                        //addToLogTableRegistratie(id_registratie, id_bron, ErrorCode.WA, 41, "Beroep: " + LinksSpecific.functieParserVerbeterCharVoorMysql(beroepTemp) + " , geen standaard aanwezig, nieuwcode x");
                    } catch (Exception e) {
                    }

                    return beroep;
                } else if (nieuwCode == null ? SC_N == null : nieuwCode.equals(SC_N)) {
                    // melding 43
                    try {
                        //addToLogTableRegistratie(id_registratie, id_bron, ErrorCode.FT, 43, "Beroep: " + LinksSpecific.functieParserVerbeterCharVoorMysql(beroepTemp) + " , Ongeldig, geen standaard aanwezig, nieuwcode n");
                    } catch (Exception e) {
                    }

                    return "";
                } else if (nieuwCode == null ? SC_I == null : nieuwCode.equals(SC_I)) {
                    // melding 43
                    try {
                        //addToLogTableRegistratie(id_registratie, id_bron, ErrorCode.WA, 45, "Beroep: " + LinksSpecific.functieParserVerbeterCharVoorMysql(beroepTemp) + " , Ongeldig, wel standaard aanwezig, nieuwcode n");
                    } catch (Exception e) {
                    }

                    String beroepFromTable = ttalOccupation.getStandardByOriginal(beroep);
                    return beroepFromTable;
                } else if (nieuwCode == null ? SC_Y == null : nieuwCode.equals(SC_Y)) {
                    String beroepFromTable = ttalOccupation.getStandardByOriginal(beroep);
                    return beroepFromTable;
                } else {
                    return "";
                }
            } else {
                // melding 41
                try {
                    // addToLogTableRegistratie(id_registratie, id_bron, ErrorCode.WA, 41, "Beroep: " + LinksSpecific.functieParserVerbeterCharVoorMysql(beroepTemp) + " , geen standaard aanwezig, nieuwcode x");
                } catch (Exception e) {
                }

                // No beroep in Origineel
                ttalOccupation.addOriginal(beroep);
                return beroep;
            }
        }
        return "";
    }

    // Verwerk in de lijst, en geef evt. ander locatie terug
    private String functieVeldLocatie(String locatieTemp) throws Exception {
        if (locatieTemp != null) {

            // String locatie = locatieTemp.toLowerCase();
            String locatie = locatieTemp;

            if (ttalLocation.originalExists(locatie)) {
                String nieuwCode = ttalLocation.getStandardCodeByOriginal(locatie);

                if (nieuwCode == null ? SC_X == null : (nieuwCode.equals(SC_X))) {
                    // melding 32
                    return "";
                } else if (nieuwCode == null ? SC_N == null : nieuwCode.equals(SC_N)) {
                    // melding 33
                    return "";
                } else if (nieuwCode == null ? SC_I == null : nieuwCode.equals(SC_I)) {
                    // melding 34
                    String locatieFromTable = ttalLocation.getColumnByOriginal("location_no", locatie);
                    return locatieFromTable;
                } else if (nieuwCode == null ? SC_Y == null : nieuwCode.equals(SC_Y)) {
                    String locatieFromTable = ttalLocation.getColumnByOriginal("location_no", locatie);
                    return locatieFromTable;
                } else {
                    return "";
                }
            } else {
                ttalLocation.addOriginal(locatie);
            }
        }
        return "";
    }

    private String devideValueDate(String valueToDevide) {

        Pattern regex = Pattern.compile("[0-9]+-[0-9]+-[0-9]+");
        Matcher m = regex.matcher(valueToDevide);

        String date = m.group();

        String onlyData = valueToDevide.replaceAll("[0-9]+-[0-9]+-[0-9]+", "");

        return onlyData + "$" + date;
    }

    /**
     * @throws Exception
     */
    public void scanRemarks(String bronnr) throws Exception {

        /**
         * Lees Scan instellingen in
         */
        funcShowMessage("Preparing remarks parsing...", false, false);
        ResultSet rsScanStrings = conGeneral.runQueryWithResult(
                "SELECT * FROM scan_remarks ORDER BY maintype, group_no, priority_no");

        String query;

        if (bronnr.isEmpty()) {
            query = "SELECT id_registration , id_source , registration_maintype , remarks FROM registration_o" + bronFilter;
        } else {
            query = "SELECT id_registration , id_source , registration_maintype , remarks FROM registration_o WHERE id_source = " + bronnr;
        }


        // Lees Opmerkingen in
        ResultSet rs = conOriginal.runQueryWithResult(query);
        funcShowMessage("OK.", false, true);

        // Parsing Opmerkingen
        funcShowMessage("Parsing remarks...", false, false);
        HashMap cache;
        try {
            cache = functieParseRemarks(rs, rsScanStrings);
        } catch (Exception e) {
            funcShowMessage(teller + " ERROR:" + e.getMessage(), false, false);
            return;
        }
        funcShowMessage("OK.", false, true);

        // Maak logtabel aan met resterende opmekingen
        String createQuery = ""
                + "CREATE TABLE IF NOT EXISTS `links_logs`.`log_rest_remarks_" + bronNr + bronnr + "_" + tempTableName + "` (  "
                + "`id_log` INT NOT NULL AUTO_INCREMENT , "
                + "`registration_maintype` VARCHAR(3) NULL , "
                + "`content` VARCHAR(500) NULL , "
                + "`frequency` INT NULL , "
                + "PRIMARY KEY (`id_log`) , "
                + "INDEX `defaultindex` (`id_log` ASC) ) "
                + "DEFAULT CHARACTER SET = utf8;";

        // Voer query uit
        conLog.runQuery(createQuery);

        // Cache overzetten
        Set keySet = cache.keySet();
        Iterator keySetIterator = keySet.iterator();

        funcShowMessage("Writing rest remarks tot database...", false, false);

        // Loop door de resterende opmerkingen heen
        /*
        while (keySetIterator.hasNext()) {
        
        Object key = keySetIterator.next();
        Object value = cache.get(key);
        
        String[] velden = {"registration_maintype", "content", "frequency"};
        
        // eventuele quotes vervangen
        String cleanKey = LinksSpecific.funcPrepareForMysql(key.toString());
        String[] data = {cleanKey.substring(0, cleanKey.indexOf(":")), cleanKey.substring((cleanKey.indexOf(":") + 1)), value.toString()};
        conLog.insertIntoTable("log_rest_remarks_" + bronNr + bronnr + "_" + tempTableName, velden, data);
        
        }
        
         */

        rs.close();
        rs = null;

        funcShowMessage("OK.", false, true);
    }

    /**
     * This section contains help functions
     * ...
     */

    /**
     * @throws Exception
     */
    private void funcRenewData() throws Exception
    {
        String ts = LinksSpecific.getTimeStamp2( "hh:mm:ss" );
        System.out.println( ts + " funcRenewData()" );
        funcShowMessage( "Renewing data for links_cleaned", false, true );

        // Delete existing data
        // Create queries
        String deletePerson = "DELETE FROM person_c" + bronFilter;
        String deleteRegistration = "DELETE FROM registration_c" + bronFilter;

        // Execute queries
        ts = LinksSpecific.getTimeStamp2( "hh:mm:ss" );
        funcShowMessage( "Deleting previous data", false, true );
        conCleaned.runQuery(deletePerson);
        conCleaned.runQuery(deleteRegistration);

        // Copy links_original data to links_cleaned
        // Create queries
        ts = LinksSpecific.getTimeStamp2( "hh:mm:ss" );
        funcShowMessage( "Copying person keys to links_cleaned", false, true );
        String keysPerson = ""
                + "INSERT INTO links_cleaned.person_c ( "
                + "id_person , id_registration , id_source , id_person_o ) "
                + " SELECT id_person , id_registration , id_source , id_person_o "
                + "FROM links_original.person_o" + bronFilterOrigineelPers;

        ts = LinksSpecific.getTimeStamp2( "hh:mm:ss" );
        funcShowMessage( "Copying registration keys to links_cleaned", false, true );
        String keysRegistration = ""
                + "INSERT INTO links_cleaned.registration_c ("
                + "id_registration, id_source, id_orig_registration, registration_maintype, registration_seq ) "
                + "SELECT id_registration, id_source, id_orig_registration, registration_maintype, registration_seq "
                + "FROM links_original.registration_o" + bronFilterOrigineelReg;

        // Execute queries
        conCleaned.runQuery( keysPerson );
        conCleaned.runQuery( keysRegistration );
    }

    /**
     * @param id
     * @param id_source
     * @param errorCode
     * @param value
     * @throws Exception
     */
    private void funcAddtoReportRegistration(int id, String id_source, int errorCode, String value) throws Exception {

        String cla = ttalReport.getColumnByColumnInt("type", "class", errorCode);
        String con = ttalReport.getColumnByColumnInt("type", "content", errorCode);

        // WORKAROUND
        // replace error chars
        value = value.replaceAll("\\\\", "");
        value = value.replaceAll("\\$", "");
        value = value.replaceAll("\\*", "");

        con = con.replaceAll("<.*>", value);

        con = LinksSpecific.funcPrepareForMysql(con);

        String query = ""
                + " insert into links_logs.log" + tempTableName + "( reg_key , id_source , report_class , report_type , content , date_time )"
                + " values( " + id + " , " + id_source + " , '" + cla.toUpperCase() + "' , " + errorCode + " , '" + con + "' , NOW() ) ; ";

        conLog.runQuery(query);
    }

    /**
     * @param id
     * @param id_source
     * @param errorCode
     * @param value
     * @throws Exception
     */
    private void funcAddtoReportPerson(int id, String id_source, int errorCode, String value) throws Exception {
        String cla = ttalReport.getColumnByColumnInt("type", "class", errorCode);
        String con = ttalReport.getColumnByColumnInt("type", "content", errorCode);

        // WORKAROUND
        // replace error chars
        value = value.replaceAll("\\\\", "");
        value = value.replaceAll("\\$", "");
        value = value.replaceAll("\\*", "");


        con = con.replaceAll("<.*>", value);

        con = LinksSpecific.funcPrepareForMysql(con);

        String query = ""
                + " insert into links_logs.log" + tempTableName + "( pers_key , id_source , report_class , report_type , content , date_time )"
                + " values( " + id + " , " + id_source + " , '" + cla.toUpperCase() + "' , " + errorCode + " , '" + con + "' , NOW() ) ; ";

        conLog.runQuery(query);
    }

    /**
     * @param logText
     * @param isMinOnly
     * @param newLine
     */
    private void funcShowMessage( String logText, boolean isMinOnly, boolean newLine )
    {
        tbLOLClatestOutput.setText( logText );

        if( !isMinOnly ) {
            String newLineToken = "";
            if( newLine ) {
                newLineToken = "\r\n";
            }

            if( logText != "." ) {
                String ts = LinksSpecific.getTimeStamp2( "hh:mm:ss" );
                taLOLCoutput.append( ts + " " );
            }
            taLOLCoutput.append( logText + newLineToken );
        }
    }

    /**
     * @param name
     * @return
     */
    private String funcCleanNaam(String name) {
        return name.replaceAll("[^A-Za-z0-9 '\\-\\.,èêéëÈÊÉËùûúüÙÛÚÜiìîíïÌÎÍÏòôóöÒÔÓÖàâáöÀÂÁÄçÇ]+", "");
    }

    private String funcCleanFirstName(String name) {
        return name.replaceAll("[^A-Za-z0-9 '\\-èêéëÈÊÉËùûúüÙÛÚÜiìîíïÌÎÍÏòôóöÒÔÓÖàâáöÀÂÁÄçÇ]+", "");
    }

    private String funcCleanFamilyname(String name) {
        return name.replaceAll("[^A-Za-z0-9 '\\-èêéëÈÊÉËùûúüÙÛÚÜiìîíïÌÎÍÏòôóöÒÔÓÖàâáöÀÂÁÄçÇ]+", "").replaceAll("\\-", " ");
    }


    /**
     * Read properties file
     * Start the application and specify the property file with a parameter:
     * java -Dproperties.path="<path-to-properiesfile>" -jar LinksProject-2.0.jar
     */
    /*
    public Properties readProperties()
    {
        System.out.println("readProperties()");

        Properties properties = new Properties();
        InputStream input = null;

        String propertiesPath = System.getProperty( "properties.path" );
        if( propertiesPath == null ) {
            funcShowMessage( "No properties file.\nSTOP.", false, true );
            return properties;
        }
        else { System.out.println( "properties path: " + propertiesPath ); }

        try {
            System.out.println( "gettings properties.path" );

            input = ( propertiesPath == null )
                    ? getClass().getClassLoader().getResourceAsStream( propertiesPath )
                    : new FileInputStream( propertiesPath );

            if( input == null ) {
                System.out.println( "Cannot read: " + propertiesPath + ".\nSTOP.");
                return properties;
            }
            System.out.println( "properties file: " + propertiesPath );

            properties.load( input );

            //get the property values
            ref_url  = properties.getProperty( "mysql_hsnref_hosturl" );
            ref_user = properties.getProperty( "mysql_hsnref_username" );
            ref_pass = properties.getProperty( "mysql_hsnref_password" );

            System.out.println( "mysql_hsnref_hosturl:  " + ref_url );
            System.out.println( "mysql_hsnref_username: " + ref_user );
            System.out.println( "mysql_hsnref_password: " + ref_pass );

            url  = properties.getProperty( "mysql_links_hosturl" );
            user = properties.getProperty( "mysql_links_username" );
            pass = properties.getProperty( "mysql_links_password" );

            System.out.println( "mysql_links_hosturl:  " + url );
            System.out.println( "mysql_links_username: " + user );
            System.out.println( "mysql_links_password: " + pass );

            int source_id_first = Integer.parseInt(properties.getProperty("source_id_first"));
            int source_id_last  = Integer.parseInt(properties.getProperty("source_id_last"));

            System.out.println( "source_id_first: " + source_id_first );
            System.out.println( "source_id_last:  " + source_id_last );
          //System.out.println( "sources: " + Arrays.toString( sources ) );     // declared at top

        } catch( IOException ex ) {
            ex.printStackTrace();
        } finally {
            if( input != null ) {
                try {
                    input.close();
                } catch( IOException ex ) {
                    ex.printStackTrace();
                }
            }
        }

        return properties;
    }
    */

    /**
     * clear GUI output text fields
     */
    public void clearTextFields() {
        String timestamp = LinksSpecific.getTimeStamp2( "hh:mm:ss" );
        System.out.println( timestamp + " clearTextFields()" );

        tbLOLClatestOutput.setText( "" );
        taLOLCoutput.setText( "" );

    }

    /**
     * @throws Exception
     */
    private void connectToDatabases()
    throws Exception
    {
        String ts = LinksSpecific.getTimeStamp2( "hh:mm:ss" );
        System.out.println( ts + " connectToDatabases()" );
        funcShowMessage( "Connecting to databases.", false, true );

        //funcShowMessage( "links_general", false, true );
        conOr = new MySqlConnector( ref_url, "links_general", ref_user, ref_pass );

        //funcShowMessage( "links_original", false, true );
        conOriginal = new MySqlConnector( url, "links_original", user, pass );

        //funcShowMessage( "links_logs", false, true );
        conLog = new MySqlConnector( url, "links_logs", user, pass );

        //funcShowMessage( "links_cleaned", false, true );
        conCleaned = new MySqlConnector( url, "links_cleaned", user, pass );

        //funcShowMessage( "links_general", false, true );
        conGeneral = new MySqlConnector( url, "links_general", user, pass );

        //funcShowMessage( "links_temp", false, true );
        conTemp = new MySqlConnector( url, "links_temp", user, pass );
    }

    private void createLogTable()
    throws Exception
    {
        String ts = LinksSpecific.getTimeStamp2( "hh:mm:ss" );
        System.out.println( ts + " createLogTable()" );
        funcShowMessage( "Creating logging table.", false, true );

        String query = ""
                + " CREATE  TABLE `links_logs`.`log" + tempTableName + "` ("
                + " `id_log` INT UNSIGNED NOT NULL AUTO_INCREMENT ,"
                + " `id_source` INT UNSIGNED NULL ,"
                + " `archive` VARCHAR(30) NULL ,"
                + " `location` VARCHAR(50) NULL ,"
                + " `reg_type` VARCHAR(30) NULL ,"
                + " `date` VARCHAR(20) NULL ,"
                + " `sequence` VARCHAR(3) NULL ,"
                + " `role` VARCHAR(30) NULL ,"
                + " `table` VARCHAR(30) NULL ,"
                + " `reg_key` INT UNSIGNED NULL ,"
                + " `pers_key` INT UNSIGNED NULL ,"
                + " `report_class` VARCHAR(2) NULL ,"
                + " `report_type` INT UNSIGNED NULL ,"
                + " `content` VARCHAR(200) NULL ,"
                + " `date_time` DATETIME NOT NULL ,"
                + " PRIMARY KEY (`id_log`) );";

        conLog.runQuery( query );
    }

    /**
     * TODO clean this, bron -> source everywhere
     */
    private void setSourceFilters()
    {
        String ts = LinksSpecific.getTimeStamp2( "hh:mm:ss" );
        System.out.println( ts + " setSourceFilters()" );
        funcShowMessage( "Set source filters for:  + bronNr", false, true );

        bronFilter = " WHERE id_source = " + bronNr;
        sourceFilter = " WHERE id_source = " + bronNr;

        bronFilterCleanPers = " WHERE person_c.id_source = " + bronNr;
        bronFilterOrigineelPers = " WHERE person_o.id_source = " + bronNr;
        bronFilterCleanReg = " WHERE registration_c.id_source = " + bronNr;
        bronFilterOrigineelReg = " WHERE registration_o.id_source = " + bronNr;
    }

    /**
     * This section contains main functions
     *
     *
     *
     *
     *
     *
     *
     *
     *
     *
     *
     *
     *
     */

    /**
     * @param sourceNo
     * @throws Exception
     */
    public void funcStandardFirstname(String sourceNo) {

        int counter = 0;
        int step = 10000;
        int stepstate = step;
        String id_source;

        try {

            // create connection

            Connection con = getConnection("links_original");
            con.isReadOnly();

            String startQuery;

            if (sourceNo.isEmpty()) {
                startQuery = "SELECT id_person , firstname FROM person_o" + bronFilter + "";
                id_source = this.bronNr + "";
            } else {
                startQuery = "SELECT id_person , firstname FROM person_o WHERE id_source = " + sourceNo;
                id_source = sourceNo;
            }

            // startQuery = "SELECT id_person , firstname FROM person_o" + " WHERE id_source = 115";

            ResultSet rsFirstName = con.createStatement().executeQuery(startQuery);
            con.createStatement().close();
            // con.createStatement().close();

//            rsFirstName.setFetchSize(100000);
//           while (rsFirstName.next()) {
////
////              
//////                int id_person = rsFirstName.getInt("id_person");
//////                String firstname = rsFirstName.getString("firstname");
//            }

//            // close 
//            int iets = 0;
//            rsFirstName = con.createStatement().executeQuery("SELECT 0;");
//            con.createStatement().close();
//            rsFirstName.close();
//            rsFirstName = null;
//            
//            con.close();
//            System.gc();


            // get total
            rsFirstName.last();

            int total = rsFirstName.getRow();

            rsFirstName.beforeFirst();

            while (rsFirstName.next()) {

                counter++;
                if (counter == stepstate) {
                    funcShowMessage(counter + " of " + total, true, true);
                    stepstate += step;
                }


//                if (counter == 97253 ){
//                    int bla = 0;
//                    bla++;
//                }


                int id_person = rsFirstName.getInt("id_person");
                String firstname = rsFirstName.getString("firstname");

                // Is firstname empty?
                if (firstname != null && !firstname.isEmpty()) {

                    // clean name
                    firstname = funcCleanFirstName(firstname);

                    firstname = firstname.toLowerCase();

//                    if (id_person == 9363889) {
//                        int iets = 9;
//                    }

                    // Check name on aliasses
                    String nameNoAlias = funcStandardAlias(id_person, id_source, firstname);

                    // Check on serried spaces
                    // Split name on spaces
                    String[] names = nameNoAlias.split(" ");
                    boolean spaces = false;

                    ArrayList<String> preList = new ArrayList<String>();
                    ArrayList<String> postList = new ArrayList<String>();

                    for (String n : names) {
                        if (n.isEmpty()) {
                            spaces = true;
                        } else { // add to list
                            preList.add(n);
                        }
                    }

                    // EC
                    if (spaces) {
                        funcAddtoReportPerson(id_person, id_source, 1103, "");
                    }

                    // loop through names
                    for (int i = 0; i < preList.size(); i++) {

                        // Does this aprt exists in ref_name?
                        if (ttalFirstname.originalExists(preList.get(i))) {

                            // Check the standard code
                            String standard_code = ttalFirstname.getStandardCodeByOriginal(preList.get(i));
                            if (standard_code.equals(SC_Y)) {
                                postList.add(ttalFirstname.getStandardByOriginal(preList.get(i)));
                            } else if (standard_code.equals(SC_I)) { // EC 1100
                                funcAddtoReportPerson(id_person, id_source, 1100, preList.get(i));
                                postList.add(ttalFirstname.getStandardByOriginal(preList.get(i)));
                            } else if (standard_code.equals(SC_N)) { // EC 1105
                                funcAddtoReportPerson(id_person, id_source, 1105, preList.get(i));
                            } else if (standard_code.equals(SC_X)) { // EC 1109
                                funcAddtoReportPerson(id_person, id_source, 1109, preList.get(i));
                                postList.add(preList.get(i));
                            } else {// EC 1100
                                funcAddtoReportPerson(id_person, id_source, 1100, preList.get(i));
                            }
                        } // name does not exists in ref_firtname
                        else {

                            // check on invalid token
                            String nameNoInvalidChars = funcCleanNaam(preList.get(i));

                            // name contains invalid chars ?
                            if (!preList.get(i).equalsIgnoreCase(nameNoInvalidChars)) {

                                // EC 1104
                                funcAddtoReportPerson(id_person, id_source, 1104, preList.get(i));

                                // Check if name exists in ref
                                // Does this aprt exists in ref_name?
                                if (ttalFirstname.originalExists(nameNoInvalidChars)) {
                                    // Check the standard code
                                    String standard_code = ttalFirstname.getStandardCodeByOriginal(nameNoInvalidChars);
                                    if (standard_code.equals(SC_Y)) {
                                        postList.add(ttalFirstname.getStandardByOriginal(nameNoInvalidChars));
                                    } else if (standard_code.equals(SC_I)) { // EC 1100
                                        funcAddtoReportPerson(id_person, id_source, 1100, nameNoInvalidChars);
                                        postList.add(ttalFirstname.getStandardByOriginal(nameNoInvalidChars));
                                    } else if (standard_code.equals(SC_N)) { // EC 1105
                                        funcAddtoReportPerson(id_person, id_source, 1105, nameNoInvalidChars);
                                    } else if (standard_code.equals(SC_X)) { // EC 1109
                                        funcAddtoReportPerson(id_person, id_source, 1109, nameNoInvalidChars);
                                        postList.add(nameNoInvalidChars);
                                    } else { // EC 1100, standard_code not invalid
                                        funcAddtoReportPerson(id_person, id_source, 1100, nameNoInvalidChars);
                                    }

                                    // continue
                                    continue;
                                }

                                // check if it ends witch suffix
                                // Check on suffix
                                ArrayListNonCase sfxO = ttalSuffix.getArray("original");
                                ArrayListNonCase sfxSc = ttalSuffix.getArray("standard_code");

                                for (int j = 0; j < sfxO.size(); j++) {

                                    if (nameNoInvalidChars.endsWith(" " + sfxO.get(j).toString())
                                            && sfxSc.get(j).toString().equals(SC_Y)) {

                                        // EC 1106
                                        funcAddtoReportPerson(id_person, id_source, 1106, nameNoInvalidChars);

                                        nameNoInvalidChars = nameNoInvalidChars.replaceAll(" " + sfxO.get(j).toString(), "");

                                        // Set suffix
                                        String query = PersonC.updateQuery("suffix", sfxO.get(j).toString(), id_person);

                                        conCleaned.runQuery(query);
                                    }
                                }

                                // check ref_prepiece
                                String nameNoPieces = funcNamePrepiece(nameNoInvalidChars, id_person);

                                if (!nameNoPieces.equals(nameNoInvalidChars)) {

                                    // EC 1107
                                    funcAddtoReportPerson(id_person, id_source, 1107, nameNoInvalidChars);

                                }

                                // last check on ref
                                if (ttalFirstname.originalExists(nameNoPieces)) {
                                    // Check the standard code
                                    String standard_code = ttalFirstname.getStandardCodeByOriginal(nameNoPieces);
                                    if (standard_code.equals(SC_Y)) {
                                        postList.add(ttalFirstname.getStandardByOriginal(nameNoPieces));
                                    } else if (standard_code.equals(SC_I)) { // EC 1100
                                        funcAddtoReportPerson(id_person, id_source, 1100, nameNoPieces);
                                        postList.add(ttalFirstname.getStandardByOriginal(nameNoPieces));
                                    } else if (standard_code.equals(SC_N)) { // EC 1105
                                        funcAddtoReportPerson(id_person, id_source, 1105, nameNoPieces);
                                    } else if (standard_code.equals(SC_X)) { // EC 1109
                                        funcAddtoReportPerson(id_person, id_source, 1109, nameNoPieces);
                                        postList.add(nameNoPieces);
                                    } else { // EC 1100, standard_code not invalid
                                        funcAddtoReportPerson(id_person, id_source, 1100, nameNoPieces);
                                    }
                                } else {
                                    // name must be added to ref_firstname with standard_code x
                                    // Also add name to postlist
                                    ttalFirstname.addOriginal(nameNoPieces);
                                    postList.add(nameNoPieces);
                                }


                            } else { // no invalid token

                                // check if it ends witch suffix
                                // Check on suffix
                                ArrayListNonCase sfxO = ttalSuffix.getArray("original");
                                ArrayListNonCase sfxSc = ttalSuffix.getArray("standard_code");

                                for (int j = 0; j < sfxO.size(); j++) {

                                    if (nameNoInvalidChars.equalsIgnoreCase(sfxO.get(j).toString())
                                            && sfxSc.get(j).toString().equals(SC_Y)) {

                                        // EC 1106
                                        funcAddtoReportPerson(id_person, id_source, 1106, nameNoInvalidChars);

                                        nameNoInvalidChars = nameNoInvalidChars.replaceAll(sfxO.get(j).toString(), "");

                                        // Set suffix
                                        String query = PersonC.updateQuery("suffix", sfxO.get(j).toString(), id_person);

                                        conCleaned.runQuery(query);
                                    }
                                }

                                // check ref_prepiece
                                String nameNoPieces = funcNamePrepiece(nameNoInvalidChars, id_person);

                                if (!nameNoPieces.equals(nameNoInvalidChars)) {

                                    // EC 1107
                                    funcAddtoReportPerson(id_person, id_source, 1107, nameNoInvalidChars);

                                }

                                // last check on ref
                                if (ttalFirstname.originalExists(nameNoPieces)) {
                                    // Check the standard code
                                    String standard_code = ttalFirstname.getStandardCodeByOriginal(nameNoPieces);
                                    if (standard_code.equals(SC_Y)) {
                                        postList.add(ttalFirstname.getStandardByOriginal(nameNoPieces));
                                    } else if (standard_code.equals(SC_I)) { // EC 1100
                                        funcAddtoReportPerson(id_person, id_source, 1100, nameNoPieces);
                                        postList.add(ttalFirstname.getStandardByOriginal(nameNoPieces));
                                    } else if (standard_code.equals(SC_N)) { // EC 1105
                                        funcAddtoReportPerson(id_person, id_source, 1105, nameNoPieces);
                                    } else if (standard_code.equals(SC_X)) { // EC 1109
                                        funcAddtoReportPerson(id_person, id_source, 1109, nameNoPieces);
                                        postList.add(nameNoPieces);
                                    } else { // EC 1100, standard_code not invalid
                                        funcAddtoReportPerson(id_person, id_source, 1100, nameNoPieces);
                                    }
                                } else {
                                    // name must be added to ref_firstname with standard_code x
                                    // Also add name to postlist
                                    ttalFirstname.addOriginal(nameNoPieces);
                                    postList.add(nameNoPieces);
                                }
                            }
                        }
                    }

                    // Write all parts to Person POSTLIST
                    String vn = "";

                    for (int i = 0; i < postList.size(); i++) {

                        vn += postList.get(i);

                        // posible space
                        if (i != (postList.size() - 1)) {
                            vn += " ";
                        }
                    }

                    // if vn not empty wrtie to vn
                    if (!vn.isEmpty()) {

                        //String query = PersonC.updateQuery("firstname", vn, id_person);

                        //conCleaned.runQuery(query);

                        writerFirstname.write(id_person + "," + vn.trim().toLowerCase() + "\n");

                    }

                    preList.clear();
                    postList.clear();
                    preList = null;
                    postList = null;
                } else {

                    // First name is empty, EC 1101
                    funcAddtoReportPerson(id_person, id_source, 1101, "");
                }

                // close this
                id_person = 0;
                firstname = null;
            }

            // TODO: empty resultset
            //rsFirstName = con.createStatement().executeQuery("SELECT 0;");

            rsFirstName.close();
            con.close();

        } catch (Exception e) {
            funcShowMessage(counter + " An error occured while cleaning Firstname: " + e.getMessage(), false, true);
        }
    }

    /**
     * @param id
     * @param id_souce
     * @param name
     * @return
     */
    private String funcStandardAlias(int id, String id_souce, String name) throws Exception {

        dataSet.ArrayListNonCase ag = ttalAlias.getArray("original");

        // to lowercase
        name = name.toLowerCase();

        for (Object ags : ag) {

            String keyword = " " + ags.toString().toLowerCase() + " ";

            if (name.contains(" " + keyword + " ")) {

                // EC 17
                funcAddtoReportPerson(id, id_souce, 17, name);

                // prepare on braces
                if (keyword.contains("\\(") || keyword.contains("\\(")) {

                    keyword = keyword.replaceAll("\\(", "").replaceAll("\\)", "");
                }

                String[] names = name.toLowerCase().split(keyword, 2);

                /*
                we must clean the name because of the braces used in aliassen
                 */

                // Set alias
                PersonC.updateQuery("alias", LinksSpecific.funcCleanSides(funcCleanNaam(names[1])), id);

                return LinksSpecific.funcCleanSides(funcCleanNaam(names[0]));
            }
        }
        return name;
    }

    /**
     * @param name
     * @param id
     * @return
     * @throws Exception
     */
    private String funcNamePrepiece(String name, int id) throws Exception {

        // if no spaces return familyname
        if (!name.contains(" ")) {
            return name;
        }

        String fullName = "";

        String list_TN = "";
        String list_TO = "";
        String list_PF = "";

        // Split familyname
        Queue<String> names = new LinkedList();

        String[] namesArray = name.split(" ");

        for (int i = 0; i < namesArray.length; i++) {
            names.add(namesArray[i]);
        }

        // Check pieces
        while (!names.isEmpty()) {

            // Get part
            String part = names.poll();

            if (ttalPrepiece.originalExists(part) && ttalPrepiece.getStandardCodeByOriginal(part).equalsIgnoreCase(SC_Y)) {

                // Add to person
                if (ttalPrepiece.getColumnByOriginal("title_noble", part) != null && !ttalPrepiece.getColumnByOriginal("title_noble", part).isEmpty()) {
                    list_TN += ttalPrepiece.getColumnByOriginal("title_noble", part) + " ";
                } else if (ttalPrepiece.getColumnByOriginal("title_other", part) != null && !ttalPrepiece.getColumnByOriginal("title_other", part).isEmpty()) {
                    list_TO += ttalPrepiece.getColumnByOriginal("title_other", part) + " ";
                } else if (ttalPrepiece.getColumnByOriginal("prefix", part) != null && !ttalPrepiece.getColumnByOriginal("prefix", part).isEmpty()) {
                    list_PF += ttalPrepiece.getColumnByOriginal("prefix", part) + " ";
                }
            } else { // return name

                while (!names.isEmpty()) {
                    fullName += " " + names.poll();
                }

                // add part tot name
                fullName = part + fullName;

                break;
            }
        }

        // remove last spaces
        if (!list_TN.isEmpty()) {
            list_TN = list_TN.substring(0, (list_TN.length() - 1));

            conCleaned.runQuery(PersonC.updateQuery("title_noble", list_TN, id));
        }
        if (!list_TO.isEmpty()) {
            list_TO = list_TO.substring(0, (list_TO.length() - 1));

            conCleaned.runQuery(PersonC.updateQuery("title_other", list_TO, id));
        }
        if (!list_PF.isEmpty()) {
            list_PF = list_PF.substring(0, (list_PF.length() - 1));

            conCleaned.runQuery(PersonC.updateQuery("prefix", list_PF, id));
        }

        return fullName;
    }

    /**
     * @param sourceNo
     * @throws Exception
     */
    public void funcStandardFamilyname(String sourceNo) {

        int counter = 0;
        int step = 10000;
        int stepstate = step;

        try {

            String startQuery;
            String id_source;

            if (sourceNo.isEmpty()) {
                startQuery = "SELECT id_person , familyname FROM person_o" + bronFilter;
                id_source = this.bronNr + "";
            } else {
                startQuery = "SELECT id_person , familyname FROM person_o WHERE id_source = " + sourceNo;
                id_source = sourceNo;
            }


            // create connection
            Connection con = getConnection("links_original");
            con.isReadOnly();

            // Read family names from table
            ResultSet rsFamilyname = con.createStatement().executeQuery(startQuery);
            con.createStatement().close();

            // get total
            rsFamilyname.last();

            int total = rsFamilyname.getRow();

            rsFamilyname.beforeFirst();

            while (rsFamilyname.next()) {

                counter++;
                if (counter == stepstate) {
                    funcShowMessage(counter + " of " + total, true, true);
                    stepstate += step;
                }

                // Get family name
                String familyname = rsFamilyname.getString("familyname");
                int id_person = rsFamilyname.getInt("id_person");

                // Check is Familyname is not empty or null
                if (familyname != null && !familyname.isEmpty()) {

                    familyname = funcCleanFamilyname(familyname);

                    familyname = familyname.toLowerCase();

                    // Familienaam in ref_familyname ?
                    if (ttalFamilyname.originalExists(familyname)) {

                        // get standard_code
                        String standard_code = ttalFamilyname.getStandardCodeByOriginal(familyname);

                        // Check the standard code
                        if (standard_code.equals(SC_Y)) {

                            writerFamilyname.write(id_person + "," + ttalFamilyname.getStandardByOriginal(familyname).toLowerCase() + "\n");

                        } else if (standard_code.equals(SC_I)) {

                            // EC 1000
                            funcAddtoReportPerson(id_person, id_source, 1000, familyname);

                            writerFamilyname.write(id_person + "," + ttalFamilyname.getStandardByOriginal(familyname).toLowerCase() + "\n");

                        } else if (standard_code.equals(SC_N)) {

                            // EC 1005
                            funcAddtoReportPerson(id_person, id_source, 1005, familyname);
                        } else if (standard_code.equals(SC_X)) {

                            // EC 1009
                            funcAddtoReportPerson(id_person, id_source, 1009, familyname);

                            writerFamilyname.write(id_person + "," + familyname.toLowerCase() + "\n");
                        } else {

                            // EC 1010
                            funcAddtoReportPerson(id_person, id_source, 1010, familyname);
                        }
                    } // Familyname does not exists in ref_familyname
                    else {

                        // EC 1002
                        funcAddtoReportPerson(id_person, id_source, 1002, familyname);

                        String nameNoSerriedSpaces = familyname.replaceAll(" [ ]+", " ");

                        // Family name contains two or more serried spaces?
                        if (!nameNoSerriedSpaces.equalsIgnoreCase(familyname)) {

                            // EC 1003
                            funcAddtoReportPerson(id_person, id_source, 1003, familyname);
                        }

                        String nameNoInvalidChars = funcCleanNaam(nameNoSerriedSpaces);

                        // Family name contains invalid chars ?
                        if (!nameNoSerriedSpaces.equalsIgnoreCase(nameNoInvalidChars)) {

                            // EC 1004
                            funcAddtoReportPerson(id_person, id_source, 1004, familyname);
                        }

                        // check if name has prepieces
                        String nameNoPrePiece = funcNamePrepiece(nameNoInvalidChars, id_person);

                        // Family name contains invalid chars ?
                        if (!nameNoPrePiece.equalsIgnoreCase(nameNoInvalidChars)) {

                            // EC 1008
                            funcAddtoReportPerson(id_person, id_source, 1008, familyname);
                        }

                        // Ckeck on Aliasses
                        String nameNoAlias = funcStandardAlias(id_person, id_source, nameNoPrePiece);

                        // Check on suffix
                        ArrayListNonCase sfxO = ttalSuffix.getArray("original");

                        for (int i = 0; i < sfxO.size(); i++) {

                            if (nameNoAlias.endsWith(" " + sfxO.get(i).toString())) {

                                // EC 1006
                                funcAddtoReportPerson(id_person, id_source, 1006, nameNoAlias);

                                nameNoAlias = nameNoAlias.replaceAll(" " + sfxO.get(i).toString(), "");

                                // Set alias
                                PersonC.updateQuery("suffix", sfxO.get(i).toString(), id_person);
                            }
                        }

                        // Clean name one more time
                        String nameNoSuffix = LinksSpecific.funcCleanSides(nameNoAlias);

                        // Check name in original
                        if (ttalFamilyname.originalExists(nameNoSuffix)) {

                            // get standard_code
                            String standard_code = ttalFamilyname.getStandardCodeByOriginal(nameNoSuffix);

                            // Check the standard code
                            if (standard_code.equals(SC_Y)) {

                                writerFamilyname.write(id_person + "," + ttalFamilyname.getStandardByOriginal(nameNoSuffix).toLowerCase() + "\n");
                            } else if (standard_code.equals(SC_I)) {

                                // EC 1000
                                funcAddtoReportPerson(id_person, id_source, 1000, nameNoSuffix);

                                writerFamilyname.write(id_person + "," + ttalFamilyname.getStandardByOriginal(nameNoSuffix).toLowerCase() + "\n");
                            } else if (standard_code.equals(SC_N)) {

                                // EC 1005
                                funcAddtoReportPerson(id_person, id_source, 1005, nameNoSuffix);
                            } else if (standard_code.equals(SC_X)) {

                                // EC 1009
                                funcAddtoReportPerson(id_person, id_source, 1009, nameNoSuffix);

                                writerFamilyname.write(id_person + "," + nameNoSuffix.toLowerCase() + "\n");
                            } else {
                                // EC 1010
                                funcAddtoReportPerson(id_person, id_source, 1010, nameNoSuffix);
                            }
                        } else {
                            // Familie is nieuw en wordt toegevoegd
                            ttalFamilyname.addOriginal(nameNoSuffix);

                            // EC 1009
                            funcAddtoReportPerson(id_person, id_source, 1009, nameNoSuffix);

                            writerFamilyname.write(id_person + "," + nameNoSuffix.trim().toLowerCase() + "\n");

                        }
                    }
                } // Familynaem empty
                else {

                    // EC 1001
                    funcAddtoReportPerson(id_person, id_source, 1001, "");
                }
            }
            con.close();
            rsFamilyname.close();
        } catch (Exception e) {
            funcShowMessage(counter + " An error occured while cleaning familyname: " + e.getMessage(), false, true);
        }
    }

    /**
     * @param sourceNo
     * @throws Exception
     */
    public void funcStandardPrepiece(String sourceNo) {

        int counter = 0;
        int step = 1000;
        int stepstate = step;

        try {

            String startQuery;
            String id_source;

            if (sourceNo.isEmpty()) {
                startQuery = "SELECT id_person , prefix FROM person_o" + bronFilter + " AND prefix <> ''";
                id_source = this.bronNr + "";
            } else {
                startQuery = "SELECT id_person , prefix FROM person_o WHERE id_source = " + sourceNo + " AND prefix <> ''";
                id_source = sourceNo;
            }

            // create connection
            Connection con = getConnection("links_original");
            con.isReadOnly();

            // Read family names from table
            ResultSet rsPrepiece = con.createStatement().executeQuery(startQuery);
            con.createStatement().close();

            // Get count
            rsPrepiece.last();

            int count = rsPrepiece.getRow();

            rsPrepiece.beforeFirst();

            while (rsPrepiece.next()) {

                // Create lists
                String listPF = "";
                String listTO = "";
                String listTN = "";

                counter++;

                if (counter == stepstate) {
                    funcShowMessage(counter + " of " + count, true, true);
                    stepstate += step;
                }

                // test
                if (counter != 398798) {
                    continue;
                }

                int id_person = rsPrepiece.getInt("id_person");
                String prepiece = rsPrepiece.getString("prefix").toLowerCase();

                // clean 
                prepiece = funcCleanNaam(prepiece);

                // Split prefix
                String[] prefixes = prepiece.split(" ");

                for (String part : prefixes) {

                    // Does Prefix exists in ref table
                    if (ttalPrepiece.originalExists(part)) {

                        String standard_code = ttalPrepiece.getStandardCodeByOriginal(part);
                        String prefix = ttalPrepiece.getColumnByOriginal("prefix", part);
                        String title_noble = ttalPrepiece.getColumnByOriginal("title_noble", part);
                        String title_other = ttalPrepiece.getColumnByOriginal("title_other", part);

                        // standard code x
                        if (standard_code.equals(SC_X)) {
                            // EC 81
                            funcAddtoReportPerson(id_person, id_source, 81, part);

                            listPF += part + " ";
                        } else if (standard_code.equals(SC_N)) {
                            // EC 83
                            funcAddtoReportPerson(id_person, id_source, 83, part);
                        } else if (standard_code.equals(SC_I)) {

                            // EC 85
                            funcAddtoReportPerson(id_person, id_source, 85, part);

                            if (prefix != null && !prefix.isEmpty()) {
                                listPF += prefix + " ";
                            } else if (title_noble != null && !title_noble.isEmpty()) {
                                listTN += title_noble + " ";
                            } else if (title_other != null && !title_other.isEmpty()) {
                                listTO += title_other + " ";
                            }
                        } else if (standard_code.equals(SC_Y)) {

                            if (prefix != null && !prefix.isEmpty()) {
                                listPF += prefix + " ";
                            } else if (title_noble != null && !title_noble.isEmpty()) {
                                listTN += title_noble + " ";
                            } else if (title_other != null && !title_other.isEmpty()) {
                                listTO += title_other + " ";
                            }
                        } else {
                            // Standard_code invalid
                            funcAddtoReportPerson(id_person, id_source, 89, part);
                        }
                    } else { // Prefix not in ref
                        funcAddtoReportPerson(id_person, id_source, 81, part);

                        // Add Prefix
                        ttalPrepiece.addOriginal(part);

                        // Add to list
                        listPF += part + " ";

                    }
                }

                // write lists to person_c
                if (!listTN.isEmpty()) {
                    conCleaned.runQuery(PersonC.updateQuery("title_noble", listTN.substring(0, (listTN.length() - 1)), id_person));
                }
                if (!listTO.isEmpty()) {
                    conCleaned.runQuery(PersonC.updateQuery("title_other", listTO.substring(0, (listTO.length() - 1)), id_person));
                }
                if (!listPF.isEmpty()) {
                    conCleaned.runQuery(PersonC.updateQuery("prefix", listPF.substring(0, (listPF.length() - 1)), id_person));
                }
            }

            // Free Resources
            rsPrepiece.close();
            con.close();
        } catch (Exception e) {
            funcShowMessage(counter + " An error occured while cleaning Prepiece: " + e.getMessage(), false, true);
        }
    }

    /**
     * @param bronnrsourceNo
     */
    public void funcStandardSuffix(String bronnrsourceNo) {

        int counter = 0;
        int step = 1000;
        int stepstate = step;

        try {
            String startQuery;
            String id_source;

            if (bronnrsourceNo.isEmpty()) {
                startQuery = "SELECT id_person , suffix FROM person_o" + bronFilter + " AND suffix <> ''";
                id_source = this.bronNr + "";
            } else {
                startQuery = "SELECT id_person , suffix FROM person_o WHERE id_source = " + bronnrsourceNo + " AND suffix <> ''";
                id_source = bronnrsourceNo;
            }

            // create connection
            Connection con = getConnection("links_original");
            con.isReadOnly();

            // Read family names from table
            ResultSet rsSuffix = con.createStatement().executeQuery(startQuery);
            con.createStatement().close();

            // Get count
            rsSuffix.last();

            int count = rsSuffix.getRow();

            rsSuffix.beforeFirst();

            while (rsSuffix.next()) {

                counter++;
                if (counter == stepstate) {
                    funcShowMessage(counter + " of " + count, true, true);
                    stepstate += step;
                }

                int id_person = rsSuffix.getInt("id_person");
                String suffix = rsSuffix.getString("suffix").toLowerCase();

                suffix = funcCleanNaam(suffix);

                // Controleer of deze voorkomt in ref tabel
                if (ttalSuffix.originalExists(suffix)) {

                    String standard_code = ttalSuffix.getStandardCodeByOriginal(suffix);

                    if (standard_code.equals(SC_X)) {

                        // EC 71
                        funcAddtoReportPerson(id_person, id_source, 71, suffix);

                        String query = PersonC.updateQuery("suffix", suffix, id_person);
                        conCleaned.runQuery(query);
                    } else if (standard_code.equals(SC_N)) {

                        // EC 73
                        funcAddtoReportPerson(id_person, id_source, 73, suffix);

                    } else if (standard_code.equals(SC_I)) {

                        // EC 74
                        funcAddtoReportPerson(id_person, id_source, 75, suffix);

                        String query = PersonC.updateQuery("suffix", suffix, id_person);
                        conCleaned.runQuery(query);

                    } else if (standard_code.equals(SC_Y)) {

                        String query = PersonC.updateQuery("suffix", suffix, id_person);
                        conCleaned.runQuery(query);
                    } else {

                        // EC 75
                        funcAddtoReportPerson(id_person, id_source, 79, suffix);
                    }
                } // Standard code x
                else {

                    // EC 71
                    funcAddtoReportPerson(id_person, id_source, 71, suffix);

                    ttalSuffix.addOriginal(suffix);

                    String query = PersonC.updateQuery("suffix", suffix, id_person);
                    conCleaned.runQuery(query);

                }
            }

            // Free resources
            rsSuffix.close();
            con.close();

        } catch (Exception e) {
            funcShowMessage(counter + " An error occured while cleaning Suffix: " + e.getMessage(), false, true);
        }
    }

    /**
     * @param sourceNo
     */
    public void funcStandardRegistrationLocation(String sourceNo) {

        String startQuery;
        String id_source;

        if (sourceNo.isEmpty()) {
            startQuery = "SELECT id_registration , registration_location FROM registration_o" + bronFilter;
            id_source = this.bronNr + "";
        } else {
            startQuery = "SELECT id_registration , registration_location FROM registration_o WHERE id_source = " + sourceNo;
            id_source = sourceNo;
        }

        try {
            ResultSet rs = conOriginal.runQueryWithResult(startQuery);

            // Call funcStandardLocation
            funcStandardLocation(rs, "id_registration", "registration_location", "registration_location_no", id_source, TableType.REGISTRATION);
        } catch (Exception e) {
            funcShowMessage(e.getMessage(), false, true);
        }
    }

    /**
     * @param sourceNo
     */
    public void funcStandardBirthLocation(String sourceNo) {

        String startQuery;
        String id_source;

        if (sourceNo.isEmpty()) {
            startQuery = "SELECT id_person , birth_location FROM person_o" + bronFilter + " AND birth_location <> ''";
            id_source = this.bronNr + "";
        } else {
            startQuery = "SELECT id_person , birth_location FROM person_o WHERE id_source = " + sourceNo + " AND birth_location <> ''";
            id_source = sourceNo;
        }

        try {
            ResultSet rs = conOriginal.runQueryWithResult(startQuery);

            funcStandardLocation(rs, "id_person", "birth_location", "birth_location", id_source, TableType.PERSON);
        } catch (Exception e) {
            funcShowMessage(e.getMessage(), false, true);
        }
    }

    /**
     * @param sourceNo
     */
    public void funcStandardMarLocation(String sourceNo) {

        String startQuery;
        String id_source;

        if (sourceNo.isEmpty()) {
            startQuery = "SELECT id_person , mar_location FROM person_o" + bronFilter + " AND mar_location <> ''";
            id_source = this.bronNr + "";
        } else {
            startQuery = "SELECT id_person , mar_location FROM person_o WHERE id_source = " + sourceNo + " AND mar_location <> ''";
            id_source = sourceNo;
        }

        try {
            ResultSet rs = conOriginal.runQueryWithResult(startQuery);
            funcStandardLocation(rs, "id_person", "mar_location", "mar_location", id_source, TableType.PERSON);
        } catch (Exception e) {
            funcShowMessage(e.getMessage(), false, true);
        }
    }

    /**
     * @param sourceNo
     */
    public void funcStandardDeathLocation(String sourceNo) {

        String startQuery;
        String id_source;

        if (sourceNo.isEmpty()) {
            startQuery = "SELECT id_person , death_location FROM person_o" + bronFilter + " AND death_location <> ''";
            id_source = this.bronNr + "";
        } else {
            startQuery = "SELECT id_person , death_location FROM person_o WHERE id_source = " + sourceNo + " AND death_location <> ''";
            id_source = sourceNo;
        }

        try {
            ResultSet rs = conOriginal.runQueryWithResult(startQuery);
            funcStandardLocation(rs, "id_person", "death_location", "death_location", id_source, TableType.PERSON);
        } catch (Exception e) {
            funcShowMessage(e.getMessage(), false, true);
        }
    }

    /**
     * @param rs
     * @param idFieldO
     * @param locationFieldO
     * @param locationFieldC
     * @param id_source
     * @param tt
     */
    private void funcStandardLocation(ResultSet rs, String idFieldO, String locationFieldO, String locationFieldC, String id_source, TableType tt) throws Exception {

        int counter = 0;
        int step = 1000;
        int stepstate = step;

        try {
            while (rs.next()) {

                counter++;
                if (counter == stepstate) {
                    funcShowMessage(counter + "", true, true);
                    stepstate += step;
                }

                int id = rs.getInt(idFieldO);
                String location = rs.getString(locationFieldO);

                if (location != null && !location.isEmpty()) {

                    location = location.toLowerCase();

                    if (ttalLocation.originalExists(location)) {

                        String nieuwCode = ttalLocation.getStandardCodeByOriginal(location);

                        if (nieuwCode == null ? SC_X == null : (nieuwCode.equals(SC_X))) {

                            // EC 91
                            if (tt == TableType.REGISTRATION) {
                                funcAddtoReportRegistration(id, id_source, 91, location);
                                String query = RegistrationC.updateIntQuery(locationFieldC, "10010", id);
                                conCleaned.runQuery(query);
                            } else {
                                funcAddtoReportPerson(id, id_source, 91, location);
                                String query = PersonC.updateIntQuery(locationFieldC, "10010", id);
                                conCleaned.runQuery(query);
                            }
                        } else if (nieuwCode == null ? SC_N == null : nieuwCode.equals(SC_N)) {

                            // EC 93
                            if (tt == TableType.REGISTRATION) {
                                funcAddtoReportRegistration(id, id_source, 91, location);
                            } else {
                                funcAddtoReportPerson(id, id_source, 93, location);
                            }
                        } else if (nieuwCode == null ? SC_I == null : nieuwCode.equals(SC_I)) {

                            // EC 95
                            if (tt == TableType.REGISTRATION) {
                                funcAddtoReportRegistration(id, id_source, 95, location);
                                String locationnumber = ttalLocation.getColumnByOriginal("location_no", location);
                                String query = RegistrationC.updateIntQuery(locationFieldC, locationnumber, id);
                                conCleaned.runQuery(query);
                            } else {
                                funcAddtoReportPerson(id, id_source, 95, location);
                                String locationnumber = ttalLocation.getColumnByOriginal("location_no", location);
                                String query = PersonC.updateIntQuery(locationFieldC, locationnumber, id);
                                conCleaned.runQuery(query);
                            }

                            String locationnumber = ttalLocation.getColumnByOriginal("location_no", location);

                        } else if (nieuwCode == null ? SC_Y == null : nieuwCode.equals(SC_Y)) {

                            if (tt == TableType.REGISTRATION) {
                                String locationnumber = ttalLocation.getColumnByOriginal("location_no", location);
                                String query = RegistrationC.updateIntQuery(locationFieldC, locationnumber, id);
                                conCleaned.runQuery(query);
                            } else {
                                String locationnumber = ttalLocation.getColumnByOriginal("location_no", location);
                                String query = PersonC.updateIntQuery(locationFieldC, locationnumber, id);
                                conCleaned.runQuery(query);
                            }


                        } else {

                            // EC 99
                            if (tt == TableType.REGISTRATION) {
                                funcAddtoReportRegistration(id, id_source, 99, location);
                            } else {
                                funcAddtoReportPerson(id, id_source, 99, location);
                            }
                        }
                    } else {

                        // EC 91
                        if (tt == TableType.REGISTRATION) {
                            funcAddtoReportRegistration(id, id_source, 91, location);
                            String query = RegistrationC.updateIntQuery(locationFieldC, "10010", id);
                            conCleaned.runQuery(query);
                        } else {
                            funcAddtoReportPerson(id, id_source, 91, location);
                            String query = PersonC.updateIntQuery(locationFieldC, "10010", id);
                            conCleaned.runQuery(query);
                        }
                        ttalLocation.addOriginal(location);
                    }
                }
            }
        } catch (Exception e) {
            throw new Exception(counter + " An error occured while cleaning Location: " + e.getMessage());
        }
    }

    /**
     * @param sourceNo
     */
    public void funcStandardSex(String sourceNo) {

        int counter = 0;
        int step = 1000;
        int stepstate = step;

        try {

            String startQuery;
            String id_source;

            if (sourceNo.isEmpty()) {
                startQuery = "SELECT id_person , sex FROM person_o" + bronFilter;
                id_source = this.bronNr + "";
            } else {
                startQuery = "SELECT id_person , sex FROM person_o WHERE id_source = " + sourceNo;
                id_source = sourceNo;
            }

            // Get gender
            ResultSet rsGeslacht = conOriginal.runQueryWithResult(startQuery);

            while (rsGeslacht.next()) {

                counter++;
                if (counter == stepstate) {
                    funcShowMessage(counter + "", true, true);
                    stepstate += step;
                }

                int id_person = rsGeslacht.getInt("id_person");
                String sex = rsGeslacht.getString("sex");

                // Check presence of the gender
                if (sex != null && !sex.isEmpty()) {

                    // Check presence in
                    if (ttalStatusSex.originalExists(sex)) {

                        String nieuwCode = ttalStatusSex.getStandardCodeByOriginal(sex);

                        if (nieuwCode.equals(SC_X)) {

                            // EC 31
                            funcAddtoReportPerson(id_person, id_source, 31, sex);

                            String query = PersonC.updateQuery("sex", sex, id_person);
                            conCleaned.runQuery(query);
                        } else if (nieuwCode.equals(SC_N)) {
                            // EC 33
                            funcAddtoReportPerson(id_person, id_source, 33, sex);
                        } else if (nieuwCode.equals(SC_I)) {

                            // EC 35
                            funcAddtoReportPerson(id_person, id_source, 35, sex);

                            String query = PersonC.updateQuery("sex", ttalStatusSex.getColumnByOriginal("standard_sex", sex), id_person);
                            conCleaned.runQuery(query);
                        } else if (nieuwCode.equals(SC_Y)) {

                            String query = PersonC.updateQuery("sex", ttalStatusSex.getColumnByOriginal("standard_sex", sex), id_person);
                            conCleaned.runQuery(query);
                        } else {
                            // Invalid standard code
                            // EC 39
                            funcAddtoReportPerson(id_person, id_source, 39, sex);
                        }
                    } else {

                        // EC 33
                        funcAddtoReportPerson(id_person, id_source, 31, sex);

                        // Add new Sex
                        ttalStatusSex.addOriginal(sex);

                        String query = PersonC.updateQuery("sex", sex, id_person);
                        conCleaned.runQuery(query);
                    }
                }
            }
        } catch (Exception e) {
            funcShowMessage(counter + " An error occured while cleaning Sex: " + e.getMessage(), false, true);
        }
    }

    /**
     * @param sourceNo
     */
    public void funcStandardStatusSex(String sourceNo) {

        int counter = 0;
        int step = 1000;
        int stepstate = step;

        try {
            String startQuery;
            String id_source;

            if (sourceNo.isEmpty()) {
                startQuery = "SELECT id_person , sex , civil_status FROM person_o" + bronFilter + " and civil_status is not null ";
                id_source = this.bronNr + "";
            } else {
                startQuery = "SELECT id_person , sex , civil_status FROM person_o WHERE id_source = " + sourceNo;
                id_source = sourceNo;
            }

            ResultSet rsStaat = conOriginal.runQueryWithResult(startQuery);

            while (rsStaat.next()) {

                counter++;

                if (counter == stepstate) {
                    funcShowMessage(counter + "", true, true);
                    stepstate += step;
                }


                int id_person = rsStaat.getInt("id_person");
                String sex = rsStaat.getString("sex");
                String civil_status = rsStaat.getString("civil_status");

                if (civil_status != null && !civil_status.isEmpty()) {

                    // Check ref
                    if (ttalStatusSex.originalExists(civil_status)) {

                        String nieuwCode = this.ttalStatusSex.getStandardCodeByOriginal(civil_status);

                        if (nieuwCode.equals(SC_X)) {

                            // EC 61
                            funcAddtoReportPerson(id_person, id_source, 61, civil_status);

                            String query = PersonC.updateQuery("civil_status", civil_status, id_person);
                            conCleaned.runQuery(query);
                        } else if (nieuwCode.equals(SC_N)) {
                            // EC 63
                            funcAddtoReportPerson(id_person, id_source, 63, civil_status);
                        } else if (nieuwCode.equals(SC_I)) {

                            // EC 65
                            funcAddtoReportPerson(id_person, id_source, 65, civil_status);

                            String query = PersonC.updateQuery("civil_status", ttalStatusSex.getColumnByOriginal("standard_civilstatus", civil_status), id_person);
                            conCleaned.runQuery(query);

                            // Extra check on sex
                            if (sex != null && !sex.isEmpty()) {

                                if (!sex.equalsIgnoreCase(this.ttalStatusSex.getColumnByOriginal("standard_sex", civil_status))) {
                                    // EC 68
                                    funcAddtoReportPerson(id_person, id_source, 68, civil_status);
                                }
                            } // Sex is empty
                            else {

                                String geslachtQuery = PersonC.updateQuery("sex", ttalStatusSex.getColumnByOriginal("standard_sex", civil_status), id_person);
                                conCleaned.runQuery(geslachtQuery);

                            }

                            String geslachtQuery = PersonC.updateQuery("civil_status", ttalStatusSex.getColumnByOriginal("standard_civilstatus", civil_status), id_person);
                            conCleaned.runQuery(geslachtQuery);

                        } else if (nieuwCode.equals(SC_Y)) {


                            String query = PersonC.updateQuery("civil_status", ttalStatusSex.getColumnByOriginal("standard_civilstatus", civil_status), id_person);
                            conCleaned.runQuery(query);

                            // Extra check on sex
                            if (sex != null && !sex.isEmpty()) {

                                if (!sex.equalsIgnoreCase(this.ttalStatusSex.getColumnByOriginal("standard_sex", civil_status))) {
                                    // EC 68
                                    funcAddtoReportPerson(id_person, id_source, 68, civil_status);
                                }
                            } // Sex is empty
                            else {

                                String geslachtQuery = PersonC.updateQuery("sex", ttalStatusSex.getColumnByOriginal("standard_sex", civil_status), id_person);
                                conCleaned.runQuery(geslachtQuery);

                            }

                            String geslachtQuery = PersonC.updateQuery("civil_status", ttalStatusSex.getColumnByOriginal("standard_civilstatus", civil_status), id_person);
                            conCleaned.runQuery(geslachtQuery);
                        } else {

                            // Invalid SC
                            // EC 69
                            funcAddtoReportPerson(id_person, id_source, 69, civil_status);
                        }
                    } // add to ref
                    else {
                        // EC 61
                        funcAddtoReportPerson(id_person, id_source, 61, civil_status);

                        // Add new Status
                        ttalStatusSex.addOriginal(civil_status);

                        // Write to Person
                        String query = PersonC.updateQuery("civil_status", civil_status, id_person);
                        conCleaned.runQuery(query);
                    }
                }
            }
        } catch (Exception e) {
            funcShowMessage(counter + " An error occured while cleaning Civil Status: " + e.getMessage(), false, true);
        }
    }

    //ROLE
    private void funcStandardRole() {

        String query = " UPDATE links_original.person_o, links_cleaned.person_c, links_general.ref_role "
                + "SET "
                + "links_cleaned.person_c.role = links_general.ref_role.role_nr "
                + "WHERE links_original.person_o.role = links_general.ref_role.original AND "
                + "links_original.person_o.id_person = links_cleaned.person_c.id_person; ";

        try {
            conCleaned.runQuery(query);
        } catch (Exception e) {
            funcShowMessage("An error occured while running funcStandardRole: " + e.getMessage(), false, true);
        }


    }

    /**
     * @param sourceNo
     */
    /*
    public void funcStandardType(String sourceNo) {
    
    int counter = 0;
    int step = 1000;
    int stepstate = step;
    
    try {
    
    String startQuery;
    String id_source;
    
    if (sourceNo.isEmpty()) {
    startQuery = "SELECT id_registration , registration_type FROM registration_o" + bronFilter;
    id_source = this.bronNr + "";
    } else {
    startQuery = "SELECT id_registration , registration_type FROM registration_o WHERE id_source = " + sourceNo;
    id_source = sourceNo;
    }
    
    // Get types
    ResultSet type = conOriginal.runQueryWithResult(startQuery);
    
    while (type.next()) {
    
    counter++;
    if (counter == stepstate) {
    funcShowMessage(counter + "", true, true);
    stepstate += step;
    }
    
    int id_registration = type.getInt("id_registration");
    String registration_type = type.getString("registration_type").toLowerCase();
    
    // check is it is empty
    if (registration_type != null && !registration_type.isEmpty()) {
    
    // check ref
    if (ttalRegistration.originalExists(registration_type)) {
    
    String nieuwCode = ttalRegistration.getStandardCodeByOriginal(registration_type);
    
    if (nieuwCode.equals(SC_X)) {
    
    // EC 51
    funcAddtoReportRegistration(id_registration, id_source, 51, registration_type);
    
    String query = RegistrationC.updateQuery("registration_type", registration_type, id_registration);
    conCleaned.runQuery(query);
    } else if (nieuwCode.equals(SC_N)) {
    // EC 53
    funcAddtoReportRegistration(id_registration, id_source, 53, registration_type);
    } else if (nieuwCode.equals(SC_I)) {
    
    // EC 55
    funcAddtoReportRegistration(id_registration, id_source, 55, registration_type);
    
    String query = RegistrationC.updateQuery("registration_type", ttalRegistration.getStandardByOriginal(registration_type), id_registration);
    conCleaned.runQuery(query);
    
    } else if (nieuwCode.equals(SC_Y)) {
    
    String query = RegistrationC.updateQuery("registration_type", ttalRegistration.getStandardByOriginal(registration_type), id_registration);
    conCleaned.runQuery(query);
    } else {
    
    // invalid SC
    // EC 59
    funcAddtoReportRegistration(id_registration, id_source, 59, registration_type);
    }
    } // standardcode x
    else {
    
    // EC 51
    funcAddtoReportRegistration(id_registration, id_source, 51, registration_type);
    
    // add to ref
    ttalRegistration.addOriginal(registration_type);
    
    // update person
    String query = RegistrationC.updateQuery("registration_type", registration_type.length() < 20 ? registration_type : registration_type.substring(0, 20), id_registration);
    conCleaned.runQuery(query);
    }
    }
    }
    } catch (Exception e) {
    funcShowMessage(counter + " An error occured while cleaning Registration Type: " + e.getMessage(), false, true);
    }
    }
     */
    public void funcStandardType(String sourceNo) {

        int counter = 0;
        int step = 1000;
        int stepstate = step;

        try {

            String startQuery;
            String id_source;

            if (sourceNo.isEmpty()) {
                startQuery = "SELECT id_registration, registration_maintype, registration_type FROM registration_o" + bronFilter;
                id_source = this.bronNr + "";
            } else {
                startQuery = "SELECT id_registration, registration_maintype, registration_type FROM registration_o WHERE id_source = " + sourceNo;
                id_source = sourceNo;
            }

            // Get types node-152.dev.socialhistoryservices.org
            ResultSet type = conOriginal.runQueryWithResult(startQuery);

            while (type.next()) {

                counter++;
                if (counter == stepstate) {
                    funcShowMessage(counter + "", true, true);
                    stepstate += step;
                }

                int id_registration = type.getInt("id_registration");
                int registration_maintype = type.getInt("registration_maintype");
                String registration_type = type.getString("registration_type") != null ? type.getString("registration_type").toLowerCase() : "";

                // check ref database
                ResultSet ref = conGeneral.runQueryWithResult("SELECT * FROM ref_registration WHERE main_type = " + registration_maintype + " AND original = '" + registration_type + "'");

                // check ref
                if (ref.next()) {

                    String nieuwCode = ref.getString("standard_code").toLowerCase();

                    if (nieuwCode.equals(SC_X)) {

                        // EC 51
                        funcAddtoReportRegistration(id_registration, id_source, 51, registration_type);

                        String query = RegistrationC.updateQuery("registration_type", registration_type, id_registration);

                        conCleaned.runQuery(query);
                    } else if (nieuwCode.equals(SC_N)) {
                        // EC 53
                        funcAddtoReportRegistration(id_registration, id_source, 53, registration_type);
                    } else if (nieuwCode.equals(SC_I)) {

                        // EC 55
                        funcAddtoReportRegistration(id_registration, id_source, 55, registration_type);

                        String query = RegistrationC.updateQuery("registration_type", ref.getString("standard").toLowerCase(), id_registration);

                        conCleaned.runQuery(query);

                    } else if (nieuwCode.equals(SC_Y)) {

                        String query = RegistrationC.updateQuery("registration_type", ref.getString("standard").toLowerCase(), id_registration);
                        conCleaned.runQuery(query);
                    } else {

                        // invalid SC
                        // EC 59
                        funcAddtoReportRegistration(id_registration, id_source, 59, registration_type);
                    }
                } // standardcode x
                else {

                    // EC 51
                    funcAddtoReportRegistration(id_registration, id_source, 51, registration_type);

                    // add to ref
                    conGeneral.runQuery("INSERT INTO ref_registration(original, main_type, standard_code) VALUES ('" + registration_type + "'," + registration_maintype + ",'x')");

                    // update person
                    String query = RegistrationC.updateQuery("registration_type", registration_type.length() < 50 ? registration_type : registration_type.substring(0, 50), id_registration);
                    conCleaned.runQuery(query);
                }
            }
        } catch (Exception e) {
            funcShowMessage(counter + " An error occured while cleaning Registration Type: " + e.getMessage(), false, true);
        }
    }

    /**
     * @param sourceNo
     */
    public void funcStandardRegistrationDate(String sourceNo) {

        int counter = 0;
        int step = 1000;
        int stepstate = step;

        try {

            String startQuery;
            String id_source;

            if (sourceNo.isEmpty()) {
                startQuery = "SELECT id_registration , registration_date FROM registration_o" + bronFilter;
                id_source = this.bronNr + "";
            } else {
                startQuery = "SELECT id_registration , registration_date FROM registration_o WHERE id_source = " + sourceNo;
                id_source = sourceNo;
            }

            ResultSet rs = conOriginal.runQueryWithResult(startQuery);

            while (rs.next()) {

                counter++;
                if (counter == stepstate) {
                    funcShowMessage(counter + "", true, true);
                    stepstate += step;
                }

                // Get Opmerking
                int id_registration = rs.getInt("id_registration");
                String registration_date = rs.getString("registration_date");

                if (registration_date == null) {

                    // EC 202
                    funcAddtoReportRegistration(id_registration, id_source, 202, "");

                    continue;
                }

                DateYearMonthDaySet dymd = LinksSpecific.devideCheckDate(registration_date);

                if (dymd.isValidDate()) {

                    String query = "UPDATE registration_c"
                            + " SET registration_c.registration_date = '" + registration_date + "' , "
                            + "registration_c.registration_day = " + dymd.getDay() + " , "
                            + "registration_c.registration_month = " + dymd.getMonth() + " , "
                            + "registration_c.registration_year = " + dymd.getYear()
                            + " WHERE registration_c.id_registration = " + id_registration;

                    conCleaned.runQuery(query);
                } // Error occured
                else {

                    // EC 201
                    funcAddtoReportRegistration(id_registration, id_source, 201, dymd.getReports());

                    String query = "UPDATE registration_c"
                            + " SET registration_c.registration_date = '" + registration_date + "' , "
                            + "registration_c.registration_day = " + dymd.getDay() + " , "
                            + "registration_c.registration_month = " + dymd.getMonth() + " , "
                            + "registration_c.registration_year = " + dymd.getYear()
                            + " WHERE registration_c.id_registration = " + id_registration;

                    conCleaned.runQuery(query);
                }
            }
            rs = null;
        } catch (Exception e) {
            funcShowMessage(counter + " An error occured while cleaning Registration date: " + e.getMessage(), false, true);
        }
    }

    /**
     * @param type
     */
    public void funcStandardDate(String type) {

        // Step vars
        int counter = 0;
        int step = 10000;
        int stepstate = step;

        try {
            String startQuery;

            startQuery = "SELECT id_person , id_source , " + type + "_date FROM person_o WHERE " + type + "_date is not null";

            ResultSet rs = conOriginal.runQueryWithResult(startQuery);

            while (rs.next()) {

                // GUI info
                counter++;
                if (counter == stepstate) {
                    funcShowMessage(counter + "", true, true);
                    stepstate += step;
                }

                int id_person = rs.getInt("id_person");
                int id_source = rs.getInt("id_source");
                String date = rs.getString(type + "_date");

                if (date.isEmpty()) {
                    continue;
                }

                DateYearMonthDaySet dymd = LinksSpecific.devideCheckDate(date);

                if (dymd.isValidDate()) {
                    String query = ""
                            + "UPDATE person_c "
                            + "SET person_c." + type + "_date = '" + dymd.getDay() + "-" + dymd.getMonth() + "-" + dymd.getYear() + "' , "
                            + "person_c." + type + "_day = " + dymd.getDay() + " , "
                            + "person_c." + type + "_month = " + dymd.getMonth() + " , "
                            + "person_c." + type + "_year = " + dymd.getYear() + " , "
                            + "person_c." + type + "_date_valid = 1 "
                            + "WHERE person_c.id_person = " + id_person;

                    conCleaned.runQuery(query);
                } else {

                    // EC 211
                    funcAddtoReportPerson(id_person, id_source + "", 211, dymd.getReports());

                    String query = ""
                            + "UPDATE person_c "
                            + "SET person_c." + type + "_date = '" + date + "' , "
                            + "person_c." + type + "_day = " + dymd.getDay() + " , "
                            + "person_c." + type + "_month = " + dymd.getMonth() + " , "
                            + "person_c." + type + "_year = " + dymd.getYear() + " "
                            + "WHERE person_c.id_person = " + id_person;

                    conCleaned.runQuery(query);
                }
            }
            rs = null;
        } catch (Exception e) {
            funcShowMessage(counter + " An error occured while cleaning " + type + " date: " + e.getMessage(), false, true);
        }
    }

    /**
     *
     */
    public void funcFlagBirthDate() {

        String query1 = "UPDATE person_c, registration_c "
                + "SET "
                + "person_c.birth_date_flag = 2, "
                + "person_c.birth_date  = registration_c.registration_date , "
                + "person_c.birth_year  = registration_c.registration_year , "
                + "person_c.birth_month = registration_c.registration_month , "
                + "person_c.birth_day   = registration_c.registration_day "
                + "WHERE person_c.birth_date is null AND "
                + "registration_maintype = 1 AND "
                + "person_c.role = 1 AND "
                + "person_c.id_registration = registration_c.id_registration; ";


        String query2 = "UPDATE person_c, registration_c "
                + "SET "
                + "person_c.birth_date_flag = 3, "
                + "person_c.birth_date  = registration_c.registration_date , "
                + "person_c.birth_year  = registration_c.registration_year , "
                + "person_c.birth_month = registration_c.registration_month , "
                + "person_c.birth_day   = registration_c.registration_day "
                + "WHERE person_c.birth_date_valid = 0 AND "
                + "person_c.birth_date_flag = 0 AND "
                + "registration_maintype = 1 AND "
                + "person_c.role = 1 AND "
                + "person_c.id_registration = registration_c.id_registration; ";

        String query3 = "UPDATE person_c, registration_c "
                + "SET "
                + "person_c.birth_date_flag = 1 "
                + "WHERE person_c.birth_date_valid = 1 AND "
                + "registration_maintype = 1 AND "
                + "person_c.role = 1 AND "
                + "person_c.id_registration = registration_c.id_registration; ";

        try {
            conCleaned.runQuery(query1);
            conCleaned.runQuery(query2);
            conCleaned.runQuery(query3);
        } catch (Exception e) {
            funcShowMessage("An error occured while flagging Birth date: " + e.getMessage(), false, true);
        }
    }

    /**
     *
     */
    public void funcFlagMarriageDate() {
        String query1 = "UPDATE person_c, registration_c "
                + "SET "
                + "person_c.mar_date_flag = 2, "
                + "person_c.mar_date    = registration_c.registration_date , "
                + "person_c.mar_year    = registration_c.registration_year , "
                + "person_c.mar_month   = registration_c.registration_month , "
                + "person_c.mar_day     = registration_c.registration_day "
                + "WHERE "
                + "registration_maintype = 2 AND "
                + "person_c.mar_date is null AND "
                + "( ( person_c.role = 4 ) || ( person_c.role = 7 ) ) AND "
                + "person_c.id_registration = registration_c.id_registration; ";

        String query2 = "UPDATE person_c, registration_c "
                + "SET "
                + "person_c.mar_date_flag = 3, "
                + "person_c.mar_date    = registration_c.registration_date , "
                + "person_c.mar_year    = registration_c.registration_year , "
                + "person_c.mar_month   = registration_c.registration_month , "
                + "person_c.mar_day     = registration_c.registration_day "
                + "WHERE "
                + "registration_maintype = 2 AND "
                + "person_c.mar_date_valid = 0 AND "
                + "person_c.mar_date_flag = 0 AND "
                + "( ( person_c.role = 4 ) || ( person_c.role = 7 ) ) AND "
                + "person_c.id_registration = registration_c.id_registration; ";

        String query3 = "UPDATE person_c, registration_c "
                + "SET "
                + "person_c.mar_date_flag = 1 "
                + "WHERE "
                + "registration_maintype = 2 AND "
                + "person_c.mar_date_valid = 1 AND "
                + "( ( person_c.role = 4 ) || ( person_c.role = 7 ) ) AND "
                + "person_c.id_registration = registration_c.id_registration; ";

        try {

            conCleaned.runQuery(query1);
            conCleaned.runQuery(query2);
            conCleaned.runQuery(query3);

        } catch (Exception e) {
            funcShowMessage("An error occured while flagging Marriage date: " + e.getMessage(), false, true);
        }
    }

    /**
     *
     */
    public void funcFlagDeathDate() {

        String query1 = "UPDATE person_c, registration_c "
                + "SET "
                + "person_c.death_date_flag = 2, "
                + "person_c.death_date  = registration_c.registration_date , "
                + "person_c.death_year  = registration_c.registration_year , "
                + "person_c.death_month = registration_c.registration_month , "
                + "person_c.death_day   = registration_c.registration_day "
                + "WHERE person_c.death_date is null AND "
                + "registration_maintype = 3 AND "
                + "person_c.role = 10 AND "
                + "person_c.id_registration = registration_c.id_registration; ";

        String query2 = "UPDATE person_c, registration_c "
                + "SET "
                + "person_c.death_date_flag = 3, "
                + "person_c.death_date  = registration_c.registration_date , "
                + "person_c.death_year  = registration_c.registration_year , "
                + "person_c.death_month = registration_c.registration_month , "
                + "person_c.death_day   = registration_c.registration_day "
                + "WHERE person_c.death_date_flag = 0 AND "
                + "person_c.death_date_valid = 0 AND "
                + "registration_maintype = 3 AND "
                + "person_c.role = 10 AND "
                + "person_c.id_registration = registration_c.id_registration; ";

        String query3 = "UPDATE person_c, registration_c "
                + "SET "
                + "person_c.death_date_flag = 1 "
                + "WHERE person_c.death_date_valid = 1 AND "
                + "registration_maintype = 3 AND "
                + "person_c.role = 10 AND "
                + "person_c.id_registration = registration_c.id_registration; ";

        try {

            conCleaned.runQuery(query1);
            conCleaned.runQuery(query2);
            conCleaned.runQuery(query3);

        } catch (Exception e) {
            funcShowMessage("An error occured while flagging Death date: " + e.getMessage(), false, true);
        }
    }

    /**
     * @param type
     */
    public void funcStandardFlaggedDate(String type) {

        // Step vars
        int counter = 0;
        int step = 10000;
        int stepstate = step;

        try {
            String startQuery;

            startQuery = "SELECT id_person , id_source , " + type + "_date FROM person_c WHERE ( ( " + type + "_date_flag = 2 ) OR ( " + type + "_date_flag = 3 ) ) AND " + type + "_date is not null";

            ResultSet rs = conCleaned.runQueryWithResult(startQuery);

            while (rs.next()) {

                // GUI info
                counter++;
                if (counter == stepstate) {
                    funcShowMessage(counter + "", true, true);
                    stepstate += step;
                }

                int id_person = rs.getInt("id_person");
                int id_source = rs.getInt("id_source");
                String date = rs.getString(type + "_date");

                if (date.isEmpty()) {
                    continue;
                }

                DateYearMonthDaySet dymd = LinksSpecific.devideCheckDate(date);

                if (dymd.isValidDate()) {
                    String query = ""
                            + "UPDATE person_c "
                            + "SET person_c." + type + "_date = '" + date + "' , "
                            + "person_c." + type + "_day = " + dymd.getDay() + " , "
                            + "person_c." + type + "_month = " + dymd.getMonth() + " , "
                            + "person_c." + type + "_year = " + dymd.getYear() + " , "
                            + "person_c." + type + "_date_valid = 1 "
                            + "WHERE person_c.id_person = " + id_person;

                    conCleaned.runQuery(query);
                } else {

                    // EC 211
                    funcAddtoReportPerson(id_person, id_source + "", 211, dymd.getReports());

                    String query = ""
                            + "UPDATE person_c "
                            + "SET person_c." + type + "_date = '" + date + "' , "
                            + "person_c." + type + "_day = " + dymd.getDay() + " , "
                            + "person_c." + type + "_month = " + dymd.getMonth() + " , "
                            + "person_c." + type + "_year = " + dymd.getYear() + " "
                            + "WHERE person_c.id_person = " + id_person;

                    conCleaned.runQuery(query);
                }
            }
            rs = null;
        } catch (Exception e) {
            funcShowMessage(counter + " An error occured while cleaning " + type + " flagged date: " + e.getMessage(), false, true);
        }
    }

    /**
     * @throws Exception
     */
    private void funcMinMaxCorrectDate() throws Exception {

        String q1 = ""
                + "UPDATE person_c "
                + "SET "
                + "birth_date_min  = birth_date , "
                + "birth_date_max  = birth_date , "
                + "birth_year_min  = birth_year , "
                + "birth_year_max  = birth_year , "
                + "birth_month_min = birth_month , "
                + "birth_month_max = birth_month , "
                + "birth_day_min   = birth_day , "
                + "birth_day_max   = birth_day "
                + "WHERE "
                + "birth_date_valid = 1";

        String q2 = ""
                + "UPDATE person_c "
                + "SET "
                + "mar_date_min  = mar_date , "
                + "mar_date_max  = mar_date , "
                + "mar_year_min  = mar_year , "
                + "mar_year_max  = mar_year , "
                + "mar_month_min = mar_month , "
                + "mar_month_max = mar_month , "
                + "mar_day_min   = mar_day , "
                + "mar_day_max   = mar_day "
                + "WHERE "
                + "mar_date_valid = 1";

        String q3 = ""
                + "UPDATE person_c "
                + "SET "
                + "death_date_min  = death_date , "
                + "death_date_max  = death_date , "
                + "death_year_min  = death_year , "
                + "death_year_max  = death_year , "
                + "death_month_min = death_month , "
                + "death_month_max = death_month , "
                + "death_day_min   = death_day , "
                + "death_day_max   = death_day "
                + "WHERE "
                + "death_date_valid = 1";

        conCleaned.runQuery(q1);
        conCleaned.runQuery(q2);
        conCleaned.runQuery(q3);
    }

    /**
     * @throws Exception
     */
    private void funcCompleteMinMaxBirth() throws Exception {

        String q1 = ""
                + " UPDATE links_cleaned.person_c, links_general.ref_date_minmax"
                + " SET"
                + " mar_day_min     = birth_day ,"
                + " mar_day_max     = birth_day ,"
                + " mar_month_min   = birth_month ,"
                + " mar_month_max   = birth_month ,"
                + " mar_date_min    = CONCAT( birth_day , '-' , birth_month , '-' ,  birth_year + min_year ) ,"
                + " mar_date_max    = CONCAT( birth_day , '-' , birth_month , '-' ,  birth_year + max_year ) ,"
                + " mar_year_min    = birth_year + min_year ,"
                + " mar_year_max    = birth_year + max_year ,"
                + " mar_date_valid = 1"
                + " WHERE"
                + " links_cleaned.person_c.role             = 1 AND"
                + " links_cleaned.person_c.birth_date_valid = 1 AND"
                + " links_general.ref_date_minmax.role      = 1 AND"
                + " links_general.ref_date_minmax.maintype  = 1 AND"
                + " links_general.ref_date_minmax.date_type = 'marriage_date'";

        String q2 = ""
                + " UPDATE links_cleaned.person_c, links_general.ref_date_minmax"
                + " SET"
                + " death_day_min   = birth_day ,"
                + " death_day_max   = birth_day ,"
                + " death_month_min = birth_month ,"
                + " death_month_max = birth_month ,"
                + " death_date_min  = CONCAT( birth_day , '-' , birth_month , '-' ,  birth_year + min_year ) ,"
                + " death_date_max  = CONCAT( birth_day , '-' , birth_month , '-' ,  birth_year + max_year ) ,"
                + " death_year_min  = birth_year + min_year ,"
                + " death_year_max  = birth_year + max_year ,"
                + " death_date_valid = 1"
                + " WHERE"
                + " links_cleaned.person_c.role             = 1 AND"
                + " links_cleaned.person_c.birth_date_valid = 1 AND"
                + " links_general.ref_date_minmax.role      = 1 AND"
                + " links_general.ref_date_minmax.maintype  = 1 AND"
                + " links_general.ref_date_minmax.date_type = 'death_date'";

        conCleaned.runQuery(q1);
        conCleaned.runQuery(q2);
    }

    /**
     * @throws Exception
     */
    private void funcCompleteMinMaxMar() throws Exception {

        String q1 = ""
                + " UPDATE links_cleaned.person_c, links_general.ref_date_minmax"
                + " SET"
                + " birth_day_min     = mar_day ,"
                + " birth_day_max     = mar_day ,"
                + " birth_month_min   = mar_month ,"
                + " birth_month_max   = mar_month ,"
                + " birth_date_min    = CONCAT( mar_day , '-' , mar_month , '-' ,  (mar_year - age_year) + min_year ) ,"
                + " birth_date_max    = CONCAT( mar_day , '-' , mar_month , '-' ,  (mar_year - age_year) + max_year ) ,"
                + " birth_year_min    = (mar_year - age_year) + min_year ,"
                + " birth_year_max    = (mar_year - age_year) + max_year ,"
                + " birth_date_valid  = 1"
                + " WHERE"
                + " ( (links_cleaned.person_c.age_year is not null ) AND ( links_cleaned.person_c.age_year <> '') ) AND"
                + " links_cleaned.person_c.role = 4 AND"
                + " links_cleaned.person_c.mar_date_valid       = 1 AND"
                + " links_cleaned.person_c.birth_date_valid     = 0 AND"
                + " links_general.ref_date_minmax.role          = 4 AND"
                + " links_general.ref_date_minmax.maintype      = 2 AND"
                + " links_general.ref_date_minmax.age_reported  = 'y' AND"
                + " links_general.ref_date_minmax.date_type     = 'birth_date'";

        String q2 = ""
                + " UPDATE links_cleaned.person_c, links_general.ref_date_minmax"
                + " SET"
                + " birth_day_min     = mar_day ,"
                + " birth_day_max     = mar_day ,"
                + " birth_month_min   = mar_month ,"
                + " birth_month_max   = mar_month ,"
                + " birth_date_min    = CONCAT( mar_day , '-' , mar_month , '-' ,  (mar_year - age_year) + min_year ) ,"
                + " birth_date_max    = CONCAT( mar_day , '-' , mar_month , '-' ,  (mar_year - age_year) + max_year ) ,"
                + " birth_year_min    = (mar_year - age_year) + min_year ,"
                + " birth_year_max    = (mar_year - age_year) + max_year ,"
                + " birth_date_valid  = 1"
                + " WHERE"
                + " ( (links_cleaned.person_c.age_year is not null ) AND ( links_cleaned.person_c.age_year <> '') ) AND"
                + " links_cleaned.person_c.role = 7 AND"
                + " links_cleaned.person_c.mar_date_valid       = 1 AND"
                + " links_cleaned.person_c.birth_date_valid     = 0 AND"
                + " links_general.ref_date_minmax.role          = 7 AND"
                + " links_general.ref_date_minmax.maintype      = 2 AND"
                + " links_general.ref_date_minmax.age_reported  = 'y' AND"
                + " links_general.ref_date_minmax.date_type     = 'birth_date'";

        String q3 = ""
                + " UPDATE links_cleaned.person_c, links_general.ref_date_minmax"
                + " SET"
                + " death_day_min     = mar_day ,"
                + " death_day_max     = mar_day ,"
                + " death_month_min   = mar_month ,"
                + " death_month_max   = mar_month ,"
                + " death_date_min    = CONCAT( mar_day , '-' , mar_month , '-' ,  mar_year ) ,"
                + " death_date_max    = CONCAT( mar_day , '-' , mar_month , '-' ,  mar_year + ( max_year - age_year ) ) ,"
                + " death_year_min    = mar_year ,"
                + " death_year_max    = mar_year + ( max_year - age_year ) ,"
                + " death_date_valid  = 1"
                + " WHERE"
                + " ( (links_cleaned.person_c.age_year is not null ) AND ( links_cleaned.person_c.age_year <> '') ) AND"
                + " links_cleaned.person_c.role = 4 AND"
                + " links_cleaned.person_c.mar_date_valid       = 1 AND"
                + " links_general.ref_date_minmax.role          = 4 AND"
                + " links_general.ref_date_minmax.maintype      = 2 AND"
                + " links_general.ref_date_minmax.age_reported  = 'y' AND"
                + " links_general.ref_date_minmax.date_type     = 'death_date'";

        String q4 = ""
                + " UPDATE links_cleaned.person_c, links_general.ref_date_minmax"
                + " SET"
                + " death_day_min     = mar_day ,"
                + " death_day_max     = mar_day ,"
                + " death_month_min   = mar_month ,"
                + " death_month_max   = mar_month ,"
                + " death_date_min    = CONCAT( mar_day , '-' , mar_month , '-' ,  mar_year ) ,"
                + " death_date_max    = CONCAT( mar_day , '-' , mar_month , '-' ,  mar_year + ( max_year - age_year ) ) ,"
                + " death_year_min    = mar_year ,"
                + " death_year_max    = mar_year + ( max_year - age_year ) ,"
                + " death_date_valid  = 1 "
                + " WHERE"
                + " ( (links_cleaned.person_c.age_year is not null ) AND ( links_cleaned.person_c.age_year <> '') ) AND"
                + " links_cleaned.person_c.role = 7 AND"
                + " links_cleaned.person_c.mar_date_valid       = 1 AND"
                + " links_general.ref_date_minmax.role          = 7 AND"
                + " links_general.ref_date_minmax.maintype      = 2 AND"
                + " links_general.ref_date_minmax.age_reported  = 'y' AND"
                + " links_general.ref_date_minmax.date_type     = 'death_date'";

        conCleaned.runQuery(q1);
        conCleaned.runQuery(q2);
        conCleaned.runQuery(q3);
        conCleaned.runQuery(q4);
    }

    private void funcSetcomplete() throws Exception {

        String q = ""
                + " UPDATE links_cleaned.person_c"
                + " SET"
                + " valid_complete = 1"
                + " WHERE"
                + " birth_date_valid    = 1 AND"
                + " mar_date_valid      = 1 AND"
                + " death_date_valid    = 1";

        conCleaned.runQuery(q);

    }

    /**
     * @param sourceNo
     */
    public void funcStandardYearAge(String sourceNo) {

        int counter = 0;
        int step = 1000;
        int stepstate = step;

        try {

            String startQuery;
            String id_source;

            if (sourceNo.isEmpty()) {
                startQuery = "SELECT id_person , age_year FROM person_o" + bronFilter;
                id_source = this.bronNr + "";
            } else {
                startQuery = "SELECT id_person , age_year FROM person_o WHERE id_source = " + sourceNo;
                id_source = sourceNo;
            }

            ResultSet rs = conOriginal.runQueryWithResult(startQuery);

            while (rs.next()) {

                counter++;
                if (counter == stepstate) {
                    funcShowMessage(counter + "", true, true);
                    stepstate += step;
                }

                int id_person = rs.getInt("id_person");
                int year_age = rs.getInt("age_year");

                // check if null
                if (year_age != 0) {

                    if ((year_age > 0) && (year_age < 115)) {
                        String query = "UPDATE person_c"
                                + " SET person_c.age_year = '" + year_age + "'"
                                + " WHERE person_c.id_person = " + id_person;

                        conCleaned.runQuery(query);
                    } else {
                        // EC 241
                        funcAddtoReportPerson(id_person, id_source, 241, year_age + "");
                    }
                }
            }
            rs.close();
            rs = null;
        } catch (Exception e) {
            funcShowMessage(counter + " An error occured while cleaning Age Year: " + e.getMessage(), false, true);
        }
    }

    /**
     * @param SourceNo
     * @throws Exception
     */
    public void funcStandardSequence(String SourceNo) throws Exception {

        int counter = 0;
        int step = 1000;
        int stepstate = step;

        try {

            String startQuery;

            if (SourceNo.isEmpty()) {

                startQuery = ""
                        + "SELECT "
                        + "id_registration , "
                        + "registration_maintype , "
                        + "registration_location_no , "
                        + "registration_year , "
                        + "registration_month , "
                        + "registration_seq , "
                        + "id_source "
                        + "FROM "
                        + "links_cleaned.registration_c "
                        + sourceFilter + " AND "
                        + "registration_location_no is not null AND "
                        + "registration_year     is not null AND "
                        + "registration_month    is not null "
                        + "ORDER BY "
                        + "registration_maintype , "
                        + "registration_location_no , "
                        + "registration_year , "
                        + "registration_month , "
                        + "registration_seq ";

            } else {
                startQuery = ""
                        + "SELECT "
                        + "id_registration , "
                        + "registration_maintype , "
                        + "registration_location_no , "
                        + "registration_year , "
                        + "registration_month , "
                        + "registration_seq , "
                        + "id_source "
                        + "FROM "
                        + "links_cleaned.registration_c "
                        + "WHERE id_source = " + SourceNo + " AND "
                        + "registration_location_no is not null AND "
                        + "registration_year     is not null AND "
                        + "registration_month    is not null "
                        + "ORDER BY "
                        + "registration_maintype , "
                        + "registration_location_no , "
                        + "registration_year , "
                        + "registration_month , "
                        + "registration_seq";
            }


            ResultSet rs = conCleaned.runQueryWithResult(startQuery);

            // Read first entry
            if (rs.next() == false) {
                return;
            }

            int previousId = rs.getInt("id_registration");
            int previousNo = -1;
            int previousMt = rs.getInt("registration_maintype");
            int previousLc = rs.getInt("registration_location_no");
            int previousYr = rs.getInt("registration_year");
            int previousMn = rs.getInt("registration_month");
            String id_source = rs.getString("id_source");

            if (rs.getString("registration_seq") == null || rs.getString("registration_seq").isEmpty()) {
                // EC 111
                funcAddtoReportRegistration(previousId, id_source, 111, "");
            } else { // Present
                // Is is numeric
                try {

                    previousNo = Integer.parseInt(rs.getString("registration_seq"));

                } catch (Exception e) {
                    // EC 112
                    funcAddtoReportRegistration(previousId, id_source, 112, rs.getString("registration_seq"));
                }
            }
            while (rs.next()) {
                counter++;
                if (counter == stepstate) {
                    funcShowMessage(counter + "", true, true);
                    stepstate += step;
                }

                int nummer;

                // Is reg_seq present ?
                if (rs.getString("registration_seq") == null || rs.getString("registration_seq").isEmpty()) {

                    // EC 111
                    funcAddtoReportRegistration(rs.getInt("id_registration"), id_source, 111, "");
                    continue;
                }
                // Is is numeric ?
                try {
                    nummer = Integer.parseInt(rs.getString("registration_seq"));
                } catch (Exception e) {
                    // EC 112
                    funcAddtoReportRegistration(rs.getInt("id_registration"), id_source, 112, rs.getString("registration_seq"));

                    // Set values
                    previousId = rs.getInt("id_registration");
                    previousMt = rs.getInt("registration_maintype");
                    previousLc = rs.getInt("registration_location_no");
                    previousYr = rs.getInt("registration_year");
                    previousMn = rs.getInt("registration_month");

                    continue;
                }

                int verschil = nummer - previousNo;

                if (verschil == 0 && (previousYr == rs.getInt("registration_year")) && (previousMt == rs.getInt("registration_maintype")) && (previousLc == rs.getInt("registration_location_no"))) {
                    // EC 113
                    funcAddtoReportRegistration(rs.getInt("id_registration"), id_source, 113, rs.getString("registration_seq"));
                } else if (verschil > 1 && (previousYr == rs.getInt("registration_year")) && (previousMt == rs.getInt("registration_maintype")) && (previousLc == rs.getInt("registration_location_no"))) {
                    // EC 114
                    funcAddtoReportRegistration(rs.getInt("id_registration"), id_source, 114, rs.getString("registration_seq"));
                }

                // Set values
                previousId = rs.getInt("id_registration");
                previousNo = nummer;
                previousMt = rs.getInt("registration_maintype");
                previousLc = rs.getInt("registration_location_no");
                previousYr = rs.getInt("registration_year");
                previousMn = rs.getInt("registration_month");
            }
        } catch (Exception e) {

            funcShowMessage(counter + " An error occured while checking sequence: " + e.getMessage(), false, true);
        }
    }

    /**
     *
     *
     *
     *
     *
     *
     * Previous Links basis function
     * They are now part of links cleaned
     *
     *
     *
     *
     *
     *
     *
     *
     *
     *
     *
     *
     */
    /**
     * @param sourceNo
     * @throws Exception
     */
    public void funcRelation(String sourceNo) throws Exception {

        int counter = 0;
        int step = 1000;
        int stepstate = step;

        try {

            String startQuery;
            String idSource;

            // source is given in GUI
            if (sourceNo.isEmpty()) {

                startQuery = "SELECT id_registration , id_person, role, sex FROM person_c " + bronFilter + " ORDER BY id_registration";

                idSource = this.bronNr + "";

            } // per source
            else {

                startQuery = "SELECT id_registration , id_person, role, sex FROM person_c WHERE id_source = " + sourceNo + " ORDER BY id_registration";

                idSource = sourceNo;
            }

            // Run ref query
            ResultSet rsactRoleRef = conGeneral.runQueryWithResult("SELECT * FROM ref_relation");


            // create searchable list
            ActRoleSet ars = new ActRoleSet();
            ars.addRessultSetToList(rsactRoleRef);

            // Run person query
            ResultSet rsPersons = conCleaned.runQueryWithResult(startQuery);

            int currentId = -1;

            ArrayList<RelationSet> rsList = new ArrayList<RelationSet>();

            while (rsPersons.next()) {

                counter++;

                if (counter == 66563) {
                    int ie = 0;
                }


                if (counter == stepstate) {
                    funcShowMessage(counter + "", true, true);
                    stepstate += step;
                }

                int id_registration = rsPersons.getInt("id_registration");
                int id_person = rsPersons.getInt("id_person");
                String role = rsPersons.getString("role");
                String sex = rsPersons.getString("sex");

                // It is the same id
                if (currentId == id_registration) {
                    RelationSet rs = new RelationSet();

                    // add
                    rs.setIdPerson(id_person);
                    rs.setRole(role);
                    rs.setSex(sex);

                    rsList.add(rs);
                } // new id, process old list
                else {
                    // we must save old list
                    ArrayList<RelationSet> rsWorkingList = new ArrayList<RelationSet>(rsList);

                    Collections.copy(rsWorkingList, rsList);

                    // clear old list
                    rsList.clear();

                    // old list will be used nou
                    RelationSet rs = new RelationSet();

                    // add
                    rs.setIdPerson(id_person);
                    rs.setRole(role);
                    rs.setSex(sex);

                    rsList.add(rs);

                    /*
                     * Proces the new list
                     * Only if id is not -1
                     * otherwise is is the first time
                     */

                    if (currentId > -1) {
                        // walk through list
                        for (int i = 0; i < rsWorkingList.size(); i++) {

                            // second walk
                            for (int j = 0; j < rsWorkingList.size(); j++) {

                                // check is it is itselfs
                                if (i != j) {

                                    int id_person1 = rsWorkingList.get(i).getIdPerson();
                                    int id_person2 = rsWorkingList.get(j).getIdPerson();
                                    String role1 = rsWorkingList.get(i).getRole();
                                    String role2 = rsWorkingList.get(j).getRole();
                                    String sex1 = rsWorkingList.get(i).getSex();
                                    String sex2 = rsWorkingList.get(j).getSex();

                                    // Get relation
                                    String relation = ars.getRelatie(role1, role2, sex1, sex2);

                                    // check is relation is fileld
                                    if (relation.isEmpty()) {

                                        // EC 101
                                        funcAddtoReportPerson(id_person1, idSource, 101, id_person2 + "");
                                    } else {

                                        // add to relation_c
                                        String query = ""
                                                + "INSERT INTO relation_c( id_person1 , id_person2 , relation ) "
                                                + "values( '" + id_person1 + "' , '" + id_person2 + "' , '" + relation + "' )";

                                        //conCleaned.runQuery(query);
                                    }
                                }
                            }
                        }

                        currentId = id_registration;

                    } // Current ID is -1
                    // Must change
                    else {
                        currentId = id_registration;
                    }
                }
            }
        } catch (Exception e) {
            funcShowMessage(counter + " An error occured while running Relation: " + e.getMessage(), false, true);
        }
    }

    public void funMinMaxDateMain(String sourceNo) throws Exception {

        int counter = 0;
        int step = 10000;
        int stepstate = step;

        try {

            String idSource;

            String startQuery = ""
                    + " SELECT "
                    + " registration_c.id_registration ,"
                    + " registration_c.id_source ,"
                    + " registration_c.registration_date ,"
                    + " registration_c.registration_maintype ,"
                    + " person_c.id_person ,"
                    + " person_c.role ,"
                    + " person_c.age_year ,"
                    + " person_c.age_month ,"
                    + " person_c.age_week ,"
                    + " person_c.age_day ,"
                    + " person_c.birth_date ,"
                    + " person_c.mar_date ,"
                    + " person_c.death_date ,"
                    + " person_c.birth_year ,"
                    + " person_c.birth_date_valid ,"
                    + " person_c.mar_date_valid ,"
                    + " person_c.death_date_valid"
                    + " FROM"
                    + " person_c , registration_c"
                    + " WHERE"
                    + " person_c.id_registration = registration_c.id_registration AND"
                    + " valid_complete = 0";

            // Source from GUI
            if (sourceNo.isEmpty()) {

                startQuery += " AND links_cleaned.person_c.id_source = " + this.bronNr;

                idSource = this.bronNr + "";

            } // per source
            else {

                startQuery += " AND links_cleaned.person_c.id_source = " + sourceNo;

                idSource = sourceNo;

            }


            // Run person query
            ResultSet rsPersons = conCleaned.runQueryWithResult(startQuery);

            // Count hits
            rsPersons.last();

            int total = rsPersons.getRow();

            rsPersons.beforeFirst();

            funcShowMessage("0 of " + total, true, true);

            // Create Objects
            int age_year;
            int age_month;
            int age_week;
            int age_day;
            int birth_year;
            int id_registration;
            int id_source;
            int registrationMaintype;
            int id_person;
            int role;
            String registrationDate;
            String birth_date;
            String mar_date;
            String death_date;
            int birth_date_valid;
            int mar_date_valid;
            int death_date_valid;
            MinMaxDateSet mmds = new MinMaxDateSet();

            while (rsPersons.next()) {

                counter++;

                if (counter == stepstate) {
                    funcShowMessage(counter + " of " + total, true, true);
                    stepstate += step;
                }

                // Inladen gegeven
                id_registration = rsPersons.getInt("id_registration");
                id_source = rsPersons.getInt("id_source");
                registrationDate = rsPersons.getString("registration_date");
                registrationMaintype = rsPersons.getInt("registration_maintype");
                id_person = rsPersons.getInt("id_person");
                role = rsPersons.getInt("role");
                age_year = rsPersons.getInt("age_year");
                age_month = rsPersons.getInt("age_month");
                age_week = rsPersons.getInt("age_week");
                age_day = rsPersons.getInt("age_day");
                birth_year = rsPersons.getInt("birth_year");
                birth_date = rsPersons.getString("person_c.birth_date");
                mar_date = rsPersons.getString("person_c.mar_date");
                death_date = rsPersons.getString("person_c.death_date");
                birth_date_valid = rsPersons.getInt("birth_date_valid");
                mar_date_valid = rsPersons.getInt("mar_date_valid");
                death_date_valid = rsPersons.getInt("death_date_valid");

                // Fill object
                mmds.setRegistrationId(id_registration);
                mmds.setSourceId(id_source);
                mmds.setRegistrationDate(registrationDate);
                mmds.setRegistrationMaintype(registrationMaintype);
                mmds.setPersonId(id_person);
                mmds.setPersonRole(role);
                mmds.setPersonAgeYear(age_year);
                mmds.setPersonAgeMonth(age_month);
                mmds.setPersonAgeWeek(age_week);
                mmds.setPersonAgeDay(age_day);
                mmds.setPersonBirthYear(birth_year);
                mmds.setDeathDate(death_date);

                int mainrole;

                switch (registrationMaintype) {
                    case 1:
                        mainrole = 1;
                        break;
                    case 2:
                        if ((role == 7) || (role == 8) || (role == 9)) {
                            mainrole = 7;
                        } else {
                            mainrole = 4;
                        }
                        break;
                    case 3:
                        mainrole = 10;
                        break;
                    default:
                        continue;
                }

                // main role
                mmds.setRegistrationMainRole(mainrole);

                String type_date = "";

                // Birth date
                if (birth_date_valid != 1) {
                    mmds.setTypeDate("birth_date");
                    type_date = "birth";
                    mmds.setDate(birth_date);

                    // Call Minmaxdate
                    DevinedMinMaxDatumSet ddmdBirth = funcMinMaxDate(mmds);

                    // TODO temporary solution
                    if (ddmdBirth.getMinYear() < 0) {
                        ddmdBirth.setMinYear(0);
                    }
                    if (ddmdBirth.getMinMonth() < 0) {
                        ddmdBirth.setMinMonth(0);
                    }
                    if (ddmdBirth.getMinDay() < 0) {
                        ddmdBirth.setMinDay(0);
                    }
                    if (ddmdBirth.getMaxYear() < 0) {
                        ddmdBirth.setMaxYear(0);
                    }
                    if (ddmdBirth.getMaxMonth() < 0) {
                        ddmdBirth.setMaxMonth(0);
                    }
                    if (ddmdBirth.getMaxDay() < 0) {
                        ddmdBirth.setMaxDay(0);
                    }

                    // Min Max to table
                    String runQueryGeb = "UPDATE person_c"
                            + " SET "
                            + type_date + "_year_min" + " = " + ddmdBirth.getMinYear() + " ,"
                            + type_date + "_month_min" + " = " + ddmdBirth.getMinMonth() + " ,"
                            + type_date + "_day_min" + " = " + ddmdBirth.getMinDay() + " ,"
                            + type_date + "_year_max" + " = " + ddmdBirth.getMaxYear() + " ,"
                            + type_date + "_month_max" + " = " + ddmdBirth.getMaxMonth() + " ,"
                            + type_date + "_day_max" + " = " + ddmdBirth.getMaxDay()
                            + " WHERE person_c.id_person = " + id_person;

                    conCleaned.runQuery(runQueryGeb);
                }
                // Marriage date
                if (mar_date_valid != 1) {
                    mmds.setTypeDate("marriage_date");
                    type_date = "mar";
                    mmds.setDate(mar_date);

                    // Call Minmaxdate
                    DevinedMinMaxDatumSet ddmdMarriage = funcMinMaxDate(mmds);

                    // temp solution
                    if (ddmdMarriage.getMinYear() < 0) {
                        ddmdMarriage.setMinYear(0);
                    }
                    if (ddmdMarriage.getMinMonth() < 0) {
                        ddmdMarriage.setMinMonth(0);
                    }
                    if (ddmdMarriage.getMinDay() < 0) {
                        ddmdMarriage.setMinDay(0);
                    }
                    if (ddmdMarriage.getMaxYear() < 0) {
                        ddmdMarriage.setMaxYear(0);
                    }
                    if (ddmdMarriage.getMaxMonth() < 0) {
                        ddmdMarriage.setMaxMonth(0);
                    }
                    if (ddmdMarriage.getMaxDay() < 0) {
                        ddmdMarriage.setMaxDay(0);
                    }

                    // Min Max to table
                    String runQueryHuw = "UPDATE person_c"
                            + " SET "
                            + type_date + "_year_min" + " = " + ddmdMarriage.getMinYear() + " ,"
                            + type_date + "_month_min" + " = " + ddmdMarriage.getMinMonth() + " ,"
                            + type_date + "_day_min" + " = " + ddmdMarriage.getMinDay() + " ,"
                            + type_date + "_year_max" + " = " + ddmdMarriage.getMaxYear() + " ,"
                            + type_date + "_month_max" + " = " + ddmdMarriage.getMaxMonth() + " ,"
                            + type_date + "_day_max" + " = " + ddmdMarriage.getMaxDay()
                            + " WHERE person_c.id_person = " + id_person;

                    conCleaned.runQuery(runQueryHuw);
                }

                // Death date
                if (death_date_valid != 1) {
                    mmds.setTypeDate("death_date");
                    type_date = "death";
                    mmds.setDate(death_date);

                    // Call Minmaxdate
                    DevinedMinMaxDatumSet ddmdDeath = funcMinMaxDate(mmds);


                    // TODO: temp solution
                    if (ddmdDeath.getMinYear() < 0) {
                        ddmdDeath.setMinYear(0);
                    }
                    if (ddmdDeath.getMinMonth() < 0) {
                        ddmdDeath.setMinMonth(0);
                    }
                    if (ddmdDeath.getMinDay() < 0) {
                        ddmdDeath.setMinDay(0);
                    }
                    if (ddmdDeath.getMaxYear() < 0) {
                        ddmdDeath.setMaxYear(0);
                    }
                    if (ddmdDeath.getMaxMonth() < 0) {
                        ddmdDeath.setMaxMonth(0);
                    }
                    if (ddmdDeath.getMaxDay() < 0) {
                        ddmdDeath.setMaxDay(0);
                    }

                    // Min Max to table
                    String runQueryOvl = "UPDATE person_c"
                            + " SET "
                            + type_date + "_year_min" + " = " + ddmdDeath.getMinYear() + " ,"
                            + type_date + "_month_min" + " = " + ddmdDeath.getMinMonth() + " ,"
                            + type_date + "_day_min" + " = " + ddmdDeath.getMinDay() + " ,"
                            + type_date + "_year_max" + " = " + ddmdDeath.getMaxYear() + " ,"
                            + type_date + "_month_max" + " = " + ddmdDeath.getMaxMonth() + " ,"
                            + type_date + "_day_max" + " = " + ddmdDeath.getMaxDay()
                            + " WHERE person_c.id_person = " + id_person;

                    conCleaned.runQuery(runQueryOvl);
                }
            }
        } catch (Exception e) {
            funcShowMessage(counter + " An error occured while running Min Max: " + e.getMessage(), false, true);
        }
    }

    /**
     * @param inputInfo
     * @return
     * @throws Exception
     */
    private DevinedMinMaxDatumSet funcMinMaxDate(MinMaxDateSet inputInfo)
            throws Exception {

        // central date
        // TODO: DATE CANNOT BE VALID
//        DateYearMonthDaySet inputYearMonthDay =
//                LinksSpecific.devideCheckDate(inputInfo.getDate());

        // registration date
        DateYearMonthDaySet inputregistrationYearMonthDday =
                LinksSpecific.devideCheckDate(inputInfo.getRegistrationDate());

        // Check: Is date valid
        // TODO: DATE CANNOT BE VALID
//        if (inputYearMonthDay.isValidDate()) {
//            DevinedMinMaxDatumSet returnSet = new DevinedMinMaxDatumSet();
//
//            returnSet.setMaxDay(inputYearMonthDay.getDay());
//            returnSet.setMaxMonth(inputYearMonthDay.getMonth());
//            returnSet.setMaxYear(inputYearMonthDay.getYear());
//            returnSet.setMinDay(inputYearMonthDay.getDay());
//            returnSet.setMinMonth(inputYearMonthDay.getMonth());
//            returnSet.setMinYear(inputYearMonthDay.getYear());
//
//            return returnSet;
//        }

        // Fact: Date invalid

        // Check: age in years given?

        // Fact: age is given in years
        if (inputInfo.getPersonAgeYear() > 0) {

            // Create new return set
            DevinedMinMaxDatumSet returnSet = new DevinedMinMaxDatumSet();

            // check if it is the deceased
            if (inputInfo.getPersonRole() == 10) {

                // registration date
                DateYearMonthDaySet inputDeathDate = LinksSpecific.devideCheckDate(inputInfo.getDeathDate());

                // check death date
                if (inputDeathDate.isValidDate()) {

                    // Day no en month no are similar to aktdate
                    returnSet.setMaxDay(inputDeathDate.getDay());
                    returnSet.setMaxMonth(inputDeathDate.getMonth());
                    returnSet.setMinDay(inputDeathDate.getDay());
                    returnSet.setMinMonth(inputDeathDate.getMonth());

                } else {

                    // Day no en month no are similar to aktdate
                    returnSet.setMaxDay(inputregistrationYearMonthDday.getDay());
                    returnSet.setMaxMonth(inputregistrationYearMonthDday.getMonth());
                    returnSet.setMinDay(inputregistrationYearMonthDday.getDay());
                    returnSet.setMinMonth(inputregistrationYearMonthDday.getMonth());

                }

            } else {

                // Day no en month no are similar to aktdate
                returnSet.setMaxDay(inputregistrationYearMonthDday.getDay());
                returnSet.setMaxMonth(inputregistrationYearMonthDday.getMonth());
                returnSet.setMinDay(inputregistrationYearMonthDday.getDay());
                returnSet.setMinMonth(inputregistrationYearMonthDday.getMonth());

            }

            // preperation tasks
            Ages ageCentralFigure =
                    funcReturnAgeCentralFigure(inputInfo.getRegistrationId(), inputInfo.getRegistrationMainType(), inputInfo.getPersonRole());

            // compute min and max year
            // via funcMinMaxCalculation
            MinMaxYearSet mmj = funcMinMaxCalculation(
                    inputInfo.getPersonId(),
                    inputregistrationYearMonthDday.getYear(),
                    inputInfo.getRegistrationMainType(),
                    inputInfo.getTypeDate(),
                    inputInfo.getPersonRole(),
                    inputInfo.getPersonAgeYear(),
                    inputInfo.getRegistrationMainRole(),
                    ageCentralFigure);

            returnSet.setMinYear(mmj.GetMinYear());
            returnSet.setMaxYear(mmj.GetMaxYear());

            return returnSet;
        }

        // Fact: age not given by years

        // Check: Is birthyear given?

        // Fact: birth year given
        if (inputInfo.getPersonBirthYear() > 0) {

            // age is = actjaar - birth year
            int birth_year = inputInfo.getPersonBirthYear();
            int act_year = inputregistrationYearMonthDday.getYear();

            int AgeInYears = act_year - birth_year;

            // Create new set
            DevinedMinMaxDatumSet returnSet = new DevinedMinMaxDatumSet();

            // Day no en month similar to act date
            returnSet.setMaxDay(inputregistrationYearMonthDday.getDay());
            returnSet.setMaxMonth(inputregistrationYearMonthDday.getMonth());
            returnSet.setMinDay(inputregistrationYearMonthDday.getDay());
            returnSet.setMinMonth(inputregistrationYearMonthDday.getMonth());

            // preparation
            Ages ageCentralFigure =
                    funcReturnAgeCentralFigure(inputInfo.getRegistrationId(), inputInfo.getRegistrationMainType(), inputInfo.getPersonRole());

            // compute min and max year
            // via funcMinMaxCalculation
            MinMaxYearSet mmj = funcMinMaxCalculation(
                    inputInfo.getPersonId(),
                    act_year,
                    inputInfo.getRegistrationMainType(),
                    inputInfo.getTypeDate(),
                    inputInfo.getPersonRole(),
                    AgeInYears,
                    inputInfo.getRegistrationMainRole(),
                    ageCentralFigure);

            returnSet.setMinYear(mmj.GetMinYear());
            returnSet.setMaxYear(mmj.GetMaxYear());

            return returnSet;
        }

        // Fact: birth year not given

        // Check: Is it the deceased him self?

        // Fact: not the deceased
        if (inputInfo.getPersonRole() != 10) {

            // Days, month, weeks to years, round up
            int ageinYears = funcRoundUp(
                    inputInfo.getPersonAgeYear(),
                    inputInfo.getPersonAgeMonth(),
                    inputInfo.getPersonAgeWeek(),
                    inputInfo.getPersonAgeDay());

            // New return set
            DevinedMinMaxDatumSet returnSet = new DevinedMinMaxDatumSet();

            // day and month is similar to act date
            returnSet.setMaxDay(inputregistrationYearMonthDday.getDay());
            returnSet.setMaxMonth(inputregistrationYearMonthDday.getMonth());
            returnSet.setMinDay(inputregistrationYearMonthDday.getDay());
            returnSet.setMinMonth(inputregistrationYearMonthDday.getMonth());

            // preparation
            Ages leeftijdHoofdpersoon =
                    funcReturnAgeCentralFigure(inputInfo.getRegistrationId(), inputInfo.getRegistrationMainType(), inputInfo.getPersonRole());

            // compute min and max year
            // via funcMinMaxCalculation
            MinMaxYearSet mmj = funcMinMaxCalculation(
                    inputInfo.getPersonId(),
                    inputregistrationYearMonthDday.getYear(),
                    inputInfo.getRegistrationMainType(),
                    inputInfo.getTypeDate(),
                    inputInfo.getPersonRole(),
                    ageinYears,
                    inputInfo.getRegistrationMainRole(),
                    leeftijdHoofdpersoon);

            returnSet.setMinYear(mmj.GetMinYear());
            returnSet.setMaxYear(mmj.GetMaxYear());

            return returnSet;
        }

        // Fact: It is de deceased

        // Check: combination of month days and weeks?

        int areMonths = 0;
        int areWeeks = 0;
        int areDays = 0;

        if (inputInfo.getPersonAgeMonth() > 0) {
            areMonths++;
        }
        if (inputInfo.getPersonAgeWeek() > 0) {
            areWeeks++;
        }
        if (inputInfo.getPersonAgeDay() > 0) {
            areDays++;
        }

        // TODO: ADDED
        // If marriage date, return 0-0-0
        // returnen
        if (inputInfo.getTypeDate().equalsIgnoreCase("marriage_date") && ((areMonths + areWeeks + areDays) > 0)) {
            DevinedMinMaxDatumSet returnSet = new DevinedMinMaxDatumSet();

            returnSet.setMaxDay(0);
            returnSet.setMaxMonth(0);
            returnSet.setMaxYear(0);
            returnSet.setMinDay(0);
            returnSet.setMinMonth(0);
            returnSet.setMinYear(0);

            return returnSet;
        }

        // TODO: added
        DateYearMonthDaySet inputDeathDate = LinksSpecific.devideCheckDate(inputInfo.getDeathDate());

        int useYear;
        int useMonth;
        int useDay;

        if (inputDeathDate.isValidDate()) {

            useYear = inputDeathDate.getYear();
            useMonth = inputDeathDate.getMonth();
            useDay = inputDeathDate.getDay();

        } else {

            useYear = inputregistrationYearMonthDday.getYear();
            useMonth = inputregistrationYearMonthDday.getMonth();
            useDay = inputregistrationYearMonthDday.getDay();

        }

        // fact: combination
        if ((areMonths + areWeeks + areDays) > 1) {

            // weeks and months to days
            int dagen = inputInfo.getPersonAgeMonth() * 30;
            dagen += inputInfo.getPersonAgeWeek() * 7;

            // Date calculation

            // new date -> date - (days - 1)

            int mindays = (dagen - 1) * -1;
            int maxdays = (dagen + 1) * -1;

            // Min date
            String minDate = funcAddTimeToDate(
                    useYear,
                    useMonth,
                    useDay,
                    TimeType.DAY,
                    mindays);

            // Max date
            String maxDate = funcAddTimeToDate(
                    useYear,
                    useMonth,
                    useDay,
                    TimeType.DAY,
                    maxdays);

            // New date return return values
            DateYearMonthDaySet computedMinDate = LinksSpecific.devideCheckDate(minDate);
            DateYearMonthDaySet computedMaxDate = LinksSpecific.devideCheckDate(maxDate);

            // Checken if max date not later than actdate
            DateYearMonthDaySet dymd = funcCheckMaxDate(
                    computedMaxDate.getYear(),
                    computedMaxDate.getMonth(),
                    computedMaxDate.getDay(),
                    useYear,
                    useMonth,
                    useDay);

            // returnen
            DevinedMinMaxDatumSet returnSet = new DevinedMinMaxDatumSet();

            returnSet.setMaxDay(dymd.getDay());
            returnSet.setMaxMonth(dymd.getMonth());
            returnSet.setMaxYear(dymd.getYear());
            returnSet.setMinDay(computedMinDate.getDay());
            returnSet.setMinMonth(computedMinDate.getMonth());
            returnSet.setMinYear(computedMinDate.getYear());

            return returnSet;
        } // Fact: age in months
        else if (areMonths == 1) {

            // convert months
            int dagen = inputInfo.getPersonAgeMonth() * 30;

            // compute date
            // new date -> date - (days - 1)
            dagen++;

            int mindagen = (dagen + 14) * -1;
            int maxdagen = (dagen - 14) * -1;

            // Min date
            String minDate = funcAddTimeToDate(
                    useYear,
                    useMonth,
                    useDay,
                    TimeType.DAY,
                    mindagen);

            // Max date
            String maxDate = funcAddTimeToDate(
                    useYear,
                    useMonth,
                    useDay,
                    TimeType.DAY,
                    maxdagen);

            // New date to return values
            DateYearMonthDaySet computedMinDate = LinksSpecific.devideCheckDate(minDate);
            DateYearMonthDaySet computedMaxDate = LinksSpecific.devideCheckDate(maxDate);

            // returnen
            DevinedMinMaxDatumSet returnSet = new DevinedMinMaxDatumSet();

            returnSet.setMaxDay(computedMaxDate.getDay());
            returnSet.setMaxMonth(computedMaxDate.getMonth());
            returnSet.setMaxYear(computedMaxDate.getYear());
            returnSet.setMinDay(computedMinDate.getDay());
            returnSet.setMinMonth(computedMinDate.getMonth());
            returnSet.setMinYear(computedMinDate.getYear());

            return returnSet;
        } // Fact: age in weeks
        else if (areWeeks == 1) {

            // weeks and months to days
            int days = inputInfo.getPersonAgeWeek() * 7;

            // compute date

            // new date -> date - (days - 1)
            days++;

            int mindays = (days + 4) * -1;
            int maxdays = (days - 4) * -1;

            // Min date
            String minDate = funcAddTimeToDate(
                    useYear,
                    useMonth,
                    useDay,
                    TimeType.DAY,
                    mindays);

            // Max datum
            String maxDate = funcAddTimeToDate(
                    useYear,
                    useMonth,
                    useDay,
                    TimeType.DAY,
                    maxdays);

            // date to return values
            DateYearMonthDaySet computedMinDate = LinksSpecific.devideCheckDate(minDate);
            DateYearMonthDaySet computedMaxDate = LinksSpecific.devideCheckDate(maxDate);

            // return
            DevinedMinMaxDatumSet returnSet = new DevinedMinMaxDatumSet();

            returnSet.setMaxDay(computedMaxDate.getDay());
            returnSet.setMaxMonth(computedMaxDate.getMonth());
            returnSet.setMaxYear(computedMaxDate.getYear());
            returnSet.setMinDay(computedMinDate.getDay());
            returnSet.setMinMonth(computedMinDate.getMonth());
            returnSet.setMinYear(computedMinDate.getYear());

            return returnSet;

        } // Fact: age in days
        else if (areDays == 1) {

            // weeks and months to days
            int days = inputInfo.getPersonAgeDay();

            // new date -> date - (days - 1)

            int mindays = (days + 1) * -1;
            int maxdays = (days - 1) * -1;

            // min date
            String minDate = funcAddTimeToDate(
                    useYear,
                    useMonth,
                    useDay,
                    TimeType.DAY,
                    mindays);

            // max date
            String maxDate = funcAddTimeToDate(
                    useYear,
                    useMonth,
                    useDay,
                    TimeType.DAY,
                    maxdays);


            // New date to return value
            DateYearMonthDaySet computedMinDate = LinksSpecific.devideCheckDate(minDate);
            DateYearMonthDaySet computedMaxDate = LinksSpecific.devideCheckDate(maxDate);


            // Checken if max date niet later than actdate
            DateYearMonthDaySet dymd = funcCheckMaxDate(
                    computedMaxDate.getYear(),
                    computedMaxDate.getMonth(),
                    computedMaxDate.getDay(),
                    useYear,
                    useMonth,
                    useDay);

            // return
            DevinedMinMaxDatumSet returnSet = new DevinedMinMaxDatumSet();

            returnSet.setMaxDay(dymd.getDay());
            returnSet.setMaxMonth(dymd.getMonth());
            returnSet.setMaxYear(dymd.getYear());
            returnSet.setMinDay(computedMinDate.getDay());
            returnSet.setMinMonth(computedMinDate.getMonth());
            returnSet.setMinYear(computedMinDate.getYear());

            return returnSet;
        }

        // No age given
        DevinedMinMaxDatumSet returnSet = new DevinedMinMaxDatumSet();

        // day and month similar to act date
        returnSet.setMaxDay(inputregistrationYearMonthDday.getDay());
        returnSet.setMaxMonth(inputregistrationYearMonthDday.getMonth());
        returnSet.setMinDay(inputregistrationYearMonthDday.getDay());
        returnSet.setMinMonth(inputregistrationYearMonthDday.getMonth());

        // compute min max year
        // via funcMinMaxCalculation
        // PREPARATION FOR THIS EXCEPTION!!!
        Ages ages = new Ages();
        ages.setYear(0);
        ages.setMonth(0);
        ages.setWeek(0);
        ages.setDay(0);

        MinMaxYearSet mmj = funcMinMaxCalculation(
                inputInfo.getPersonId(),
                inputregistrationYearMonthDday.getYear(),
                inputInfo.getRegistrationMainType(),
                inputInfo.getTypeDate(),
                inputInfo.getPersonRole(),
                0,
                inputInfo.getRegistrationMainRole(),
                ages);

        returnSet.setMinYear(mmj.GetMinYear());
        returnSet.setMaxYear(mmj.GetMaxYear());

        return returnSet;

    }

    /**
     * @param id_registration
     * @param registration_maintype
     * @param rol
     * @return
     */
    private Ages funcReturnAgeCentralFigure(int id_registration, int registration_maintype, int rol) {

        Ages ages = new Ages();

        // age of central figure
        if (registration_maintype == 1) {
            // int indexNr = hpChildRegistration.indexOf(id_registration);
            int indexNr = Collections.binarySearch(hpChildRegistration, id_registration);

            // Check if number excists in list
            // add age
            if (indexNr > -1) {
                ages.setYear(hpChildAge.get(indexNr));
                ages.setMonth(hpChildMonth.get(indexNr));
                ages.setWeek(hpChildWeek.get(indexNr));
                ages.setDay(hpChildDay.get(indexNr));
            }
        } // age of central figure
        // age of central figure
        else if ((registration_maintype == 2) && ((rol == 5) || (rol == 6))) {
            // int indexNr = hpBrideRegistration.indexOf(id_registration);
            int indexNr = Collections.binarySearch(hpBrideRegistration, id_registration);

            // check is number exists
            // add age
            if (indexNr > -1) {
                ages.setYear(hpBrideAge.get(indexNr));
                ages.setMonth(hpBrideMonth.get(indexNr));
                ages.setWeek(hpBrideWeek.get(indexNr));
                ages.setDay(hpBrideDay.get(indexNr));
            }
        } else if ((registration_maintype == 2) && ((rol == 8) || (rol == 9))) {
            // int indexNr = hpGroomRegistration.indexOf(id_registration);
            int indexNr = Collections.binarySearch(hpGroomRegistration, id_registration);

            // check exeistence of number in list
            // add age
            if (indexNr > -1) {
                ages.setYear(hpGroomAge.get(indexNr));
                ages.setMonth(hpGroomMonth.get(indexNr));
                ages.setWeek(hpGroomWeek.get(indexNr));
                ages.setDay(hpGroomDay.get(indexNr));
            }
        } // central figure age
        else if (registration_maintype == 3) {
            // int indexNr = hpDeceasedRegistration.indexOf(id_registration);
            int indexNr = Collections.binarySearch(hpDeceasedRegistration, id_registration);

            // check number if exists
            // add age
            if (indexNr > -1) {
                ages.setYear(hpDeceasedAge.get(indexNr));
                ages.setMonth(hpDeceasedMonth.get(indexNr));
                ages.setWeek(hpDeceasedWeek.get(indexNr));
                ages.setDay(hpDeceasedDay.get(indexNr));
            }
        }

        return ages;
    }

    /**
     * @param act_year
     * @param main_type
     * @param date_type
     * @param role
     * @param age
     * @param main_role
     * @param age_main_role
     * @return
     * @throws Exception
     */
    private MinMaxYearSet funcMinMaxCalculation(
            int id_person,
            int act_year,
            int main_type,
            String date_type,
            int role,
            int age,
            int main_role,
            Ages age_main_role) throws Exception {
        String yn_age_reported = "";
        String yn_age_main_role = "";

        // Age is given
        if ((age > 0) || ((role == 1) && (main_type == 1))) {
            yn_age_reported = "y";
        } else {
            yn_age_reported = "n";
        }

        if (age_main_role.getYear() > 0) {
            yn_age_main_role = "y";
        } else {
            yn_age_main_role = "n";
        }

        // UITSTAPJE, geldt voor ouders van de overledene
        if ((main_type == 3) && ((role == 2) || (role == 3)) && (yn_age_main_role.equals("n"))) {
            if (age_main_role.getMonth() > 0
                    || age_main_role.getWeek() > 0
                    || age_main_role.getDay() > 0) {
                yn_age_main_role = "y";

                // omrekenen
                int y = 0;
                int m = age_main_role.getMonth();
                int w = age_main_role.getWeek();
                int d = age_main_role.getDay();

                // to year
                w += (d / 7);
                m += (w / 4);
                y += (m / 12);

                age_main_role.setYear(y);

            }
        }

        // EINDE UITSTAPJE


        // Maak query
        String query = ""
                + "SELECT * FROM ref_date_minmax WHERE "
                + "maintype = '" + main_type + "' AND "
                + "date_type = '" + date_type + "' AND "
                + "role = '" + role + "' AND "
                + "age_reported = '" + yn_age_reported + "' AND "
                + "( age_main_role = '" + yn_age_main_role + "' OR "
                + "age_main_role = 'nvt' )";

        // Run query
        ResultSet rs = conGeneral.runQueryWithResult(query);

        // check rs is empty
        if (!rs.next()) {

            // EC 
            funcAddtoReportPerson(id_person, "0", 105, "Null -> [rh:" + main_type + "][ad:" + date_type + "][rol:" + role + "][lg:" + yn_age_reported + "][lh:" + yn_age_main_role + "]");

            MinMaxYearSet mmj = new MinMaxYearSet();

            mmj.SetMaxYear(0);
            mmj.SetMinYear(0);

            return mmj;
        }

        // To last row
        rs.last();

        // read from to database
        String function = rs.getString("function");
        int min_year = rs.getInt("min_year");
        int max_year = rs.getInt("max_year");
        int min_person = rs.getInt("min_person");
        int max_person = rs.getInt("max_person");

        /*
        min en max age-role search
         */
        int min_age = 0;
        int max_age = 0;

        // find correct age
        // Min
        if (min_person == role) {
            min_age = age;
        } else if (min_person == main_role) {
            min_age = age_main_role.getYear();
        }
        // Max
        if (max_person == role) {
            max_age = age;
        } else if (max_person == main_role) {
            max_age = age_main_role.getYear();
        }

        /**
         * Calculation
         */
        int minimal_year = act_year - min_age + min_year;
        int maximum_year = act_year - max_age + max_year;

        // set in dataset
        MinMaxYearSet mmj = new MinMaxYearSet();

        mmj.SetMaxYear(maximum_year);
        mmj.SetMinYear(minimal_year);


        /**
         * Functions
         */
        // If E, deceased
        if (function.equals("E")) {

            if (age < 14) {
                mmj.SetMaxYear(0);
                mmj.SetMinYear(0);
            }

            return mmj;
        } // function0 C, check by act year
        else if (function.equals("C")) {

            if (maximum_year > act_year) {
                mmj.SetMaxYear(act_year);
            }
            return mmj;

        } // function D
        else if (function.equals("D")) {

            if (minimal_year > (act_year - 14)) {

                mmj.SetMinYear(act_year - 14);

            }
            if (maximum_year > (act_year - 14)) {

                mmj.SetMaxYear(act_year - 14);

            }

            return mmj;

        }

        // Function A
        return mmj;

    }

    /**
     * Use this function to add or substract a amount of time from a date.
     *
     * @param year
     * @param month
     * @param day
     * @param tt
     * @param timeAmount
     * @return
     */
    private String funcAddTimeToDate(
            int year,
            int month,
            int day,
            TimeType tt,
            int timeAmount) {

        // new calendar instance
        Calendar c1 = Calendar.getInstance();

        // set(int year, int month, int date)
        c1.set(year, month, day);

        // Check of time type
        if (tt == tt.DAY) {
            c1.add(Calendar.DAY_OF_MONTH, timeAmount);
        } else if (tt == tt.WEEK) {
            c1.add(Calendar.WEEK_OF_MONTH, timeAmount);
        } else if (tt == tt.MONTH) {
            c1.add(Calendar.MONTH, timeAmount);
        } else if (tt == tt.YEAR) {
            c1.add(Calendar.YEAR, timeAmount);
        }

        // return new date
        String am = "" + c1.get(Calendar.DATE) + "-" + c1.get(Calendar.MONTH) + "-" + c1.get(Calendar.YEAR);

        return am;
    }

    /**
     * @param pYear
     * @param pMonth
     * @param pDay
     * @param rYear
     * @param rMonth
     * @param rDay
     * @return
     */
    private DateYearMonthDaySet funcCheckMaxDate(int pYear, int pMonth, int pDay, int rYear, int rMonth, int rDay) {

        // year is greater than age year
        if (pYear > rYear) {

            //return akterdatum
            DateYearMonthDaySet dy = new DateYearMonthDaySet();
            dy.setYear(rYear);
            dy.setMonth(rMonth);
            dy.setDay(rDay);
            return dy;

        } // lower, date is correct, return original date
        else if (pYear < rYear) {

            // return person date
            DateYearMonthDaySet dy = new DateYearMonthDaySet();
            dy.setYear(pYear);
            dy.setMonth(pMonth);
            dy.setDay(pDay);
            return dy;

        }

        /*
        years are equal, rest must be checked
         */

        // month is higher than act month
        if (pMonth > rMonth) {

            // return return act month
            DateYearMonthDaySet dy = new DateYearMonthDaySet();
            dy.setYear(rYear);
            dy.setMonth(rMonth);
            dy.setDay(rDay);
            return dy;

        } // month is correct, return original month
        else if (pMonth < rMonth) {

            // return return persons date
            DateYearMonthDaySet dy = new DateYearMonthDaySet();
            dy.setYear(pYear);
            dy.setMonth(pMonth);
            dy.setDay(pDay);
            return dy;
        }

        /*
        months are equal, check rest
         */

        // day is higher than act day
        if (pDay > rDay) {

            // return act date
            DateYearMonthDaySet dy = new DateYearMonthDaySet();
            dy.setYear(rYear);
            dy.setMonth(rMonth);
            dy.setDay(rDay);
            return dy;
        }

        // day is lower or similar to act day
        DateYearMonthDaySet dy = new DateYearMonthDaySet();
        dy.setYear(pYear);
        dy.setMonth(pMonth);
        dy.setDay(pDay);
        return dy;
    }

    /**
     * @param year
     * @param month
     * @param week
     * @param day
     * @return
     */
    public int funcRoundUp(int year, int month, int week, int day) {

        int tempYear = year;
        int tempMonth = month;
        int tempWeek = week;

        // day to week
        if (day > 0) {
            tempWeek += (day / 7);

            if ((day % 7) != 0) {
                tempWeek++;
            }
        }
        week = tempWeek;

        // week to month
        if (week > 0) {
            tempMonth += (week / 4);

            if ((week % 4) != 0) {
                tempMonth++;
            }
        }

        month = tempMonth;

        // week to month
        if (month > 0) {
            tempYear += (month / 12);

            if ((month % 12) != 0) {
                tempYear++;
            }
        }
        return tempYear;
    }

    /**
     * @param hjpsList
     * @param refMinMaxMarriageYaar
     * @throws Exception
     */
    private void funcMinMaxMarriageYear(
            ArrayList<MarriageYearPersonsSet> hjpsList,
            ResultSet refMinMaxMarriageYaar) throws Exception {

        int counter = 0;
        int step = 1000;
        int stepstate = step;

        // Loop through all persons
        for (int i = 0; i < hjpsList.size(); i++) {

            counter++;

            if (counter == stepstate) {
                funcShowMessage(counter + "", true, true);
                stepstate += step;
            }

            // walk through
            refMinMaxMarriageYaar.beforeFirst();

            boolean role1Found = false;
            int role1 = 0;
            int role2 = 0;

            while (refMinMaxMarriageYaar.next()) {

                int tempRht = refMinMaxMarriageYaar.getInt("maintype");
                int tempRole1 = refMinMaxMarriageYaar.getInt("role1");
                int tempRole2 = refMinMaxMarriageYaar.getInt("role2");

                if ((tempRole1 == hjpsList.get(i).getRole()) && tempRht == (hjpsList.get(i).getRegistrationMainType())) {
                    // rol found
                    role1Found = true;
                    role1 = tempRole1;
                    role2 = tempRole2;

                    break;
                }
            }

            // check if role 1 is found
            if (role1Found) {

                // search role 2
                boolean role2Found = false;
                int role2Id = 0;
                int role2MarYearMin = 0;
                int role2MarYearMax = 0;
                int role2MarMonthMin = 0;
                int role2MarMonthMax = 0;
                int role2MarDayMin = 0;
                int role2MarDayMax = 0;

                // walk trough all persons of registration
                for (int j = (((i - 7) > 0) ? i - 7 : 0); j < ((i + 7) > hjpsList.size() ? hjpsList.size() : i + 7); j++) {

                    if ((role2 == hjpsList.get(j).getRole()) && (hjpsList.get(i).getIdRegistration() == hjpsList.get(j).getIdRegistration())) {

                        // Role 2 found
                        role2Found = true;
                        role2Id = hjpsList.get(j).getIdPerson();
                        role2MarYearMin = hjpsList.get(j).getMarriageYearMin();
                        role2MarYearMax = hjpsList.get(j).getMarriageYearMax();
                        role2MarMonthMin = hjpsList.get(j).getMarriageMonthMin();
                        role2MarMonthMax = hjpsList.get(j).getMarriageMonthMax();
                        role2MarDayMin = hjpsList.get(j).getMarriageDayMin();
                        role2MarDayMax = hjpsList.get(j).getMarriageDayMax();

                        break;
                    }

                }

                // check is role 2 found
                if (role2Found) {

                    int role1Id = hjpsList.get(i).getIdPerson();
                    int role1MarYearMax = hjpsList.get(i).getMarriageYearMax();
                    int role1MarYearMin = hjpsList.get(i).getMarriageYearMin();
                    int role1MarMonthMax = hjpsList.get(i).getMarriageMonthMax();
                    int role1MarMonthMin = hjpsList.get(i).getMarriageMonthMin();
                    int role1MarDayMax = hjpsList.get(i).getMarriageDayMax();
                    int role1MarDayMin = hjpsList.get(i).getMarriageDayMin();

                    // First role 2, min Year
                    if (funcDateLeftIsGreater(role1MarYearMin, role1MarMonthMin, role1MarDayMin, role2MarYearMin, role2MarMonthMin, role2MarDayMin)) {

                        // Query
                        String query = ""
                                + " UPDATE person_c"
                                + " SET"
                                + " mar_year_min = " + hjpsList.get(i).getMarriageYearMin() + ","
                                + " mar_month_min = " + hjpsList.get(i).getMarriageMonthMin() + ","
                                + " mar_day_min = " + hjpsList.get(i).getMarriageDayMin()
                                + " WHERE"
                                + " id_person = " + role2Id;

                        conCleaned.runQuery(query);

                    }

                    // Role 2, max year
                    if (funcDateLeftIsGreater(role2MarYearMax, role2MarMonthMax, role2MarDayMax, role1MarYearMax, role1MarMonthMax, role1MarDayMax)) {

                        // Query
                        String query = ""
                                + " UPDATE person_c"
                                + " SET"
                                + " mar_year_max = " + hjpsList.get(i).getMarriageYearMax() + ","
                                + " mar_month_max = " + hjpsList.get(i).getMarriageMonthMax() + ","
                                + " mar_day_max = " + hjpsList.get(i).getMarriageDayMax()
                                + " WHERE"
                                + " id_person = " + role2Id;
                        conCleaned.runQuery(query);

                    }

                    // role 1
                    if (funcDateLeftIsGreater(role2MarYearMin, role2MarMonthMin, role2MarDayMin, role1MarYearMin, role1MarMonthMin, role1MarDayMin)) {

                        // Query
                        String query = "UPDATE person_c"
                                + " SET"
                                + " mar_year_min = " + role2MarYearMin + ","
                                + " mar_month_min = " + role2MarMonthMin + ","
                                + " mar_day_min = " + role2MarDayMin
                                + " WHERE"
                                + " id_person = " + role1Id;
                        conCleaned.runQuery(query);

                    }

                    // Role 1, max year
                    if (funcDateLeftIsGreater(role1MarYearMax, role1MarMonthMax, role1MarDayMax, role2MarYearMax, role2MarMonthMax, role2MarDayMax)) {

                        // Query
                        String query = "UPDATE person_c"
                                + " SET"
                                + " mar_year_max = " + role2MarYearMax + ","
                                + " mar_month_max = " + role2MarMonthMax + ","
                                + " mar_day_max = " + role2MarDayMax
                                + " WHERE"
                                + " id_person = " + role1Id;
                        conCleaned.runQuery(query);
                    }
                }
            }
        }
    }

    /**
     * @param lYear
     * @param lMonth
     * @param lDay
     * @param rYear
     * @param rMonth
     * @param rDay
     * @return
     */
    private boolean funcDateLeftIsGreater(int lYear, int lMonth, int lDay, int rYear, int rMonth, int rDay) {

        // year is greater than ryear year
        if (lYear > rYear) {

            return true;

        } // lower, date is correct, return original date
        else if (lYear < rYear) {

            // return person date
            return false;
        }

        /*
        years are equal, rest must be checked
         */

        // month is higher than act month
        if (lMonth > rMonth) {

            return true;

        } // month is correct, return original month
        else if (lMonth < rMonth) {

            return false;
        }

        /*
        months are equal, check rest
         */

        // day is higher than act day
        if (lDay > rDay) {

            return true;
        }

        return false;
    }

    /**
     * @param sourceNo
     * @return
     * @throws Exception
     */
    private ArrayList<MarriageYearPersonsSet> funcSetMarriageYear(String sourceNo) throws Exception {

        String query = ""
                + " SELECT "
                + " registration_c.id_registration ,"
                + " registration_c.registration_maintype ,"
                + " person_c.id_person ,"
                + " person_c.role ,"
                + " person_c.mar_day_min ,"
                + " person_c.mar_day_max ,"
                + " person_c.mar_month_min ,"
                + " person_c.mar_month_max ,"
                + " person_c.mar_year_min ,"
                + " person_c.mar_year_max"
                + " FROM registration_c , person_c"
                + " WHERE registration_c.id_registration = person_c.id_registration"
                + " AND registration_c.id_source = " + sourceNo + " ORDER by id_registration";

//                String query = ""
//                + " SELECT "
//                + " registration_c.id_registration ,"
//                + " registration_c.registration_maintype ,"
//                + " pers.id_person ,"
//                + " pers.role ,"
//                + " pers.mar_day_min ,"
//                + " pers.mar_day_max ,"
//                + " pers.mar_month_min ,"
//                + " pers.mar_month_max ,"
//                + " pers.mar_year_min ,"
//                + " pers.mar_year_max"
//                + " FROM registration_c , pers"
//                + " WHERE registration_c.id_registration = pers.id_registration ORDER BY pers.id_registration;";

        ResultSet minmaxjaarRs = conCleaned.runQueryWithResult(query);

        ArrayList<MarriageYearPersonsSet> hjpsList = new ArrayList<MarriageYearPersonsSet>();

        while (minmaxjaarRs.next()) {

            MarriageYearPersonsSet hjps = new MarriageYearPersonsSet();

            hjps.setIdRegistration(minmaxjaarRs.getInt("id_registration"));
            hjps.setRegistrationMainType(minmaxjaarRs.getInt("registration_maintype"));
            hjps.setIdPerson(minmaxjaarRs.getInt("id_person"));
            hjps.setRole(minmaxjaarRs.getInt("role"));
            hjps.setMarriageDayMin(minmaxjaarRs.getInt("mar_day_min"));
            hjps.setMarriageDayMax(minmaxjaarRs.getInt("mar_day_max"));
            hjps.setMarriageMonthMin(minmaxjaarRs.getInt("mar_month_min"));
            hjps.setMarriageMonthMax(minmaxjaarRs.getInt("mar_month_max"));
            hjps.setMarriageYearMin(minmaxjaarRs.getInt("mar_year_min"));
            hjps.setMarriageYearMax(minmaxjaarRs.getInt("mar_year_max"));

            hjpsList.add(hjps);

        }

        return hjpsList;
    }

    private void funcPartsToDate() {
        String query = "UPDATE links_cleaned.person_c SET "
                + "links_cleaned.person_c.birth_date_min  = CONCAT( links_cleaned.person_c.birth_day_min , '-' , links_cleaned.person_c.birth_month_min , '-' , links_cleaned.person_c.birth_year_min ) ,"
                + "links_cleaned.person_c.mar_date_min    = CONCAT( links_cleaned.person_c.mar_day_min , '-' , links_cleaned.person_c.mar_month_min , '-' , links_cleaned.person_c.mar_year_min ) ,"
                + "links_cleaned.person_c.death_date_min  = CONCAT( links_cleaned.person_c.death_day_min , '-' , links_cleaned.person_c.death_month_min , '-' , links_cleaned.person_c.death_year_min ) ,"
                + "links_cleaned.person_c.birth_date_max  = CONCAT( links_cleaned.person_c.birth_day_max , '-' , links_cleaned.person_c.birth_month_max , '-' , links_cleaned.person_c.birth_year_max ) ,"
                + "links_cleaned.person_c.mar_date_max    = CONCAT( links_cleaned.person_c.mar_day_max , '-' , links_cleaned.person_c.mar_month_max , '-' , links_cleaned.person_c.mar_year_max ) ,"
                + "links_cleaned.person_c.death_date_max  = CONCAT( links_cleaned.person_c.death_day_max , '-' , links_cleaned.person_c.death_month_max , '-' , links_cleaned.person_c.death_year_max ) ;";
//                + "WHERE "
//                + "links_cleaned.person_c.id_person = links_cleaned.person_c.id_person;";

        try {
            conCleaned.runQuery(query);
        } catch (Exception e) {
            funcShowMessage("An error occured while Creating full dates from parts: " + e.getMessage(), false, true);
        }
    }

    private void funcDaysSinceBegin() {

        String query1 = "UPDATE IGNORE person_c SET birth_min_days = DATEDIFF( date_format( str_to_date( birth_date_min, '%d-%m-%Y' ), '%Y-%m-%d' ) , '1-1-1' ) WHERE birth_date_min  NOT LIKE '0-%' AND birth_date_min   NOT LIKE '%-0-%'";
        String query2 = "UPDATE IGNORE person_c SET birth_max_days = DATEDIFF( date_format( str_to_date( birth_date_max, '%d-%m-%Y' ), '%Y-%m-%d' ) , '1-1-1' ) WHERE birth_date_max  NOT LIKE '0-%' AND birth_date_max   NOT LIKE '%-0-%'";
        String query3 = "UPDATE IGNORE person_c SET mar_min_days   = DATEDIFF( date_format( str_to_date( mar_date_min,   '%d-%m-%Y' ), '%Y-%m-%d' ) , '1-1-1' ) WHERE mar_date_min    NOT LIKE '0-%' AND mar_date_min     NOT LIKE '%-0-%'";
        String query4 = "UPDATE IGNORE person_c SET mar_max_days   = DATEDIFF( date_format( str_to_date( mar_date_max,   '%d-%m-%Y' ), '%Y-%m-%d' ) , '1-1-1' ) WHERE mar_date_max    NOT LIKE '0-%' AND mar_date_max     NOT LIKE '%-0-%'";
        String query5 = "UPDATE IGNORE person_c SET death_min_days = DATEDIFF( date_format( str_to_date( death_date_min, '%d-%m-%Y' ), '%Y-%m-%d' ) , '1-1-1' ) WHERE death_date_min  NOT LIKE '0-%' AND death_date_min   NOT LIKE '%-0-%'";
        String query6 = "UPDATE IGNORE person_c SET death_max_days = DATEDIFF( date_format( str_to_date( death_date_max, '%d-%m-%Y' ), '%Y-%m-%d' ) , '1-1-1' ) WHERE death_date_max  NOT LIKE '0-%' AND death_date_max   NOT LIKE '%-0-%'";

        String queryReg = "UPDATE registration_c SET "
                + "registration_days = DATEDIFF( date_format( str_to_date( registration_date, '%d-%m-%Y' ), '%Y-%m-%d' ) , '1-1-1' ) WHERE registration_date  NOT LIKE '0-%' AND registration_date   NOT LIKE '%-0-%'";

        try {
            funcShowMessage("q1", false, true);
            conCleaned.runQuery(query1);

            funcShowMessage("q2", false, true);
            conCleaned.runQuery(query2);

            funcShowMessage("q3", false, true);
            conCleaned.runQuery(query3);

            funcShowMessage("q4", false, true);
            conCleaned.runQuery(query4);

            funcShowMessage("q5", false, true);
            conCleaned.runQuery(query5);

            funcShowMessage("q6", false, true);
            conCleaned.runQuery(query6);

            funcShowMessage("q7", false, true);
            conCleaned.runQuery(queryReg);
        } catch (Exception e) {
            funcShowMessage("An error occured while computing days since 1-1-1: " + e.getMessage(), false, true);
        }
    }

    /**
     * @param sourceNo
     * @throws Exception
     */
    private void funcFillMinMaxArrays(String sourceNo) throws Exception {

        // Cleanen
        hpChildRegistration.clear();
        hpChildAge.clear();
        hpBrideRegistration.clear();
        hpBrideAge.clear();
        hpGroomRegistration.clear();
        hpGroomAge.clear();
        hpDeceasedRegistration.clear();
        hpDeceasedAge.clear();
        String query1 = "SELECT id_registration , age_year , age_month , age_week , age_day FROM person_c WHERE role = '1' AND id_source = " + sourceNo + " ORDER BY id_registration ASC";
        String query2 = "SELECT id_registration , age_year , age_month , age_week , age_day FROM person_c WHERE role = '7' AND id_source = " + sourceNo + " ORDER BY id_registration ASC";
        String query3 = "SELECT id_registration , age_year , age_month , age_week , age_day FROM person_c WHERE role = '4' AND id_source = " + sourceNo + " ORDER BY id_registration ASC";
        String query4 = "SELECT id_registration , age_year , age_month , age_week , age_day FROM person_c WHERE role = '10' AND id_source = " + sourceNo + " ORDER BY id_registration ASC";

        // run de queries
        ResultSet rs1 = conCleaned.runQueryWithResult(query1);
        ResultSet rs2 = conCleaned.runQueryWithResult(query2);
        ResultSet rs3 = conCleaned.runQueryWithResult(query3);
        ResultSet rs4 = conCleaned.runQueryWithResult(query4);

        while (rs1.next()) {
            hpChildRegistration.add(rs1.getInt("id_registration"));
            hpChildAge.add(rs1.getInt("age_year"));
            hpChildMonth.add(rs1.getInt("age_month"));
            hpChildWeek.add(rs1.getInt("age_week"));
            hpChildDay.add(rs1.getInt("age_day"));
        }
        while (rs2.next()) {
            hpGroomRegistration.add(rs2.getInt("id_registration"));
            hpGroomAge.add(rs2.getInt("age_year"));
            hpGroomMonth.add(rs2.getInt("age_month"));
            hpGroomWeek.add(rs2.getInt("age_week"));
            hpGroomDay.add(rs2.getInt("age_day"));
        }
        while (rs3.next()) {
            hpBrideRegistration.add(rs3.getInt("id_registration"));
            hpBrideAge.add(rs3.getInt("age_year"));
            hpBrideMonth.add(rs3.getInt("age_month"));
            hpBrideWeek.add(rs3.getInt("age_week"));
            hpBrideDay.add(rs3.getInt("age_day"));
        }
        while (rs4.next()) {
            hpDeceasedRegistration.add(rs4.getInt("id_registration"));
            hpDeceasedAge.add(rs4.getInt("age_year"));
            hpDeceasedMonth.add(rs4.getInt("age_month"));
            hpDeceasedWeek.add(rs4.getInt("age_week"));
            hpDeceasedDay.add(rs4.getInt("age_day"));
        }
    }

    /**
     * @throws Exception
     */
    private void createTempFamilyname() throws Exception {
        String ts = LinksSpecific.getTimeStamp2( "hh:mm:ss" );
        funcShowMessage( ts + " Creating familyname_t table", false, false );

        String query = "CREATE  TABLE links_temp.familyname_t ("
                + " person_id INT UNSIGNED NOT NULL AUTO_INCREMENT ,"
                + " familyname VARCHAR(80) NULL ,"
                + " PRIMARY KEY (person_id) );";

        conTemp.runQuery(query);
        funcShowMessage(".", false, true);
    }

    /**
     * @throws Exception
     */
    private void createTempFamilynameFile() throws Exception {
        String ts = LinksSpecific.getTimeStamp2( "hh:mm:ss" );
        funcShowMessage( ts + " Creating familyname_t csv", false, false );

        writerFamilyname = new java.io.FileWriter("familyname_t.csv");
        funcShowMessage(".", false, true);
    }

    /**
     * @throws Exception
     */
    private void loadFamilynameToTable() throws Exception {
        String ts = LinksSpecific.getTimeStamp2( "hh:mm:ss" );
        funcShowMessage( ts + " Loading CSV data into temp table", false, false );

        {
            String query = "LOAD DATA LOCAL INFILE 'familyname_t.csv' INTO TABLE familyname_t FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' ( person_id , familyname );";
            conTemp.runQuery(query);
        }
        funcShowMessage(".", false, true);
    }

    /**
     *
     */
    private void updateFamilynameToPersonC() throws Exception {
        String ts = LinksSpecific.getTimeStamp2( "hh:mm:ss" );
        funcShowMessage( ts + " Moving familynames from temp table to person_c", false, false );

        {
            String query = "UPDATE links_cleaned.person_c, links_temp.familyname_t"
                    + " SET links_cleaned.person_c.familyname = links_temp.familyname_t.familyname"
                    + " WHERE links_cleaned.person_c.id_person = links_temp.familyname_t.person_id;";

            conTemp.runQuery(query);
        }
        funcShowMessage(".", false, true);
    }

    public void removeFamilynameFile() throws Exception {
        String ts = LinksSpecific.getTimeStamp2( "hh:mm:ss" );
        funcShowMessage( ts + "Removing familyname_t csv", false, false );

        {
            java.io.File f = new java.io.File("familyname_t.csv");
            f.delete();
        }
        funcShowMessage(".", false, true);
    }

    public void removeFamilynameTable() throws Exception {
        String ts = LinksSpecific.getTimeStamp2( "hh:mm:ss" );
        funcShowMessage( ts + " Removing familyname_t table", false, false );

        String query = "DROP TABLE IF EXISTS familyname_t;";
        conTemp.runQuery(query);
        funcShowMessage(".", false, true);
    }

    /**
     * @throws Exception
     */
    private void createTempFirstname() throws Exception {
        String ts = LinksSpecific.getTimeStamp2( "hh:mm:ss" );
        funcShowMessage( "Creating firstname_t table", false, false );

        String query = "CREATE  TABLE links_temp.firstname_t ("
                + " person_id INT UNSIGNED NOT NULL AUTO_INCREMENT ,"
                + " firstname VARCHAR(80) NULL ,"
                + " PRIMARY KEY (person_id) );";

        conTemp.runQuery(query);
        funcShowMessage(".", false, true);
    }

    /**
     * @throws Exception
     */
    private void createTempFirstnameFile() throws Exception {
        String ts = LinksSpecific.getTimeStamp2( "hh:mm:ss" );
        funcShowMessage( "Creating firstname_t csv", false, false );

        writerFirstname = new java.io.FileWriter("firstname_t.csv");
        funcShowMessage(".", false, true);
    }

    /**
     * @throws Exception
     */
    private void loadFirstnameToTable() throws Exception {
        String ts = LinksSpecific.getTimeStamp2( "hh:mm:ss" );
        funcShowMessage( "Loading CSV data into temp table", false, false );

        {
            String query = "LOAD DATA LOCAL INFILE 'firstname_t.csv' INTO TABLE firstname_t FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' ( person_id , firstname );";
            conTemp.runQuery(query);
        }
        funcShowMessage(".", false, true);
    }

    /**
     *
     */
    private void updateFirstnameToPersonC() throws Exception {
        String ts = LinksSpecific.getTimeStamp2( "hh:mm:ss" );
        funcShowMessage( "Moving first names from temp table to person_c...", false, false );

        {
            String query = "UPDATE links_cleaned.person_c, links_temp.firstname_t"
                    + " SET links_cleaned.person_c.firstname = links_temp.firstname_t.firstname"
                    + " WHERE links_cleaned.person_c.id_person = links_temp.firstname_t.person_id;";

            conTemp.runQuery(query);
        }
        funcShowMessage("OK.", false, true);
    }

    /**
     * @throws Exception
     */
    public void removeFirstnameFile() throws Exception {
        String ts = LinksSpecific.getTimeStamp2( "hh:mm:ss" );
        funcShowMessage( "Removing firstname_t csv file", false, false );

        {
            java.io.File f = new java.io.File("firstname_t.csv");
            f.delete();
        }
        funcShowMessage(".", false, true);
    }

    /**
     * @throws Exception
     */
    public void removeFirstnameTable() throws Exception {
        String ts = LinksSpecific.getTimeStamp2( "hh:mm:ss" );
        funcShowMessage( "Removing firstname_t table", false, false );

        String query = "DROP TABLE IF EXISTS firstname_t;";
        conTemp.runQuery(query);
        funcShowMessage(".", false, true);
    }

    private void funcPostTasks() throws Exception {
        String ts = LinksSpecific.getTimeStamp2( "hh:mm:ss" );
        funcShowMessage( "Post tasks", false, false );

        String[] queries = {
                "UPDATE links_cleaned.person_c SET sex = 'v' WHERE role = 2;",
                "UPDATE links_cleaned.person_c SET sex = 'm' WHERE role = 3;",
                "UPDATE links_cleaned.person_c SET sex = 'v' WHERE role = 4;",
                "UPDATE links_cleaned.person_c SET sex = 'v' WHERE role = 5;",
                "UPDATE links_cleaned.person_c SET sex = 'm' WHERE role = 6;",
                "UPDATE links_cleaned.person_c SET sex = 'm' WHERE role = 7;",
                "UPDATE links_cleaned.person_c SET sex = 'v' WHERE role = 8;",
                "UPDATE links_cleaned.person_c SET sex = 'm' WHERE role = 9;",
                "UPDATE links_cleaned.person_c SET sex = '' WHERE sex <> 'm' AND sex <> 'v';",
                "CREATE  TABLE links_match.male ( id_registration INT NOT NULL , PRIMARY KEY (id_registration) );",
                "CREATE  TABLE links_match.female ( id_registration INT NOT NULL , PRIMARY KEY (id_registration) );",
                "INSERT INTO links_match.male(id_registration) SELECT id_registration FROM links_cleaned.person_c WHERE role = 10 AND sex = 'm';",
                "INSERT INTO links_match.female(id_registration) SELECT id_registration FROM links_cleaned.person_c WHERE role = 10 AND sex = 'v';",
                "UPDATE links_cleaned.person_c, links_match.male SET sex = 'v' WHERE links_match.male.id_registration = links_cleaned.person_c.id_registration AND role = 11;",
                "UPDATE links_cleaned.person_c, links_match.female SET sex = 'm' WHERE links_match.female.id_registration = links_cleaned.person_c.id_registration AND role = 11;",
                "DROP TABLE links_match.male;",
                "DROP TABLE links_match.female;",
                "UPDATE links_cleaned.person_c SET firstname = '' , stillborn = 1 WHERE firstname like '%ood%ebore%';",
                "UPDATE links_cleaned.person_c SET firstname = LOWER(firstname),  familyname = LOWER(familyname);",
                "UPDATE IGNORE links_cleaned.person_c "
                        + "SET "
                        + "age_year = FLOOR( DATEDIFF( STR_TO_DATE( mar_date , '%d-%m-%Y' ) , STR_TO_DATE( birth_date , '%d-%m-%Y') ) / 365 ) "
                        + "WHERE "
                        + "birth_date_valid = 1 "
                        + "AND "
                        + "mar_date_valid = 1 "
                        + "AND "
                        + "age_year is null "
                        + "AND "
                        + "( role = 7 OR role = 4 ) "
                        + "AND mar_date NOT LIKE '0-%' "
                        + "AND mar_date NOT LIKE '%-0-%' "
                        + "AND birth_date NOT LIKE '0-%' "
                        + "AND birth_date NOT LIKE '%-0-%' "
        };

        // Execute queries
        for (String s : queries) {
            conCleaned.runQuery(s);
        }
    }

    private void funcDeleteRows() throws Exception {
        String q1 = "DELETE FROM links_cleaned.person_c WHERE ( familyname = '' OR familyname is null ) AND ( firstname = '' OR firstname is null )";
        conCleaned.runQuery(q1);
    }

    private Connection getConnection(String dbName) throws Exception {

        String driver = "org.gjt.mm.mysql.Driver";

        String _url = "jdbc:mysql://" + this.url + "/" + dbName + "?dontTrackOpenResources=true";
        String username = user;
        String password = pass;

        Class.forName(driver);

        // Class.forName("externalModules.jdbcDriver.Driver").newInstance();

        Connection conn = DriverManager.getConnection(_url, username, password);

        return conn;
    }
}
