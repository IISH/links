package modulemain;

import java.lang.reflect.Method;

import java.io.File;
import java.io.FileWriter;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Queue;
import java.util.Set;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.swing.JTextArea;
import javax.swing.JTextField;

import dataset.ActRoleSet;
import dataset.Ages;
import dataset.ArrayListNonCase;
import dataset.DateYearMonthDaySet;
import dataset.DevinedMinMaxDatumSet;
import dataset.DoSet;
import dataset.MarriageYearPersonsSet;
import dataset.MinMaxDateSet;
import dataset.MinMaxYearSet;
import dataset.PersonC;
import dataset.RegistrationC;
import dataset.RelationSet;
import dataset.TableToArraysSet;

import connectors.MySqlConnector;
import enumdefinitions.TableType;
import enumdefinitions.TimeType;
import linksmanager.ManagerGui;
import general.Functions;
import general.PrintLogger;

/**
 * @author Omar Azouguagh
 * @author Fons Laan
 *
 * <p/>
 * FL-30-Jun-2014 Imported from OA backup
 * FL-28-Jul-2014 Timing functions
 * FL-20-Aug-2014 Occupation added
 * FL-01-Sep-2014 Latest change
 */
public class LinksCleaned extends Thread
{
    // Table to Array Sets
    private TableToArraysSet ttalAlias;
    private TableToArraysSet ttalFamilyname;
    private TableToArraysSet ttalFirstname;
    private TableToArraysSet ttalLocation;
    private TableToArraysSet ttalOccupation;
    private TableToArraysSet ttalPrepiece;
    private TableToArraysSet ttalStatusSex;
    private TableToArraysSet ttalSuffix;
    private TableToArraysSet ttalRegistration;      // formerly used in standardType()
    private TableToArraysSet ttalReport;

    private JTextField tbLOLClatestOutput;
    private JTextArea  taLOLCoutput;

    private String bronFilter = "";
    private String sourceFilter = "";
    private String bronFilterCleanPers = "";
    private String bronFilterOrigineelPers = "";
    private String bronFilterCleanReg = "";
    private String bronFilterOrigineelReg = "";

    private MySqlConnector conOr;           // remote reference db
    private MySqlConnector conLog;
    private MySqlConnector conCleaned;      // cleaned, from original
    private MySqlConnector conGeneral;      // local reference db
    private MySqlConnector conOriginal;     // original data from A2A
    private MySqlConnector conTemp;

    private Runtime r = Runtime.getRuntime();
    private String logTableName;
    private DoSet dos;

    private final static String SC_U = "u"; // Unknown Standard value assigned (although the original value is not valid)
    private final static String SC_X = "x"; //    X    Standard yet to be assigned
    private final static String SC_N = "n"; //    No   standard value assigned (original value is not valid)
    private final static String SC_Y = "y"; //    Yes  Standard value assigned (valid original value)

    // old links_base
    // needed by MinMaxDate functions
    private ArrayList<Integer> hpChildRegistration    = new ArrayList<Integer>();
    private ArrayList<Integer> hpChildAge             = new ArrayList<Integer>();
    private ArrayList<Integer> hpChildMonth           = new ArrayList<Integer>();
    private ArrayList<Integer> hpChildWeek            = new ArrayList<Integer>();
    private ArrayList<Integer> hpChildDay             = new ArrayList<Integer>();
    private ArrayList<Integer> hpBrideRegistration    = new ArrayList<Integer>();
    private ArrayList<Integer> hpBrideAge             = new ArrayList<Integer>();
    private ArrayList<Integer> hpBrideMonth           = new ArrayList<Integer>();
    private ArrayList<Integer> hpBrideWeek            = new ArrayList<Integer>();
    private ArrayList<Integer> hpBrideDay             = new ArrayList<Integer>();
    private ArrayList<Integer> hpGroomRegistration    = new ArrayList<Integer>();
    private ArrayList<Integer> hpGroomAge             = new ArrayList<Integer>();
    private ArrayList<Integer> hpGroomMonth           = new ArrayList<Integer>();
    private ArrayList<Integer> hpGroomWeek            = new ArrayList<Integer>();
    private ArrayList<Integer> hpGroomDay             = new ArrayList<Integer>();
    private ArrayList<Integer> hpDeceasedRegistration = new ArrayList<Integer>();
    private ArrayList<Integer> hpDeceasedAge          = new ArrayList<Integer>();
    private ArrayList<Integer> hpDeceasedMonth        = new ArrayList<Integer>();
    private ArrayList<Integer> hpDeceasedWeek         = new ArrayList<Integer>();
    private ArrayList<Integer> hpDeceasedDay          = new ArrayList<Integer>();

    private FileWriter writerFirstname;
    private FileWriter writerFamilyname;

    private ManagerGui mg;

    private String ref_url = "";       // reference db access
    private String ref_user = "";
    private String ref_pass = "";
    private String ref_db = "";

    private String url = "";           // links db's access
    private String user = "";
    private String pass = "";

    private int teller = 0;
    private int sourceId;

    //private int[] sources = { 10, 225 };      // Oegstgeest is already 231 !
    //private int[] sources = { 10, 999 };      // put in GUI or preferences file
    //private int[] sources;                      // fill with source_ids from links_original.person_o
    private int[] sources;                      // only needed as global by runMethod()
    //private int source_id_first =  10;
    //private int source_id_last  = 999;

    private String endl = ". OK.";              // ".";

    private PrintLogger plog;

    /**
     * Constructor
     *
     * @param ref_url
     * @param ref_user
     * @param ref_pass
     * @param ref_db
     * @param url
     * @param user
     * @param pass
     * @param sourceId
     * @param tbLOLClatestOutput
     * @param taLOLCoutput
     * @param dos
     * @param plog
     * @param mg
     */
    public LinksCleaned
    (
            String ref_url,
            String ref_user,
            String ref_pass,
            String ref_db,
            String url,
            String user,
            String pass,
            int sourceId,
            JTextField tbLOLClatestOutput,
            JTextArea taLOLCoutput,
            DoSet dos,
            general.PrintLogger plog,
            ManagerGui mg
    ) {
        this.sourceId = sourceId;

        this.ref_url = ref_url;
        this.ref_user = ref_user;
        this.ref_pass = ref_pass;
        this.ref_db = ref_db;

        this.url = url;
        this.user = user;
        this.pass = pass;

        this.tbLOLClatestOutput = tbLOLClatestOutput;
        this.taLOLCoutput = taLOLCoutput;
        this.dos = dos;
        this.plog = plog;
        this.mg = mg;

        String timestamp = LinksSpecific.getTimeStamp2( "HH:mm:ss" );
        System.out.println( timestamp + "  linksCleaned()" );

        System.out.println( "mysql_hsnref_hosturl:\t" + ref_url );
        System.out.println( "mysql_hsnref_username:\t" + ref_user );
        System.out.println( "mysql_hsnref_password:\t" + ref_pass );
        System.out.println( "mysql_hsnref_dbname:\t" + ref_db );
    }


    @Override
    /**
     * Begin
     */
    public void run()
    {
        showMessage( "LinksCleaned/run()", false, true );

        try {
            plog.show( "Links Match Manager 2.0" );

            long timeExpand = 0;
            long begintime = System.currentTimeMillis();
            logTableName = LinksSpecific.getLogTableName();

            clearTextFields();                                  // Clear output text fields on form
            connectToDatabases();                               // Create databases connectors
            createLogTable();                                   // Create log table with timestamp

            sources = getOrigSourceIds();                       // get source ids from links_original.registration_o

            if( sourceId == 0 ) {
                showMessage( "Processing source ids: ", false, true );
                String s = "";
                for( int i : sources ) { s = s + i + " "; }
                showMessage( s, false, true );
            }

            if( sourceId != 0 ) { setSourceFilters(); }         // Set source filters

            // links_general.ref_report contains 75 error definitions,
            // to be used when the normalization encounters errors
            showMessage( "Loading report table...", false, true );
            {
                ttalReport = new TableToArraysSet( conGeneral, conOr, "", "report" );
            }

            doRenewData( dos.isDoRenewData() );                 // GUI cb: Remove previous data

            doPreBasicNames( dos.isDoPreBasicNames() );         // GUI cb: Basic names temp

            doRemarks( dos.isDoRemarks() );                     // GUI cb: Parse remarks

            doNames( dos.isDoNames() );                         // GUI cb: Names

            doLocations( dos.isDoLocations() );                 // GUI cb: Locations

            doStatusSex( dos.isDoStatusSex() );                 // GUI cb: Status and Sex

            doRegType( dos.isDoRegType() );                     // GUI cb: Registration Type

            doSequence( dos.isDoSequence() );                   // GUI cb: Sequence

            doRelation( dos.isDoRelation() );                   // GUI cb: Relation

            doAgeYear( dos.isDoAgeYear() );                     // GUI cb: Year Age

            doRole( dos.isDoRole() );                           // GUI cb: Role

            doOccupation( dos.isDoOccupation() );               // GUI cb: Occupation

            doDates( dos.isDoDates() );                         // GUI cb: Dates

            doMinMaxDate( dos.isDoMinMaxDate() );               // GUI cb: Min Max Date

            doMinMaxMarriage( dos.isDoMinMaxMarriage() );       // GUI cb: Min Max Marriage

            doPartsToFullDate( dos.isDoPartsToFullDate() );     // GUI cb: Parts to Full Date

            doDaysSinceBegin( dos.isDoDaysSinceBegin() );       // GUI cb: Days since begin

            doPostTasks( dos.isDoPostTasks() );                 // GUI cb: Post Tasks

            // Close db connections
            conOriginal.close();
            conLog.close();
            conCleaned.close();
            conGeneral.close();
            conTemp.close();

            doPrematch( dos.isDoPrematch() );                   // GUI cb: Run PreMatch

            // Total time
            timeExpand = System.currentTimeMillis() - begintime;
            int iTimeEx = (int) (timeExpand / 1000);

            showMessage("Conversion from Original to Cleaned is done; Total time: " + LinksSpecific.stopWatch(iTimeEx), false, true);

        } catch (Exception ex) {
            showMessage("Error: " + ex.getMessage(), false, true);
        }
    } // run


     /*---< functions corresponding to GUI Cleaning options >-----------------*/

    /**
     * Remove previous data
     * @param go
     * @throws Exception
     */
    private void doRenewData(boolean go) throws Exception
    {
        String funcname = "doRenewData";
        if( !go ) {
            showMessage( "Skipping " + funcname, false, true );
            return;
        }

        long timeStart = System.currentTimeMillis();
        showMessage( funcname, false, true );

        // Delete all existing cleaned data
        // Create queries
        String deletePerson = "DELETE FROM person_c" + bronFilter;
        String deleteRegistration = "DELETE FROM registration_c" + bronFilter;

        // Execute queries
        showMessage("Deleting previous data", false, true);
        conCleaned.runQuery(deletePerson);
        conCleaned.runQuery(deleteRegistration);

        // Copy selected columns links_original data to links_cleaned
        // Create queries
        showMessage("Copying person keys to links_cleaned", false, true);
        String keysPerson = ""
            + "INSERT INTO links_cleaned.person_c "
            +       "( id_person, id_registration, id_source, registration_maintype, id_person_o ) "
            + " SELECT id_person, id_registration, id_source, registration_maintype, id_person_o "
            + "FROM links_original.person_o" + bronFilterOrigineelPers;
        //System.out.println( keysPerson );
        conCleaned.runQuery( keysPerson );              // Execute query

        showMessage("Copying registration keys to links_cleaned", false, true);
        String keysRegistration = ""
            + "INSERT INTO links_cleaned.registration_c "
            +      "( id_registration, id_source, id_persist_registration, id_orig_registration, registration_maintype, registration_seq ) "
            + "SELECT id_registration, id_source, id_persist_registration, id_orig_registration, registration_maintype, registration_seq "
            + "FROM links_original.registration_o" + bronFilterOrigineelReg;
        //System.out.println( keysRegistration );
        conCleaned.runQuery( keysRegistration );        // Execute query

        elapsedShowMessage( funcname, timeStart, System.currentTimeMillis() );
    } // doRenewData


    /**
     * Basic names temp
     * @param go
     * @throws Exception
     */
    private void doPreBasicNames( boolean go ) throws Exception
    {
        String funcname = "doPreBasicNames";
        if( !go ) {
            showMessage( "Skipping " + funcname, false, true );
            return;
        }

        long timeStart = System.currentTimeMillis();
        showMessage( funcname, false, true );

        // load the ref tables
        showMessage( "Loading reference tables: firstname/familyname/prepiece/suffix...", false, true );
        {
            //ttalFirstname  = new TableToArraysSet( conGeneral, "original", "firstname" );
            //ttalFamilyname = new TableToArraysSet( conGeneral, "original", "familyname" );
            ttalPrepiece = new TableToArraysSet(conGeneral, conOr, "original", "prepiece");
            ttalSuffix = new TableToArraysSet(conGeneral, conOr, "original", "suffix");
            ttalAlias = new TableToArraysSet(conGeneral, conOr, "original", "alias");
        }

        // Firstname
        if (doesTableExist(conTemp, "links_temp", "firstname_t")) {
            showMessage("Deleting table links_temp.firstname_t", false, true);
            dropTable(conTemp, "links_temp", "firstname_t");
        }

        createTempFirstnameTable();
        createTempFirstnameFile();
        ttalFirstname = new TableToArraysSet(conGeneral, conOr, "original", "firstname");

        System.out.println("before");
        runMethod("standardFirstname");
        System.out.println("after");

        ttalFirstname.updateTable();
        ttalFirstname.free();
        writerFirstname.close();
        loadFirstnameToTable();
        updateFirstnameToPersonC();
        removeFirstnameFile();
        removeFirstnameTable();

        // Familyname
        if (doesTableExist(conTemp, "links_temp", "familyname_t")) {
            showMessage("Deleting table links_temp.familyname_t", false, true);
            dropTable(conTemp, "links_temp", "familyname_t");
        }

        createTempFamilynameTable();
        createTempFamilynameFile();
        ttalFamilyname = new TableToArraysSet(conGeneral, conOr, "original", "familyname");
        runMethod("standardFamilyname");
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

        showMessage("Converting names to lowercase", false, false);
        {
            String qLower = "UPDATE links_cleaned.person_c SET firstname = LOWER(firstname),  familyname = LOWER(familyname);";
            conCleaned.runQuery(qLower);
        }
        showMessage(endl, false, true);

        // Prematch
        LinksPrematch lpm = new LinksPrematch(url, user, pass, taLOLCoutput, tbLOLClatestOutput);

        // temp
        showMessage("Splitting names", false, false);
        {
            lpm.doSplitName();
        }
        showMessage(endl, false, true);

        showMessage("Creating unique name tables", false, false);
        {
            lpm.doUniqueNameTablesTemp();
        }

        showMessage("Basic names tables", false, false);
        {
            lpm.doBasicName();
        }
        showMessage(endl, false, true);

        elapsedShowMessage( funcname, timeStart, System.currentTimeMillis() );
    } // doPreBasicNames


    /**
     * Parse remarks
     * @param go
     * @throws Exception
     */
    private void doRemarks( boolean go ) throws Exception
    {
        String funcname = "doRemarks";
        if( !go ) {
            showMessage( "Skipping " + funcname, false, true );
            return;
        }

        long timeStart = System.currentTimeMillis();
        showMessage( funcname, false, true );

        // load all refs used by remarks Parser
        showMessage( "Loading reference tables: location/occupation...", false, true );
        {
            //ttalLocation = new TableToArraysSet(conGeneral, "original", "location");
            //ttalOccupation = new TableToArraysSet(conGeneral, "original", "occupation");
        }

        runMethod("scanRemarks");

        showMessage( "Updating reference tables: " + "location/occupation" + "...", false, true );
        {
            //ttalLocation.updateTable();
            //ttalOccupation.updateTable();
        }

        elapsedShowMessage( funcname, timeStart, System.currentTimeMillis() );
    } // doRemarks


    /**
     * Names
     * @param go
     * @throws Exception
     */
    private void doNames( boolean go ) throws Exception
    {
        String funcname = "doNames";
        if( !go ) {
            showMessage( "Skipping " + funcname, false, true );
            return;
        }

        long timeStart = System.currentTimeMillis();
        showMessage( funcname, false, true );

        String mmss = "";
        String msg = "";
        String ts = "";                               // timestamp

        long start = 0;
        long stop = 0;

        ts = LinksSpecific.getTimeStamp2("HH:mm:ss");
        System.out.println(ts + " dos.isDoNames");

        // Loading reference tables
        start = System.currentTimeMillis();
        msg = "Loading name reference tables";
        showMessage( msg + "...", false, true );
        {
            ttalPrepiece = new TableToArraysSet( conGeneral, conOr, "original", "prepiece" );
            ttalSuffix   = new TableToArraysSet( conGeneral, conOr, "original", "suffix" );
            ttalAlias    = new TableToArraysSet( conGeneral, conOr, "original", "alias" );
        }
        showTimingMessage( msg, start );

        // First name
        start = System.currentTimeMillis();
        if( doesTableExist(conTemp, "links_temp", "firstname_t") ) {
            showMessage( "Deleting table links_temp.firstname_t", false, true );
            dropTable( conTemp, "links_temp", "firstname_t" );
        }

        createTempFirstnameTable();
        createTempFirstnameFile();
        String IndexField = "original";
        String tableName = "firstname";
        showMessage( "TableToArraysSet: " + IndexField + ", " + tableName, false, true );
        ttalFirstname = new TableToArraysSet( conGeneral, conOr, "original", "firstname" );
        showTimingMessage( "TableToArraysSet", start );

        start = System.currentTimeMillis();
        msg = "standardFirstname";
        showMessage( msg + "...", false, true );
        runMethod( "standardFirstname" );
        showTimingMessage( "standardFirstname", start );

        start = System.currentTimeMillis();
        ttalFirstname.updateTable();
        ttalFirstname.free();
        writerFirstname.close();
        loadFirstnameToTable();
        updateFirstnameToPersonC();
        removeFirstnameFile();
        removeFirstnameTable();
        showTimingMessage( "remains Firstname", start );

        // Family name
        start = System.currentTimeMillis();
        if( doesTableExist( conTemp, "links_temp", "familyname_t" ) ) {
            showMessage( "Deleting table links_temp.familyname_t", false, true );
            dropTable( conTemp, "links_temp", "familyname_t" );
        }

        createTempFamilynameTable();
        createTempFamilynameFile();
        tableName = "familyname";
        showMessage( "TableToArraysSet: " + IndexField + ", " + tableName, false, true );
        ttalFamilyname = new TableToArraysSet( conGeneral, conOr, "original", "familyname" );
        showTimingMessage( "TableToArraysSet", start );

        start = System.currentTimeMillis();
        msg = "standardFamilyname";
        showMessage( msg + "...", false, true );
        runMethod( "standardFamilyname" );
        showTimingMessage( msg, start );

        start = System.currentTimeMillis();
        msg = "remains Familyname";
        showMessage( msg + "...", false, true );
        ttalFamilyname.updateTable();
        ttalFamilyname.free();
        writerFamilyname.close();
        loadFamilynameToTable();
        updateFamilynameToPersonC();
        removeFamilynameFile();
        removeFamilynameTable();
        showTimingMessage( msg, start );

        // KM: Do not delete here.
        //showMessage("Skipping deleting empty links_cleaned.person_c records.", false, true);
        //funcDeleteRows();               // Delete records with empty firstname and empty familyname

        // Names to lowercase
        start = System.currentTimeMillis();
        msg = "Converting names to lowercase";
        showMessage( msg + "...", false, true ) ;
        {
            String qLower = "UPDATE links_cleaned.person_c SET firstname = LOWER(firstname),  familyname = LOWER(familyname);";
            conCleaned.runQuery(qLower);
        }

        runMethod( "standardPrepiece" );
        runMethod( "standardSuffix" );
        showTimingMessage( msg, start );

        // Update reference
        start = System.currentTimeMillis();
        msg = "Updating reference tables: prepiece/suffix/alias...";
        showMessage( msg + "...", false, true );
        {
            ttalPrepiece.updateTable();
            ttalSuffix.updateTable();
            ttalAlias.updateTable();
        }
        showTimingMessage( msg, start );

        elapsedShowMessage( funcname, timeStart, System.currentTimeMillis() );
    } // doNames


    /**
     * Locations
     * @param go
     * @throws Exception
     */
    private void doLocations( boolean go ) throws Exception
    {
        String funcname = "doLocations";
        if( !go ) {
            showMessage( "Skipping " + funcname, false, true );
            return;
        }

        long timeStart = System.currentTimeMillis();
        showMessage( funcname, false, true );

        showMessage( "Loading reference table: location...", false, true );
        {
            ttalLocation = new TableToArraysSet( conGeneral, conOr, "original", "location" );
        }

        runMethod("standardRegistrationLocation");
        runMethod( "standardBirthLocation" );
        runMethod( "standardMarLocation" );
        runMethod( "standardDeathLocation" );

        showMessage( "Updating reference table: location...", false, true );
        {
            ttalLocation.updateTable();
        }

        elapsedShowMessage( funcname, timeStart, System.currentTimeMillis() );
    } // doLocations


    /**
     * Status and Sex
     * @param go
     * @throws Exception
     */
    private void doStatusSex( boolean go ) throws Exception
    {
        String funcname = "doStatusSex";
        if( !go ) {
            showMessage( "Skipping " + funcname, false, true );
            return;
        }

        long timeStart = System.currentTimeMillis();
        showMessage( funcname, false, true );

        showMessage( "Loading reference table: status_sex...", false, true );
        {
            ttalStatusSex = new TableToArraysSet( conGeneral, conOr, "original", "status_sex" );
        }

        runMethod("standardSex");
        runMethod( "standardStatusSex" );

        showMessage( "Updating reference table status_sex...", false, true );
        {
            ttalStatusSex.updateTable();
        }

        elapsedShowMessage( funcname, timeStart, System.currentTimeMillis() );
    } // doStatusSex


    /**
     * Registration Type
     * @param go
     * @throws Exception
     */
    private void doRegType( boolean go ) throws Exception
    {
        String funcname = "doType";
        if( !go ) {
            showMessage( "Skipping " + funcname, false, true );
            return;
        }

        long timeStart = System.currentTimeMillis();
        showMessage( funcname, false, true );

        runMethod( "standardType" );

        elapsedShowMessage( funcname, timeStart, System.currentTimeMillis() );
    } // doRegType


    /**
     * Sequence
     * @param go
     * @throws Exception
     */
    private void doSequence( boolean go ) throws Exception
    {
        String funcname = "doSequence";
        if( !go ) {
            showMessage( "Skipping " + funcname, false, true );
            return;
        }

        long timeStart = System.currentTimeMillis();
        showMessage( funcname, false, true );

        runMethod( "standardSequence" );

        elapsedShowMessage( funcname, timeStart, System.currentTimeMillis() );
    } // doSequence


    /**
     * Relation
     * @param go
     * @throws Exception
     */
    private void doRelation( boolean go ) throws Exception
    {
        String funcname = "doRelation";
        if( !go ) {
            showMessage( "Skipping " + funcname, false, true );
            return;
        }

        long timeStart = System.currentTimeMillis();
        showMessage( funcname, false, true );

        runMethod( "funcRelation" );

        elapsedShowMessage( funcname, timeStart, System.currentTimeMillis() );
    } // doRelation


    /**
     * Year Age
     * @param go
     * @throws Exception
     */
    private void doAgeYear( boolean go ) throws Exception
    {
        String funcname = "doAgeYear";
        if( !go ) {
            showMessage( "Skipping " + funcname, false, true );
            return;
        }

        long timeStart = System.currentTimeMillis();
        showMessage( funcname, false, true );

        runMethod( "standardYearAge" );

        elapsedShowMessage( funcname, timeStart, System.currentTimeMillis() );
    } // doAgeYear


    /**
     * Role
     * @param go
     * @throws Exception
     */
    private void doRole( boolean go ) throws Exception
    {
        String funcname = "doRole";
        if( !go ) {
            showMessage( "Skipping " + funcname, false, true );
            return;
        }

        long timeStart = System.currentTimeMillis();
        showMessage( funcname, false, true );

        showMessage( "Running standardRole on all sources...", false, true );
        standardRole();

        elapsedShowMessage( funcname, timeStart, System.currentTimeMillis() );
    } // doRole


    /**
     * Occupation
     * @param go
     * @throws Exception
     */
    private void doOccupation( boolean go ) throws Exception
    {
        String funcname = "doOccupation";
        if( !go ) {
            showMessage( "Skipping " + funcname, false, true );
            return;
        }

        long timeStart = System.currentTimeMillis();
        showMessage( funcname, false, true );

        showMessage( "Loading reference table: occupation...", false, true );
        {
            ttalOccupation = new TableToArraysSet( conGeneral, conOr, "original", "occupation" );
        }

        runMethod("standardOccupation");

        showMessage( "Updating reference table: occupation...", false, true );
        ttalOccupation.updateTable();
        ttalOccupation.free();

        elapsedShowMessage(funcname, timeStart, System.currentTimeMillis());
    } // doOccupation


    /**
     * Dates
     * @param go
     * @throws Exception
     */
    private void doDates( boolean go ) throws Exception
    {
        String funcname = "doDates";
        if( !go ) {
            showMessage( "Skipping " + funcname, false, true );
            return;
        }

        long timeStart = System.currentTimeMillis();
        showMessage( funcname, false, true );

        showMessage( "Running Date functions on all sources...", false, true );

        // Clean dates
        runMethod( "standardRegistrationDate" );

        // Clean
        standardDate( "birth" );
        standardDate( "mar" );
        standardDate( "death" );

        // Fill empty dates with register dates
        funcFlagBirthDate();
        funcFlagMarriageDate();
        funcFlagDeathDate();

        standardFlaggedDate( "birth" );
        standardFlaggedDate( "mar" );
        standardFlaggedDate( "death" );

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

        elapsedShowMessage( funcname, timeStart, System.currentTimeMillis() );
    } // doDates


    /**
     * Min Max Marriage
     * @param go
     * @throws Exception
     */
    private void doMinMaxMarriage( boolean go ) throws Exception
    {
        String funcname = "doMinMaxMarriage";
        if( !go ) {
            showMessage( "Skipping " + funcname, false, true );
            return;
        }

        long timeStart = System.currentTimeMillis();
        showMessage( funcname, false, true );

        try {
            // loading ref
            ResultSet refMinMaxMarriageYear = conGeneral.runQueryWithResult("SELECT * FROM ref_minmax_marriageyear");

            if (bronFilter.isEmpty()) {
                for (int i : sources) {
                    showMessage("Running funcMinMaxMarriageYear for source: " + i + "...", false, false);
                    {
                        funcMinMaxMarriageYear(funcSetMarriageYear(i + ""), refMinMaxMarriageYear);
                    }
                    showMessage(endl, false, true);
                }

            } else {
                showMessage("Running funcMinMaxMarriageYear...", false, false);
                {
                    funcMinMaxMarriageYear(funcSetMarriageYear(this.sourceId + ""), refMinMaxMarriageYear);
                }
                showMessage(endl, false, true);
            }
        } catch (Exception e) {
            showMessage("An error occured while running Min max Marriage date, properly ref_minmax_marriageyear error: " + e.getMessage(), false, true);
        }

        elapsedShowMessage( funcname, timeStart, System.currentTimeMillis() );
    } // doMinMaxMarriage


    /**
     * Parts to Full Date
     * @param go
     * @throws Exception
     */
    private void doPartsToFullDate( boolean go ) throws Exception
    {
        String funcname = "doPartsToFullDate";
        if( !go ) {
            showMessage( "Skipping " + funcname, false, true );
            return;
        }

        long timeStart = System.currentTimeMillis();
        showMessage( funcname, false, true );

        showMessage("Running func Part to Date on all sources...", false, true );
        funcPartsToDate();

        elapsedShowMessage( funcname, timeStart, System.currentTimeMillis() );
    } // doPartsToFullDate


    /**
     * Days since begin
     * @param go
     * @throws Exception
     */
    private void doDaysSinceBegin( boolean go ) throws Exception
    {
        String funcname = "doDaysSinceBegin";
        if( !go ) {
            showMessage( "Skipping " + funcname, false, true );
            return;
        }

        long timeStart = System.currentTimeMillis();
        showMessage( funcname, false, true );

        showMessage("Running func Days since begin on all sources...", false, false);
        funcDaysSinceBegin();

        elapsedShowMessage( funcname, timeStart, System.currentTimeMillis() );
    } // doDaysSinceBegin


    /**
     * Post Tasks
     * @param go
     * @throws Exception
     */
    private void doPostTasks( boolean go ) throws Exception
    {
        String funcname = "doPostTasks";
        if( !go ) {
            showMessage( "Skipping " + funcname, false, true );
            return;
        }

        long timeStart = System.currentTimeMillis();
        showMessage( funcname, false, true );

        showMessage( "Running func post tasks all sources...", false, true );
        funcPostTasks();

        elapsedShowMessage( funcname, timeStart, System.currentTimeMillis() );
    } // doPostTasks


    /**
     * Run PreMatch
     * @param go
     * @throws Exception
     */
    private void doPrematch( boolean go ) throws Exception
    {
        String funcname = "doPrematch";
        if( !go ) {
            showMessage( "Skipping " + funcname, false, true );
            return;
        }

        long timeStart = System.currentTimeMillis();
        showMessage( funcname, false, true );

        showMessage("Running PREMATCH...", false, false);
        mg.firePrematch();

        elapsedShowMessage( funcname, timeStart, System.currentTimeMillis() );
    } // doPrematch


     /*---< Standardizing functions >-----------------------------------------*/

    /**
     * @param id
     * @param id_souce
     * @param name
     * @return
     */
    private String standardAlias( int id, String id_souce, String name ) throws Exception
    {
        dataset.ArrayListNonCase ag = ttalAlias.getArray("original");

        // to lowercase
        name = name.toLowerCase();

        for (Object ags : ag) {

            String keyword = " " + ags.toString().toLowerCase() + " ";

            if (name.contains(" " + keyword + " ")) {

                // EC 17
                addToReportPerson(id, id_souce, 17, name);

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
    } // standardAlias


    /**
     * @param sourceNo
     */
    public void standardBirthLocation( String sourceNo )
    {
        String startQuery;
        String id_source;

        if (sourceNo.isEmpty()) {
            startQuery = "SELECT id_person , birth_location FROM person_o" + bronFilter + " AND birth_location <> ''";
            id_source = this.sourceId + "";
        } else {
            startQuery = "SELECT id_person , birth_location FROM person_o WHERE id_source = " + sourceNo + " AND birth_location <> ''";
            id_source = sourceNo;
        }

        try {
            ResultSet rs = conOriginal.runQueryWithResult(startQuery);

            standardLocation(rs, "id_person", "birth_location", "birth_location", id_source, TableType.PERSON);
        } catch (Exception e) {
            showMessage(e.getMessage(), false, true);
        }
    } // standardBirthLocation


    /**
     * @param type
     */
    public void standardDate( String type )
    {
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
                    showMessage(counter + "", true, true);
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
                    addToReportPerson(id_person, id_source + "", 211, dymd.getReports());

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
            showMessage(counter + " An error occured while cleaning " + type + " date: " + e.getMessage(), false, true);
        }
    } // standardDate


    /**
     * @param sourceNo
     */
    public void standardDeathLocation( String sourceNo )
    {
        String startQuery;
        String id_source;

        if (sourceNo.isEmpty()) {
            startQuery = "SELECT id_person , death_location FROM person_o" + bronFilter + " AND death_location <> ''";
            id_source = this.sourceId + "";
        } else {
            startQuery = "SELECT id_person , death_location FROM person_o WHERE id_source = " + sourceNo + " AND death_location <> ''";
            id_source = sourceNo;
        }

        try {
            ResultSet rs = conOriginal.runQueryWithResult(startQuery);
            standardLocation(rs, "id_person", "death_location", "death_location", id_source, TableType.PERSON);
        } catch (Exception e) {
            showMessage(e.getMessage(), false, true);
        }
    } // standardDeathLocation


    /**
     * @param sourceNo
     * @throws Exception
     */
    public void standardFirstname( String sourceNo )
    {
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
                id_source = this.sourceId + "";
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
                    showMessage(counter + " of " + total, true, true);
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
                    String nameNoAlias = standardAlias(id_person, id_source, firstname);

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
                        addToReportPerson(id_person, id_source, 1103, "");
                    }

                    // loop through names
                    for (int i = 0; i < preList.size(); i++) {

                        // Does this aprt exists in ref_name?
                        if (ttalFirstname.originalExists(preList.get(i))) {

                            // Check the standard code
                            String standard_code = ttalFirstname.getStandardCodeByOriginal(preList.get(i));
                            if (standard_code.equals(SC_Y)) {
                                postList.add(ttalFirstname.getStandardByOriginal(preList.get(i)));
                            } else if (standard_code.equals(SC_U)) { // EC 1100
                                addToReportPerson(id_person, id_source, 1100, preList.get(i));
                                postList.add(ttalFirstname.getStandardByOriginal(preList.get(i)));
                            } else if (standard_code.equals(SC_N)) { // EC 1105
                                addToReportPerson(id_person, id_source, 1105, preList.get(i));
                            } else if (standard_code.equals(SC_X)) { // EC 1109
                                addToReportPerson(id_person, id_source, 1109, preList.get(i));
                                postList.add(preList.get(i));
                            } else {// EC 1100
                                addToReportPerson(id_person, id_source, 1100, preList.get(i));
                            }
                        } // name does not exists in ref_firtname
                        else {

                            // check on invalid token
                            String nameNoInvalidChars = funcCleanNaam(preList.get(i));

                            // name contains invalid chars ?
                            if (!preList.get(i).equalsIgnoreCase(nameNoInvalidChars)) {

                                // EC 1104
                                addToReportPerson(id_person, id_source, 1104, preList.get(i));

                                // Check if name exists in ref
                                // Does this aprt exists in ref_name?
                                if (ttalFirstname.originalExists(nameNoInvalidChars)) {
                                    // Check the standard code
                                    String standard_code = ttalFirstname.getStandardCodeByOriginal(nameNoInvalidChars);
                                    if (standard_code.equals(SC_Y)) {
                                        postList.add(ttalFirstname.getStandardByOriginal(nameNoInvalidChars));
                                    } else if (standard_code.equals(SC_U)) { // EC 1100
                                        addToReportPerson(id_person, id_source, 1100, nameNoInvalidChars);
                                        postList.add(ttalFirstname.getStandardByOriginal(nameNoInvalidChars));
                                    } else if (standard_code.equals(SC_N)) { // EC 1105
                                        addToReportPerson(id_person, id_source, 1105, nameNoInvalidChars);
                                    } else if (standard_code.equals(SC_X)) { // EC 1109
                                        addToReportPerson(id_person, id_source, 1109, nameNoInvalidChars);
                                        postList.add(nameNoInvalidChars);
                                    } else { // EC 1100, standard_code not invalid
                                        addToReportPerson(id_person, id_source, 1100, nameNoInvalidChars);
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
                                        addToReportPerson(id_person, id_source, 1106, nameNoInvalidChars);

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
                                    addToReportPerson(id_person, id_source, 1107, nameNoInvalidChars);

                                }

                                // last check on ref
                                if (ttalFirstname.originalExists(nameNoPieces)) {
                                    // Check the standard code
                                    String standard_code = ttalFirstname.getStandardCodeByOriginal(nameNoPieces);
                                    if (standard_code.equals(SC_Y)) {
                                        postList.add(ttalFirstname.getStandardByOriginal(nameNoPieces));
                                    } else if (standard_code.equals(SC_U)) { // EC 1100
                                        addToReportPerson(id_person, id_source, 1100, nameNoPieces);
                                        postList.add(ttalFirstname.getStandardByOriginal(nameNoPieces));
                                    } else if (standard_code.equals(SC_N)) { // EC 1105
                                        addToReportPerson(id_person, id_source, 1105, nameNoPieces);
                                    } else if (standard_code.equals(SC_X)) { // EC 1109
                                        addToReportPerson(id_person, id_source, 1109, nameNoPieces);
                                        postList.add(nameNoPieces);
                                    } else { // EC 1100, standard_code not invalid
                                        addToReportPerson(id_person, id_source, 1100, nameNoPieces);
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
                                        addToReportPerson(id_person, id_source, 1106, nameNoInvalidChars);

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
                                    addToReportPerson(id_person, id_source, 1107, nameNoInvalidChars);

                                }

                                // last check on ref
                                if (ttalFirstname.originalExists(nameNoPieces)) {
                                    // Check the standard code
                                    String standard_code = ttalFirstname.getStandardCodeByOriginal(nameNoPieces);
                                    if (standard_code.equals(SC_Y)) {
                                        postList.add(ttalFirstname.getStandardByOriginal(nameNoPieces));
                                    } else if (standard_code.equals(SC_U)) { // EC 1100
                                        addToReportPerson(id_person, id_source, 1100, nameNoPieces);
                                        postList.add(ttalFirstname.getStandardByOriginal(nameNoPieces));
                                    } else if (standard_code.equals(SC_N)) { // EC 1105
                                        addToReportPerson(id_person, id_source, 1105, nameNoPieces);
                                    } else if (standard_code.equals(SC_X)) { // EC 1109
                                        addToReportPerson(id_person, id_source, 1109, nameNoPieces);
                                        postList.add(nameNoPieces);
                                    } else { // EC 1100, standard_code not invalid
                                        addToReportPerson(id_person, id_source, 1100, nameNoPieces);
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
                    addToReportPerson(id_person, id_source, 1101, "");
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
            showMessage(counter + " An error occured while cleaning Firstname: " + e.getMessage(), false, true);
        }
    } // standardFirstname


    /**
     * @param sourceNo
     * @throws Exception
     */
    public void standardFamilyname( String sourceNo )
    {
        int counter = 0;
        int step = 10000;
        int stepstate = step;

        try {

            String startQuery;
            String id_source;

            if (sourceNo.isEmpty()) {
                startQuery = "SELECT id_person , familyname FROM person_o" + bronFilter;
                id_source = this.sourceId + "";
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
                    showMessage(counter + " of " + total, true, true);
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

                        } else if (standard_code.equals(SC_U)) {

                            // EC 1000
                            addToReportPerson(id_person, id_source, 1000, familyname);

                            writerFamilyname.write(id_person + "," + ttalFamilyname.getStandardByOriginal(familyname).toLowerCase() + "\n");

                        } else if (standard_code.equals(SC_N)) {

                            // EC 1005
                            addToReportPerson(id_person, id_source, 1005, familyname);
                        } else if (standard_code.equals(SC_X)) {

                            // EC 1009
                            addToReportPerson(id_person, id_source, 1009, familyname);

                            writerFamilyname.write(id_person + "," + familyname.toLowerCase() + "\n");
                        } else {

                            // EC 1010
                            addToReportPerson(id_person, id_source, 1010, familyname);
                        }
                    } // Familyname does not exists in ref_familyname
                    else {

                        // EC 1002
                        addToReportPerson(id_person, id_source, 1002, familyname);

                        String nameNoSerriedSpaces = familyname.replaceAll(" [ ]+", " ");

                        // Family name contains two or more serried spaces?
                        if (!nameNoSerriedSpaces.equalsIgnoreCase(familyname)) {

                            // EC 1003
                            addToReportPerson(id_person, id_source, 1003, familyname);
                        }

                        String nameNoInvalidChars = funcCleanNaam(nameNoSerriedSpaces);

                        // Family name contains invalid chars ?
                        if (!nameNoSerriedSpaces.equalsIgnoreCase(nameNoInvalidChars)) {

                            // EC 1004
                            addToReportPerson(id_person, id_source, 1004, familyname);
                        }

                        // check if name has prepieces
                        String nameNoPrePiece = funcNamePrepiece(nameNoInvalidChars, id_person);

                        // Family name contains invalid chars ?
                        if (!nameNoPrePiece.equalsIgnoreCase(nameNoInvalidChars)) {

                            // EC 1008
                            addToReportPerson(id_person, id_source, 1008, familyname);
                        }

                        // Ckeck on Aliasses
                        String nameNoAlias = standardAlias(id_person, id_source, nameNoPrePiece);

                        // Check on suffix
                        ArrayListNonCase sfxO = ttalSuffix.getArray("original");

                        for (int i = 0; i < sfxO.size(); i++) {

                            if (nameNoAlias.endsWith(" " + sfxO.get(i).toString())) {

                                // EC 1006
                                addToReportPerson(id_person, id_source, 1006, nameNoAlias);

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
                            } else if (standard_code.equals(SC_U)) {

                                // EC 1000
                                addToReportPerson(id_person, id_source, 1000, nameNoSuffix);

                                writerFamilyname.write(id_person + "," + ttalFamilyname.getStandardByOriginal(nameNoSuffix).toLowerCase() + "\n");
                            } else if (standard_code.equals(SC_N)) {

                                // EC 1005
                                addToReportPerson(id_person, id_source, 1005, nameNoSuffix);
                            } else if (standard_code.equals(SC_X)) {

                                // EC 1009
                                addToReportPerson(id_person, id_source, 1009, nameNoSuffix);

                                writerFamilyname.write(id_person + "," + nameNoSuffix.toLowerCase() + "\n");
                            } else {
                                // EC 1010
                                addToReportPerson(id_person, id_source, 1010, nameNoSuffix);
                            }
                        } else {
                            // Familie is nieuw en wordt toegevoegd
                            ttalFamilyname.addOriginal(nameNoSuffix);

                            // EC 1009
                            addToReportPerson(id_person, id_source, 1009, nameNoSuffix);

                            writerFamilyname.write(id_person + "," + nameNoSuffix.trim().toLowerCase() + "\n");

                        }
                    }
                } // Familyname empty
                else {

                    // EC 1001
                    addToReportPerson(id_person, id_source, 1001, "");
                }
            }
            con.close();
            rsFamilyname.close();
        } catch (Exception e) {
            showMessage(counter + " An error occured while cleaning familyname: " + e.getMessage(), false, true);
        }
    } // standardFamilyname


    /**
     * @param type
     */
    public void standardFlaggedDate( String type )
    {
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
                    showMessage(counter + "", true, true);
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
                    addToReportPerson(id_person, id_source + "", 211, dymd.getReports());

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
            showMessage(counter + " An error occured while cleaning " + type + " flagged date: " + e.getMessage(), false, true);
        }
    } // standardFlaggedDate


    /**
     * @param rs
     * @param idFieldO
     * @param locationFieldO
     * @param locationFieldC
     * @param id_source
     * @param tt
     */
    private void standardLocation( ResultSet rs, String idFieldO, String locationFieldO, String locationFieldC, String id_source, TableType tt )
    throws Exception
    {
        int counter = 0;
        int step = 1000;
        int stepstate = step;

        try {
            while (rs.next()) {

                counter++;
                if (counter == stepstate) {
                    showMessage(counter + "", true, true);
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
                                addToReportRegistration(id, id_source, 91, location);
                                String query = RegistrationC.updateIntQuery(locationFieldC, "10010", id);
                                conCleaned.runQuery(query);
                            } else {
                                addToReportPerson(id, id_source, 91, location);
                                String query = PersonC.updateIntQuery(locationFieldC, "10010", id);
                                conCleaned.runQuery(query);
                            }
                        } else if (nieuwCode == null ? SC_N == null : nieuwCode.equals(SC_N)) {

                            // EC 93
                            if (tt == TableType.REGISTRATION) {
                                addToReportRegistration(id, id_source, 91, location);
                            } else {
                                addToReportPerson(id, id_source, 93, location);
                            }
                        } else if (nieuwCode == null ? SC_U == null : nieuwCode.equals(SC_U)) {

                            // EC 95
                            if (tt == TableType.REGISTRATION) {
                                addToReportRegistration(id, id_source, 95, location);
                                String locationnumber = ttalLocation.getColumnByOriginal("location_no", location);
                                String query = RegistrationC.updateIntQuery(locationFieldC, locationnumber, id);
                                conCleaned.runQuery(query);
                            } else {
                                addToReportPerson(id, id_source, 95, location);
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
                                addToReportRegistration(id, id_source, 99, location);
                            } else {
                                addToReportPerson(id, id_source, 99, location);
                            }
                        }
                    } else {

                        // EC 91
                        if (tt == TableType.REGISTRATION) {
                            addToReportRegistration(id, id_source, 91, location);
                            String query = RegistrationC.updateIntQuery(locationFieldC, "10010", id);
                            conCleaned.runQuery(query);
                        } else {
                            addToReportPerson(id, id_source, 91, location);
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
    } // standardLocation


    /**
     * @param sourceNo
     */
    public void standardMarLocation( String sourceNo )
    {
        String startQuery;
        String id_source;

        if (sourceNo.isEmpty()) {
            startQuery = "SELECT id_person , mar_location FROM person_o" + bronFilter + " AND mar_location <> ''";
            id_source = this.sourceId + "";
        } else {
            startQuery = "SELECT id_person , mar_location FROM person_o WHERE id_source = " + sourceNo + " AND mar_location <> ''";
            id_source = sourceNo;
        }

        try {
            ResultSet rs = conOriginal.runQueryWithResult(startQuery);
            standardLocation(rs, "id_person", "mar_location", "mar_location", id_source, TableType.PERSON);
        } catch (Exception e) {
            showMessage(e.getMessage(), false, true);
        }
    } // standardMarLocation


    /**
     * @param sourceNo
     */
    public void standardOccupation( String sourceNo )
    {
        boolean debug = true;
        int counter = 0;
        int step = 1000;
        int stepstate = step;
        int empty = 0;

        try
        {
            String startQuery;
            String id_source;

            if( sourceNo.isEmpty() ) {
                startQuery = "SELECT id_person , occupation FROM person_o" + bronFilter;
                id_source = this.sourceId + "";
            } else {
                startQuery = "SELECT id_person , occupation FROM person_o WHERE id_source = " + sourceNo;
                id_source = sourceNo;
            }

            // Get occupation
            ResultSet rs = conOriginal.runQueryWithResult( startQuery );

            while( rs.next() )
            {
                counter++;
                if( counter == stepstate ) {
                    showMessage( counter + "", true, true );
                    stepstate += step;
                }

                int id_person = rs.getInt( "id_person" );
                String occupation = rs.getString( "occupation" );
                if( debug ) { showMessage( "occupation: " + occupation, false, true  ); }

                if( occupation != null && !occupation.isEmpty() )
                {
                    String newCode = "";
                    try { newCode = ttalOccupation.getStandardCodeByOriginal( occupation ); }
                    catch( Exception ex ) {
                        System.out.println( ex.getMessage() );
                    }
                    if( debug ) { showMessage( "newCode: " + newCode, false, true  ); }

                    if( newCode.equals( SC_X ) ) {
                        if( debug ) { showMessage( "Warning 41: id_person: " + id_person + ", occupation: " + occupation, false, true ); }
                        addToReportPerson( id_person, id_source, 41, occupation );     // warning 41

                        String query = PersonC.updateQuery( "occupation", occupation, id_person );
                        conCleaned.runQuery( query );
                    }
                    else if( newCode.equals( SC_N ) ) {
                        if( debug ) { showMessage( "Warning 43: id_person: " + id_person + ", occupation: " + occupation, false, true ); }
                        addToReportPerson( id_person, id_source, 43, occupation );     // warning 43
                    }
                    else if( newCode.equals( SC_U ) ) {
                        if( debug ) { showMessage( "Warning 45: id_person: " + id_person + ", occupation: " + occupation, false, true ); }
                        addToReportPerson( id_person, id_source, 45, occupation );     // warning 45

                        String query = PersonC.updateQuery( "occupation",
                            ttalStatusSex.getColumnByOriginal( "occupation", occupation ), id_person );
                        conCleaned.runQuery( query );
                    }
                    else if( newCode.equals( SC_Y ) ) {
                        String query = PersonC.updateQuery( "occupation",
                            ttalStatusSex.getColumnByOriginal( "occupation", occupation ), id_person );
                        conCleaned.runQuery( query );
                    }
                    else {     // Invalid standard code
                        if( debug ) { showMessage( "Warning 49: id_person: " + id_person + ", occupation: " + occupation, false, true ); }
                        addToReportPerson( id_person, id_source, 49, occupation );     // warning 49
                    }
                }
                else {        // not present in original
                    empty += 1;
                    if( debug ) { showMessage( "Warning 41: id_person: " + id_person + ", occupation: " + occupation, false, true ); }
                    if( debug ) { showMessage( "not present in original: skipping ", false, true ); }
                    if( 1==1 ) { continue; }

                    addToReportPerson( id_person, id_source, 41, occupation );         // warning 41
                    ttalOccupation.addOriginal( occupation );                          // Add new Occupation "x" ??

                    String query = PersonC.updateQuery( "occupation", occupation, id_person );
                    conCleaned.runQuery( query );
                }
            }
            showMessage( counter + " persons, " + empty + " without occupation" , false, true );

        } catch( Exception ex ) {
            showMessage( "counter: " + counter + ", Exception while cleaning Occupation: " + ex.getMessage(), false, true );
        }
    } // standardOccupation


    /**
     * @param sourceNo
     * @throws Exception
     */
    public void standardPrepiece( String sourceNo )
    {
        int counter = 0;
        int step = 1000;
        int stepstate = step;

        try {

            String startQuery;
            String id_source;

            if (sourceNo.isEmpty()) {
                startQuery = "SELECT id_person , prefix FROM person_o" + bronFilter + " AND prefix <> ''";
                id_source = this.sourceId + "";
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
                    showMessage(counter + " of " + count, true, true);
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
                            addToReportPerson(id_person, id_source, 81, part);

                            listPF += part + " ";
                        } else if (standard_code.equals(SC_N)) {
                            // EC 83
                            addToReportPerson(id_person, id_source, 83, part);
                        } else if (standard_code.equals(SC_U)) {

                            // EC 85
                            addToReportPerson(id_person, id_source, 85, part);

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
                            addToReportPerson(id_person, id_source, 89, part);
                        }
                    } else { // Prefix not in ref
                        addToReportPerson(id_person, id_source, 81, part);

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
            showMessage(counter + " An error occured while cleaning Prepiece: " + e.getMessage(), false, true);
        }
    } // standardPrepiece


    /**
     * @param sourceNo
     */
    public void standardRegistrationDate( String sourceNo )
    {
        int counter = 0;
        int step = 1000;
        int stepstate = step;

        try {

            String startQuery;
            String id_source;

            if (sourceNo.isEmpty()) {
                startQuery = "SELECT id_registration , registration_date FROM registration_o" + bronFilter;
                id_source = this.sourceId + "";
            } else {
                startQuery = "SELECT id_registration , registration_date FROM registration_o WHERE id_source = " + sourceNo;
                id_source = sourceNo;
            }

            ResultSet rs = conOriginal.runQueryWithResult(startQuery);

            while (rs.next()) {

                counter++;
                if (counter == stepstate) {
                    showMessage(counter + "", true, true);
                    stepstate += step;
                }

                // Get Opmerking
                int id_registration = rs.getInt("id_registration");
                String registration_date = rs.getString("registration_date");

                if (registration_date == null) {

                    // EC 202
                    addToReportRegistration(id_registration, id_source, 202, "");

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
                    addToReportRegistration(id_registration, id_source, 201, dymd.getReports());

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
            showMessage(counter + " An error occured while cleaning Registration date: " + e.getMessage(), false, true);
        }
    } // standardRegistrationDate


    /**
     * @param sourceNo
     */
    public void standardRegistrationLocation( String sourceNo )
    {
        String startQuery;
        String id_source;

        if (sourceNo.isEmpty()) {
            startQuery = "SELECT id_registration , registration_location FROM registration_o" + bronFilter;
            id_source = this.sourceId + "";
        } else {
            startQuery = "SELECT id_registration , registration_location FROM registration_o WHERE id_source = " + sourceNo;
            id_source = sourceNo;
        }

        try {
            ResultSet rs = conOriginal.runQueryWithResult(startQuery);

            // Call standardLocation
            standardLocation(rs, "id_registration", "registration_location", "registration_location_no", id_source, TableType.REGISTRATION);
        } catch (Exception e) {
            showMessage(e.getMessage(), false, true);
        }
    } // standardRegistrationLocation


    private void standardRole()
    {
        String query = " UPDATE links_original.person_o, links_cleaned.person_c, links_general.ref_role "
                + "SET "
                + "links_cleaned.person_c.role = links_general.ref_role.role_nr "
                + "WHERE links_original.person_o.role = links_general.ref_role.original AND "
                + "links_original.person_o.id_person = links_cleaned.person_c.id_person; ";

        try {
            conCleaned.runQuery(query);
        } catch (Exception e) {
            showMessage("An error occured while running standardRole: " + e.getMessage(), false, true);
        }
    } // standardRole


    /**
     * @param SourceNo
     * @throws Exception
     */
    public void standardSequence( String SourceNo) throws Exception
    {
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
                addToReportRegistration(previousId, id_source, 111, "");
            } else { // Present
                // Is is numeric
                try {

                    previousNo = Integer.parseInt(rs.getString("registration_seq"));

                } catch (Exception e) {
                    // EC 112
                    addToReportRegistration(previousId, id_source, 112, rs.getString("registration_seq"));
                }
            }
            while (rs.next()) {
                counter++;
                if (counter == stepstate) {
                    showMessage(counter + "", true, true);
                    stepstate += step;
                }

                int nummer;

                // Is reg_seq present ?
                if (rs.getString("registration_seq") == null || rs.getString("registration_seq").isEmpty()) {

                    // EC 111
                    addToReportRegistration(rs.getInt("id_registration"), id_source, 111, "");
                    continue;
                }
                // Is is numeric ?
                try {
                    nummer = Integer.parseInt(rs.getString("registration_seq"));
                } catch (Exception e) {
                    // EC 112
                    addToReportRegistration(rs.getInt("id_registration"), id_source, 112, rs.getString("registration_seq"));

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
                    addToReportRegistration(rs.getInt("id_registration"), id_source, 113, rs.getString("registration_seq"));
                } else if (verschil > 1 && (previousYr == rs.getInt("registration_year")) && (previousMt == rs.getInt("registration_maintype")) && (previousLc == rs.getInt("registration_location_no"))) {
                    // EC 114
                    addToReportRegistration(rs.getInt("id_registration"), id_source, 114, rs.getString("registration_seq"));
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

            showMessage(counter + " An error occured while checking sequence: " + e.getMessage(), false, true);
        }
    } // standardSequence


    /**
     * @param sourceNo
     */
    public void standardSex( String sourceNo )
    {
        boolean debug = false;
        int counter = 0;
        int step = 1000;
        int stepstate = step;

        try
        {
            String startQuery;
            String id_source;

            if( sourceNo.isEmpty() ) {
                startQuery = "SELECT id_person , sex FROM person_o" + bronFilter;
                id_source = this.sourceId + "";
            } else {
                startQuery = "SELECT id_person , sex FROM person_o WHERE id_source = " + sourceNo;
                id_source = sourceNo;
            }

            ResultSet rs = conOriginal.runQueryWithResult( startQuery );

            while( rs.next() )
            {
                counter++;
                if( counter == stepstate ) {
                    showMessage( counter + "", true, true );
                    stepstate += step;
                }

                int id_person = rs.getInt( "id_person" );
                //String sex = rs.getString( "sex" );
                String sex = rs.getString( "sex" ) != null ? rs.getString( "sex" ).toLowerCase() : "";
                if( debug ) { showMessage( "sex: " + sex , false, true ); }

                if( sex != null && !sex.isEmpty() )                 // check presence of the gender
                {
                    if( ttalStatusSex.originalExists( sex ) )       // check presence in original
                    {
                        String newCode = ttalStatusSex.getStandardCodeByOriginal( sex );
                        if( debug ) { showMessage( "newCode: " + newCode , false, true ); }

                        if( newCode.equals( SC_X ) ) {
                            if( debug ) { showMessage( "Warning 31: id_person: " + id_person + ", sex: " + sex, false, true ); }

                            addToReportPerson( id_person, id_source, 31, sex );     // warning 31

                            String query = PersonC.updateQuery( "sex", sex, id_person );
                            conCleaned.runQuery( query );
                        }
                        else if( newCode.equals( SC_N ) ) {
                            if( debug ) { showMessage( "Warning 33: id_person: " + id_person + ", sex: " + sex, false, true ); }

                            addToReportPerson( id_person, id_source, 33, sex );     // warning 33
                        }
                        else if( newCode.equals( SC_U ) ) {
                            if( debug ) { showMessage( "Warning 35: id_person: " + id_person + ", sex: " + sex, false, true ); }

                            addToReportPerson( id_person, id_source, 35, sex );     // warning 35

                            String query = PersonC.updateQuery( "sex",
                                ttalStatusSex.getColumnByOriginal( "standard_sex", sex ), id_person );
                            conCleaned.runQuery( query );
                        }
                        else if( newCode.equals( SC_Y ) ) {
                            if( debug ) { showMessage( "Standard sex: id_person: " + id_person + ", sex: " + sex, false, true ); }

                            String query = PersonC.updateQuery( "sex",
                                ttalStatusSex.getColumnByOriginal( "standard_sex", sex ), id_person );
                            conCleaned.runQuery( query );
                        }
                        else {     // Invalid standard code
                            if( debug ) { showMessage( "Warning 39: id_person: " + id_person + ", sex: " + sex, false, true ); }

                            addToReportPerson( id_person, id_source, 39, sex );     // warning 39
                        }
                    }
                    else // not present in original
                    {
                        if( debug ) { showMessage( "not present in original", false, true ); }
                        if( debug ) { showMessage( "Warning 31: id_person: " + id_person + ", sex: " + sex, false, true ); }

                        addToReportPerson( id_person, id_source, 31, sex );         // warning 31
                        ttalStatusSex.addOriginal( sex );                               // Add new Sex "x" ??

                        String query = PersonC.updateQuery( "sex", sex, id_person );
                        conCleaned.runQuery( query );
                    }
                }
            }
        } catch( Exception ex ) {
            showMessage( counter + " Exception while cleaning Sex: " + ex.getMessage(), false, true );
        }
    } // standardSex


    /**
     * @param sourceNo
     */
    public void standardStatusSex( String sourceNo )
    {
        int counter = 0;
        int step = 1000;
        int stepstate = step;

        try
        {
            String startQuery;
            String id_source;

            if( sourceNo.isEmpty() ) {
                startQuery = "SELECT id_person , sex , civil_status FROM person_o" + bronFilter + " and civil_status is not null ";
                id_source = this.sourceId + "";
            } else {
                startQuery = "SELECT id_person , sex , civil_status FROM person_o WHERE id_source = " + sourceNo;
                id_source = sourceNo;
            }

            ResultSet rs = conOriginal.runQueryWithResult( startQuery );

            while( rs.next() )
            {
                counter++;
                if( counter == stepstate ) {
                    showMessage( counter + "", true, true );
                    stepstate += step;
                }

                int id_person = rs.getInt( "id_person" );
                //String sex = rs.getString( "sex" );
                String sex = rs.getString( "sex" ) != null ? rs.getString( "sex" ).toLowerCase() : "";
                //String civil_status = rs.getString( "civil_status" );
                String civil_status = rs.getString( "civil_status" ) != null ? rs.getString( "civil_status" ).toLowerCase() : "";

                if( civil_status != null && !civil_status.isEmpty() )   // check presence of civil status
                {
                    if( ttalStatusSex.originalExists( civil_status ) )  // check presence in original
                    {
                        String newCode = this.ttalStatusSex.getStandardCodeByOriginal( civil_status );

                        if( newCode.equals( SC_X ) ) {
                            addToReportPerson( id_person, id_source, 61, civil_status );            // warning 61

                            String query = PersonC.updateQuery( "civil_status", civil_status, id_person );
                            conCleaned.runQuery( query );
                        }
                        else if( newCode.equals( SC_N ) ) {
                            addToReportPerson( id_person, id_source, 63, civil_status );            // warning 63
                        }
                        else if( newCode.equals( SC_U ) ) {
                            addToReportPerson( id_person, id_source, 65, civil_status );            // warning 65

                            String query = PersonC.updateQuery( "civil_status",
                                ttalStatusSex.getColumnByOriginal( "standard_civilstatus", civil_status ), id_person );
                            conCleaned.runQuery( query );

                            if( sex != null && !sex.isEmpty() ) {           // Extra check on sex
                                if( !sex.equalsIgnoreCase( this.ttalStatusSex.getColumnByOriginal( "standard_sex", civil_status ) ) ) {
                                    addToReportPerson( id_person, id_source, 68, civil_status );    // warning 68
                                }
                            }
                            else            // Sex is empty
                            {
                                String sexQuery = PersonC.updateQuery( "sex",
                                    ttalStatusSex.getColumnByOriginal( "standard_sex", civil_status ), id_person );
                                conCleaned.runQuery( sexQuery );
                            }

                            String sexQuery = PersonC.updateQuery( "civil_status",
                                ttalStatusSex.getColumnByOriginal( "standard_civilstatus", civil_status ), id_person );
                            conCleaned.runQuery( sexQuery );
                        }
                        else if( newCode.equals( SC_Y ) ) {
                            String query = PersonC.updateQuery( "civil_status",
                                ttalStatusSex.getColumnByOriginal( "standard_civilstatus", civil_status ), id_person );
                            conCleaned.runQuery( query );

                            if( sex != null && !sex.isEmpty() ) {      // Extra check on sex
                                if( !sex.equalsIgnoreCase( this.ttalStatusSex.getColumnByOriginal( "standard_sex", civil_status ) ) ) {
                                    addToReportPerson( id_person, id_source, 68, civil_status );    // warning 68
                                }
                            }
                            else {      // Sex is empty
                                String sexQuery = PersonC.updateQuery( "sex",
                                    ttalStatusSex.getColumnByOriginal( "standard_sex", civil_status ), id_person );
                                conCleaned.runQuery( sexQuery );
                            }

                            String sexQuery = PersonC.updateQuery( "civil_status",
                                ttalStatusSex.getColumnByOriginal( "standard_civilstatus", civil_status ), id_person );
                            conCleaned.runQuery( sexQuery );
                        }
                        else {          // Invalid SC
                            addToReportPerson( id_person, id_source, 69, civil_status );            // warning 68
                        }
                    }
                    else {      // add to ref
                        addToReportPerson( id_person, id_source, 61, civil_status );                // warning 61

                        ttalStatusSex.addOriginal( civil_status );                                      // Add new Status "x" ??

                        String query = PersonC.updateQuery( "civil_status", civil_status, id_person );  // Write to Person
                        conCleaned.runQuery( query );
                    }
                }
            }
        } catch( Exception ex ) {
            showMessage( counter + " Exception while cleaning Civil Status: " + ex.getMessage(), false, true );
        }
    } // standardStatusSex


    /**
     * @param bronnrsourceNo
     */
    public void standardSuffix( String bronnrsourceNo )
    {
        int counter = 0;
        int step = 1000;
        int stepstate = step;

        try {
            String startQuery;
            String id_source;

            if (bronnrsourceNo.isEmpty()) {
                startQuery = "SELECT id_person , suffix FROM person_o" + bronFilter + " AND suffix <> ''";
                id_source = this.sourceId + "";
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
                    showMessage(counter + " of " + count, true, true);
                    stepstate += step;
                }

                int id_person = rsSuffix.getInt("id_person");
                String suffix = rsSuffix.getString("suffix").toLowerCase();

                suffix = funcCleanNaam(suffix);

                // Controleer of deze voorkomt in ref table
                if (ttalSuffix.originalExists(suffix)) {

                    String standard_code = ttalSuffix.getStandardCodeByOriginal(suffix);

                    if (standard_code.equals(SC_X)) {

                        // EC 71
                        addToReportPerson(id_person, id_source, 71, suffix);

                        String query = PersonC.updateQuery("suffix", suffix, id_person);
                        conCleaned.runQuery(query);
                    } else if (standard_code.equals(SC_N)) {

                        // EC 73
                        addToReportPerson(id_person, id_source, 73, suffix);

                    } else if (standard_code.equals(SC_U)) {

                        // EC 74
                        addToReportPerson(id_person, id_source, 75, suffix);

                        String query = PersonC.updateQuery("suffix", suffix, id_person);
                        conCleaned.runQuery(query);

                    } else if (standard_code.equals(SC_Y)) {

                        String query = PersonC.updateQuery("suffix", suffix, id_person);
                        conCleaned.runQuery(query);
                    } else {

                        // EC 75
                        addToReportPerson(id_person, id_source, 79, suffix);
                    }
                } // Standard code x
                else {

                    // EC 71
                    addToReportPerson(id_person, id_source, 71, suffix);

                    ttalSuffix.addOriginal(suffix);

                    String query = PersonC.updateQuery("suffix", suffix, id_person);
                    conCleaned.runQuery(query);

                }
            }

            // Free resources
            rsSuffix.close();
            con.close();

        } catch (Exception e) {
            showMessage(counter + " An error occured while cleaning Suffix: " + e.getMessage(), false, true);
        }
    } // standardSuffix


    /**
     * @param sourceNo
     */
    public void standardType( String sourceNo )
    {
        boolean debug = false;
        int counter = 0;
        int step = 1000;
        int stepstate = step;

        try
        {
            String startQuery;
            String id_source;

            if( sourceNo.isEmpty() ) {
                startQuery = "SELECT id_registration, registration_maintype, registration_type FROM registration_o" + bronFilter;
                id_source = this.sourceId + "";
            } else {
                startQuery = "SELECT id_registration, registration_maintype, registration_type FROM registration_o WHERE id_source = " + sourceNo;
                id_source = sourceNo;
            }

            ResultSet rs = conOriginal.runQueryWithResult( startQuery );

            while( rs.next() )      // process data from links_original
            {
                counter++;
                if( counter == stepstate ) {
                    showMessage( counter + "", true, true );
                    stepstate += step;
                }

                int id_registration = rs.getInt( "id_registration" );
                int registration_maintype = rs.getInt( "registration_maintype" );
                registration_maintype = 2;
                String registration_type = rs.getString( "registration_type" ) != null ? rs.getString( "registration_type" ).toLowerCase() : "";

                String refQuery = "SELECT * FROM ref_registration WHERE main_type = " +
                    registration_maintype + " AND original = '" + registration_type + "'";
                ResultSet ref = conGeneral.runQueryWithResult( refQuery );

                if( ref.next() )        // compare with reference
                {
                    String newCode = ref.getString( "standard_code" ).toLowerCase();

                    if( newCode.equals( SC_X ) ) {
                        if( debug ) { showMessage( "Warning 51: id_registration: " + id_registration + ", reg type: " + registration_type, false, true ); }

                        addToReportRegistration( id_registration, id_source, 51, registration_type );       // warning 51

                        String query = RegistrationC.updateQuery( "registration_type", registration_type, id_registration );
                        conCleaned.runQuery( query );
                    }
                    else if( newCode.equals( SC_N ) ) {
                        if( debug ) { showMessage( "Warning 53: id_registration: " + id_registration + ", reg type: " + registration_type, false, true ); }

                        addToReportRegistration( id_registration, id_source, 53, registration_type );       // warning 53
                    }
                    else if( newCode.equals( SC_U ) ) {
                        if( debug ) { showMessage( "Warning 55: id_registration: " + id_registration + ", reg type: " + registration_type, false, true ); }

                        addToReportRegistration( id_registration, id_source, 55, registration_type );       // warning 55

                        String query = RegistrationC.updateQuery( "registration_type", ref.getString( "standard" ).toLowerCase(), id_registration );
                        conCleaned.runQuery( query );
                    } else if( newCode.equals( SC_Y ) ) {
                        if( debug ) { showMessage( "Standard reg type: id_person: " + id_registration + ", reg type: " + registration_type, false, true ); }

                        String query = RegistrationC.updateQuery( "registration_type", ref.getString( "standard" ).toLowerCase(), id_registration );
                        conCleaned.runQuery( query );
                    }
                    else {    // invalid SC
                        if( debug ) { showMessage( "Warning 59: id_registration: " + id_registration + ", reg type: " + registration_type, false, true ); }

                        addToReportRegistration( id_registration, id_source, 59, registration_type );       // warning 59
                    }
                }
                else {      // not in reference; add to reference with "x"
                    if( debug ) { showMessage( "Warning 51: id_registration: " + id_registration + ", reg type: " + registration_type, false, true ); }

                    addToReportRegistration( id_registration, id_source, 51, registration_type );           // warning 51

                    // add to links_general
                    conGeneral.runQuery( "INSERT INTO ref_registration( original, main_type, standard_code ) VALUES ('" + registration_type + "'," + registration_maintype + ",'x')" );

                    // update links_cleaned_.registration_c
                    String query = RegistrationC.updateQuery( "registration_type", registration_type.length() < 50 ? registration_type : registration_type.substring(0, 50), id_registration );
                    conCleaned.runQuery( query );
                }
            }
        } catch( Exception ex ) {
            showMessage( "counter: " + counter + ", Exception while cleaning Registration Type: " + ex.getMessage(), false, true );
        }
    } // standardType


    /**
     * @param sourceNo
     */
    public void standardYearAge( String sourceNo )
    {
        int counter = 0;
        int step = 1000;
        int stepstate = step;

        try {

            String startQuery;
            String id_source;

            if (sourceNo.isEmpty()) {
                startQuery = "SELECT id_person , age_year FROM person_o" + bronFilter;
                id_source = this.sourceId + "";
            } else {
                startQuery = "SELECT id_person , age_year FROM person_o WHERE id_source = " + sourceNo;
                id_source = sourceNo;
            }

            ResultSet rs = conOriginal.runQueryWithResult(startQuery);

            while (rs.next()) {

                counter++;
                if (counter == stepstate) {
                    showMessage(counter + "", true, true);
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
                        addToReportPerson(id_person, id_source, 241, year_age + "");
                    }
                }
            }
            rs.close();
            rs = null;
        } catch (Exception e) {
            showMessage(counter + " An error occured while cleaning Age Year: " + e.getMessage(), false, true);
        }
    } // standardYearAge

     /*---< end Standardizing functions >------------------------------------*/


    /**
     * Read distinct source ids from links_original.registration_o
     * @return
     */
    private int[] getOrigSourceIds()
    {
        //sources = null;

        ArrayList< String > ids = new ArrayList();
        String query = "SELECT DISTINCT id_source FROM registration_o ORDER BY id_source;";
        try {
            ResultSet rs = conOriginal.runQueryWithResult( query );
            rs.first();
            for( ;; ) {
                String id = rs.getString( "id_source" );
                if( id == null || id.isEmpty() ) { break; }
                else {
                    //System.out.printf( "id: %s\n", id );
                    ids.add(id);
                    rs.next();
                }
            }
        }
        catch( Exception ex ) {
            if( ex.getMessage() != "After end of result set" ) {
                //System.out.printf("'%s'\n", ex.getMessage());
            }
        }
        //System.out.println( ids );

        int[] idsInt = new int[ ids.size() ];
        int i = 0;
        for( String id : ids ) {
            //System.out.println( id );
            idsInt[ i ] = Integer.parseInt(id);
            i += 1;
        }
        return idsInt;
    } // getOrigSourceIds


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
     * @param MethodName
     * @throws Exception
     */
    private void runMethod(String MethodName) throws Exception
    {
        Class[] partypes = new Class[1];
        Object[] argList = new Object[1];

        partypes[0] = String.class;

        // source 1 by 1
        if( bronFilter.isEmpty() )
        {
            for( int i : sources ) {
                showMessage( "Running " + MethodName + " for source: " + i + "...", false, false );

                argList[0] = i + "";
                Method m = this.getClass().getMethod(MethodName, partypes);

                // Call method
                m.invoke(this, argList);

                showMessage( endl, false, true );
                System.out.println( "" + i );
            }
        }
        else
        {
            showMessage( "Running " + MethodName + "...", false, false );

            argList[0] = "";
            Method m = this.getClass().getMethod(MethodName, partypes);

            // Call method
            m.invoke(this, argList);

            showMessage( endl, false, true );
        }
    } // runMethod


    /**************************************************************************/

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
                    showMessage( (teller - 1) + "", true, true );
                    step += 10000;

                    // Clean memory
                    if (((teller - 1) % 50000) == 0) {
                        showMessage( "Cleaning unused memory...", true, false );
                        r.gc();
                        showMessage( "DONE!", true, true );
                    }
                }

                // Set matchfound boolean
                boolean matchFound = false;

                /**
                 * Opmerking strippen aan de hand van de table
                 * We lopen hier door alle regexen uit de table
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

                    // Haal regex uit de table
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
    } // functieParseRemarks


    private String functieOpmerkingenBeroep(String id_registratie, String bron, String value) throws Exception {

        String beroep = LinksSpecific.functieOpmerkingenWaardeNaDubbelePunt(value);

        // verwerken, gaat via een generieke methode, ergens anders hebben we geen waarde na opmerking
        String nieuweBeroep = functieVeldBeroep(id_registratie, bron, beroep);

        // return event. bewerkte beroep
        return nieuweBeroep;
    } // functieOpmerkingenBeroep


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
    } // functieOpmerkingenTeLocatie


    private String functieOpmerkingenLocatie(String value) throws Exception {
        String locatie = LinksSpecific.functieOpmerkingenWaardeNaDubbelePunt(value);

        // verwerken, gaat via een generieke methode, ergens anders hebben we geen waarde na opmerking
        String nieuweLocatie = functieVeldLocatie(locatie);

        // return event. bewerkte beroep
        return nieuweLocatie;
    } // functieOpmerkingenLocatie


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
    } // functieOpmerkingenGebDatOverl


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
    } // functieOpmerkingenLocatieEnDatum


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
                } else if (nieuwCode == null ? SC_U == null : nieuwCode.equals(SC_U)) {
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
    } // functieVeldBeroep


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
                } else if (nieuwCode == null ? SC_U == null : nieuwCode.equals(SC_U)) {
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
    } // functieVeldLocatie


    private String devideValueDate(String valueToDevide) {

        Pattern regex = Pattern.compile("[0-9]+-[0-9]+-[0-9]+");
        Matcher m = regex.matcher(valueToDevide);

        String date = m.group();

        String onlyData = valueToDevide.replaceAll("[0-9]+-[0-9]+-[0-9]+", "");

        return onlyData + "$" + date;
    } // devideValueDate


    /**
     * @throws Exception
     */
    public void scanRemarks(String bronnr) throws Exception {

        /**
         * Lees Scan instellingen in
         */
        showMessage("Preparing remarks parsing...", false, false);
        ResultSet rsScanStrings = conGeneral.runQueryWithResult(
                "SELECT * FROM scan_remarks ORDER BY maintype, group_no, priority_no");

        String query;

        if (bronnr.isEmpty()) {
            query = "SELECT id_registration , id_source , registration_maintype , remarks FROM registration_o" + bronFilter;
        } else {
            query = "SELECT id_registration , id_source , registration_maintype , remarks FROM registration_o WHERE id_source = " + bronnr;
        }


        // Lees Opmerkingen in
        ResultSet rs = conOriginal.runQueryWithResult( query );
        showMessage( endl, false, true );

        // Parsing Opmerkingen
        showMessage("Parsing remarks...", false, false);
        HashMap cache;
        try {
            cache = functieParseRemarks(rs, rsScanStrings);
        } catch (Exception e) {
            showMessage(teller + " ERROR:" + e.getMessage(), false, false);
            return;
        }
        showMessage( endl, false, true );

        // Maak log table aan met resterende opmekingen
        String createQuery = ""
                + "CREATE TABLE IF NOT EXISTS `links_logs`.`log_rest_remarks_" + sourceId + bronnr + "_" + logTableName + "` (  "
                + "`id_log` INT NOT NULL AUTO_INCREMENT , "
                + "`registration_maintype` VARCHAR(3) NULL , "
                + "`content` VARCHAR(500) NULL , "
                + "`frequency` INT NULL , "
                + "PRIMARY KEY (`id_log`) , "
                + "INDEX `defaultindex` (`id_log` ASC) ) "
                + "DEFAULT CHARACTER SET = utf8;";

        // Voer query uit
        conLog.runQuery( createQuery );

        // Cache overzetten
        Set keySet = cache.keySet();
        Iterator keySetIterator = keySet.iterator();

        showMessage( "Writing rest remarks tot database...", false, false );

        // Loop door de resterende opmerkingen heen
        /*
        while (keySetIterator.hasNext()) {
        
        Object key = keySetIterator.next();
        Object value = cache.get(key);
        
        String[] velden = {"registration_maintype", "content", "frequency"};
        
        // eventuele quotes vervangen
        String cleanKey = LinksSpecific.funcPrepareForMysql(key.toString());
        String[] data = {cleanKey.substring(0, cleanKey.indexOf(":")), cleanKey.substring((cleanKey.indexOf(":") + 1)), value.toString()};
        conLog.insertIntoTable("log_rest_remarks_" + sourceId + bronnr + "_" + logTableName, velden, data);
        
        }
        
         */

        rs.close();
        rs = null;

        showMessage( endl, false, true );
    } // scanRemarks


    /**
     * @param id
     * @param id_source
     * @param errorCode
     * @param value
     * @throws Exception
     */
    private void addToReportRegistration( int id, String id_source, int errorCode, String value )
    throws Exception
    {
        String cla = ttalReport.getColumnByColumnInt( "type", "class",   errorCode );
        String con = ttalReport.getColumnByColumnInt( "type", "content", errorCode );

        // WORKAROUND
        // replace error chars
        value = value.replaceAll( "\\\\", "" );
        value = value.replaceAll( "\\$", "" );
        value = value.replaceAll( "\\*", "" );

        con = con.replaceAll( "<.*>", value );

        con = LinksSpecific.funcPrepareForMysql( con );
        //showMessage( con, false, true );

        String query = ""
            + " insert into links_logs.`" + logTableName + "`( reg_key , id_source , report_class , report_type , content , date_time )"
            + " values( " + id + " , " + id_source + " , '" + cla.toUpperCase() + "' , " + errorCode + " , '" + con + "' , NOW() ) ; ";
        //showMessage( query, false, true );

        conLog.runQuery( query );
    } // addToReportRegistration


    /**
     * @param id
     * @param id_source
     * @param errorCode
     * @param value
     * @throws Exception
     */
    private void addToReportPerson( int id, String id_source, int errorCode, String value )
    throws Exception
    {
        String cla = ttalReport.getColumnByColumnInt( "type", "class",   errorCode );
        String con = ttalReport.getColumnByColumnInt( "type", "content", errorCode );

        // WORKAROUND
        // replace error chars
        value = value.replaceAll( "\\\\", "" );
        value = value.replaceAll( "\\$", "" );
        value = value.replaceAll( "\\*", "" );


        con = con.replaceAll( "<.*>", value );

        con = LinksSpecific.funcPrepareForMysql( con );

        String query = ""
            + " insert into links_logs.`" + logTableName + "`( pers_key , id_source , report_class , report_type , content , date_time )"
            + " values( " + id + " , " + id_source + " , '" + cla.toUpperCase() + "' , " + errorCode + " , '" + con + "' , NOW() ) ; ";

        conLog.runQuery( query );
    } // addToReportPerson


    /**
     * @param logText
     * @param start
     */
    private void showTimingMessage( String logText, long start )
    {
        long stop = System.currentTimeMillis();
        String mmss = Functions.millisec2hms( start, stop );
        String msg = logText + mmss;
        showMessage( msg, false, true );
    }


    /**
     * @param logText
     * @param isMinOnly
     * @param newLine
     */
    private void showMessage( String logText, boolean isMinOnly, boolean newLine )
    {
        tbLOLClatestOutput.setText( logText );

        if( !isMinOnly ) {
            String newLineToken = "";
            if( newLine ) {
                newLineToken = "\r\n";
            }

            if( logText != endl ) {
                String ts = LinksSpecific.getTimeStamp2( "HH:mm:ss" );

                taLOLCoutput.append( ts + " " );
                // System.out.printf( "%s ", ts );
                //logger.info( logText );
                try { plog.show( logText ); }
                catch( Exception ex ) { System.out.println( ex.getMessage() ); }
            }

            taLOLCoutput.append( logText + newLineToken );
            //System.out.printf( "%s%s", logText, newLineToken );
            try { plog.show( logText ); }
            catch( Exception ex ) { System.out.println( ex.getMessage() ); }
        }
    } // showMessage


    /**
     * @param name
     * @return
     */
    private String funcCleanNaam(String name) {
        return name.replaceAll("[^A-Za-z0-9 '\\-\\.,èêéëÈÊÉËùûúüÙÛÚÜiìîíïÌÎÍÏòôóöÒÔÓÖàâáöÀÂÁÄçÇ]+", "");
    } // funcCleanNaam

    private String funcCleanFirstName(String name) {
        return name.replaceAll("[^A-Za-z0-9 '\\-èêéëÈÊÉËùûúüÙÛÚÜiìîíïÌÎÍÏòôóöÒÔÓÖàâáöÀÂÁÄçÇ]+", "");
    } // funcCleanFirstName

    private String funcCleanFamilyname(String name) {
        return name.replaceAll("[^A-Za-z0-9 '\\-èêéëÈÊÉËùûúüÙÛÚÜiìîíïÌÎÍÏòôóöÒÔÓÖàâáöÀÂÁÄçÇ]+", "").replaceAll("\\-", " ");
    } // funcCleanFamilyname


    /**
     * clear GUI output text fields
     */
    public void clearTextFields() {
        String timestamp = LinksSpecific.getTimeStamp2( "HH:mm:ss" );
        System.out.println( timestamp + " clearTextFields()" );

        tbLOLClatestOutput.setText( "" );
        taLOLCoutput.setText( "" );
    } // clearTextFields


    /**
     * @throws Exception
     */
    private void connectToDatabases()
    throws Exception
    {
        showMessage( "Connecting to databases:", false, true );

        //showMessage( "links_general (ref)", false, true );
        showMessage( ref_db + " (ref)", false, true );
        conOr = new MySqlConnector( ref_url, ref_db, ref_user, ref_pass );

        showMessage( "links_general", false, true );
        conGeneral = new MySqlConnector( url, "links_general", user, pass );

        showMessage( "links_original", false, true );
        conOriginal = new MySqlConnector( url, "links_original", user, pass );

        showMessage( "links_logs", false, true );
        conLog = new MySqlConnector( url, "links_logs", user, pass );

        showMessage( "links_cleaned", false, true );
        conCleaned = new MySqlConnector( url, "links_cleaned", user, pass );

        showMessage( "links_temp", false, true );
        conTemp = new MySqlConnector( url, "links_temp", user, pass );
    } // connectToDatabases


    private void createLogTable()
    throws Exception
    {
        showMessage( "Creating logging table: " + logTableName , false, true );

        String query = ""
            + " CREATE  TABLE `links_logs`.`" + logTableName + "` ("
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

    } // createLogTable


    /**
     * TODO clean this, bron -> source everywhere
     */
    private void setSourceFilters()
    {
        showMessage( "Set source filters for: " + sourceId, false, true );

        bronFilter   = " WHERE id_source = " + sourceId;
        sourceFilter = " WHERE id_source = " + sourceId;

        bronFilterCleanPers     = " WHERE person_c.id_source = "       + sourceId;
        bronFilterOrigineelPers = " WHERE person_o.id_source = "       + sourceId;
        bronFilterCleanReg      = " WHERE registration_c.id_source = " + sourceId;
        bronFilterOrigineelReg  = " WHERE registration_o.id_source = " + sourceId;

    } // setSourceFilters


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
    } // funcNamePrepiece


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
            showMessage("An error occured while flagging Birth date: " + e.getMessage(), false, true);
        }
    } // funcFlagBirthDate


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
            showMessage("An error occured while flagging Marriage date: " + e.getMessage(), false, true);
        }
    } // funcFlagMarriageDate


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
            showMessage("An error occured while flagging Death date: " + e.getMessage(), false, true);
        }
    } // funcFlagDeathDate


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
    } // funcMinMaxCorrectDate


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
    } // funcCompleteMinMaxBirth


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
    } // funcCompleteMinMaxMar


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

    } // funcSetcomplete



    /**
     * Previous Links basis functions
     * They are now part of links cleaned
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

                idSource = this.sourceId + "";

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
                    showMessage(counter + "", true, true);
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
                                        addToReportPerson(id_person1, idSource, 101, id_person2 + "");
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
            showMessage(counter + " An error occured while running Relation: " + e.getMessage(), false, true);
        }
    } // funcRelation


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
            addToReportPerson(id_person, "0", 105, "Null -> [rh:" + main_type + "][ad:" + date_type + "][rol:" + role + "][lg:" + yn_age_reported + "][lh:" + yn_age_main_role + "]");

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

    } // funcMinMaxCalculation


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
    } // funcAddTimeToDate


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
    } // funcCheckMaxDate


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
    } // funcRoundUp


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
                showMessage(counter + "", true, true);
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
    } // funcMinMaxMarriageYear


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
    } // funcDateLeftIsGreater


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
    } // funcSetMarriageYear


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
            showMessage("An error occured while Creating full dates from parts: " + e.getMessage(), false, true);
        }
    } // funcPartsToDate


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
            showMessage("q1", false, true);
            conCleaned.runQuery(query1);

            showMessage("q2", false, true);
            conCleaned.runQuery(query2);

            showMessage("q3", false, true);
            conCleaned.runQuery(query3);

            showMessage("q4", false, true);
            conCleaned.runQuery(query4);

            showMessage("q5", false, true);
            conCleaned.runQuery(query5);

            showMessage("q6", false, true);
            conCleaned.runQuery(query6);

            showMessage("q7", false, true);
            conCleaned.runQuery(queryReg);
        } catch (Exception e) {
            showMessage("An error occured while computing days since 1-1-1: " + e.getMessage(), false, true);
        }
    } // funcDaysSinceBegin


    /**
     * @throws Exception
     */
    private void createTempFamilynameTable() throws Exception
    {
        showMessage( "Creating familyname_t table", false, false );

        String query = "CREATE  TABLE links_temp.familyname_t ("
            + " person_id INT UNSIGNED NOT NULL AUTO_INCREMENT ,"
            + " familyname VARCHAR(80) NULL ,"
            + " PRIMARY KEY (person_id) );";

        conTemp.runQuery( query );

        showMessage( endl, false, true );
    } // createTempFamilynameTable


    /**
     * @throws Exception
     */
    private void createTempFamilynameFile() throws Exception
    {
        long start = System.currentTimeMillis();
        String filename = "familyname_t.csv";
        showMessage( "Creating " + filename, false, true );

        File f = new File( filename );
        if( f.exists() ) { f.delete(); }
        writerFamilyname = new FileWriter( filename );

        long stop = System.currentTimeMillis();
        String elapsed = Functions.millisec2hms( start, stop );
        String msg = "Creating familyname_t csv OK " + elapsed;
        showMessage( msg, false, true );
    } // createTempFamilynameFile


    /**
     * @throws Exception
     */
    private void loadFamilynameToTable() throws Exception
    {
        long start = System.currentTimeMillis();
        showMessage( "Loading CSV data into temp table", false, true );

        {
            String query = "LOAD DATA LOCAL INFILE 'familyname_t.csv' INTO TABLE familyname_t FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' ( person_id , familyname );";
            conTemp.runQuery( query );
        }

        long stop = System.currentTimeMillis();
        String elapsed = Functions.millisec2hms( start, stop );
        String msg = "Loading CSV data into temp table OK " + elapsed;
        showMessage( msg, false, true );
    } // loadFamilynameToTable


    /**
     *
     */
    private void updateFamilynameToPersonC() throws Exception
    {
        long start = System.currentTimeMillis();
        showMessage( "Moving familynames from temp table to person_c", false, true );

        {
            String query = "UPDATE links_cleaned.person_c, links_temp.familyname_t"
                    + " SET links_cleaned.person_c.familyname = links_temp.familyname_t.familyname"
                    + " WHERE links_cleaned.person_c.id_person = links_temp.familyname_t.person_id;";

            conTemp.runQuery( query );
        }

        long stop = System.currentTimeMillis();
        String elapsed = Functions.millisec2hms( start, stop );
        String msg = "Moving familynames from temp table to person_c OK " + elapsed;
        showMessage( msg, false, true );
    } // updateFamilynameToPersonC


    public void removeFamilynameFile() throws Exception
    {
        long start = System.currentTimeMillis();
        showMessage( "Removing familyname_t csv", false, true );

        {
            java.io.File f = new java.io.File("familyname_t.csv");
            f.delete();
        }

        long stop = System.currentTimeMillis();
        String elapsed = Functions.millisec2hms( start, stop );
        String msg = "Removing familyname_t csv OK " + elapsed;
        showMessage( msg, false, true );
    } // removeFamilynameFile


    public void removeFamilynameTable() throws Exception
    {
        long start = System.currentTimeMillis();
        showMessage( "Removing familyname_t table", false, true );

        String query = "DROP TABLE IF EXISTS familyname_t;";

        conTemp.runQuery( query );

        long stop = System.currentTimeMillis();
        String elapsed = Functions.millisec2hms( start, stop );
        String msg = "Removing familyname_t table OK " + elapsed;
        showMessage( msg, false, true );
    } // removeFamilynameTable


    /**
     * @throws Exception
     */
    private void createTempFirstnameTable() throws Exception
    {
        showMessage( "Creating firstname_t table", false, false );

        String query = "CREATE  TABLE links_temp.firstname_t ("
            + " person_id INT UNSIGNED NOT NULL AUTO_INCREMENT ,"
            + " firstname VARCHAR(80) NULL ,"
            + " PRIMARY KEY (person_id) );";

        conTemp.runQuery( query );

        showMessage( endl, false, true );
    } // createTempFirstnameTable


    /**
     * @throws Exception
     */
    private void createTempFirstnameFile() throws Exception
    {
        long start = System.currentTimeMillis();
        String filename = "firstname_t.csv";
        showMessage( "Creating " + filename, false, true );

        File f = new File( filename );
        if( f.exists() ) { f.delete(); }
        writerFirstname = new FileWriter( filename );

        long stop = System.currentTimeMillis();
        String elapsed = Functions.millisec2hms( start, stop );
        String msg = "Creating " + filename + " OK " + elapsed;
        showMessage( msg, false, true );
    } // createTempFirstnameFile


    /**
     * @throws Exception
     */
    private void loadFirstnameToTable() throws Exception
    {
        long start = System.currentTimeMillis();
        showMessage( "Loading CSV data into temp table", false, true );
        {
            String query = "LOAD DATA LOCAL INFILE 'firstname_t.csv' INTO TABLE firstname_t FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' ( person_id , firstname );";
            conTemp.runQuery( query );
        }

        long stop = System.currentTimeMillis();
        String elapsed = Functions.millisec2hms( start, stop );
        String msg = "Loading CSV data into temp table OK " + elapsed;
        showMessage( msg, false, true );
    } // loadFirstnameToTable


    /**
     *
     */
    private void updateFirstnameToPersonC() throws Exception
    {
        long start = System.currentTimeMillis();
        showMessage( "Moving first names from temp table to person_c...", false, true );
        {
            String query = "UPDATE links_cleaned.person_c, links_temp.firstname_t"
                + " SET links_cleaned.person_c.firstname = links_temp.firstname_t.firstname"
                + " WHERE links_cleaned.person_c.id_person = links_temp.firstname_t.person_id;";

            conTemp.runQuery( query );
        }

        long stop = System.currentTimeMillis();
        String elapsed = Functions.millisec2hms( start, stop );
        String msg = "Moving first names from temp table to person_c OK " + elapsed;
        showMessage( msg, false, true );
    } // updateFirstnameToPersonC


    /**
     * @throws Exception
     */
    public void removeFirstnameFile() throws Exception
    {
        long start = System.currentTimeMillis();
        showMessage( "Removing firstname_t csv file", false, true );
        {
            File f = new File( "firstname_t.csv" );
            f.delete();
        }

        long stop = System.currentTimeMillis();
        String elapsed = Functions.millisec2hms( start, stop );
        String msg = "Removing firstname_t csv file OK " + elapsed;
        showMessage( msg, false, true );
    } // removeFirstnameFile


    /**
     * @throws Exception
     */
    public void removeFirstnameTable() throws Exception
    {
        long start = System.currentTimeMillis();
        showMessage( "Removing firstname_t table", false, true );

        String query = "DROP TABLE IF EXISTS firstname_t;";
        conTemp.runQuery( query );

        long stop = System.currentTimeMillis();
        String elapsed = Functions.millisec2hms( start, stop );
        String msg = "Removing firstname_t table OK " + elapsed;
        showMessage( msg, false, true );
    } // removeFirstnameTable


    /**
     * @throws Exception
     */
    private boolean doesTableExist( MySqlConnector db_conn, String db_name, String table_name ) throws Exception
    {
        String query = "SELECT COUNT(*) FROM information_schema.tables"
                + " WHERE table_schema = '" + db_name + "'"
                + " AND table_name = '" + table_name + "'";

        ResultSet rs = db_conn.runQueryWithResult( query );
        rs.first();
        int count = rs.getInt( "COUNT(*)" );
        //showMessage( "doesTableExist: " + db_name + " " + table_name + " : " + count, false, true );

        if( count == 1 ) return true;
        else return false;
    } // doesTableExist


    /**
     * @throws Exception
     */
    private void dropTable( MySqlConnector db_conn, String db_name, String table_name ) throws Exception
    {
        String query = "DROP TABLE `" + db_name + "`.`" + table_name + "`";
        db_conn.runQuery( query );
    } // dropTable


    private void funcPostTasks() throws Exception
    {
        long start = System.currentTimeMillis();
        showMessage( "Post tasks", false, true );

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
        for( String s : queries ) {
            conCleaned.runQuery( s );
        }

        long stop = System.currentTimeMillis();
        String elapsed = Functions.millisec2hms( start, stop );
        String msg = " OK " + elapsed;
        showMessage( msg, false, true );
    } // funcPostTasks


    private void funcDeleteRows()
    throws Exception
    {
        showMessage( "funcDeleteRows() deleting empty links_cleaned.person_c records.", false, true );
        String q1 = "DELETE FROM links_cleaned.person_c WHERE ( familyname = '' OR familyname is null ) AND ( firstname = '' OR firstname is null )";
        conCleaned.runQuery( q1 );
    } // funcDeleteRows


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

    // ---< Min Max Date functions >--------------------------------------------

    /**
     * Min Max Date
     * @param go
     * @throws Exception
     */
    private void doMinMaxDate( boolean go ) throws Exception
    {
        String funcname = "MinMaxDate";
        if( !go ) {
            showMessage( "Skipping " + funcname, false, true );
            return;
        }

        long timeStart = System.currentTimeMillis();
        showMessage( funcname, false, true );

        if (bronFilter.isEmpty()) {
            for (int i : sources) {
                showMessage("Running funMinMaxDateMain for source: " + i + "...", false, false);
                {
                    funcFillMinMaxArrays("" + i);
                    funMinMaxDateMain("" + i);
                }
                showMessage(endl, false, true);
            }
        } else {
            showMessage("Running funMinMaxDateMain...", false, false);
            {
                funcFillMinMaxArrays("" + this.sourceId);
                funMinMaxDateMain("");
            }
            showMessage(endl, false, true);
        }

        elapsedShowMessage( funcname, timeStart, System.currentTimeMillis() );
    } // doMinMaxDate


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

                startQuery += " AND links_cleaned.person_c.id_source = " + this.sourceId;

                idSource = this.sourceId + "";

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

            showMessage("0 of " + total, true, true);

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
                    showMessage(counter + " of " + total, true, true);
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
            showMessage(counter + " An error occured while running Min Max: " + e.getMessage(), false, true);
        }
    } // funMinMaxDateMain


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

    } // funcMinMaxDate


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
    } // funcReturnAgeCentralFigure


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
    } // funcFillMinMaxArrays

}

// [eof]
