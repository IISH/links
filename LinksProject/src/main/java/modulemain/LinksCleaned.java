package modulemain;

import java.lang.reflect.Method;

import java.io.File;
import java.io.FileWriter;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;

import java.sql.SQLException;
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
import dataset.DivideMinMaxDatumSet;
import dataset.DoSet;
import dataset.MarriageYearPersonsSet;
import dataset.MinMaxDateSet;
import dataset.MinMaxMainAgeSet;
import dataset.MinMaxYearSet;
import dataset.PersonC;
import dataset.RegistrationC;
import dataset.RelationSet;
import dataset.TableToArraysSet;
import dataset.TabletoArrayListMultimap;

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
 * FL-26-Sep-2014 Latest change
 */
public class LinksCleaned extends Thread
{
    // Table -> ArraysSet
    private TableToArraysSet ttalPrepiece;          // Names
    private TableToArraysSet ttalSuffix;            // Names
    private TableToArraysSet ttalAlias;             // Names
    //private TableToArraysSet ttalFirstname;         // Names
    //private TableToArraysSet ttalFamilyname;        // names

    //private TableToArraysSet ttalLocation;
    //private TableToArraysSet ttalOccupation;
    private TableToArraysSet ttalRegistration;      // formerly used in standardType()
    private TableToArraysSet ttalReport;
    private TableToArraysSet ttalRole;
    //private TableToArraysSet ttalStatusSex;

    // Table -> ArrayListMultiMap
    //private TabletoArrayListMultimap almmPrepiece;      // Names
    //private TabletoArrayListMultimap almmSuffix;        // Names
    //private TabletoArrayListMultimap almmAlias;         // Names
    private TabletoArrayListMultimap almmFirstname;     // Names
    private TabletoArrayListMultimap almmFamilyname;    // Names

    private TabletoArrayListMultimap almmLocation;
    private TabletoArrayListMultimap almmOccupation;
    //private TabletoArrayListMultimap almmRole;
    private TabletoArrayListMultimap almmCivilstatus;
    private TabletoArrayListMultimap almmSex;

    private JTextField tbLOLClatestOutput;
    private JTextArea  taLOLCoutput;

    // WHERE [...]id_source = ...   shortcuts
    //private String sourceFilter = "";
    //private String sourceFilterCleanPers = "";
    //private String sourceFilterOrigPers = "";
    //private String sourceFilterCleanReg = "";
    //private String sourceFilterOrigReg = "";

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
    private final static String SC_Y = "y"; //    Yes  Standard value assigned (original value is valid)

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
    private int sourceIdGui;

    private int[] sourceList;                      // either sourceListAvail, or [sourceId] from GUI

    private String endl = ". OK.";              // ".";

    private PrintLogger plog;
    private boolean showskip = false;

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
    )
    {
        this.sourceIdGui = sourceId;

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

        class CleaningThread extends Thread
        {
            String source;

            CleaningThread( String source ) {
                this.source = source;
            }

            public void run()
            {
                long begintime = System.currentTimeMillis();

                int ncores = Runtime.getRuntime().availableProcessors();
                showMessage( "Available cores: " + ncores, false, true );
                int nthreads = java.lang.Thread.activeCount();
                showMessage( "Active threads: " + nthreads, false, true );

                String msg = "CleaningThread, source: " + source;
                System.out.println(msg);
                showMessage(msg, false, true);

                try
                {
                    doRenewData( dos.isDoRenewData(), source );                 // GUI cb: Remove previous data

                    //doPreBasicNames( dos.isDoPreBasicNames(), source );         // GUI cb: Basic names temp

                    //doRemarks( dos.isDoRemarks(), source );                     // GUI cb: Parse remarks

                    doNames( dos.isDoNames(), source );                         // GUI cb: Names

                    doLocations( dos.isDoLocations(), source );                 // GUI cb: Locations

                    doStatusSex( dos.isDoStatusSex(), source );                 // GUI cb: Status and Sex

                    doRegType( dos.isDoRegType(), source );                     // GUI cb: Registration Type

                    //doSequence( dos.isDoSequence(), source );                   // GUI cb: Sequence
                    //doRelation( dos.isDoRelation(), source );                   // GUI cb: Relation

                    doOccupation( dos.isDoOccupation(), source );               // GUI cb: Occupation

                    //doAge( dos.isDoAgeYear(), source );                       // part of Dates
                    //doRole( dos.isDoRole(), source );                         // part of Dates
                    doDates( dos.isDoDates(), source );                         // GUI cb: Dates

                    doMinMaxMarriage( dos.isDoMinMaxMarriage(), source );       // GUI cb: Min Max Marriage

                    doPartsToFullDate( dos.isDoPartsToFullDate(), source );     // GUI cb: Parts to Full Date

                    doDaysSinceBegin( dos.isDoDaysSinceBegin(), source );       // GUI cb: Days since begin

                    doPostTasks( dos.isDoPostTasks(), source );                 // GUI cb: Post Tasks
                }
                catch( Exception ex ) {
                    showMessage( "Error: " + ex.getMessage(), false, true );
                }

                msg = "Cleaning sourceId " + source + " is done";
                showTimingMessage( msg, begintime );
            }
        }


        try {
            plog.show( "Links Match Manager 2.0" );
            plog.show( "LinksCleaned/run()" );
            int ncores = Runtime.getRuntime().availableProcessors();
            plog.show( "Available cores: " + ncores );

            long begintime = System.currentTimeMillis();
            logTableName = LinksSpecific.getLogTableName();

            clearTextFields();                                  // Clear output text fields on form
            connectToDatabases();                               // Create databases connectors
            createLogTable();                                   // Create log table with timestamp

            int[] sourceListAvail = getOrigSourceIds();               // get source ids from links_original.registration_o
            sourceList  = createSourceList( sourceIdGui, sourceListAvail );

            if( sourceIdGui == 0 ) {
                String s = "Available source Ids: ";
                for( int i : sourceListAvail ) { s = s + i + " "; }
                showMessage( s, false, true );
            }

            String s = "";
            if( sourceList.length == 1 ) { s = "Processing source: "; }
            else { s = "Processing sources: "; }
            for( int i : sourceList ) { s = s + i + " "; }
            showMessage( s, false, true );

            //if( sourceIdGui != 0 ) { setSourceFilters(); }         // Set source filters

            // links_general.ref_report contains about 75 error definitions,
            // to be used when the normalization encounters errors
            showMessage( "Loading report table...", false, true );
            ttalReport = new TableToArraysSet( conGeneral, conOr, "", "report" );

            for( int sourceId : sourceList )
            {
                String source = Integer.toString( sourceId );

                CleaningThread ct = new CleaningThread( source );
                ct.start();

                /*
                doRenewData( dos.isDoRenewData(), source );                 // GUI cb: Remove previous data

                //doPreBasicNames( dos.isDoPreBasicNames(), source );         // GUI cb: Basic names temp

                //doRemarks( dos.isDoRemarks(), source );                     // GUI cb: Parse remarks

                doNames( dos.isDoNames(), source );                         // GUI cb: Names

                doLocations( dos.isDoLocations(), source );                 // GUI cb: Locations

                doStatusSex( dos.isDoStatusSex(), source );                 // GUI cb: Status and Sex

                doRegType( dos.isDoRegType(), source );                     // GUI cb: Registration Type

                //doSequence( dos.isDoSequence(), source );                   // GUI cb: Sequence
                //doRelation( dos.isDoRelation(), source );                   // GUI cb: Relation

                doOccupation( dos.isDoOccupation(), source );               // GUI cb: Occupation

                //doAge( dos.isDoAgeYear(), source );                       // part of Dates
                //doRole( dos.isDoRole(), source );                         // part of Dates
                doDates( dos.isDoDates(), source );                         // GUI cb: Dates

                doMinMaxMarriage( dos.isDoMinMaxMarriage(), source );       // GUI cb: Min Max Marriage

                doPartsToFullDate( dos.isDoPartsToFullDate(), source );     // GUI cb: Parts to Full Date

                doDaysSinceBegin( dos.isDoDaysSinceBegin(), source );       // GUI cb: Days since begin

                doPostTasks( dos.isDoPostTasks(), source );                 // GUI cb: Post Tasks
                */
            }

            /*
            // we need to open/close the connections in the threads
            // Close db connections
            conOriginal.close();
            conLog.close();
            conCleaned.close();
            conGeneral.close();
            conTemp.close();
            */

            for( int sourceId : sourceList ) {
                String source = Integer.toString( sourceId );
                doPrematch( dos.isDoPrematch(), source );                   // GUI cb: Run PreMatch
            }

            //String msg = "Conversion from Original to Cleaned is done. ";
            //showTimingMessage( msg, begintime );

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
    private void doRenewData( boolean go, String source ) throws Exception
    {
        String funcname = "doRenewData";
        if( !go ) {
            if( showskip ) { showMessage( "Skipping " + funcname, false, true ); }
            return;
        }

        long timeStart = System.currentTimeMillis();
        showMessage( funcname + "...", false, true );

        // Delete cleaned data for given source
        String deletePerson = "DELETE FROM person_c WHERE id_source = " + source;
        String deleteRegist = "DELETE FROM registration_c WHERE id_source = " + source;

        showMessage( "Deleting previous data for source: " + source, false, true );
        conCleaned.runQuery( deletePerson );
        conCleaned.runQuery( deleteRegist );

        // Copy selected columns links_original data to links_cleaned
        // Create queries
        showMessage("Copying links_original person keys to links_cleaned", false, true);
        String keysPerson = ""
            + "INSERT INTO links_cleaned.person_c"
            +      " ( id_person, id_registration, id_source, registration_maintype, id_person_o )"
            + " SELECT id_person, id_registration, id_source, registration_maintype, id_person_o"
            + " FROM links_original.person_o"
            + " WHERE person_o.id_source = " + source;

        //System.out.println( keysPerson );
        conCleaned.runQuery( keysPerson );

        showMessage("Copying links_original registration keys to links_cleaned", false, true);
        String keysRegistration = ""
            + "INSERT INTO links_cleaned.registration_c"
            +      " ( id_registration, id_source, id_persist_registration, id_orig_registration, registration_maintype, registration_seq )"
            + " SELECT id_registration, id_source, id_persist_registration, id_orig_registration, registration_maintype, registration_seq"
            + " FROM links_original.registration_o"
            + " WHERE registration_o.id_source = " + source;

        //System.out.println( keysRegistration );
        conCleaned.runQuery( keysRegistration );

        elapsedShowMessage( funcname, timeStart, System.currentTimeMillis() );
    } // doRenewData


    /**
     * Basic names temp
     * @param go
     * @throws Exception
     */
    /*
    private void doPreBasicNames( boolean go, int sourceId ) throws Exception
    {
        String funcname = "doPreBasicNames";
        if( !go ) {
            if( showskip ) { showMessage( "Skipping " + funcname, false, true ); }
            return;
        }

        long timeStart = System.currentTimeMillis();
        showMessage( funcname + "...", false, true );

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

        //runMethod("standardFirstname");
        for( int i : sourceList ) {
            showMessage( "Processing standardFirstname for source: " + i + "...", false, true );
            standardFirstname( "" + i );
        }

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
        ttalFamilyname = new TableToArraysSet( conGeneral, conOr, "original", "familyname" );

        //runMethod("standardFamilyname");
        for( int i : sourceList ) {
            showMessage( "Processing standardFamilyname for source: " + i + "...", false, true );
            standardFamilyname( "" + i );
        }

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
    */

    /**
     * Parse remarks
     * @param go
     * @throws Exception
     */
    /*
    private void doRemarks( boolean go, int sourceId ) throws Exception
    {
        String funcname = "doRemarks";
        if( !go ) {
            if( showskip ) { showMessage( "Skipping " + funcname, false, true ); }
            return;
        }

        long timeStart = System.currentTimeMillis();
        showMessage( funcname + "...", false, true );

        // load all refs used by remarks Parser
        showMessage( "Loading reference tables: location/occupation...", false, true );
        {
            //ttalLocation = new TableToArraysSet(conGeneral, "original", "location");
            //ttalOccupation = new TableToArraysSet(conGeneral, "original", "occupation");
        }

        //runMethod("scanRemarks");
        for( int i : sourceList ) {
            showMessage( "Processing scanRemarks for source: " + i + "...", false, true );
            scanRemarks( "" + i );
        }

        showMessage( "Updating reference tables: " + "location/occupation" + "...", false, true );
        {
            //ttalLocation.updateTable();
            //ttalOccupation.updateTable();
        }

        elapsedShowMessage( funcname, timeStart, System.currentTimeMillis() );
    } // doRemarks
    */

    /**
     * Names
     * @param go
     * @throws Exception
     */
    private void doNames( boolean go, String source ) throws Exception
    {
        String funcname = "doNames";
        if( !go ) {
            if( showskip ) { showMessage( "Skipping " + funcname, false, true ); }
            return;
        }

        long timeStart = System.currentTimeMillis();
        showMessage( funcname + "...", false, true );

        String mmss = "";
        String msg = "";
        String ts = "";                               // timestamp

        long start = 0;
        long stop = 0;

        ts = LinksSpecific.getTimeStamp2("HH:mm:ss");
        System.out.println(ts + " dos.isDoNames");

        // Loading Prepiece/Suffix/Alias reference tables
        start = System.currentTimeMillis();
        msg = "Loading Prepiece/Suffix/Alias reference tables";
        showMessage( msg + "...", false, true );
        ttalPrepiece = new TableToArraysSet( conGeneral, conOr, "original", "prepiece" );
        ttalSuffix   = new TableToArraysSet( conGeneral, conOr, "original", "suffix" );
        ttalAlias    = new TableToArraysSet( conGeneral, conOr, "original", "alias" );
        //almmPrepiece = new TabletoArrayListMultimap( conGeneral, conOr, "ref_prepiece", "original" );
        //almmSuffix   = new TabletoArrayListMultimap( conGeneral, conOr, "ref_suffix",   "original" );
        //almmAlias    = new TabletoArrayListMultimap( conGeneral, conOr, "ref_alias",    "original" );
        showTimingMessage( msg, start );

        // First name
        String tmp_firstname = "firstname_t_" + source;
        if( doesTableExist( conTemp, "links_temp", tmp_firstname ) ) {
            showMessage( "Deleting table links_temp." + tmp_firstname, false, true );
            dropTable(conTemp, "links_temp", tmp_firstname);
        }
        createTempFirstnameTable( source );
        createTempFirstnameFile(  source );

        start = System.currentTimeMillis();
        msg = "Loading reference table: ref_firstname";
        showMessage( msg + "...", false, true );

        //ttalFirstname = new TableToArraysSet( conGeneral, conOr, "original", "firstname" );
        //showMessage( "Number of rows in reference table: " + ttalFirstname.countRows(), false, true );
        almmFirstname = new TabletoArrayListMultimap( conGeneral, conOr, "ref_firstname", "original", "standard" );
        int numrows = almmFirstname.numrows();
        int numkeys = almmFirstname.numkeys();
        showMessage( "Number of rows in reference table: " + numrows, false, true );
        if( numrows != numkeys )
        { showMessage( "Number of keys in arraylist multimap: " + numkeys, false, true ); }
        showTimingMessage( msg, start );

        msg = "standardFirstname";
        showMessage( msg + "...", false, true );
        standardFirstname( source );
        showTimingMessage( "standardFirstname", start );

        start = System.currentTimeMillis();
        //ttalFirstname.updateTable();
        //ttalFirstname.free();
        almmFirstname.updateTable();
        almmFirstname.free();

        writerFirstname.close();
        loadFirstnameToTable(     source );
        updateFirstnameToPersonC( source );
        removeFirstnameFile(      source );
        removeFirstnameTable(     source );
        showTimingMessage( "remains Firstname", start );

        // Family name
        String tmp_familyname = "familyname_t_" + source;
        if( doesTableExist( conTemp, "links_temp",tmp_familyname  ) ) {
            showMessage( "Deleting table links_temp." + tmp_familyname, false, true );
            dropTable( conTemp, "links_temp", tmp_familyname );
        }

        createTempFamilynameTable( source );
        createTempFamilynameFile(  source );

        start = System.currentTimeMillis();
        msg = "Loading reference table: ref_familyname";
        showMessage( msg + "...", false, true );

        //ttalFamilyname = new TableToArraysSet( conGeneral, conOr, "original", "familyname" );
        //showMessage( "Number of rows in reference table: " + ttalFamilyname.countRows(), false, true );
        almmFamilyname = new TabletoArrayListMultimap( conGeneral, conOr, "ref_familyname", "original", "standard" );
        numrows = almmFamilyname.numrows();
        numkeys = almmFamilyname.numkeys();
        showMessage( "Number of rows in reference table: " + almmFamilyname.numrows(), false, true );
        if( numrows != numkeys )
        { showMessage( "Number of keys in arraylist multimap: " + almmFamilyname.numkeys(), false, true ); }
        showTimingMessage( msg, start );

        msg = "standardFamilyname";
        showMessage( msg + "...", false, true );
        standardFamilyname( source );
        showTimingMessage( msg, start );

        start = System.currentTimeMillis();
        msg = "remains Familyname";
        showMessage( msg + "...", false, true );

        //ttalFamilyname.updateTable();
        //ttalFamilyname.free();
        almmFamilyname.updateTable();
        almmFamilyname.free();

        writerFamilyname.close();
        loadFamilynameToTable(     source );
        updateFamilynameToPersonC( source );
        removeFamilynameFile(      source );
        removeFamilynameTable(     source );
        showTimingMessage( msg, start );

        // KM: Do not delete here.
        //showMessage("Skipping deleting empty links_cleaned.person_c records.", false, true);
        //deleteRows();               // Delete records with empty firstname and empty familyname

        // Names to lowercase
        start = System.currentTimeMillis();
        msg = "Converting names to lowercase";
        showMessage( msg + "...", false, true ) ;
        String qLower = "UPDATE links_cleaned.person_c SET firstname = LOWER(firstname),  familyname = LOWER(familyname);";
        conCleaned.runQuery( qLower );

        standardPrepiece( source );
        standardSuffix( source );

        showTimingMessage( msg, start );

        // Update reference
        start = System.currentTimeMillis();
        msg = "Updating reference tables: Prepiece/Suffix/Alias...";
        showMessage( msg + "...", false, true );

        ttalPrepiece.updateTable();
        ttalSuffix.updateTable();
        ttalAlias.updateTable();
        //almmPrepiece.updateTable();
        //almmSuffix.updateTable();
        //almmAlias.updateTable();

        showTimingMessage( msg, start );

        elapsedShowMessage( funcname, timeStart, System.currentTimeMillis() );
    } // doNames


    /**
     * Locations
     * @param go
     * @throws Exception
     */
    private void doLocations( boolean go, String source ) throws Exception
    {
        String funcname = "doLocations";
        if( !go ) {
            if( showskip ) { showMessage( "Skipping " + funcname, false, true ); }
            return;
        }

        long timeStart = System.currentTimeMillis();
        String msg = "Loading reference table: location";
        showMessage( msg + "...", false, true );
        long start = System.currentTimeMillis();
        //ttalLocation = new TableToArraysSet( conGeneral, conOr, "original", "location" );
        almmLocation = new TabletoArrayListMultimap( conGeneral, conOr, "ref_location", "original", "location_no" );
        showTimingMessage( msg, start );
        int numrows = almmLocation.numrows();
        int numkeys = almmLocation.numkeys();
        showMessage( "Number of rows in reference table: " + numrows, false, true );
        if( numrows != numkeys )
        { showMessage( "Number of keys in arraylist multimap: " + numkeys, false, true ); }

        start = System.currentTimeMillis();
        standardRegistrationLocation( source );
        showTimingMessage( "standardRegistrationLocation ", start );

        start = System.currentTimeMillis();
        standardBirthLocation( source );
        showTimingMessage( "standardBirthLocation ", start );

        start = System.currentTimeMillis();
        standardMarLocation( source );
        showTimingMessage( "standardMarLocation ", start );

        start = System.currentTimeMillis();
        standardDeathLocation( source );
        showTimingMessage( "standardDeathLocation ", start );

        start = System.currentTimeMillis();
        showMessage( "Updating reference table: location...", false, true );
        //ttalLocation.updateTable();
        almmLocation.updateTable();
        showTimingMessage( "Updating reference table: location ", start );

        elapsedShowMessage( funcname, timeStart, System.currentTimeMillis() );
    } // doLocations


    /**
     * Sex and Civilstatus
     * @param go
     * @throws Exception
     */
    private void doStatusSex( boolean go, String source ) throws Exception
    {
        String funcname = "doStatusSex";
        if( !go ) {
            if( showskip ) { showMessage( "Skipping " + funcname, false, true ); }
            return;
        }

        long timeStart = System.currentTimeMillis();
        showMessage( funcname + "...", false, true );

        showMessage( "Loading reference table: ref_status_sex (sex as key)...", false, true );
        //ttalStatusSex = new TableToArraysSet( conGeneral, conOr, "original", "status_sex" );
        almmSex = new TabletoArrayListMultimap( conGeneral, conOr, "ref_status_sex", "original", "standard_sex" );

        showMessage("Loading reference table: status_sex (civil status as key)...", false, true);
        almmCivilstatus = new TabletoArrayListMultimap( conGeneral, conOr, "ref_status_sex", "original", "standard_civilstatus" );

        int numrows = almmCivilstatus.numrows();
        int numkeys = almmCivilstatus.numkeys();
        showMessage( "Number of rows in reference table: " + numrows, false, true );
        if( numrows != numkeys )
        { showMessage( "Number of keys in arraylist multimap: " + numkeys, false, true ); }

        standardSex( source );
        standardCivilstatus(source);

        //ttalStatusSex.updateTable();
        showMessage("Updating reference table: ref_status_sex", false, true);
        almmCivilstatus.updateTable();

        almmSex.free();
        almmCivilstatus.free();

        elapsedShowMessage( funcname, timeStart, System.currentTimeMillis() );
    } // doStatusSex


    /**
     * Registration Type
     * @param go
     * @throws Exception
     */
    private void doRegType( boolean go, String source ) throws Exception
    {
        String funcname = "doType";
        if( !go ) {
            if( showskip ) { showMessage( "Skipping " + funcname, false, true ); }
            return;
        }

        long timeStart = System.currentTimeMillis();
        showMessage( funcname + "...", false, true );

        standardType( source );

        elapsedShowMessage( funcname, timeStart, System.currentTimeMillis() );
    } // doRegType


    /**
     * Sequence
     * @param go
     * @throws Exception
     */
    /*
    private void doSequence( boolean go, String source ) throws Exception
    {
        String funcname = "doSequence";
        if( !go ) {
            if( showskip ) { showMessage( "Skipping " + funcname, false, true ); }
            return;
        }

        long timeStart = System.currentTimeMillis();
        showMessage( funcname + "...", false, true );

        standardSequence( source );

        elapsedShowMessage( funcname, timeStart, System.currentTimeMillis() );
    } // doSequence
    */

    /**
     * Relation
     * @param go
     * @throws Exception
     */
    /*
    private void doRelation( boolean go, String source ) throws Exception
    {
        String funcname = "doRelation";
        if( !go ) {
            if( showskip ) { showMessage( "Skipping " + funcname, false, true ); }
            return;
        }

        long timeStart = System.currentTimeMillis();
        showMessage( funcname + "...", false, true );

        funcRelation( source );

        elapsedShowMessage( funcname, timeStart, System.currentTimeMillis() );
    } // doRelation
    */

    /**
     * Year Age
     * @param go
     * @throws Exception
     */
    private void doAge( boolean go, String source ) throws Exception
    {
        String funcname = "doAge";
        if( !go ) {
            if( showskip ) { showMessage( "Skipping " + funcname, false, true ); }
            return;
        }

        long timeStart = System.currentTimeMillis();

        showMessage( funcname + "...", false, true );

        showMessage( "Processing standardAge for source: " + source + "...", false, true );
        standardAge( source );

        elapsedShowMessage( funcname, timeStart, System.currentTimeMillis() );
    } // doAge


    /**
     * Role
     * @param go
     * @throws Exception
     */
    private void doRole( boolean go, String source ) throws Exception
    {
        String funcname = "doRole";
        if( !go ) {
            if( showskip ) { showMessage( "Skipping " + funcname, false, true ); }
            return;
        }

        showMessage( funcname + "...", false, true );

        long start = System.currentTimeMillis();

        ttalRole = new TableToArraysSet( conGeneral, conOr, "original", "role" );
        int size = ttalRole.countRows();
        //almmRole = new TabletoArrayListMultimap( conGeneral, conOr, "ref_role", "original", "standard" );
        //almmRole.contentsOld();
        //int size = almmRole.numkeys();

        elapsedShowMessage( "almRole [" + size + " records]", start, System.currentTimeMillis() );

        long timeStart = System.currentTimeMillis();

        showMessage( "Processing standardRole for source: " + source + "...", false, true );
        standardRole( source );

        ttalRole.updateTable();
        ttalRole.free();
        //almmRole.updateTable();
        //almmRole.free();

        elapsedShowMessage( funcname, timeStart, System.currentTimeMillis() );
    } // doRole


    /**
     * doOccupation
     * @param go
     * @throws Exception
     */
    private void doOccupation( boolean go, String source ) throws Exception
    {
        boolean debug = false;

        String funcname = "doOccupation";
        if( !go ) {
            if( showskip ) { showMessage( "Skipping " + funcname, false, true ); }
            return;
        }

        long funcstart = System.currentTimeMillis();
        showMessage( funcname + "...", false, true );

        long start = System.currentTimeMillis();
        String msg = "Loading reference table: ref_occupation";
        showMessage( msg + "...", false, true );
        //ttalOccupation = new TableToArraysSet( conGeneral, conOr, "original", "occupation" );
        almmOccupation = new TabletoArrayListMultimap( conGeneral, conOr, "ref_occupation", "original", "standard" );
        elapsedShowMessage( msg, start, System.currentTimeMillis() );

        //showMessage( "Number of rows in reference table: " + ttalOccupation.countRows(), false, true );
        int numrows = almmOccupation.numrows();
        int numkeys = almmOccupation.numkeys();
        showMessage( "Number of rows in reference table: " + numrows, false, true );
        if( numrows != numkeys )
        { showMessage( "Number of keys in arraylist multimap: " + numkeys, false, true ); }

        showMessage( "Processing standardOccupation for source: " + source + "...", false, true );
        standardOccupation( debug, source );

        showMessage( "Updating reference table: ref_occupation", false, true );
        //ttalOccupation.updateTable();
        almmOccupation.updateTable();
        //ttalOccupation.free();
        almmOccupation.free();

        elapsedShowMessage( funcname, funcstart, System.currentTimeMillis() );
    } // doOccupation


    /**
     * doDates
     * @param go
     * @throws Exception
     */
    private void doDates( boolean go, String source ) throws Exception
    {
        String funcname = "doDates";
        if( !go ) {
            if( showskip ) { showMessage( "Skipping " + funcname, false, true ); }
            return;
        }

        long timeStart = System.currentTimeMillis();
        showMessage( funcname + "...", false, true );

        doAge(  go, source );        // required for dates
        doRole( go, source );        // required for dates

        showMessage( "Processing standardRegistrationDate for source: " + source + "...", false, true );
        standardRegistrationDate( source );

        showMessage( "Processing standardDate for source: " + source + "...", false, true );
        standardDate( source, "birth" );
        standardDate( source, "mar" );
        standardDate( source, "death" );

        showMessage( "Processing setComplete for source: " + source + "...", false, true );
        setValidDateComplete( source );

        showMessage( "Processing minMaxValidDate for source: " + source + "...", false, true );
        minMaxValidDate( source );

        //fillMinMaxArrays( "" + i );

        showMessage( "Processing minMaxDateMain for source: " + source + "...", false, true );
        minMaxDateMain( source );


        // Fill empty dates with register dates: this is still buggy
        /*
        flagBirthDate();
        flagMarriageDate();
        flagDeathDate();

        standardFlaggedDate( "birth" );
        standardFlaggedDate( "mar" );
        standardFlaggedDate( "death" );
        */

        // FL-04-Sep-2014
        // commented out function calls below: probably overlapping functionality with doMinMaxDate ?!

        //showMessage( "Running completeMinMaxBirth...", false, true );
        //completeMinMaxBirth();

        //showMessage( "Running completeMinMaxMar...", false, true );
        //completeMinMaxMar();

        // no function completeMinMaxDeath() : NO, not needed


        if( showskip ) { showMessage( "Skipping registration_c updates", false, true ); }
        /*
        showMessage( "Running update queries...", false, true );
        // extra function to correct registration data
        String q1 = "UPDATE links_cleaned.registration_c AS r, links_cleaned.person_c AS p SET r.registration_date = p.birth_date WHERE r.registration_date IS NULL AND r.id_registration = p.id_registration AND r.registration_maintype = 1 AND p.role = 1;";
        String q2 = "UPDATE links_cleaned.registration_c AS r, links_cleaned.person_c AS p SET r.registration_date = p.mar_date   WHERE r.registration_date IS NULL AND r.id_registration = p.id_registration AND r.registration_maintype = 2 AND p.role = 4;";
        String q3 = "UPDATE links_cleaned.registration_c AS r, links_cleaned.person_c AS p SET r.registration_date = p.mar_date   WHERE r.registration_date IS NULL AND r.id_registration = p.id_registration AND r.registration_maintype = 2 AND p.role = 7;";
        String q4 = "UPDATE links_cleaned.registration_c AS r, links_cleaned.person_c AS p SET r.registration_date = p.death_date WHERE r.registration_date IS NULL AND r.id_registration = p.id_registration AND r.registration_maintype = 3 AND p.role = 10;";
        String q5 = "UPDATE links_cleaned.registration_c AS r, links_cleaned.person_c AS p SET r.registration_date = p.death_date WHERE r.registration_date IS NULL AND r.id_registration = p.id_registration AND r.registration_maintype = 7 AND p.role = 10;";

        conCleaned.runQuery( q1 );
        conCleaned.runQuery( q2 );
        conCleaned.runQuery( q3 );
        conCleaned.runQuery( q4 );
        conCleaned.runQuery( q5 );
        */


        // the remains are copied from doMinMaxDate()
        //minMaxValidDate();              // for all sources ? NO, per source, -> change

        /*
        if( sourceFilter.isEmpty() ) {
            for( int i : sourceList ) {
                showMessage( "Processing minMaxValidDate for source: " + i + "...", false, true );
                minMaxValidDate( "" + i );

                //fillMinMaxArrays( "" + i );

                showMessage( "Processing minMaxDateMain for source: " + i + "...", false, true );
                minMaxDateMain(   "" + i );
            }
        } else {
            showMessage( "Processing minMaxValidDate", false, true );
            minMaxValidDate( "" + this.sourceId );

            //fillMinMaxArrays( "" + this.sourceId );

            showMessage( "Processing minMaxDateMain", false, true );
            minMaxDateMain( "" + this.sourceId );
        }
        */

        elapsedShowMessage( funcname, timeStart, System.currentTimeMillis() );
    } // doDates


    /**
     * Min Max Marriage
     * @param go
     * @throws Exception
     */
    private void doMinMaxMarriage( boolean go, String source ) throws Exception
    {
        String funcname = "doMinMaxMarriage";
        if( !go ) {
            if( showskip ) { showMessage( "Skipping " + funcname, false, true ); }
            return;
        }

        long timeStart = System.currentTimeMillis();
        showMessage( funcname + "...", false, true );

        try
        {
            ResultSet refMinMaxMarriageYear = conGeneral.runQueryWithResult( "SELECT * FROM ref_minmax_marriageyear" );

            showMessage( "Processing minMaxMarriageYear for source: " + source + "...", false, true );
            minMaxMarriageYear( setMarriageYear(  source ), refMinMaxMarriageYear );
        }
        catch( Exception ex ) {
            showMessage( "Exception while running minMaxMarriageYear, properly ref_minmax_marriageyear error: " + ex.getMessage(), false, true );
        }

        elapsedShowMessage( funcname, timeStart, System.currentTimeMillis() );
    } // doMinMaxMarriage


    /**
     * Parts to Full Date
     * @param go
     * @throws Exception
     */
    private void doPartsToFullDate( boolean go, String source ) throws Exception
    {
        String funcname = "doPartsToFullDate";
        if( !go ) {
            if( showskip ) { showMessage( "Skipping " + funcname, false, true ); }
            return;
        }

        long timeStart = System.currentTimeMillis();
        showMessage( funcname + "...", false, true );

        showMessage( "processing partsToDate for source: " + source + "...", false, true );
        partsToDate( source );

        elapsedShowMessage( funcname, timeStart, System.currentTimeMillis() );
    } // doPartsToFullDate


    /**
     * Days since begin
     * @param go
     * @throws Exception
     */
    private void doDaysSinceBegin( boolean go, String source ) throws Exception
    {
        String funcname = "doDaysSinceBegin";
        if( !go ) {
            if( showskip ) { showMessage( "Skipping " + funcname, false, true ); }
            return;
        }

        long timeStart = System.currentTimeMillis();
        showMessage( funcname + "...", false, true );

        showMessage( "Processing daysSinceBegin for source: " + source + "...", false, true );
        daysSinceBegin( source );

        elapsedShowMessage( funcname, timeStart, System.currentTimeMillis() );
    } // doDaysSinceBegin


    /**
     * Post Tasks
     * @param go
     * @throws Exception
     */
    private void doPostTasks( boolean go, String source ) throws Exception
    {
        String funcname = "doPostTasks";
        if( !go ) {
            if( showskip ) { showMessage( "Skipping " + funcname, false, true ); }
            return;
        }

        long timeStart = System.currentTimeMillis();
        showMessage( funcname + "...", false, true );

        showMessage( "Processing postTasks for source: " + source + "...", false, true );
        postTasks( source );

        elapsedShowMessage( funcname, timeStart, System.currentTimeMillis() );
    } // doPostTasks


    /**
     * Run PreMatch
     * @param go
     * @throws Exception
     */
    private void doPrematch( boolean go, String source ) throws Exception
    {
        String funcname = "doPrematch";
        if( !go ) {
            if( showskip ) { showMessage( "Skipping " + funcname, false, true ); }
            return;
        }

        long timeStart = System.currentTimeMillis();
        showMessage( funcname + "...", false, true );

        showMessage("Running PREMATCH...", false, false);
        mg.firePrematch();

        elapsedShowMessage( funcname, timeStart, System.currentTimeMillis() );
    } // doPrematch


     /*---< Standardizing functions >-----------------------------------------*/

    /**
     * @param id
     * @param souce
     * @param name
     * @return
     */
    private String standardAlias( int id, String souce, String name ) throws Exception
    {
        dataset.ArrayListNonCase ag = ttalAlias.getArray( "original" );

        name = name.toLowerCase();

        for( Object ags : ag )
        {
            String keyword = " " + ags.toString().toLowerCase() + " ";

            if( name.contains( " " + keyword + " " ) )
            {
                addToReportPerson( id, souce, 17, name );      // EC 17

                // prepare on braces
                if( keyword.contains( "\\(" ) || keyword.contains( "\\(" ) ) {
                    keyword = keyword.replaceAll( "\\(", "" ).replaceAll( "\\)", "" );
                }

                String[] names = name.toLowerCase().split( keyword, 2 );

                // we must clean the name because of the braces used in aliasses

                // Set alias
                PersonC.updateQuery( "alias", LinksSpecific.funcCleanSides(cleanName( names[ 1 ] ) ), id );

                return LinksSpecific.funcCleanSides( cleanName( names[ 0 ] ) );
            }
        }

        return name;
    } // standardAlias


    /**
     * @param source
     */
    public void standardBirthLocation( String source )
    {
        String selectQuery = "SELECT id_person , birth_location FROM person_o WHERE id_source = " + source + " AND birth_location <> ''";

        try {
            ResultSet rs = conOriginal.runQueryWithResult( selectQuery );

            standardLocation( rs, "id_person", "birth_location", "birth_location", source, TableType.PERSON );
        } catch( Exception ex ) {
            showMessage( ex.getMessage(), false, true );
        }
    } // standardBirthLocation


    /**
     * @param type      // "birth", "mar", or "death"
     */
    public void standardDate( String source, String type )
    {
        int counter = 0;
        int step = 10000;
        int stepstate = step;

        try
        {
            String startQuery = "SELECT id_person , id_source , " + type + "_date FROM person_o WHERE " + type + "_date is not null";
            startQuery =  startQuery + " AND id_source = " + source;

            ResultSet rs = conOriginal.runQueryWithResult( startQuery );

            while( rs.next() )
            {
                // GUI info
                counter++;
                if( counter == stepstate ) {
                    showMessage( counter + "", true, true );
                    stepstate += step;
                }

                int id_person = rs.getInt( "id_person" );
                int id_source = rs.getInt( "id_source" );
                String date   = rs.getString( type + "_date" );

                if( date.isEmpty() ) { continue; }

                DateYearMonthDaySet dymd = LinksSpecific.divideCheckDate( date );

                if( dymd.isValidDate() )
                {
                    String query = ""
                        + "UPDATE person_c "
                        + "SET person_c." + type + "_date = '" + dymd.getDay() + "-" + dymd.getMonth() + "-" + dymd.getYear() + "' , "
                        + "person_c." + type + "_day = " + dymd.getDay() + " , "
                        + "person_c." + type + "_month = " + dymd.getMonth() + " , "
                        + "person_c." + type + "_year = " + dymd.getYear() + " , "
                        + "person_c." + type + "_date_valid = 1 "
                        + "WHERE person_c.id_person = " + id_person;

                    conCleaned.runQuery(query);
                }
                else
                {
                    addToReportPerson( id_person, id_source + "", 211, dymd.getReports() );   // EC 211

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

            showMessage( "Number of " + type + " records: " + counter, false, true );
            rs = null;
        } catch( Exception ex ) {
            showMessage( counter + " Exception while cleaning " + type + " date: " + ex.getMessage(), false, true );
        }
    } // standardDate


    /**
     * @param source
     */
    public void standardDeathLocation( String source )
    {
        String selectQuery = "SELECT id_person , death_location FROM person_o WHERE id_source = " + source + " AND death_location <> ''";

        try {
            ResultSet rs = conOriginal.runQueryWithResult( selectQuery );
            standardLocation( rs, "id_person", "death_location", "death_location", source, TableType.PERSON );
        } catch( Exception ex ) {
            showMessage( ex.getMessage(), false, true );
        }
    } // standardDeathLocation


    /**
     * @param source
     * @throws Exception
     */
    public void standardFirstname( String source )
    {
        int count = 0;
        int count_empty = 0;
        int step = 10000;
        int stepstate = step;

        try
        {
            // WHY IS A LOCAL CONNECTION USED?
            Connection con = getConnection( "links_original" );
            con.isReadOnly();

            String selectQuery = "SELECT id_person , firstname FROM person_o WHERE id_source = " + source;

            ResultSet rsFirstName = con.createStatement().executeQuery( selectQuery );
            con.createStatement().close();

            // get total
            rsFirstName.last();
            int total = rsFirstName.getRow();
            rsFirstName.beforeFirst();

            while( rsFirstName.next() )
            {
                count++;
                if( count == stepstate ) {
                    showMessage( count + " of " + total, true, true );
                    stepstate += step;
                }

                int id_person = rsFirstName.getInt( "id_person" );
                String firstname = rsFirstName.getString( "firstname" );

                // Is firstname empty?
                if( firstname != null && !firstname.isEmpty() )
                {
                    firstname = cleanFirstName( firstname );
                    firstname = firstname.toLowerCase();

                    // Check name on aliasses
                    String nameNoAlias = standardAlias( id_person, source, firstname );

                    // Check on serried spaces; // Split name on spaces
                    String[] names = nameNoAlias.split( " " );
                    boolean spaces = false;

                    ArrayList<String> preList  = new ArrayList<String>();
                    ArrayList<String> postList = new ArrayList<String>();

                    for( String n : names ) {
                        if( n.isEmpty() ) {
                            spaces = true;
                        } else { // add to list
                            preList.add( n );
                        }
                    }

                    if( spaces ) {
                        addToReportPerson( id_person, source, 1103, "" );      // EC 1103
                    }

                    // loop through names
                    for( int i = 0; i < preList.size(); i++ )
                    {
                        String prename = preList.get( i );       // Does this aprt exists in ref_name?

                        //if( ttalFirstname.originalExists( prename ) )
                        if( almmFirstname.contains( prename ) )
                        {
                            // Check the standard code
                            //String standard_code = ttalFirstname.getStandardCodeByOriginal( prename );
                            String standard_code = almmFirstname.code(prename);

                            if( standard_code.equals( SC_Y ) )
                            {
                                //postList.add( ttalFirstname.getStandardByOriginal( prename ) );
                                postList.add( almmFirstname.standard( prename ) );
                            }
                            else if( standard_code.equals( SC_U ) )
                            {
                                addToReportPerson( id_person, source, 1100, prename );           // EC 1100
                                //postList.add( ttalFirstname.getStandardByOriginal( prename ) );
                                postList.add( almmFirstname.standard( prename ) );
                            }
                            else if( standard_code.equals( SC_N ) )
                            {
                                addToReportPerson( id_person, source, 1105, prename );           // EC 1105
                            }
                            else if( standard_code.equals( SC_X ) )
                            {
                                addToReportPerson( id_person, source, 1109, prename );           // EC 1109
                                postList.add(preList.get(i));
                            }
                            else {
                                addToReportPerson( id_person, source, 1100, prename );           // EC 1100
                            }
                        }
                        else    // name does not exist in ref_firstname
                        {
                            // check on invalid token
                            String nameNoInvalidChars = cleanName( prename );

                            // name contains invalid chars ?
                            if( ! prename.equalsIgnoreCase( nameNoInvalidChars ) )
                            {
                                addToReportPerson( id_person, source, 1104, prename );  // EC 1104

                                // Check if name exists in ref
                                // Does this aprt exists in ref_name?
                                //if( ttalFirstname.originalExists( nameNoInvalidChars ) )
                                if( almmFirstname.contains( nameNoInvalidChars ) )
                                {
                                    // Check the standard code
                                    //String standard_code = ttalFirstname.getStandardCodeByOriginal( nameNoInvalidChars );
                                    String standard_code = almmFirstname.code(nameNoInvalidChars);

                                    if( standard_code.equals( SC_Y ) )
                                    {
                                        //postList.add( ttalFirstname.getStandardByOriginal( nameNoInvalidChars ) );
                                        postList.add( almmFirstname.standard( nameNoInvalidChars ) );
                                    }
                                    else if( standard_code.equals( SC_U ) )
                                    {
                                        addToReportPerson( id_person, source, 1100, nameNoInvalidChars );    // EC 1100
                                        //postList.add( ttalFirstname.getStandardByOriginal( nameNoInvalidChars ) );
                                        postList.add( almmFirstname.standard( nameNoInvalidChars ) );
                                    }
                                    else if( standard_code.equals( SC_N ) )
                                    {
                                        addToReportPerson( id_person, source, 1105, nameNoInvalidChars );    // EC 1105
                                    }
                                    else if( standard_code.equals( SC_X ) )
                                    {
                                        addToReportPerson(id_person, source, 1109, nameNoInvalidChars);      // EC 1109
                                        postList.add( nameNoInvalidChars );
                                    }
                                    else { // EC 1100, standard_code not invalid
                                        addToReportPerson(id_person, source, 1100, nameNoInvalidChars);      // EC 1100
                                    }

                                    continue;
                                }

                                // Check on suffix
                                ArrayListNonCase sfxO  = ttalSuffix.getArray( "original" );
                                ArrayListNonCase sfxSc = ttalSuffix.getArray( "standard_code" );

                                for( int j = 0; j < sfxO.size(); j++ )
                                {
                                    if( nameNoInvalidChars.endsWith( " " + sfxO.get( j ).toString() )
                                        && sfxSc.get( j ).toString().equals( SC_Y ) )
                                    {
                                        addToReportPerson( id_person, source, 1106, nameNoInvalidChars );  // EC 1106

                                        nameNoInvalidChars = nameNoInvalidChars.replaceAll( " " + sfxO.get( j ).toString(), "" );

                                        // Set suffix
                                        String query = PersonC.updateQuery( "suffix", sfxO.get( j ).toString(), id_person );

                                        conCleaned.runQuery( query );
                                    }
                                }

                                // check ref_prepiece
                                String nameNoPieces = namePrepiece( nameNoInvalidChars, id_person );

                                if( !nameNoPieces.equals( nameNoInvalidChars ) ) {
                                    addToReportPerson(id_person, source, 1107, nameNoInvalidChars);  // EC 1107
                                }

                                // last check on ref
                                //if( ttalFirstname.originalExists( nameNoPieces ) )
                                if( almmFirstname.contains( nameNoPieces ) )
                                {
                                    // Check the standard code
                                    //String standard_code = ttalFirstname.getStandardCodeByOriginal( nameNoPieces );
                                    String standard_code = almmFirstname.code(nameNoPieces);

                                    if( standard_code.equals( SC_Y ) )
                                    {
                                        //postList.add( ttalFirstname.getStandardByOriginal( nameNoPieces ) );
                                        postList.add( almmFirstname.standard( nameNoPieces ) );
                                    }
                                    else if( standard_code.equals( SC_U ) )
                                    {
                                        addToReportPerson( id_person, source, 1100, nameNoPieces );    // EC 1100
                                        //postList.add( ttalFirstname.getStandardByOriginal( nameNoPieces ) );
                                        postList.add( almmFirstname.standard( nameNoPieces ) );
                                    }
                                    else if( standard_code.equals( SC_N ) )
                                    {
                                        addToReportPerson( id_person, source, 1105, nameNoPieces );    // EC 1105
                                    }
                                    else if( standard_code.equals( SC_X ) )
                                    {
                                        addToReportPerson( id_person, source, 1109, nameNoPieces );   // EC 1109
                                        postList.add( nameNoPieces );
                                    }
                                    else { // EC 1100, standard_code not invalid
                                        addToReportPerson( id_person, source, 1100, nameNoPieces );    // EC 1100
                                    }
                                }
                                else {
                                    // name must be added to ref_firstname with standard_code x
                                    //ttalFirstname.addOriginal( nameNoPieces );
                                    almmFirstname.add( nameNoPieces );
                                    postList.add( nameNoPieces );   // Also add name to postlist
                                }
                            }
                            else  // no invalid token
                            {
                                // check if it ends with suffix
                                // Check on suffix
                                ArrayListNonCase sfxO  = ttalSuffix.getArray( "original" );
                                ArrayListNonCase sfxSc = ttalSuffix.getArray( "standard_code" );

                                for( int j = 0; j < sfxO.size(); j++ )
                                {
                                    if( nameNoInvalidChars.equalsIgnoreCase( sfxO.get( j ).toString() )
                                        && sfxSc.get( j ).toString().equals( SC_Y ) )
                                    {
                                        addToReportPerson( id_person, source, 1106, nameNoInvalidChars );  // EC 1106

                                        nameNoInvalidChars = nameNoInvalidChars.replaceAll( sfxO.get( j ).toString(), "" );

                                        // Set suffix
                                        String query = PersonC.updateQuery( "suffix", sfxO.get( j ).toString(), id_person );

                                        conCleaned.runQuery( query );
                                    }
                                }

                                // check ref_prepiece
                                String nameNoPieces = namePrepiece( nameNoInvalidChars, id_person );

                                if( !nameNoPieces.equals( nameNoInvalidChars ) ) {
                                    addToReportPerson(id_person, source, 1107, nameNoInvalidChars);   // EC 1107
                                }

                                // last check on ref
                                //if( ttalFirstname.originalExists( nameNoPieces ) )
                                if( almmFirstname.contains( nameNoPieces ) )
                                {
                                    // Check the standard code
                                    //String standard_code = ttalFirstname.getStandardCodeByOriginal( nameNoPieces );
                                    String standard_code = almmFirstname.code(nameNoPieces);

                                    if( standard_code.equals( SC_Y ) )
                                    {
                                        //postList.add( ttalFirstname.getStandardByOriginal( nameNoPieces ) );
                                        postList.add( almmFirstname.standard( nameNoPieces ) );
                                    }
                                    else if( standard_code.equals( SC_U ) )
                                    {
                                        addToReportPerson( id_person, source, 1100, nameNoPieces );   // EC 1100
                                        //postList.add( ttalFirstname.getStandardByOriginal( nameNoPieces ) );
                                        postList.add( almmFirstname.standard( nameNoPieces ) );
                                    }
                                    else if( standard_code.equals( SC_N ) )
                                    {
                                        addToReportPerson( id_person, source, 1105, nameNoPieces );    // EC 1105
                                    }
                                    else if( standard_code.equals( SC_X ) )
                                    {
                                        addToReportPerson( id_person, source, 1109, nameNoPieces );    // EC 1109
                                        postList.add( nameNoPieces );
                                    }
                                    else { // EC 1100, standard_code not invalid
                                        addToReportPerson(id_person, source, 1100, nameNoPieces);    // EC 1100
                                    }
                                }
                                else {
                                    // name must be added to ref_firstname with standard_code x
                                    //ttalFirstname.addOriginal( nameNoPieces );
                                    almmFirstname.add( nameNoPieces );
                                    postList.add( nameNoPieces );   // Also add name to postlist
                                }
                            }
                        }
                    }

                    // Write all parts to Person POSTLIST
                    String vn = "";

                    for( int i = 0; i < postList.size(); i++ )
                    {
                        vn += postList.get( i );

                        // posible space
                        if( i != ( postList.size() - 1) ) {
                            vn += " ";
                        }
                    }

                    // if vn not empty write to vn
                    if( !vn.isEmpty() ) {
                        //String query = PersonC.updateQuery("firstname", vn, id_person);
                        //conCleaned.runQuery(query);

                        writerFirstname.write( id_person + "," + vn.trim().toLowerCase() + "\n" );

                    }

                    preList.clear();
                    postList.clear();
                    preList = null;
                    postList = null;
                }
                else    // First name is empty
                {
                    count_empty++;
                    addToReportPerson( id_person, source, 1101, "" );        // EC 1101
                }

                // close this
                id_person = 0;
                firstname = null;
            }

            // TODO: empty resultset
            //rsFirstName = con.createStatement().executeQuery("SELECT 0;");

            rsFirstName.close();
            con.close();

            int count_new = almmFirstname.newcount();
            String strNew = "";
            if( count_new == 0 ) { strNew = "no new firstnames"; }
            else if( count_new == 1 ) { strNew = "1 new firstname"; }
            else { strNew = "" + count_new + " new firstnames"; }
            showMessage( count + " firstname records, " + count_empty + " without a firstname, " + strNew, false, true );
        }
        catch( Exception ex ) {
            showMessage( count + " Exception while cleaning Firstname: " + ex.getMessage(), false, true );
        }
    } // standardFirstname


    /**
     * @param source
     * @throws Exception
     */
    public void standardFamilyname( String source )
    {
        int count = 0;
        int count_empty = 0;
        int step = 10000;
        int stepstate = step;

        try {
            String selectQuery = "SELECT id_person , familyname FROM person_o WHERE id_source = " + source;

            // WHY IS A LOCAL CONNECTION USED?
            Connection con = getConnection( "links_original" );
            con.isReadOnly();

            // Read family names from table
            ResultSet rsFamilyname = con.createStatement().executeQuery( selectQuery );
            con.createStatement().close();

            // get total
            rsFamilyname.last();
            int total = rsFamilyname.getRow();
            rsFamilyname.beforeFirst();

            while( rsFamilyname.next() )
            {
                count++;
                if( count == stepstate ) {
                    showMessage( count + " of " + total, true, true );
                    stepstate += step;
                }

                // Get family name
                String familyname = rsFamilyname.getString( "familyname" );
                int id_person = rsFamilyname.getInt( "id_person" );

                // Check is Familyname is not empty or null
                if( familyname != null && !familyname.isEmpty() )
                {
                    familyname = cleanFamilyname( familyname );
                    familyname = familyname.toLowerCase();

                    // familyname in ref_familyname ?
                    //if( ttalFamilyname.originalExists( familyname ) )
                    if( almmFamilyname.contains( familyname ) )
                    {
                        // get standard_code
                        //String standard_code = ttalFamilyname.getStandardCodeByOriginal( familyname );
                        String standard_code = almmFamilyname.code(familyname);

                        // Check the standard code
                        if( standard_code.equals( SC_Y ) )
                        {
                            //writerFamilyname.write( id_person + "," + ttalFamilyname.getStandardByOriginal( familyname ).toLowerCase() + "\n" );
                            writerFamilyname.write( id_person + "," + almmFamilyname.standard( familyname ).toLowerCase() + "\n" );
                        }
                        else if( standard_code.equals( SC_U ) )
                        {
                            addToReportPerson( id_person, source, 1000, familyname ); // EC 1000

                            //writerFamilyname.write( id_person + "," + ttalFamilyname.getStandardByOriginal( familyname ).toLowerCase() + "\n" );
                            writerFamilyname.write( id_person + "," + almmFamilyname.standard( familyname ).toLowerCase() + "\n" );
                        }
                        else if( standard_code.equals( SC_N ) )
                        {
                            addToReportPerson( id_person, source, 1005, familyname );  // EC 1005
                        }
                        else if( standard_code.equals( SC_X ) )
                        {
                            addToReportPerson( id_person, source, 1009, familyname );  // EC 1009

                            writerFamilyname.write( id_person + "," + familyname.toLowerCase() + "\n" );
                        }
                        else {
                            addToReportPerson( id_person, source, 1010, familyname );  // EC 1010
                        }
                    }
                    else        // Familyname does not exists in ref_familyname
                    {
                        addToReportPerson( id_person, source, 1002, familyname );  // EC 1002

                        String nameNoSerriedSpaces = familyname.replaceAll( " [ ]+", " " );

                        // Family name contains two or more serried spaces?
                        if( !nameNoSerriedSpaces.equalsIgnoreCase( familyname ) ) {
                            addToReportPerson(id_person, source, 1003, familyname);  // EC 1003
                        }

                        String nameNoInvalidChars = cleanName( nameNoSerriedSpaces );

                        // Family name contains invalid chars ?
                        if( !nameNoSerriedSpaces.equalsIgnoreCase( nameNoInvalidChars ) ) {
                            addToReportPerson( id_person, source, 1004, familyname );   // EC 1004
                        }

                        // check if name has prepieces
                        String nameNoPrePiece = namePrepiece( nameNoInvalidChars, id_person );

                        // Family name contains invalid chars ?
                        if( !nameNoPrePiece.equalsIgnoreCase( nameNoInvalidChars ) ) {
                            addToReportPerson( id_person, source, 1008, familyname );  // EC 1008
                        }

                        // Check on Aliasses
                        String nameNoAlias = standardAlias( id_person, source, nameNoPrePiece );

                        // Check on suffix
                        ArrayListNonCase sfxO = ttalSuffix.getArray( "original" );

                        for( int i = 0; i < sfxO.size(); i++ )
                        {
                            if( nameNoAlias.endsWith( " " + sfxO.get( i ).toString() ) ) {
                                addToReportPerson( id_person, source, 1006, nameNoAlias );      // EC 1006

                                nameNoAlias = nameNoAlias.replaceAll( " " + sfxO.get( i ).toString(), "" );

                                // Set alias
                                PersonC.updateQuery( "suffix", sfxO.get( i ).toString(), id_person );
                            }
                        }

                        // Clean name one more time
                        String nameNoSuffix = LinksSpecific.funcCleanSides( nameNoAlias );

                        // Check name in original
                        //if( ttalFamilyname.originalExists( nameNoSuffix ) )
                        if( almmFamilyname.contains( nameNoSuffix ) )
                        {
                            // get standard_code
                            //String standard_code = ttalFamilyname.getStandardCodeByOriginal( nameNoSuffix );
                            String standard_code = almmFamilyname.code(nameNoSuffix);

                            // Check the standard code
                            if( standard_code.equals( SC_Y ) )
                            {
                                //writerFamilyname.write( id_person + "," + ttalFamilyname.getStandardByOriginal( nameNoSuffix ).toLowerCase() + "\n" );
                                writerFamilyname.write( id_person + "," + almmFamilyname.standard( nameNoSuffix ).toLowerCase() + "\n" );
                            }
                            else if( standard_code.equals( SC_U ) ) {
                                addToReportPerson( id_person, source, 1000, nameNoSuffix );    // EC 1000

                                //writerFamilyname.write( id_person + "," + ttalFamilyname.getStandardByOriginal( nameNoSuffix ).toLowerCase() + "\n" );
                                writerFamilyname.write( id_person + "," + almmFamilyname.standard( nameNoSuffix ).toLowerCase() + "\n" );
                            }
                            else if( standard_code.equals( SC_N ) ) {
                                addToReportPerson( id_person, source, 1005, nameNoSuffix );     // EC 1005
                            }
                            else if( standard_code.equals( SC_X ) ) {
                                addToReportPerson( id_person, source, 1009, nameNoSuffix );    // EC 1009

                                writerFamilyname.write( id_person + "," + nameNoSuffix.toLowerCase() + "\n" );
                            }
                            else {
                                addToReportPerson( id_person, source, 1010, nameNoSuffix );    // EC 1010
                            }
                        } else {
                            // add new familyname
                            //ttalFamilyname.addOriginal( nameNoSuffix );
                            almmFamilyname.add( nameNoSuffix );

                            addToReportPerson( id_person, source, 1009, nameNoSuffix );    // EC 1009

                            writerFamilyname.write( id_person + "," + nameNoSuffix.trim().toLowerCase() + "\n" );

                        }
                    }
                }
                else {  // Familyname empty
                    count_empty++;
                    addToReportPerson( id_person, source, 1001, "" );  // EC 1001
                }
            }
            con.close();
            rsFamilyname.close();

            int count_new = almmFamilyname.newcount();
            String strNew = "";
            if( count_new == 0 ) { strNew = "no new familynames"; }
            else if( count_new == 1 ) { strNew = "1 new familyname"; }
            else { strNew = "" + count_new + " new familynames"; }
            showMessage( count + " familyname records, " + count_empty + " without a familyname, " + strNew, false, true );
        }
        catch( Exception ex) {
            showMessage( count + " Exception while cleaning familyname: " + ex.getMessage(), false, true );
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

            ResultSet rs = conCleaned.runQueryWithResult( startQuery );

            while( rs.next() )
            {
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

                DateYearMonthDaySet dymd = LinksSpecific.divideCheckDate( date );

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
            showMessage(counter + " Exception while cleaning " + type + " flagged date: " + e.getMessage(), false, true);
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
        boolean debug = false;

        int count = 0;
        int count_empty = 0;
        int step = 1000;
        int stepstate = step;

        try
        {
            while( rs.next() )
            {
                count++;
                if( count == stepstate ) {
                    showMessage( count + "", true, true );
                    stepstate += step;
                }

                int id = rs.getInt( idFieldO );
                String location = rs.getString( locationFieldO );

                if( location != null && !location.isEmpty() )
                {
                    location = location.toLowerCase();
                    if( debug ) { System.out.println( "" + count + ": " + location ); }
                    //if( ttalLocation.originalExists( location ) )
                    if( almmLocation.contains( location ) )

                    {
                        //String refSCode = ttalLocation.getStandardCodeByOriginal( location );
                        String refSCode = almmLocation.code(location);
                        if( debug ) { System.out.println( "refSCode: " + refSCode );  }

                        if( refSCode.equals( SC_X ) )             // EC 91
                        {
                            if( tt == TableType.REGISTRATION )
                            {
                                addToReportRegistration( id, id_source, 91, location );
                                String query = RegistrationC.updateIntQuery( locationFieldC, "10010", id );
                                conCleaned.runQuery( query );
                            }
                            else
                            {
                                addToReportPerson( id, id_source, 91, location );
                                String query = PersonC.updateIntQuery( locationFieldC, "10010", id );
                                conCleaned.runQuery( query );
                            }
                        }
                        else if( refSCode.equals( SC_N ) )       // EC 93
                        {
                            if( tt == TableType.REGISTRATION ) {
                                addToReportRegistration( id, id_source, 93, location );
                            }
                            else
                            {
                                addToReportPerson( id, id_source, 93, location );
                            }
                        }
                        else if( refSCode.equals( SC_U ) )       // EC 95
                        {
                            if( tt == TableType.REGISTRATION )
                            {
                                addToReportRegistration( id, id_source, 95, location );

                                //String locationnumber = ttalLocation.getColumnByOriginal( "location_no", location );
                                String locationnumber = almmLocation.locationno( location );

                                String query = RegistrationC.updateIntQuery( locationFieldC, locationnumber, id );
                                conCleaned.runQuery( query );
                            }
                            else
                            {
                                addToReportPerson( id, id_source, 95, location );

                                //String locationnumber = ttalLocation.getColumnByOriginal( "location_no", location );
                                String locationnumber = almmLocation.locationno( location );

                                String query = PersonC.updateIntQuery( locationFieldC, locationnumber, id );
                                conCleaned.runQuery( query );
                            }
                        }
                        else if( refSCode.equals( SC_Y ) )
                        {
                            if( tt == TableType.REGISTRATION )
                            {
                                //String locationnumber = ttalLocation.getColumnByOriginal( "location_no", location );
                                String locationnumber = almmLocation.locationno( location );

                                String query = RegistrationC.updateIntQuery( locationFieldC, locationnumber, id );
                                conCleaned.runQuery( query );
                            }
                            else
                            {
                                //String locationnumber = ttalLocation.getColumnByOriginal( "location_no", location );
                                String locationnumber = almmLocation.locationno( location );

                                String query = PersonC.updateIntQuery( locationFieldC, locationnumber, id );
                                conCleaned.runQuery( query );
                            }
                        }
                        else
                        {
                            // EC 99
                            if( tt == TableType.REGISTRATION ) {
                                addToReportRegistration( id, id_source, 99, location );
                            } else {
                                addToReportPerson( id, id_source, 99, location );
                            }
                        }
                    }
                    else     // EC 91
                    {
                        if( tt == TableType.REGISTRATION )
                        {
                            addToReportRegistration( id, id_source, 91, location );
                            String query = RegistrationC.updateIntQuery( locationFieldC, "10010", id );
                            conCleaned.runQuery( query );
                        }
                        else
                        {
                            addToReportPerson( id, id_source, 91, location );
                            String query = PersonC.updateIntQuery( locationFieldC, "10010", id );
                            conCleaned.runQuery( query );
                        }

                        //ttalLocation.addOriginal( location );
                        almmLocation.add( location );
                    }
                }
                else
                { count_empty++; }
            }

            int count_new = almmLocation.newcount();
            String strNew = "";
            if( count_new == 0 ) { strNew = "no new locations"; }
            else if( count_new == 1 ) { strNew = "1 new location"; }
            else { strNew = "" + count_new + " new locations"; }
            showMessage( count + " location records, " + count_empty + " without location, " + strNew, false, true );
        }
        catch( Exception ex ) {
              throw new Exception( count + " Exception while cleaning Location: " + ex.getMessage() );
        }
    } // standardLocation


    /**
     * @param source
     */
    public void standardMarLocation( String source )
    {
        String selectQuery = "SELECT id_person , mar_location FROM person_o WHERE id_source = " + source + " AND mar_location <> ''";

        try {
            ResultSet rs = conOriginal.runQueryWithResult( selectQuery );
            standardLocation( rs, "id_person", "mar_location", "mar_location", source, TableType.PERSON );
        } catch( Exception ex ) {
            showMessage( ex.getMessage(), false, true );
        }
    } // standardMarLocation


    /**
     * @param debug
     * @param source
     */
    public void standardOccupation( boolean debug, String source )
    {
        int count = 0;
        int count_empty = 0;
        int step = 1000;
        int stepstate = step;

        String query = "SELECT id_person , occupation FROM person_o WHERE id_source = " + source;
        if( debug ) { showMessage( query, false, true ); }

        try
        {
            ResultSet rs = conOriginal.runQueryWithResult( query );           // Get occupation

            while( rs.next() )
            {
                count++;
                if( count == stepstate ) {
                    showMessage( count + "", true, true );
                    stepstate += step;
                }

                int id_person = rs.getInt( "id_person" );
                String occupation = rs.getString( "occupation") != null ? rs.getString( "occupation" ).toLowerCase() : "";
                if( occupation.isEmpty() ) {
                    count_empty += 1;
                }
                else {
                    if( debug ) { showMessage( "id_person: " + id_person + ", occupation: " + occupation, false, true ); }
                    System.out.println( "" + count + " " + occupation );
                    standardOccupationRecord( debug, source, id_person, occupation );
                }
            }
            //showMessage( counter + " persons, " + empty + " without occupation", false, true );
            int count_new = almmOccupation.newcount();
            String strNew = "";
            if( count_new == 0 ) { strNew = "no new occupations"; }
            else if( count_new == 1 ) { strNew = "1 new occupation"; }
            else { strNew = "" + count_new + " new occupations"; }
            showMessage( count + " occupation records, " + count_empty + " without occupation, " + strNew, false, true );
        }
        catch( SQLException sex )
        { showMessage( "\ncounter: " + count + " SQLException while cleaning Occupation: " + sex.getMessage(), false, true ); }
        catch( Exception jex )
        { showMessage( "\ncounter: " + count + " Exception while cleaning Occupation: " + jex.getMessage(), false, true ); }

    } // standardOccupation


    /**
     * @param debug
     * @param id_person
     * @param occupation
     */
    public void standardOccupationRecord( boolean debug, String sourceNo, int id_person, String occupation )
    {
        try
        {
            if( !occupation.isEmpty() )                 // check presence of the occupation
            {
                boolean exists = false;
                //exists = ttalOccupation.originalExists( occupation );
                exists = almmOccupation.contains( occupation );
                if( exists )
                {
                    //showMessage( "old: " + occupation, false, true );
                    if( debug ) { showMessage("getStandardCodeByOriginal: " + occupation, false, true); }

                    //String refSCode = ttalOccupation.getStandardCodeByOriginal( occupation );
                    String refSCode = almmOccupation.code(occupation);

                    if( debug ) { showMessage( "refSCode: " + refSCode, false, true ); }

                    if( refSCode.equals( SC_X ) )
                    {
                        if( debug ) { showMessage( "Warning 41 (via SC_X): id_person: " + id_person + ", occupation: " + occupation, false, true ); }
                        addToReportPerson( id_person, sourceNo, 41, occupation );      // warning 41

                        String query = PersonC.updateQuery( "occupation", occupation, id_person );
                        conCleaned.runQuery( query );
                    }
                    else if( refSCode.equals( SC_N ) )
                    {
                        if( debug ) { showMessage( "Warning 43: id_person: " + id_person + ", occupation: " + occupation, false, true ); }
                        addToReportPerson( id_person, sourceNo, 43, occupation );      // warning 43
                    }
                    else if( refSCode.equals(SC_U) )
                    {
                        if( debug ) { showMessage( "Warning 45: id_person: " + id_person + ", occupation: " + occupation, false, true ); }
                        addToReportPerson( id_person, sourceNo, 45, occupation );      // warning 45

                        //String refOccupation = ttalOccupation.getColumnByOriginal( "standard", occupation );
                        String refOccupation = almmOccupation.standard( occupation );

                        String query = PersonC.updateQuery("occupation", refOccupation, id_person);
                        conCleaned.runQuery( query );
                    }
                    else if( refSCode.equals( SC_Y ) )
                    {
                        //String refOccupation = ttalOccupation.getColumnByOriginal( "standard", occupation );
                        String refOccupation = almmOccupation.standard(occupation);

                        if( debug ) { showMessage( "occupation: " + refOccupation, false, true ); }

                        String query = PersonC.updateQuery( "occupation", refOccupation, id_person );
                        conCleaned.runQuery( query );
                    }
                    else      // Invalid standard code
                    {
                        if( debug ) { showMessage( "Warning 49: id_person: " + id_person + ", occupation: " + occupation, false, true ); }
                        addToReportPerson( id_person, sourceNo, 49, occupation );      // warning 49
                    }
                }
                else        // not present, collect as new
                {
                    //showMessage( "new: " + occupation, false, true );
                    if( debug ) { showMessage( "Warning 41 (not in ref_): id_person: " + id_person + ", occupation: " + occupation, false, true ); }
                    addToReportPerson( id_person, sourceNo, 41, occupation );       // warning 41

                    //ttalOccupation.addOriginal( occupation );
                    almmOccupation.add( occupation );

                    String query = PersonC.updateQuery( "occupation", occupation, id_person );
                    conCleaned.runQuery( query );
                }
            }
        }
        catch( Exception ex3 )
        { showMessage( "Exception while cleaning Occupation: " + ex3.getMessage(), false, true); }
    } // standardOccupationRecord


    /**
     * @param source
     * @throws Exception
     */
    public void standardPrepiece( String source )
    {
        int count = 0;
        int step = 1000;
        int stepstate = step;

        try {

            String selectQuery = "SELECT id_person , prefix FROM person_o WHERE id_source = " + source + " AND prefix <> ''";

            // WHY IS A LOCAL CONNECTION USED?
            Connection con = getConnection( "links_original" );
            con.isReadOnly();

            // Read family names from table
            ResultSet rsPrepiece = con.createStatement().executeQuery( selectQuery );
            con.createStatement().close();

            // get total
            rsPrepiece.last();
            int total = rsPrepiece.getRow();
            rsPrepiece.beforeFirst();

            while (rsPrepiece.next()) {

                // Create lists
                String listPF = "";
                String listTO = "";
                String listTN = "";

                count++;

                if( count == stepstate ) {
                    showMessage( count + " of " + total, true, true );
                    stepstate += step;
                }

                int id_person = rsPrepiece.getInt( "id_person" );
                String prepiece = rsPrepiece.getString( "prefix" ).toLowerCase();

                prepiece = cleanName( prepiece );

                String[] prefixes = prepiece.split( " " );

                for( String part : prefixes )
                {
                    // Does Prefix exist in ref table
                    if( ttalPrepiece.originalExists( part ) )
                    {
                        String standard_code = ttalPrepiece.getStandardCodeByOriginal( part );
                        String prefix        = ttalPrepiece.getColumnByOriginal( "prefix", part );
                        String title_noble   = ttalPrepiece.getColumnByOriginal( "title_noble", part );
                        String title_other   = ttalPrepiece.getColumnByOriginal( "title_other", part );

                        // standard code x
                        if( standard_code.equals( SC_X ) )
                        {
                            addToReportPerson(id_person, source, 81, part);       // EC 81

                            listPF += part + " ";
                        }
                        else if( standard_code.equals( SC_N ) )
                        {
                            addToReportPerson( id_person, source, 83, part );     // EC 83
                        }
                        else if( standard_code.equals( SC_U ) )
                        {
                            addToReportPerson( id_person, source, 85, part );     // EC 85

                            if( prefix != null && !prefix.isEmpty() ) {
                                listPF += prefix + " ";
                            }
                            else if( title_noble != null && !title_noble.isEmpty() ) {
                                listTN += title_noble + " ";
                            }
                            else if( title_other != null && !title_other.isEmpty() ) {
                                listTO += title_other + " ";
                            }
                        }
                        else if( standard_code.equals( SC_Y ) )
                        {
                            if( prefix != null && !prefix.isEmpty() ) {
                                listPF += prefix + " ";
                            }
                            else if( title_noble != null && !title_noble.isEmpty() ) {
                                listTN += title_noble + " ";
                            }
                            else if( title_other != null && !title_other.isEmpty() ) {
                                listTO += title_other + " ";
                            }
                        }
                        else {  // Standard_code invalid
                            addToReportPerson(id_person, source, 89, part);       // EC 89
                        }
                    }
                    else    // Prefix not in ref
                    {
                        addToReportPerson(id_person, source, 81, part);           // EC 81

                        ttalPrepiece.addOriginal(part);     // Add Prefix

                        listPF += part + " ";               // Add to list
                    }
                }

                // write lists to person_c
                if( !listTN.isEmpty() ) {
                    conCleaned.runQuery( PersonC.updateQuery( "title_noble", listTN.substring( 0, ( listTN.length() - 1 ) ), id_person ) );
                }

                if( !listTO.isEmpty() ) {
                    conCleaned.runQuery( PersonC.updateQuery( "title_other", listTO.substring( 0, ( listTO.length() - 1 ) ), id_person ) );
                }

                if( !listPF.isEmpty() ) {
                    conCleaned.runQuery( PersonC.updateQuery( "prefix", listPF.substring( 0, ( listPF.length() - 1 ) ), id_person) ) ;
                }
            }

            rsPrepiece.close();
            con.close();
        }
        catch( Exception ex ) {
            showMessage(count + " Exception while cleaning Prepiece: " + ex.getMessage(), false, true );
        }
    } // standardPrepiece


    /**
     * @param source
     */
    public void standardRegistrationDate( String source )
    {
        int counter = 0;
        int step = 1000;
        int stepstate = step;

        try
        {
            String startQuery = "SELECT id_registration , registration_date FROM registration_o WHERE id_source = " + source;

            ResultSet rs = conOriginal.runQueryWithResult( startQuery );

            while( rs.next() )
            {
                counter++;
                if( counter == stepstate ) {
                    showMessage( counter + "", true, true );
                    stepstate += step;
                }

                // Get Opmerking
                int id_registration = rs.getInt( "id_registration" );
                String registration_date = rs.getString( "registration_date" );

                if( registration_date == null ) {
                    addToReportRegistration( id_registration, source, 202, "" );   // EC 202

                    continue;
                }

                DateYearMonthDaySet dymd = LinksSpecific.divideCheckDate( registration_date );

                if( dymd.isValidDate() )
                {
                    String query = "UPDATE registration_c"
                        + " SET registration_c.registration_date = '" + registration_date + "' , "
                        + "registration_c.registration_day = "        + dymd.getDay() + " , "
                        + "registration_c.registration_month = "      + dymd.getMonth() + " , "
                        + "registration_c.registration_year = "       + dymd.getYear()
                        + " WHERE registration_c.id_registration = "  + id_registration;

                    conCleaned.runQuery( query );
                }
                else         // Error occured
                {
                    addToReportRegistration( id_registration, source, 201, dymd.getReports() );    // EC 201

                    String query = "UPDATE registration_c"
                        + " SET registration_c.registration_date = '" + registration_date + "' , "
                        + "registration_c.registration_day = "        + dymd.getDay() + " , "
                        + "registration_c.registration_month = "      + dymd.getMonth() + " , "
                        + "registration_c.registration_year = "       + dymd.getYear()
                        + " WHERE registration_c.id_registration = "  + id_registration;

                    conCleaned.runQuery( query );
                }
            }
            rs = null;
        } catch( Exception ex ) {
            showMessage( counter + " Exception while cleaning Registration date: " + ex.getMessage(), false, true );
        }
    } // standardRegistrationDate


    /**
     * @param source
     */
    public void standardRegistrationLocation( String source )
    {
        String selectQuery = "SELECT id_registration , registration_location FROM registration_o WHERE id_source = " + source;

        try {
            ResultSet rs = conOriginal.runQueryWithResult( selectQuery );

            standardLocation( rs, "id_registration", "registration_location", "registration_location_no", source, TableType.REGISTRATION );
        } catch( Exception ex ) {
            showMessage( ex.getMessage(), false, true );
        }
    } // standardRegistrationLocation


    private void standardRole( String source )
    {
        /*
        String query = "UPDATE links_original.person_o, links_cleaned.person_c, links_general.ref_role "
            + "SET links_cleaned.person_c.role = links_general.ref_role.role_nr "
            + "WHERE links_original.person_o.role = links_general.ref_role.original "
            + "AND links_original.person_o.id_person = links_cleaned.person_c.id_person "
            + "AND links_original.person_o.id_source = " + source;

        try {
            conCleaned.runQuery( query );
        } catch( Exception ex ) {
            showMessage( "Exception while running standardRole: " + ex.getMessage(), false, true );
        }
        */

        boolean debug = false;
        int counter = 0;
        int count_empty = 0;
        int count_noref = 0;
        int step = 1000;
        int stepstate = step;

        try
        {
            String selectQuery = "SELECT id_person , role FROM links_original.person_o WHERE id_source = " + source;

            ResultSet rs = conOriginal.runQueryWithResult( selectQuery );

            while( rs.next() )
            {
                counter++;
                if( counter == stepstate ) {
                    showMessage( counter + "", true, true );
                    stepstate += step;
                }

                int id_person = rs.getInt( "id_person" );
                String role = rs.getString( "role" ) != null ? rs.getString( "role" ).toLowerCase() : "";
                //if( debug ) { System.out.println( "role: " + role  ); }

                if( role.isEmpty() ) { count_empty++; }
                else
                {
                    if( ttalRole.originalExists( role ) )       // present in ref_role.original
                    {
                        String refSCode = ttalRole.getStandardCodeByOriginal( role );
                        //if( debug ) { System.out.println( "refSCode: " + refSCode ); }

                        if( refSCode.equals( SC_Y ) ) {
                            if( debug ) { showMessage("Standard Role: id_person: " + id_person + ", role: " + role, false, true); }

                            String updateQuery = PersonC.updateQuery( "role", ttalRole.getColumnByOriginal( "role_nr", role ), id_person );
                            conCleaned.runQuery( updateQuery );
                        }
                        else
                        {
                            if( debug ) { showMessage( "Warning 101: id_person: " + id_person + ", role: " + role, false, true ); }

                            addToReportPerson( id_person, source, 101, role );       // report warning 101
                            ttalRole.addOriginal( role );                               // add new role
                        }
                    }
                    else
                    {
                        count_noref++;
                        if( debug ) { showMessage( "Warning 101: id_person: " + id_person + ", role: " + role, false, true ); }

                        addToReportPerson( id_person, source, 101, role );       // report warning 101
                        ttalRole.addOriginal( role );                               // add new role
                    }
                }
            }
            showMessage( counter + " person records, " + count_empty + " without a role, and " + count_noref + " without a standard role", false, true );
        } catch( Exception ex ) {
            showMessage( counter + " Exception while cleaning Role: " + ex.getMessage(), false, true );
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

            String selectQuery = ""
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

            ResultSet rs = conCleaned.runQueryWithResult( selectQuery );

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
                addToReportRegistration( previousId, id_source, 111, "" );
            } else { // Present
                // Is is numeric
                try {

                    previousNo = Integer.parseInt(rs.getString("registration_seq"));

                } catch (Exception e) {
                    // EC 112
                    addToReportRegistration( previousId, id_source, 112, rs.getString( "registration_seq" ) );
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
                    addToReportRegistration( rs.getInt("id_registration"), id_source, 111, "" );
                    continue;
                }
                // Is is numeric ?
                try {
                    nummer = Integer.parseInt(rs.getString("registration_seq"));
                } catch (Exception e) {
                    // EC 112
                    addToReportRegistration( rs.getInt("id_registration"), id_source, 112, rs.getString( "registration_seq" ) );

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
                    addToReportRegistration( rs.getInt("id_registration"), id_source, 113, rs.getString("registration_seq") );
                } else if (verschil > 1 && (previousYr == rs.getInt("registration_year")) && (previousMt == rs.getInt("registration_maintype")) && (previousLc == rs.getInt("registration_location_no"))) {
                    // EC 114
                    addToReportRegistration( rs.getInt("id_registration"), id_source, 114, rs.getString("registration_seq") );
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

            showMessage(counter + " Exception while checking sequence: " + e.getMessage(), false, true);
        }
    } // standardSequence


    /**
     * @param source
     */
    public void standardSex( String source )
    {
        boolean debug = false;
        int count = 0;
        int count_empty = 0;
        int step = 1000;
        int stepstate = step;

        try
        {
            String selectQuery = "SELECT id_person , sex FROM person_o WHERE id_source = " + source;

            ResultSet rs = conOriginal.runQueryWithResult( selectQuery );

            while( rs.next() )
            {
                count++;
                if( count == stepstate ) {
                    showMessage( count + "", true, true );
                    stepstate += step;
                }

                int id_person = rs.getInt( "id_person" );
                //String sex = rs.getString( "sex" );
                String sex = rs.getString( "sex" ) != null ? rs.getString( "sex" ).toLowerCase() : "";
                if( debug ) { showMessage( "sex: " + sex , false, true ); }

                if( sex != null && !sex.isEmpty() )                 // check presence of the gender
                {
                    //if( ttalStatusSex.originalExists( sex ) )     // check presence in original
                    if( almmSex.contains( sex ) )                   // check presence in original

                    {
                        //String refSCode = ttalStatusSex.getStandardCodeByOriginal( sex );
                        String refSCode = almmSex.code(sex);
                        if( debug ) { showMessage( "refSCode: " + refSCode , false, true ); }

                        if( refSCode.equals( SC_X ) ) {
                            if( debug ) { showMessage( "Warning 31: id_person: " + id_person + ", sex: " + sex, false, true ); }

                            addToReportPerson( id_person, source, 31, sex );     // warning 31

                            String query = PersonC.updateQuery( "sex", sex, id_person );
                            conCleaned.runQuery( query );
                        }
                        else if( refSCode.equals( SC_N ) ) {
                            if( debug ) { showMessage( "Warning 33: id_person: " + id_person + ", sex: " + sex, false, true ); }

                            addToReportPerson( id_person, source, 33, sex );     // warning 33
                        }
                        else if( refSCode.equals( SC_U ) ) {
                            if( debug ) { showMessage( "Warning 35: id_person: " + id_person + ", sex: " + sex, false, true ); }

                            addToReportPerson( id_person, source, 35, sex );     // warning 35

                            //String query = PersonC.updateQuery( "sex", ttalStatusSex.getColumnByOriginal( "standard_sex", sex ), id_person );
                            String query = PersonC.updateQuery( "sex", almmSex.standard(sex), id_person );
                            conCleaned.runQuery( query );
                        }
                        else if( refSCode.equals( SC_Y ) ) {
                            if( debug ) { showMessage( "Standard sex: id_person: " + id_person + ", sex: " + sex, false, true ); }

                            //String query = PersonC.updateQuery( "sex", ttalStatusSex.getColumnByOriginal( "standard_sex", sex ), id_person );
                            String query = PersonC.updateQuery( "sex", almmSex.standard(sex), id_person );
                            conCleaned.runQuery( query );
                        }
                        else {     // Invalid standard code
                            if( debug ) { showMessage( "Warning 39: id_person: " + id_person + ", sex: " + sex, false, true ); }

                            addToReportPerson( id_person, source, 39, sex );     // warning 39
                        }
                    }
                    else // not present in original
                    {
                        if( debug ) { showMessage( "standardSex: not present in original", false, true ); }
                        if( debug ) { showMessage( "Warning 31: id_person: " + id_person + ", sex: " + sex, false, true ); }

                        addToReportPerson( id_person, source, 31, sex );         // warning 31
                        //ttalStatusSex.addOriginal( sex );                         // warning 31
                        almmCivilstatus.add( sex );         // only almmCivilstatus is used for update

                        String query = PersonC.updateQuery( "sex", sex, id_person );
                        conCleaned.runQuery( query );
                    }
                }
                else { count_empty++; }
            }

            int count_new = almmCivilstatus.newcount();     // only almmCivilstatus is used for update
            String strNew = "";
            if( count_new == 0 ) { strNew = "no new genders"; }
            else if( count_new == 1 ) { strNew = "1 new gender"; }
            else { strNew = "" + count_new + " new genders"; }
            showMessage( count + " gender records, " + count_empty + " without gender, " + strNew, false, true );
        }
        catch( Exception ex ) {
            showMessage( count + " Exception while cleaning Sex: " + ex.getMessage(), false, true );
        }
    } // standardSex


    /**
     * @param source
     */
    public void standardCivilstatus( String source )
    {
        boolean debug = false;
        int count = 0;
        int count_empty = 0;
        int step = 1000;
        int stepstate = step;

        int count_sex_new = almmCivilstatus.newcount();     // standardSex also writes to almmCivilstatus

        try
        {
            String selectQuery = "SELECT id_person , sex , civil_status FROM person_o WHERE id_source = " + source;

            ResultSet rs = conOriginal.runQueryWithResult( selectQuery );

            while( rs.next() )
            {
                count++;
                if( count == stepstate ) {
                    showMessage( count + "", true, true );
                    stepstate += step;
                }

                int id_person = rs.getInt( "id_person" );
                String sex = rs.getString( "sex" ) != null ? rs.getString( "sex" ).toLowerCase() : "";
                String civil_status = rs.getString( "civil_status" ) != null ? rs.getString( "civil_status" ).toLowerCase() : "";
                //showMessage( "id: " + id_person + ", sex: " + sex + ", civil: " + civil_status, false, true );

                if( civil_status != null && !civil_status.isEmpty() )       // check presence of civil status
                {
                    //if( ttalStatusSex.originalExists( civil_status ) )    // check presence in original
                    if( almmCivilstatus.contains(civil_status) )          // check presence in original

                    {
                        //String refSCode = ttalStatusSex.getStandardCodeByOriginal( civil_status );
                        String refSCode = almmCivilstatus.code(civil_status);
                        //showMessage( "code: " + refSCode, false, true );

                        if( refSCode.equals( SC_X ) ) {
                            addToReportPerson( id_person, source, 61, civil_status );            // warning 61

                            String query = PersonC.updateQuery( "civil_status", civil_status, id_person );
                            conCleaned.runQuery( query );
                        }
                        else if( refSCode.equals( SC_N ) ) {
                            addToReportPerson( id_person, source, 63, civil_status );            // warning 63
                        }
                        else if( refSCode.equals( SC_U ) ) {
                            addToReportPerson( id_person, source, 65, civil_status );            // warning 65

                            //String query = PersonC.updateQuery( "civil_status", ttalStatusSex.getColumnByOriginal( "standard_civilstatus", civil_status ), id_person );
                            String query = PersonC.updateQuery( "civil_status", almmCivilstatus.standard( civil_status ), id_person );
                            conCleaned.runQuery( query );

                            if( sex != null && !sex.isEmpty() ) {           // Extra check on sex
                                //if( !sex.equalsIgnoreCase( this.ttalStatusSex.getColumnByOriginal( "standard_sex", civil_status ) ) ) {
                                if( !sex.equalsIgnoreCase( this.almmCivilstatus.value( "standard_sex", civil_status) ) ) {
                                    if( sex != "u" ) {
                                        addToReportPerson(id_person, source, 68, civil_status);    // warning 68
                                    }
                                }
                            }
                            else            // Sex is empty
                            {
                                //String sexQuery = PersonC.updateQuery( "sex", ttalStatusSex.getColumnByOriginal( "standard_sex", civil_status ), id_person );
                                String sexQuery = PersonC.updateQuery( "sex", almmCivilstatus.value("standard_sex", civil_status), id_person );
                                conCleaned.runQuery( sexQuery );
                            }

                            //String sexQuery = PersonC.updateQuery( "civil_status", ttalStatusSex.getColumnByOriginal( "standard_civilstatus", civil_status ), id_person );
                            String sexQuery = PersonC.updateQuery( "civil_status", almmCivilstatus.standard(civil_status), id_person );
                            conCleaned.runQuery( sexQuery );
                        }
                        else if( refSCode.equals( SC_Y ) ) {
                            //String query = PersonC.updateQuery( "civil_status", ttalStatusSex.getColumnByOriginal( "standard_civilstatus", civil_status ), id_person );
                            String query = PersonC.updateQuery( "civil_status", almmCivilstatus.standard(civil_status), id_person );
                            conCleaned.runQuery( query );

                            if( sex == null || sex.isEmpty() )  {      // Sex is empty
                                //String sexQuery = PersonC.updateQuery( "sex", ttalStatusSex.getColumnByOriginal( "standard_sex", civil_status ), id_person );
                                String sexQuery = PersonC.updateQuery( "sex", almmCivilstatus.value("standard_sex", civil_status), id_person );
                                conCleaned.runQuery( sexQuery );
                            }

                            //String sexQuery = PersonC.updateQuery( "civil_status", ttalStatusSex.getColumnByOriginal( "standard_civilstatus", civil_status ), id_person );
                            String sexQuery = PersonC.updateQuery( "civil_status", almmCivilstatus.standard(civil_status), id_person );
                            conCleaned.runQuery( sexQuery );
                        }
                        else {          // Invalid SC
                            addToReportPerson( id_person, source, 69, civil_status );            // warning 68
                        }
                    }
                    else {      // add to ref
                        if( debug ) { showMessage( "standardCivilstatus: not present in original", false, true ); }
                        if( debug ) { showMessage( "Warning 31: id_person: " + id_person + ", sex: " + sex, false, true ); }

                        addToReportPerson( id_person, source, 61, civil_status );                // warning 61

                        //ttalStatusSex.addOriginal( civil_status );                                  // Add new civil_status
                        almmCivilstatus.add(civil_status);                                        // Add new civil_status

                        String query = PersonC.updateQuery( "civil_status", civil_status, id_person );  // Write to Person
                        conCleaned.runQuery( query );
                    }
                }
                else { count_empty++; }
            }

            int count_civil_new = almmCivilstatus.newcount() - count_sex_new;
            String strNew = "";
            if( count_civil_new == 0 ) { strNew = "no new civil statusses"; }
            else if( count_civil_new == 1 ) { strNew = "1 new civil status"; }
            else { strNew = "" + count_civil_new + " new civil statusses"; }
            showMessage( count + " civil status records, " + count_empty + " without civil status, " + strNew, false, true );
        }
        catch( Exception ex ) {
            showMessage( count + " Exception while cleaning Civil Status: " + ex.getMessage(), false, true );
        }
    } // standardCivilstatus


    /**
     * @param source
     */
    public void standardSuffix( String source )
    {
        int count = 0;
        int step = 1000;
        int stepstate = step;

        try {
            String selectQuery = "SELECT id_person , suffix FROM person_o WHERE id_source = " + source + " AND suffix <> ''";

            // WHY IS A LOCAL CONNECTION USED?
            Connection con = getConnection( "links_original" );
            con.isReadOnly();

            // Read family names from table
            ResultSet rsSuffix = con.createStatement().executeQuery( selectQuery );
            con.createStatement().close();

            // get total
            rsSuffix.last();
            int total = rsSuffix.getRow();
            rsSuffix.beforeFirst();

            while( rsSuffix.next() )
            {
                count++;
                if( count == stepstate ) {
                    showMessage( count + " of " + total, true, true );
                    stepstate += step;
                }

                int id_person = rsSuffix.getInt( "id_person" );
                String suffix = rsSuffix.getString( "suffix" ).toLowerCase();

                suffix = cleanName( suffix );

                // Check occurrence in ref table
                if( ttalSuffix.originalExists( suffix ) )
                {
                    String standard_code = ttalSuffix.getStandardCodeByOriginal( suffix );

                    if( standard_code.equals( SC_X ) )
                    {
                        addToReportPerson(id_person, source, 71, suffix);     // EC 71

                        String query = PersonC.updateQuery( "suffix", suffix, id_person );
                        conCleaned.runQuery( query );
                    }
                    else if( standard_code.equals( SC_N ) )
                    {
                        addToReportPerson( id_person, source, 73, suffix );   // EC 73
                    }
                    else if( standard_code.equals( SC_U ) )
                    {
                        addToReportPerson( id_person, source, 75, suffix );   // EC 74

                        String query = PersonC.updateQuery( "suffix", suffix, id_person );
                        conCleaned.runQuery( query );
                    }
                    else if( standard_code.equals( SC_Y ) )
                    {
                        String query = PersonC.updateQuery( "suffix", suffix, id_person );
                        conCleaned.runQuery( query );
                    }
                    else {
                        addToReportPerson(id_person, source, 79, suffix);     // EC 75
                    }
                }
                else // Standard code x
                {
                    addToReportPerson( id_person, source, 71, suffix);        // EC 71

                    ttalSuffix.addOriginal(suffix);

                    String query = PersonC.updateQuery( "suffix", suffix, id_person );
                    conCleaned.runQuery( query );

                }
            }

            rsSuffix.close();
            con.close();

        }
        catch( Exception ex ) {
            showMessage( count + " Exception while cleaning Suffix: " + ex.getMessage(), false, true );
        }
    } // standardSuffix


    /**
     * @param source
     */
    public void standardType( String source )
    {
        boolean debug = false;
        int counter = 0;
        int step = 1000;
        int stepstate = step;

        try
        {
            String selectQuery = "SELECT id_registration, registration_maintype, registration_type FROM registration_o WHERE id_source = " + source;

            ResultSet rs = conOriginal.runQueryWithResult( selectQuery );

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
                    String refSCode = ref.getString( "standard_code" ).toLowerCase();

                    if( refSCode.equals( SC_X ) ) {
                        if( debug ) { showMessage( "Warning 51: id_registration: " + id_registration + ", reg type: " + registration_type, false, true ); }

                        addToReportRegistration( id_registration, source, 51, registration_type );       // warning 51

                        String query = RegistrationC.updateQuery( "registration_type", registration_type, id_registration );
                        conCleaned.runQuery( query );
                    }
                    else if( refSCode.equals( SC_N ) ) {
                        if( debug ) { showMessage( "Warning 53: id_registration: " + id_registration + ", reg type: " + registration_type, false, true ); }

                        addToReportRegistration( id_registration, source, 53, registration_type );       // warning 53
                    }
                    else if( refSCode.equals( SC_U ) ) {
                        if( debug ) { showMessage( "Warning 55: id_registration: " + id_registration + ", reg type: " + registration_type, false, true ); }

                        addToReportRegistration( id_registration, source, 55, registration_type );       // warning 55

                        String query = RegistrationC.updateQuery( "registration_type", ref.getString( "standard" ).toLowerCase(), id_registration );
                        conCleaned.runQuery( query );
                    } else if( refSCode.equals( SC_Y ) ) {
                        if( debug ) { showMessage( "Standard reg type: id_person: " + id_registration + ", reg type: " + registration_type, false, true ); }

                        String query = RegistrationC.updateQuery( "registration_type", ref.getString( "standard" ).toLowerCase(), id_registration );
                        conCleaned.runQuery( query );
                    }
                    else {    // invalid SC
                        if( debug ) { showMessage( "Warning 59: id_registration: " + id_registration + ", reg type: " + registration_type, false, true ); }

                        addToReportRegistration( id_registration, source, 59, registration_type );       // warning 59
                    }
                }
                else {      // not in reference; add to reference with "x"
                    if( debug ) { showMessage( "Warning 51: id_registration: " + id_registration + ", reg type: " + registration_type, false, true ); }

                    addToReportRegistration( id_registration, source, 51, registration_type );           // warning 51

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
     * @param source
     */
    public void standardAge( String source )
    {
        boolean debug = false;
        int counter = 0;
        int step = 1000;
        int stepstate = step;

        try {

            String selectQuery = "SELECT id_person , age_year , age_month , age_week , age_day FROM links_original.person_o WHERE id_source = " + source;
            if( debug ) { showMessage( selectQuery, false, true ); }

            ResultSet rs = conOriginal.runQueryWithResult( selectQuery );

            while( rs.next() )
            {
                counter++;
                if( counter == stepstate ) {
                    showMessage( counter + "", true, true );
                    stepstate += step;
                }

                int id_person = rs.getInt( "id_person" );
                int age_year  = rs.getInt( "age_year" );
                int age_month = rs.getInt( "age_month" );
                int age_week  = rs.getInt( "age_week" );
                int age_day   = rs.getInt( "age_day" );

                //System.out.printf("%d: %d %d %d %d %d\n", counter, id_person, age_year, age_month, age_week, age_day);

                boolean update = false;

                if( ( age_year >= 0 ) && ( age_year < 115 ) ) { update = true; }
                else { addToReportPerson( id_person, source, 244, age_year + "" ); }

                if( ( age_month >= 0 ) && ( age_month < 50 ) ) { update = true; }
                else { addToReportPerson( id_person, source, 243, age_month + "" ); }

                if( ( age_week >= 0 ) && ( age_week < 50 ) ) { update = true; }
                else { addToReportPerson( id_person, source, 242, age_week + "" ); }

                if( ( age_day >= 0 ) && ( age_day < 100 ) ) { update = true; }
                else { addToReportPerson( id_person, source, 241, age_day + "" ); }

                if( update )
                {
                    String updateQuery = "UPDATE links_cleaned.person_c"
                        + " SET"
                        + " age_year = "  + age_year  + " ,"
                        + " age_month = " + age_month + " ,"
                        + " age_week = "  + age_week  + " ,"
                        + " age_day = "   + age_day
                        + " WHERE id_person = " + id_person
                        + " AND id_source = " + source;

                    if( debug ) { showMessage( updateQuery, false, true ); }
                    conCleaned.runQuery( updateQuery );
                    //System.out.println( "after update of: " + counter );
                }
            }
            showMessage( counter + " person records", false, true );
        }
        catch( Exception ex ) {
            showMessage( counter + " Exception while cleaning Age: " + ex.getMessage(), false, true );
        }
    } // standardAge

     /*---< end Standardizing functions >------------------------------------*/


    /**
     * Read distinct source ids from links_original.registration_o
     * @return
     */
    private int[] getOrigSourceIds()
    {
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
     * Read distinct source ids from links_original.registration_o
     * @return
     */
    private int[] createSourceList( int sourceIdGui, int[] sourceListAvail )
    {
        int[] idsInt;

        if( sourceIdGui == 0 ) { idsInt = sourceListAvail; }
        else {
            idsInt = new int[ 1 ];
            idsInt[ 0 ] = sourceIdGui;
        }

        return idsInt;
    } // createSourceList


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
    /*
    private void runMethod( String MethodName ) throws Exception
    {
        Class[] partypes = new Class[1];
        Object[] argList = new Object[1];

        partypes[0] = String.class;

        for( int i : sourceList ) {
            showMessage( "Running " + MethodName + " for source: " + i + "...", false, true );

            argList[ 0 ] = i + "";
            Method m = this.getClass().getMethod( MethodName, partypes );

            // Call method
            m.invoke( this, argList );
        }

    } // runMethod
    */

    /**************************************************************************/

    /**
     * calls functieOpmerkingenBeroep(),
     *      calls functieVeldBeroep,
     *          uses ttalOccupation
     *
     * @param rs
     * @param rsScanStrings
     * @return
     * @throws Exception
     */
    /*
    private HashMap functieParseRemarks(ResultSet rs, ResultSet rsScanStrings) throws Exception {

        // Hashmap voor de overgebleven
        HashMap cache = new HashMap();

        // Counter
        teller = 0;

        // Stappen instellen
        int step = 10000;

        // Door de opmerkingen heen lopen
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

            // Controleren of de opmerking al in de cache voorkomt
            if (cache.containsKey(registratie_hoofdtype + ":" + opmerking)) {
                Object o = cache.get(registratie_hoofdtype + ":" + opmerking).toString();
                int oToInt = Integer.parseInt(o.toString());
                int newValue = oToInt + 1;
                cache.put(registratie_hoofdtype + ":" + opmerking, newValue);
            } else {
                // TODO: Skip werkt niet helemaal goed
                int skipGroup = 0;

                // Gebruiker op de hoogte stellen
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

                // Opmerking strippen aan de hand van de table
                // We lopen hier door alle regexen uit de table
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

                    // Reguliere expressie gaat gebruikt worden
                    Pattern regex = Pattern.compile(scan_string);

                    // Voer expressie uit op de opmerking
                    Matcher m = regex.matcher(opmerking);

                    // Controleer of er iets gevonden is
                    if (m.find()) {

                        // Controleer of deze groep overgeslagen moet worden
                        skipGroup = groepnummer;

                        // Opmerking strippen
                        opmerking = opmerking.replaceAll(scan_string, "");

                        // Destilleer het benodigde stukje op uit de opmerking
                        String currentPart = m.group();

                        HashMap insertValues = new HashMap();

                        // We controleren of de gebruiker een functie aanroept,
                        // of een expressie wil uitvoeren
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
                        }
                        // Het gaat om ene expressie en geen functie
                        else {

                            // Expressie wordt uitgevoerd
                            Pattern regexval = Pattern.compile(scan_waarde);
                            Matcher mval = regexval.matcher(currentPart);

                            // Als er wat gevonden, wordt dit toegevoegd
                            if (mval.find()) {

                                insertValues.put(veld, mval.group());

                            }

                        }

                        // Verwerkingsfase:
                        // Controleer of er iets te verwerken valt
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

                // We zijn door de opmerking heen
                // Controleer of de 'rest' opmerking ana de cache toegevoegd wordt
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
    */

    /**
     * calls functieVeldBeroep()
     *
     * @return
     * @throws Exception
     */
    /*
    private String functieOpmerkingenBeroep(String id_registratie, String bron, String value) throws Exception
    {
        String beroep = LinksSpecific.functieOpmerkingenWaardeNaDubbelePunt(value);

        // verwerken, gaat via een generieke methode, ergens anders hebben we geen waarde na opmerking
        String nieuweBeroep = functieVeldBeroep(id_registratie, bron, beroep);

        // return event. bewerkte beroep
        return nieuweBeroep;
    } // functieOpmerkingenBeroep
    */

    private String functieOpmerkingenTeLocatie(String currentPart) {
        String[] locationRaw = currentPart.split("te");

        // to prevent null pointer exception
        if (locationRaw.length > 1) {
            String location = locationRaw[1];
            String cleanLocation = LinksSpecific.funcCleanSides(location);
            return cleanLocation;
        } else {
            return "";
        }
    } // functieOpmerkingenTeLocatie

    /*
    private String functieOpmerkingenLocatie(String value) throws Exception {
        String locatie = LinksSpecific.functieOpmerkingenWaardeNaDubbelePunt(value);

        // verwerken, gaat via een generieke methode, ergens anders hebben we geen waarde na opmerking
        String nieuweLocatie = functieVeldLocatie(locatie);

        // return event. bewerkte beroep
        return nieuweLocatie;
    } // functieOpmerkingenLocatie
    */

    private HashMap functieOpmerkingenGebDatOverl(String currentPart) {
        String cleanValue = currentPart.replaceAll("[G|g]eboren", "");

        String cleanDate = LinksSpecific.funcCleanSides(cleanValue);

        // create hashmap to put values into
        HashMap values = new HashMap();

        values.put("birth_date", cleanDate);

        // date has to be devided
        DateYearMonthDaySet dymd = LinksSpecific.divideCheckDate( cleanDate );

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

    /*
    private HashMap functieOpmerkingenLocatieEnDatum(String currentPart) throws Exception {
        String cleanValue = LinksSpecific.functieOpmerkingenWaardeNaDubbelePunt(currentPart);

        // functie datum scheiding
        String[] devidedValueDate = divideValueDate( cleanValue ).split( "&" );

        HashMap values = new HashMap();

        //add location, via verwijzing
        values.put("birth_location", functieVeldLocatie(LinksSpecific.funcCleanSides(devidedValueDate[0])));


        String date = LinksSpecific.funcCleanSides(devidedValueDate[1]);

        values.put("birth_date", date);

        // date has to be devided
        DateYearMonthDaySet dymd = LinksSpecific.divideCheckDate( date );

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
    */

    /**
     * Verwerk in de lijst, en geef evt. ander beroep terug
     * uses ttalOccupation
     *
     * @return
     * @throws Exception
     */
    /*
    private String functieVeldBeroep(String id_registratie, String id_bron, String beroepTemp) throws Exception
    {
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
    */

    // Verwerk in de lijst, en geef evt. ander locatie terug
    /*
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
    */

    private String divideValueDate( String valueToDevide )
    {
        Pattern regex = Pattern.compile( "[0-9]+-[0-9]+-[0-9]+" );
        Matcher m = regex.matcher( valueToDevide );

        String date = m.group();

        String onlyData = valueToDevide.replaceAll( "[0-9]+-[0-9]+-[0-9]+", "" );

        return onlyData + "$" + date;
    } // divideValueDate


    /**
     * calls functieParseRemarks()
     *
     * @throws Exception
     */
    /*
    public void scanRemarks(String bronnr) throws Exception
    {
        // Lees Scan instellingen in
        showMessage("Preparing remarks parsing...", false, false);
        ResultSet rsScanStrings = conGeneral.runQueryWithResult(
                "SELECT * FROM scan_remarks ORDER BY maintype, group_no, priority_no");

        String query;

        if (bronnr.isEmpty()) {
            query = "SELECT id_registration , id_source , registration_maintype , remarks FROM registration_o" + sourceFilter;
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
        // while (keySetIterator.hasNext()) {
        //
        // Object key = keySetIterator.next();
        // Object value = cache.get(key);
        //
        // String[] velden = {"registration_maintype", "content", "frequency"};
        //
        // // eventuele quotes vervangen
        // String cleanKey = LinksSpecific.funcPrepareForMysql(key.toString());
        // String[] data = {cleanKey.substring(0, cleanKey.indexOf(":")), cleanKey.substring((cleanKey.indexOf(":") + 1)), value.toString()};
        // conLog.insertIntoTable("log_rest_remarks_" + sourceId + bronnr + "_" + logTableName, velden, data);
        // }

        rs.close();
        rs = null;

        showMessage( endl, false, true );
    } // scanRemarks
    */

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
        showMessage( query, false, true );

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
        String msg = logText + " " + mmss;
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
     */
    private void showMessage_nl()
    {
        String newLineToken = "\r\n";

        taLOLCoutput.append( newLineToken );

        try { plog.show( newLineToken ); }
        catch( Exception ex ) { System.out.println( ex.getMessage() ); }
    } // showMessage_nl


    /**
     * @param name
     * @return
     */
    private String cleanName( String name ) {
        return name.replaceAll("[^A-Za-z0-9 '\\-\\.,èêéëÈÊÉËùûúüÙÛÚÜiìîíïÌÎÍÏòôóöÒÔÓÖàâáöÀÂÁÄçÇ]+", "");
    } // cleanName

    /**
     * @param name
     * @return
     */
    private String cleanFirstName( String name ) {
        return name.replaceAll("[^A-Za-z0-9 '\\-èêéëÈÊÉËùûúüÙÛÚÜiìîíïÌÎÍÏòôóöÒÔÓÖàâáöÀÂÁÄçÇ]+", "");
    } // cleanFirstName

    /**
     * @param name
     * @return
     */
    private String cleanFamilyname( String name ) {
        return name.replaceAll("[^A-Za-z0-9 '\\-èêéëÈÊÉËùûúüÙÛÚÜiìîíïÌÎÍÏòôóöÒÔÓÖàâáöÀÂÁÄçÇ]+", "").replaceAll("\\-", " ");
    } // cleanFamilyname


    /**
     * clear GUI output text fields
     */
    public void clearTextFields() {
        //String timestamp = LinksSpecific.getTimeStamp2( "HH:mm:ss" );
        //System.out.println( timestamp + " clearTextFields()" );

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
     *
     */
    /*
    private void setSourceFilters()
    {
        showMessage( "Set source filters for: " + sourceId, false, true );

        //sourceFilter = " WHERE id_source = " + sourceId;

        //sourceFilterCleanPers = " WHERE person_c.id_source = "       + sourceId;
        //sourceFilterOrigPers  = " WHERE person_o.id_source = "       + sourceId;
        //sourceFilterCleanReg  = " WHERE registration_c.id_source = " + sourceId;
        //sourceFilterOrigReg   = " WHERE registration_o.id_source = " + sourceId;

    } // setSourceFilters
    */

    /**
     * @param name
     * @param id
     * @return
     * @throws Exception
     */
    private String namePrepiece( String name, int id ) throws Exception {


        if( !name.contains( " " ) ) {
            return name;            // if no spaces return name
        }

        String fullName = "";

        String list_TN = "";
        String list_TO = "";
        String list_PF = "";

        // Split name
        Queue<String> names = new LinkedList();

        String[] namesArray = name.split( " " );

        for (int i = 0; i < namesArray.length; i++) {
            names.add(namesArray[i]);
        }

        // Check pieces
        while( !names.isEmpty() )
        {
            // Get part
            String part = names.poll();

            if( ttalPrepiece.originalExists( part ) && ttalPrepiece.getStandardCodeByOriginal( part ).equalsIgnoreCase( SC_Y ) )
            {
                // Add to person
                if( ttalPrepiece.getColumnByOriginal( "title_noble", part ) != null && !ttalPrepiece.getColumnByOriginal( "title_noble", part ).isEmpty() )
                {
                    list_TN += ttalPrepiece.getColumnByOriginal( "title_noble", part ) + " ";
                }
                else if( ttalPrepiece.getColumnByOriginal( "title_other", part ) != null && !ttalPrepiece.getColumnByOriginal( "title_other", part ).isEmpty() )
                {
                    list_TO += ttalPrepiece.getColumnByOriginal( "title_other", part ) + " ";
                }
                else if( ttalPrepiece.getColumnByOriginal( "prefix", part ) != null && !ttalPrepiece.getColumnByOriginal( "prefix", part ).isEmpty() )
                {
                    list_PF += ttalPrepiece.getColumnByOriginal( "prefix", part ) + " ";
                }
            }
            else    // return name
            {
                while( !names.isEmpty() ) {
                    fullName += " " + names.poll();
                }

                fullName = part + fullName;      // add part to name

                break;
            }
        }

        // remove last spaces
        if( !list_TN.isEmpty() ) {
            list_TN = list_TN.substring( 0, ( list_TN.length() - 1 ) );

            conCleaned.runQuery( PersonC.updateQuery( "title_noble", list_TN, id ) );
        }

        if( !list_TO.isEmpty() ) {
            list_TO = list_TO.substring( 0, ( list_TO.length() - 1 ) );

            conCleaned.runQuery( PersonC.updateQuery( "title_other", list_TO, id ) );
        }

        if( !list_PF.isEmpty() ) {
            list_PF = list_PF.substring( 0, ( list_PF.length() - 1 ) );

            conCleaned.runQuery( PersonC.updateQuery( "prefix", list_PF, id ) );
        }

        return fullName;
    } // namePrepiece


    /**
     *
     */
    public void flagBirthDate()
    {
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
            conCleaned.runQuery( query1 );
            conCleaned.runQuery( query2 );
            conCleaned.runQuery( query3) ;
        } catch( Exception ex ) {
            showMessage( "Exception while flagging Birth date: " + ex.getMessage(), false, true );
        }
    } // flagBirthDate


    /**
     *
     */
    public void flagMarriageDate()
    {
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
            conCleaned.runQuery( query1 );
            conCleaned.runQuery( query2 );
            conCleaned.runQuery( query3 );

        } catch( Exception ex ) {
            showMessage( "Exception while flagging Marriage date: " + ex.getMessage(), false, true );
        }
    } // flagMarriageDate


    /**
     *
     */
    public void flagDeathDate()
    {
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
            conCleaned.runQuery( query1 );
            conCleaned.runQuery( query2 );
            conCleaned.runQuery( query3 );

        } catch( Exception ex ) {
            showMessage( "Exception while flagging Death date: " + ex.getMessage(), false, true );
        }
    } // flagDeathDate


    /**
     * @throws Exception
     */
    private void completeMinMaxBirth() throws Exception
    {
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

        conCleaned.runQuery( q1 );
        conCleaned.runQuery( q2 );
    } // completeMinMaxBirth


    /**
     * @throws Exception
     */
    private void completeMinMaxMar() throws Exception
    {
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

        conCleaned.runQuery( q1 );
        conCleaned.runQuery( q2 );
        conCleaned.runQuery( q3 );
        conCleaned.runQuery( q4 );
    } // completeMinMaxMar


    // ---< Previous Links basis functions, They are now part of links cleaned >---

    /**
     * @param sourceNo
     * @throws Exception
     */
    /*
    public void funcRelation(String sourceNo) throws Exception {

        int counter = 0;
        int step = 1000;
        int stepstate = step;

        try {

            String startQuery;
            String idSource;

            // source is given in GUI
            if (sourceNo.isEmpty()) {

                startQuery = "SELECT id_registration , id_person, role, sex FROM person_c " + sourceFilter + " ORDER BY id_registration";

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

                //if (counter == 66563) {
                //    int ie = 0;
                //}


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

                    // Proces the new list
                    // Only if id is not -1
                    // otherwise is is the first time

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
            showMessage(counter + " Exception while running Relation: " + e.getMessage(), false, true);
        }
    } // funcRelation
    */

    /**
     * @param id_person
     * @param id_registration
     * @param reg_year
     * @param main_type
     * @param date_type
     * @param role
     * @param age
     * @return
     * @throws Exception
     */
    private MinMaxYearSet minMaxCalculation
    (
        int    id_person,
        int    id_registration,
        int    reg_year,
        int    main_type,
        String date_type,
        int    role,
        int    age
    )
    throws Exception
    {
        showMessage( "minMaxCalculation()", false, true );

        String min_age_0 = "n";
        String max_age_0 = "n";
        String age_main_role = "nvt";

        String age_reported  = "";
        String function = "";
        int min_year = 0;
        int max_year = 0;

        if( age > 0 )
        {
            age_reported  = "y";

            String query = "SELECT function, min_year, max_year FROM ref_date_minmax"
                + " WHERE maintype = '"    + main_type + "'"
                + " AND role = '"          + role + "'"
                + " AND date_type = '"     + date_type + "'"
                + " AND age_reported = '"  + age_reported + "'"
                + " AND age_main_role = '" + age_main_role  + "'";

            System.out.println( query );
            ResultSet rs = conGeneral.runQueryWithResult( query );

            if( !rs.next() )
            {
                addToReportPerson( id_person, "0", 105, "Null -> [rh:" + main_type + "][ad:" + date_type + "][rol:" + role + "][lg:" + age_reported + "][lh:" + age_main_role + "]" );

                MinMaxYearSet mmj = new MinMaxYearSet();
                return mmj;
            }

            rs.last();        // to last row

            function = rs.getString( "function" );
            min_year = rs.getInt( "min_year" );
            max_year = rs.getInt( "max_year" );
        }
        else
        {
            age_reported  = "n";
            MinMaxMainAgeSet mmmas = minMaxMainAge
            (
                id_person,
                id_registration,
                main_type,
                date_type,
                role,
                age_reported,
                age_main_role
            );
            age       = mmmas.getAgeYear();
            min_age_0 = mmmas.getMinAge0();
            max_age_0 = mmmas.getMaxAge0();

            function = mmmas.getFunction();
            min_year = mmmas.getMinYear();
            max_year = mmmas.getMaxYear();

            System.out.println( "age_year: "  + age );
            System.out.println( "min_age_0: " + min_age_0 );
            System.out.println( "max_age_0: " + max_age_0 );

            System.out.println( "function: " + function );
            System.out.println( "min_year: " + min_year );
            System.out.println( "max_year: " + max_year );
        }

        if( min_age_0 == "y" ) { age = 0; }
        if( max_age_0 == "y" ) { age = 0; }

        int minimal_year = reg_year - age + min_year;
        int maximum_year = reg_year - age + max_year;

        MinMaxYearSet mmj = new MinMaxYearSet();

        mmj.SetMaxYear( maximum_year );
        mmj.SetMinYear( minimal_year );

        if( function.equals( "A" ) ) {
            return mmj;
        }
        else if( function.equals( "E" ) )               // If E, deceased
        {
            if( age < 14 ) {
                mmj.SetMaxYear( 0 );
                mmj.SetMinYear( 0 );
            }

            return mmj;
        }
        else if( function.equals( "C" ) )               // function C, check by reg year
        {
            if( maximum_year > reg_year ) {
                mmj.SetMaxYear( reg_year );
            }
            return mmj;

        }
        else if( function.equals( "D" ) )               // function D
        {
            if( minimal_year > reg_year ) {
                mmj.SetMinYear( reg_year - 14 );
            }
            if( maximum_year > reg_year ) {
                mmj.SetMaxYear( reg_year - 14 );
            }

            return mmj;
        }
        else
        {
            addToReportPerson( id_person, "0", 105, "Null -> [rh:" + main_type + "][ad:" + date_type + "][rol:" + role + "][lg:" + age_reported + "][lh:" + age_main_role + "]" );
        }

        return mmj;                                     // Function A

    } // minMaxCalculation


    /**
     * @param id_person
     * @param id_registration
     * @param main_type
     * @param date_type
     * @param role
     * @return
     * @throws Exception
     */
    private MinMaxMainAgeSet minMaxMainAge
    (
        int    id_person,
        int    id_registration,
        int    main_type,
        String date_type,
        int    role,
        String age_reported,
        String age_main_role
    )
    throws Exception
    {
        boolean done = false;

        MinMaxMainAgeSet mmmas = new MinMaxMainAgeSet();
        mmmas.setMinAge0( "n" );
        mmmas.setMaxAge0( "n" );

        while( !done )
        {
            showMessage( "minMaxMainAge: age_main_role = " + age_main_role, false, true );

            String queryRef = "SELECT function, min_year, max_year, min_person, max_person FROM links_general.ref_date_minmax"
                + " WHERE maintype = '" + main_type + "'"
                + " AND role = '" + role + "'"
                + " AND date_type = '" + date_type + "'"
                + " AND age_reported = '" + age_reported + "'"
                + " AND age_main_role = '" + age_main_role + "'";

            ResultSet rs_ref = conGeneral.runQueryWithResult( queryRef );

            int min_person_role = 0;
            int max_person_role = 0;

            if( !rs_ref.next() ) {
                if( age_main_role.equals( "nvt" ) ) { age_main_role = "y"; }
                else { done = true; }
            }
            else
            {
                min_person_role = rs_ref.getInt( "min_person" );
                max_person_role = rs_ref.getInt( "max_person" );

                mmmas.setFunction( rs_ref.getString( "function" ) );
                mmmas.setMinYear(rs_ref.getInt("min_year"));
                mmmas.setMaxYear(rs_ref.getInt("max_year"));

                boolean readPc = false;

                int main_role = 0;
                if( min_person_role > 0 ) {
                    readPc = true;
                    main_role = min_person_role;
                }
                else { mmmas.setMinAge0( "y" ); }

                if( max_person_role > 0 ) {
                    readPc = true;
                    main_role = max_person_role;
                }
                else { mmmas.setMaxAge0( "y" ); }

                int age = 0;
                String death = "";
                if( readPc ) {
                    String queryPc = "SELECT age_year, death FROM links_cleaned.person_c"
                            + " WHERE id_registration = '" + id_registration + "'"
                            + " AND role = '" + main_role + "'";

                    ResultSet rs_pc = conCleaned.runQueryWithResult( queryPc );

                    int countPc = 0;

                    while (rs_pc.next()) {
                        countPc++;
                        age = rs_pc.getInt("age_year");
                        death = rs_pc.getString("death");
                        mmmas.setAgeYear(age);
                    }

                    if (countPc != 1) {
                        showMessage(queryPc, false, true);
                        throw new Exception("minMaxMainAge: person_c count = " + countPc);
                    }

                    if( age > 0 ) { done = true; }
                    else {
                        if( age_main_role.equals( "y" ) ) { age_main_role = "n"; }
                        else { done = true; }
                    }
                }
                else { done = true; }
            }
        } // while

        return mmmas;

    } // minMaxMainAge


    /**
     * @param hjpsList
     * @param refMinMaxMarriageYear
     * @throws Exception
     */
    private void minMaxMarriageYear(
        ArrayList< MarriageYearPersonsSet > hjpsList,
        ResultSet refMinMaxMarriageYear )
    throws Exception
    {
        int counter = 0;
        int step = 1000;
        int stepstate = step;

        // Loop through all persons
        for( int i = 0; i < hjpsList.size(); i++ )
        {
            counter++;

            if( counter == stepstate ) {
                showMessage( counter + "", true, true );
                stepstate += step;
            }

            // walk through
            refMinMaxMarriageYear.beforeFirst();

            boolean role1Found = false;
            int role1 = 0;      // role1 not used ?
            int role2 = 0;

            while( refMinMaxMarriageYear.next() )
            {
                int tempRht   = refMinMaxMarriageYear.getInt( "maintype" );
                int tempRole1 = refMinMaxMarriageYear.getInt( "role1" );
                int tempRole2 = refMinMaxMarriageYear.getInt( "role2" );

                if( ( tempRole1 == hjpsList.get( i ).getRole() ) &&
                      tempRht == ( hjpsList.get( i ).getRegistrationMainType() ) ) {
                    // role found
                    role1Found = true;
                    role1      = tempRole1;
                    role2      = tempRole2;

                    break;
                }
            }

            if( role1Found )             // check if role 1 is found
            {
                // search role 2
                boolean role2Found = false;
                int role2Id = 0;

                int role2MarYearMin  = 0;
                int role2MarYearMax  = 0;
                int role2MarMonthMin = 0;
                int role2MarMonthMax = 0;
                int role2MarDayMin   = 0;
                int role2MarDayMax   = 0;

                // walk trough all persons of registration
                for( int j = (((i - 7) > 0) ? i - 7 : 0); j < ((i + 7) > hjpsList.size() ? hjpsList.size() : i + 7); j++ )
                {
                    if( (role2 == hjpsList.get(j).getRole()) && (hjpsList.get(i).getIdRegistration() == hjpsList.get(j).getIdRegistration()) )
                    {
                        // Role 2 found
                        role2Found       = true;
                        role2Id          = hjpsList.get(j).getIdPerson();
                        role2MarYearMin  = hjpsList.get(j).getMarriageYearMin();
                        role2MarYearMax  = hjpsList.get(j).getMarriageYearMax();
                        role2MarMonthMin = hjpsList.get(j).getMarriageMonthMin();
                        role2MarMonthMax = hjpsList.get(j).getMarriageMonthMax();
                        role2MarDayMin   = hjpsList.get(j).getMarriageDayMin();
                        role2MarDayMax   = hjpsList.get(j).getMarriageDayMax();

                        break;
                    }
                }

                // check is role 2 found
                if( role2Found )
                {
                    int role1Id          = hjpsList.get(i).getIdPerson();
                    int role1MarYearMax  = hjpsList.get(i).getMarriageYearMax();
                    int role1MarYearMin  = hjpsList.get(i).getMarriageYearMin();
                    int role1MarMonthMax = hjpsList.get(i).getMarriageMonthMax();
                    int role1MarMonthMin = hjpsList.get(i).getMarriageMonthMin();
                    int role1MarDayMax   = hjpsList.get(i).getMarriageDayMax();
                    int role1MarDayMin   = hjpsList.get(i).getMarriageDayMin();

                    // First role 2, min Year
                    if( dateLeftIsGreater( role1MarYearMin, role1MarMonthMin, role1MarDayMin, role2MarYearMin, role2MarMonthMin, role2MarDayMin ) )
                    {
                        // Query
                        String query = ""
                            + " UPDATE person_c"
                            + " SET"
                            + " mar_year_min = " + hjpsList.get(i).getMarriageYearMin() + ","
                            + " mar_month_min = " + hjpsList.get(i).getMarriageMonthMin() + ","
                            + " mar_day_min = " + hjpsList.get(i).getMarriageDayMin()
                            + " WHERE"
                            + " id_person = " + role2Id;

                        conCleaned.runQuery( query );
                    }

                    // Role 2, max year
                    if( dateLeftIsGreater( role2MarYearMax, role2MarMonthMax, role2MarDayMax, role1MarYearMax, role1MarMonthMax, role1MarDayMax ) )
                    {
                        // Query
                        String query = ""
                            + " UPDATE person_c"
                            + " SET"
                            + " mar_year_max = " + hjpsList.get(i).getMarriageYearMax() + ","
                            + " mar_month_max = " + hjpsList.get(i).getMarriageMonthMax() + ","
                            + " mar_day_max = " + hjpsList.get(i).getMarriageDayMax()
                            + " WHERE"
                            + " id_person = " + role2Id;
                        conCleaned.runQuery( query );

                    }

                    // role 1
                    if( dateLeftIsGreater( role2MarYearMin, role2MarMonthMin, role2MarDayMin, role1MarYearMin, role1MarMonthMin, role1MarDayMin ) )
                    {
                        // Query
                        String query = "UPDATE person_c"
                            + " SET"
                            + " mar_year_min = " + role2MarYearMin + ","
                            + " mar_month_min = " + role2MarMonthMin + ","
                            + " mar_day_min = " + role2MarDayMin
                            + " WHERE"
                            + " id_person = " + role1Id;

                        conCleaned.runQuery( query );
                    }

                    // Role 1, max year
                    if( dateLeftIsGreater( role1MarYearMax, role1MarMonthMax, role1MarDayMax, role2MarYearMax, role2MarMonthMax, role2MarDayMax ) )
                    {
                        // Query
                        String query = "UPDATE person_c"
                            + " SET"
                            + " mar_year_max = " + role2MarYearMax + ","
                            + " mar_month_max = " + role2MarMonthMax + ","
                            + " mar_day_max = " + role2MarDayMax
                            + " WHERE"
                            + " id_person = " + role1Id;

                        conCleaned.runQuery( query );
                    }
                }
            }
        }
    } // minMaxMarriageYear


    /**
     * @param lYear
     * @param lMonth
     * @param lDay
     * @param rYear
     * @param rMonth
     * @param rDay
     * @return
     */
    private boolean dateLeftIsGreater( int lYear, int lMonth, int lDay, int rYear, int rMonth, int rDay )
    {
        if( lYear > rYear )      { return true; }   // lower, date is correct, return original date
        else if( lYear < rYear ) { return false; }  // return person date

        // years are equal, check months
        if( lMonth > rMonth ) { return true; }      // month is correct, return original month
        else if (lMonth < rMonth) { return false; }

        // months are equal, check days
        if( lDay > rDay ) { return true; }

        return false;
    } // dateLeftIsGreater


    /**
     * @param sourceNo
     * @return
     * @throws Exception
     */
    private ArrayList< MarriageYearPersonsSet > setMarriageYear( String sourceNo )
    throws Exception
    {
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

        /*
        String query = ""
            + " SELECT "
            + " registration_c.id_registration ,"
            + " registration_c.registration_maintype ,"
            + " pers.id_person ,"
            + " pers.role ,"
            + " pers.mar_day_min ,"
            + " pers.mar_day_max ,"
            + " pers.mar_month_min ,"
            + " pers.mar_month_max ,"
            + " pers.mar_year_min ,"
            + " pers.mar_year_max"
            + " FROM registration_c , pers"
            + " WHERE registration_c.id_registration = pers.id_registration ORDER BY pers.id_registration;";
        */

        ResultSet minmaxjaarRs = conCleaned.runQueryWithResult( query );

        ArrayList< MarriageYearPersonsSet > hjpsList = new ArrayList< MarriageYearPersonsSet >();

        while( minmaxjaarRs.next() )
        {
            MarriageYearPersonsSet hjps = new MarriageYearPersonsSet();

            hjps.setIdRegistration( minmaxjaarRs.getInt( "id_registration" ) );
            hjps.setRegistrationMainType( minmaxjaarRs.getInt( "registration_maintype" ) );
            hjps.setIdPerson( minmaxjaarRs.getInt( "id_person" ) );
            hjps.setRole( minmaxjaarRs.getInt( "role" ) );
            hjps.setMarriageDayMin( minmaxjaarRs.getInt( "mar_day_min" ) );
            hjps.setMarriageDayMax( minmaxjaarRs.getInt( "mar_day_max" ) );
            hjps.setMarriageMonthMin( minmaxjaarRs.getInt( "mar_month_min" ) );
            hjps.setMarriageMonthMax( minmaxjaarRs.getInt( "mar_month_max" ) );
            hjps.setMarriageYearMin( minmaxjaarRs.getInt( "mar_year_min" ) );
            hjps.setMarriageYearMax( minmaxjaarRs.getInt( "mar_year_max" ) );

            hjpsList.add( hjps );
        }

        return hjpsList;
    } // setMarriageYear


    private void partsToDate( String source )
    {
        String query = "UPDATE links_cleaned.person_c SET "
            + "links_cleaned.person_c.birth_date_min  = CONCAT( links_cleaned.person_c.birth_day_min , '-' , links_cleaned.person_c.birth_month_min , '-' , links_cleaned.person_c.birth_year_min ) ,"
            + "links_cleaned.person_c.mar_date_min    = CONCAT( links_cleaned.person_c.mar_day_min ,   '-' , links_cleaned.person_c.mar_month_min ,   '-' , links_cleaned.person_c.mar_year_min ) ,"
            + "links_cleaned.person_c.death_date_min  = CONCAT( links_cleaned.person_c.death_day_min , '-' , links_cleaned.person_c.death_month_min , '-' , links_cleaned.person_c.death_year_min ) ,"
            + "links_cleaned.person_c.birth_date_max  = CONCAT( links_cleaned.person_c.birth_day_max , '-' , links_cleaned.person_c.birth_month_max , '-' , links_cleaned.person_c.birth_year_max ) ,"
            + "links_cleaned.person_c.mar_date_max    = CONCAT( links_cleaned.person_c.mar_day_max ,   '-' , links_cleaned.person_c.mar_month_max ,   '-' , links_cleaned.person_c.mar_year_max ) ,"
            + "links_cleaned.person_c.death_date_max  = CONCAT( links_cleaned.person_c.death_day_max , '-' , links_cleaned.person_c.death_month_max , '-' , links_cleaned.person_c.death_year_max ) "
            + "WHERE id_source = " + source;

        try {
            conCleaned.runQuery( query );
        } catch( Exception ex ) {
            showMessage( "Exception while Creating full dates from parts: " + ex.getMessage(), false, true );
        }
    } // partsToDate


    private void daysSinceBegin( String source )
    {
        boolean debug = false;

        String query1 = "UPDATE IGNORE person_c SET birth_min_days = DATEDIFF( date_format( str_to_date( birth_date_min, '%d-%m-%Y' ), '%Y-%m-%d' ) , '1-1-1' ) WHERE birth_date_min  NOT LIKE '0-%' AND birth_date_min   NOT LIKE '%-0-%'";
        String query2 = "UPDATE IGNORE person_c SET birth_max_days = DATEDIFF( date_format( str_to_date( birth_date_max, '%d-%m-%Y' ), '%Y-%m-%d' ) , '1-1-1' ) WHERE birth_date_max  NOT LIKE '0-%' AND birth_date_max   NOT LIKE '%-0-%'";
        String query3 = "UPDATE IGNORE person_c SET mar_min_days   = DATEDIFF( date_format( str_to_date( mar_date_min,   '%d-%m-%Y' ), '%Y-%m-%d' ) , '1-1-1' ) WHERE mar_date_min    NOT LIKE '0-%' AND mar_date_min     NOT LIKE '%-0-%'";
        String query4 = "UPDATE IGNORE person_c SET mar_max_days   = DATEDIFF( date_format( str_to_date( mar_date_max,   '%d-%m-%Y' ), '%Y-%m-%d' ) , '1-1-1' ) WHERE mar_date_max    NOT LIKE '0-%' AND mar_date_max     NOT LIKE '%-0-%'";
        String query5 = "UPDATE IGNORE person_c SET death_min_days = DATEDIFF( date_format( str_to_date( death_date_min, '%d-%m-%Y' ), '%Y-%m-%d' ) , '1-1-1' ) WHERE death_date_min  NOT LIKE '0-%' AND death_date_min   NOT LIKE '%-0-%'";
        String query6 = "UPDATE IGNORE person_c SET death_max_days = DATEDIFF( date_format( str_to_date( death_date_max, '%d-%m-%Y' ), '%Y-%m-%d' ) , '1-1-1' ) WHERE death_date_max  NOT LIKE '0-%' AND death_date_max   NOT LIKE '%-0-%'";

        query1 += "AND id_source = " + source;
        query2 += "AND id_source = " + source;
        query3 += "AND id_source = " + source;
        query4 += "AND id_source = " + source;
        query5 += "AND id_source = " + source;
        query6 += "AND id_source = " + source;

        String queryReg = "UPDATE registration_c SET "
            + "registration_days = DATEDIFF( date_format( str_to_date( registration_date, '%d-%m-%Y' ), '%Y-%m-%d' ) , '1-1-1' ) "
            + "WHERE registration_date  NOT LIKE '0-%' AND registration_date   NOT LIKE '%-0-%' "
            + "AND id_source = " + source;

        try
        {
            if( debug ) { showMessage( "q1", false, true ); }
            conCleaned.runQuery( query1 );

            if( debug ) { showMessage( "q2", false, true ); }
            conCleaned.runQuery( query2 );

            if( debug ) { showMessage( "q3", false, true ); }
            conCleaned.runQuery( query3 );

            if( debug ) { showMessage( "q4", false, true ); }
            conCleaned.runQuery( query4 );

            if( debug ) { showMessage( "q5", false, true ); }
            conCleaned.runQuery( query5 );

            if( debug ) { showMessage( "q6", false, true ); }
            conCleaned.runQuery( query6 );

            if( debug ) { showMessage( "q7", false, true ); }
            conCleaned.runQuery( queryReg );
        }
        catch( Exception ex ) {
            showMessage( "Exception while computing days since 1-1-1: " + ex.getMessage(), false, true );
        }
    } // daysSinceBegin


    /**
     * @throws Exception
     */
    private void createTempFamilynameTable( String source ) throws Exception
    {
        String tablename = "familyname_t_" + source;

        showMessage( "Creating " + tablename + " table", false, true );

        String query = "CREATE  TABLE links_temp." + tablename + " ("
            + " person_id INT UNSIGNED NOT NULL AUTO_INCREMENT ,"
            + " familyname VARCHAR(80) NULL ,"
            + " PRIMARY KEY (person_id) );";

        conTemp.runQuery( query );

    } // createTempFamilynameTable


    /**
     * @throws Exception
     */
    private void createTempFamilynameFile( String source ) throws Exception
    {
        String filename = "familyname_t_" + source + ".csv";
        showMessage( "Creating " + filename, false, true );

        File f = new File( filename );
        if( f.exists() ) { f.delete(); }
        writerFamilyname = new FileWriter( filename );
    } // createTempFamilynameFile


    /**
     * @throws Exception
     */
    private void loadFamilynameToTable( String source ) throws Exception
    {
        showMessage( "Loading CSV data into temp table", false, true );

        String csvname   = "familyname_t_" + source + ".csv";
        String tablename = "familyname_t_" + source;

        String query = "LOAD DATA LOCAL INFILE '" + csvname + "'"
            + " INTO TABLE " + tablename
            + " FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' ( person_id , familyname );";

        conTemp.runQuery( query );

    } // loadFamilynameToTable


    /**
     *
     */
    private void updateFamilynameToPersonC( String source ) throws Exception
    {
        showMessage( "Moving familynames from temp table to person_c", false, true );

        String tablename = "familyname_t_" + source;

        String query = "UPDATE links_cleaned.person_c, links_temp."   + tablename
            + " SET links_cleaned.person_c.familyname = links_temp."  + tablename + ".familyname"
            + " WHERE links_cleaned.person_c.id_person = links_temp." + tablename + ".person_id;";

        conTemp.runQuery( query );
    } // updateFamilynameToPersonC


    public void removeFamilynameFile( String source ) throws Exception
    {
        String csvname = "familyname_t_" + source + ".csv";

        showMessage( "Removing " + csvname, false, true );

        java.io.File f = new java.io.File( csvname );
        f.delete();
    } // removeFamilynameFile


    public void removeFamilynameTable( String source ) throws Exception
    {
        String tablename = "familyname_t_" + source;

        showMessage( "Removing table " + tablename, false, true );

        String query = "DROP TABLE IF EXISTS " + tablename + ";";

        conTemp.runQuery( query );
    } // removeFamilynameTable


    /**
     * @throws Exception
     */
    private void createTempFirstnameTable( String source ) throws Exception
    {
        String tablename = "firstname_t_" + source;

        showMessage( "Creating " + tablename + " table", false, true );

        String query = "CREATE  TABLE links_temp." + tablename + " ("
            + " person_id INT UNSIGNED NOT NULL AUTO_INCREMENT ,"
            + " firstname VARCHAR(80) NULL ,"
            + " PRIMARY KEY (person_id) );";

        conTemp.runQuery( query );
    } // createTempFirstnameTable


    /**
     * @throws Exception
     */
    private void createTempFirstnameFile( String source ) throws Exception
    {
        String filename = "firstname_t_" + source + ".csv";
        showMessage( "Creating " + filename, false, true );

        File f = new File( filename );
        if( f.exists() ) { f.delete(); }
        writerFirstname = new FileWriter( filename );
    } // createTempFirstnameFile


    /**
     * @throws Exception
     */
    private void loadFirstnameToTable( String source ) throws Exception
    {
        showMessage( "Loading CSV data into temp table", false, true );

        String csvname   = "firstname_t_" + source + ".csv";
        String tablename = "firstname_t_" + source;

        String query = "LOAD DATA LOCAL INFILE '" + csvname + "'"
            + " INTO TABLE " + tablename
            + " FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' ( person_id , firstname );";

        conTemp.runQuery( query );
    } // loadFirstnameToTable


    /**
     *
     */
    private void updateFirstnameToPersonC( String source ) throws Exception
    {
        showMessage( "Moving first names from temp table to person_c...", false, true );

        String tablename = "firstname_t_" + source;

        String query = "UPDATE links_cleaned.person_c, links_temp."   + tablename
            + " SET links_cleaned.person_c.firstname = links_temp."   + tablename + ".firstname"
            + " WHERE links_cleaned.person_c.id_person = links_temp." + tablename + ".person_id;";

        conTemp.runQuery( query );
    } // updateFirstnameToPersonC


    /**
     * @throws Exception
     */
    public void removeFirstnameFile( String source ) throws Exception
    {
        String csvname = "firstname_t_" + source + ".csv";

        showMessage( "Removing " + csvname, false, true );

        File f = new File( csvname );
        f.delete();
    } // removeFirstnameFile


    /**
     * @throws Exception
     */
    public void removeFirstnameTable( String source ) throws Exception
    {
        String tablename = "firstname_t_" + source;

        showMessage( "Removing table " + tablename, false, true );

        String query = "DROP TABLE IF EXISTS " + tablename + ";";
        conTemp.runQuery( query );
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


    private void postTasks( String source ) throws Exception
    {
        /*
        String[] queries =
        {
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
        */

        String[] queries =
        {
            "UPDATE links_cleaned.person_c SET sex = 'v' WHERE role = 2 AND id_source = " + source,
            "UPDATE links_cleaned.person_c SET sex = 'm' WHERE role = 3 AND id_source = " + source,
            "UPDATE links_cleaned.person_c SET sex = 'v' WHERE role = 4 AND id_source = " + source,
            "UPDATE links_cleaned.person_c SET sex = 'v' WHERE role = 5 AND id_source = " + source,
            "UPDATE links_cleaned.person_c SET sex = 'm' WHERE role = 6 AND id_source = " + source,
            "UPDATE links_cleaned.person_c SET sex = 'm' WHERE role = 7 AND id_source = " + source,
            "UPDATE links_cleaned.person_c SET sex = 'v' WHERE role = 8 AND id_source = " + source,
            "UPDATE links_cleaned.person_c SET sex = 'm' WHERE role = 9 AND id_source = " + source,

            "UPDATE links_cleaned.person_c SET sex = '' WHERE sex <> 'm' AND sex <> 'v' AND id_source = " + source,


            "CREATE  TABLE links_match.male ( id_registration INT NOT NULL , PRIMARY KEY (id_registration) );",

            "CREATE  TABLE links_match.female ( id_registration INT NOT NULL , PRIMARY KEY (id_registration) );",

            "INSERT INTO links_match.male(id_registration) SELECT id_registration FROM links_cleaned.person_c WHERE role = 10 AND sex = 'm';",

            "INSERT INTO links_match.female(id_registration) SELECT id_registration FROM links_cleaned.person_c WHERE role = 10 AND sex = 'v';",

            "UPDATE links_cleaned.person_c, links_match.male SET sex = 'v' WHERE links_match.male.id_registration = links_cleaned.person_c.id_registration AND role = 11 "
                + " AND id_source = " + source,

            "UPDATE links_cleaned.person_c, links_match.female SET sex = 'm' WHERE links_match.female.id_registration = links_cleaned.person_c.id_registration AND role = 11 "
                + " AND id_source = " + source,

            "DROP TABLE links_match.male;",

            "DROP TABLE links_match.female;",


            "UPDATE links_cleaned.person_c SET firstname = '' , stillborn = 1 WHERE firstname like '%ood%ebore%' AND id_source = " + source,

            "UPDATE links_cleaned.person_c SET firstname = LOWER(firstname),  familyname = LOWER(familyname) WHERE id_source = " + source,

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
                + "AND id_source = " + source
        };

        // Execute queries
        for( String s : queries ) {
            conCleaned.runQuery( s );
        }
    } // postTasks


    private void deleteRows()
    throws Exception
    {
        showMessage( "deleteRows() deleting empty links_cleaned.person_c records.", false, true );
        String q1 = "DELETE FROM links_cleaned.person_c WHERE ( familyname = '' OR familyname is null ) AND ( firstname = '' OR firstname is null )";
        conCleaned.runQuery( q1 );
    } // deleteRows


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


    // ---< Date functions >----------------------------------------------------

    private void setValidDateComplete( String source ) throws Exception
    {
        String q = ""
            + " UPDATE links_cleaned.person_c"
            + " SET"
            + " valid_complete = 1"
            + " WHERE"
            + " birth_date_valid = 1 AND"
            + " mar_date_valid   = 1 AND"
            + " death_date_valid = 1 AND"
            + " links_cleaned.person_c.id_source = " + source;

        conCleaned.runQuery( q );
    } // setValidDateComplete



    /**
     * @param source
     * @throws Exception
     *
     * if the date is valid, set the min en max values of date, year, month, day equal to the given values
     * do this for birth, marriage and death
     */
    private void minMaxValidDate( String source ) throws Exception
    {
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
                + "birth_date_valid = 1 "
                + "AND links_cleaned.person_c.id_source = " + source;

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
                + "mar_date_valid = 1 "
                + "AND links_cleaned.person_c.id_source = " + source;

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
                + "death_date_valid = 1 "
                + "AND links_cleaned.person_c.id_source = " + source;

        conCleaned.runQuery( q1 );
        conCleaned.runQuery( q2 );
        conCleaned.runQuery( q3 );
    } // minMaxValidDate


    /**
     * minMaxDateMain
     * @param source
     * @throws Exception
     */
    public void minMaxDateMain( String source ) throws Exception
    {
        boolean debug = true;
        int counter = 0;
        int step = 10000;
        int stepstate = step;

        // Because this query acts on person_c and registration_c (instead of person_o and registration_o),
        // the cleaning options Age and Role must be run together with Dates.
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
            + " valid_complete = 0"
            + " AND links_cleaned.person_c.id_source = " + source;

        if( debug ) {
            showMessage( "minMaxDateMain()", false, true );
            showMessage( startQuery, false, true );
        }

        try
        {
            ResultSet rsPersons = conCleaned.runQueryWithResult( startQuery );            // Run person query

            // Count hits
            rsPersons.last();
            int total = rsPersons.getRow();
            rsPersons.beforeFirst();
            showMessage( "record 0 of " + total, true, true );
            showMessage( "record 0 of " + total, false, true );

            MinMaxDateSet mmds = new MinMaxDateSet();

            while( rsPersons.next() )
            {
                counter++;

                if( counter == stepstate ) {
                    showMessage( counter + " of " + total, true, true );
                    stepstate += step;
                }

                // Get
                int    id_registration      = rsPersons.getInt(    "id_registration" );
                int    id_source            = rsPersons.getInt(    "id_source" );
                String registrationDate     = rsPersons.getString( "registration_date" );
                int    registrationMaintype = rsPersons.getInt(    "registration_maintype" );
                int    id_person            = rsPersons.getInt(    "id_person" );
                int    role                 = rsPersons.getInt(    "role" );
                int    age_year             = rsPersons.getInt(    "age_year" );
                int    age_month            = rsPersons.getInt(    "age_month" );
                int    age_week             = rsPersons.getInt(    "age_week" );
                int    age_day              = rsPersons.getInt(    "age_day" );
                String birth_date           = rsPersons.getString( "person_c.birth_date" );
                String mar_date             = rsPersons.getString( "person_c.mar_date" );
                String death_date           = rsPersons.getString( "person_c.death_date" );
                int    birth_year           = rsPersons.getInt(    "birth_year" );
                int birth_date_valid        = rsPersons.getInt(    "birth_date_valid" );
                int mar_date_valid          = rsPersons.getInt(    "mar_date_valid" );
                int death_date_valid        = rsPersons.getInt(    "death_date_valid" );

                if( debug ) {
                    if( id_registration == 668084 ) {
                        showMessage_nl();
                        showMessage( "id_registration: "      + id_registration,      false, true );
                        showMessage( "id_person: "            + id_person,            false, true );
                        showMessage( "id_source: "            + id_source,            false, true );
                        showMessage( "registrationDate: "     + registrationDate,     false, true );
                        showMessage( "registrationMaintype: " + registrationMaintype, false, true );
                        showMessage( "role: "                 + role,                 false, true );
                        showMessage( "age_year: "             + age_year,             false, true );
                        showMessage( "age_month: "            + age_month,            false, true );
                        showMessage( "age_week: "             + age_week,             false, true );
                        showMessage( "age_day: "              + age_day,              false, true );
                        showMessage( "birth_date: "           + birth_date,           false, true );
                        showMessage( "mar_date: "             + mar_date,             false, true );
                        showMessage( "death_date: "           + death_date,           false, true );
                        showMessage( "birth_year: "           + birth_year,           false, true );
                        showMessage( "birth_date_valid: "     + birth_date_valid,     false, true );
                        showMessage( "mar_date_valid: "       + mar_date_valid,       false, true );
                        showMessage( "death_date_valid: "     + death_date_valid,     false, true );
                    }
                    else { continue; }
                }

                // Fill object
                mmds.setRegistrationId( id_registration );
                mmds.setSourceId( id_source );
                mmds.setRegistrationDate( registrationDate );
                mmds.setRegistrationMaintype( registrationMaintype );
                mmds.setPersonId( id_person );
                mmds.setPersonRole( role );
                mmds.setPersonAgeYear( age_year );
                mmds.setPersonAgeMonth( age_month );
                mmds.setPersonAgeWeek( age_week );
                mmds.setPersonAgeDay( age_day );
                mmds.setPersonBirthYear( birth_year );
                mmds.setDeathDate( death_date );

                int mainrole;

                switch( registrationMaintype ) {
                    case 1:
                        mainrole = 1;
                        break;
                    case 2:
                        if( (role == 7) || (role == 8) || (role == 9) ) {
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

                if( debug ) { showMessage( "mainrole: " + mainrole, false, true ); }
                mmds.setRegistrationMainRole( mainrole );                // main role

                String type_date = "";

                if( birth_date_valid != 1 )                 // invalid birth date
                {
                    if( debug ) { showMessage( "invalid birth date", false, true ); }

                    mmds.setTypeDate( "birth_date" );
                    type_date = "birth";
                    mmds.setDate( birth_date );

                    DivideMinMaxDatumSet ddmdBirth = minMaxDate( mmds );
                    ddmdBirth.nonnegative();        // is this necessary?

                    if( debug ) { showMessage( ddmdBirth.getMinMaxDate(), false, true ); }

                    // Min Max to table
                    String queryBirth = "UPDATE person_c"
                        + " SET "
                        + type_date + "_year_min"  + " = " + ddmdBirth.getMinYear() + " ,"
                        + type_date + "_month_min" + " = " + ddmdBirth.getMinMonth() + " ,"
                        + type_date + "_day_min"   + " = " + ddmdBirth.getMinDay() + " ,"
                        + type_date + "_year_max"  + " = " + ddmdBirth.getMaxYear() + " ,"
                        + type_date + "_month_max" + " = " + ddmdBirth.getMaxMonth() + " ,"
                        + type_date + "_day_max"   + " = " + ddmdBirth.getMaxDay()
                        + " WHERE person_c.id_person = "   + id_person;

                    conCleaned.runQuery( queryBirth );
                }
                else { if( debug ) { showMessage( "birth date is valid: " + birth_date, false, true ); } }

                if( mar_date_valid != 1 )                // invalid marriage date
                {
                    if( debug ) { showMessage( "invalid marriage date", false, true ); }

                    mmds.setTypeDate( "marriage_date" );
                    type_date = "mar";
                    mmds.setDate(mar_date);

                    DivideMinMaxDatumSet ddmdMarriage = minMaxDate( mmds );
                    ddmdMarriage.nonnegative();        // is this necessary?

                    if( debug ) { showMessage( ddmdMarriage.getMinMaxDate(), false, true ); }

                    // Min Max to table
                    String queryMar = "UPDATE person_c"
                        + " SET "
                        + type_date + "_year_min"  + " = " + ddmdMarriage.getMinYear() + " ,"
                        + type_date + "_month_min" + " = " + ddmdMarriage.getMinMonth() + " ,"
                        + type_date + "_day_min"   + " = " + ddmdMarriage.getMinDay() + " ,"
                        + type_date + "_year_max"  + " = " + ddmdMarriage.getMaxYear() + " ,"
                        + type_date + "_month_max" + " = " + ddmdMarriage.getMaxMonth() + " ,"
                        + type_date + "_day_max"   + " = " + ddmdMarriage.getMaxDay()
                        + " WHERE person_c.id_person = "   + id_person;

                    conCleaned.runQuery( queryMar );
                }
                else { if( debug ) { showMessage( "mar date is valid: " + mar_date, false, true ); } }

                if( death_date_valid != 1 )                // invalid death date
                {
                    if( debug ) { showMessage( "invalid death date", false, true ); }

                    mmds.setTypeDate( "death_date" );
                    type_date = "death";
                    mmds.setDate( death_date );

                    DivideMinMaxDatumSet ddmdDeath = minMaxDate( mmds );
                    ddmdDeath.nonnegative();        // is this necessary?

                    if( debug ) { showMessage( ddmdDeath.getMinMaxDate(), false, true ); }

                    // Min Max to table
                    String queryDeath = "UPDATE person_c"
                        + " SET "
                        + type_date + "_year_min"  + " = " + ddmdDeath.getMinYear()  + " ,"
                        + type_date + "_month_min" + " = " + ddmdDeath.getMinMonth() + " ,"
                        + type_date + "_day_min"   + " = " + ddmdDeath.getMinDay()   + " ,"
                        + type_date + "_year_max"  + " = " + ddmdDeath.getMaxYear()  + " ,"
                        + type_date + "_month_max" + " = " + ddmdDeath.getMaxMonth() + " ,"
                        + type_date + "_day_max"   + " = " + ddmdDeath.getMaxDay()
                        + " WHERE person_c.id_person = "   + id_person;

                    conCleaned.runQuery( queryDeath );
                }
                else { if( debug ) { showMessage( "death date is valid: " + death_date, false, true ); } }
            }
        } catch( Exception ex ) {
            showMessage( counter + " Exception in minMaxDateMain(): " + ex.getMessage(), false, true );
        }
    } // minMaxDateMain


    /**
     * minMaxDate : called by minMaxDateMain for invalid dates
     *
     * @param inputInfo
     * @return
     * @throws Exception
     */
    private DivideMinMaxDatumSet minMaxDate( MinMaxDateSet inputInfo )
    throws Exception
    {
        boolean debug = true;
        if( debug ) { showMessage( "minMaxDate()", false, true ); }

        // registration date
        DateYearMonthDaySet inputregistrationYearMonthDday =
                LinksSpecific.divideCheckDate( inputInfo.getRegistrationDate() );

        // Check: age in years given?
        if( inputInfo.getPersonAgeYear() > 0 )                              // age given in years ?
        {
            if( debug ) { showMessage( "age given in years: " + inputInfo.getPersonAgeYear() , false, true ); }

            DivideMinMaxDatumSet returnSet = new DivideMinMaxDatumSet();    // Create new return set

            if( inputInfo.getPersonRole() == 10 )   // it is the deceased
            {
                // registration date
                DateYearMonthDaySet inputDeathDate = LinksSpecific.divideCheckDate( inputInfo.getDeathDate() );

                // check death date
                if( inputDeathDate.isValidDate() )
                {
                    // Day no and month no are similar to death date
                    returnSet.setMaxDay(   inputDeathDate.getDay() );
                    returnSet.setMaxMonth( inputDeathDate.getMonth() );
                    returnSet.setMinDay(   inputDeathDate.getDay() );
                    returnSet.setMinMonth( inputDeathDate.getMonth() );
                }
                else
                {
                    // Day no and month no are similar to regis date
                    returnSet.setMaxDay(   inputregistrationYearMonthDday.getDay() );
                    returnSet.setMaxMonth( inputregistrationYearMonthDday.getMonth() );
                    returnSet.setMinDay(   inputregistrationYearMonthDday.getDay() );
                    returnSet.setMinMonth( inputregistrationYearMonthDday.getMonth() );
                }
            }
            else        // it is not the deceased
            {
                // Day no en month no are similar to regis date
                returnSet.setMaxDay(   inputregistrationYearMonthDday.getDay() );
                returnSet.setMaxMonth( inputregistrationYearMonthDday.getMonth() );
                returnSet.setMinDay(   inputregistrationYearMonthDday.getDay() );
                returnSet.setMinMonth( inputregistrationYearMonthDday.getMonth() );
            }

            MinMaxYearSet mmj = minMaxCalculation(
                inputInfo.getPersonId(),
                inputInfo.getRegistrationId(),
                inputregistrationYearMonthDday.getYear(),
                inputInfo.getRegistrationMainType(),
                inputInfo.getTypeDate(),
                inputInfo.getPersonRole(),
                inputInfo.getPersonAgeYear() );

            returnSet.setMinYear( mmj.GetMinYear() );
            returnSet.setMaxYear( mmj.GetMaxYear() );

            return returnSet;
        } // age is given in years


        // Check: Is birth year given?
        if( inputInfo.getPersonBirthYear() > 0 )           // birth year given
        {
            if( debug ) { showMessage( "birth year given: " + inputInfo.getPersonBirthYear() , false, true ); }

            // age is = regis jaar - birth year
            int birth_year = inputInfo.getPersonBirthYear();
            int regis_year = inputregistrationYearMonthDday.getYear();
            int AgeInYears = regis_year - birth_year;

            // Create new set
            DivideMinMaxDatumSet returnSet = new DivideMinMaxDatumSet();

            // Day no and month similar to regis date
            returnSet.setMaxDay(   inputregistrationYearMonthDday.getDay() );
            returnSet.setMaxMonth( inputregistrationYearMonthDday.getMonth() );
            returnSet.setMinDay(   inputregistrationYearMonthDday.getDay() );
            returnSet.setMinMonth( inputregistrationYearMonthDday.getMonth() );

            MinMaxYearSet mmj = minMaxCalculation(
                inputInfo.getPersonId(),
                inputInfo.getRegistrationId(),
                regis_year,
                inputInfo.getRegistrationMainType(),
                inputInfo.getTypeDate(),
                inputInfo.getPersonRole(),
                AgeInYears );

            returnSet.setMinYear(mmj.GetMinYear());
            returnSet.setMaxYear(mmj.GetMaxYear());

            return returnSet;
        } // birth year given

        // Check: Is it the deceased himself?
        if( inputInfo.getPersonRole() != 10 )           // not the deceased
        {
            if( debug ) { showMessage( "not the deceased, role: " + inputInfo.getPersonRole() , false, true ); }

            // Days, month, weeks to years, round up
            int ageinYears = roundUp(
                inputInfo.getPersonAgeYear(),
                inputInfo.getPersonAgeMonth(),
                inputInfo.getPersonAgeWeek(),
                inputInfo.getPersonAgeDay() );

            DivideMinMaxDatumSet returnSet = new DivideMinMaxDatumSet();            // New return set

            // day and month is similar to act date
            returnSet.setMaxDay(inputregistrationYearMonthDday.getDay());
            returnSet.setMaxMonth(inputregistrationYearMonthDday.getMonth());
            returnSet.setMinDay(inputregistrationYearMonthDday.getDay());
            returnSet.setMinMonth(inputregistrationYearMonthDday.getMonth());

            MinMaxYearSet mmj = minMaxCalculation(
                inputInfo.getPersonId(),
                inputInfo.getRegistrationId(),
                inputregistrationYearMonthDday.getYear(),
                inputInfo.getRegistrationMainType(),
                inputInfo.getTypeDate(),
                inputInfo.getPersonRole(),
                ageinYears );

            returnSet.setMinYear(mmj.GetMinYear());
            returnSet.setMaxYear(mmj.GetMaxYear());

            return returnSet;
        } // not the deceased


        // Check: combination of month days and weeks?
        int areMonths = 0;
        int areWeeks  = 0;
        int areDays   = 0;

        if( inputInfo.getPersonAgeMonth() > 0 ) { areMonths = 1; }
        if( inputInfo.getPersonAgeWeek()  > 0 ) { areWeeks  = 1; }
        if( inputInfo.getPersonAgeDay()   > 0 ) { areDays   = 1; }

        // TODO: ADDED
        // If marriage date, return 0-0-0
        if( inputInfo.getTypeDate().equalsIgnoreCase( "marriage_date" ) && ( (areMonths + areWeeks + areDays) > 0) )
        {
            DivideMinMaxDatumSet returnSet = new DivideMinMaxDatumSet();

            returnSet.setMaxDay(   0 );
            returnSet.setMaxMonth( 0 );
            returnSet.setMaxYear(  0 );
            returnSet.setMinDay(   0 );
            returnSet.setMinMonth( 0 );
            returnSet.setMinYear(  0 );

            return returnSet;
        }

        // TODO: added
        DateYearMonthDaySet inputDeathDate = LinksSpecific.divideCheckDate( inputInfo.getDeathDate() );

        int useYear;
        int useMonth;
        int useDay;

        if( inputDeathDate.isValidDate() )
        {
            useYear  = inputDeathDate.getYear();
            useMonth = inputDeathDate.getMonth();
            useDay   = inputDeathDate.getDay();
        }
        else
        {
            useYear  = inputregistrationYearMonthDday.getYear();
            useMonth = inputregistrationYearMonthDday.getMonth();
            useDay   = inputregistrationYearMonthDday.getDay();
        }

        if( ( areMonths + areWeeks + areDays ) >= 2 )         // at least 2 given
        {
            // weeks and months to days
            int dagen = inputInfo.getPersonAgeMonth() * 30;
            dagen += inputInfo.getPersonAgeWeek() * 7;

            // Date calculation

            // new date -> date - (days - 1)

            int mindays = (dagen - 1) * -1;
            int maxdays = (dagen + 1) * -1;

            // Min date
            String minDate = addTimeToDate(
                useYear,
                useMonth,
                useDay,
                TimeType.DAY,
                mindays);

            // Max date
            String maxDate = addTimeToDate(
                useYear,
                useMonth,
                useDay,
                TimeType.DAY,
                maxdays);

            // New date return return values
            DateYearMonthDaySet computedMinDate = LinksSpecific.divideCheckDate( minDate );
            DateYearMonthDaySet computedMaxDate = LinksSpecific.divideCheckDate( maxDate );

            // Checken if max date not later than actdate
            DateYearMonthDaySet dymd = checkMaxDate(
                computedMaxDate.getYear(),
                computedMaxDate.getMonth(),
                computedMaxDate.getDay(),
                useYear,
                useMonth,
                useDay);

            // returnen
            DivideMinMaxDatumSet returnSet = new DivideMinMaxDatumSet();

            returnSet.setMaxDay(   dymd.getDay() );
            returnSet.setMaxMonth( dymd.getMonth() );
            returnSet.setMaxYear(  dymd.getYear() );
            returnSet.setMinDay(   computedMinDate.getDay() );
            returnSet.setMinMonth( computedMinDate.getMonth() );
            returnSet.setMinYear(  computedMinDate.getYear() );

            return returnSet;
        }

        else if( areMonths == 1 )       // age in months given
        {
            // convert months
            int dagen = inputInfo.getPersonAgeMonth() * 30;

            // compute date
            // new date -> date - (days - 1)
            dagen++;

            int mindagen = (dagen + 14) * -1;
            int maxdagen = (dagen - 14) * -1;

            // Min date
            String minDate = addTimeToDate(
                useYear,
                useMonth,
                useDay,
                TimeType.DAY,
                mindagen);

            // Max date
            String maxDate = addTimeToDate(
                useYear,
                useMonth,
                useDay,
                TimeType.DAY,
                maxdagen);

            // New date to return values
            DateYearMonthDaySet computedMinDate = LinksSpecific.divideCheckDate( minDate );
            DateYearMonthDaySet computedMaxDate = LinksSpecific.divideCheckDate( maxDate );

            // returnen
            DivideMinMaxDatumSet returnSet = new DivideMinMaxDatumSet();

            returnSet.setMaxDay(   computedMaxDate.getDay() );
            returnSet.setMaxMonth( computedMaxDate.getMonth() );
            returnSet.setMaxYear(  computedMaxDate.getYear() );
            returnSet.setMinDay(   computedMinDate.getDay() );
            returnSet.setMinMonth( computedMinDate.getMonth() );
            returnSet.setMinYear(  computedMinDate.getYear() );

            return returnSet;
        }

        else if( areWeeks == 1 )             // age in weeks given
        {
            // weeks and months to days
            int days = inputInfo.getPersonAgeWeek() * 7;

            // compute date

            // new date -> date - (days - 1)
            days++;

            int mindays = (days + 4) * -1;
            int maxdays = (days - 4) * -1;

            // Min date
            String minDate = addTimeToDate(
                useYear,
                useMonth,
                useDay,
                TimeType.DAY,
                mindays);

            // Max datum
            String maxDate = addTimeToDate(
                useYear,
                useMonth,
                useDay,
                TimeType.DAY,
                maxdays);

            // date to return values
            DateYearMonthDaySet computedMinDate = LinksSpecific.divideCheckDate( minDate );
            DateYearMonthDaySet computedMaxDate = LinksSpecific.divideCheckDate( maxDate );

            // return
            DivideMinMaxDatumSet returnSet = new DivideMinMaxDatumSet();

            returnSet.setMaxDay(   computedMaxDate.getDay() );
            returnSet.setMaxMonth( computedMaxDate.getMonth() );
            returnSet.setMaxYear(  computedMaxDate.getYear() );
            returnSet.setMinDay(   computedMinDate.getDay() );
            returnSet.setMinMonth( computedMinDate.getMonth() );
            returnSet.setMinYear(  computedMinDate.getYear() );

            return returnSet;
        }

        else if( areDays == 1 )             // age in days given
        {
            // weeks and months to days
            int days = inputInfo.getPersonAgeDay();

            // new date -> date - (days - 1)

            int mindays = (days + 1) * -1;
            int maxdays = (days - 1) * -1;

            // min date
            String minDate = addTimeToDate(
                useYear,
                useMonth,
                useDay,
                TimeType.DAY,
                mindays);

            // max date
            String maxDate = addTimeToDate(
                useYear,
                useMonth,
                useDay,
                TimeType.DAY,
                maxdays);


            // New date to return value
            DateYearMonthDaySet computedMinDate = LinksSpecific.divideCheckDate( minDate );
            DateYearMonthDaySet computedMaxDate = LinksSpecific.divideCheckDate( maxDate );


            // Checken if max date niet later than actdate
            DateYearMonthDaySet dymd = checkMaxDate(
                computedMaxDate.getYear(),
                computedMaxDate.getMonth(),
                computedMaxDate.getDay(),
                useYear,
                useMonth,
                useDay);

            // return
            DivideMinMaxDatumSet returnSet = new DivideMinMaxDatumSet();

            returnSet.setMaxDay(   dymd.getDay() );
            returnSet.setMaxMonth( dymd.getMonth() );
            returnSet.setMaxYear(  dymd.getYear() );
            returnSet.setMinDay(   computedMinDate.getDay() );
            returnSet.setMinMonth( computedMinDate.getMonth() );
            returnSet.setMinYear(  computedMinDate.getYear() );

            return returnSet;
        }

        // No age given
        DivideMinMaxDatumSet returnSet = new DivideMinMaxDatumSet();

        // day and month similar to act date
        returnSet.setMaxDay(   inputregistrationYearMonthDday.getDay() );
        returnSet.setMaxMonth( inputregistrationYearMonthDday.getMonth() );
        returnSet.setMinDay(   inputregistrationYearMonthDday.getDay() );
        returnSet.setMinMonth( inputregistrationYearMonthDday.getMonth() );

        MinMaxYearSet mmj = minMaxCalculation(
            inputInfo.getPersonId(),
            inputInfo.getRegistrationId(),
            inputregistrationYearMonthDday.getYear(),
            inputInfo.getRegistrationMainType(),
            inputInfo.getTypeDate(),
            inputInfo.getPersonRole(),
            0 );

        returnSet.setMinYear( mmj.GetMinYear() );
        returnSet.setMaxYear( mmj.GetMaxYear() );

        return returnSet;
    } // minMaxDate


    /**
     * @param year
     * @param month
     * @param week
     * @param day
     * @return
     */
    public int roundUp(int year, int month, int week, int day)
    {
        int tempYear  = year;
        int tempMonth = month;
        int tempWeek  = week;

        // day to week
        if( day > 0 ) {
            tempWeek += (day / 7);

            if( (day % 7) != 0 ) { tempWeek++; }
        }
        week = tempWeek;

        // week to month
        if( week > 0 ) {
            tempMonth += (week / 4);

            if( (week % 4 ) != 0) { tempMonth++; }
        }

        month = tempMonth;

        // week to month
        if( month > 0 ) {
            tempYear += (month / 12);

            if( (month % 12 ) != 0) { tempYear++; }
        }

        return tempYear;
    } // roundUp


    /**
     * Use this function to add or subtract an amount of time from a date.
     *
     * @param year
     * @param month
     * @param day
     * @param tt
     * @param timeAmount
     * @return
     */
    private String addTimeToDate(
            int year,
            int month,
            int day,
            TimeType tt,
            int timeAmount)
    {

        Calendar c1 = Calendar.getInstance();       // new calendar instance

        c1.set( year, month, day );                 // set(int year, int month, int date)

        // Check of time type
        if( tt == tt.DAY ) {
            c1.add( Calendar.DAY_OF_MONTH, timeAmount );
        }
        else if( tt == tt.WEEK ) {
            c1.add( Calendar.WEEK_OF_MONTH, timeAmount );
        }
        else if( tt == tt.MONTH ) {
            c1.add( Calendar.MONTH, timeAmount );
        }
        else if( tt == tt.YEAR ) {
            c1.add( Calendar.YEAR, timeAmount );
        }

        // return new date
        String am = "" + c1.get( Calendar.DATE ) + "-" + c1.get( Calendar.MONTH ) + "-" + c1.get( Calendar.YEAR );

        return am;
    } // addTimeToDate


    /**
     * @param pYear
     * @param pMonth
     * @param pDay
     * @param rYear
     * @param rMonth
     * @param rDay
     * @return
     */
    private DateYearMonthDaySet checkMaxDate( int pYear, int pMonth, int pDay, int rYear, int rMonth, int rDay )
    {
        // year is greater than age year
        if (pYear > rYear) {

            //return akterdatum
            DateYearMonthDaySet dy = new DateYearMonthDaySet( "" );
            dy.setYear(rYear);
            dy.setMonth(rMonth);
            dy.setDay(rDay);
            return dy;

        } // lower, date is correct, return original date
        else if (pYear < rYear) {

            // return person date
            DateYearMonthDaySet dy = new DateYearMonthDaySet( "" );
            dy.setYear(pYear);
            dy.setMonth(pMonth);
            dy.setDay(pDay);
            return dy;

        }

        // years are equal, rest must be checked

        // month is higher than act month
        if (pMonth > rMonth) {

            // return return act month
            DateYearMonthDaySet dy = new DateYearMonthDaySet( "" );
            dy.setYear(rYear);
            dy.setMonth(rMonth);
            dy.setDay(rDay);
            return dy;

        } // month is correct, return original month
        else if (pMonth < rMonth) {

            // return return persons date
            DateYearMonthDaySet dy = new DateYearMonthDaySet( "" );
            dy.setYear(pYear);
            dy.setMonth(pMonth);
            dy.setDay(pDay);
            return dy;
        }

        // months are equal, check rest

        // day is higher than act day
        if (pDay > rDay) {

            // return act date
            DateYearMonthDaySet dy = new DateYearMonthDaySet( "" );
            dy.setYear(rYear);
            dy.setMonth(rMonth);
            dy.setDay(rDay);
            return dy;
        }

        // day is lower or similar to act day
        DateYearMonthDaySet dy = new DateYearMonthDaySet( "" );
        dy.setYear(pYear);
        dy.setMonth(pMonth);
        dy.setDay(pDay);
        return dy;
    } // checkMaxDate


}

// [eof]
