package modulemain;

import general.PrintLogger;

import java.io.PrintStream;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;

import java.sql.PreparedStatement;
import java.sql.ResultSet;

import java.text.SimpleDateFormat;
import java.time.LocalDateTime;

import java.util.Calendar;
import java.util.Set;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import javax.swing.JTextArea;
import javax.swing.JTextField;

import com.zaxxer.hikari.HikariDataSource;

import connectors.HikariConnection;

import dataset.Options;
import dataset.PersonC;
import dataset.TableToArrayListMultimapHikari;
import dataset.ThreadManager;

import general.Functions;

/**
 * @author Fons Laan
 *
 * FL-30-Sep-2019 Created
 * FL-28-Oct-2019
 */
public class LinksCleanAsync extends Thread
{
	//private ThreadManager tm;
	private Semaphore semaphore;

	private Options opts;

	private String source;
	private String rmtype;
	private PrintLogger plog;

	private JTextField guiLine;
	private JTextArea guiArea;

	private String endl = ". OK.";			// ".";
	private boolean showskip;

	// Reference Table -> ArrayListMultiMap
	private TableToArrayListMultimapHikari almmReport = null;   // Report warnings; read-only, so can remain file global
	//private TableToArrayListMultimap almmRole         = null;   // Role
	//private TableToArrayListMultimap almmCivilstatus  = null;   // Civilstatus & Gender
	//private TableToArrayListMultimap almmSex          = null;   // Civilstatus & Gender
	//private TableToArrayListMultimap almmMarriageYear = null;   // min/max marriage year
	//private TableToArrayListMultimap almmLitAge       = null;   // age_literal

	private boolean dbconref_single = true;     // true: same ref for reading and writing

	private String ref_url = "";			// reference db access
	private String ref_user = "";
	private String ref_pass = "";
	private String ref_db = "";

	String logTableName = "";

	private HikariDataSource dsLog;
	private HikariDataSource dsRefRead;
	private HikariDataSource dsRefWrite;
	private HikariDataSource dsOriginal;
	private HikariDataSource dsCleaned;

	private HikariConnection dbconLog      = null;
	private HikariConnection dbconRefRead  = null;
	private HikariConnection dbconRefWrite = null;
	private HikariConnection dbconOriginal = null;
	private HikariConnection dbconCleaned  = null;

	private final static String SC_U = "u"; // Unknown Standard value assigned (although the original value is not valid)
	private final static String SC_X = "x"; //    X    Standard yet to be assigned
	private final static String SC_N = "n"; //    No   standard value assigned (original value is not valid)
	private final static String SC_Y = "y"; //    Yes  Standard value assigned (original value is valid)

	boolean use_links_logs = true;
	private int count_step = 10000;             // used to be 1000


public LinksCleanAsync
	(
		//ThreadManager tm,
		Semaphore semaphore,

		Options opts,
		JTextField guiLine,
		JTextArea guiArea,
		String source,
		String rmtype,
		boolean showskip,

		String ref_url,
		String ref_db,
		String ref_user,
		String ref_pass,

		String logTableName,

		HikariDataSource dsLog,
		HikariDataSource dsRefRead,
		HikariDataSource dsRefWrite,
		HikariDataSource dsOriginal,
		HikariDataSource dsCleaned
	)
	{
		//this.tm = tm;
		this.semaphore = semaphore;

		this.opts = opts;
		this.guiLine = guiLine;
		this.guiArea = guiArea;
		this.source = source;
		this.rmtype = rmtype;
		this.plog = opts.getLogger();
		this.showskip = showskip;

		this.ref_url  = ref_url;
		this.ref_db   = ref_db;
		this.ref_user = ref_user;
		this.ref_pass = ref_pass;

		this.logTableName = logTableName;

		this.dsLog      = dsLog;
		this.dsRefRead  = dsRefRead;
		this.dsRefWrite = dsRefWrite;
		this.dsOriginal = dsOriginal;
		this.dsCleaned  = dsCleaned;
	}


	@Override
	public void run()
	{
		// If you want the actual CPU time of the current thread (or indeed, any arbitrary thread) rather than
		// the wall clock time then you can get this via ThreadMXBean. Basically, do this at the start:
		ThreadMXBean threadMXB = ManagementFactory.getThreadMXBean();
		threadMXB.setThreadCpuTimeEnabled( true );

		long threadStart = System.currentTimeMillis();			// clock time
		long threadId    = Thread.currentThread().getId();

		try
		{
			long nanoseconds_begin  = ManagementFactory.getThreadMXBean().getThreadCpuTime( Thread.currentThread().getId() );
			long milliseconds_begin = TimeUnit.SECONDS.convert( nanoseconds_begin, TimeUnit.MILLISECONDS );

			//String format = "HH:mm:ss";
			String format = "yyyy.MM.dd HH:mm:ss";
			String ts = getTimeStamp2( format );
			System.out.println( ts ); plog.show( "" );

			String msg = String.format("Thread id %02d; LinksCleanAsync/run(): running for source %s, rmtype %s", threadId, source, rmtype);
			plog.show(msg);
			showMessage(msg, false, true);

			// database connections
			dbconRefWrite = new HikariConnection( dsRefWrite.getConnection() );
			if( dbconref_single )			// same connector for reading and writing
			{ dbconRefRead = dbconRefWrite; }
			else							// separate connector for reading
			{ dbconRefRead  = new HikariConnection( dsRefRead.getConnection() ); }

			dbconLog      = new HikariConnection( dsLog.getConnection() );
			dbconOriginal = new HikariConnection( dsOriginal.getConnection() );
			dbconCleaned  = new HikariConnection( dsCleaned.getConnection() );

			dbconCleaned.showMetaData( "dbconCleaned" );

			// links_general.ref_report contains about 75 error definitions, to be used when the normalization encounters errors
			showMessage(String.format( "Thread id %02d; Loading report table", threadId), false, true );
			almmReport = new TableToArrayListMultimapHikari( dbconRefRead, "ref_report", "type", null );


			doRefreshData( opts.isDbgRefreshData(), opts.isDoRefreshData(), source, rmtype );                     // GUI cb: Remove previous data

			doPrepieceSuffix(opts.isDbgPrepieceSuffix(), opts.isDoPrepieceSuffix(), source, rmtype);      // GUI cb: Prepiece, Suffix
			/*
			doFirstnames(opts.isDbgFirstnames(), opts.isDoFirstnames(), source, rmtype);                  // GUI cb: Firstnames

			doFamilynames(opts.isDbgFamilynames(), opts.isDoFamilynames(), source, rmtype);               // GUI cb: Familynames

			doLocations(opts.isDbgLocations(), opts.isDoLocations(), source, rmtype);                     // GUI cb: Locations

			doStatusSex(opts.isDbgStatusSex(), opts.isDoStatusSex(), source, rmtype);                     // GUI cb: Status and Sex

			doRegistrationType(opts.isDbgRegType(), opts.isDoRegType(), source, rmtype);                  // GUI cb: Registration Type

			doOccupation(opts.isDbgOccupation(), opts.isDoOccupation(), source, rmtype);                  // GUI cb: Occupation

			doAge(opts.isDbgAge(), opts.isDoAge(), source, rmtype);                                       // GUI cb: Age, Role,Dates

			doRole(opts.isDbgRole(), opts.isDoRole(), source, rmtype);                                    // GUI cb: Age, Role, Dates

			// doDates1(): all other datesfunctions
			doDates1(opts.isDbgDates(), opts.isDoDates(), source, rmtype);                                // GUI cb: Age, Role, Dates
			// doDates2(): only minMaxDateMain()
			doDates2(opts.isDbgDates(), opts.isDoDates(), source, rmtype);                                // GUI cb: Age, Role, Dates

			doMinMaxMarriage(opts.isDbgMinMaxMarriage(), opts.isDoMinMaxMarriage(), source, rmtype);      // GUI cb: Min Max Marriage

			doPartsToFullDate(opts.isDbgPartsToFullDate(), opts.isDoPartsToFullDate(), source, rmtype);   // GUI cb: Parts to Full Date

			doDaysSinceBegin(opts.isDbgDaysSinceBegin(), opts.isDoDaysSinceBegin(), source, rmtype);      // GUI cb: Days since begin

			doPostTasks(opts.isDbgPostTasks(), opts.isDoPostTasks(), source, rmtype);                     // GUI cb: Post Tasks

			doFlagRegistrations(opts.isDbgFlagRegistrations(), opts.isDoFlagRegistrations(), source, rmtype);   // GUI cb: Remove Duplicate Reg's

			doFlagPersonRecs(opts.isDbgFlagPersons(), opts.isDoFlagPersons(), source, rmtype);   // GUI cb: Remove Empty Role Reg's

			doScanRemarks(opts.isDbgScanRemarks(), opts.isDoScanRemarks(), source, rmtype);                           // GUI cb: Scan Remarks
			*/

			//if( dbconCleaned != null ) { dbconCleaned.close(); dbconCleaned = null; }


			// Close db connections
			// Close db connections
			if( dbconLog      != null ) { dbconLog.close();      dbconLog      = null; }
			if( dbconRefRead  != null ) { dbconRefRead.close();  dbconRefRead  = null; }
			if( dbconRefWrite != null ) { dbconRefWrite.close(); dbconRefWrite = null; }
			if( dbconOriginal != null ) { dbconOriginal.close(); dbconOriginal = null; }
			if( dbconCleaned  != null ) { dbconCleaned.close();  dbconCleaned  = null; }

			msg = String.format( "Thread id %02d; clock time", threadId );
			elapsedShowMessage( msg, threadStart, System.currentTimeMillis() );

			long cpuTimeNsec  = threadMXB.getCurrentThreadCpuTime();   // elapsed CPU time for current thread in nanoseconds
			long cpuTimeMsec  = TimeUnit.NANOSECONDS.toMillis( cpuTimeNsec );
			msg = String.format( "Thread id %02d; thread time", threadId );
			elapsedShowMessage( msg, 0, cpuTimeMsec );

			//long userTimeNsec = thx.getCurrentThreadUserTime();  // elapsed user time in nanoseconds
			//long userTimeMsec = TimeUnit.NANOSECONDS.toMillis( userTimeNsec );
			//elapsedShowMessage( "user m time elapsed", 0, userTimeMsec );   // user mode part of thread time

			System.gc();    // request Garbage Collection
		}
		catch( Exception ex ) {
			String msg = String.format( "Thread id %02d; Exception: %s", threadId, ex.getMessage());
			showMessage(msg, false, true);
			ex.printStackTrace(new PrintStream(System.out));
		}

		String msg = String.format("Thread id %02d; Cleaning source %s is done", threadId, source);
		showTimingMessage(msg, threadStart);
		System.out.println(msg);

		LocalDateTime timePoint = LocalDateTime.now();  // The current date and time
		msg = String.format("Thread id %02d; current time: %s", threadId, timePoint.toString());
		showMessage(msg, false, true);

		//int count = tm.removeThread();
		//msg = String.format("Thread id %02d; Remaining cleaning threads: %d\n", threadId, count);
		//showMessage(msg, false, true);
		//System.out.println(msg);
		semaphore.release();
		int npermits = semaphore.availablePermits();
		msg = String.format( "Thread id %02d; Semaphore: # of permits: %d", threadId, npermits );
		showMessage(msg, false, true);
		System.out.println( msg );

	} // run


	/**
	 * @param format
	 * @return
	 */
	public static String getTimeStamp2( String format ) {
		Calendar cal = Calendar.getInstance();
		SimpleDateFormat sdf = new SimpleDateFormat( format );
		return sdf.format( cal.getTime() );
	}


	/**
	 * @param logText
	 * @param isMinOnly
	 * @param newLine
	 */
	private void showMessage(String logText, boolean isMinOnly, boolean newLine)
	{
		guiLine.setText( logText );

		if( !isMinOnly ) {
			String newLineToken = "";
			if( newLine ) {
				newLineToken = "\r\n";
			}

			// prefix string with timestamp
			if( logText != endl ) {
				String ts = LinksSpecific.getTimeStamp2( "HH:mm:ss" );

				guiArea.append( ts + " " );
				// System.out.printf( "%s ", ts );
				//logger.info( logText );
				try { plog.show( logText ); }
				catch( Exception ex ) {
					System.out.println( ex.getMessage() );
					ex.printStackTrace( new PrintStream( System.out ) );
				}
			}

			guiArea.append( logText + newLineToken );
			//System.out.printf( "%s%s", logText, newLineToken );
			try { plog.show( logText ); }
			catch( Exception ex ) {
				System.out.println( ex.getMessage() );
				ex.printStackTrace( new PrintStream( System.out ) );
			}
		}
	} // showMessage


	/**
	 */
	private void showMessage_nl()
	{
		String newLineToken = "\r\n";

		guiArea.append( newLineToken );

		try { plog.show( "" ); }
		catch( Exception ex ) {
			System.out.println( ex.getMessage() );
			ex.printStackTrace( new PrintStream( System.out ) );
		}
	} // showMessage_nl


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
	 * @param id
	 * @param id_source
	 * @param errorCode
	 * @param value
	 * @throws Exception
	 */
	private void addToReportRegistration( int id, String id_source, int errorCode, String value )
	{
		if( ! use_links_logs ) { return; }

		boolean debug = false;
		if( debug ) { showMessage( "addToReportRegistration()", false, true ); }

		String errorCodeStr = Integer.toString( errorCode );

		String report_class   = almmReport.value( "class",   errorCodeStr );
		String report_content = almmReport.value( "content", errorCodeStr );
		if( debug ) { System.out.println( "value: " + value + ", report_class: " + report_class + ", report_content: " + report_content ); }

		// A $ in value disrupts the replaceAll regex
		value = java.util.regex.Matcher.quoteReplacement( value );

		// replace recognition substring with the value
		report_content = report_content.replaceAll( "<.*>", value );
		if( debug ) { System.out.println( "report_class: " + report_class + ", report_content: " + report_content ); }

		// get registration values from links_original.registration_o
		String location  = "";
		String reg_type  = "";
		String date      = "";
		String sequence  = "";
		String guid      = "";

		String selectQuery = "SELECT registration_location , registration_type , registration_date , registration_seq , id_persist_registration"
			+ " FROM registration_o WHERE id_registration = " + id;

		if( debug ) {
			System.out.println( selectQuery );
			showMessage( selectQuery, false, true );
		}

		try
		{
			ResultSet rs = dbconOriginal.runQueryWithResult( selectQuery );
			rs.first();
			location = rs.getString( "registration_location" );
			reg_type = rs.getString( "registration_type" );
			date     = rs.getString( "registration_date" );
			sequence = rs.getString( "registration_seq" );
			guid     = rs.getString( "id_persist_registration" );
			rs.close();
		}
		catch( Exception ex )
		{
			showMessage( selectQuery, false, true );
			showMessage( ex.getMessage(), false, true );
			ex.printStackTrace( new PrintStream( System.out ) );
		}

		if( location == null) { location = ""; }
		if( reg_type == null) { reg_type = ""; }
		if( date     == null) { date     = ""; }
		if( sequence == null) { sequence = ""; }
		if( guid     == null) { guid     = ""; }

		String insertQuery = "INSERT INTO links_logs.`" + logTableName + "`"
			+ " ( reg_key , id_source , report_class , report_type , content ,"
			+ " date_time , location , reg_type , date , sequence , guid )"
			+ " VALUES ( ? , ? , ? , ? , ? , NOW() , ? , ? , ? , ? , ? ) ;";

		try( PreparedStatement pstmt = dbconLog.prepareStatement( insertQuery ) )
		{
			int i = 1;
			pstmt.setInt(    i++, id );
			pstmt.setString( i++, id_source );
			pstmt.setString( i++, report_class.toUpperCase() );
			pstmt.setString( i++, errorCodeStr );
			pstmt.setString( i++, report_content );
			pstmt.setString( i++, location );
			pstmt.setString( i++, reg_type );
			pstmt.setString( i++, date );
			pstmt.setString( i++, sequence );
			pstmt.setString( i++, guid );

			int numRowsChanged = pstmt.executeUpdate();
			pstmt.close();
		}
		catch( Exception ex ) {
			showMessage( "source: " + id_source + ", query: " + insertQuery, false, true );
			showMessage( ex.getMessage(), false, true );
			ex.printStackTrace();
		}

	} // addToReportRegistration


	/**
	 * @param id
	 * @param id_source
	 * @param errorCode
	 * @param value
	 * @throws Exception
	 */
	private void addToReportPerson( int id, String id_source, int errorCode, String value )
	{
		if( ! use_links_logs ) { return; }

		boolean debug = false;
		if( debug ) { showMessage( "addToReportPerson()", false, true ); }

		String errorCodeStr = Integer.toString( errorCode );

		String report_class   = almmReport.value( "class",   errorCodeStr );
		String report_content = almmReport.value( "content", errorCodeStr );
		if( debug ) { System.out.println( "value: " + value + ", report_class: " + report_class + ", report_content: " + report_content ); }

		// A $ in value disrupts the replaceAll regex
		value = java.util.regex.Matcher.quoteReplacement( value );

		// replace recognition substring with the value
		report_content = report_content.replaceAll( "<.*>", value );
		if( debug ) { System.out.println( "report_class: " + report_class + ", report_content: " + report_content ); }

		// get person values from links_original.person_o
		String id_registration = "";
		String role            = "";

		String selectQueryP = "SELECT id_registration, role FROM person_o WHERE id_person = " + id;

		if( debug ) {
			System.out.println( selectQueryP );
			showMessage( selectQueryP, false, true );
		}

		try
		{
			ResultSet rs = dbconOriginal.runQueryWithResult(selectQueryP);
			rs.first();
			id_registration = rs.getString( "id_registration" );
			role            = rs.getString( "role" );
			rs.close();
		}
		catch( Exception ex )
		{
			showMessage( selectQueryP, false, true );
			showMessage( ex.getMessage(), false, true );
			ex.printStackTrace( new PrintStream( System.out ) );
		}

		if( id_registration == null) { id_registration = ""; }
		if( role            == null) { role = ""; }

		// get registration values from links_original.registration_o
		String location  = "";
		String reg_type  = "";
		String date      = "";
		String sequence  = "";
		String guid      = "";

		if( ! id_registration.isEmpty() )
		{
			String selectQueryR = "SELECT registration_location , registration_type , registration_date , registration_seq , id_persist_registration"
				+ " FROM registration_o WHERE id_registration = " + id_registration;

			if( debug ) { showMessage( selectQueryR, false, true ); }

			try
			{
				ResultSet rs = dbconOriginal.runQueryWithResult(selectQueryR);
				rs.first();
				location = rs.getString( "registration_location" );
				reg_type = rs.getString( "registration_type" );
				date     = rs.getString( "registration_date" );
				sequence = rs.getString( "registration_seq" );
				guid     = rs.getString( "id_persist_registration" );
				rs.close();
			}
			catch( Exception ex )
			{
				showMessage( selectQueryR, false, true );
				showMessage( ex.getMessage(), false, true );
				ex.printStackTrace( new PrintStream( System.out ) );
			}
		}

		if( location == null) { location = ""; }
		if( reg_type == null) { reg_type = ""; }
		if( date     == null) { date     = ""; }
		if( sequence == null) { sequence = ""; }
		if( guid     == null) { guid     = ""; }


		// to prevent: Data truncation: Data too long for column 'sequence'
		if( ! sequence.isEmpty() && sequence.length() > 20 )
		{ sequence = sequence.substring( 0, 20 ); }

		String insertQuery = "INSERT INTO links_logs.`" + logTableName + "`"
			+ " ( pers_key , id_source , report_class , report_type , content ,"
			+ " date_time , location , reg_type , date , sequence , role, reg_key, guid )"
			+ " VALUES ( ? , ? , ? , ? , ? , NOW() , ? , ? , ? , ? , ? , ?, ? ) ;";

		try( PreparedStatement pstmt = dbconLog.prepareStatement( insertQuery ) )
		{
			int i = 1;
			pstmt.setInt(    i++, id );
			pstmt.setString( i++, id_source );
			pstmt.setString( i++, report_class.toUpperCase() );
			pstmt.setString( i++, errorCodeStr );
			pstmt.setString( i++, report_content );
			pstmt.setString( i++, location );
			pstmt.setString( i++, reg_type );
			pstmt.setString( i++, date );
			pstmt.setString( i++, sequence );
			pstmt.setString( i++, role );
			pstmt.setString( i++, id_registration );
			pstmt.setString( i++, guid );

			int numRowsChanged = pstmt.executeUpdate();
			pstmt.close();
		}
		catch( Exception ex ) {
			showMessage( "source: " + id_source + ", query: " + insertQuery, false, true );
			showMessage( ex.getMessage(), false, true );
			ex.printStackTrace();
		}

	} // addToReportPerson

	/*---< End Helper functions >---------------------------------------------*/

	/*===< functions corresponding to GUI Cleaning options >==================*/

	/*---< Remove previous data >---------------------------------------------*/

	/**
	 * Remove previous data from links_cleaned,
	 * and then copy keys from links_original
	 * @param debug
	 * @param go
	 * @param source
	 * @param rmtype
	 * @throws Exception
	 */
	private void doRefreshData( boolean debug, boolean go, String source, String rmtype )
	throws Exception
	{
		long threadId = Thread.currentThread().getId();

		String funcname = String.format( "Thread id %02d; doRefreshData for source: %s, rmtype: %s (debug: %s)", threadId, source, rmtype, debug );

		if( !go ) {
			if( showskip ) { showMessage( "Skipping " + funcname, false, true ); }
			return;
		}

		long timeStart = System.currentTimeMillis();
		showMessage( funcname + " ...", false, true );

		// Delete cleaned data for given source
		String deleteRegist = String.format( "DELETE FROM registration_c WHERE id_source = %s", source );
		String deletePerson = String.format( "DELETE FROM person_c WHERE id_source = %s", source );

		String msg;
		if( rmtype.isEmpty() )
		{ msg = String.format( "Thread id %02d; Deleting previous cleaned data for source: %s", threadId, source ); }
		else
		{
			deleteRegist += String.format( " AND registration_maintype = %s", rmtype );
			deletePerson += String.format( " AND registration_maintype = %s", rmtype );

			msg = String.format( "Thread id %02d; Deleting previous data for source: %s and rmtype: %s", threadId, source, rmtype );
		}

		showMessage( msg, false, true );
		if( debug ) {
			showMessage( deleteRegist, false, true );
			showMessage( deletePerson, false, true );
		}

		int delRegistCCount = dbconCleaned.runQueryUpdate( deleteRegist );
		int delPersonCCount = dbconCleaned.runQueryUpdate( deletePerson );
		msg = String.format( "Thread id %02d; %d records deleted from registration_c", threadId, delRegistCCount );
		showMessage( msg, false, true );
		msg = String.format( "Thread id %02d; %d records deleted from person_c", threadId, delPersonCCount );
		showMessage( msg, false, true );

		// if links_cleaned is now empty, we reset the AUTO_INCREMENT
		// that eases comparison with links_a2a tables
		String qRegistCCount = "SELECT COUNT(*) FROM registration_c";
		String qPersonCCount = "SELECT COUNT(*) FROM person_c";

		ResultSet rsR = dbconCleaned.runQueryWithResult( qRegistCCount );
		rsR.first();
		int registCCount = rsR.getInt("COUNT(*)" );
		rsR.close();


		ResultSet rsP = dbconCleaned.runQueryWithResult( qPersonCCount );
		rsP.first();
		int personCCount = rsP.getInt( "COUNT(*)" );
		rsP.close();

		if( registCCount == 0 && personCCount == 0 )
		{
			msg = String.format( "Thread id %02d; Resetting AUTO_INCREMENTs for links_cleaned", threadId );
			showMessage( msg, false, true );
			String auincRegist = "ALTER TABLE registration_c AUTO_INCREMENT = 1";
			String auincPerson = "ALTER TABLE person_c AUTO_INCREMENT = 1";

			dbconCleaned.runQueryUpdate( auincRegist );
			dbconCleaned.runQueryUpdate( auincPerson );
		}
		else
		{
			msg = String.format( "Thread id %02d; %d records in registration_c", threadId, registCCount );
			showMessage( msg, false, true );
			msg = String.format( "Thread id %02d; %d records in person_c", threadId, personCCount );
			showMessage( msg, false, true );
		}

		/*
		// Copy key column data from links_original to links_cleaned
		String keysRegistration = ""
			+ "INSERT INTO links_cleaned.registration_c"
			+ " ( id_registration, id_source, id_persist_registration, id_orig_registration, registration_maintype, registration_seq )"
			+ " SELECT id_registration, id_source, id_persist_registration, id_orig_registration, registration_maintype, registration_seq"
			+ " FROM links_original.registration_o"
			+ " WHERE registration_o.id_source = " + source;

		if( ! rmtype.isEmpty() )
		{ keysRegistration += String.format( " AND registration_maintype = %s", rmtype ); }

		msg = String.format( "Thread id %02d; Copying links_original registration keys to links_cleaned", threadId );
		showMessage( msg, false, true );
		if( debug ) { showMessage( keysRegistration, false, true ); }

		dbconCleaned.runQuery( keysRegistration );

		// Strip {} from id_persist_registration
		String Update_id_persist_registration = ""
			+ "UPDATE links_cleaned.registration_c"
			+ " SET id_persist_registration = SUBSTR(id_persist_registration, 2, 36)"
			+ " WHERE registration_c.id_source = " + source;

		Update_id_persist_registration += " AND LENGTH(id_persist_registration) = 38";      // only CBG data have this string

		if( ! rmtype.isEmpty() )
		{ Update_id_persist_registration += String.format( " AND registration_maintype = %s", rmtype ); }

		msg = String.format( "Thread id %02d; Removing {} from id_persist_registration", threadId );
		showMessage( msg, false, true );
		if( debug ) { showMessage( Update_id_persist_registration, false, true ); }

		dbconCleaned.runQuery( Update_id_persist_registration );

		String keysPerson = ""
			+ "INSERT INTO links_cleaned.person_c"
			+ " ( id_person, id_registration, id_source, registration_maintype, id_person_o )"
			+ " SELECT id_person, id_registration, id_source, registration_maintype, id_person_o"
			+ " FROM links_original.person_o"
			+ " WHERE person_o.id_source = " + source;

		if( ! rmtype.isEmpty() )
		{ keysPerson += String.format( " AND registration_maintype = %s", rmtype ); }

		msg = String.format( "Thread id %02d; Copying links_original person keys to links_cleaned", threadId );
		showMessage( msg, false, true );
		if( debug ) { showMessage( keysPerson, false, true ); }

		dbconCleaned.runQuery( keysPerson );
		*/
		elapsedShowMessage( funcname, timeStart, System.currentTimeMillis() );
		showMessage_nl();
	} // doRefreshData


	/*---< First- and Familynames >-------------------------------------------*/

	/**
	 * Prepiece, Suffix
	 * @param go
	 * @throws Exception
	 */
	private void doPrepieceSuffix( boolean debug, boolean go, String source, String rmtype ) throws Exception
	{
		long threadId = Thread.currentThread().getId();

		String funcname = String.format( "Thread id %02d; doPrepieceSuffix for source: %s, rmtype: %s", threadId, source, rmtype );

		if( !go ) {
			if( showskip ) { showMessage( "Skipping " + funcname, false, true ); }
			return;
		}

		long timeStart = System.currentTimeMillis();
		showMessage( funcname + " ...", false, true );

		long start = System.currentTimeMillis();
		//MySqlConnector dbconnRefR = new MySqlConnector( ref_url, ref_db, ref_user, ref_pass );

		TableToArrayListMultimapHikari almmPrepiece = new TableToArrayListMultimapHikari( dbconRefRead, "ref_prepiece", "original", "prefix" );
		TableToArrayListMultimapHikari almmSuffix   = new TableToArrayListMultimapHikari( dbconRefRead, "ref_suffix",   "original", "standard" );
		//TableToArrayListMultimap almmAlias    = new TableToArrayListMultimap( dbconnRefR, "ref_alias",    "original",  null );

		//dbconnRefR.close();
		showTimingMessage( String.format( "Thread id %02d; Loaded Prepiece/Suffix/Alias reference tables", threadId ), start );

		int numrows = almmPrepiece.numrows();
		int numkeys = almmPrepiece.numkeys();
		showMessage( String.format( "Thread id %02d; Number of rows in reference table ref_prepiece: %d", threadId, numrows ), false, true );
		if( numrows != numkeys )
		{ showMessage( String.format( "Thread id %02d; Number of keys in arraylist multimap: %d", threadId, numkeys ), false, true ); }

		numrows = almmSuffix.numrows();
		numkeys = almmSuffix.numkeys();
		showMessage( String.format( "Thread id %02d; Number of rows in reference table ref_suffix: %d", threadId, numrows ), false, true );
		if( numrows != numkeys )
		{ showMessage( String.format( "Thread id %02d; Number of keys in arraylist multimap: %d", threadId, numkeys ), false, true ); }

		showMessage( String.format( "Thread id %02d; standardPrepiece", threadId ), false, true );
		standardPrepiece( debug, almmPrepiece, source, rmtype );

		showMessage( String.format( "Thread id %02d; standardSuffix", threadId ), false, true );
		standardSuffix( debug, almmSuffix, source, rmtype );

		//MySqlConnector dbconnRefW = new MySqlConnector( ref_url, ref_db, ref_user, ref_pass );

		// Wait until we can update
		while( almmPrepiece.isBusy().get() ) {
			plog.show( "No permission to update ref_prepiece: Waiting 60 seconds" );
			Thread.sleep( 60000 );
		}
		int newcount = almmPrepiece.newcount();
		showMessage( String.format( "Thread id %02d; New entries for ref table prepiece: %d", threadId, newcount ), false, true );
		almmPrepiece.updateTable( dbconRefWrite );

		while( almmSuffix.isBusy().get() ) {
			plog.show( "No permission to update ref table: Waiting 60 seconds" );
			Thread.sleep( 60000 );
		}
		newcount = almmSuffix.newcount();
		showMessage( String.format( "Thread id %02d; New entries for ref table suffix: %d", threadId, newcount ), false, true );
		almmSuffix.updateTable( dbconRefWrite );

		// almmAlias.updateTable( dbconRefWrite );         // almmAlias.add() never called; nothing added to almmAlias

		//dbconnRefW.close();

		almmPrepiece.free();
		almmSuffix.free();
		//almmAlias.free();
		showMessage( String.format( "Thread id %02d; Freed almmPrepiece/almmSuffix", threadId ), false, true );

		elapsedShowMessage( funcname, timeStart, System.currentTimeMillis() );
		showMessage_nl();
	} // doPrepieceSuffix


	/**
	 * @param source
	 * @throws Exception
	 */
	public void standardPrepiece( boolean debug, TableToArrayListMultimapHikari almmPrepiece, String source, String rmtype )
	{
		long threadId = Thread.currentThread().getId();
		int count = 0;
		int stepstate = count_step;

		try
		{
			String selectQuery = "SELECT id_person , prefix FROM person_o WHERE id_source = " + source + " AND prefix <> ''";
			if ( ! rmtype.isEmpty() ) { selectQuery += " AND registration_maintype = " + rmtype; }
			if( debug ) { showMessage( "standardPrepiece: " + selectQuery, false, true ); }

			ResultSet rs = dbconOriginal.runQueryWithResult( selectQuery );
			rs.last();
			int total = rs.getRow();
			rs.beforeFirst();

			while( rs.next() )
			{
				count++;
				if( count == stepstate ) {
					long pct = Math.round( 100.0 * (float)count / (float)total );
					String msg = String.format( "Thread id %02d, standardPrepiece, %d-of-%d (%d%%)", threadId, count, total, pct );
					showMessage( msg, true, true );
					stepstate += count_step;
				}

				// Create lists
				String listPF = "";
				String listTO = "";
				String listTN = "";

				int id_person   = rs.getInt( "id_person" );
				String prepiece = rs.getString( "prefix" ).toLowerCase();

				prepiece = cleanName( debug, source, id_person, prepiece );

				String[] prefixes = prepiece.split( " " );

				for( String part : prefixes )
				{
					// Does Prefix exist in ref table
					if( almmPrepiece.contains( part ) )
					{
						String standard_code = almmPrepiece.code( part );
						String prefix        = almmPrepiece.value( "prefix",      part );
						String title_noble   = almmPrepiece.value( "title_noble", part );
						String title_other   = almmPrepiece.value( "title_other", part );

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

						almmPrepiece.add( part );               // Add Prefix

						listPF += part + " ";                   // Add to list
					}
				}

				// write lists to person_c
				if( !listTN.isEmpty() ) {
					dbconCleaned.runQueryUpdate( PersonC.updateQuery( "title_noble", listTN.substring( 0, ( listTN.length() - 1 ) ), id_person ) );
				}

				if( !listTO.isEmpty() ) {
					dbconCleaned.runQueryUpdate( PersonC.updateQuery( "title_other", listTO.substring( 0, ( listTO.length() - 1 ) ), id_person ) );
				}

				if( !listPF.isEmpty() ) {
					dbconCleaned.runQueryUpdate( PersonC.updateQuery( "prefix", listPF.substring( 0, ( listPF.length() - 1 ) ), id_person) );
				}
			}
		}
		catch( Exception ex ) {
			String msg = String.format( "Thread id %02d; count: %d, Exception while cleaning Prepiece: %s", threadId, count, ex.getMessage() );
			showMessage( msg, false, true );
			ex.printStackTrace( new PrintStream( System.out ) );
		}
	} // standardPrepiece


	/**
	 * @param source
	 */
	public void standardSuffix( boolean debug, TableToArrayListMultimapHikari almmSuffix, String source, String rmtype )
	{
		long threadId = Thread.currentThread().getId();
		int count = 0;
		int stepstate = count_step;

		try
		{
			String selectQuery = "SELECT id_person , suffix FROM person_o WHERE id_source = " + source + " AND suffix <> ''";
			if ( ! rmtype.isEmpty() ) { selectQuery += " AND registration_maintype = " + rmtype; }
			if( debug ) { showMessage( "standardSuffix: " + selectQuery, false, true ); }

			ResultSet rs = dbconOriginal.runQueryWithResult( selectQuery );
			rs.last();
			int total = rs.getRow();
			rs.beforeFirst();

			while( rs.next() )
			{
				count++;
				if( count == stepstate ) {
					long pct = Math.round( 100.0 * (float)count / (float)total );
					String msg = String.format( "Thread id %02d, standardSuffix, %d-of-%d (%d%%)", threadId, count, total, pct );
					showMessage( msg, true, true );
					stepstate += count_step;
				}

				int id_person = rs.getInt( "id_person" );
				String suffix = rs.getString( "suffix" ).toLowerCase();

				suffix = cleanName( debug, source, id_person, suffix );

				// Check occurrence in ref table
				if( almmSuffix.contains( suffix ) )
				{
					String standard_code = almmSuffix.code( suffix );

					if( standard_code.equals( SC_X ) )
					{
						addToReportPerson(id_person, source, 71, suffix);     // EC 71

						String query = PersonC.updateQuery( "suffix", suffix, id_person );
						dbconCleaned.runQueryUpdate( query );
					}
					else if( standard_code.equals( SC_N ) )
					{
						addToReportPerson( id_person, source, 73, suffix );   // EC 73
					}
					else if( standard_code.equals( SC_U ) )
					{
						addToReportPerson( id_person, source, 75, suffix );   // EC 74

						String query = PersonC.updateQuery( "suffix", suffix, id_person );
						dbconCleaned.runQueryUpdate( query );
					}
					else if( standard_code.equals( SC_Y ) )
					{
						String query = PersonC.updateQuery( "suffix", suffix, id_person );
						dbconCleaned.runQueryUpdate( query );
					}
					else {
						addToReportPerson(id_person, source, 79, suffix);     // EC 75
					}
				}
				else // Standard code x
				{
					addToReportPerson( id_person, source, 71, suffix);        // EC 71

					almmSuffix.add( suffix );

					String query = PersonC.updateQuery( "suffix", suffix, id_person );
					dbconCleaned.runQueryUpdate( query );

				}
			}
		}
		catch( Exception ex ) {
			String msg = String.format( "Thread id %02d; count: %d, Exception while cleaning Suffix: %s", threadId, count, ex.getMessage() );
			showMessage( msg, false, true );
			ex.printStackTrace( new PrintStream( System.out ) );
		}
	} // standardSuffix


	/**
	 * @param id
	 * @param source
	 * @param name
	 * @return
	 */
	private String standardAlias( boolean debug, TableToArrayListMultimapHikari almmAlias, int id, String source, String name, int errCode )
		throws Exception
	{
		name = name.toLowerCase();
		Set< String > keys = almmAlias.keySet();

		for( String keyword : keys )
		{
			if( name.contains( " " + keyword + " " ) )
			{
				addToReportPerson( id, source, errCode, name );      // firstname: EC 1107, familyname: EC 1007

				// prepare on braces
				if( keyword.contains( "\\(" ) || keyword.contains( "\\(" ) ) {
					keyword = keyword.replaceAll( "\\(", "" ).replaceAll( "\\)", "" );
				}

				String[] names = name.toLowerCase().split( keyword, 2 );

				// we must clean the name because of the braces used in aliases
				// Set alias
				String clean = cleanName( debug, source, id, names[ 1 ] );
				PersonC.updateQuery( "alias", LinksSpecific.funcCleanSides( clean ), id );

				clean = cleanName( debug, source, id, names[ 0 ] );
				return LinksSpecific.funcCleanSides( clean );
			}
		}

		return name;
	} // standardAlias


	/**
	 * @param name
	 * @return
	 */
	private String cleanName( boolean debug, String id_source, int id_person, String name )
		throws Exception
	{
		String clean = name.replaceAll( "[^A-Za-z0-9 '\\-\\.,èêéëÈÊÉËùûúüÙÛÚÜiìîíïÌÎÍÏòôóöÒÔÓÖàâáÀÂÁÄçÇ]+", "" );

		if( ! clean.contains( " " ) && clean.length() > 18 ) {
			if( debug ) { System.out.println( "cleanName() long name: " + clean ); }
			addToReportPerson( id_person, id_source, 1121, clean );
		}

		return clean;
	} // cleanName


}
