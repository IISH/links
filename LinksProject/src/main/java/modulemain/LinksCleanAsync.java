package modulemain;

import java.io.File;
import java.io.FileWriter;
import java.io.PrintStream;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;

import java.sql.PreparedStatement;
import java.sql.ResultSet;

import java.text.SimpleDateFormat;
import java.time.LocalDateTime;

import java.util.*;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import javax.swing.JTextArea;
import javax.swing.JTextField;

import com.google.common.base.Splitter;

import com.zaxxer.hikari.HikariDataSource;

import connectors.HikariConnection;

//import connectors.MySqlConnector;

import general.Functions;

import dataset.*;
import enumdefinitions.TableType;
import general.PrintLogger;

/**
 * @author Fons Laan
 *
 * FL-30-Sep-2019 Created
 * FL-29-Oct-2019
 *
 * TODO check all queries for new try(){} syntax
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

	private TableToArrayListMultimapHikari almmReport = null;   // Report warnings; read-only, so can remain file global

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
	private HikariDataSource dsTemp;

	private HikariConnection dbconLog      = null;
	private HikariConnection dbconRefRead  = null;
	private HikariConnection dbconRefWrite = null;
	private HikariConnection dbconOriginal = null;
	private HikariConnection dbconCleaned  = null;
	private HikariConnection dbconTemp     = null;

	private final static String SC_U = "u"; // Unknown Standard value assigned (although the original value is not valid)
	private final static String SC_X = "x"; //    X    Standard yet to be assigned
	private final static String SC_N = "n"; //    No   standard value assigned (original value is not valid)
	private final static String SC_Y = "y"; //    Yes  Standard value assigned (original value is valid)

	boolean use_links_logs = true;
	private int count_step = 10000;             // used to be 1000

	/**
	 * Constructor
	 * TODO remove params that are already in opts
	 * @param semaphore
	 * @param opts
	 * @param guiLine
	 * @param guiArea
	 * @param source
	 * @param rmtype
	 * @param showskip
	 * @param ref_url
	 * @param ref_db
	 * @param ref_user
	 * @param ref_pass
	 * @param logTableName
	 * @param dsLog
	 * @param dsRefRead
	 * @param dsRefWrite
	 * @param dsOriginal
	 * @param dsCleaned
	 * @param dsTemp
	 */
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
		HikariDataSource dsCleaned,
		HikariDataSource dsTemp
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
		this.dsTemp     = dsTemp;
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
			dbconTemp     = new HikariConnection( dsTemp.getConnection() );

			dbconCleaned.showMetaData( "dbconCleaned" );

			// links_general.ref_report contains about 75 error definitions, to be used when the normalization encounters errors
			showMessage(String.format( "Thread id %02d; Loading report table", threadId), false, true );
			almmReport = new TableToArrayListMultimapHikari( dbconRefRead, "ref_report", "type", null );

			doRefreshData( opts.isDbgRefreshData(), opts.isDoRefreshData(), source, rmtype );			// GUI cb: Remove previous data

			doPrepieceSuffix( opts.isDbgPrepieceSuffix(), opts.isDoPrepieceSuffix(), source, rmtype );	// GUI cb: Prepiece, Suffix

			doFirstnames( opts.isDbgFirstnames(), opts.isDoFirstnames(), source, rmtype );				// GUI cb: Firstnames

			doFamilynames( opts.isDbgFamilynames(), opts.isDoFamilynames(), source, rmtype );			// GUI cb: Familynames

			doLocations( opts.isDbgLocations(), opts.isDoLocations(), source, rmtype );					// GUI cb: Locations

			doStatusSex( opts.isDbgStatusSex(), opts.isDoStatusSex(), source, rmtype );					// GUI cb: Status and Sex

			doRegistrationType( opts.isDbgRegType(), opts.isDoRegType(), source, rmtype );				// GUI cb: Registration Type

			doOccupation( opts.isDbgOccupation(), opts.isDoOccupation(), source, rmtype );				// GUI cb: Occupation

			doAge( opts.isDbgAge(), opts.isDoAge(), source, rmtype );									// GUI cb: Age, Role,Dates

			doRole( opts.isDbgRole(), opts.isDoRole(), source, rmtype );								// GUI cb: Age, Role, Dates
			/*
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


			almmReport.free();

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
			ex.printStackTrace( new PrintStream( System.out ) );
		}

		String msg = String.format( "Thread id %02d; Cleaning source %s is done", threadId, source );
		showTimingMessage( msg, threadStart );
		System.out.println( msg );

		LocalDateTime timePoint = LocalDateTime.now();  // The current date and time
		msg = String.format( "Thread id %02d; current time: %s", threadId, timePoint.toString() );
		showMessage( msg, false, true );

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
	private void showMessage( String logText, boolean isMinOnly, boolean newLine )
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

		try( ResultSet rs = dbconOriginal.executeQuery( selectQuery ); )
		{
			rs.first();
			location = rs.getString( "registration_location" );
			reg_type = rs.getString( "registration_type" );
			date     = rs.getString( "registration_date" );
			sequence = rs.getString( "registration_seq" );
			guid     = rs.getString( "id_persist_registration" );
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

		try( ResultSet rs = dbconOriginal.executeQuery( selectQueryP ) )
		{
			rs.first();
			id_registration = rs.getString( "id_registration" );
			role            = rs.getString( "role" );
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

			try( ResultSet rs = dbconOriginal.executeQuery( selectQueryR ) )
			{
				rs.first();
				location = rs.getString( "registration_location" );
				reg_type = rs.getString( "registration_type" );
				date     = rs.getString( "registration_date" );
				sequence = rs.getString( "registration_seq" );
				guid     = rs.getString( "id_persist_registration" );
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

		String funcname = String.format( "Thread id %02d; doRefreshData for source: %s, rmtype: %s", threadId, source, rmtype );

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

		int delRegistCCount = dbconCleaned.executeUpdate( deleteRegist );
		int delPersonCCount = dbconCleaned.executeUpdate( deletePerson );
		msg = String.format( "Thread id %02d; %d records deleted from registration_c", threadId, delRegistCCount );
		showMessage( msg, false, true );
		msg = String.format( "Thread id %02d; %d records deleted from person_c", threadId, delPersonCCount );
		showMessage( msg, false, true );

		// if links_cleaned is now empty, we reset the AUTO_INCREMENT
		// that eases comparison with links_a2a tables
		String qRegistCCount = "SELECT COUNT(*) FROM registration_c";
		String qPersonCCount = "SELECT COUNT(*) FROM person_c";

		ResultSet rsR = dbconCleaned.executeQuery( qRegistCCount );
		rsR.first();
		int registCCount = rsR.getInt("COUNT(*)" );
		rsR.close();


		ResultSet rsP = dbconCleaned.executeQuery( qPersonCCount );
		rsP.first();
		int personCCount = rsP.getInt( "COUNT(*)" );
		rsP.close();

		if( registCCount == 0 && personCCount == 0 )
		{
			msg = String.format( "Thread id %02d; Resetting AUTO_INCREMENTs for links_cleaned", threadId );
			showMessage( msg, false, true );
			String auincRegist = "ALTER TABLE registration_c AUTO_INCREMENT = 1";
			String auincPerson = "ALTER TABLE person_c AUTO_INCREMENT = 1";

			dbconCleaned.executeUpdate( auincRegist );
			dbconCleaned.executeUpdate( auincPerson );
		}
		else
		{
			msg = String.format( "Thread id %02d; %d records in registration_c", threadId, registCCount );
			showMessage( msg, false, true );
			msg = String.format( "Thread id %02d; %d records in person_c", threadId, personCCount );
			showMessage( msg, false, true );
		}

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

		registCCount = dbconCleaned.executeUpdate( keysRegistration );
		msg = String.format( "Thread id %02d; %d records inserted in registration_c", threadId, registCCount );
		showMessage( msg, false, true );

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

		dbconCleaned.executeUpdate( Update_id_persist_registration );

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

		personCCount = dbconCleaned.executeUpdate( keysPerson );
		msg = String.format( "Thread id %02d; %d records inserted in person_c", threadId, personCCount );
		showMessage( msg, false, true );

		elapsedShowMessage( funcname, timeStart, System.currentTimeMillis() );
		showMessage_nl();
	} // doRefreshData


	/*---< First- and Familynames >-------------------------------------------*/

	/**
	 * @param debug
	 * @param go
	 * @param source
	 * @param rmtype
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

		TableToArrayListMultimapHikari almmPrepiece = new TableToArrayListMultimapHikari( dbconRefRead, "ref_prepiece", "original", "prefix" );
		TableToArrayListMultimapHikari almmSuffix   = new TableToArrayListMultimapHikari( dbconRefRead, "ref_suffix",   "original", "standard" );
		//TableToArrayListMultimapHikari almmAlias    = new TableToArrayListMultimapHikari( dbconnRefR, "ref_alias",    "original",  null );

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

		almmPrepiece.free();
		almmSuffix.free();
		//almmAlias.free();
		showMessage( String.format( "Thread id %02d; Freed almmPrepiece/almmSuffix", threadId ), false, true );

		elapsedShowMessage( funcname, timeStart, System.currentTimeMillis() );
		showMessage_nl();
	} // doPrepieceSuffix


	/**
	 * @param debug
	 * @param almmPrepiece
	 * @param source
	 * @param rmtype
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

			ResultSet rs = dbconOriginal.executeQuery( selectQuery );
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
					dbconCleaned.executeUpdate( PersonC.updateQuery( "title_noble", listTN.substring( 0, ( listTN.length() - 1 ) ), id_person ) );
				}

				if( !listTO.isEmpty() ) {
					dbconCleaned.executeUpdate( PersonC.updateQuery( "title_other", listTO.substring( 0, ( listTO.length() - 1 ) ), id_person ) );
				}

				if( !listPF.isEmpty() ) {
					dbconCleaned.executeUpdate( PersonC.updateQuery( "prefix", listPF.substring( 0, ( listPF.length() - 1 ) ), id_person) );
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
	 * @param debug
	 * @param almmPrepiece
	 * @param name
	 * @param id
	 * @return
	 * @throws Exception
	 */
	private String namePrepiece( boolean debug, TableToArrayListMultimapHikari almmPrepiece, String name, int id ) throws Exception
	{
		if( !name.contains( " " ) ) {
			return name;            // if no spaces return name
		}

		String fullName = "";

		String list_TN = "";
		String list_TO = "";
		String list_PF = "";

		// Split name
		Queue< String > names = new LinkedList();

		String[] namesArray = name.split( " " );

		for( int i = 0; i < namesArray.length; i++ ) {
			names.add( namesArray[ i ] );
		}

		// Check pieces
		while( !names.isEmpty() )
		{
			// Get part
			String part = names.poll();

			if( almmPrepiece.contains( part ) && almmPrepiece.code( part ).equalsIgnoreCase( SC_Y ) )
			{
				// Add to person
				if( almmPrepiece.value( "title_noble", part ) != null && !almmPrepiece.value( "title_noble", part ).isEmpty() )
				{
					list_TN += almmPrepiece.value( "title_noble", part ) + " ";
				}
				else if( almmPrepiece.value( "title_other", part ) != null && !almmPrepiece.value( "title_other", part ).isEmpty() )
				{
					list_TO += almmPrepiece.value( "title_other", part ) + " ";
				}
				else if( almmPrepiece.value("prefix", part) != null && !almmPrepiece.value( "prefix", part ).isEmpty() )
				{
					list_PF += almmPrepiece.value( "prefix", part) + " ";
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

			dbconCleaned.executeUpdate( PersonC.updateQuery( "title_noble", list_TN, id ) );
		}

		if( !list_TO.isEmpty() ) {
			list_TO = list_TO.substring( 0, ( list_TO.length() - 1 ) );

			dbconCleaned.executeUpdate( PersonC.updateQuery( "title_other", list_TO, id ) );
		}

		if( !list_PF.isEmpty() ) {
			list_PF = list_PF.substring( 0, ( list_PF.length() - 1 ) );

			dbconCleaned.executeUpdate( PersonC.updateQuery( "prefix", list_PF, id ) );
		}

		names.clear();

		return fullName;
	} // namePrepiece


	/**
	 *
	 * @param debug
	 * @param almmSuffix
	 * @param source
	 * @param rmtype
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

			ResultSet rs = dbconOriginal.executeQuery( selectQuery );
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
						dbconCleaned.executeUpdate( query );
					}
					else if( standard_code.equals( SC_N ) )
					{
						addToReportPerson( id_person, source, 73, suffix );   // EC 73
					}
					else if( standard_code.equals( SC_U ) )
					{
						addToReportPerson( id_person, source, 75, suffix );   // EC 74

						String query = PersonC.updateQuery( "suffix", suffix, id_person );
						dbconCleaned.executeUpdate( query );
					}
					else if( standard_code.equals( SC_Y ) )
					{
						String query = PersonC.updateQuery( "suffix", suffix, id_person );
						dbconCleaned.executeUpdate( query );
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
					dbconCleaned.executeUpdate( query );

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
	 *
	 * @param debug
	 * @param almmAlias
	 * @param id
	 * @param source
	 * @param name
	 * @param errCode
	 * @return
	 * @throws Exception
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
	 * @param debug
	 * @param id_source
	 * @param id_person
	 * @param name
	 * @return
	 * @throws Exception
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


	/**
	 * @param debug
	 * @param go
	 * @param source
	 * @param rmtype
	 * @throws Exception
	 */
	private void doFirstnames( boolean debug, boolean go, String source, String rmtype ) throws Exception
	{
		long threadId = Thread.currentThread().getId();

		String funcname = String.format( "Thread id %02d; doFirstnames for source: %s, rmtype: %s", threadId, source, rmtype );

		if( !go ) {
			if( showskip ) { showMessage( "Skipping " + funcname, false, true ); }
			return;
		}

		long timeStart = System.currentTimeMillis();
		showMessage( funcname + " ...", false, true );

		// almmPrepiece, almmSuffix and almmAlias used by Firstnames & Familynames
		long start = System.currentTimeMillis();

		TableToArrayListMultimapHikari almmPrepiece = new TableToArrayListMultimapHikari( dbconRefRead, "ref_prepiece", "original", "prefix" );
		TableToArrayListMultimapHikari almmSuffix   = new TableToArrayListMultimapHikari( dbconRefRead, "ref_suffix",   "original", "standard" );
		TableToArrayListMultimapHikari almmAlias    = new TableToArrayListMultimapHikari( dbconRefRead, "ref_alias",    "original",  null );
		showTimingMessage( String.format( "Thread id %02d; Loaded Prepiece/Suffix/Alias reference tables", threadId ), start );

		// Firstnames
		String msg = "";
		String tmp_firstname = "firstname_t_" + source;
		if( doesTableExist( dbconTemp, "links_temp", tmp_firstname ) ) {
			msg = String.format( "Thread id %02d; Deleting table links_temp.%s", threadId, tmp_firstname );
			showMessage( msg, false, true );
			dropTable( dbconTemp, "links_temp", tmp_firstname );
		}

		createTempFirstnameTable( dbconTemp, source );

		FileWriter writerFirstname = createTempFirstnameFile( source );

		start = System.currentTimeMillis();
		TableToArrayListMultimapHikari almmFirstname = new TableToArrayListMultimapHikari( dbconRefRead, "ref_firstname", "original", "standard" );
		showTimingMessage( String.format( "Thread id %02d; Loaded Firstname reference table", threadId ), start );

		int numrows = almmFirstname.numrows();
		int numkeys = almmFirstname.numkeys();
		showMessage( String.format( "Thread id %02d; Number of rows in reference table ref_firstname: %d", threadId, numrows ), false, true );
		if( numrows != numkeys )
		{ showMessage( String.format( "Thread id %02d; Number of keys in arraylist multimap: %d", threadId, numkeys ), false, true ); }

		msg = String.format( "Thread id %02d; standardFirstname ...", threadId );
		showMessage( msg, false, true );
		standardFirstname( debug, almmPrepiece, almmSuffix, almmAlias, almmFirstname, writerFirstname, source, rmtype );

		almmPrepiece.free();
		almmSuffix.free();
		almmAlias.free();
		showMessage( String.format( "Thread id %02d; Freed almmPrepiece/almmSuffix/almmAlias", threadId ), false, true );

		showTimingMessage( msg, start );

		start = System.currentTimeMillis();

		while( almmFirstname.isBusy().get() ) {
			plog.show( String.format( "Thread id %02d; No permission to update ref_firstname: Waiting 60 seconds", threadId ) );
			Thread.sleep( 60000 );
		}

		int newcount = almmFirstname.newcount();
		showMessage( String.format( "Thread id %02d; New entries for ref table: %d", threadId, newcount ), false, true );
		almmFirstname.updateTable( dbconRefWrite );

		almmFirstname.free();
		showMessage( String.format( "Thread id %02d; Freed almmFirstname", threadId ), false, true );

		writerFirstname.close();
		loadFirstnameCsvToTableT( dbconTemp, source );      // insert csv -> temp table
		updateFirstnameToPersonC( dbconTemp, source );      // update person_c with temp table
		removeFirstnameFile(      source );                 // cleanup
		removeFirstnameTable(     dbconTemp, source );      // cleanup

		msg = String.format( "Thread id %02d; remains Firstname", threadId );
		showTimingMessage( msg, start );

		// Firstnames to lowercase
		start = System.currentTimeMillis();
		msg = String.format( "Thread id %02d; Converting firstnames to lowercase ...", threadId );
		showMessage( msg, false, true );
		String qLower = "UPDATE links_cleaned.person_c SET firstname = LOWER( firstname ) WHERE id_source = " +  source + ";";
		dbconCleaned.executeUpdate( qLower );

		msg = String.format( "Thread id %02d; Converting firstnames to lowercase ", threadId );
		showTimingMessage( msg, start );

		elapsedShowMessage( funcname, timeStart, System.currentTimeMillis() );
		showMessage_nl();
	} // doFirstnames


	/**
	 * @param debug
	 * @param almmPrepiece
	 * @param almmSuffix
	 * @param almmAlias
	 * @param almmFirstname
	 * @param writerFirstname
	 * @param source
	 * @param rmtype
	 */
	public void standardFirstname( boolean debug, TableToArrayListMultimapHikari almmPrepiece, TableToArrayListMultimapHikari almmSuffix, TableToArrayListMultimapHikari almmAlias, TableToArrayListMultimapHikari almmFirstname, FileWriter writerFirstname, String source, String rmtype )
	{
		long threadId = Thread.currentThread().getId();

		int count = 0;
		int count_empty = 0;
		int count_still = 0;
		int stepstate = count_step;

		try
		{
			String selectQuery = "SELECT id_person , firstname , stillbirth FROM person_o WHERE id_source = " + source;
			if ( ! rmtype.isEmpty() ) { selectQuery += " AND registration_maintype = " + rmtype; }
			if( debug ) { showMessage( "standardFirstname: " + selectQuery, false, true ); }

			ResultSet rs = dbconOriginal.executeQuery( selectQuery );
			rs.last();
			int total = rs.getRow();
			rs.beforeFirst();

			while( rs.next() )
			{
				count++;
				if( count == stepstate ) {
					long pct = Math.round( 100.0 * (float)count / (float)total );
					String msg = String.format( "Thread id %02d, standardFirstname, %d-of-%d (%d%%)", threadId, count, total, pct );
					showMessage( msg, true, true );
					stepstate += count_step;
				}

				int id_person    = rs.getInt( "id_person" );
				String firstname = rs.getString( "firstname" );

				//if( id_person == 35241111 ) { debug = true; }
				//else { debug = false; continue; }

				if( debug ) {
					String msg = String.format( "count: %d, id_person: %d, firstname: %s", count, id_person, firstname );
					showMessage( msg, false, true );
				}

				// currently never filled in person_o, but flagged by having a firstname 'Levenloos'
				//String stillbirth = rsFirstName.getString( "stillbirth" );

				// Is firstname empty?
				if( firstname != null && ! firstname.isEmpty() )
				{
					if( debug ) { showMessage( "firstname: " + firstname, false, true ); }
					firstname = cleanFirstname( debug, source, id_person, firstname );

					// cleanFirstname() assumes the presence of uppercase letters, so only now to lowercase
					firstname = firstname.toLowerCase();
					if( debug ) { showMessage( "firstname: " + firstname, false, true ); }

					// Check name on aliases
					String nameNoAlias = standardAlias( debug, almmAlias, id_person, source, firstname, 1107 );

					// split name on spaces
					String[] names = nameNoAlias.split( " " );

					ArrayList< String > preList  = new ArrayList< String >();
					ArrayList< String > postList = new ArrayList< String >();

					boolean empty_name = false;
					for( String name : names ) {
						if( name.isEmpty() ) { empty_name = true; }
						else { preList.add( name ); }       // add to list
					}

					if( empty_name ) { addToReportPerson( id_person, source, 1101, "" ); }  // EC 1101

					String stillbirth = "";
					// loop through the pieces of the name
					for( int i = 0; i < preList.size(); i++ )
					{
						String prename = preList.get( i );       // does this name part exist in ref_firstname?
						if( debug ) { System.out.println( "prename: " + prename ); }

						if( almmFirstname.contains( prename ) )
						{
							// Check the standard code
							String standard_code = almmFirstname.code( prename );
							if( standard_code == null ) { standard_code = ""; }
							if( debug ) { System.out.println( "standard_code: " + standard_code ); }

							if( standard_code.equals( SC_Y ) )
							{
								almmFirstname.standard( "Levenloos" );
								String standard = almmFirstname.standard( prename );
								if( debug ) { System.out.println( "standard: " + standard ); }
								postList.add( standard );

								// if stillbirth has been set to 'y' for this firstname we keep it,
								// and do not let it be overwritten to '' by another prename of the same firstname
								if( stillbirth.isEmpty() )
								{
									// if the firstname equals or contains 'levenloos' the stillbirth column contains 'y'
									stillbirth = almmFirstname.value( "stillbirth", prename );
									if( debug ) { System.out.println( "stillbirth: " + stillbirth ); }
									if( stillbirth == null ) { stillbirth = ""; }
									else if( stillbirth.equals( "y" ) ) {
										if( debug ) {
											String msg = String.format( "#: %d, id_person: %d, firstname: %s, prename: %s",
												count_still, id_person, firstname, prename );
											System.out.println( msg );
										}
										count_still++;
									}
								}
							}
							else if( standard_code.equals( SC_U ) )
							{
								addToReportPerson( id_person, source, 1100, prename );           // EC 1100
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
								addToReportPerson( id_person, source, 1110, prename );           // EC 1110
							}
						}
						else    // name part does not exist in ref_firstname
						{
							// check on invalid token
							String nameNoInvalidChars = cleanName( debug, source, id_person, prename );

							// name contains invalid chars ?
							if( ! prename.equalsIgnoreCase( nameNoInvalidChars ) )
							{
								addToReportPerson( id_person, source, 1104, prename );  // EC 1104

								// Check if name exists in ref
								// Does this part exists in ref_name?
								if( almmFirstname.contains( nameNoInvalidChars ) )
								{
									// Check the standard code
									String standard_code = almmFirstname.code( nameNoInvalidChars );

									if( standard_code.equals( SC_Y ) )
									{
										postList.add( almmFirstname.standard( nameNoInvalidChars ) );
									}
									else if( standard_code.equals( SC_U ) )
									{
										addToReportPerson( id_person, source, 1100, nameNoInvalidChars );    // EC 1100
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
									else { // EC 1110, standard_code not valid
										addToReportPerson(id_person, source, 1110, nameNoInvalidChars);      // EC 1110
									}

									continue;
								}

								// Check on suffix
								Set< String > keys = almmSuffix.keySet();
								for( String key : keys )
								{
									if( nameNoInvalidChars.endsWith( " " + key ) && almmSuffix.code( key ).equals( SC_Y ) )
									{
										addToReportPerson( id_person, source, 1106, nameNoInvalidChars );   // EC 1106

										nameNoInvalidChars = nameNoInvalidChars.replaceAll( " " + key, "" );

										String query = PersonC.updateQuery( "suffix", key, id_person );     // Set suffix

										dbconCleaned.executeUpdate( query );
									}
								}

								// check ref_prepiece
								String nameNoPieces = namePrepiece( debug, almmPrepiece, nameNoInvalidChars, id_person );

								if( ! nameNoPieces.equals( nameNoInvalidChars ) ) {
									addToReportPerson( id_person, source, 1107, nameNoInvalidChars );  // EC 1107
								}

								// last check on ref
								if( almmFirstname.contains( nameNoPieces ) )
								{
									// Check the standard code
									String standard_code = almmFirstname.code( nameNoPieces );

									if( standard_code.equals( SC_Y ) )
									{
										postList.add( almmFirstname.standard( nameNoPieces ) );
									}
									else if( standard_code.equals( SC_U ) )
									{
										addToReportPerson( id_person, source, 1100, nameNoPieces );    // EC 1100
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
									else { // EC 1110, standard_code not valid
										addToReportPerson( id_person, source, 1110, nameNoPieces );    // EC 1110
									}
								}
								else {
									// name must be added to ref_firstname with standard_code x
									almmFirstname.add( nameNoPieces );
									postList.add( nameNoPieces );   // Also add name to postlist
								}
							}
							else  // no invalid token
							{
								// Check on suffix
								Set< String > keys = almmSuffix.keySet();
								for( String key : keys )
								{
									if( nameNoInvalidChars.endsWith( " " + key ) && almmSuffix.code( key ).equals( SC_Y ) )
									{
										addToReportPerson( id_person, source, 1106, nameNoInvalidChars );   // EC 1106

										nameNoInvalidChars = nameNoInvalidChars.replaceAll( " " + key, "" );

										String query = PersonC.updateQuery( "suffix", key, id_person );     // Set suffix

										dbconCleaned.executeUpdate( query );
									}
								}

								// check ref_prepiece
								String nameNoPieces = namePrepiece( debug, almmPrepiece, nameNoInvalidChars, id_person );

								if( ! nameNoPieces.equals( nameNoInvalidChars ) ) {
									addToReportPerson(id_person, source, 1108, nameNoInvalidChars);   // EC 1108
								}

								// last check on ref
								if( almmFirstname.contains( nameNoPieces ) )
								{
									// Check the standard code
									String standard_code = almmFirstname.code( nameNoPieces );

									if( standard_code.equals( SC_Y ) )
									{
										postList.add( almmFirstname.standard( nameNoPieces ) );
									}
									else if( standard_code.equals( SC_U ) )
									{
										addToReportPerson( id_person, source, 1100, nameNoPieces );     // EC 1100
										postList.add( almmFirstname.standard( nameNoPieces ) );
									}
									else if( standard_code.equals( SC_N ) )
									{
										addToReportPerson( id_person, source, 1105, nameNoPieces );     // EC 1105
									}
									else if( standard_code.equals( SC_X ) )
									{
										addToReportPerson( id_person, source, 1109, nameNoPieces );     // EC 1109
										postList.add( nameNoPieces );
									}
									else { // EC 1110, standard_code not valid
										addToReportPerson( id_person, source, 1110, nameNoPieces );     // EC 1110
									}
								}
								else {
									// name must be added to ref_firstname with standard_code x
									almmFirstname.add( nameNoPieces );
									postList.add( nameNoPieces );   // Also add name to postlist
								}
							}
						}
					}

					// recollect firstnames, and set individual firstnames
					String firstnames = "";
					String firstname1 = "";
					String firstname2 = "";
					String firstname3 = "";
					String firstname4 = "";

					for( int n = 0; n < postList.size(); n++ )
					{
						if( n > 0 ) { firstnames += " "; }      // add space
						String name = postList.get( n );
						firstnames += name;

						if( n == 0 ) { firstname1 = name; }
						if( n == 1 ) { firstname2 = name; }
						if( n == 2 ) { firstname3 = name; }
						if( n == 3 ) { firstname4 = name; }
					}

					//writerFirstname.write( id_person + "," + firstnames + "," + stillbirth + "\n" );
					String line = String.format( "%d,%s,%s,%s,%s,%s,%s\n",
						id_person, firstnames, firstname1, firstname2, firstname3, firstname4, stillbirth );
					writerFirstname.write( line );

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
			}

			int count_new = almmFirstname.newcount();
			String strNew = "";
			if( count_new == 0 ) { strNew = "no new firstnames"; }
			else if( count_new == 1 ) { strNew = "1 new firstname"; }
			else { strNew = "" + count_new + " new firstnames"; }

			String strStill = "";
			if( count_still == 0 ) { strStill = "no stillbirths"; }
			else if( count_still == 1 ) { strStill = "1 stillbirth"; }
			else { strStill = "" + count_still + " stillbirths"; }

			showMessage( String.format( "Thread id %02d; %d firstname records, %d without a Firstname, %s, %s", threadId, count, count_empty, strNew, strStill ), false, true );
		}
		catch( Exception ex ) {
			String msg = String.format( "Thread id %02d; count: %d, Exception while cleaning Firstname: %s", threadId, count, ex.getMessage() );
			showMessage( msg, false, true );
			ex.printStackTrace( new PrintStream( System.out ) );
		}
	} // standardFirstname


	/**
	 *
	 * @param debug
	 * @param id_source
	 * @param id_person
	 * @param name
	 * @return
	 * @throws Exception
	 */
	private String cleanFirstname( boolean debug, String id_source, int id_person, String name )
		throws Exception
	{
		// some characters should be interpreted as a missing <space>, e.g.:
		// '/' -> ' ', '<br/>' -> ' '
		// check components

		String new_name = "";
		Iterable< String > rawparts = Splitter.on( ' ' ).split( name );
		for ( String part : rawparts )
		{
			// forward slash ?
			if( part.contains( "/" ) ) {
				if( debug ) { System.out.println( "cleanFirstame() id_person: " + id_person + ", part contains '/': " + part ); }
				addToReportPerson(id_person, id_source, 1113, part);
				part = part.replace( "/", " ");
			}

			// <br/> ?
			if( part.contains( "<br/>" ) ) {
				if( debug ) { System.out.println( "cleanFirstame() id_person: " + id_person + ", part contains '<br/>': " + part ); }
				addToReportPerson(id_person, id_source, 1114, part);
				part = part.replace( "<br/>", " ");
			}

			// intermediate uppercase letter ? -> insert space
			// check for uppercase letters beyond the second char; notice: IJ should be an exception (IJsbrand)
			// there are many garbage characters
			if( part.length() > 2 ) {
				for( int i = 2; i < part.length(); i++) {
					char ch = part.charAt( i );
					if( Character.isUpperCase( ch ) )
					{
						// only insert a space before the uppercase char if it is not preceded by a '-'
						if( part.charAt( i-1 ) != '-' ) {
							if( debug ) { System.out.println( "cleanFirstname() id_person: " + id_person + ", part contains uppercase letter: " + part ); }
							addToReportPerson( id_person, id_source, 1112, part );
							part =  part.substring( 0, i-1 ) + " " + part.substring( i );
						}
					}
				}
			}

			if(! new_name.isEmpty() ) { new_name += " "; }
			new_name += part;
		}

		if( debug && ! name.equals( new_name ) ) { System.out.println( " -> " + new_name ); }
		name = new_name;

		String clean = name.replaceAll( "[^A-Za-z0-9 '\\-èêéëÈÊÉËùûúüÙÛÚÜiìîíïÌÎÍÏòôóöÒÔÓÖàâáÀÂÁÄçÇ]+", "" );


		if( clean.contains( " " ) ) {
			// check components
			Iterable< String > cleanparts = Splitter.on( ' ' ).split( clean );
			for ( String part : cleanparts ) {
				if( part.length() > 18 ) {
					if( debug ) { System.out.println( "cleanFirstname() id_person: " + id_person + ", long firstname: " + part + " in: " + clean ); }
					addToReportPerson( id_person, id_source, 1121, part );
				}
			}
		}
		else {
			if( clean.length() > 18 ) {
				if( debug ) { System.out.println( "cleanFirstname() id_person: " + id_person + ", long firstname: " + clean ); }
				addToReportPerson( id_person, id_source, 1121, clean );
			}
		}

		return clean;
	} // cleanFirstname


	/**
	 * @param dbconTemp
	 * @param source
	 * @throws Exception
	 */
	private void createTempFirstnameTable( HikariConnection dbconTemp, String source ) throws Exception
	{
		long threadId = Thread.currentThread().getId();

		String tablename = "firstname_t_" + source;

		showMessage( String.format( "Thread id %02d; Creating %s", threadId, tablename + " table" ), false, true );

		// Notice: the stillbirth column is not yet used
		String query = "CREATE TABLE links_temp." + tablename + " ("
			+ " person_id INT UNSIGNED NOT NULL AUTO_INCREMENT ,"
			+ " firstname VARCHAR(100) NULL ,"
			+ " firstname1 VARCHAR(30) NULL ,"
			+ " firstname2 VARCHAR(30) NULL ,"
			+ " firstname3 VARCHAR(30) NULL ,"
			+ " firstname4 VARCHAR(30) NULL ,"
			+ " stillbirth VARCHAR(3) NULL ,"
			+ " PRIMARY KEY (person_id) ) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin;";

		dbconTemp.executeUpdate( query );
	} // createTempFirstnameTable


	/**
	 * @param source
	 * @return
	 * @throws Exception
	 */
	private FileWriter createTempFirstnameFile( String source ) throws Exception
	{
		long threadId = Thread.currentThread().getId();

		String filename = "firstname_t_" + source + ".csv";

		File file = new File( filename );
		if( file.exists() ) {
			showMessage( String.format( "Thread id %02d; Deleting file %s", threadId, filename ), false, true );
			file.delete();
		}

		showMessage( String.format( "Thread id %02d; Creating %s", threadId, filename ), false, true );
		return new FileWriter( filename );
	} // createTempFirstnameFile


	/**
	 * @param dbconTemp
	 * @param source
	 * @throws Exception
	 */
	private void loadFirstnameCsvToTableT( HikariConnection dbconTemp, String source ) throws Exception
	{
		long threadId = Thread.currentThread().getId();

		String csvname   = "firstname_t_" + source + ".csv";
		String tablename = "firstname_t_" + source;

		showMessage( String.format( "Thread id %02d; Loading %s into %s table", threadId, csvname, tablename ), false, true );

		String query = "LOAD DATA LOCAL INFILE '" + csvname + "'"
			+ " INTO TABLE " + tablename
			+ " FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n'"
			+ " ( person_id , firstname , firstname1 , firstname2 , firstname3 , firstname4 , stillbirth );";

		dbconTemp.executeUpdate( query );
	} // loadFirstnameCsvToTableT


	/**
	 * @param dbconTemp
	 * @param source
	 * @throws Exception
	 */
	private void updateFirstnameToPersonC( HikariConnection dbconTemp, String source ) throws Exception
	{
		long threadId = Thread.currentThread().getId();

		showMessage( String.format( "Thread id %02d; Moving first names from temp table to person_c ...", threadId ), false, true );

		String tablename = "firstname_t_" + source;

		String query   = "UPDATE links_cleaned.person_c, links_temp." + tablename
			+ " SET"
			+ " links_cleaned.person_c.firstname = links_temp."  + tablename + ".firstname ,"
			+ " links_cleaned.person_c.firstname1 = links_temp." + tablename + ".firstname1 ,"
			+ " links_cleaned.person_c.firstname2 = links_temp." + tablename + ".firstname2 ,"
			+ " links_cleaned.person_c.firstname3 = links_temp." + tablename + ".firstname3 ,"
			+ " links_cleaned.person_c.firstname4 = links_temp." + tablename + ".firstname4 ,"
			+ " links_cleaned.person_c.stillbirth = links_temp." + tablename + ".stillbirth"
			+ " WHERE links_cleaned.person_c.id_person = links_temp." + tablename + ".person_id;";

		dbconTemp.executeUpdate(query);
	} // updateFirstnameToPersonC


	/**
	 *
	 * @param source
	 * @throws Exception
	 */
	public void removeFirstnameFile( String source ) throws Exception
	{
		long threadId = Thread.currentThread().getId();

		String csvname = "firstname_t_" + source + ".csv";

		showMessage( String.format( "Thread id %02d; Deleting file %s", threadId, csvname ), false, true );

		File f = new File( csvname );
		f.delete();
	} // removeFirstnameFile


	/**
	 * @param dbconTemp
	 * @param source
	 * @throws Exception
	 */
	public void removeFirstnameTable( HikariConnection dbconTemp, String source ) throws Exception
	{
		long threadId = Thread.currentThread().getId();

		String tablename = "firstname_t_" + source;

		showMessage( String.format( "Thread id %02d; Deleting table %s", threadId, tablename ), false, true );

		String query = "DROP TABLE IF EXISTS " + tablename + ";";
		dbconTemp.executeUpdate( query );
	} // removeFirstnameTable


	/**
	 * @param debug
	 * @param go
	 * @param source
	 * @param rmtype
	 * @throws Exception
	 */
	private void doFamilynames( boolean debug, boolean go, String source, String rmtype ) throws Exception
	{
		long threadId = Thread.currentThread().getId();

		String funcname = String.format( "Thread id %02d; doFamilynames for source: %s, rmtype: %s", threadId, source, rmtype );

		if( !go ) {
			if( showskip ) { showMessage( "Skipping " + funcname, false, true ); }
			return;
		}

		long timeStart = System.currentTimeMillis();
		showMessage( funcname + " ...", false, true );

		// almmPrepiece, almmSuffix and almmAlias used by Firstnames & Familynames
		long start = System.currentTimeMillis();

		TableToArrayListMultimapHikari almmPrepiece = new TableToArrayListMultimapHikari( dbconRefRead, "ref_prepiece", "original", "prefix" );
		TableToArrayListMultimapHikari almmSuffix   = new TableToArrayListMultimapHikari( dbconRefRead, "ref_suffix",   "original", "standard" );
		TableToArrayListMultimapHikari almmAlias    = new TableToArrayListMultimapHikari( dbconRefRead, "ref_alias",    "original",  null );
		showTimingMessage( String.format( "Thread id %02d; Loaded Prepiece/Suffix/Alias reference tables", threadId ), start );

		// Familynames
		String msg = "";
		String tmp_familyname = "familyname_t_" + source;
		if( doesTableExist( dbconTemp, "links_temp",tmp_familyname  ) ) {
			msg = String.format( "Thread id %02d; Deleting table links_temp.%s", threadId, tmp_familyname );
			showMessage( msg, false, true );
			dropTable( dbconTemp, "links_temp", tmp_familyname );
		}

		createTempFamilynameTable( dbconTemp, source );

		FileWriter writerFamilyname = createTempFamilynameFile(  source );

		start = System.currentTimeMillis();
		TableToArrayListMultimapHikari almmFamilyname = new TableToArrayListMultimapHikari( dbconRefRead, "ref_familyname", "original", "standard" );
		showTimingMessage( String.format( "Thread id %02d; Loaded Familyname reference table", threadId ), start );

		int numrows = almmFamilyname.numrows();
		int numkeys = almmFamilyname.numkeys();
		showMessage( String.format( "Thread id %02d; Number of rows in reference table ref_familyname: %d", threadId, numrows ), false, true );
		if( numrows != numkeys )
		{ showMessage( String.format( "Number of keys in arraylist multimap: %d", threadId, numkeys ), false, true ); }

		msg = String.format( "Thread id %02d; standardFamilyname ...", threadId );
		showMessage( msg + " ...", false, true );
		standardFamilyname( debug, almmPrepiece, almmSuffix, almmAlias, almmFamilyname, writerFamilyname, source, rmtype );

		almmPrepiece.free();
		almmSuffix.free();
		almmAlias.free();
		showMessage( String.format( "Thread id %02d; Freed almmPrepiece/almmSuffix/almmAlias", threadId ), false, true );
		showTimingMessage( msg, start );

		start = System.currentTimeMillis();
		msg = String.format( "Thread id %02d; remains Familyname", threadId );
		showMessage( msg + " ...", false, true );

		while( almmFamilyname.isBusy().get() ) {
			plog.show( String.format( "Thread id %02d; No permission to update ref_familyname: Waiting 60 seconds", threadId ) );
			Thread.sleep( 60000 );
		}

		int newcount = almmFamilyname.newcount();
		showMessage( String.format( "Thread id %02d; New entries for ref table: %d", threadId, newcount ), false, true );
		almmFamilyname.updateTable( dbconRefWrite );

		almmFamilyname.free();
		showMessage( String.format( "Thread id %02d; Freed almmFamilyname", threadId ), false, true );

		writerFamilyname.close();
		loadFamilynameCsvToTableT( dbconTemp, source );
		updateFamilynameToPersonC( dbconTemp, source );
		removeFamilynameFile(      source );
		removeFamilynameTable(     dbconTemp, source );

		msg = String.format( "Thread id %02d; remains Familyname", threadId );
		showTimingMessage( msg, start );

		// Familynames to lowercase
		start = System.currentTimeMillis();
		msg = String.format( "Thread id %02d; Converting familynames to lowercase ...", threadId );
		showMessage( msg, false, true );
		String qLower = "UPDATE links_cleaned.person_c SET familyname = LOWER( familyname ) WHERE id_source = " + source + ";";
		dbconCleaned.executeUpdate( qLower );
		msg = String.format( "Thread id %02d; Converting familynames to lowercase ", threadId );
		showTimingMessage( msg, start );

		elapsedShowMessage( funcname, timeStart, System.currentTimeMillis() );
		showMessage_nl();
	} // doFamilynames


	/**
	 * @param debug
	 * @param almmPrepiece
	 * @param almmSuffix
	 * @param almmAlias
	 * @param almmFamilyname
	 * @param writerFamilyname
	 * @param source
	 * @param rmtype
	 */
	public void standardFamilyname( boolean debug, TableToArrayListMultimapHikari almmPrepiece, TableToArrayListMultimapHikari almmSuffix, TableToArrayListMultimapHikari almmAlias, TableToArrayListMultimapHikari almmFamilyname, FileWriter writerFamilyname, String source, String rmtype )
	{
		long threadId = Thread.currentThread().getId();

		int count = 0;
		int count_empty = 0;
		int stepstate = count_step;

		try
		{
			String selectQuery = "SELECT id_person , familyname FROM person_o WHERE id_source = " + source;
			if ( ! rmtype.isEmpty() ) { selectQuery += " AND registration_maintype = " + rmtype; }
			if( debug ) { showMessage( "standardFamilyname: " + selectQuery, false, true ); }

			ResultSet rs = dbconOriginal.executeQuery( selectQuery );
			rs.last();
			int total = rs.getRow();
			rs.beforeFirst();

			while( rs.next() )
			{
				count++;
				if( count == stepstate ) {
					long pct = Math.round( 100.0 * (float)count / (float)total );
					String msg = String.format( "Thread id %02d, standardFamilyname, %d-of-%d (%d%%)", threadId, count, total, pct );
					showMessage( msg, true, true );
					stepstate += count_step;
				}

				// Get family name
				String familyname = rs.getString( "familyname" );
				int id_person     = rs.getInt( "id_person" );

				//if( id_person == 35241111 ) { debug = true; }
				//else { debug = false; continue; }

				if( debug ) {
					String msg = String.format( "count: %d, id_person: %d, familyname: %s", count, id_person, familyname );
					showMessage( msg, false, true );
				}

				// Check is Familyname is not empty or null
				if( familyname != null && ! familyname.isEmpty() )
				{
					if( debug ) { showMessage( "familyname: " + familyname , false, true ); }
					familyname = cleanFamilyname( debug, source, id_person, familyname );
					familyname = familyname.toLowerCase();
					if( debug ) { showMessage( "familyname: " + familyname , false, true ); }

					// familyname in ref_familyname ?
					if( almmFamilyname.contains( familyname ) )
					{
						// get standard_code
						String standard_code = almmFamilyname.code( familyname );
						String standard = almmFamilyname.standard( familyname );
						if( debug ) { showMessage( "standard_code: " + standard_code, false, true ); }
						if( debug ) { showMessage( "standard: " + standard, false, true ); }

						// Check the standard code
						if( standard_code.equals( SC_Y ) )
						{
							writerFamilyname.write( id_person + "," + almmFamilyname.standard( familyname ) + "\n" );
						}
						else if( standard_code.equals( SC_U ) )
						{
							addToReportPerson( id_person, source, 1000, familyname ); // EC 1000

							writerFamilyname.write( id_person + "," + almmFamilyname.standard( familyname ) + "\n" );
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
						if( debug ) { showMessage( "not in ref_familyname: " + familyname, false, true ); }
						addToReportPerson( id_person, source, 1002, familyname );  // EC 1002

						String nameNoSerriedSpaces = familyname.replaceAll( " [ ]+", " " );

						// Family name contains two or more serried spaces?
						if( !nameNoSerriedSpaces.equalsIgnoreCase( familyname ) ) {
							addToReportPerson(id_person, source, 1003, familyname);  // EC 1003
						}

						String nameNoInvalidChars = cleanName( debug, source, id_person, nameNoSerriedSpaces );

						// Family name contains invalid chars ?
						if( !nameNoSerriedSpaces.equalsIgnoreCase( nameNoInvalidChars ) ) {
							addToReportPerson( id_person, source, 1004, familyname );   // EC 1004
						}

						// check if name has prepieces
						String nameNoPrePiece = namePrepiece( debug, almmPrepiece, nameNoInvalidChars, id_person );

						// Family name contains invalid chars ?
						if( !nameNoPrePiece.equalsIgnoreCase( nameNoInvalidChars ) ) {
							addToReportPerson( id_person, source, 1008, familyname );  // EC 1008
						}

						// Check on aliases
						String nameNoAlias = standardAlias( debug, almmAlias, id_person, source, nameNoPrePiece, 1007 );

						// Check on suffix
						if( debug ) { showMessage( "suffix keySet()", false, true ); }
						Set< String > keys = almmSuffix.keySet();
						for( String key : keys )
						{
							if( debug ) { showMessage( "suffix key: " + key, false, true ); }
							if( nameNoAlias.endsWith( " " + key ) ) {
								addToReportPerson( id_person, source, 1006, nameNoAlias );      // EC 1006

								nameNoAlias = nameNoAlias.replaceAll( " " + key, "" );

								PersonC.updateQuery( "suffix", key, id_person );                // Set alias
							}
						}

						// Clean name one more time
						String nameNoSuffix = LinksSpecific.funcCleanSides( nameNoAlias );

						// Check name in original
						if( almmFamilyname.contains( nameNoSuffix ) )
						{
							// get standard_code
							String standard_code = almmFamilyname.code( nameNoSuffix );

							// Check the standard code
							if( standard_code.equals( SC_Y ) )
							{
								writerFamilyname.write( id_person + "," + almmFamilyname.standard( nameNoSuffix ).toLowerCase() + "\n" );
							}
							else if( standard_code.equals( SC_U ) ) {
								addToReportPerson( id_person, source, 1000, nameNoSuffix );    // EC 1000

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
							almmFamilyname.add( nameNoSuffix );

							addToReportPerson( id_person, source, 1009, nameNoSuffix );    // EC 1009

							writerFamilyname.write( id_person + "," + nameNoSuffix.trim().toLowerCase() + "\n" );

						}
					}
				}
				else {      // Familyname empty
					count_empty++;
					addToReportPerson( id_person, source, 1001, "" );  // EC 1001
				}
			}

			int count_new = almmFamilyname.newcount();
			String strNew = "";
			if( count_new == 0 ) { strNew = "no new familynames"; }
			else if( count_new == 1 ) { strNew = "1 new familyname"; }
			else { strNew = "" + count_new + " new familynames"; }

			showMessage( String.format( "Thread id %02d; %d familyname records, %d without a Familyname, %s", threadId, count, count_empty, strNew ), false, true );
		}
		catch( Exception ex) {
			String msg = String.format( "Thread id %02d; count: %d, Exception while cleaning Familyname: %s", threadId, count, ex.getMessage() );
			showMessage( msg, false, true );
			ex.printStackTrace( new PrintStream( System.out ) );
		}
	} // standardFamilyname


	/**
	 * @param debug
	 * @param id_source
	 * @param id_person
	 * @param name
	 * @return
	 * @throws Exception
	 */
	private String cleanFamilyname( boolean debug, String id_source, int id_person, String name )
		throws Exception
	{
		String clean = name.replaceAll( "[^A-Za-z0-9 '\\-èêéëÈÊÉËùûúüÙÛÚÜiìîíïÌÎÍÏòôóöÒÔÓÖàâáÀÂÁÄçÇ]+", "").replaceAll("\\-", " " );

		if( ! clean.contains( " " ) && clean.length() > 18 ) {
			if( debug ) { System.out.println( "cleanFamilyname() long familyname: " + clean ); }
			addToReportPerson( id_person, id_source, 1121, clean );
		}

		return clean;
	} // cleanFamilyname


	/**
	 * @param dbconTemp
	 * @param source
	 * @throws Exception
	 */
	private void createTempFamilynameTable( HikariConnection dbconTemp, String source ) throws Exception
	{
		long threadId = Thread.currentThread().getId();

		String tablename = "familyname_t_" + source;

		showMessage( String.format( "Thread id %02d; Creating %s table", threadId, tablename  ), false, true );

		String query = "CREATE TABLE links_temp." + tablename + " ("
			+ " person_id INT UNSIGNED NOT NULL AUTO_INCREMENT ,"
			+ " familyname VARCHAR(110) NULL ,"
			+ " PRIMARY KEY (person_id) ) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin;";

		dbconTemp.executeUpdate( query );

	} // createTempFamilynameTable


	/**
	 * @param source
	 * @return
	 * @throws Exception
	 */
	private FileWriter createTempFamilynameFile( String source ) throws Exception
	{
		long threadId = Thread.currentThread().getId();

		String filename = "familyname_t_" + source + ".csv";

		File f = new File( filename );
		if( f.exists() ) {
			showMessage( String.format( "Thread id %02d; Deleting file %s", threadId, filename ), false, true );
			f.delete();
		}

		showMessage( String.format( "Thread id %02d; Creating %s", threadId, filename ), false, true );
		return new FileWriter( filename );
	} // createTempFamilynameFile


	/**
	 * @param dbconTemp
	 * @param source
	 * @throws Exception
	 */
	private void loadFamilynameCsvToTableT( HikariConnection dbconTemp, String source ) throws Exception
	{
		long threadId = Thread.currentThread().getId();

		String csvname   = "familyname_t_" + source + ".csv";
		String tablename = "familyname_t_" + source;

		showMessage( String.format( "Thread id %02d; Loading %s into %s table", threadId, csvname, tablename ), false, true );

		String query = "LOAD DATA LOCAL INFILE '" + csvname + "'"
			+ " INTO TABLE " + tablename
			+ " FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' ( person_id , familyname );";

		dbconTemp.executeUpdate( query );

	} // loadFamilynameCsvToTableT


	/**
	 * @param dbconTemp
	 * @param source
	 * @throws Exception
	 */
	private void updateFamilynameToPersonC( HikariConnection dbconTemp, String source ) throws Exception
	{
		long threadId = Thread.currentThread().getId();

		showMessage( String.format( "Thread id %02d; Moving familynames from temp table to person_c", threadId ), false, true );

		String tablename = "familyname_t_" + source;

		String query = "UPDATE links_cleaned.person_c, links_temp."   + tablename
			+ " SET links_cleaned.person_c.familyname = links_temp."  + tablename + ".familyname"
			+ " WHERE links_cleaned.person_c.id_person = links_temp." + tablename + ".person_id;";

		dbconTemp.executeUpdate( query );
	} // updateFamilynameToPersonC


	/**
	 * @param source
	 * @throws Exception
	 */
	public void removeFamilynameFile( String source ) throws Exception
	{
		long threadId = Thread.currentThread().getId();

		String csvname = "familyname_t_" + source + ".csv";

		showMessage( String.format( "Thread id %02d; Deleting file %s", threadId, csvname ), false, true );

		java.io.File f = new java.io.File( csvname );
		f.delete();
	} // removeFamilynameFile


	/**
	 * @param dbconTemp
	 * @param source
	 * @throws Exception
	 */
	public void removeFamilynameTable( HikariConnection dbconTemp, String source ) throws Exception
	{
		long threadId = Thread.currentThread().getId();

		String tablename = "familyname_t_" + source;

		showMessage( String.format( "Thread id %02d; Deleting table %s", threadId, tablename ), false, true );

		String query = "DROP TABLE IF EXISTS " + tablename + ";";

		dbconTemp.executeUpdate( query );
	} // removeFamilynameTable


	/**
	 * @param dbcon
	 * @param db_name
	 * @param table_name
	 * @return
	 * @throws Exception
	 */
	private boolean doesTableExist( HikariConnection dbcon, String db_name, String table_name ) throws Exception
	{
		String query = "SELECT COUNT(*) FROM information_schema.tables"
			+ " WHERE table_schema = '" + db_name + "'"
			+ " AND table_name = '" + table_name + "'";

		ResultSet rs = dbcon.executeQuery( query );
		rs.first();
		int count = rs.getInt( "COUNT(*)" );
		//showMessage( "doesTableExist: " + db_name + " " + table_name + " : " + count, false, true );

		if( count == 1 ) return true;
		else return false;
	} // doesTableExist


	/**
	 * @param dbcon
	 * @param db_name
	 * @param table_name
	 * @throws Exception
	 */
	private void dropTable( HikariConnection dbcon, String db_name, String table_name ) throws Exception
	{
		String query = "DROP TABLE `" + db_name + "`.`" + table_name + "`";
		dbcon.executeUpdate( query );
	} // dropTable


	/*---< Locations >--------------------------------------------------------*/

	/**
	 * @param debug
	 * @param go
	 * @param source
	 * @param rmtype
	 * @throws Exception
	 */
	private void doLocations( boolean debug, boolean go, String source, String rmtype ) throws Exception
	{
		long threadId = Thread.currentThread().getId();

		String funcname = String.format( "Thread id %02d; doLocations for source: %s, rmtype: %s", threadId, source, rmtype );

		if( !go ) {
			if( showskip ) { showMessage( "Skipping " + funcname, false, true ); }
			return;
		}

		long funcStart = System.currentTimeMillis();
		showMessage( funcname + " ...", false, true );

		long start = System.currentTimeMillis();

		TableToArrayListMultimapHikari almmLocation = new TableToArrayListMultimapHikari( dbconRefRead, "ref_location", "original", "location_no" );

		showTimingMessage( String.format( "Thread id %02d; Loaded Location reference table", threadId ), start );

		int numrows = almmLocation.numrows();
		int numkeys = almmLocation.numkeys();
		showMessage( String.format( "Thread id %02d; Number of rows in reference table ref_location: %d", threadId, numrows ), false, true );
		if( numrows != numkeys )
		{ showMessage( String.format( "Thread id %02d; Number of keys in arraylist multimap: %d", threadId, numkeys ), false, true ); }

		String msg = "";

		start = System.currentTimeMillis();
		msg = String.format( "Thread id %02d; standardRegistrationLocation ...", threadId );
		showMessage( msg, false, true );
		standardRegistrationLocation( debug, almmLocation, source, rmtype );
		msg = String.format( "Thread id %02d; standardRegistrationLocation ", threadId );
		showTimingMessage( msg, start );

		start = System.currentTimeMillis();
		msg = String.format( "Thread id %02d; standardBirthLocation ...", threadId );
		showMessage( msg, false, true );
		standardBirthLocation( debug, almmLocation, source, rmtype );
		msg = String.format( "Thread id %02d; standardBirthLocation ", threadId );
		showTimingMessage( msg, start );

		start = System.currentTimeMillis();
		msg = String.format( "Thread id %02d; standardMarriageLocation ...", threadId );
		showMessage( msg, false, true );
		standardMarriageLocation( debug, almmLocation, source, rmtype );
		msg = String.format( "Thread id %02d; standardMarriageLocation ", threadId );
		showTimingMessage( msg, start );

		start = System.currentTimeMillis();
		msg = String.format( "Thread id %02d; standardDivorceLocation ...", threadId );
		showMessage( msg, false, true );
		standardDivorceLocation( debug, almmLocation, source, rmtype );
		msg = String.format( "Thread id %02d; standardDivorceLocation ", threadId );
		showTimingMessage( msg, start );

		start = System.currentTimeMillis();
		msg = String.format( "Thread id %02d; standardLivingLocation ...", threadId );
		showMessage( msg, false, true );
		standardLivingLocation( debug, almmLocation, source, rmtype );
		msg = String.format( "Thread id %02d; standardLivingLocation ", threadId );
		showTimingMessage( msg, start );

		start = System.currentTimeMillis();
		msg = String.format( "Thread id %02d; standardDeathLocation ...", threadId );
		showMessage( msg, false, true );
		standardDeathLocation( debug,  almmLocation, source, rmtype );
		msg = String.format( "Thread id %02d; standardDeathLocation ", threadId );
		showTimingMessage( msg, start );

		while( almmLocation.isBusy().get() ) {
			plog.show( String.format( "Thread id %02d; No permission to update ref_location: Waiting 60 seconds", threadId ) );
			Thread.sleep( 60000 );
		}

		int newcount = almmLocation.newcount();
		showMessage( String.format( "Thread id %02d; New entries for ref table: %d", threadId, newcount ), false, true );
		almmLocation.updateTable( dbconRefWrite );

		almmLocation.free();
		showMessage( String.format( "Thread id %02d; Freed almmLocation", threadId ), false, true );

		elapsedShowMessage( funcname, funcStart, System.currentTimeMillis() );
		showMessage_nl();
	} // doLocations


	/**
	 * @param debug
	 * @param almmLocation
	 * @param source
	 * @param rmtype
	 */
	public void standardRegistrationLocation( boolean debug, TableToArrayListMultimapHikari almmLocation, String source, String rmtype )
	{
		String selectQuery = "SELECT id_registration , registration_location FROM registration_o WHERE id_source = " + source;
		if ( ! rmtype.isEmpty() ) { selectQuery += " AND registration_maintype = " + rmtype; }
		if( debug ) { showMessage( "standardRegistrationLocation: " + selectQuery, false, true ); }

		standardLocation( debug, almmLocation, selectQuery, "id_registration", "registration_location", "registration_location_no", source, TableType.REGISTRATION );
	} // standardRegistrationLocation


	/**
	 * @param debug
	 * @param almmLocation
	 * @param source
	 * @param rmtype
	 */
	public void standardBirthLocation( boolean debug, TableToArrayListMultimapHikari almmLocation, String source, String rmtype )
	{
		String selectQuery = "SELECT id_person , birth_location FROM person_o WHERE id_source = " + source + " AND birth_location <> ''";
		if ( ! rmtype.isEmpty() ) { selectQuery += " AND registration_maintype = " + rmtype; }
		if( debug ) { showMessage( "standardBirthLocation: " + selectQuery, false, true ); }

		standardLocation( debug, almmLocation, selectQuery, "id_person", "birth_location", "birth_location", source, TableType.PERSON );
	} // standardBirthLocation


	/**
	 * @param debug
	 * @param almmLocation
	 * @param source
	 * @param rmtype
	 */
	public void standardMarriageLocation( boolean debug, TableToArrayListMultimapHikari almmLocation, String source, String rmtype )
	{
		String selectQuery = "SELECT id_person , mar_location FROM person_o WHERE id_source = " + source + " AND mar_location <> ''";
		if ( ! rmtype.isEmpty() ) { selectQuery += " AND registration_maintype = " + rmtype; }
		if( debug ) { showMessage( "standardMarLocation: " + selectQuery, false, true ); }

		standardLocation( debug, almmLocation, selectQuery, "id_person", "mar_location", "mar_location", source, TableType.PERSON );
	} // standardMarriageLocation


	/**
	 * @param debug
	 * @param almmLocation
	 * @param source
	 * @param rmtype
	 */
	public void standardDivorceLocation( boolean debug, TableToArrayListMultimapHikari almmLocation, String source, String rmtype )
	{
		String selectQuery = "SELECT id_person , divorce_location FROM person_o WHERE id_source = " + source + " AND divorce_location <> ''";
		if ( ! rmtype.isEmpty() ) { selectQuery += " AND registration_maintype = " + rmtype; }
		if( debug ) { showMessage( "standardDivorceLocation: " + selectQuery, false, true ); }

		standardLocation( debug, almmLocation, selectQuery, "id_person", "divorce_location", "divorce_location", source, TableType.PERSON );
	} // standardDivorceLocation


	/**
	 * @param debug
	 * @param almmLocation
	 * @param source
	 * @param rmtype
	 */
	public void standardLivingLocation( boolean debug, TableToArrayListMultimapHikari almmLocation, String source, String rmtype )
	{
		String selectQuery = "SELECT id_person , living_location FROM person_o WHERE id_source = " + source + " AND living_location <> ''";
		if ( ! rmtype.isEmpty() ) { selectQuery += " AND registration_maintype = " + rmtype; }
		if( debug ) { showMessage( "standardLivingLocation: " + selectQuery, false, true ); }

		standardLocation( debug, almmLocation, selectQuery, "id_person", "living_location", "living_location", source, TableType.PERSON );
	} // standardLivingLocation


	/**
	 * @param debug
	 * @param almmLocation
	 * @param source
	 * @param rmtype
	 */
	public void standardDeathLocation( boolean debug, TableToArrayListMultimapHikari almmLocation, String source, String rmtype )
	{
		String selectQuery = "SELECT id_person , death_location FROM person_o WHERE id_source = " + source + " AND death_location <> ''";
		if ( ! rmtype.isEmpty() ) { selectQuery += " AND registration_maintype = " + rmtype; }
		if( debug ) { showMessage( "standardDeathLocation: " + selectQuery, false, true ); }

		standardLocation( debug, almmLocation, selectQuery, "id_person", "death_location", "death_location", source, TableType.PERSON );
	} // standardDeathLocation


	/**
	 * @param debug
	 * @param almmLocation
	 * @param selectQuery
	 * @param idFieldO
	 * @param locationFieldO
	 * @param locationFieldC
	 * @param id_source
	 * @param tt
	 */
	private void standardLocation( boolean debug, TableToArrayListMultimapHikari almmLocation, String selectQuery, String idFieldO, String locationFieldO, String locationFieldC, String id_source, TableType tt )
	{
		long threadId = Thread.currentThread().getId();

		int count = 0;
		int count_empty = 0;
		int stepstate = count_step;

		try
		{
			ResultSet rs = dbconOriginal.executeQuery( selectQuery );
			rs.last();
			int total = rs.getRow();
			rs.beforeFirst();

			while( rs.next() )
			{
				count++;
				if( count == stepstate ) {
					long pct = Math.round( 100.0 * (float)count / (float)total );
					String msg = String.format( "Thread id %02d, standardLocation, %d-of-%d (%d%%)", threadId, count, total, pct );
					showMessage( msg, true, true );
					stepstate += count_step;
				}

				int id = rs.getInt(idFieldO);
				String location = rs.getString( locationFieldO );

				if( location != null && !location.isEmpty() )
				{
					location = location.toLowerCase();
					if( debug ) { System.out.println( "id_person: " + id + ", original: " + locationFieldO + ", location: " + location ); }
					if( almmLocation.contains( location ) )
					{
						String refSCode = almmLocation.code( location );
						if( debug ) { System.out.println( "refSCode: " + refSCode ); }

						if( refSCode.equals( SC_X ) )             // EC 91
						{
							if( tt == TableType.REGISTRATION )
							{
								addToReportRegistration( id, id_source, 91, location );
								String query = RegistrationC.updateIntQuery( locationFieldC, "10010", id );
								dbconCleaned.executeUpdate( query );
							}
							else
							{
								addToReportPerson( id, id_source, 91, location );
								String query = PersonC.updateIntQuery( locationFieldC, "10010", id );
								dbconCleaned.executeUpdate( query );
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

								String locationnumber = almmLocation.locationno( location );

								String query = RegistrationC.updateIntQuery( locationFieldC, locationnumber, id );
								dbconCleaned.executeUpdate( query );
							}
							else
							{
								addToReportPerson( id, id_source, 95, location );

								String locationnumber = almmLocation.locationno( location );

								String query = PersonC.updateIntQuery( locationFieldC, locationnumber, id );
								dbconCleaned.executeUpdate( query );
							}
						}
						else if( refSCode.equals( SC_Y ) )
						{
							if( tt == TableType.REGISTRATION )
							{
								String locationnumber = almmLocation.locationno( location );

								String query = RegistrationC.updateIntQuery( locationFieldC, locationnumber, id );
								dbconCleaned.executeUpdate( query );
							}
							else
							{
								String locationnumber = almmLocation.locationno( location );

								String query = PersonC.updateIntQuery( locationFieldC, locationnumber, id );
								dbconCleaned.executeUpdate( query );
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
							dbconCleaned.executeUpdate( query );
						}
						else
						{
							addToReportPerson( id, id_source, 91, location );
							String query = PersonC.updateIntQuery( locationFieldC, "10010", id );
							dbconCleaned.executeUpdate( query );
						}

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

			showMessage( String.format( "Thread id %02d; %d %s records, %d without location, %s", threadId, count, locationFieldO, count_empty, strNew ), false, true );
		}
		catch( Exception ex ) {
			String msg = String.format( "Thread id %02d; count: %d, Exception while cleaning Location: %s", threadId, count, ex.getMessage() );
			showMessage( msg, false, true );
			ex.printStackTrace( new PrintStream( System.out ) );
		}
	} // standardLocation

	/*---< Civil status and Sex >---------------------------------------------*/

	/**
	 * @param debug
	 * @param go
	 * @param source
	 * @param rmtype
	 * @throws Exception
	 */
	private void doStatusSex( boolean debug, boolean go, String source, String rmtype ) throws Exception
	{
		long threadId = Thread.currentThread().getId();

		String funcname = String.format( "Thread id %02d; doStatusSex for source: %s, rmtype: %s", threadId, source, rmtype );

		if( !go ) {
			if( showskip ) { showMessage( "Skipping " + funcname, false, true ); }
			return;
		}

		long timeStart = System.currentTimeMillis();
		showMessage( funcname + " ...", false, true );

		long start = System.currentTimeMillis();

		TableToArrayListMultimapHikari almmCivilstatus = new TableToArrayListMultimapHikari( dbconRefRead, "ref_status_sex", "original", "standard_civilstatus" );
		TableToArrayListMultimapHikari almmSex = new TableToArrayListMultimapHikari( dbconRefRead, "ref_status_sex", "original", "standard_sex" );

		showTimingMessage( String.format( "Thread id %02d; Loaded Civilstatus/Sex reference table", threadId ), start );

		int numrows = almmCivilstatus.numrows();
		int numkeys = almmCivilstatus.numkeys();
		showMessage( String.format( "Thread id %02d; Number of rows in reference table ref_status_sex: %d", threadId, numrows ), false, true );
		if( numrows != numkeys )
		{ showMessage( String.format( "Thread id %02d; Number of keys in arraylist multimap: %d", threadId, numkeys ), false, true ); }

		standardSex( debug, almmCivilstatus, almmSex, source, rmtype );
		standardCivilstatus( debug, almmCivilstatus, source, rmtype );

		while( almmCivilstatus.isBusy().get() ) {
			plog.show( String.format( "Thread id %02d; No permission to update ref table: Waiting 60 seconds", threadId ) );
			Thread.sleep( 60000 );
		}

		int newcount = almmCivilstatus.newcount();
		showMessage( String.format( "Thread id %02d; New entries for ref table: %d", threadId, newcount ), false, true );
		almmCivilstatus.updateTable( dbconRefWrite );

		almmCivilstatus.free();
		almmSex.free();

		elapsedShowMessage( funcname, timeStart, System.currentTimeMillis() );
		showMessage_nl();
	} // doStatusSex


	/**
	 * @param debug
	 * @param almmCivilstatus
	 * @param almmSex
	 * @param source
	 * @param rmtype
	 */
	public void standardSex( boolean debug, TableToArrayListMultimapHikari almmCivilstatus, TableToArrayListMultimapHikari almmSex, String source, String rmtype )
	{
		long threadId = Thread.currentThread().getId();

		int count = 0;
		int count_empty = 0;
		int stepstate = count_step;

		try
		{
			String selectQuery = "SELECT id_person , sex FROM person_o WHERE id_source = " + source;
			if ( ! rmtype.isEmpty() ) { selectQuery += " AND registration_maintype = " + rmtype; }
			if( debug ) { showMessage( selectQuery, false, true ); }

			ResultSet rs = dbconOriginal.executeQuery( selectQuery );
			rs.last();
			int total = rs.getRow();
			rs.beforeFirst();

			while( rs.next() )
			{
				count++;
				if( count == stepstate ) {
					long pct = Math.round( 100.0 * (float)count / (float)total );
					String msg = String.format( "Thread id %02d, standardSex, %d-of-%d (%d%%)", threadId, count, total, pct );
					showMessage( msg, true, true );
					stepstate += count_step;
				}

				int id_person = rs.getInt( "id_person" );

				String sex = rs.getString( "sex" ) != null ? rs.getString( "sex" ).toLowerCase() : "";
				if( sex.startsWith( "other:" ) ) { sex = sex.substring( 6 ); }

				if( debug ) { showMessage( "sex: " + sex , false, true ); }

				if( sex != null && !sex.isEmpty() )                 // check presence of the gender
				{
					if( almmSex.contains( sex ) )                   // check presence in original

					{
						String refSCode = almmSex.code( sex );
						if( debug ) { showMessage( "refSCode: " + refSCode , false, true ); }

						if( refSCode.equals( SC_X ) ) {
							if( debug ) { showMessage( "Warning 31: id_person: " + id_person + ", sex: " + sex, false, true ); }

							addToReportPerson( id_person, source, 31, sex );     // warning 31

							String query = PersonC.updateQuery( "sex", sex, id_person );
							dbconCleaned.executeUpdate( query );
						}
						else if( refSCode.equals( SC_N ) ) {
							if( debug ) { showMessage( "Warning 33: id_person: " + id_person + ", sex: " + sex, false, true ); }

							addToReportPerson( id_person, source, 33, sex );     // warning 33
						}
						else if( refSCode.equals( SC_U ) ) {
							if( debug ) { showMessage( "Warning 35: id_person: " + id_person + ", sex: " + sex, false, true ); }

							addToReportPerson( id_person, source, 35, sex );     // warning 35

							String query = PersonC.updateQuery( "sex", almmSex.standard(sex), id_person );
							dbconCleaned.executeUpdate( query );
						}
						else if( refSCode.equals( SC_Y ) ) {
							if( debug ) { showMessage( "Standard sex: id_person: " + id_person + ", sex: " + sex, false, true ); }

							String query = PersonC.updateQuery( "sex", almmSex.standard(sex), id_person );
							dbconCleaned.executeUpdate( query );
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
						almmCivilstatus.add( sex );         // only almmCivilstatus is used for update

						String query = PersonC.updateQuery( "sex", sex, id_person );
						dbconCleaned.executeUpdate( query );
					}
				}
				else { count_empty++; }
			}

			int count_new = almmCivilstatus.newcount();     // only almmCivilstatus is used for update
			String strNew = "";
			if( count_new == 0 ) { strNew = "no new genders"; }
			else if( count_new == 1 ) { strNew = "1 new gender"; }
			else { strNew = "" + count_new + " new genders"; }

			showMessage( String.format( "Thread id %02d; %d gender records, %d without gender, %s", threadId, count, count_empty, strNew ), false, true );
		}
		catch( Exception ex ) {
			String msg = String.format( "Thread id %02d; count: %d, Exception in standardSex: %s", threadId, count, ex.getMessage() );
			showMessage( msg, false, true );
			ex.printStackTrace( new PrintStream( System.out ) );
		}
	} // standardSex


	/**
	 * @param debug
	 * @param almmCivilstatus
	 * @param source
	 * @param rmtype
	 */
	public void standardCivilstatus( boolean debug, TableToArrayListMultimapHikari almmCivilstatus, String source, String rmtype )
	{
		long threadId = Thread.currentThread().getId();

		int count = 0;
		int count_empty = 0;
		int stepstate = count_step;

		int count_sex_new = almmCivilstatus.newcount();     // standardSex also writes to almmCivilstatus

		try
		{
			String selectQuery = "SELECT id_person , sex , civil_status FROM person_o WHERE id_source = " + source;
			if ( ! rmtype.isEmpty() ) { selectQuery += " AND registration_maintype = " + rmtype; }
			if( debug ) { showMessage( selectQuery, false, true ); }

			ResultSet rs = dbconOriginal.executeQuery( selectQuery );
			rs.last();
			int total = rs.getRow();
			rs.beforeFirst();

			while( rs.next() )
			{
				count++;
				if( count == stepstate ) {
					long pct = Math.round( 100.0 * (float)count / (float)total );
					String msg = String.format( "Thread id %02d, standardCivilstatus, %d-of-%d (%d%%)", threadId, count, total, pct );
					showMessage( msg, true, true );
					stepstate += count_step;
				}

				int id_person = rs.getInt( "id_person" );

				String sex = rs.getString( "sex" ) != null ? rs.getString( "sex" ).toLowerCase() : "";
				if( sex.startsWith( "other:" ) ) { sex = sex.substring( 6 ); }

				String civil_status = rs.getString( "civil_status" ) != null ? rs.getString( "civil_status" ).toLowerCase() : "";
				if( civil_status.startsWith( "other:" ) ) { civil_status = civil_status.substring( 6 ); }

				//showMessage( "id: " + id_person + ", sex: " + sex + ", civil: " + civil_status, false, true );

				if( civil_status != null && !civil_status.isEmpty() )       // check presence of civil status
				{
					if( almmCivilstatus.contains( civil_status ) )          // check presence in original
					{
						String refSCode = almmCivilstatus.code( civil_status );
						//showMessage( "code: " + refSCode, false, true );

						if( refSCode.equals( SC_X ) ) {
							addToReportPerson( id_person, source, 61, civil_status );            // warning 61

							String query = PersonC.updateQuery( "civil_status", civil_status, id_person );
							dbconCleaned.executeUpdate( query );
						}
						else if( refSCode.equals( SC_N ) ) {
							addToReportPerson( id_person, source, 63, civil_status );            // warning 63
						}
						else if( refSCode.equals( SC_U ) ) {
							addToReportPerson( id_person, source, 65, civil_status );            // warning 65

							String query = PersonC.updateQuery( "civil_status", almmCivilstatus.standard( civil_status ), id_person );
							dbconCleaned.executeUpdate( query );

							if( sex != null && !sex.isEmpty() ) {           // Extra check on sex
								if( !sex.equalsIgnoreCase( almmCivilstatus.value( "standard_sex", civil_status) ) ) {
									if( sex != SC_U ) {
										addToReportPerson(id_person, source, 68, civil_status);    // warning 68
									}
								}
							}
							else            // Sex is empty
							{
								String sexQuery = PersonC.updateQuery( "sex", almmCivilstatus.value("standard_sex", civil_status), id_person );
								dbconCleaned.executeUpdate( sexQuery );
							}

							String sexQuery = PersonC.updateQuery( "civil_status", almmCivilstatus.standard(civil_status), id_person );
							dbconCleaned.executeUpdate( sexQuery );
						}
						else if( refSCode.equals( SC_Y ) ) {
							String query = PersonC.updateQuery( "civil_status", almmCivilstatus.standard(civil_status), id_person );
							dbconCleaned.executeUpdate( query );

							if( sex == null || sex.isEmpty() )  {      // Sex is empty
								String sexQuery = PersonC.updateQuery( "sex", almmCivilstatus.value("standard_sex", civil_status), id_person );
								dbconCleaned.executeUpdate( sexQuery );
							}

							String sexQuery = PersonC.updateQuery( "civil_status", almmCivilstatus.standard(civil_status), id_person );
							dbconCleaned.executeUpdate( sexQuery );
						}
						else {          // Invalid SC
							addToReportPerson( id_person, source, 69, civil_status );               // warning 68
						}
					}
					else {      // add to ref
						if( debug ) { showMessage( "standardCivilstatus: not present in original", false, true ); }
						if( debug ) { showMessage( "Warning 31: id_person: " + id_person + ", sex: " + sex, false, true ); }

						addToReportPerson( id_person, source, 61, civil_status );                   // warning 61

						almmCivilstatus.add( civil_status );                                        // Add new civil_status

						String query = PersonC.updateQuery( "civil_status", civil_status, id_person );  // Write to Person
						dbconCleaned.executeUpdate( query );
					}
				}
				else { count_empty++; }
			}

			int count_civil_new = almmCivilstatus.newcount() - count_sex_new;
			String strNew = "";
			if( count_civil_new == 0 ) { strNew = "no new civil statusses"; }
			else if( count_civil_new == 1 ) { strNew = "1 new civil status"; }
			else { strNew = "" + count_civil_new + " new civil statusses"; }

			showMessage( String.format( "Thread id %02d; %d civil status records, %d without civil status, %s", threadId, count, count_empty, strNew ), false, true );
		}
		catch( Exception ex ) {
			String msg = String.format( "Thread id %02d; count: %d, Exception in standardCivilstatus: %s", threadId, count, ex.getMessage() );
			showMessage( msg, false, true );
			ex.printStackTrace( new PrintStream( System.out ) );
		}
	} // standardCivilstatus


	/*---< Registration Type >------------------------------------------------*/

	/**
	 * @param debug
	 * @param go
	 * @param source
	 * @param rmtype
	 * @throws Exception
	 */
	private void doRegistrationType( boolean debug, boolean go, String source, String rmtype ) throws Exception
	{
		long threadId = Thread.currentThread().getId();

		String funcname = String.format( "Thread id %02d; doRegistrationType for source: %s, rmtype: %s", threadId, source, rmtype );

		if( !go ) {
			if( showskip ) { showMessage( "Skipping " + funcname, false, true ); }
			return;
		}

		long timeStart = System.currentTimeMillis();
		showMessage( funcname + " ...", false, true );

		long start = System.currentTimeMillis();

		TableToArrayListMultimapHikari almmRegistration = new TableToArrayListMultimapHikari( dbconRefRead, "ref_registration", "original", "standard" );

		showTimingMessage( String.format( "Thread id %02d; Loaded Registration reference table", threadId ), start );

		int numrows = almmRegistration.numrows();
		int numkeys = almmRegistration.numkeys();
		showMessage( String.format( "Thread id %02d; Number of rows in reference table ref_registration: %d", threadId, numrows ), false, true );
		if( numrows != numkeys )
		{ showMessage( String.format( "Thread id %02d; Number of keys in arraylist multimap: %d", threadId, numkeys ), false, true ); }

		// glue original with registration_maintype
        /*
        Note that split() takes a regular expression, so remember to escape special characters if necessary.
        there are 12 characters with special meanings: the backslash \,
        the caret ^, the dollar sign $, the period or dot ., the vertical bar or pipe symbol |, the question mark ?,
        the asterisk or star *, the plus sign +, the opening parenthesis (, the closing parenthesis ),
        and the opening square bracket [, the opening curly brace {, These special characters are often called "metacharacters".
        */
		String delimiter = "_";
		standardRegistrationType( debug, almmRegistration, delimiter, source, rmtype );

		while( almmRegistration.isBusy().get() ) {
			plog.show( String.format( "Thread id %02d; No permission to update ref table: Waiting 60 seconds", threadId ) );
			Thread.sleep( 60000 );
		}

		String extra_col = "main_type";
		int newcount = almmRegistration.newcount();
		showMessage( String.format( "Thread id %02d; New entries for ref table: %d", threadId, newcount ), false, true );
		almmRegistration.updateTable( dbconRefWrite, extra_col, delimiter );

		almmRegistration.free();

		elapsedShowMessage( funcname, timeStart, System.currentTimeMillis() );
		showMessage_nl();
	} // doRegistrationType


	/**
	 * @param debug
	 * @param almmRegistration
	 * @param delimiter
	 * @param source
	 * @param rmtype
	 */
	public void standardRegistrationType( boolean debug, TableToArrayListMultimapHikari almmRegistration, String delimiter, String source, String rmtype )
	{
		long threadId = Thread.currentThread().getId();
		int count = 0;
		int stepstate = count_step;

		try
		{
			String selectQuery = "SELECT id_registration, registration_maintype, registration_type";
			selectQuery += " FROM registration_o WHERE id_source = " + source;
			if ( ! rmtype.isEmpty() ) { selectQuery += " AND registration_maintype = " + rmtype; }
			if( debug ) { showMessage( selectQuery, false, true ); }

			ResultSet rs = dbconOriginal.executeQuery( selectQuery );
			rs.last();
			int total = rs.getRow();
			rs.beforeFirst();

			while( rs.next() )      // process data from links_original
			{
				count++;
				if( count == stepstate ) {
					long pct = Math.round( 100.0 * (float)count / (float)total );
					String msg = String.format( "Thread id %02d, standardRegistrationType, %d-of-%d (%d%%)", threadId, count, total, pct );
					showMessage( msg, true, true );
					stepstate += count_step;
				}

				int id_registration = rs.getInt( "id_registration" );
				String registration_maintype = rs.getString( "registration_maintype" );
				String registration_type = rs.getString( "registration_type" ) != null ? rs.getString( "registration_type" ).toLowerCase() : "";

				if( almmRegistration.contains( registration_type ) )
				{
					//String refSCode = ref.getString( "standard_code" ).toLowerCase();
					String refSCode = almmRegistration.code( registration_type );
					if( debug ) { System.out.println( "refSCode: " + refSCode ); }

					if( refSCode.equals( SC_X ) ) {
						if( debug ) { showMessage( "Warning 51: id_registration: " + id_registration + ", reg type: " + registration_type, false, true ); }

						addToReportRegistration( id_registration, source, 51, registration_type );       // warning 51

						String query_r = RegistrationC.updateQuery( "registration_type", registration_type, id_registration );
						dbconCleaned.executeUpdate( query_r );
					}
					else if( refSCode.equals( SC_N ) ) {
						if( debug ) { showMessage( "Warning 53: id_registration: " + id_registration + ", reg type: " + registration_type, false, true ); }

						addToReportRegistration( id_registration, source, 53, registration_type );       // warning 53
					}
					else if( refSCode.equals( SC_U ) ) {
						if( debug ) { showMessage( "Warning 55: id_registration: " + id_registration + ", reg type: " + registration_type, false, true ); }

						addToReportRegistration( id_registration, source, 55, registration_type );       // warning 55

						String refSRegisType = almmRegistration.standard( registration_type );

						String query_r = RegistrationC.updateQuery( "registration_type", refSRegisType, id_registration );
						dbconCleaned.executeUpdate( query_r );
					}
					else if( refSCode.equals( SC_Y ) ) {
						if( debug ) { showMessage( "Standard reg type: id_person: " + id_registration + ", reg type: " + registration_type, false, true ); }

						String refSRegisType = almmRegistration.standard( registration_type );

						String query_r = RegistrationC.updateQuery( "registration_type", refSRegisType, id_registration );
						dbconCleaned.executeUpdate( query_r );
					}
					else {    // invalid SC
						if( debug ) { showMessage( "Warning 59: id_registration: " + id_registration + ", reg type: " + registration_type, false, true ); }

						addToReportRegistration( id_registration, source, 59, registration_type );       // warning 59
					}
				}
				else
				{      // not in reference; add to reference with SC_X
					if( debug ) { showMessage( "Warning 51 (not in ref): id_registration: " + id_registration + ", reg type: " + registration_type, false, true ); }

					addToReportRegistration( id_registration, source, 51, registration_type );           // warning 51

					almmRegistration.add( registration_type + delimiter + registration_maintype );

					// update links_cleaned.registration_c
					String query_r = RegistrationC.updateQuery( "registration_type", registration_type, id_registration );
					dbconCleaned.executeUpdate( query_r );
				}
			}

			// update links_cleaned.person_c.registration_type
			String query_p = "UPDATE links_cleaned.person_c, links_cleaned.registration_c"
				+ " SET person_c.registration_type = registration_c.registration_type"
				+ " WHERE person_c.id_registration = registration_c.id_registration"
				+ " AND person_c.id_source = " + source;
			if ( ! rmtype.isEmpty() ) { query_p += " AND person_c.registration_maintype = " + rmtype; }
			if( debug ) { showMessage( query_p, false, true ); }

			int nrec = dbconCleaned.executeUpdate( query_p );
			String msg = String.format( "Thread id %02d; person_c.registration_type: %d rows updated", threadId, nrec  );
			showMessage( msg, false, true );
		}
		catch( Exception ex ) {
			String msg = String.format( "Thread id %02d; count: %d, Exception in standardRegistrationType: %s", threadId, count, ex.getMessage() );
			showMessage( msg, false, true );
			ex.printStackTrace( new PrintStream( System.out ) );
		}
	} // standardRegistrationType


	/*---< Occupation >-------------------------------------------------------*/

	/**
	 * @param debug
	 * @param go
	 * @param source
	 * @param rmtype
	 * @throws Exception
	 */
	private void doOccupation( boolean debug, boolean go, String source, String rmtype ) throws Exception
	{
		long threadId = Thread.currentThread().getId();

		String funcname = String.format( "Thread id %02d; doOccupation for source: %s, rmtype: %s", threadId, source, rmtype );

		if( !go ) {
			if( showskip ) { showMessage( "Skipping " + funcname, false, true ); }
			return;
		}

		long funcstart = System.currentTimeMillis();
		showMessage( funcname + " ...", false, true );

		long start = System.currentTimeMillis();

		TableToArrayListMultimapHikari almmOccupation = new TableToArrayListMultimapHikari( dbconRefRead, "ref_occupation", "original", "standard" );

		showTimingMessage( String.format( "Thread id %02d; Loaded Occupation reference table", threadId ), start );

		int numrows = almmOccupation.numrows();
		int numkeys = almmOccupation.numkeys();
		showMessage( String.format( "Thread id %02d; Number of rows in reference table ref_occupation: %d", threadId, numrows ), false, true );
		if( numrows != numkeys )
		{ showMessage( String.format( "Thread id %02d; Number of keys in arraylist multimap: %d", threadId, numkeys ), false, true ); }

		String msg = String.format( "Thread id %02d; Processing standardOccupation for source: %s", threadId, source );
		showMessage( msg, false, true );
		standardOccupation( debug, almmOccupation, source, rmtype );

		msg = String.format( "Thread id %02d; Updating ref_occupation", threadId );
		showMessage( msg, false, true );

		while( almmOccupation.isBusy().get() ) {
			plog.show( String.format( "Thread id %02d; No permission to update ref_occupation: Waiting 60 seconds", threadId ) );
			Thread.sleep( 60000 );
		}

		int newcount = almmOccupation.newcount();
		showMessage( String.format( "Thread id %02d; New entries for ref table: %d", threadId, newcount ), false, true );
		almmOccupation.updateTable( dbconRefWrite );

		almmOccupation.free();
		showMessage( String.format( "Thread id %02d; Freed almmOccupation", threadId ), false, true );

		elapsedShowMessage( funcname, funcstart, System.currentTimeMillis() );
		showMessage_nl();
	} // doOccupation


	/**
	 * @param debug
	 * @param almmOccupation
	 * @param source
	 * @param rmtype
	 */
	public void standardOccupation( boolean debug, TableToArrayListMultimapHikari almmOccupation, String source, String rmtype )
	{
		long threadId = Thread.currentThread().getId();

		int count = 0;
		int count_empty = 0;
		int stepstate = count_step;

		String query = "SELECT id_person , occupation FROM person_o WHERE id_source = " + source;
		if ( ! rmtype.isEmpty() ) { query += " AND registration_maintype = " + rmtype; }
		if( debug ) { showMessage( query, false, true ); }

		try
		{
			ResultSet rs = dbconOriginal.executeQuery( query );           // Get occupation
			rs.last();
			int total = rs.getRow();
			rs.beforeFirst();

			while( rs.next() )
			{
				count++;
				if( count == stepstate ) {
					long pct = Math.round( 100.0 * (float)count / (float)total );
					String msg = String.format( "Thread id %02d, standardOccupation, %d-of-%d (%d%%)", threadId, count, total, pct );
					showMessage( msg, true, true );
					stepstate += count_step;
				}

				int id_person = rs.getInt( "id_person" );
				String occupation = rs.getString( "occupation") != null ? rs.getString( "occupation" ).toLowerCase() : "";
				if( occupation.isEmpty() ) {
					count_empty += 1;
				}
				else {
					if( debug ) { showMessage( "count: " + count + ", id_person: " + id_person + ", occupation: " + occupation, false, true ); }
					standardOccupationRecord( debug, almmOccupation, count, source, id_person, occupation );
				}
			}

			int count_new = almmOccupation.newcount();
			String strNew = "";
			if( count_new == 0 ) { strNew = "no new occupations"; }
			else if( count_new == 1 ) { strNew = "1 new occupation"; }
			else { strNew = "" + count_new + " new occupations"; }

			showMessage( String.format( "Thread id %02d; %d occupation records, %d without occupation, %s", threadId, count, count_empty, strNew ), false, true );
		}
		catch( Exception ex ) {
			String msg = String.format( "Thread id %02d; count: %d, Exception in standardOccupation: %s", threadId, count, ex.getMessage() );
			showMessage( msg, false, true );
			ex.printStackTrace( new PrintStream( System.out ) );
		}

	} // standardOccupation


	/**
	 * @param debug
	 * @param almmOccupation
	 * @param count
	 * @param source
	 * @param id_person
	 * @param occupation
	 */
	public void standardOccupationRecord( boolean debug, TableToArrayListMultimapHikari almmOccupation, int count, String source, int id_person, String occupation )
	{
		long threadId = Thread.currentThread().getId();

		try
		{
			if( !occupation.isEmpty() )                 // check presence of the occupation
			{
				boolean exists = false;
				exists = almmOccupation.contains( occupation );
				if( exists )
				{
					//showMessage( "old: " + occupation, false, true );
					if( debug ) { showMessage("getStandardCodeByOriginal: " + occupation, false, true ); }

					String refSCode = almmOccupation.code( occupation );

					if( debug ) { showMessage( "refSCode: " + refSCode, false, true ); }

					if( refSCode.equals( SC_X ) )
					{
						if( debug ) { showMessage( "Warning 41 (via SC_X): id_person: " + id_person + ", occupation: " + occupation, false, true ); }
						addToReportPerson( id_person, source, 41, occupation );      // warning 41

						String query = PersonC.updateQuery( "occupation", occupation, id_person );
						dbconCleaned.executeUpdate( query );
					}
					else if( refSCode.equals( SC_N ) )
					{
						if( debug ) { showMessage( "Warning 43: id_person: " + id_person + ", occupation: " + occupation, false, true ); }
						addToReportPerson( id_person, source, 43, occupation );      // warning 43
					}
					else if( refSCode.equals(SC_U) )
					{
						if( debug ) { showMessage( "Warning 45: id_person: " + id_person + ", occupation: " + occupation, false, true ); }
						addToReportPerson( id_person, source, 45, occupation );      // warning 45

						String refOccupation = almmOccupation.standard( occupation );

						String query = PersonC.updateQuery( "occupation", refOccupation, id_person );
						dbconCleaned.executeUpdate( query );
					}
					else if( refSCode.equals( SC_Y ) )
					{
						String refOccupation = almmOccupation.standard( occupation );

						if( debug ) { showMessage( "occupation: " + refOccupation, false, true ); }

						String query = PersonC.updateQuery( "occupation", refOccupation, id_person );
						dbconCleaned.executeUpdate( query );
					}
					else      // Invalid standard code
					{
						if( debug ) { showMessage( "Warning 49: id_person: " + id_person + ", occupation: " + occupation, false, true ); }
						addToReportPerson( id_person, source, 49, occupation );      // warning 49
					}
				}
				else        // not present, collect as new
				{
					//showMessage( "new: " + occupation, false, true );
					if( debug ) { showMessage( "Warning 41 (not in ref_): id_person: " + id_person + ", occupation: " + occupation, false, true ); }
					addToReportPerson( id_person, source, 41, occupation );       // warning 41

					almmOccupation.add( occupation );

					String query = PersonC.updateQuery( "occupation", occupation, id_person );
					dbconCleaned.executeUpdate( query );
				}
			}
		}
		catch( Exception ex ) {
			String msg = String.format( "Thread id %02d; count: %d, Exception in standardOccupationRecord: %s", threadId, count, ex.getMessage() );
			showMessage( msg, false, true );
			ex.printStackTrace( new PrintStream( System.out ) );
		}
	} // standardOccupationRecord


	/*---< Age, Role, Dates >-------------------------------------------------*/

	/**
	 * @param debug
	 * @param go
	 * @param source
	 * @param rmtype
	 * @throws Exception
	 */
	private void doAge( boolean debug, boolean go, String source, String rmtype ) throws Exception
	{
		long threadId = Thread.currentThread().getId();

		String funcname = String.format( "Thread id %02d; doAge for source %s, rmtype: %s", threadId, source, rmtype );

		if( !go ) {
			if( showskip ) { showMessage( "Skipping " + funcname, false, true ); }
			return;
		}

		long timeStart = System.currentTimeMillis();
		showMessage( funcname + " ...", false, true );

		long start = System.currentTimeMillis();

		TableToArrayListMultimapHikari almmLitAge = new TableToArrayListMultimapHikari( dbconRefRead, "ref_age", "original", "standard_year" );

		showTimingMessage( String.format( "Thread id %02d; Loaded LitAge reference table", threadId ), start );

		int numrows = almmLitAge.numrows();
		int numkeys = almmLitAge.numkeys();
		showMessage( String.format( "Thread id %02d; Number of rows in reference table ref_age: %d", threadId, numrows ), false, true );
		if( numrows != numkeys )
		{ showMessage( String.format( "Thread id %02d; Number of keys in arraylist multimap: %d", threadId, numkeys ), false, true ); }

		long timeSAL = System.currentTimeMillis();
		String msg = String.format( "Thread id %02d; Processing standardAgeLiteral for source: %s, rmtype: %s ...", threadId, source, rmtype );
		showMessage( msg, false, true );
		standardAgeLiteral( debug, almmLitAge, source, rmtype );
		msg = String.format( "Thread id %02d; Processing standardAgeLiteral for source: %s, rmtype: %s ", threadId, source, rmtype );
		elapsedShowMessage( msg, timeSAL, System.currentTimeMillis() );

		long timeSA = System.currentTimeMillis();
		msg = String.format( "Thread id %02d; Processing standardAge for source: %s, rmtype: %s ...", threadId, source, rmtype );
		showMessage( msg, false, true );
		standardAge( debug, source, rmtype );
		msg = String.format( "Thread id %02d; Processing standardAge for source: %s, rmtype: %s ", threadId, source, rmtype );
		elapsedShowMessage( msg, timeSA, System.currentTimeMillis() );

		msg = String.format( "Thread id %02d; Updating ref_age: ref_age", threadId );
		showMessage( msg, false, true );

		while( almmLitAge.isBusy().get() ) {
			plog.show( "No permission to update ref table: Waiting 60 seconds" );
			Thread.sleep( 60000 );
		}

		int newcount = almmLitAge.newcount();
		showMessage( String.format( "Thread id %02d; New entries for ref table: %d", threadId, newcount ), false, true );
		almmLitAge.updateTable( dbconRefWrite );

		almmLitAge.free();
		showMessage( String.format( "Thread id %02d; Freed almmLitAge", threadId ), false, true );

		elapsedShowMessage( funcname, timeStart, System.currentTimeMillis() );
		showMessage_nl();
	} // doAge


	/**
	 * @param debug
	 * @param almmLitAge
	 * @param source
	 * @param rmtype
	 */
	public void standardAgeLiteral( boolean debug, TableToArrayListMultimapHikari almmLitAge, String source, String rmtype )
	{
		long threadId = Thread.currentThread().getId();
		int count = 0;
		int stepstate = count_step;

		try
		{
			String selectQuery = "SELECT id_person , id_registration , role, age_literal , age_year , age_month , age_week , age_day ";
			selectQuery += "FROM links_original.person_o WHERE id_source = " + source;
			if ( ! rmtype.isEmpty() ) { selectQuery += " AND registration_maintype = " + rmtype; }
			if( debug ) { showMessage( selectQuery, false, true ); }

			ResultSet rs = dbconOriginal.executeQuery( selectQuery );
			rs.last();
			int total = rs.getRow();
			rs.beforeFirst();

			while( rs.next() )
			{
				count++;
				if( count == stepstate ) {
					long pct = Math.round( 100.0 * (float)count / (float)total );
					String msg = String.format( "Thread id %02d, standardAgeLiteral, %d-of-%d (%d%%)", threadId, count, total, pct );
					showMessage( msg, true, true );
					stepstate += count_step;
				}

				int id_registration  = rs.getInt( "id_registration" );
				int id_person        = rs.getInt( "id_person" );

				String role          = rs.getString( "role" );
				String age_literal   = rs.getString( "age_literal" );
				String age_day_str   = rs.getString( "age_day" );
				String age_week_str  = rs.getString( "age_week" );
				String age_month_str = rs.getString( "age_month" );
				String age_year_str  = rs.getString( "age_year" );

				// clear destination fields
				String updateQuery = ""
					+ "UPDATE links_cleaned.person_c SET"
					+ " age_literal = null,"
					+ " age_day = null,"
					+ " age_week = null,"
					+ " age_month = null,"
					+ " age_year = null"
					+ " WHERE id_person = " + "" + id_person;
				//if( debug ) { showMessage( updateQuery, false, true ); }
				dbconCleaned.executeUpdate( updateQuery );

				if( ( age_literal   == null || age_literal.isEmpty() ) &&
					( age_day_str   == null || age_day_str.isEmpty() ) &&
					( age_week_str  == null || age_week_str.isEmpty() ) &&
					( age_month_str == null || age_month_str.isEmpty() ) &&
					( age_year_str  == null || age_year_str.isEmpty() )
				)
				{ continue; }           // nothing (more) to do

				if( age_literal == null ) { age_literal = ""; }
				if( role == null ) { role = ""; }

				//if(  id_registration == 36229656 ) { debug = true; }
				//else { debug = false; continue; }

				//if(  id_person == 117528653 ) { debug = true; }
				//else { debug = false; continue; }

				// strings to ints
				int age_day   = 0;
				int age_week  = 0;
				int age_month = 0;
				int age_year  = 0;

				updateQuery = "UPDATE links_cleaned.person_c SET";

				if( age_day_str == null )
				{ updateQuery += " age_day = null"; }
				else
				{
					try { age_day = Integer.parseInt( age_day_str ); }
					catch( NumberFormatException nfe ) { ; }
					updateQuery += " age_day = " + age_day;
				}

				if( age_week_str == null )
				{ updateQuery += ", age_week = null"; }
				else
				{
					try { age_week = Integer.parseInt( age_week_str ); }
					catch( NumberFormatException nfe ) { ; }
					updateQuery += ", age_week = " + age_week;
				}

				if( age_month_str == null )
				{ updateQuery += ", age_month = null"; }
				else
				{
					try { age_month = Integer.parseInt( age_month_str ); }
					catch( NumberFormatException nfe ) { ; }
					updateQuery += ", age_month = " + age_month;
				}

				if( age_year_str == null )
				{ updateQuery += ", age_year = null"; }
				else
				{
					try { age_year = Integer.parseInt( age_year_str ); }
					catch( NumberFormatException nfe ) { ; }
					updateQuery += ", age_year = " + age_year;
				}
				updateQuery += " WHERE id_person = " + "" + id_person;


				if( age_literal.isEmpty() )     // write date components, and done
				{
					if( debug ) { showMessage( updateQuery, false, true ); }
					dbconCleaned.executeUpdate( updateQuery );
					continue;
				}

				if( debug ) { showMessage( "id_person: " + id_person + ", age_literal: " + age_literal, false, true ); }

				// birth certificates with role = Kind must have empty age_literal
				if( ! ( age_literal.isEmpty() ) && role.equalsIgnoreCase( "kind" ) )
				{
					if( debug ) {
						String msg = String.format( "id_person: %d, role: %s, age_literal: %s", id_person, role, age_literal );
						showMessage( msg, false, true );
					}

					addToReportPerson( id_person, source, 267, age_literal + " Not empty" );    // warning 267
					continue;
				}

				boolean numeric = true;
				int lit_year = 0;

				if( age_literal == null ) { continue; }
				else
				{
					try {
						lit_year = Integer.parseInt( age_literal );
						numeric = true;
						if( debug ) { showMessage( "lit_year: " + lit_year, false, true ); }
					}
					catch( NumberFormatException nfe ) {
						numeric = false;        // not a single number
						if( debug ) { showMessage( "not a single number", false, true ); }
					}
					catch( Exception lex ) {
						numeric = false;        // not a single number
						showMessage( "count: " + count + " Exception during standardAgeLiteral: " + lex.getMessage(), false, true );
						lex.printStackTrace( new PrintStream( System.out ) );
					}
				}

				if( numeric && lit_year > 0 && lit_year < 115 )
				{
					String standard_year_str = almmLitAge.value( "standard_year", age_literal );
					if( standard_year_str.isEmpty() ) { standard_year_str = "0"; }
					int standard_year = Integer.parseInt( standard_year_str );

					if( age_year > 0 && age_year != standard_year )
					{ addToReportPerson( id_person, source, 261, lit_year + " != " + standard_year ); }    // warning 261

					String queryLiteral = PersonC.updateQuery( "age_literal", age_literal, id_person );
					if( debug ) { showMessage( queryLiteral, false, true ); }
					dbconCleaned.executeUpdate( queryLiteral );

					String queryYear = PersonC.updateQuery( "age_year", age_literal, id_person );
					if( debug ) { showMessage( queryYear, false, true ); }
					dbconCleaned.executeUpdate( queryYear );

					continue;       // done
				}

				boolean check4 = false;
				boolean exists = false;
				exists = almmLitAge.contains( age_literal );
				if( exists )
				{
					String refSCode = almmLitAge.code(age_literal);
					if( debug ) { showMessage( "refSCode: " + refSCode, false, true ); }

					if( refSCode.equals( SC_X ) )
					{
						if( debug ) { showMessage( "Warning 251 (via SC_X): id_person: " + id_person + ", age_literal: " + age_literal, false, true ); }
						addToReportPerson( id_person, source, 251, age_literal );      // warning 251

						String query = PersonC.updateQuery( "age_literal", age_literal, id_person);
						dbconCleaned.executeUpdate( query );
					}
					else if( refSCode.equals( SC_N ) )
					{
						if( debug ) { showMessage( "Warning 253: id_person: " + id_person + ", age_literal: " + age_literal, false, true ); }
						addToReportPerson( id_person, source, 253, age_literal );      // warning 253
					}
					else if( refSCode.equals( SC_U ) )
					{
						check4 = true;      // below

						if (debug) { showMessage("Warning 255: id_person: " + id_person + ", age_literal: " + age_literal, false, true ); }
						addToReportPerson(id_person, source, 255, age_literal);      // warning 255

						String standard_year_str = almmLitAge.value( "standard_year", age_literal );

						String query = PersonC.updateQuery("age_literal", standard_year_str, id_person);
						dbconCleaned.executeUpdate(query);
					}
					else if( refSCode.equals( SC_Y ) )
					{
						check4 = true;      // below
					}
					else      // Invalid standard code
					{
						if( debug ) { showMessage("Warning 259: id_person: " + id_person + ", age_literal: " + age_literal, false, true ); }
						addToReportPerson( id_person, source, 259, age_literal );      // warning 259
					}

					if( check4 )
					{
						String standard_year_str  = almmLitAge.value( "standard_year",  age_literal );
						String standard_month_str = almmLitAge.value( "standard_month", age_literal );
						String standard_week_str  = almmLitAge.value( "standard_week",  age_literal );
						String standard_day_str   = almmLitAge.value( "standard_day",   age_literal );

						int standard_year  = Integer.parseInt( standard_year_str );
						int standard_month = Integer.parseInt( standard_month_str );
						int standard_week  = Integer.parseInt( standard_week_str );
						int standard_day   = Integer.parseInt( standard_day_str );

						if( debug ) {
							showMessage( "age_literal: " + age_literal + ", year: " + standard_year + ", month: "
								+ standard_month + ", week: " + standard_week + ", day: " + standard_day, false, true );
						}

						String query = PersonC.updateQuery( "age_literal", age_literal, id_person );
						dbconCleaned.executeUpdate( query );

						if( age_year > 0 && age_year != standard_year )
						{ addToReportPerson( id_person, source, 261, standard_year_str ); }     // warning 261

						query = PersonC.updateQuery( "age_year", standard_year_str, id_person );
						dbconCleaned.executeUpdate( query );

						if( age_month > 0 && age_month != standard_month )
						{ addToReportPerson( id_person, source, 262, standard_month_str ); }  // warning 262

						query = PersonC.updateQuery( "age_month", standard_month_str, id_person );
						dbconCleaned.executeUpdate( query );

						if( age_week > 0 && age_week != standard_week )
						{ addToReportPerson( id_person, source, 263, standard_week_str ); }     // warning 263

						query = PersonC.updateQuery( "age_week", standard_week_str, id_person );
						dbconCleaned.executeUpdate( query );

						if( age_day > 0 && age_day != standard_day )
						{ addToReportPerson( id_person, source, 264, standard_day_str ); }        // warning 264

						query = PersonC.updateQuery( "age_day", standard_day_str, id_person );
						dbconCleaned.executeUpdate( query );
					}
				}
				else        // not present in ref, collect as new
				{
					if (debug) { showMessage( "Warning 251: id_person: " + id_person + ", age_literal: " + age_literal, false, true ); }
					addToReportPerson( id_person, source, 251, age_literal );      // warning 251

					almmLitAge.add( age_literal );

					String query = PersonC.updateQuery( "age_literal", age_literal, id_person );
					dbconCleaned.executeUpdate( query );
				}
			}
		}
		catch( Exception ex ) {
			String msg = String.format( "Thread id %02d; count: %d, Exception in standardAgeLiteral: %s", threadId, count, ex.getMessage() );
			showMessage( msg, false, true );
			ex.printStackTrace( new PrintStream( System.out ) );
		}
	} // standardAgeLiteral


	/**
	 * @param debug
	 * @param source
	 * @param rmtype
	 */
	public void standardAge( boolean debug, String source, String rmtype )
	{
		long threadId = Thread.currentThread().getId();
		int count = 0;
		int stepstate = count_step;

		try
		{
			String selectQuery = "SELECT id_registration, id_person , age_year , age_month , age_week , age_day ";
			selectQuery += "FROM links_original.person_o WHERE id_source = " + source;
			if ( ! rmtype.isEmpty() ) { selectQuery += " AND registration_maintype = " + rmtype; }
			if( debug ) { showMessage( selectQuery, false, true ); }

			ResultSet rs = dbconOriginal.executeQuery( selectQuery );
			rs.last();
			int total = rs.getRow();
			rs.beforeFirst();

			while( rs.next() )
			{
				count++;
				if( count == stepstate ) {
					long pct = Math.round( 100.0 * (float)count / (float)total );
					String msg = String.format( "Thread id %02d, standardAge, %d-of-%d (%d%%)", threadId, count, total, pct );
					showMessage( msg, true, true );
					stepstate += count_step;
				}

				int id_registration = rs.getInt( "id_registration" );
				int id_person       = rs.getInt( "id_person" );

				//if(  id_person == 117528653 ) { debug = true; }
				//else { debug = false; continue; }

				//String msg = String.format( "id_registration: %d, ", id_registration );
				//    showMessage( msg, true, true );
                /*
                if(  id_registration == 36229656 ) {
                    debug = true;
                    { showMessage( "id_registration == 36229656", false, true ); }
                }
                else { debug = false; continue; }
                */

				int age_day   = 0;
				int age_week  = 0;
				int age_month = 0;
				int age_year  = 0;

				String age_day_str   = rs.getString( "age_day" );
				String age_week_str  = rs.getString( "age_week" );
				String age_month_str = rs.getString( "age_month" );
				String age_year_str  = rs.getString( "age_year" );

				if( ! ( age_day_str == null || age_day_str.isEmpty() ) ) {
					try { age_day = Integer.parseInt( age_day_str ); }
					catch( NumberFormatException nfe ) { ; }
				}

				if( ! ( age_week_str == null || age_week_str.isEmpty() ) ) {
					try { age_week = Integer.parseInt( age_week_str ); }
					catch( NumberFormatException nfe ) { ; }
				}

				if( ! ( age_month_str == null || age_month_str.isEmpty() ) ) {
					try { age_month = Integer.parseInt( age_month_str ); }
					catch( NumberFormatException nfe ) { ; }
				}

				if( ! ( age_year_str == null || age_year_str.isEmpty() ) ) {
					try { age_year = Integer.parseInt( age_year_str ); }
					catch( NumberFormatException nfe ) { ; }
				}


				boolean update = false;

				if( ( age_year >= 0 ) && ( age_year < 115 ) )
				{ update = true; }
				else
				{
					addToReportPerson( id_person, source, 244, age_year + "" );
					//System.out.println( "standardAge: " + age_year );

					String updateQuery = "UPDATE links_cleaned.person_c SET age_year = null"
						+ " WHERE id_person = " + id_person + " AND id_source = " + source;

					if( debug ) { showMessage( updateQuery, false, true ); }
					dbconCleaned.executeUpdate( updateQuery );
				}

				if( ( age_month >= 0 ) && ( age_month < 50 ) ) { update = true; }
				else { addToReportPerson( id_person, source, 243, age_month + "" ); }

				if( ( age_week >= 0 ) && ( age_week < 53 ) ) { update = true; }
				else { addToReportPerson( id_person, source, 242, age_week + "" ); }

				if( ( age_day >= 0 ) && ( age_day < 100 ) ) { update = true; }
				else { addToReportPerson( id_person, source, 241, age_day + "" ); }

				if( update )
				{
					// Notice: death = 'a' means alive
					if( age_day > 0 || age_week > 0 || age_month > 0 || age_year > 0 )
					{
						String updateQuery = "UPDATE links_cleaned.person_c SET death = 'a'"
							+ " WHERE id_person = " + id_person + " AND id_source = " + source;

						if( debug ) { showMessage( updateQuery, false, true ); }
						dbconCleaned.executeUpdate( updateQuery );
					}
				}
			}
			//showMessage( count + " person records", false, true );
		}
		catch( Exception ex ) {
			String msg = String.format( "Thread id %02d; count: %d, Exception in standardAge: %s", threadId, count, ex.getMessage() );
			showMessage( msg, false, true );
			ex.printStackTrace( new PrintStream( System.out ) );
		}
	} // standardAge


	/**
	 * @param debug
	 * @param go
	 * @param source
	 * @param rmtype
	 * @throws Exception
	 */
	private void doRole( boolean debug, boolean go, String source, String rmtype ) throws Exception
	{
		long threadId = Thread.currentThread().getId();

		String funcname = String.format( "Thread id %02d; doRole for source: %s, rmtype %s", threadId, source, rmtype );

		if( !go ) {
			if( showskip ) { showMessage( "Skipping " + funcname, false, true ); }
			return;
		}

		showMessage( funcname + " ...", false, true );

		long timeStart = System.currentTimeMillis();

		TableToArrayListMultimapHikari almmRole = new TableToArrayListMultimapHikari( dbconRefRead, "ref_role", "original", "standard" );

		showTimingMessage( String.format( "Thread id %02d; Loaded Role reference table", threadId ), timeStart );

		int numrows = almmRole.numrows();
		int numkeys = almmRole.numkeys();
		showMessage( String.format( "Thread id %02d; Number of rows in reference table ref_role: %d", threadId, numrows ), false, true );
		if( numrows != numkeys )
		{ showMessage( String.format( "Thread id %02d; Number of keys in arraylist multimap: %d", threadId, numkeys ), false, true ); }

		String msg = String.format( "Thread id %02d; Processing standardRole for source: %s ...", threadId, source );
		showMessage( msg, false, true );

		standardRole( debug, almmRole, source, rmtype );

		while( almmRole.isBusy().get() ) {
			plog.show( "No permission to update ref_role: Waiting 60 seconds" );
			Thread.sleep( 60000 );
		}

		int newcount = almmRole.newcount();
		showMessage( String.format( "Thread id %02d; New entries for ref table: %d", threadId, newcount ), false, true );
		almmRole.updateTable( dbconRefWrite );

		almmRole.free();
		showMessage( String.format( "Thread id %02d; Freed almmRole", threadId ), false, true );

		msg = String.format( "Thread id %02d; Processing standardAlive for source: %s ...", threadId, source );
		showMessage( msg, false, true );
		standardAlive( debug, source, rmtype );

		elapsedShowMessage( funcname, timeStart, System.currentTimeMillis() );
		showMessage_nl();
	} // doRole


	/**
	 * @param debug
	 * @param almmRole
	 * @param source
	 * @param rmtype
	 */
	private void standardRole( boolean debug, TableToArrayListMultimapHikari almmRole, String source, String rmtype )
	{
		long threadId = Thread.currentThread().getId();

		int count = 0;
		int count_empty = 0;
		int count_noref = 0;
		int stepstate = count_step;

		try
		{
			String selectQuery = "SELECT id_person , registration_maintype, role ";
			selectQuery += "FROM links_original.person_o WHERE id_source = " + source;
			if ( ! rmtype.isEmpty() ) { selectQuery += " AND registration_maintype = " + rmtype; }
			if( debug ) { showMessage( selectQuery, false, true ); }

			ResultSet rs = dbconOriginal.executeQuery( selectQuery );
			rs.last();
			int total = rs.getRow();
			rs.beforeFirst();

			while( rs.next() )
			{
				count++;
				if( count == stepstate ) {
					long pct = Math.round( 100.0 * (float)count / (float)total );
					String msg = String.format( "Thread id %02d, standardRole, %d-of-%d (%d%%)", threadId, count, total, pct );
					showMessage( msg, true, true );
					stepstate += count_step;
				}

				int id_person = rs.getInt( "id_person" );
				int registration_maintype = rs.getInt( "registration_maintype" );
				String role = rs.getString( "role" ) != null ? rs.getString( "role" ).toLowerCase() : "";
				if( debug ) { System.out.println( "role: " + role  ); }

				if( role.isEmpty() ) { count_empty++; }
				else
				{
					if( almmRole.contains( role ) )             // present in ref_role
					{
						String refSCode = almmRole.code( role );
						if( debug ) { System.out.println( "refSCode: " + refSCode ); }

						if( refSCode.equals( SC_X ) )
						{
							if( debug ) { showMessage( "Warning 141 (via SC_X): id_person: " + id_person + ", role: " + role, false, true ); }
							addToReportPerson( id_person, source, 141, role );      // warning 141

							String role_nr = "99";
							String updateQuery = PersonC.updateQuery( "role", role_nr, id_person );
							if( debug ) { showMessage( updateQuery, false, true ); }
							dbconCleaned.executeUpdate( updateQuery );
						}
						else if( refSCode.equals( SC_N ) )
						{
							if( debug ) { showMessage( "Warning 143: id_person: " + id_person + ", role: " + role, false, true ); }
							addToReportPerson( id_person, source, 143, role );      // warning 143
						}
						else if( refSCode.equals( SC_U ) )
						{
							if( debug ) { showMessage( "Warning 145: id_person: " + id_person + ", role: " + role, false, true ); }
							addToReportPerson( id_person, source, 145, role );      // warning 145

							String role_nr = almmRole.value( "role_nr", role );
							String updateQuery = PersonC.updateQuery( "role", role_nr, id_person );
							if( debug ) { showMessage( updateQuery, false, true ); }
							dbconCleaned.executeUpdate( updateQuery );
						}
						else if( refSCode.equals( SC_Y ) )
						{
							if( debug ) { showMessage( "Standard Role: id_person: " + id_person + ", role: " + role, false, true ); }
							String role_nr = almmRole.value( "role_nr", role );
							// Check for acceptable roles are acceptable per maintype
							if
							( ( registration_maintype == 1 &&
								Arrays.asList( "1", "2", "3", "14" ).contains( role_nr ) ) ||
								( ( registration_maintype == 2 || registration_maintype == 4 ) &&
									Arrays.asList( "4", "5" , "6", "7", "8", "9", "12", "13" ).contains( role_nr ) ) ||
								( registration_maintype == 3 &&
									Arrays.asList( "2", "3", "10", "11", "12", "14" ).contains( role_nr ) )
							)
							{
								String updateQuery = PersonC.updateQuery( "role", role_nr, id_person );
								if( debug ) { showMessage( updateQuery, false, true ); }
								dbconCleaned.executeUpdate( updateQuery );
							}
							else
							{
								if( debug ) { showMessage( "Warning 142: id_person: " + id_person + ", role: " + role, false, true ); }
								addToReportPerson( id_person, source, 142, role );      // warning 142
							}
						}
						else
						{
							if( debug ) { showMessage( "Warning 149: id_person: " + id_person + ", role: " + role, false, true ); }

							addToReportPerson( id_person, source, 149, role );      // report warning 149
							almmRole.add( role );                                   // add new role
						}
					}
					else    // not in ref_role
					{
						count_noref++;
						if( debug ) { showMessage( "Warning 141 (not in ref_): id_person: " + id_person + ", role: " + role, false, true ); }

						addToReportPerson( id_person, source, 141, role );      // report warning 141
						almmRole.add( role );                                   // add new role

						String role_nr = "99";
						String updateQuery = PersonC.updateQuery( "role", role_nr, id_person );
						if( debug ) { showMessage( updateQuery, false, true ); }
						dbconCleaned.executeUpdate( updateQuery );
					}
				}
			}

			showMessage( String.format( "Thread id %02d; %d person records, %d without a role, and %d without a standard role", threadId, count, count_empty, count_noref ), false, true );
		}
		catch( Exception ex ) {
			String msg = String.format( "Thread id %02d; count: %d, Exception in standardRole: %s", threadId, count, ex.getMessage() );
			showMessage( msg, false, true );
			ex.printStackTrace( new PrintStream( System.out ) );
		}
	} // standardRole


	/**
	 * @param debug
	 * @param source
	 * @param rmtype
	 */
	public void standardAlive( boolean debug, String source, String rmtype )
	{
		long threadId = Thread.currentThread().getId();

		int count = 0;
		int count_empty = 0;
		int stepstate = count_step;
		int id_person = 0;

		try
		{
			String selectQuery = "SELECT id_registration , id_person , role , death , occupation, age_year ";
			selectQuery += "FROM links_cleaned.person_c WHERE id_source = " + source;
			if ( ! rmtype.isEmpty() ) { selectQuery += " AND registration_maintype = " + rmtype; }
			if( debug ) { showMessage( selectQuery, false, true ); }

			ResultSet rs = dbconCleaned.executeQuery( selectQuery );
			rs.last();
			int total = rs.getRow();
			rs.beforeFirst();

			while( rs.next() )
			{
				count++;
				if( count == stepstate ) {
					long pct = Math.round( 100.0 * (float)count / (float)total );
					String msg = String.format( "Thread id %02d, standardAlive, %d-of-%d (%d%%)", threadId, count, total, pct );
					showMessage( msg, true, true );
					stepstate += count_step;
				}

				id_person           = rs.getInt( "id_person" );
				int id_registration = rs.getInt( "id_registration" );
				int role            = rs.getInt( "role" );
				String death        = rs.getString( "death" );
				String occupation   = rs.getString( "occupation" );
				int age_year        = rs.getInt( "age_year" );      // read as int: NULL -> 0

				if( debug ) {
					showMessage( "count: " + count + ", id_person: " + id_person + ", role: " + role + ", death: " + death + ", occupation: " + occupation, false, true ); }

				if( death != null ) { death = death.toLowerCase(); }
				if( death == null || death.isEmpty() ) { count_empty++; }

				if( ( ( role > 1 && role < 10 ) || role == 11 ) && ( age_year != 0 && age_year < 14 ) )  {
					addToReportPerson( id_person, source, 265, Integer.toString( role ) );      // report warning 265
				}

				if( role == 1 || role == 4 || role == 7 || role == 10 ) {
					if( debug ) { showMessage( "role: " + role + ", death -> 'a'", false, true ); }

					String updateQuery = PersonC.updateQuery( "death", "a", id_person );        // set death to a[live]
					dbconCleaned.executeUpdate( updateQuery );
				}
				else
				{
					if( occupation != null ) {
						if( debug ) { showMessage( "occupation: " + occupation + ", death -> 'a'", false, true ); }

						String updateQuery = PersonC.updateQuery( "death", "a", id_person );     // set death to a[live]
						dbconCleaned.executeUpdate( updateQuery );
					}
					else
					{
						if( death == null ) {
							if( debug ) { showMessage( "death: " + death + ", death -> 'n'", false, true ); }

							String updateQuery = PersonC.updateQuery( "death", "n", id_person );     // set death to n[o]
							dbconCleaned.executeUpdate( updateQuery );
						}
						else
						{ if( debug ) { showMessage( "death stays: " + death, false, true ); } }
					}
				}
			}

			showMessage( String.format( "Thread id %02d; %d person records, %d without alive specification", threadId, count, count_empty ), false, true );
		}
		catch( Exception ex ) {
			String msg = String.format( "Thread id %02d; count: %d, Exception while cleaning standardAlive: %s", threadId, count, ex.getMessage() );
			showMessage( msg, false, true );
			msg = String.format( "Thread id %02d; id_person: %d, ", threadId, id_person );
			showMessage( msg, false, true );
			ex.printStackTrace( new PrintStream( System.out ) );
		}
	} // standardAlive




}
