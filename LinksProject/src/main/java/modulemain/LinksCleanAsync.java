package modulemain;

import java.io.File;
import java.io.FileWriter;
import java.io.PrintStream;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;

import java.sql.PreparedStatement;
import java.sql.ResultSet;

import java.text.SimpleDateFormat;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.Period;

import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.LinkedList;
import java.util.Queue;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.Set;
import java.util.Vector;

import javax.swing.JTextArea;
import javax.swing.JTextField;

import com.google.common.base.Splitter;

import com.zaxxer.hikari.HikariDataSource;

import connectors.HikariConnection;

import dataset.DateYearMonthDaySet;
import dataset.DivideMinMaxDatumSet;
import dataset.MinMaxDateSet;
import dataset.MinMaxMainAgeSet;
import dataset.MinMaxYearSet;
import dataset.Options;
import dataset.PersonC;
import dataset.RegistrationC;
import dataset.Remarks;
import dataset.TableToArrayListMultimapHikari;

import enumdefinitions.TableType;
import enumdefinitions.TimeType;

import general.Functions;
import general.PrintLogger;

/**
 * @author Fons Laan
 *
 * FL-30-Jun-2014 Imported from @author Omar Azouguagh backup
 * FL-28-Jul-2014 Timing functions
 * FL-20-Aug-2014 Occupation added
 * FL-13-Oct-2014 Removed ttal code
 * FL-04-Feb-2015 dbconRefWrite instead of dbconRefRead for writing in standardRegistrationType
 * FL-01-Apr-2015 DivorceLocation
 * FL-08-Apr-2015 Remove duplicate registrations from links_cleaned
 * FL-27-Jul-2015 Bad registration dates in id_source = 10 (HSN)
 * FL-17-Sep-2015 Bad registration dates: db NULLs
 * FL-30-Oct-2015 minMaxCalculation() function C omission
 * FL-20-Nov-2015 registration_days bug with date strings containing leading zeros
 * FL-22-Jan-2016 registration_days bug with date strings containing leading zeros
 * FL-13-May-2016 Split firstnames now in standardFirstnames()
 * FL-21-May-2016 Each thread its own ref table multimaps
 * FL-04-Nov-2016 Small change minMaxCalculation
 * FL-07-Nov-2016 Flag instead of remove registrations
 * FL-21-Nov-2016 Old date difference bug in minMaxDate
 * FL-25-Jan-2017 Divorce info from remarks
 * FL-01-Feb-2017 Temp tables ENGINE, CHARACTER SET, COLLATION
 * FL-28-Jun-2017 Local db_ref connections, immediate open/close
 * FL-05-Jul-2017 almmRegistration use
 * FL-11-Jul-2017 More not_linksbase flagging
 * FL-01-Sep-2017 registration_type also in person_c
 * FL-27-Mar-2018 Missing 2 query params in standardRegistrationDate()
 * FL-12-Jun-2018 Echtscheiding: registration_maintype = 4
 * FL-15-Oct-2018 Strip {} from id_persist_registration
 * FL-05-Dec-2018 Debug standardRegistrationDate()
 * FL-10-Dec-2018 Escape trailing backslash in ReportRegistration()
 * FL-22-Jul-2019 StandardAgeLiteral bug
 * FL-30-Jul-2019 addToReportRegistration() cleanup
 * FL-05-Aug-2019 split doDates() into 1 & 2
 * FL-24-Sep-2019 debug flag regression in standardRegistrationDate()
 * FL-30-Sep-2019 standardRole(): check role against registration_maintype
 * FL-05-Nov-2019 Extensive cleanup
 * FL-01-Jan-2020 Do not keep connection to Reference db endlessly open
 * FL-07-Jan-2020 Single-threaded refreshing in LinksCleanedMain
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

	//private String ref_url = "";			// reference db access
	//private String ref_user = "";
	//private String ref_pass = "";
	//private String ref_db = "";

	String logTableName = "";

	private HikariDataSource dsReference;
	private HikariDataSource dsLog;
	private HikariDataSource dsOriginal;
	private HikariDataSource dsCleaned;
	private HikariDataSource dsTemp;

	private HikariConnection dbconLog      = null;
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
	 * @param semaphore
	 * @param opts
	 * @param guiLine
	 * @param guiArea
	 * @param source
	 * @param rmtype
	 * @param showskip
	 * @param logTableName
	 * @param dsLog
	 * @param dsReference
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

		String logTableName,

		HikariDataSource dsLog,
		HikariDataSource dsReference,
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

		this.logTableName = logTableName;

		this.dsLog       = dsLog;
		this.dsReference = dsReference;
		this.dsOriginal  = dsOriginal;
		this.dsCleaned   = dsCleaned;
		this.dsTemp      = dsTemp;
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

			//ref_url  = opts.getDb_ref_url();
			//ref_db   = opts.getDb_ref_db();
			//ref_user = opts.getDb_ref_user();
			//ref_pass = opts.getDb_ref_pass();

			// database connections
			dbconLog      = new HikariConnection( dsLog.getConnection() );
			dbconOriginal = new HikariConnection( dsOriginal.getConnection() );
			dbconCleaned  = new HikariConnection( dsCleaned.getConnection() );
			dbconTemp     = new HikariConnection( dsTemp.getConnection() );

			// links_general.ref_report contains about 75 error definitions, to be used when the normalization encounters errors
			showMessage( String.format( "Thread id %02d; Loading report table", threadId ), false, true );
			almmReport = new TableToArrayListMultimapHikari( dsReference, "ref_report", "type", null );

			// Single-threaded refreshing in LinksCleanedMain
			//doRefreshData( opts.isDbgRefreshData(), opts.isDoRefreshData(), source, rmtype );			// GUI cb: Remove previous data

			doPrepieceSuffix( opts.isDbgPrepieceSuffix(), opts.isDoPrepieceSuffix(), source, rmtype );	// GUI cb: Prepiece, Suffix

			doFirstnames( opts.isDbgFirstnames(), opts.isDoFirstnames(), source, rmtype );				// GUI cb: Firstnames

			doFamilynames( opts.isDbgFamilynames(), opts.isDoFamilynames(), source, rmtype );			// GUI cb: Familynames

			doLocations( opts.isDbgLocations(), opts.isDoLocations(), source, rmtype );					// GUI cb: Locations

			doStatusSex( opts.isDbgStatusSex(), opts.isDoStatusSex(), source, rmtype );					// GUI cb: Status and Sex

			doRegistrationType( opts.isDbgRegType(), opts.isDoRegType(), source, rmtype );				// GUI cb: Registration Type

			doOccupation( opts.isDbgOccupation(), opts.isDoOccupation(), source, rmtype );				// GUI cb: Occupation

			doAge( opts.isDbgAge(), opts.isDoAge(), source, rmtype );									// GUI cb: Age, Role,Dates

			doRole( opts.isDbgRole(), opts.isDoRole(), source, rmtype );								// GUI cb: Age, Role, Dates

			// doDates1(): all other datesfunctions
			doDates1(  opts.isDbgDates(), opts.isDoDates(), source, rmtype );							// GUI cb: Age, Role, Dates
			// doDates2(): only minMaxDateMain()
			doDates2( opts.isDbgDates(), opts.isDoDates(), source, rmtype );							// GUI cb: Age, Role, Dates

			doMinMaxMarriage( opts.isDbgMinMaxMarriage(), opts.isDoMinMaxMarriage(), source, rmtype );	// GUI cb: Min Max Marriage

			doPartsToFullDate( opts.isDbgPartsToFullDate(), opts.isDoPartsToFullDate(), source, rmtype );	// GUI cb: Parts to Full Date

			doDaysSinceBegin( opts.isDbgDaysSinceBegin(), opts.isDoDaysSinceBegin(), source, rmtype );	// GUI cb: Days since begin

			doPostTasks( opts.isDbgPostTasks(), opts.isDoPostTasks(), source, rmtype );					// GUI cb: Post Tasks

			doFlagRegistrations( opts.isDbgFlagRegistrations(), opts.isDoFlagRegistrations(), source, rmtype );	// GUI cb: Remove Duplicate Reg's

			doFlagPersonRecs( opts.isDbgFlagPersons(), opts.isDoFlagPersons(), source, rmtype );			// GUI cb: Remove Empty Role Reg's

			doScanRemarks( opts.isDbgScanRemarks(), opts.isDoScanRemarks(), source, rmtype );				// GUI cb: Scan Remarks


			almmReport.free();

			// Close db connections
			if( dbconLog      != null ) { dbconLog.close();      dbconLog      = null; }
			//if( dbconRefRead  != null ) { dbconRefRead.close();  dbconRefRead  = null; }
			if( dbconOriginal != null ) { dbconOriginal.close(); dbconOriginal = null; }
			if( dbconCleaned  != null ) { dbconCleaned.close();  dbconCleaned  = null; }
			if( dbconTemp     != null ) { dbconTemp.close();     dbconTemp  = null; }

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
	 * getTimeStamp2()
	 * @param format
	 * @return
	 */
	public static String getTimeStamp2( String format ) {
		Calendar cal = Calendar.getInstance();
		SimpleDateFormat sdf = new SimpleDateFormat( format );
		return sdf.format( cal.getTime() );
	}


	/**
	 * showMessage()
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
	 * showMessage_nl()
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
	 * showTimingMessage()
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
	 * elapsedShowMessage()
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
	 * addToReportRegistration()
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
	 * addToReportPerson()
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
	 * doRefreshData()
	 * @param debug
	 * @param go
	 * @param source
	 * @param rmtype
	 * @throws Exception
	 *
	 * Remove previous data from links_cleaned, and then copy keys from links_original
	 */
	/*
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

		int registCCount = -1;
		try( ResultSet rsR = dbconCleaned.executeQuery( qRegistCCount ) )
		{
			rsR.first();
			registCCount = rsR.getInt("COUNT(*)" );
		}

		int personCCount = -1;
		try( ResultSet rsP = dbconCleaned.executeQuery( qPersonCCount ) ) {
			rsP.first();
			personCCount = rsP.getInt( "COUNT(*)" );
		}

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
	*/

	/*---< First- and Familynames >-------------------------------------------*/

	/**
	 * doPrepieceSuffix()
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

		TableToArrayListMultimapHikari almmPrepiece = new TableToArrayListMultimapHikari( dsReference, "ref_prepiece", "original", "prefix" );
		TableToArrayListMultimapHikari almmSuffix   = new TableToArrayListMultimapHikari( dsReference, "ref_suffix",   "original", "standard" );
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
		almmPrepiece.updateTable();

		while( almmSuffix.isBusy().get() ) {
			plog.show( "No permission to update ref table: Waiting 60 seconds" );
			Thread.sleep( 60000 );
		}
		newcount = almmSuffix.newcount();
		showMessage( String.format( "Thread id %02d; New entries for ref table suffix: %d", threadId, newcount ), false, true );
		almmSuffix.updateTable();

		// almmAlias.updateTable( dbconRefWrite );         // almmAlias.add() never called; nothing added to almmAlias

		almmPrepiece.free();
		almmSuffix.free();
		//almmAlias.free();
		showMessage( String.format( "Thread id %02d; Freed almmPrepiece/almmSuffix", threadId ), false, true );

		elapsedShowMessage( funcname, timeStart, System.currentTimeMillis() );
		showMessage_nl();
	} // doPrepieceSuffix


	/**
	 * standardPrepiece()
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

			try( ResultSet rs = dbconOriginal.executeQuery( selectQuery ) )
			{
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
		}
		catch( Exception ex ) {
			String msg = String.format( "Thread id %02d; count: %d, Exception while cleaning Prepiece: %s", threadId, count, ex.getMessage() );
			showMessage( msg, false, true );
			ex.printStackTrace( new PrintStream( System.out ) );
		}
	} // standardPrepiece


	/**
	 * namePrepiece()
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
	 * standardSuffix()
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

			try( ResultSet rs = dbconOriginal.executeQuery( selectQuery ) )
			{
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
		}
		catch( Exception ex ) {
			String msg = String.format( "Thread id %02d; count: %d, Exception while cleaning Suffix: %s", threadId, count, ex.getMessage() );
			showMessage( msg, false, true );
			ex.printStackTrace( new PrintStream( System.out ) );
		}
	} // standardSuffix


	/**
	 * standardAlias()
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
	 * cleanName()
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
	 * doFirstnames()
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

		TableToArrayListMultimapHikari almmPrepiece = new TableToArrayListMultimapHikari( dsReference, "ref_prepiece", "original", "prefix" );
		TableToArrayListMultimapHikari almmSuffix   = new TableToArrayListMultimapHikari( dsReference, "ref_suffix",   "original", "standard" );
		TableToArrayListMultimapHikari almmAlias    = new TableToArrayListMultimapHikari( dsReference, "ref_alias",    "original",  null );
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
		TableToArrayListMultimapHikari almmFirstname = new TableToArrayListMultimapHikari( dsReference, "ref_firstname", "original", "standard" );
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
		almmFirstname.updateTable();

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
	 * standardFirstname()
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

			try( ResultSet rs = dbconOriginal.executeQuery( selectQuery ) )
			{
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
	 * cleanFirstname()
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
	 * createTempFirstnameTable()
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
	 * createTempFirstnameFile()
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
	 * loadFirstnameCsvToTableT()
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
	 * updateFirstnameToPersonC()
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
	 * removeFirstnameFile()
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
	 * removeFirstnameTable()
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
	 * doFamilynames()
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

		TableToArrayListMultimapHikari almmPrepiece = new TableToArrayListMultimapHikari( dsReference, "ref_prepiece", "original", "prefix" );
		TableToArrayListMultimapHikari almmSuffix   = new TableToArrayListMultimapHikari( dsReference, "ref_suffix",   "original", "standard" );
		TableToArrayListMultimapHikari almmAlias    = new TableToArrayListMultimapHikari( dsReference, "ref_alias",    "original",  null );
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
		TableToArrayListMultimapHikari almmFamilyname = new TableToArrayListMultimapHikari( dsReference, "ref_familyname", "original", "standard" );
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
		almmFamilyname.updateTable();

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
	 * standardFamilyname()
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

			try( ResultSet rs = dbconOriginal.executeQuery( selectQuery ) )
			{
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
	 * cleanFamilyname()
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
	 * createTempFamilynameTable()
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
	 * createTempFamilynameFile()
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
	 * loadFamilynameCsvToTableT()
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
	 * updateFamilynameToPersonC()
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
	 * removeFamilynameFile()
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
	 * removeFamilynameTable()
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
	 * doesTableExist()
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

		int count = 0;
		try( ResultSet rs = dbcon.executeQuery( query ) )
		{
			rs.first();
			count = rs.getInt( "COUNT(*)" );
			//showMessage( "doesTableExist: " + db_name + " " + table_name + " : " + count, false, true );
		}

		if( count == 1 ) return true;
		else return false;
	} // doesTableExist


	/**
	 * dropTable()
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
	 * doLocations()
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

		TableToArrayListMultimapHikari almmLocation = new TableToArrayListMultimapHikari( dsReference, "ref_location", "original", "location_no" );

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
		almmLocation.updateTable();

		almmLocation.free();
		showMessage( String.format( "Thread id %02d; Freed almmLocation", threadId ), false, true );

		elapsedShowMessage( funcname, funcStart, System.currentTimeMillis() );
		showMessage_nl();
	} // doLocations


	/**
	 * standardRegistrationLocation()
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
	 * standardBirthLocation()
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
	 * standardMarriageLocation()
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
	 * standardDivorceLocation()
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
	 * standardLivingLocation()
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
	 * standardDeathLocation()
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
	 * standardLocation()
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

		try( ResultSet rs = dbconOriginal.executeQuery( selectQuery ) )
		{
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
	 * doStatusSex()
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

		TableToArrayListMultimapHikari almmCivilstatus = new TableToArrayListMultimapHikari( dsReference, "ref_status_sex", "original", "standard_civilstatus" );
		TableToArrayListMultimapHikari almmSex = new TableToArrayListMultimapHikari( dsReference, "ref_status_sex", "original", "standard_sex" );

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
		almmCivilstatus.updateTable();

		almmCivilstatus.free();
		almmSex.free();

		elapsedShowMessage( funcname, timeStart, System.currentTimeMillis() );
		showMessage_nl();
	} // doStatusSex


	/**
	 * standardSex()
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

		String selectQuery = "SELECT id_person , sex FROM person_o WHERE id_source = " + source;
		if ( ! rmtype.isEmpty() ) { selectQuery += " AND registration_maintype = " + rmtype; }
		if( debug ) { showMessage( selectQuery, false, true ); }

		try( ResultSet rs = dbconOriginal.executeQuery( selectQuery ) )
		{
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
	 * standardCivilstatus()
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

		String selectQuery = "SELECT id_person , sex , civil_status FROM person_o WHERE id_source = " + source;
		if ( ! rmtype.isEmpty() ) { selectQuery += " AND registration_maintype = " + rmtype; }
		if( debug ) { showMessage( selectQuery, false, true ); }

		try( ResultSet rs = dbconOriginal.executeQuery( selectQuery ) )
		{
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
	 * doRegistrationType()
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

		TableToArrayListMultimapHikari almmRegistration = new TableToArrayListMultimapHikari( dsReference, "ref_registration", "original", "standard" );

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
		almmRegistration.updateTable( extra_col, delimiter );

		almmRegistration.free();

		elapsedShowMessage( funcname, timeStart, System.currentTimeMillis() );
		showMessage_nl();
	} // doRegistrationType


	/**
	 * standardRegistrationType()
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

		String selectQuery = "SELECT id_registration, registration_maintype, registration_type";
		selectQuery += " FROM registration_o WHERE id_source = " + source;
		if ( ! rmtype.isEmpty() ) { selectQuery += " AND registration_maintype = " + rmtype; }
		if( debug ) { showMessage( selectQuery, false, true ); }

		try( ResultSet rs = dbconOriginal.executeQuery( selectQuery ) )
		{
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
	 * doOccupation()
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

		TableToArrayListMultimapHikari almmOccupation = new TableToArrayListMultimapHikari( dsReference, "ref_occupation", "original", "standard" );

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
		almmOccupation.updateTable();

		almmOccupation.free();
		showMessage( String.format( "Thread id %02d; Freed almmOccupation", threadId ), false, true );

		elapsedShowMessage( funcname, funcstart, System.currentTimeMillis() );
		showMessage_nl();
	} // doOccupation


	/**
	 * standardOccupation()
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

		try( ResultSet rs = dbconOriginal.executeQuery( query ) )
		{
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
	 * standardOccupationRecord()
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
	 * doAge()
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

		TableToArrayListMultimapHikari almmLitAge = new TableToArrayListMultimapHikari( dsReference, "ref_age", "original", "standard_year" );

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
		almmLitAge.updateTable();

		almmLitAge.free();
		showMessage( String.format( "Thread id %02d; Freed almmLitAge", threadId ), false, true );

		elapsedShowMessage( funcname, timeStart, System.currentTimeMillis() );
		showMessage_nl();
	} // doAge


	/**
	 * standardAgeLiteral()
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

		String selectQuery = "SELECT id_person , id_registration , role, age_literal , age_year , age_month , age_week , age_day ";
		selectQuery += "FROM links_original.person_o WHERE id_source = " + source;
		if ( ! rmtype.isEmpty() ) { selectQuery += " AND registration_maintype = " + rmtype; }
		if( debug ) { showMessage( selectQuery, false, true ); }

		try( ResultSet rs = dbconOriginal.executeQuery( selectQuery ) )
		{
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
	 * standardAge()
	 * @param debug
	 * @param source
	 * @param rmtype
	 */
	public void standardAge( boolean debug, String source, String rmtype )
	{
		long threadId = Thread.currentThread().getId();
		int count = 0;
		int stepstate = count_step;

		String selectQuery = "SELECT id_registration, id_person , age_year , age_month , age_week , age_day ";
		selectQuery += "FROM links_original.person_o WHERE id_source = " + source;
		if ( ! rmtype.isEmpty() ) { selectQuery += " AND registration_maintype = " + rmtype; }
		if( debug ) { showMessage( selectQuery, false, true ); }

		try( ResultSet rs = dbconOriginal.executeQuery( selectQuery ) )
		{
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
	 * doRole()
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

		TableToArrayListMultimapHikari almmRole = new TableToArrayListMultimapHikari( dsReference, "ref_role", "original", "standard" );

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
		almmRole.updateTable();

		almmRole.free();
		showMessage( String.format( "Thread id %02d; Freed almmRole", threadId ), false, true );

		msg = String.format( "Thread id %02d; Processing standardAlive for source: %s ...", threadId, source );
		showMessage( msg, false, true );
		standardAlive( debug, source, rmtype );

		elapsedShowMessage( funcname, timeStart, System.currentTimeMillis() );
		showMessage_nl();
	} // doRole


	/**
	 * standardRole()
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

		String selectQuery = "SELECT id_person , registration_maintype, role ";
		selectQuery += "FROM links_original.person_o WHERE id_source = " + source;
		if ( ! rmtype.isEmpty() ) { selectQuery += " AND registration_maintype = " + rmtype; }
		if( debug ) { showMessage( selectQuery, false, true ); }

		try( ResultSet rs = dbconOriginal.executeQuery( selectQuery ) )
		{
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
	 * standardAlive()
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

		String selectQuery = "SELECT id_registration , id_person , role , death , occupation, age_year ";
		selectQuery += "FROM links_cleaned.person_c WHERE id_source = " + source;
		if ( ! rmtype.isEmpty() ) { selectQuery += " AND registration_maintype = " + rmtype; }
		if( debug ) { showMessage( selectQuery, false, true ); }

		try( ResultSet rs = dbconCleaned.executeQuery( selectQuery ) )
		{
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


	/*---< Dates >------------------------------------------------------------*/

	/**
	 * doDates1()
	 * @param debug
	 * @param go
	 * @param source
	 * @param rmtype
	 * @throws Exception
	 */
	private void doDates1( boolean debug, boolean go, String source, String rmtype ) throws Exception
	{
		long threadId = Thread.currentThread().getId();

		String funcname = String.format( "Thread id %02d; doDates1 for source: %s, rmtype: %s", threadId, source, rmtype );

		if( !go ) {
			if( showskip ) { showMessage( "Skipping " + funcname, false, true ); }
			return;
		}

		long timeStart = System.currentTimeMillis();
		showMessage( funcname + " ...", false, true );

		long ts = System.currentTimeMillis();
		String msg = "";

		//msg = "skipping untill standardRegistrationDate()";
		//msg = "ONLY flag functions";
		//showMessage( msg, false, true );
		///*
		ts = System.currentTimeMillis();
		String type = "birth";
		msg = String.format( "Thread id %02d; Processing standardDate for source: %s, rmtype: %s, type: %s ...", threadId, source, rmtype, type );
		showMessage( msg, false, true );
		standardDate( debug, source, type, rmtype );
		msg = String.format( "Thread id %02d; Processing standard dates", threadId );
		elapsedShowMessage( msg, ts, System.currentTimeMillis() );

		ts = System.currentTimeMillis();
		type = "mar";
		msg = String.format( "Thread id %02d; Processing standardDate for source: %s, rmtype: %s, type: %s ...", threadId, source, rmtype, type );
		showMessage( msg, false, true );
		standardDate( debug, source, type, rmtype );
		msg = String.format( "Thread id %02d; Processing standard dates", threadId );
		elapsedShowMessage( msg, ts, System.currentTimeMillis() );

		ts = System.currentTimeMillis();
		type = "divorce";
		msg = String.format( "Thread id %02d; Processing standardDate for source: %s, rmtype: %s, type: %s ...", threadId, source, rmtype, type );
		showMessage( msg, false, true );
		standardDate( debug, source, type, rmtype );
		msg = String.format( "Thread id %02d; Processing standard dates", threadId );
		elapsedShowMessage( msg, ts, System.currentTimeMillis() );

		ts = System.currentTimeMillis();
		type = "death";
		msg = String.format( "Thread id %02d; Processing standardDate for source: %s, rmtype: %s, type: %s ...", threadId, source, rmtype, type );
		showMessage( msg, false, true );
		standardDate( debug, source, type, rmtype );
		msg = String.format( "Thread id %02d; Processing standard dates", threadId );
		elapsedShowMessage( msg, ts, System.currentTimeMillis() );
		//*/

		///*
		ts = System.currentTimeMillis();
		msg = String.format( "Thread id %02d; Processing standardRegistrationDate for source: %s, rmtype: %s ...", threadId, source, rmtype );
		showMessage( msg, false, true );
		standardRegistrationDate( debug, source, rmtype );
		msg = String.format( "Thread id %02d; Processing standardRegistrationDate", threadId );
		elapsedShowMessage( msg, ts, System.currentTimeMillis() );
		//*/

		//msg = "skipping remaining date functions";
		//showMessage( msg, false, true );
		///*
		// Fill empty event dates with registration dates
		ts = System.currentTimeMillis();
		msg = String.format( "Thread id %02d; Flagging birth dates (-> Reg dates) for source: %s, rmtype: %s ...", threadId, source, rmtype );
		showMessage( msg, false, true );
		flagBirthDate( debug, source, rmtype );

		msg = String.format( "Thread id %02d; Flagging marriage dates (-> Reg dates) for source: %s, rmtype: %s ...", threadId, source, rmtype );
		showMessage( msg, false, true );
		flagMarriageDate( debug, source, rmtype );

		msg = String.format( "Thread id %02d; Flagging divorce dates (-> Reg dates) for source: %s, rmtype: %s ...", threadId, source, rmtype );
		showMessage( msg, false, true );
		flagDivorceDate( debug, source, rmtype );

		msg = String.format( "Thread id %02d; Flagging death dates (-> Reg dates) for source: %s, rmtype: %s ...", threadId, source, rmtype );
		showMessage( msg, false, true );
		flagDeathDate( debug, source, rmtype );

		msg = String.format( "Thread id %02d; Flagging empty dates", threadId );
		elapsedShowMessage( msg, ts, System.currentTimeMillis() );
		//*/

		///*
		ts = System.currentTimeMillis();
		msg = String.format( "Thread id %02d; Processing minMaxDateValid for source: %s, rmtype: %s ...", threadId, source, rmtype );
		showMessage( msg, false, true );
		minMaxDateValid( debug, source, rmtype );
		msg = String.format( "Thread id %02d; Processing minMaxDateValid", threadId );
		elapsedShowMessage( msg, ts, System.currentTimeMillis() );
		//*/

		elapsedShowMessage( funcname, timeStart, System.currentTimeMillis() );
		showMessage_nl();
	} // doDates1


	/**
	 * standardDate()
	 * @param debug
	 * @param source
	 * @param type
	 * @param rmtype
	 */
	public void standardDate( boolean debug, String source, String type, String rmtype )
	{
		long threadId = Thread.currentThread().getId();

		int count = 0;
		int count_empty = 0;
		int count_invalid = 0;
		int stepstate = count_step;

		String startQuery = "SELECT id_person , id_source , registration_type";
		startQuery += String.format(", %s_date , %s_day , %s_month , %s_year ", type, type, type, type );
		startQuery += "FROM links_original.person_o WHERE id_source = " + source;
		if ( ! rmtype.isEmpty() ) { startQuery += " AND registration_maintype = " + rmtype; }
		if( debug ) { showMessage( startQuery, false, true ); }

		try( ResultSet rs = dbconOriginal.executeQuery( startQuery ) )
		{
			rs.last();
			int total = rs.getRow();
			rs.beforeFirst();

			while( rs.next() )
			{
				count++;
				if( count == stepstate ) {
					long pct = Math.round( 100.0 * (float)count / (float)total );
					String msg = String.format( "Thread id %02d, standardDate %s, %d-of-%d (%d%%)", threadId, type, count, total, pct );
					showMessage( msg, true, true );
					stepstate += count_step;
				}
				String rtype = rs.getString( "registration_type" );
				String date_str = rs.getString( type + "_date" );

				if( date_str == null || date_str.isEmpty() )    // try to repair date from components
				{
					int year = rs.getInt( type + "_year" );
					if( year == 0 ) {
						count_empty++;
						continue;           // cannot be repaired
					}

					// create new date from components
					int day   = rs.getInt( type + "_day" );
					int month = rs.getInt( type + "_month" );
					date_str  = String.format( "%02d-%02d-%04d", day, month, year );
				}

				int id_person = rs.getInt( "id_person" );
				int id_source = rs.getInt( "id_source" );

				DateYearMonthDaySet dymd = LinksSpecific.divideCheckDate( date_str );

				if( dymd.isValidDate() )
				{
					int day   = dymd.getDay();
					int month = dymd.getMonth();
					int year  = dymd.getYear();
					date_str  = String.format( "%02d-%02d-%04d", day, month, year );

					String query = ""
						+ "UPDATE person_c SET "
						+ "person_c." + type + "_date = '" + date_str + "' , "
						+ "person_c." + type + "_day = "   + day + " , "
						+ "person_c." + type + "_month = " + month + " , "
						+ "person_c." + type + "_year = "  + year + " , "
						+ "person_c." + type + "_date_valid = 1 "
						+ "WHERE person_c.id_person = " + id_person;

					dbconCleaned.executeUpdate( query );
				}
				else
				{
					// clear existing contents of date fields
					String query = ""
						+ "UPDATE person_c SET "
						+ "person_c." + type + "_date = NULL , "
						+ "person_c." + type + "_day = NULL , "
						+ "person_c." + type + "_month = NULL , "
						+ "person_c." + type + "_year = NULL , "
						+ "person_c." + type + "_date_valid = 0 "
						+ "WHERE person_c.id_person = " + id_person;

					dbconCleaned.executeUpdate( query );

					count_invalid++;

					int errno = 0;
					if(       type.equals( "birth" ) )   { errno = 211; }
					else if ( type.equals( "mar" ) )     { errno = 221; }
					else if ( type.equals( "divorce" ) ) { errno = 221; }
					else if ( type.equals( "death" ) )   { errno = 231; }

					addToReportPerson( id_person, id_source + "", errno, dymd.getReports() );   // EC 211 / 221 / 231
				}
			}

			//String msg = String.format( "Thread id %02d; Number of %s records: %d, empty dates: %d, invalid dates: %d", threadId, type, count, count_empty, count_invalid );
			String msg = String.format( "Thread id %02d; Number of records: %d, empty dates: %d, invalid dates: %d", threadId, count, count_empty, count_invalid );
			showMessage( msg, false, true );
		}
		catch( Exception ex ) {
			String msg = String.format( "Thread id %02d; count: %d, Exception in standardDate: %s", threadId, count, ex.getMessage() );
			showMessage( msg, false, true );
			showMessage( "count: " + count + " Exception while cleaning " + type + " date: " + ex.getMessage(), false, true );
			ex.printStackTrace( new PrintStream( System.out ) );
		}
	} // standardDate


	/**
	 * standardRegistrationDate()
	 * @param debug
	 * @param source
	 * @param rmtype
	 */
	public void standardRegistrationDate( boolean debug, String source, String rmtype )
	{
		long threadId = Thread.currentThread().getId();

		int count = 0;
		int stepstate = count_step;
		int nInvalidRegDates = 0;
		int nTooManyHyphens = 0;

		String query_r = "SELECT id_registration, registration_maintype, registration_type, registration_date, registration_day, registration_month, registration_year ";
		query_r += "FROM registration_o WHERE id_source = " + source;
		if ( ! rmtype.isEmpty() ) { query_r += " AND registration_maintype = " + rmtype; }
		if( debug ) { showMessage( query_r, false, true ); }

		try( ResultSet rs_r = dbconOriginal.executeQuery( query_r ) )
		{
			rs_r.last();
			int total = rs_r.getRow();
			rs_r.beforeFirst();

			while( rs_r.next() )
			{
				count++;
				if( count == stepstate || count == 1 ) {
					long pct = Math.round( 100.0 * (float)count / (float)total );
					String msg = String.format( "Thread id %02d, standardRegistrationDate, %d-of-%d (%d%%)", threadId, count, total, pct );
					showMessage( msg, true, true );
					stepstate += count_step;
				}

				int id_registration       = rs_r.getInt( "id_registration" );
				int registration_maintype = rs_r.getInt( "registration_maintype" );
				String registration_type  = rs_r.getString( "registration_type" );
				String registration_date  = rs_r.getString( "registration_date" );

				int regist_day   = rs_r.getInt( "registration_day" );
				int regist_month = rs_r.getInt( "registration_month" );
				int regist_year  = rs_r.getInt( "registration_year" );

				//if( id_registration == 65557816 ) { debug = true; }
				//else { debug = false; continue; }

				if( debug )
				{
					System.out.println( "id_registration: "    + id_registration );
					System.out.println( "registration_date: "  + registration_date );
					System.out.println( "registration_day: "   + regist_day );
					System.out.println( "registration_month: " + regist_month );
					System.out.println( "registration_year: "  + regist_year );
				}

				// valid date string: dd-mm-yyyy
				// exactly 2 hyphens should occur, but substrings like '-1', '-2', '-3', and '-4' are used to flag
				// e.g. unreadable date strings
				int nhyphens = 0;
				if( registration_date != null  ) {
					for( int i = 0; i < registration_date.length(); i++ ) {
						if( registration_date.charAt(i) == '-' ) { nhyphens++; }
					}
					if( nhyphens > 2 ) { nTooManyHyphens++; }
				}

				// The initial registration_date is copied from the a2a field source.literaldate, which sometimes is NULL.
				// Then we will try to use the source fields day-month-year,
				// and all 3 data components must be numeric and > 0.
				// Otherwise we try to replace the date by the event date.
				int registration_flag = -2;                 // undefined, to-be-replaced

				// date object from links_original date string
				// -1- FIRST dymd
				if( debug ) { System.out.println( "-1- FIRST dymd" ); }

				DateYearMonthDaySet dymd = LinksSpecific.divideCheckDate( registration_date );
				if( dymd.isFixedDate() || dymd.isReformattedDate() )
				{
					registration_date = dymd.getDate();     // input string string changed
					if( dymd.isFixedDate() )                // input string was invalid, but the date could be fixed
					{ addToReportRegistration( id_registration, source, 203, dymd.getReports() ); }     // EC 203
				}

				if( debug ) { System.out.println( "dymd.isValidDate(): " + dymd.isValidDate() ); }
				if( debug ) { System.out.println( "dymd.isFixedDate(): " + dymd.isFixedDate() ); }
				if( debug ) { System.out.println( dymd.getReports() ); }

				// date object from links_original date components
				String regist_comp = String.format( "%02d-%02d-%04d", regist_day, regist_month, regist_year );
				DateYearMonthDaySet dymd_comp = LinksSpecific.divideCheckDate( regist_comp );
				if( debug ) { System.out.println( "dymd_comp.isValidDate(): " + dymd_comp.isValidDate() ); }
				if( debug ) { System.out.println( dymd_comp.getReports() ); }

				// compare the string date and the components date
				boolean use_event_date = false;

				// divideCheckDate() fucks up with additional hyphens!
				if( nhyphens == 2 && dymd.isValidDate() )   // valid registration_date
				{
					registration_flag = 0;                  // 0 = valid registration_date

					if( dymd_comp.isValidDate() )           // valid components date
					{
						if( ! ( dymd.getDay() == regist_day && dymd.getMonth() == regist_month && dymd.getYear() == regist_year ) )
						{
							// both valid but unequal; components from dymd will be used
							String unequal_str = String.format( "[ %s not equal %d %d %d ]", registration_date, regist_day, regist_month, regist_year );
							addToReportRegistration( id_registration, source + "", 206, unequal_str );
						}    // EC 206
					}
					else                                    // invalid components date + valid registration_date
					{
						// components from dymd will be used, extracted after the  "if( use_event_date )" loop
						//if( ! ( regist_day == 0 && regist_month == 0 && regist_year == 0 ) )  // only report if components are not all 3 empty
						//{ addToReportRegistration( id_registration, source + "", 204, dymd_comp.getReports() ); }  // EC 204
					}
				}
				else                                        // invalid registration_date
				{
					if( dymd_comp.isValidDate() )           // valid components date + invalid registration_date
					{
						//if( registration_date != "" )       // only report if registration_date is not empty
						//{ addToReportRegistration( id_registration, source + "", 203, dymd.getReports() ); }    // EC 203
						// -2- REPLACE dymd with comp values
						if( debug ) { System.out.println( "-2- REPLACE dymd with comp values" ); }
						dymd = dymd_comp;                   // use components date
						registration_flag = 2;              // 2 =  registration_date components used
						registration_date = String.format( "%02d-%02d-%04d", regist_day, regist_month, regist_year );
					}
					else { use_event_date = true; }         // both invalid, try to use event_date
				}

				if( use_event_date )    // no valid registration_date; try to use event_date
				{
					if( debug ) { System.out.println( "try to use event_date" ); }
					// try to replace invalid registration date with birth-/marriage-/death- date
					nInvalidRegDates++;

					if( nhyphens > 2 ) {
						String msg = String.format( "Thread id %02d; id_registration: %d, registration_date: %s", threadId, id_registration, registration_date );
						System.out.println( msg );
					}

					String query_p = "SELECT registration_maintype , birth_date , mar_date , divorce_date, death_date FROM person_c WHERE id_registration = " + id_registration;
					ResultSet rs_p = dbconCleaned.executeQuery( query_p );

					DateYearMonthDaySet dymd_event = null;
					while( rs_p.next() )
					{
						// try to use the event date
						if( registration_type == null ) { registration_type = ""; }
						String event_date = "";

						if( registration_maintype == 1 ) {
							event_date = rs_p.getString( "birth_date" ); }
						else if( registration_maintype == 2 && registration_type.equals( 'h' ) ) {
							event_date = rs_p.getString( "mar_date" ); }
						else if( registration_maintype == 2 && registration_type.equals( 's' ) ) {
							event_date = rs_p.getString( "divorce_date" ); }
						else if( registration_maintype == 4 ) {
							event_date = rs_p.getString( "divorce_date" ); }
						else if( registration_maintype == 3 ) {
							event_date = rs_p.getString( "death_date" ); }

						dymd_event = LinksSpecific.divideCheckDate( event_date );

						if( dymd_event.isValidDate() )
						{
							// we have a valid event date; skip the remaining reg persons
							registration_date = event_date;
							if( debug ) { System.out.println( "valid event_date: " + event_date ); }
							break;
						}
						else
						if( debug ) {
							System.out.println( "invalid event_date: " + event_date );
						}
					}

					if( dymd_event != null && dymd_event.isValidDate() )
					{
						// -3- REPLACE registration_date with event_date
						if( debug ) { System.out.println( "-3- REPLACE registration_date with event_date" ); }
						registration_flag = 1;
						dymd = dymd_event;
						int event_day   = dymd.getDay();
						int event_month = dymd.getMonth();
						int event_year  = dymd.getYear();
						registration_date = String.format( "%02d-%02d-%04d", event_day, event_month, event_year );

						//if( debug ) { System.out.println( "use event_date: " + registration_date ); }
					}
					else
					{
						if( debug ) { System.out.println( "invalid event_date; use/set registration components " ); }

						// (invalid) event_date not used; continue with registration_date components
						// Notice: (HSN date) components may be negative if registration_date contains those

						// -4- conditionally REPLACE dymd components of registration_date
						if( debug ) { System.out.println( "-4- conditionally REPLACE dymd components of registration_date" ); }
						registration_flag = 2;
						int day   = dymd_comp.getDay();
						int month = dymd_comp.getMonth();
						int year  = dymd_comp.getYear();

						if( debug ) { System.out.println( String.format( "date components (initial) %02d-%02d-%04d", day, month, year ) ); }

						if( day <= 0 && month <= 0 && year > 0 )
						{
							// KM: for HSN data with negative date components in day and/or month:
							// birth    -> registration_date = 01-01-yyyy
							// marriage -> registration_date = 01-07-yyyy
							// death    -> registration_date = 31-12-yyyy

							addToReportRegistration( id_registration, source, 202, dymd.getReports() ); // EC 202

							if( registration_maintype == 1 ) {      // birth
								day   = 1;
								month = 1;
							}
							else if( registration_maintype == 2 ) { // marriage
								day   = 1;
								month = 7;
							}
							else if( registration_maintype == 4 ) { // divorce
								day   = 1;
								month = 7;
							}
							else if( registration_maintype == 3 ) { // death
								day   = 31;
								month = 12;
							}
						}
						else
						{
							addToReportRegistration( id_registration, source, 201, dymd.getReports() ); // EC 201

							if( month == 2 && day > 28 ) {
								day   = 1;
								month = 3;
							}

							if( day > 30 && ( month == 4 || month == 6 || month == 9 || month == 11 ) ) {
								day    = 1;
								month += 1;
							}

							if( day   > 31 ) { day   = 31; }
							if( month > 12 ) { month = 12; }

							// HSN data (id_source = 10) may contain negative data components, that have special meaning,
							// but we do not want invalid date strings in links_cleaned
							if (day   < 1 ) { day   = 1; }
							if( month < 1 ) { month = 1; }
						}

						// final check
						if( debug ) { System.out.println( String.format( "date components (changed) %02d-%02d-%04d", day, month, year ) ); }

						registration_date = String.format( "%02d-%02d-%04d", day, month, year );
						if( debug ) { System.out.println( "registration_date from components: " + registration_date ); }

						dymd = LinksSpecific.divideCheckDate( registration_date );
						if( debug ) { System.out.println( "dymd.isValidDate(): " + dymd.isValidDate() ); }
					}
				}

				String query = "";
				if( dymd.isValidDate() )
				{
					regist_day   = dymd.getDay();
					regist_month = dymd.getMonth();
					regist_year  = dymd.getYear();
				}
				else    // could not get a valid registration_date; avoid confusing garbage
				{
					registration_flag = -1;
					registration_date  = "";
					regist_day   = 0;
					regist_month = 0;
					regist_year  = 0;
					addToReportRegistration( id_registration, source, 205, dymd.getReports() ); // EC 205
				}

				query = "UPDATE registration_c SET "
					+ "registration_c.registration_date = '" + registration_date  + "' , "
					+ "registration_c.registration_day = "   + regist_day   + " , "
					+ "registration_c.registration_month = " + regist_month + " , "
					+ "registration_c.registration_year = "  + regist_year  + " ";

				query += ", registration_c.registration_flag = " + registration_flag + " ";

				query += "WHERE registration_c.id_registration = "  + id_registration;
				if( debug ) { System.out.println( "query: " + query ); }
				dbconCleaned.executeUpdate( query );
			}

			if( nInvalidRegDates > 0 )
			{ showMessage( String.format( "Thread id %02d; Number of registrations without a (valid) reg date: %d", threadId, nInvalidRegDates ), false, true ); }

			if( nTooManyHyphens  > 0 )
			{ showMessage( String.format( "Thread id %02d; Number of registrations with too many hyphens in reg date: %s", threadId, nTooManyHyphens ), false, true ); }
		}
		catch( Exception ex ) {
			String msg = String.format( "Thread id %02d; count: %d, Exception in standardRegistrationDate: %s", threadId, count, ex.getMessage() );
			showMessage( msg, false, true );
			ex.printStackTrace( new PrintStream( System.out ) );
		}
	} // standardRegistrationDate


	/**
	 * flagBirthDate()
	 * @param debug
	 * @param source
	 * @param rmtype
	 */
	public void flagBirthDate( boolean debug, String source, String rmtype )
	{
		// birth_date_flag is not used elsewhere in the cleaning.
		// In standardDate() birth_date_valid is set to either 1 (valid birth date) or 0 (invalid birth date)

		long threadId = Thread.currentThread().getId();

		String[] queries =
			{
				"UPDATE person_c, registration_c"
					+ " SET"
					+ " person_c.birth_date_flag = 0"
					+ " WHERE person_c.id_source = " + source
					+ " AND person_c.registration_maintype = 1"
					+ " AND person_c.birth_date_valid = 0"
					+ " AND person_c.role = 1"
					+ " AND ( registration_c.registration_flag IS NULL OR registration_c.registration_flag < 0 )"
					+ " AND person_c.id_registration = registration_c.id_registration",

				"UPDATE person_c, registration_c"
					+ " SET"
					+ " person_c.birth_date_flag = 1"
					+ " WHERE person_c.id_source = " + source
					+ " AND person_c.registration_maintype = 1"
					+ " AND person_c.birth_date_valid = 1"
					+ " AND person_c.role = 1"
					+ " AND person_c.id_registration = registration_c.id_registration",

				"UPDATE person_c, registration_c"
					+ " SET"
					+ " person_c.birth_date_flag  = 2,"
					+ " person_c.birth_date_valid = 1,"
					+ " person_c.birth_date  = registration_c.registration_date,"
					+ " person_c.birth_year  = registration_c.registration_year,"
					+ " person_c.birth_month = registration_c.registration_month,"
					+ " person_c.birth_day   = registration_c.registration_day"
					+ " WHERE person_c.id_source = " + source
					+ " AND person_c.registration_maintype = 1"
					+ " AND ( person_c.birth_date IS NULL OR person_c.birth_date = '' )"
					+ " AND person_c.role = 1"
					+ " AND registration_c.registration_flag IS NOT NULL AND registration_c.registration_flag >= 0"
					+ " AND person_c.id_registration = registration_c.id_registration",

				"UPDATE person_c, registration_c"
					+ " SET"
					+ " person_c.birth_date_flag  = 3,"
					+ " person_c.birth_date_valid = 1,"
					+ " person_c.birth_date  = registration_c.registration_date,"
					+ " person_c.birth_year  = registration_c.registration_year,"
					+ " person_c.birth_month = registration_c.registration_month,"
					+ " person_c.birth_day   = registration_c.registration_day"
					+ " WHERE person_c.id_source = " + source
					+ " AND person_c.registration_maintype = 1"
					+ " AND person_c.birth_date_valid = 0"
					+ " AND NOT ( person_c.birth_date IS NULL OR person_c.birth_date = '' )"
					+ " AND person_c.role = 1"
					+ " AND registration_c.registration_flag IS NOT NULL AND registration_c.registration_flag >= 0"
					+ " AND person_c.id_registration = registration_c.id_registration"
			};

		int nq = 0;
		for( String query : queries )
		{
			try {
				long ts = System.currentTimeMillis();
				int flag = nq;
				nq++;

				if ( ! rmtype.isEmpty() ) { query += " AND person_c.registration_maintype = " + rmtype; }
				if( debug ) { showMessage( query, false, true ); }

				int nrec = dbconCleaned.executeUpdate( query );
				String msg = String.format( "Thread id %02d; query %d-of-%d, %d flags set to %d", threadId, nq, queries.length, nrec, flag );
				elapsedShowMessage( msg, ts, System.currentTimeMillis() );
			}
			catch( Exception ex ) {
				String msg = String.format( "Thread id %02d; Exception in flagBirthDate: %s", threadId, ex.getMessage() );
				showMessage( msg, false, true );
				showMessage( query, false, true );
				ex.printStackTrace( new PrintStream( System.out ) );
			}
		}
	} // flagBirthDate


	/**
	 * flagMarriageDate()
	 * @param debug
	 * @param source
	 * @param rmtype
	 */
	public void flagMarriageDate( boolean debug, String source, String rmtype )
	{
		// mar_date_flag is not used elsewhere in the cleaning.
		// In standardDate() mar_date_valid is set to either 1 (valid mar date) or 0 (invalid mar date)

		long threadId = Thread.currentThread().getId();

		String[] queries =
			{
				"UPDATE person_c, registration_c"
					+ " SET"
					+ " person_c.mar_date_flag = 0"
					+ " WHERE person_c.id_source = " + source
					+ " AND person_c.registration_maintype = 2"
					+ " AND person_c.registration_type = 'h'"
					+ " AND person_c.mar_date_valid = 0"
					+ " AND ( ( person_c.role = 4 ) || ( person_c.role = 7 ) )"
					+ " AND ( registration_c.registration_flag IS NULL OR registration_c.registration_flag < 0 )"
					+ " AND person_c.id_registration = registration_c.id_registration",

				"UPDATE person_c, registration_c"
					+ " SET"
					+ " person_c.mar_date_flag = 1"
					+ " WHERE person_c.id_source = " + source
					+ " AND person_c.registration_maintype = 2"
					+ " AND person_c.registration_type = 'h'"
					+ " AND person_c.mar_date_valid = 1"
					+ " AND ( ( person_c.role = 4 ) || ( person_c.role = 7 ) )"
					+ " AND person_c.id_registration = registration_c.id_registration",

				"UPDATE person_c, registration_c"
					+ " SET"
					+ " person_c.mar_date_flag  = 2,"
					+ " person_c.mar_date_valid = 1,"
					+ " person_c.mar_date  = registration_c.registration_date,"
					+ " person_c.mar_year  = registration_c.registration_year,"
					+ " person_c.mar_month = registration_c.registration_month,"
					+ " person_c.mar_day   = registration_c.registration_day"
					+ " WHERE person_c.id_source = " + source
					+ " AND person_c.registration_maintype = 2"
					+ " AND person_c.registration_type = 'h'"
					+ " AND ( person_c.mar_date IS NULL OR person_c.mar_date = '' )"
					+ " AND ( ( person_c.role = 4 ) || ( person_c.role = 7 ) )"
					+ " AND registration_c.registration_flag IS NOT NULL AND registration_c.registration_flag >= 0"
					+ " AND person_c.id_registration = registration_c.id_registration",

				"UPDATE person_c, registration_c"
					+ " SET"
					+ " person_c.mar_date_flag  = 3,"
					+ " person_c.mar_date_valid = 1,"
					+ " person_c.mar_date  = registration_c.registration_date,"
					+ " person_c.mar_year  = registration_c.registration_year,"
					+ " person_c.mar_month = registration_c.registration_month,"
					+ " person_c.mar_day   = registration_c.registration_day"
					+ " WHERE person_c.id_source = " + source
					+ " AND person_c.registration_maintype = 2"
					+ " AND person_c.registration_type = 'h'"
					+ " AND person_c.mar_date_valid = 0"
					+ " AND NOT ( person_c.mar_date IS NULL OR person_c.mar_date = '' )"
					+ " AND ( ( person_c.role = 4 ) || ( person_c.role = 7 ) )"
					+ " AND registration_c.registration_flag IS NOT NULL AND registration_c.registration_flag >= 0"
					+ " AND person_c.id_registration = registration_c.id_registration"
			};

		int nq = 0;
		for( String query : queries )
		{
			try {
				long ts = System.currentTimeMillis();
				int flag = nq;
				nq++;

				if ( ! rmtype.isEmpty() ) { query += " AND person_c.registration_maintype = " + rmtype; }
				if( debug ) { showMessage( query, false, true ); }

				int nrec = dbconCleaned.executeUpdate( query );
				String msg = String.format( "Thread id %02d; query %d-of-%d, %d flags set to %d", threadId, nq, queries.length, nrec, flag );
				elapsedShowMessage( msg, ts, System.currentTimeMillis() );
			}
			catch( Exception ex ) {
				String msg = String.format( "Thread id %02d; Exception in flagMarriageDate: %s", threadId, ex.getMessage() );
				showMessage( msg, false, true );
				showMessage( query, false, true );
				ex.printStackTrace( new PrintStream( System.out ) );
			}
		}
	} // flagMarriageDate


	/**
	 * flagDivorceDate()
	 * @param debug
	 * @param source
	 * @param rmtype
	 */
	public void flagDivorceDate( boolean debug, String source, String rmtype )
	{
		// divorce_date_flag is not used elsewhere in the cleaning.
		// In standardDate() divorce_date_valid is set to either 1 (valid mar date) or 0 (invalid mar date)

		long threadId = Thread.currentThread().getId();

		String[] queries =
			{
				"UPDATE person_c, registration_c"
					+ " SET"
					+ " person_c.divorce_date_flag = 0"
					+ " WHERE person_c.id_source = " + source
					+ " AND person_c.registration_maintype = 2"
					+ " AND person_c.registration_type = 's'"
					+ " AND person_c.divorce_date_valid = 0"
					+ " AND ( ( person_c.role = 4 ) || ( person_c.role = 7 ) )"
					+ " AND ( registration_c.registration_flag IS NULL OR registration_c.registration_flag < 0 )"
					+ " AND person_c.id_registration = registration_c.id_registration",

				"UPDATE person_c, registration_c"
					+ " SET"
					+ " person_c.divorce_date_flag = 1"
					+ " WHERE person_c.id_source = " + source
					+ " AND person_c.registration_maintype = 2"
					+ " AND person_c.registration_type = 's'"
					+ " AND person_c.divorce_date_valid = 1"
					+ " AND ( ( person_c.role = 4 ) || ( person_c.role = 7 ) )"
					+ " AND person_c.id_registration = registration_c.id_registration",

				"UPDATE person_c, registration_c"
					+ " SET"
					+ " person_c.divorce_date_flag  = 2,"
					+ " person_c.divorce_date_valid = 1,"
					+ " person_c.divorce_date  = registration_c.registration_date,"
					+ " person_c.divorce_year  = registration_c.registration_year,"
					+ " person_c.divorce_month = registration_c.registration_month,"
					+ " person_c.divorce_day   = registration_c.registration_day"
					+ " WHERE person_c.id_source = " + source
					+ " AND person_c.registration_maintype = 2"
					+ " AND person_c.registration_type = 's'"
					+ " AND ( person_c.divorce_date IS NULL OR person_c.divorce_date = '' )"
					+ " AND ( ( person_c.role = 4 ) || ( person_c.role = 7 ) )"
					+ " AND registration_c.registration_flag IS NOT NULL AND registration_c.registration_flag >= 0"
					+ " AND person_c.id_registration = registration_c.id_registration",

				"UPDATE person_c, registration_c"
					+ " SET"
					+ " person_c.divorce_date_flag  = 3,"
					+ " person_c.divorce_date_valid = 1,"
					+ " person_c.divorce_date  = registration_c.registration_date,"
					+ " person_c.divorce_year  = registration_c.registration_year,"
					+ " person_c.divorce_month = registration_c.registration_month,"
					+ " person_c.divorce_day   = registration_c.registration_day"
					+ " WHERE person_c.id_source = " + source
					+ " AND person_c.registration_maintype = 2"
					+ " AND person_c.registration_type = 's'"
					+ " AND person_c.divorce_date_valid = 0"
					+ " AND NOT ( person_c.divorce_date IS NULL OR person_c.divorce_date = '' )"
					+ " AND ( ( person_c.role = 4 ) || ( person_c.role = 7 ) )"
					+ " AND registration_c.registration_flag IS NOT NULL AND registration_c.registration_flag >= 0"
					+ " AND person_c.id_registration = registration_c.id_registration"
			};

		int nq = 0;
		for( String query : queries )
		{
			try {
				long ts = System.currentTimeMillis();
				int flag = nq;
				nq++;

				if ( ! rmtype.isEmpty() ) { query += " AND person_c.registration_maintype = " + rmtype; }
				if( debug ) { showMessage( query, false, true ); }

				int nrec = dbconCleaned.executeUpdate( query );
				String msg = String.format( "Thread id %02d; query %d-of-%d, %d flags set to %d", threadId, nq, queries.length, nrec, flag );
				elapsedShowMessage( msg, ts, System.currentTimeMillis() );
			}
			catch( Exception ex ) {
				String msg = String.format( "Thread id %02d; Exception in flagMarriageDate: %s", threadId, ex.getMessage() );
				showMessage( msg, false, true );
				showMessage( query, false, true );
				ex.printStackTrace( new PrintStream( System.out ) );
			}
		}
	} // flagDivorceDate


	/**
	 * flagDeathDate()
	 * @param debug
	 * @param source
	 * @param rmtype
	 */
	public void flagDeathDate( boolean debug, String source, String rmtype )
	{
		// death_date_flag is not used elsewhere in the cleaning.
		// In standardDate() death_date_valid is set to either 1 (valid death date) or 0 (invalid death date)

		long threadId = Thread.currentThread().getId();

		String[] queries =
			{
				"UPDATE person_c, registration_c"
					+ " SET"
					+ " person_c.death_date_flag = 0"
					+ " WHERE person_c.id_source = " + source
					+ " AND person_c.registration_maintype = 3"
					+ " AND person_c.death_date_valid = 0"
					+ " AND person_c.role = 10"
					+ " AND ( registration_c.registration_flag IS NULL OR registration_c.registration_flag < 0 )"
					+ " AND person_c.id_registration = registration_c.id_registration",

				"UPDATE person_c, registration_c "
					+ " SET"
					+ " person_c.death_date_flag = 1"
					+ " WHERE person_c.id_source = " + source
					+ " AND person_c.registration_maintype = 3"
					+ " AND person_c.death_date_valid = 1"
					+ " AND person_c.role = 10"
					+ " AND person_c.id_registration = registration_c.id_registration",

				"UPDATE person_c, registration_c"
					+ " SET"
					+ " person_c.death_date_flag  = 2,"
					+ " person_c.death_date_valid = 1,"
					+ " person_c.death_date  = registration_c.registration_date,"
					+ " person_c.death_year  = registration_c.registration_year,"
					+ " person_c.death_month = registration_c.registration_month,"
					+ " person_c.death_day   = registration_c.registration_day"
					+ " WHERE person_c.id_source = " + source
					+ " AND person_c.registration_maintype = 3"
					+ " AND ( person_c.death_date IS NULL OR person_c.death_date = '' )"
					+ " AND person_c.role = 10"
					+ " AND registration_c.registration_flag IS NOT NULL AND registration_c.registration_flag >= 0"
					+ " AND person_c.id_registration = registration_c.id_registration",

				"UPDATE person_c, registration_c "
					+ " SET "
					+ " person_c.death_date_flag  = 3,"
					+ " person_c.death_date_valid = 1,"
					+ " person_c.death_date  = registration_c.registration_date,"
					+ " person_c.death_year  = registration_c.registration_year,"
					+ " person_c.death_month = registration_c.registration_month,"
					+ " person_c.death_day   = registration_c.registration_day"
					+ " WHERE person_c.id_source = " + source
					+ " AND person_c.registration_maintype = 3"
					+ " AND person_c.death_date_valid = 0"
					+ " AND NOT ( person_c.death_date IS NULL OR person_c.death_date = '' )"
					+ " AND person_c.role = 10"
					+ " AND registration_c.registration_flag IS NOT NULL AND registration_c.registration_flag >= 0"
					+ " AND person_c.id_registration = registration_c.id_registration"
			};

		int nq = 0;
		for( String query : queries )
		{
			try {
				long ts = System.currentTimeMillis();
				int flag = nq;
				nq++;

				if ( ! rmtype.isEmpty() ) { query += " AND person_c.registration_maintype = " + rmtype; }
				if( debug ) { showMessage( query, false, true ); }

				int nrec = dbconCleaned.executeUpdate( query );
				String msg = String.format( "Thread id %02d; query %d-of-%d, %d flags set to %d", threadId, nq, queries.length, nrec, flag );
				elapsedShowMessage( msg, ts, System.currentTimeMillis() );
			}
			catch( Exception ex ) {
				String msg = String.format( "Thread id %02d; Exception in flagDeathDate: %s", threadId, ex.getMessage() );
				showMessage( msg, false, true );
				showMessage( query, false, true );
				ex.printStackTrace( new PrintStream( System.out ) );
			}
		}
	} // flagDeathDate


	/**
	 * minMaxDateValid()
	 * @param debug
	 * @param source
	 * @param rmtype
	 * @throws Exception
	 *
	 * if the date is valid, set the min en max values of date, year, month, day equal to the given values
	 * do this for birth, marriage and death
	 */
	private void minMaxDateValid( boolean debug, String source, String rmtype ) throws Exception
	{
		long threadId = Thread.currentThread().getId();

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
			+ "WHERE ( birth_date_valid = 1 OR LEFT(stillbirth, 1) = 'y' ) "
			+ "AND links_cleaned.person_c.id_source = " + source;

		if ( ! rmtype.isEmpty() ) { q1 += " AND registration_maintype = " + rmtype; }

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
			+ "WHERE mar_date_valid = 1 "
			+ "AND links_cleaned.person_c.id_source = " + source;

		if ( ! rmtype.isEmpty() ) { q2 += " AND registration_maintype = " + rmtype; }

		String q4 = ""
			+ "UPDATE person_c "
			+ "SET "
			+ "divorce_date_min  = divorce_date , "
			+ "divorce_date_max  = divorce_date , "
			+ "divorce_year_min  = divorce_year , "
			+ "divorce_year_max  = divorce_year , "
			+ "divorce_month_min = divorce_month , "
			+ "divorce_month_max = divorce_month , "
			+ "divorce_day_min   = divorce_day , "
			+ "divorce_day_max   = divorce_day "
			+ "WHERE divorce_date_valid = 1 "
			+ "AND links_cleaned.person_c.id_source = " + source;

		if ( ! rmtype.isEmpty() ) { q4 += " AND registration_maintype = " + rmtype; }

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
			+ "WHERE death_date_valid = 1 "
			+ "AND links_cleaned.person_c.id_source = " + source;

		if ( ! rmtype.isEmpty() ) { q3 += " AND registration_maintype = " + rmtype; }

		if( debug ) { showMessage( q1, false, true ); }
		else { showMessage( String.format( "Thread id %02d; 1-of-4, minMaxValidDate", threadId ), false, true ); }
		dbconCleaned.executeUpdate( q1 );

		if( debug ) { showMessage( q2, false, true ); }
		else { showMessage( String.format( "Thread id %02d; 2-of-4, minMaxValidDate", threadId ), false, true ); }
		dbconCleaned.executeUpdate( q2 );

		if( debug ) { showMessage( q4, false, true ); }
		else { showMessage( String.format( "Thread id %02d; 3-of-4, minMaxValidDate", threadId ), false, true ); }
		dbconCleaned.executeUpdate( q4 );

		if( debug ) { showMessage( q3, false, true ); }
		else { showMessage( String.format( "Thread id %02d; 4-of-4, minMaxValidDate", threadId ), false, true ); }
		dbconCleaned.executeUpdate( q3 );
	} // minMaxDateValid


	/*---< Dates2 >-----------------------------------------------------------*/

/**
	 * doDates2()
	 * @param debug
	 * @param go
	 * @param source
	 * @param rmtype
	 * @throws Exception
	 *
	 * Make minMaxDateMain() also a separate GUI option:
	 * we often have date issues, and redoing the whole date cleaning takes so long.
	 */
	private void doDates2( boolean debug, boolean go, String source, String rmtype ) throws Exception
	{
		long threadId = Thread.currentThread().getId();

		String funcname = String.format( "Thread id %02d; doDates2 for source: %s, rmtype: %s", threadId, source, rmtype );

		if( !go ) {
			if( showskip ) { showMessage( "Skipping " + funcname, false, true ); }
			return;
		}

		long timeStart = System.currentTimeMillis();
		showMessage( funcname + " ...", false, true );

		long ts = System.currentTimeMillis();
		String msg = "";

		ts = System.currentTimeMillis();
		msg = String.format( "Thread id %02d; Processing minMaxDateMain for source: %s ...", threadId, source );
		showMessage( msg, false, true );

		HikariConnection dbconReference = new HikariConnection( dsReference.getConnection() );
		minMaxDateMain( debug, dbconReference, source, rmtype );
		dbconReference.close();

		msg = String.format( "Thread id %02d; Processing minMaxDateMain", threadId );
		elapsedShowMessage( msg, ts, System.currentTimeMillis() );

		elapsedShowMessage( funcname, timeStart, System.currentTimeMillis() );
		showMessage_nl();
	} // doDates2


	/**
	 * minMaxDateMain()
	 * @param debug
	 * @param dbconRef
	 * @param source
	 * @param rmtype
	 * @throws Exception
	 */
	public void minMaxDateMain( boolean debug, HikariConnection dbconRef, String source, String rmtype ) throws Exception
	{
		long threadId = Thread.currentThread().getId();

		int count = 0;
		int stepstate = count_step;

		// Because this query acts on person_c and registration_c (instead of person_o and registration_o),
		// the cleaning options Age and Role must be run together with (i.e. before) Dates.
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
			+ " person_c.stillbirth ,"
			+ " person_c.mar_date ,"
			+ " person_c.death_date ,"
			+ " person_c.birth_year ,"
			+ " person_c.birth_month ,"
			+ " person_c.birth_day ,"
			+ " person_c.birth_date_valid ,"
			+ " person_c.mar_date_valid ,"
			+ " person_c.death_date_valid ,"
			+ " person_c.death"
			+ " FROM person_c , registration_c"
			+ " WHERE person_c.id_registration = registration_c.id_registration"
			+ " AND links_cleaned.person_c.id_source = " + source;

		if ( ! rmtype.isEmpty() ) { startQuery += " AND person_c.registration_maintype = " + rmtype; }
		if( debug ) { showMessage( startQuery, false, true ); }

		int id_registration = -1;   // want to show it when exception occurs
		int id_person = -1;         // want to show it when exception occurs

		try( ResultSet rsPersons = dbconCleaned.executeQuery( startQuery ) )
		{
			rsPersons.last();
			int total = rsPersons.getRow();
			rsPersons.beforeFirst();

			String msg = String.format( "Thread id %02d; person_c records: %d", threadId, total );
			showMessage( msg, true, true );
			showMessage( msg, false, true );

			while( rsPersons.next() )
			{
				count++;

				if( count == stepstate ) {
					long pct = Math.round( 100.0 * (float)count / (float)total );
					msg = String.format( "Thread id %02d, minMaxDateMain, %d-of-%d (%d%%)", threadId, count, total, pct );
					showMessage( msg, true, true );
					stepstate += count_step;
				}

				// Get
				id_registration             = rsPersons.getInt(    "id_registration" );
				int    id_source            = rsPersons.getInt(    "id_source" );
				String registrationDate     = rsPersons.getString( "registration_date" );
				int    registrationMaintype = rsPersons.getInt(    "registration_maintype" );
				id_person                   = rsPersons.getInt(    "id_person" );
				int    role                 = rsPersons.getInt(    "role" );
				int    age_year             = rsPersons.getInt(    "age_year" );
				int    age_month            = rsPersons.getInt(    "age_month" );
				int    age_week             = rsPersons.getInt(    "age_week" );
				int    age_day              = rsPersons.getInt(    "age_day" );
				String birth_date           = rsPersons.getString( "person_c.birth_date" );
				String stillbirth           = rsPersons.getString( "person_c.stillbirth" );
				String mar_date             = rsPersons.getString( "person_c.mar_date" );
				String death_date           = rsPersons.getString( "person_c.death_date" );
				int    birth_year           = rsPersons.getInt(    "birth_year" );
				int    birth_month          = rsPersons.getInt(    "birth_month" );
				int    birth_day            = rsPersons.getInt(    "birth_day" );
				int birth_date_valid        = rsPersons.getInt(    "birth_date_valid" );
				int mar_date_valid          = rsPersons.getInt(    "mar_date_valid" );
				int death_date_valid        = rsPersons.getInt(    "death_date_valid" );
				String death                = rsPersons.getString( "person_c.death" );

				if( stillbirth == null ) { stillbirth = ""; }

				//if( id_person == 37336505 || id_person == 37336508 ) { debug = true; }
				//else { debug = false; continue; }

				if( debug )
				{
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
					showMessage( "stillbirth: "           + stillbirth,           false, true );
					showMessage( "mar_date: "             + mar_date,             false, true );
					showMessage( "death_date: "           + death_date,           false, true );
					showMessage( "birth_year: "           + birth_year,           false, true );
					showMessage( "birth_month: "          + birth_month,          false, true );
					showMessage( "birth_day: "            + birth_day,            false, true );
					showMessage( "birth_date_valid: "     + birth_date_valid,     false, true );
					showMessage( "mar_date_valid: "       + mar_date_valid,       false, true );
					showMessage( "death_date_valid: "     + death_date_valid,     false, true );
					showMessage( "death: "                + death,                false, true );
				}

				MinMaxDateSet mmds = new MinMaxDateSet();

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
				mmds.setPersonBirthMonth( birth_month );
				mmds.setPersonBirthDay( birth_day );
				mmds.setDeathDate( death_date );
				mmds.setDeath( death );

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

				if( debug && mmds.getPersonRole() == 0 ) { showMessage( "minMaxDateMain() role = 0", false, true ); }

				// invalid birth date, but not a stillbirth
				if( birth_date_valid != 1 && ! stillbirth.startsWith( "y" ) )
				{
					if( debug ) { showMessage( "invalid birth date", false, true ); }

					mmds.setTypeDate( "birth_date" );
					type_date = "birth";
					mmds.setDate( birth_date );

					DivideMinMaxDatumSet ddmdBirth = minMaxDate( debug, dbconRef, mmds );
					ddmdBirth.nonnegative();        // is this necessary?

					if( debug ) { showMessage( "Birth.getMinMaxDate(): " + ddmdBirth.getMinMaxDate(), false, true ); }

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

					dbconCleaned.executeUpdate( queryBirth );
				}
				else { if( debug ) { showMessage( "birth date is valid: " + birth_date, false, true ); } }

				if( mar_date_valid != 1 )                // invalid marriage date
				{
					if( debug ) { showMessage( "invalid marriage date", false, true ); }

					mmds.setTypeDate( "marriage_date" );
					type_date = "mar";
					mmds.setDate( mar_date );

					DivideMinMaxDatumSet ddmdMarriage = minMaxDate( debug, dbconRef, mmds );
					ddmdMarriage.nonnegative();        // is this necessary?

					if( debug ) { showMessage( "Marriage.getMinMaxDate(): " + ddmdMarriage.getMinMaxDate(), false, true ); }

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

					dbconCleaned.executeUpdate( queryMar );
				}
				else { if( debug ) { showMessage( "mar date is valid: " + mar_date, false, true ); } }

				if( death_date_valid != 1 )                // invalid death date
				{
					if( debug ) { showMessage( "invalid death date", false, true ); }

					mmds.setTypeDate( "death_date" );
					type_date = "death";
					mmds.setDate( death_date );

					DivideMinMaxDatumSet ddmdDeath = minMaxDate( debug, dbconRef, mmds );
					ddmdDeath.nonnegative();        // is this necessary?

					if( debug ) { showMessage( "Death.getMinMaxDate(): " + ddmdDeath.getMinMaxDate(), false, true ); }

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

					dbconCleaned.executeUpdate( queryDeath );
				}
				else { if( debug ) { showMessage( "death date is valid: " + death_date, false, true ); } }
			}
		}
		catch( Exception ex ) {
			String msg = String.format( "Thread id %02d; count: %d, Exception in minMaxDateMain: %s", threadId, count, ex.getMessage() );
			showMessage( msg, false, true );
			showMessage( "id_registration: " + id_registration + ", id_person: " + id_person, false, true );
			ex.printStackTrace( new PrintStream( System.out ) );
		}
	} // minMaxDateMain


	/**
	 * minMaxDate()
	 * @param debug
	 * @param dbconRef
	 * @param inputInfo
	 * @return
	 * @throws Exception
	 *
	 * Called by minMaxDateMain for invalid dates
	 */
	private DivideMinMaxDatumSet minMaxDate( boolean debug, HikariConnection dbconRef, MinMaxDateSet inputInfo )
		throws Exception
	{
		if( debug && inputInfo.getPersonRole() == 0 ) { showMessage( "minMaxDate() role = 0", false, true ); }

		if( debug ) { showMessage( "minMaxDate()", false, true ); }

		// registration date
		if( debug ) { showMessage( "inputInfo.getRegistrationDate()", false, true ); }
		DateYearMonthDaySet inputregistrationYearMonthDay = LinksSpecific.divideCheckDate( inputInfo.getRegistrationDate() );

		// age given in years
		if( inputInfo.getPersonAgeYear() > 0 )
		{
			if( debug ) { showMessage( "age given in years: " + inputInfo.getPersonAgeYear(), false, true ); }

			DivideMinMaxDatumSet returnSet = new DivideMinMaxDatumSet();

			if( inputInfo.getPersonRole() == 10 )   // it is the deceased
			{
				// death date
				if( debug ) { showMessage( "inputInfo.getDeathDate()", false, true ); }
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
					returnSet.setMaxDay(   inputregistrationYearMonthDay.getDay() );
					returnSet.setMaxMonth( inputregistrationYearMonthDay.getMonth() );
					returnSet.setMinDay(   inputregistrationYearMonthDay.getDay() );
					returnSet.setMinMonth( inputregistrationYearMonthDay.getMonth() );
				}
			}
			else        // it is not the deceased
			{
				// Day no en month no are similar to regis date
				returnSet.setMaxDay(   inputregistrationYearMonthDay.getDay() );
				returnSet.setMaxMonth( inputregistrationYearMonthDay.getMonth() );
				returnSet.setMinDay(   inputregistrationYearMonthDay.getDay() );
				returnSet.setMinMonth( inputregistrationYearMonthDay.getMonth() );
			}

			MinMaxYearSet mmj = minMaxCalculation(
				debug,
				dbconRef,
				inputInfo.getSourceId(),
				inputInfo.getPersonId(),
				inputInfo.getRegistrationId(),
				inputregistrationYearMonthDay.getYear(),
				inputInfo.getRegistrationMainType(),
				inputInfo.getTypeDate(),
				inputInfo.getPersonRole(),
				inputInfo.getDeath(),
				inputInfo.getPersonAgeYear() );

			returnSet.setMinYear( mmj.getMinYear() );
			returnSet.setMaxYear( mmj.getMaxYear() );

			//showMessage( "1 - age given in years", false, true );
			return returnSet;
		} // age given in years


		// birth year given?
		if( inputInfo.getPersonBirthYear() > 0 )
		{
			if( debug ) { showMessage( "birth year given: " + inputInfo.getPersonBirthYear() , false, true ); }

			int ageInYears = 0;     // use 0 for birth certificates
			int regis_year = inputregistrationYearMonthDay.getYear();

			// need a registration year, and must not be a birth certificate
			if( regis_year > 0 && inputInfo.getRegistrationMainType() != 1 )
			{
				int birth_year  = inputInfo.getPersonBirthYear();
				int birth_month = inputInfo.getPersonBirthMonth();
				int birth_day   = inputInfo.getPersonBirthDay();

				int regis_month = inputregistrationYearMonthDay.getMonth();
				int regis_day   = inputregistrationYearMonthDay.getDay();

				// need valid components for LocalDate
				if( birth_month <= 0 ) { birth_month = 6; }
				if( regis_month <= 0 ) { regis_month = 6; }

				if( birth_day <= 0 ) { birth_day = 15; }
				if( regis_day <= 0 ) { regis_day = 15; }

				// age is = regis jaar - birth year             // old code
				//int AgeInYears = regis_year - birth_year;     // old code

				LocalDate ldBirth = LocalDate.of( birth_year, birth_month, birth_day );
				LocalDate ldRegis = LocalDate.of( regis_year, regis_month, regis_day );

				Period betweenDates = Period.between( ldBirth, ldRegis );

				ageInYears  = betweenDates.getYears();      // overwrite the initial ageInYears = 0

				if( debug ) {
					int ageInMonths = betweenDates.getMonths();
					int ageInDays   = betweenDates.getDays();

					String msg = String.format( "ageInYears: %d, ageInMonths: %d, ageInDays: %d", ageInYears, ageInMonths, ageInDays );
					showMessage( msg , false, true );
				}
			}

			// Create new set
			DivideMinMaxDatumSet returnSet = new DivideMinMaxDatumSet();

			// Day no and month similar to regis date
			returnSet.setMaxDay(   inputregistrationYearMonthDay.getDay() );
			returnSet.setMaxMonth( inputregistrationYearMonthDay.getMonth() );
			returnSet.setMinDay(   inputregistrationYearMonthDay.getDay() );
			returnSet.setMinMonth( inputregistrationYearMonthDay.getMonth() );

			MinMaxYearSet mmj = minMaxCalculation(
				debug,
				dbconRef,
				inputInfo.getSourceId(),
				inputInfo.getPersonId(),
				inputInfo.getRegistrationId(),
				regis_year,
				inputInfo.getRegistrationMainType(),
				inputInfo.getTypeDate(),
				inputInfo.getPersonRole(),
				inputInfo.getDeath(),
				ageInYears );

			returnSet.setMinYear( mmj.getMinYear() );
			returnSet.setMaxYear( mmj.getMaxYear() );

			//showMessage( "2 - birth year given", false, true );
			return returnSet;
		} // birth year given


		// not the deceased
		if( inputInfo.getPersonRole() != 10 )
		{
			if( debug ) { showMessage( "not the deceased, role: " + inputInfo.getPersonRole() , false, true ); }

			// Days, month, weeks to 1 additional year ?
			int ageinYears = roundDownAge(
				inputInfo.getPersonAgeYear(),
				inputInfo.getPersonAgeMonth(),
				inputInfo.getPersonAgeWeek(),
				inputInfo.getPersonAgeDay() );

			DivideMinMaxDatumSet returnSet = new DivideMinMaxDatumSet();

			// day and month is similar to registration date
			returnSet.setMaxDay(   inputregistrationYearMonthDay.getDay() );
			returnSet.setMaxMonth( inputregistrationYearMonthDay.getMonth() );
			returnSet.setMinDay(   inputregistrationYearMonthDay.getDay() );
			returnSet.setMinMonth( inputregistrationYearMonthDay.getMonth() );

			MinMaxYearSet mmj = minMaxCalculation(
				debug,
				dbconRef,
				inputInfo.getSourceId(),
				inputInfo.getPersonId(),
				inputInfo.getRegistrationId(),
				inputregistrationYearMonthDay.getYear(),
				inputInfo.getRegistrationMainType(),
				inputInfo.getTypeDate(),
				inputInfo.getPersonRole(),
				inputInfo.getDeath(),
				ageinYears );

			returnSet.setMinYear( mmj.getMinYear() );
			returnSet.setMaxYear( mmj.getMaxYear() );

			//showMessage( "3 - not the deceased", false, true );
			return returnSet;
		} // not the deceased


		// combination of days and weeks and months
		int areMonths = 0;
		int areWeeks  = 0;
		int areDays   = 0;

		if( inputInfo.getPersonAgeMonth() > 0 ) { areMonths = 1; }
		if( inputInfo.getPersonAgeWeek()  > 0 ) { areWeeks  = 1; }
		if( inputInfo.getPersonAgeDay()   > 0 ) { areDays   = 1; }

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

			//showMessage( "4 - combination; marriage date, return 0-0-0", false, true );
			return returnSet;
		} // combination; marriage date, return 0-0-0


		if( debug ) { showMessage( "inputInfo.getDeathDate()", false, true ); }
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
			useYear  = inputregistrationYearMonthDay.getYear();
			useMonth = inputregistrationYearMonthDay.getMonth();
			useDay   = inputregistrationYearMonthDay.getDay();
		}
		if( debug ) { showMessage( String.format( "useDay: %d, useMonth: %d, useYear: %d", useDay, useMonth, useYear ), false, true ); }


		if( ( areMonths + areWeeks + areDays ) >= 2 )         // at least 2 given
		{
			// weeks and months to days
			int days = inputInfo.getPersonAgeMonth() * 30;
			days += inputInfo.getPersonAgeWeek() * 7;

			// new date -> date - (days - 1)

			int mindays = (days - 1) * -1;
			int maxdays = (days + 1) * -1;

			// Min date
			String minDate = addTimeToDate(
				useYear,
				useMonth,
				useDay,
				TimeType.DAY,
				mindays );

			// Max date
			String maxDate = addTimeToDate(
				useYear,
				useMonth,
				useDay,
				TimeType.DAY,
				maxdays );

			if( debug ) { showMessage( "at least 2 given minDate", false, true ); }
			DateYearMonthDaySet computedMinDate = LinksSpecific.divideCheckDate( minDate );
			if( debug ) { showMessage( "at least 2 given maxDate", false, true ); }
			DateYearMonthDaySet computedMaxDate = LinksSpecific.divideCheckDate( maxDate );

			// Check if max date not later than registration date
			DateYearMonthDaySet dymd = checkMaxDate(
				computedMaxDate.getYear(),
				computedMaxDate.getMonth(),
				computedMaxDate.getDay(),
				useYear,
				useMonth,
				useDay );

			DivideMinMaxDatumSet returnSet = new DivideMinMaxDatumSet();

			returnSet.setMaxDay(   dymd.getDay() );
			returnSet.setMaxMonth( dymd.getMonth() );
			returnSet.setMaxYear(  dymd.getYear() );
			returnSet.setMinDay(   computedMinDate.getDay() );
			returnSet.setMinMonth( computedMinDate.getMonth() );
			returnSet.setMinYear(  computedMinDate.getYear() );

			//showMessage( "5 - combination", false, true );
			return returnSet;
		} // combination


		// age given in months
		else if( areMonths == 1 )
		{
			// convert months
			int days = inputInfo.getPersonAgeMonth() * 30;

			// compute date
			// new date -> date - (days - 1)
			days++;

			int mindagen = (days + 30) * -1;
			int maxdagen = (days - 30) * -1;

			// Min date
			String minDate = addTimeToDate(
				useYear,
				useMonth,
				useDay,
				TimeType.DAY,
				mindagen );

			// Max date
			String maxDate = addTimeToDate(
				useYear,
				useMonth,
				useDay,
				TimeType.DAY,
				maxdagen );

			// New date
			if( debug ) { showMessage( "age given in months minDate", false, true ); }
			DateYearMonthDaySet computedMinDate = LinksSpecific.divideCheckDate( minDate );
			if( debug ) { showMessage( "age given in months maxDate", false, true ); }
			DateYearMonthDaySet computedMaxDate = LinksSpecific.divideCheckDate( maxDate );

			DivideMinMaxDatumSet returnSet = new DivideMinMaxDatumSet();

			returnSet.setMaxDay(   computedMaxDate.getDay() );
			returnSet.setMaxMonth( computedMaxDate.getMonth() );
			returnSet.setMaxYear(  computedMaxDate.getYear() );
			returnSet.setMinDay(   computedMinDate.getDay() );
			returnSet.setMinMonth( computedMinDate.getMonth() );
			returnSet.setMinYear(  computedMinDate.getYear() );

			//showMessage( "6 - combination", false, true );
			return returnSet;
		} // age given in months


		// age given in weeks
		else if( areWeeks == 1 )
		{
			// weeks and months to days
			int days = inputInfo.getPersonAgeWeek() * 7;

			// compute date

			// new date -> date - (days - 1)
			days++;

			int mindays = (days + 8) * -1;
			int maxdays = (days - 8) * -1;

			// Min date
			String minDate = addTimeToDate(
				useYear,
				useMonth,
				useDay,
				TimeType.DAY,
				mindays );

			// Max date
			String maxDate = addTimeToDate(
				useYear,
				useMonth,
				useDay,
				TimeType.DAY,
				maxdays );

			if( debug ) { showMessage( "age given in weeks minDate", false, true ); }
			DateYearMonthDaySet computedMinDate = LinksSpecific.divideCheckDate( minDate );
			if( debug ) { showMessage( "age given in weeks maxDate", false, true ); }
			DateYearMonthDaySet computedMaxDate = LinksSpecific.divideCheckDate( maxDate );

			// return
			DivideMinMaxDatumSet returnSet = new DivideMinMaxDatumSet();

			returnSet.setMaxDay(   computedMaxDate.getDay() );
			returnSet.setMaxMonth( computedMaxDate.getMonth() );
			returnSet.setMaxYear(  computedMaxDate.getYear() );
			returnSet.setMinDay(   computedMinDate.getDay() );
			returnSet.setMinMonth( computedMinDate.getMonth() );
			returnSet.setMinYear(  computedMinDate.getYear() );

			//showMessage( "7 - age given in weeks", false, true );
			return returnSet;
		} // age given in weeks


		// age given in days
		else if( areDays == 1 )
		{
			// weeks and months to days
			int days = inputInfo.getPersonAgeDay();

			// new date -> date - (days - 1)

			int mindays = (days + 4) * -1;
			//int maxdays = (days - 4) * -1;    // NO, set to registration date (below)

			// min date
			String minDate = addTimeToDate(
				useYear,
				useMonth,
				useDay,
				TimeType.DAY,
				mindays );

			String maxDate = inputInfo.getRegistrationDate();

			// New date to return value
			if( debug ) { showMessage( "age given in days minDate", false, true ); }
			DateYearMonthDaySet computedMinDate = LinksSpecific.divideCheckDate( minDate );
			if( debug ) { showMessage( "age given in days maxDate", false, true ); }
			DateYearMonthDaySet computedMaxDate = LinksSpecific.divideCheckDate( maxDate );


			// Check if max date not later than registration date
			DateYearMonthDaySet dymd = checkMaxDate(
				computedMaxDate.getYear(),
				computedMaxDate.getMonth(),
				computedMaxDate.getDay(),
				useYear,
				useMonth,
				useDay );

			// return
			DivideMinMaxDatumSet returnSet = new DivideMinMaxDatumSet();

			returnSet.setMaxDay(   dymd.getDay() );
			returnSet.setMaxMonth( dymd.getMonth() );
			returnSet.setMaxYear(  dymd.getYear() );
			returnSet.setMinDay(   computedMinDate.getDay() );
			returnSet.setMinMonth( computedMinDate.getMonth() );
			returnSet.setMinYear(  computedMinDate.getYear() );

			//showMessage( "8 - age given in weeks", false, true );
			return returnSet;
		} // age given in days


		// age not given
		DivideMinMaxDatumSet returnSet = new DivideMinMaxDatumSet();

		// day and month similar to registration date
		returnSet.setMaxDay(   inputregistrationYearMonthDay.getDay() );
		returnSet.setMaxMonth( inputregistrationYearMonthDay.getMonth() );
		returnSet.setMinDay(   inputregistrationYearMonthDay.getDay() );
		returnSet.setMinMonth( inputregistrationYearMonthDay.getMonth() );

		MinMaxYearSet mmj = minMaxCalculation(
			debug,
			dbconRef,
			inputInfo.getSourceId(),
			inputInfo.getPersonId(),
			inputInfo.getRegistrationId(),
			inputregistrationYearMonthDay.getYear(),
			inputInfo.getRegistrationMainType(),
			inputInfo.getTypeDate(),
			inputInfo.getPersonRole(),
			inputInfo.getDeath(),
			0 );

		returnSet.setMinYear( mmj.getMinYear() );
		returnSet.setMaxYear( mmj.getMaxYear() );

		//showMessage( "9 - age not given", false, true );
		return returnSet;   // age not given
	} // minMaxDate


	/**
	 * minMaxCalculation()
	 * @param debug
	 * @param dbconRef
	 * @param id_source
	 * @param id_person
	 * @param id_registration
	 * @param reg_year
	 * @param main_type
	 * @param date_type
	 * @param role
	 * @param death
	 * @param age
	 * @return
	 * @throws Exception
	 */
	private MinMaxYearSet minMaxCalculation
	(
		boolean debug,
		HikariConnection dbconRef,
		int     id_source,
		int     id_person,
		int     id_registration,
		int     reg_year,
		int     main_type,
		String  date_type,
		int     role,
		String  death,
		int     age
	)
	throws Exception
	{
		if( debug ) {
			showMessage( "minMaxCalculation() death = " + death + ", age = " + age, false, true );
			if( role == 0 ) { showMessage( "minMaxCalculation() role = 0, id_person = " + id_person, false, true ); }
		}

		String source = Integer.toString( id_source );

		String min_age_0 = "n";
		String max_age_0 = "n";
		String age_main_role = "nvt";

		String age_reported  = "";
		String function = "";
		int min_year  = 0;
		int max_year  = 0;
		int main_role = 0;

		if( age > 0 )
		{
			age_reported  = "y";
		}
		else
		{
			if( death == null || death.isEmpty() ) { death = "n"; }
			age_reported = death;

			MinMaxMainAgeSet mmmas = minMaxMainAge
			(
				debug,
				dbconRef,
				id_source,
				id_person,
				id_registration,
				main_type,
				date_type,
				role,
				age_reported,
				age_main_role
			);

			main_role = mmmas.getMainRole();
			age       = mmmas.getAgeYear();
			min_age_0 = mmmas.getMinAge0();
			max_age_0 = mmmas.getMaxAge0();

			function = mmmas.getFunction();
			min_year = mmmas.getMinYear();
			max_year = mmmas.getMaxYear();

			mmmas = null;

			if( debug ) { showMessage( "after minMaxMainAge() age: " + age + ", function: " + function +
				", min_year: " + min_year + ", max_year: " + max_year + ", main_role: " + main_role, false, true ); }

			if( role == main_role ) { age_main_role = "nvt"; }
			else {
				if( age > 0 ) { age_main_role = "y"; }
				else { age_main_role = "n"; }
			}
		}

		String query = "SELECT function, min_year, max_year FROM ref_date_minmax"
			+ " WHERE maintype = '"    + main_type + "'"
			+ " AND role = '"          + role + "'"
			+ " AND date_type = '"     + date_type + "'"
			+ " AND age_reported = '"  + age_reported + "'"
			+ " AND age_main_role = '" + age_main_role  + "'";

		if( debug ) { showMessage( query, false, true ); }

		MinMaxYearSet mmj = new MinMaxYearSet();

		try( ResultSet rs = dbconRef.executeQuery( query ) )
		{
			if (!rs.next()) {
				if (debug) {
					showMessage("Not found", false, true);
					showMessage(query, false, true);
				}

				addToReportPerson(id_person, source, 105, "Null -> [rh:" + main_type + "][ad:" + date_type + "][rol:" + role + "][lg:" + age_reported + "][lh:" + age_main_role + "]");

				return mmj;
			}

			rs.last();        // to last row

			function = rs.getString("function");
			min_year = rs.getInt("min_year");
			max_year = rs.getInt("max_year");

			if (debug) {
				showMessage("minMaxCalculation() age: " + age + ", function: " + function +
					", min_year: " + min_year + ", max_year: " + max_year + ", main_role: " + main_role, false, true);
			}

			int agecopy = age;
			if (min_age_0 == "y") {
				agecopy = 0;
			}
			int minimum_year = reg_year - agecopy + min_year;
			if (debug) {
				showMessage("minMaxCalculation() min_age_0: " + min_age_0 + ", agecopy: " + agecopy + ", min_year: " + min_year, false, true);
			}

			agecopy = age;
			if (max_age_0 == "y") {
				agecopy = 0;
			}
			int maximum_year = reg_year - agecopy + max_year;
			if (debug) {
				showMessage("minMaxCalculation() max_age_0: " + max_age_0 + ", agecopy: " + agecopy + ", max_year: " + max_year, false, true);
			}

			mmj.setMinYear(minimum_year);
			mmj.setMaxYear(maximum_year);

			// function "B" is not needed here; its role is being dealt with somewhere else [minMaxDate() ?]

			if (function.equals("A"))                    // function A, the contents of mmj is already OK
			{
				;
			} else if (function.equals("C"))               // function C, check by reg year
			{
				if (maximum_year > reg_year) {
					mmj.setMaxYear(reg_year);
				}
			} else if (function.equals("D"))               // function D
			{
				if (minimum_year > (reg_year - 14)) {
					mmj.setMinYear(reg_year - 14);
				}
				if (maximum_year > (reg_year - 14)) {
					mmj.setMaxYear(reg_year - 14);
				}
			} else if (function.equals("E"))               // If E, deceased
			{
				if (minimum_year > reg_year) {
					mmj.setMinYear(reg_year);
				}
				if (maximum_year > reg_year) {
					mmj.setMaxYear(reg_year);
				}
			} else if (function.equals("F"))               // function F
			{
				if (minimum_year < reg_year) {
					mmj.setMinYear(reg_year);
				}
			} else if (function.equals("G"))               // function F
			{
				if (minimum_year < reg_year) {
					mmj.setMinYear(reg_year);
				}
				if (maximum_year > (reg_year + 86)) {
					mmj.setMaxYear(reg_year + 86);
				}
			} else if (function.equals("H"))               // function H
			{
				if (maximum_year > (reg_year + 86)) {
					mmj.setMaxYear(reg_year + 86);
				}
			} else if (function.equals("I"))               // function I
			{
				if (maximum_year > reg_year) {
					mmj.setMaxYear(reg_year);
				}
				if (minimum_year > mmj.getMaxYear()) {
					mmj.setMinYear(mmj.getMaxYear());
				}
			} else {
				if (debug) {
					showMessage("minMaxCalculation() function = " + function, false, true);
				}
				addToReportPerson(id_person, "0", 104, "Null -> [rh:" + main_type + "][ad:" + date_type + "][rol:" + role + "][lg:" + age_reported + "][lh:" + age_main_role + "]");
			}

			if (mmj.getMinYear() > mmj.getMaxYear())   // min/max consistency check
			{
				//showMessage( "minMaxCalculation() Error: min_year exceeds max_year for id_person = " + id_person, false, true );
				String msg_minmax = "minYear: " + mmj.getMinYear() + ", maxYear: " + mmj.getMaxYear();
				addToReportPerson(id_person, source, 266, msg_minmax);       // error 266 + min & max

				// KM: day & month both from registration date, so with min_year = max_year date are equal
				mmj.setMinYear(mmj.getMaxYear());                 // min = max
			}
		}

		return mmj;
	} // minMaxCalculation


	/**
	 * minMaxMainAge()
	 * @param debug
	 * @param dbconRef
	 * @param id_source
	 * @param id_person
	 * @param id_registration
	 * @param main_type
	 * @param date_type
	 * @param role
	 * @param age_reported
	 * @param age_main_role
	 * @return
	 * @throws Exception
	 */
	private MinMaxMainAgeSet minMaxMainAge
	(
		boolean debug,
		HikariConnection dbconRef,
		int     id_source,
		int     id_person,
		int     id_registration,
		int     main_type,
		String  date_type,
		int     role,
		String  age_reported,
		String  age_main_role
	)
		throws Exception
	{
		if( debug && role == 0 ) { showMessage( "minMaxMainAge() role = 0, id_person = " + id_person , false, true ); }

		boolean done = false;

		MinMaxMainAgeSet mmmas = new MinMaxMainAgeSet();

		if( debug ) { showMessage( "minMaxMainAge() function: " + mmmas.getFunction(), false, true ); }

		mmmas.setMinAge0( "n" );
		mmmas.setMaxAge0( "n" );

		int loop = 0;
		while( !done )
		{
			if( debug ) { showMessage( "minMaxMainAge(): age_main_role = " + age_main_role + " [loop = " + loop + "]", false, true ); }

			String queryRef = "SELECT function, min_year, max_year, min_person, max_person FROM links_general.ref_date_minmax"
				+ " WHERE maintype = '"    + main_type + "'"
				+ " AND role = '"          + role + "'"
				+ " AND date_type = '"     + date_type + "'"
				+ " AND age_reported = '"  + age_reported + "'"
				+ " AND age_main_role = '" + age_main_role + "'";

			if( debug ) { showMessage( queryRef, false, true ); }

			try( ResultSet rs_ref = dbconRef.executeQuery( queryRef ) )
			{
				int min_person_role = 0;
				int max_person_role = 0;

				if( !rs_ref.next() )
				{
					if( debug ) { showMessage( "Not found; age_main_role = " + age_main_role, false, true ); }

					if( age_main_role.equals( "nvt" ) ) { age_main_role = "y"; }
					else { age_main_role = "n"; }
				}
				else
				{
					if( debug ) { showMessage( "Found", false, true ); }

					min_person_role = rs_ref.getInt( "min_person" );
					max_person_role = rs_ref.getInt( "max_person" );
					if( debug ) { showMessage( "min_person_role = " + min_person_role + ", max_person_role = " + max_person_role, false, true ); }

					mmmas.setFunction( rs_ref.getString( "function" ) );
					mmmas.setMinYear(rs_ref.getInt( "min_year" ));
					mmmas.setMaxYear(rs_ref.getInt( "max_year" ));

					boolean readPc = false;
					int mm_main_role = 0;

					if( min_person_role > 0 ) {
						readPc = true;
						mm_main_role = min_person_role;
					}
					else {
						done = true;
						mmmas.setMinAge0( "y" );
					}

					if( max_person_role > 0 ) {
						readPc = true;
						mm_main_role = max_person_role;
					}
					else {
						done = true;
						mmmas.setMaxAge0( "y" );
					}

					if( debug ) { showMessage( "querying person_c: " + readPc, false, true ); }
					if( readPc ) {
						String queryPc = "SELECT age_day, age_week, age_month, age_year, role FROM links_cleaned.person_c"
							+ " WHERE id_registration = " + id_registration
							+ " AND role = " + mm_main_role;

						if( debug ) { showMessage( queryPc, false, true ); }

						int age_day = 0;
						int age_week = 0;
						int age_month = 0;
						int age_year = 0;

						int main_role = 0;
						int countPc = 0;

						try( ResultSet rs_pc = dbconCleaned.executeQuery( queryPc ) )
						{
							while( rs_pc.next() ) {
								countPc++;
								age_day   = rs_pc.getInt( "age_day" );
								age_week  = rs_pc.getInt( "age_week" );
								age_month = rs_pc.getInt( "age_month" );
								age_year  = rs_pc.getInt( "age_year" );
								main_role = rs_pc.getInt( "role" );

								mmmas.setAgeYear( age_year );
								mmmas.setMainRole( main_role );
							}
						}

						if( countPc == 0 )
						{
							if( debug ) { showMessage( "minMaxMainAge: person_c count = 0, with query:", false, true ); }
							if( debug ) { showMessage( queryPc, false, true ); }
							if( mm_main_role ==  1 ) { addToReportRegistration( id_registration, "" + id_source, 271, "" ); }   // WA 271
							else if( mm_main_role ==  4 ) { addToReportRegistration( id_registration, "" + id_source, 272, "" ); }   // WA 272
							else if( mm_main_role ==  7 ) { addToReportRegistration( id_registration, "" + id_source, 273, "" ); }   // WA 273
							else if( mm_main_role == 10 ) { addToReportRegistration( id_registration, "" + id_source, 274, "" ); }   // WA 274
						}
						else if( countPc > 1 )
						{
							if( debug ) { showMessage( "minMaxMainAge: person_c count > 1, with query:", false, true ); }
							if( debug ) { showMessage( queryPc, false, true ); }
							if( mm_main_role ==  1 ) { addToReportRegistration( id_registration, "" + id_source, 281, "" ); }   // WA 281
							else if( mm_main_role ==  4 ) { addToReportRegistration( id_registration, "" + id_source, 282, "" ); }   // WA 282
							else if( mm_main_role ==  7 ) { addToReportRegistration( id_registration, "" + id_source, 283, "" ); }   // WA 283
							else if( mm_main_role == 10 ) { addToReportRegistration( id_registration, "" + id_source, 284, "" ); }   // WA 284
						}

						if( debug ) { showMessage( "minMaxMainAge() age_day: " + age_day + ", age_week: " + age_week + ", age_month: " + age_month + ", age_year: " + age_year + ", main_role: " + main_role, false, true ); }

						if( age_day > 0 || age_week > 0 || age_month > 0 ) {
							int iyear = age_month / 12;
							// Adding 1 is only for getting proper values of date range estimates (e.g., of parents of a baby that only lived a few days).
							// the person_c.age_year should not be changed; it may be 0
							age_year += ( 1 + iyear );
							mmmas.setAgeYear( age_year );
						}

						if( age_year > 0 ) { done = true; }
						else {
							if( age_main_role.equals( "y" ) ) { age_main_role = "n"; }
							else { done = true; }
						}
					}
					else { done = true; }

				}
			}

			if( done == false && loop > 2 ) {
				if( debug ) {
					// no ref_date_minmax entry that satisfies the conditions
					showMessage( "minMaxMainAge(): id_person = " + id_person + ", looping too much, quit (warning 106)", false, true );
					showMessage( queryRef, false, true );
				}
				addToReportPerson( id_person, "0", 106, "" );
				done = true;
			}
			loop++;
		} // while

		return mmmas;

	} // minMaxMainAge


	/**
	 * roundDownAge()
	 * @param year
	 * @param month
	 * @param week
	 * @param day
	 * @return
	 */
	public int roundDownAge( int year, int month, int week, int day )
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
	} // roundDownAge


	/**
	 * addTimeToDate()
	 * @param year
	 * @param month
	 * @param day
	 * @param tt
	 * @param timeAmount
	 * @return
	 *
	 * Use this function to add or subtract an amount of time from a date.
	 * NOTICE: the Java Calendar.MONTH values are [0...11]
	 * Our month values are [1...12]
	 * So: subtract 1 in the begining, add 1 in the end.
	 */
	private String addTimeToDate( int year, int month, int day, TimeType tt, int timeAmount )
	{
		Calendar c1 = Calendar.getInstance();       // new calendar instance
		c1.set( year, month -1, day );              // set(int year, int month, int date); subtract 1 from month number

		//String msg = String.format( "%02d-%02d-%04d", c1.get( Calendar.DAY_OF_MONTH ), c1.get( Calendar.MONTH ), c1.get( Calendar.YEAR ) );
		//showMessage( msg, false, true );
		//showMessage( String.format( "timeAmount: %d", timeAmount ), false, true );

		// Check of time type
		if( tt == tt.DAY )   { c1.add( Calendar.DAY_OF_MONTH,  timeAmount ); }
		else if( tt == tt.WEEK )  { c1.add( Calendar.WEEK_OF_MONTH, timeAmount ); }
		else if( tt == tt.MONTH ) { c1.add( Calendar.MONTH,         timeAmount ); }
		else if( tt == tt.YEAR )  { c1.add( Calendar.YEAR,          timeAmount ); }

		// add 1 to month number
		String am = String.format( "%02d-%02d-%04d", c1.get( Calendar.DAY_OF_MONTH ), 1 + c1.get( Calendar.MONTH ), c1.get( Calendar.YEAR ) );
		//showMessage( am, false, true );

		return am;
	} // addTimeToDate


	/**
	 * checkMaxDate()
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

			//return registration date
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

		// month is higher than registration month
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

		// day is higher than registration day
		if (pDay > rDay) {

			// return act date
			DateYearMonthDaySet dy = new DateYearMonthDaySet( "" );
			dy.setYear(rYear);
			dy.setMonth(rMonth);
			dy.setDay(rDay);
			return dy;
		}

		// day is lower or similar to registration day
		DateYearMonthDaySet dy = new DateYearMonthDaySet( "" );
		dy.setYear(pYear);
		dy.setMonth(pMonth);
		dy.setDay(pDay);
		return dy;
	} // checkMaxDate


	/*---< Min Max Marriage >-------------------------------------------------*/

	/**
	 * doMinMaxMarriage()
	 * @param debug
	 * @param go
	 * @param source
	 * @param rmtype
	 * @throws Exception
	 */
	private void doMinMaxMarriage( boolean debug, boolean go, String source, String rmtype ) throws Exception
	{
		long threadId = Thread.currentThread().getId();

		String funcname = String.format( "Thread id %02d; doMinMaxMarriage for source: %s, rmtype: %s", threadId, source, rmtype );

		if( !go ) {
			if( showskip ) { showMessage( "Skipping " + funcname, false, true ); }
			return;
		}

		long timeStart = System.currentTimeMillis();
		showMessage( funcname + " ...", false, true );

		long start = System.currentTimeMillis();

		TableToArrayListMultimapHikari almmMarriageYear = new TableToArrayListMultimapHikari( dsReference, "ref_minmax_marriageyear", "role_A", "role_B" );

		showTimingMessage( String.format( "Thread id %02d; Loaded MarriageYear reference table", threadId ), start );

		int numrows = almmMarriageYear.numrows();
		int numkeys = almmMarriageYear.numkeys();
		showMessage( String.format( "Thread id %02d; Number of rows in reference table ref_role: %d", threadId, numrows ), false, true );
		if( numrows != numkeys )
		{ showMessage( String.format( "Thread id %02d; Number of keys in arraylist multimap: %d", threadId, numkeys ), false, true ); }

		minMaxMarriageYear( debug, almmMarriageYear, source, rmtype );

		almmMarriageYear.free();
		showMessage( String.format( "Thread id %02d; Freed almmMarriageYear", threadId ), false, true );

		// Notice: nothing to update for almmMarriageYear

		elapsedShowMessage( funcname, timeStart, System.currentTimeMillis() );
		showMessage_nl();
	} // doMinMaxMarriage


	/**
	 * minMaxMarriageYear()
	 * @param debug
	 * @param almmMarriageYear
	 * @param source
	 * @param rmtype
	 * @throws Exception
	 */
	private void minMaxMarriageYear( boolean debug, TableToArrayListMultimapHikari almmMarriageYear, String source, String rmtype )
		throws Exception
	{
		long threadId = Thread.currentThread().getId();
		int count = 0;
		int stepstate = count_step;

		String selectQueryA = ""
			+ " SELECT "
			+ " person_c.id_person ,"
			+ " person_c.id_registration ,"
			+ " person_c.registration_maintype ,"
			+ " person_c.role ,"
			+ " person_c.mar_day_min ,"
			+ " person_c.mar_day_max ,"
			+ " person_c.mar_month_min ,"
			+ " person_c.mar_month_max ,"
			+ " person_c.mar_year_min ,"
			+ " person_c.mar_year_max"
			+ " FROM person_c"
			+ " WHERE id_source = " + source;

		if ( ! rmtype.isEmpty() ) { selectQueryA += " AND registration_maintype = " + rmtype; }
		if( debug ) { showMessage( "standardMinMaxMarriageYear() " + selectQueryA, false, true ); }

		try( ResultSet rsA = dbconCleaned.executeQuery( selectQueryA ) )
		{
			while( rsA.next() )
			{
				count++;
				if( count == stepstate ) {
					showMessage( count + "", true, true );
					stepstate += count_step;
				}

				int id_personA = rsA.getInt( "id_person" );
				int id_registration = rsA.getInt( "id_registration" );
				int roleA = rsA.getInt( "role" );

				int mar_day_minA   = rsA.getInt( "mar_day_min" );
				int mar_day_maxA   = rsA.getInt( "mar_day_max" );
				int mar_month_minA = rsA.getInt( "mar_month_min" );
				int mar_month_maxA = rsA.getInt( "mar_month_max" );
				int mar_year_minA  = rsA.getInt( "mar_year_min" );
				int mar_year_maxA  = rsA.getInt( "mar_year_max" );

				if( ( mar_year_minA == 0 && mar_month_minA == 0 && mar_day_minA == 0 ) ||
					( mar_year_maxA == 0 && mar_month_maxA == 0 && mar_day_maxA == 0 ) )
				{ continue; }       // do nothing

				String minA = "";
				String maxA = "";
				if( debug ) {
					minA = "[" + mar_year_minA + "." + mar_month_minA + "." + mar_day_minA + "]";
					maxA = "[" + mar_year_maxA + "." + mar_month_maxA + "." + mar_day_maxA + "]";

					showMessage( "id_person: " + id_personA, false, true );
					showMessage( "roleA: " + roleA + " " + minA + "-" + maxA, false, true );
				}

				String roleB = almmMarriageYear.standard( Integer.toString( roleA ) );
				if( roleB.isEmpty() ) { continue; }     // role 1, 4 & 7 not in ref table (because not needed)

				String selectQueryB = ""
					+ " SELECT "
					+ " person_c.id_person ,"
					+ " person_c.registration_maintype ,"
					+ " person_c.role ,"
					+ " person_c.mar_day_min ,"
					+ " person_c.mar_day_max ,"
					+ " person_c.mar_month_min ,"
					+ " person_c.mar_month_max ,"
					+ " person_c.mar_year_min ,"
					+ " person_c.mar_year_max"
					+ " FROM person_c"
					+ " WHERE person_c.id_registration = " + id_registration
					+ " AND role = " + roleB;

				if( debug) { showMessage( selectQueryB, false, true ); }

				int id_personB     = 0;
				int mar_day_minB   = 0;
				int mar_day_maxB   = 0;
				int mar_month_minB = 0;
				int mar_month_maxB = 0;
				int mar_year_minB  = 0;
				int mar_year_maxB  = 0;

				int countB = 0;
				try( ResultSet rsB = dbconCleaned.executeQuery( selectQueryB ) )
				{
					while (rsB.next()) {
						countB++;
						id_personB     = rsB.getInt( "id_person" );
						mar_day_minB   = rsB.getInt( "mar_day_min" );
						mar_day_maxB   = rsB.getInt( "mar_day_max" );
						mar_month_minB = rsB.getInt( "mar_month_min" );
						mar_month_maxB = rsB.getInt( "mar_month_max" );
						mar_year_minB  = rsB.getInt( "mar_year_min" );
						mar_year_maxB  = rsB.getInt( "mar_year_max" );
					}
				}

				if( countB == 0 ) {
					if( debug ) { showMessage( "No match for roleB: " + roleB, false, true ); }
					continue;
				}
				else if( countB != 1 ) {
					if( debug ) { showMessage( "standardMinMaxMarriageYear() id_person: " + id_personA + " has multiple roleB matches", false, true ); }
					addToReportPerson( id_personA, source + "", 107, "" );
					continue;
				}

				String minB = "";
				String maxB = "";
				if( debug ) {
					minB = "[" + mar_year_minB + "." + mar_month_minB + "." + mar_day_minB + "]";
					maxB = "[" + mar_year_maxB + "." + mar_month_maxB + "." + mar_day_maxB + "]";
					showMessage( "roleB: " + roleB + " " + minB + "-" + maxB, false, true );
				}

				// choose greater of min dates
				if( ! datesEqual( mar_year_minA, mar_month_minA, mar_day_minA, mar_year_minB, mar_month_minB, + mar_day_minB ) )
				{
					if( dateLeftIsGreater( mar_year_minA, mar_month_minA, mar_day_minA, mar_year_minB, mar_month_minB, + mar_day_minB ) ) {
						if( debug ) { showMessage( "update id_personB: " + id_personB + " with minimum " + minA, false, true ); }

						String updateQuery = ""
							+ "UPDATE links_cleaned.person_c SET"
							+ " mar_day_min = "   + mar_day_minA   + " , "
							+ " mar_month_min = " + mar_month_minA + " , "
							+ " mar_year_min = "  + mar_year_minA
							+ " WHERE id_person = " + "" + id_personB;

						if( debug) { showMessage( updateQuery, false, true ); }
						dbconCleaned.executeUpdate( updateQuery );
					}
					else {
						if( debug ) { showMessage( "update id_personA: " + id_personA + " with minimum " + minB, false, true ); }

						String updateQuery = ""
							+ "UPDATE links_cleaned.person_c SET"
							+ " mar_day_min = "   + mar_day_minB   + " , "
							+ " mar_month_min = " + mar_month_minB + " , "
							+ " mar_year_min = "  + mar_year_minB
							+ " WHERE id_person = " + "" + id_personA;

						if( debug) { showMessage( updateQuery, false, true ); }
						dbconCleaned.executeUpdate( updateQuery );
					}
				}

				// choose lesser of max dates
				if( ! datesEqual( mar_year_maxA, mar_month_maxA, mar_day_maxA, mar_year_maxB, mar_month_maxB, + mar_day_maxB ) )
				{
					if( dateLeftIsGreater( mar_year_maxA, mar_month_maxA, mar_day_maxA, mar_year_maxB, mar_month_maxB, + mar_day_maxB ) ) {
						if( debug ) { showMessage( "update id_personA: " + id_personA + " with maximum " + maxB, false, true ); }

						String updateQuery = ""
							+ "UPDATE links_cleaned.person_c SET"
							+ " mar_day_max = "   + mar_day_maxB   + " , "
							+ " mar_month_max = " + mar_month_maxB + " , "
							+ " mar_year_max = "  + mar_year_maxB
							+ " WHERE id_person = " + "" + id_personA;

						if( debug) { showMessage( updateQuery, false, true ); }
						dbconCleaned.executeUpdate( updateQuery );
					}
					else {
						if( debug ) { showMessage( "update id_personB: " + id_personB + " with maximun " + maxA, false, true ); }

						String updateQuery = ""
							+ "UPDATE links_cleaned.person_c SET"
							+ " mar_day_max = "   + mar_day_maxA   + " , "
							+ " mar_month_max = " + mar_month_maxA + " , "
							+ " mar_year_max = "  + mar_year_maxA
							+ " WHERE id_person = " + "" + id_personB;

						if( debug) { showMessage( updateQuery, false, true ); }
						dbconCleaned.executeUpdate( updateQuery );
					}
				}
			}
		}
		catch( Exception ex ) {
			String msg = String.format( "Thread id %02d; count: %d, Exception in minMaxMarriageYear: %s", threadId, count, ex.getMessage() );
			showMessage( msg, false, true );
			ex.printStackTrace( new PrintStream( System.out ) );
		}

	} // minMaxMarriageYear


	/**
	 * dateLeftIsGreater()
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
	 * datesEqual()
	 * @param lYear
	 * @param lMonth
	 * @param lDay
	 * @param rYear
	 * @param rMonth
	 * @param rDay
	 * @return
	 */
	private boolean datesEqual( int lYear, int lMonth, int lDay, int rYear, int rMonth, int rDay )
	{
		if( lYear == rYear && lMonth == rMonth && lDay == rDay ) { return true; }

		return false;
	} // datesEqual


	/*---< Parts to Full Date >-----------------------------------------------*/

	/**
	 * doPartsToFullDate()
	 * @param debug
	 * @param go
	 * @param source
	 * @param rmtype
	 * @throws Exception
	 */
	private void doPartsToFullDate( boolean debug, boolean go, String source, String rmtype ) throws Exception
	{
		long threadId = Thread.currentThread().getId();

		String funcname = String.format( "Thread id %02d; doPartsToFullDate for source: %s, rmtype: %s", threadId, source, rmtype );

		if( !go ) {
			if( showskip ) { showMessage( "Skipping " + funcname, false, true ); }
			return;
		}

		long timeStart = System.currentTimeMillis();
		showMessage( funcname + " ...", false, true );

		String msg = String.format( "Thread id %02d; Processing partsToFullDate for source: %s, rmtype: %s ...", threadId, source, rmtype );
		showMessage( msg, false, true );
		partsToFullDate( debug, source, rmtype );

		elapsedShowMessage( funcname, timeStart, System.currentTimeMillis() );
		showMessage_nl();
	} // doPartsToFullDate


	/**
	 * partsToFullDate()
	 * @param debug
	 * @param source
	 * @param rmtype
	 */
	private void partsToFullDate( boolean debug, String source, String rmtype )
	{
        /*
        Notice: the date components from person_c are INTs.
        Below they are CONCATenated directly, implying that no leading zeros are inserted in the output string,
        so we do NOT comply to the format '%02d-%02d-%04d' as dd-mm-yyyy.

        If we would additionally use the the MySQL STR_TO_DATE as:
        DATE = STR_TO_DATE( CONCAT( d, '-', m , '-', y), '%d-%m-%Y' );
        then we get our wanted leading zeros, but we also get by definition the DATE format as YYY-MM-DD.
        Because the format '%d-%m-%Y' is only used for the input; the output is fixed to YYY-MM-DD.

        We actually like YYY-MM-DD, but then we should convert ALL our date strings in links_cleaned to YYY-MM-DD.
        Also notice that STR_TO_DATE accepts 0's for components, but acts strange with 0 year:
        SELECT STR_TO_DATE( CONCAT( 0, '-', 0, '-', 0 ), '%d-%m-%Y' );
        +--------------------------------------------------------+
        | STR_TO_DATE( CONCAT( 0, '-', 0, '-', 0 ), '%d-%m-%Y' ) |
        +--------------------------------------------------------+
        | 2000-00-00                                             |
        +--------------------------------------------------------+
        The documented supported range of DATE is '1000-01-01' to '9999-12-31'.
        We should always check for 0 years, because then we have no valid date anyway
        The MySQL date/time functions are not rock solid, we maybe be we should avoid them, and use Java Joda-time.
        */

		long threadId = Thread.currentThread().getId();

		// Notice: should we alse have divorce_date_min & divorce_date_max ?
		String query = "UPDATE links_cleaned.person_c SET "
			+ "links_cleaned.person_c.birth_date_min   = CONCAT( links_cleaned.person_c.birth_day_min ,   '-' , links_cleaned.person_c.birth_month_min ,   '-' , links_cleaned.person_c.birth_year_min ) ,"
			+ "links_cleaned.person_c.mar_date_min     = CONCAT( links_cleaned.person_c.mar_day_min ,     '-' , links_cleaned.person_c.mar_month_min ,     '-' , links_cleaned.person_c.mar_year_min ) ,"
			+ "links_cleaned.person_c.divorce_date_min = CONCAT( links_cleaned.person_c.divorce_day_min , '-' , links_cleaned.person_c.divorce_month_min , '-' , links_cleaned.person_c.divorce_year_min ) ,"
			+ "links_cleaned.person_c.death_date_min   = CONCAT( links_cleaned.person_c.death_day_min ,   '-' , links_cleaned.person_c.death_month_min ,   '-' , links_cleaned.person_c.death_year_min ) ,"

			+ "links_cleaned.person_c.birth_date_max   = CONCAT( links_cleaned.person_c.birth_day_max ,   '-' , links_cleaned.person_c.birth_month_max ,   '-' , links_cleaned.person_c.birth_year_max ) ,"
			+ "links_cleaned.person_c.mar_date_max     = CONCAT( links_cleaned.person_c.mar_day_max ,     '-' , links_cleaned.person_c.mar_month_max ,     '-' , links_cleaned.person_c.mar_year_max ) ,"
			+ "links_cleaned.person_c.divorce_date_max = CONCAT( links_cleaned.person_c.divorce_day_max , '-' , links_cleaned.person_c.divorce_month_max , '-' , links_cleaned.person_c.divorce_year_max ) ,"
			+ "links_cleaned.person_c.death_date_max   = CONCAT( links_cleaned.person_c.death_day_max ,   '-' , links_cleaned.person_c.death_month_max ,   '-' , links_cleaned.person_c.death_year_max ) "

			+ "WHERE person_c.id_source = " + source;

		if ( ! rmtype.isEmpty() ) { query += " AND person_c.registration_maintype = " + rmtype; }
		if( debug ) { showMessage( query, false, true ); }

		try { dbconCleaned.executeUpdate( query ); }
		catch( Exception ex ) {
			String msg = String.format( "Thread id %02d; Exception in partsToFullDate: %s", threadId, ex.getMessage() );
			showMessage( msg, false, true );
			ex.printStackTrace( new PrintStream( System.out ) );
		}
	} // partsToFullDate


	/*---< Days Since Begin >-------------------------------------------------*/

	/**
	 * doDaysSinceBegin()
	 * @param debug
	 * @param go
	 * @param source
	 * @param rmtype
	 * @throws Exception
	 */
	private void doDaysSinceBegin( boolean debug, boolean go, String source, String rmtype ) throws Exception
	{
		long threadId = Thread.currentThread().getId();

		String funcname = String.format( "Thread id %02d; doDaysSinceBegin for source: %s, rmtype: %s", threadId, source, rmtype );

		if( !go ) {
			if( showskip ) { showMessage( "Skipping " + funcname, false, true ); }
			return;
		}

		long timeStart = System.currentTimeMillis();
		showMessage( funcname + " ...", false, true );

		String msg = String.format( "Thread id %02d; Processing daysSinceBegin for source: %s, rmtype: %s ...", threadId, source, rmtype );
		showMessage( msg, false, true );
		daysSinceBegin( debug, source, rmtype );

		elapsedShowMessage( funcname, timeStart, System.currentTimeMillis() );
		showMessage_nl();
	} // doDaysSinceBegin


	/**
	 * daysSinceBegin()
	 * @param debug
	 * @param source
	 * @param rmtype
	 */
	private void daysSinceBegin( boolean debug, String source, String rmtype )
	{
		long threadId = Thread.currentThread().getId();

		String qRclean  = "UPDATE links_cleaned.registration_c SET "
			+ "registration_date = NULL "
			+ "WHERE ( registration_date = '00-00-0000' OR registration_date = '0000-00-00' ) "
			+ "AND id_source = " + source;

		if ( ! rmtype.isEmpty() ) { qRclean += " AND registration_maintype = " + rmtype; }
		if( debug ) { showMessage( qRclean, false, true ); }

		if( debug ) { showMessage( qRclean, false, true ); }
		else {
			showMessage( String.format( "Thread id %02d; reg dates '00-00-0000' -> NULL", threadId ), false, true );
			showMessage( String.format( "Thread id %02d; reg dates '0000-00-00' -> NULL", threadId ), false, true );
		}

		try { dbconCleaned.executeUpdate( qRclean ); }
		catch( Exception ex ) {
			String msg = String.format( "Thread id %02d; Exception 1 in daysSinceBegin: %s", threadId, ex.getMessage() );
			showMessage( msg, false, true );
			ex.printStackTrace( new PrintStream( System.out ) );
		}

		// Notice: UPDATE IGNORE means "ignore rows that break unique constraints, instead of failing the query".

		String queryP1 = "UPDATE IGNORE person_c SET birth_min_days   = DATEDIFF( DATE_FORMAT( STR_TO_DATE( birth_date_min,   '%d-%m-%Y' ), '%Y-%m-%d' ) , '1-1-1' ) WHERE birth_date_min   IS NOT NULL AND birth_date_min   NOT LIKE '0-%' AND birth_date_min   NOT LIKE '%-0-%' AND birth_date_min   NOT LIKE '%-0' ";
		String queryP2 = "UPDATE IGNORE person_c SET birth_max_days   = DATEDIFF( DATE_FORMAT( STR_TO_DATE( birth_date_max,   '%d-%m-%Y' ), '%Y-%m-%d' ) , '1-1-1' ) WHERE birth_date_max   IS NOT NULL AND birth_date_max   NOT LIKE '0-%' AND birth_date_max   NOT LIKE '%-0-%' AND birth_date_max   NOT LIKE '%-0' ";
		String queryP3 = "UPDATE IGNORE person_c SET mar_min_days     = DATEDIFF( DATE_FORMAT( STR_TO_DATE( mar_date_min,     '%d-%m-%Y' ), '%Y-%m-%d' ) , '1-1-1' ) WHERE mar_date_min     IS NOT NULL AND mar_date_min     NOT LIKE '0-%' AND mar_date_min     NOT LIKE '%-0-%' AND mar_date_min     NOT LIKE '%-0' ";
		String queryP4 = "UPDATE IGNORE person_c SET mar_max_days     = DATEDIFF( DATE_FORMAT( STR_TO_DATE( mar_date_max,     '%d-%m-%Y' ), '%Y-%m-%d' ) , '1-1-1' ) WHERE mar_date_max     IS NOT NULL AND mar_date_max     NOT LIKE '0-%' AND mar_date_max     NOT LIKE '%-0-%' AND mar_date_max     NOT LIKE '%-0' ";
		String queryP5 = "UPDATE IGNORE person_c SET divorce_min_days = DATEDIFF( DATE_FORMAT( STR_TO_DATE( divorce_date_min, '%d-%m-%Y' ), '%Y-%m-%d' ) , '1-1-1' ) WHERE divorce_date_min IS NOT NULL AND divorce_date_min NOT LIKE '0-%' AND divorce_date_min NOT LIKE '%-0-%' AND divorce_date_min NOT LIKE '%-0' ";
		String queryP6 = "UPDATE IGNORE person_c SET divorce_max_days = DATEDIFF( DATE_FORMAT( STR_TO_DATE( divorce_date_max, '%d-%m-%Y' ), '%Y-%m-%d' ) , '1-1-1' ) WHERE divorce_date_max IS NOT NULL AND divorce_date_max NOT LIKE '0-%' AND divorce_date_max NOT LIKE '%-0-%' AND divorce_date_max NOT LIKE '%-0' ";
		String queryP7 = "UPDATE IGNORE person_c SET death_min_days   = DATEDIFF( DATE_FORMAT( STR_TO_DATE( death_date_min,   '%d-%m-%Y' ), '%Y-%m-%d' ) , '1-1-1' ) WHERE death_date_min   IS NOT NULL AND death_date_min   NOT LIKE '0-%' AND death_date_min   NOT LIKE '%-0-%' AND death_date_min   NOT LIKE '%-0' ";
		String queryP8 = "UPDATE IGNORE person_c SET death_max_days   = DATEDIFF( DATE_FORMAT( STR_TO_DATE( death_date_max,   '%d-%m-%Y' ), '%Y-%m-%d' ) , '1-1-1' ) WHERE death_date_max   IS NOT NULL AND death_date_max   NOT LIKE '0-%' AND death_date_max   NOT LIKE '%-0-%' AND death_date_max   NOT LIKE '%-0' ";

		// The min/max dates in person_c are not normalized to '%02d-%02d-%04d'; there are no leading zero's.
		// See partsToFullDate()

		queryP1 += "AND id_source = " + source;
		queryP2 += "AND id_source = " + source;
		queryP3 += "AND id_source = " + source;
		queryP4 += "AND id_source = " + source;
		queryP5 += "AND id_source = " + source;
		queryP6 += "AND id_source = " + source;
		queryP7 += "AND id_source = " + source;
		queryP8 += "AND id_source = " + source;

		if ( ! rmtype.isEmpty() ) {
			queryP1 += " AND registration_maintype = " + rmtype;
			queryP2 += " AND registration_maintype = " + rmtype;
			queryP3 += " AND registration_maintype = " + rmtype;
			queryP4 += " AND registration_maintype = " + rmtype;
			queryP5 += " AND registration_maintype = " + rmtype;
			queryP6 += " AND registration_maintype = " + rmtype;
			queryP7 += " AND registration_maintype = " + rmtype;
			queryP8 += " AND registration_maintype = " + rmtype;
		}

		// registration_date strings '01-01-0000' give a negative DATEDIFF, which gives an exception
		// because the column links_cleaned.registration_days is defined as unsigned.
		// We skip such negative results.
		// 22-Jan-2016: we now assume that the registration_date of registration_c is formatted as '%02d-%02d-%04d'
		// 15-Mar-2016: check for STR_TO_DATE result NOT NULL
        /*
        String queryR = "UPDATE registration_c SET "
            + "registration_days = DATEDIFF( DATE_FORMAT( STR_TO_DATE( registration_date, '%d-%m-%Y' ), '%Y-%m-%d' ) , '1-1-1' ) "
            + "WHERE registration_date IS NOT NULL "
            + "AND registration_date NOT LIKE '00-%' "
            + "AND registration_date NOT LIKE '0000-%' "
            + "AND registration_date NOT LIKE '%-00-%' "
            + "AND registration_date NOT LIKE '%-0000' "
            + "AND STR_TO_DATE( registration_date, '%d-%m-%Y' ) IS NOT NULL "
            + "AND STR_TO_DATE( registration_date, '%d-%m-%Y' ) <> '0000-00-00' "
            + "AND DATEDIFF( DATE_FORMAT( STR_TO_DATE( registration_date, '%d-%m-%Y' ), '%Y-%m-%d' ) , '1-1-1' ) > 0 "
            + "AND id_source = " + source;
        */

		try
		{
			if( debug ) { showMessage( queryP1, false, true ); }
			else { showMessage( String.format( "Thread id %02d; 1-of-9: birth_date_min", threadId ), false, true ); }
			dbconCleaned.executeUpdate( queryP1 );

			if( debug ) { showMessage( queryP2, false, true ); }
			else { showMessage( String.format( "Thread id %02d; 2-of-9: birth_date_max", threadId ), false, true ); }
			dbconCleaned.executeUpdate( queryP2 );

			if( debug ) { showMessage( queryP3, false, true ); }
			else { showMessage( String.format( "Thread id %02d; 3-of-9: mar_date_min", threadId ), false, true ); }
			dbconCleaned.executeUpdate( queryP3 );

			if( debug ) { showMessage( queryP4, false, true ); }
			else { showMessage( String.format( "Thread id %02d; 4-of-9: mar_date_max", threadId ), false, true ); }
			dbconCleaned.executeUpdate( queryP4 );

			if( debug ) { showMessage( queryP5, false, true ); }
			else { showMessage( String.format( "Thread id %02d; 5-of-9: divorce_date_min", threadId ), false, true ); }
			dbconCleaned.executeUpdate( queryP5 );

			if( debug ) { showMessage( queryP6, false, true ); }
			else { showMessage( String.format( "Thread id %02d; 6-of-9: divorce_date_max", threadId ), false, true ); }
			dbconCleaned.executeUpdate( queryP6 );

			if( debug ) { showMessage( queryP7, false, true ); }
			else { showMessage( String.format( "Thread id %02d; 7-of-9: death_date_min", threadId ), false, true ); }
			dbconCleaned.executeUpdate( queryP7 );

			if( debug ) { showMessage( queryP8, false, true ); }
			else { showMessage( String.format( "Thread id %02d; 8-of-9: death_date_max", threadId ), false, true ); }
			dbconCleaned.executeUpdate( queryP8 );

            /*
            // sometimes exceptions for invalid dates ...
            if( debug ) { showMessage( queryR, false, true ); }
            else { showMessage( String.format( "Thread id %02d; 7-of-7: registration_days", threadId ), false, true ); }
            int rowsAffected = dbconCleaned.executeUpdate( queryR );
            showMessage( "registration_days rows affected: " + rowsAffected, false, true );
            */
		}
		catch( Exception ex ) {
			String msg = String.format( "Thread id %02d; Exception 2 in daysSinceBegin: %s", threadId, ex.getMessage() );
			showMessage( msg, false, true );
			ex.printStackTrace( new PrintStream( System.out ) );
		}

		showMessage( String.format( "Thread id %02d; 9-of-9: registration_days", threadId ), false, true );

		String queryS = "SELECT id_registration, registration_date FROM registration_c WHERE id_source = " + source;
		if ( ! rmtype.isEmpty() ) { queryS += " AND registration_maintype = " + rmtype; }
		if( debug ) { showMessage( queryS, false, true ); }

		try( ResultSet rs_s = dbconCleaned.executeQuery( queryS ) )
		{
			int count = 0;
			int stepstate = count_step;
			while( rs_s.next() )
			{
				count++;
				if( count == stepstate ) {
					showMessage( count + "", true, true );
					stepstate += count_step;
				}

				int id_registration = rs_s.getInt( "id_registration" );
				String registration_date  = rs_s.getString( "registration_date" );
				//System.out.println( "count: " + count + ", id_registration: " + id_registration + ", registration_date: " + registration_date );

				String queryU = "UPDATE registration_c SET "
					+ "registration_days = DATEDIFF( DATE_FORMAT( STR_TO_DATE( registration_date, '%d-%m-%Y' ), '%Y-%m-%d' ) , '1-1-1' ) "
					+ "WHERE registration_date IS NOT NULL "
					+ "AND registration_date NOT LIKE '00-%' "
					+ "AND registration_date NOT LIKE '0000-%' "
					+ "AND registration_date NOT LIKE '%-00-%' "
					+ "AND registration_date NOT LIKE '%-0000' "
					+ "AND STR_TO_DATE( registration_date, '%d-%m-%Y' ) IS NOT NULL "
					+ "AND STR_TO_DATE( registration_date, '%d-%m-%Y' ) <> '0000-00-00' "
					+ "AND DATEDIFF( DATE_FORMAT( STR_TO_DATE( registration_date, '%d-%m-%Y' ), '%Y-%m-%d' ) , '1-1-1' ) > 0 "
					+ "AND id_source = " + source + " "
					+ "AND id_registration = " + id_registration;

				if ( ! rmtype.isEmpty() ) { queryU += " AND registration_maintype = " + rmtype; }

				try {
					int rowsAffected = dbconCleaned.executeUpdate( queryU );
					//System.out.println( "registration_days rows affected: " + rowsAffected );
				}
				catch( Exception ex ) {
					String msg = String.format( "Thread id %02d; Exception 3 in daysSinceBegin: %s", threadId, ex.getMessage() );
					showMessage( msg, false, true );
					System.out.println( "count: " + count + ", id_registration: " + id_registration + ", registration_date: " + registration_date );
					ex.printStackTrace( new PrintStream( System.out ) );
				}
			}
			//System.out.println( "end count: " + count );
		}
		catch( Exception ex ) {
			String msg = String.format( "Thread id %02d; Exception 4 in daysSinceBegin: %s", threadId, ex.getMessage() );
			showMessage( msg, false, true );
			ex.printStackTrace( new PrintStream( System.out ) );
		}

	} // daysSinceBegin


	/*---< Post Tasks >-------------------------------------------------------*/

	/**
	 * doPostTasks()
	 * @param debug
	 * @param go
	 * @param source
	 * @param rmtype
	 * @throws Exception
	 */
	private void doPostTasks( boolean debug, boolean go, String source, String rmtype ) throws Exception
	{
		long threadId = Thread.currentThread().getId();

		String funcname = String.format( "Thread id %02d; doPostTasks for source: %s, rmtype: %s", threadId, source, rmtype );

		if( !go ) {
			if( showskip ) { showMessage( "Skipping " + funcname, false, true ); }
			return;
		}

		long start = System.currentTimeMillis();
		showMessage( funcname + " ...", false, true );

		String msg = String.format( "Thread id %02d; Processing postTasks for source: %s, rmtype: %s ...", threadId, source, rmtype );
		showMessage( msg, false, true );
		postTasks( debug, source, rmtype );

		elapsedShowMessage( funcname, start, System.currentTimeMillis() );
		showMessage_nl();

		// The location flag functions below use the "role" variable. Therefore they must be called after
		// doRole() has been run. The calling of these functions has been (temporarily?) moved from doLocations() to
		// here in doPostTasks(). (The function bodies were left in the Locations segment.)
		start = System.currentTimeMillis();
		msg = String.format( "Thread id %02d; flagBirthLocation for source: %s, rmtype: %s ...", threadId, source, rmtype );
		showMessage( msg, false, true );
		flagBirthLocation( debug, source, rmtype );
		msg = String.format( "Thread id %02d; flagBirthLocation ", threadId );
		showTimingMessage( msg, start );

		start = System.currentTimeMillis();
		msg = String.format( "Thread id %02d; flagMarriageLocation for source: %s, rmtype: %s ...", threadId, source, rmtype );
		showMessage( msg, false, true );
		flagMarriageLocation( debug, source, rmtype );
		msg = String.format( "Thread id %02d; flagMarriageLocation ", threadId );
		showTimingMessage( msg, start );

		start = System.currentTimeMillis();
		msg = String.format( "Thread id %02d; flagDivorceLocation for source: %s, rmtype: %s ...", threadId, source, rmtype );
		showMessage( msg, false, true );
		flagDivorceLocation( debug, source, rmtype );
		msg = String.format( "Thread id %02d; flagDivorceLocation ", threadId );
		showTimingMessage( msg, start );


		start = System.currentTimeMillis();
		msg = String.format( "Thread id %02d; flagDeathLocation for source: %s, rmtype: %s ...", threadId, source, rmtype );
		showMessage( msg, false, true );
		flagDeathLocation( debug, source, rmtype );
		msg = String.format( "Thread id %02d; flagDeathLocation ", threadId );
		showTimingMessage( msg, start );
		showMessage_nl();

	} // doPostTasks


	/**
	 * postTasks()
	 * @param debug
	 * @param source
	 * @param rmtype
	 * @throws Exception
	 */
	private void postTasks( boolean debug, String source, String rmtype )
		throws Exception
	{
		long threadId = Thread.currentThread().getId();

		if( debug ) { showMessage( "postTasks()", false, true ); }

		String table_male   = "links_temp.male_"   + Long.toString( threadId );
		String table_female = "links_temp.female_" + Long.toString( threadId );

		String[] queries1 =
			{
				"DROP TABLE IF EXISTS " + table_male   + ";",
				"DROP TABLE IF EXISTS " + table_female + ";",

				"CREATE TABLE " + table_male   + " ( id_registration INT NOT NULL , PRIMARY KEY (id_registration) ) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin;",
				"CREATE TABLE " + table_female + " ( id_registration INT NOT NULL , PRIMARY KEY (id_registration) ) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin;"
			};

		int n = 0;
		for( String query : queries1 )
		{
			n++;
			String msg = String.format( "Thread id %02d; query %d-of-%d", threadId, n, queries1.length );
			showMessage( msg, false, true );
			dbconCleaned.executeUpdate( query );
		}


		String[] queries2 =
			{
				"UPDATE links_cleaned.person_c SET sex = 'f' WHERE role = 2 AND id_source = " + source,
				"UPDATE links_cleaned.person_c SET sex = 'm' WHERE role = 3 AND id_source = " + source,
				"UPDATE links_cleaned.person_c SET sex = 'f' WHERE role = 4 AND id_source = " + source,
				"UPDATE links_cleaned.person_c SET sex = 'f' WHERE role = 5 AND id_source = " + source,
				"UPDATE links_cleaned.person_c SET sex = 'm' WHERE role = 6 AND id_source = " + source,
				"UPDATE links_cleaned.person_c SET sex = 'm' WHERE role = 7 AND id_source = " + source,
				"UPDATE links_cleaned.person_c SET sex = 'f' WHERE role = 8 AND id_source = " + source,
				"UPDATE links_cleaned.person_c SET sex = 'm' WHERE role = 9 AND id_source = " + source,

				"UPDATE links_cleaned.person_c SET sex = 'u' WHERE (sex IS NULL OR (sex <> 'm' AND sex <> 'f')) AND id_source = " + source,

				"INSERT INTO " + table_male   + " (id_registration) SELECT id_registration FROM links_cleaned.person_c "
					+ "WHERE role = 10 AND sex = 'm' AND id_source = " + source,

				"INSERT INTO " + table_female + " (id_registration) SELECT id_registration FROM links_cleaned.person_c "
					+ "WHERE role = 10 AND sex = 'f' AND id_source = " + source,

				"UPDATE links_cleaned.person_c, " + table_male   + " SET sex = 'f' "
					+ "WHERE " + table_male   + ".id_registration = links_cleaned.person_c.id_registration AND role = 11 "
					+ "AND id_source = " + source,

				"UPDATE links_cleaned.person_c, " + table_female + " SET sex = 'm' "
					+ "WHERE " + table_female + ".id_registration = links_cleaned.person_c.id_registration AND role = 11 "
					+ "AND id_source = " + source
			};

		n = 0;
		for( String query : queries2 )
		{
			n++;

			if ( ! rmtype.isEmpty() ) { query += " AND registration_maintype = " + rmtype; }
			if( debug ) { System.out.println( query ); }

			String msg = String.format( "Thread id %02d; query %d-of-%d", threadId, n, queries2.length );
			showMessage( msg, false, true );
			dbconCleaned.executeUpdate( query );
		}


		String[] queries3 =
			{
				"DROP TABLE IF EXISTS " + table_male + ";",
				"DROP TABLE IF EXISTS " + table_female + ";"
			};

		n = 0;
		for( String query : queries3 )
		{
			n++;
			String msg = String.format( "Thread id %02d; query %d-of-%d", threadId, n, queries3.length );
			showMessage( msg, false, true );
			dbconCleaned.executeUpdate( query );
		}


		// stillbirth with missing birth_date -> registration_date
		String query = "UPDATE links_cleaned.registration_c, links_cleaned.person_c "
			+ "SET "
			+ "person_c.birth_date = registration_c.registration_date, "
			+ "person_c.birth_year = registration_c.registration_year, "
			+ "person_c.birth_month = registration_c.registration_month, "
			+ "person_c.birth_day = registration_c.registration_day, "
			+ "person_c.birth_date_flag = 2 "
			+ "WHERE registration_c.id_registration = person_c.id_registration "
			+ "AND registration_date IS NOT NULL "
			+ "AND person_c.stillbirth IS NOT NULL AND person_c.stillbirth <> '' "
			+ "AND person_c.birth_date IS NULL "
			+ "AND person_c.role = 10 "
			+ "AND person_c.id_source = " + source;

		if ( ! rmtype.isEmpty() ) { query += " AND person_c.registration_maintype = " + rmtype; }
		if( debug ) { System.out.println( query ); }

		String msg = String.format( "Thread id %02d; running stillbirth birth_date query ...", threadId  );
		showMessage( msg, false, true );
		int count = dbconCleaned.executeUpdate( query );
		msg = String.format( "Thread id %02d; stillbirth: %d rows updated", threadId, count  );
		showMessage( msg, false, true );

	} // postTasks


	/**
	 * flagBirthLocation()
	 * @param debug
	 * @param source
	 * @param rmtype
	 */
	public void flagBirthLocation( boolean debug, String source, String rmtype )
	{
		long threadId = Thread.currentThread().getId();

		String[] queries =
			{
				"UPDATE person_c, registration_c"
					+ " SET"
					+ " person_c.birth_location_flag = 1"
					+ " WHERE person_c.id_source = " + source
					+ " AND person_c.registration_maintype = 1"
					+ " AND person_c.role = 1"
					+ " AND person_c.birth_location IS NOT NULL"
					+ " AND person_c.id_registration = registration_c.id_registration",

				"UPDATE person_c, registration_c"
					+ " SET"
					+ " person_c.birth_location_flag = 2,"
					+ " person_c.birth_location = registration_c.registration_location_no"
					+ " WHERE person_c.id_source = " + source
					+ " AND person_c.registration_maintype = 1"
					+ " AND person_c.role = 1"
					+ " AND person_c.birth_location IS NULL"
					+ " AND person_c.id_registration = registration_c.id_registration"
			};

		int n = 0;
		for( String query : queries )
		{
			try
			{
				if ( ! rmtype.isEmpty() ) { query += " AND person_c.registration_maintype = " + rmtype; }
				if( debug ) { showMessage( query, false, true ); }

				int rowsAffected = dbconCleaned.executeUpdate( query );
				String msg = "";
				if( n == 0 )
				{ msg = String.format( "Thread id %02d; flag = 1 # of rows affected: %d", threadId, rowsAffected ); }
				else
				{ msg = String.format( "Thread id %02d; flag = 2 # of rows affected: %d", threadId, rowsAffected ); }

				showMessage( msg, false, true );
				n++;
			}
			catch( Exception ex ) {
				String msg = String.format( "Thread id %02d; Exception in flagBirthLocation: %s", threadId, ex.getMessage() );
				showMessage( msg, false, true );
				showMessage( query, false, true );
				ex.printStackTrace( new PrintStream( System.out ) );
			}
		}
	} // flagBirthLocation


	/**
	 * flagMarriageLocation()
	 * @param debug
	 * @param source
	 * @param rmtype
	 */
	public void flagMarriageLocation( boolean debug, String source, String rmtype )
	{
		long threadId = Thread.currentThread().getId();

		String[] queries =
			{
				"UPDATE person_c, registration_c"
					+ " SET"
					+ " person_c.mar_location_flag = 1"
					+ " WHERE person_c.id_source = " + source
					+ " AND person_c.registration_maintype = 2"
					+ " AND ( ( person_c.role = 4 ) || ( person_c.role = 7 ) )"
					+ " AND person_c.mar_location IS NOT NULL"
					+ " AND person_c.id_registration = registration_c.id_registration",

				"UPDATE person_c, registration_c"
					+ " SET"
					+ " person_c.mar_location_flag = 2,"
					+ " person_c.mar_location = registration_c.registration_location_no"
					+ " WHERE person_c.id_source = " + source
					+ " AND person_c.registration_maintype = 2"
					+ " AND ( ( person_c.role = 4 ) || ( person_c.role = 7 ) )"
					+ " AND person_c.mar_location IS NULL"
					+ " AND person_c.id_registration = registration_c.id_registration"
			};

		int n = 0;
		for( String query : queries )
		{
			try
			{
				if ( ! rmtype.isEmpty() ) { query += " AND person_c.registration_maintype = " + rmtype; }
				if( debug ) { showMessage( query, false, true ); }

				int rowsAffected = dbconCleaned.executeUpdate( query );
				String msg = "";
				if( n == 0 )
				{ msg = String.format( "Thread id %02d; flag = 1 # of rows affected: %d", threadId, rowsAffected ); }
				else
				{ msg = String.format( "Thread id %02d; flag = 2 # of rows affected: %d", threadId, rowsAffected ); }

				showMessage( msg, false, true );
				n++;
			}
			catch( Exception ex ) {
				String msg = String.format( "Thread id %02d; Exception in flagMarriageLocation: %s", threadId, ex.getMessage() );
				showMessage( msg, false, true );
				showMessage( query, false, true );
				ex.printStackTrace( new PrintStream( System.out ) );
			}
		}
	} // flagMarriageLocation


	/**
	 * flagDivorceLocation()
	 * @param debug
	 * @param source
	 * @param rmtype
	 *
	 * flagDivoreLocation: same roles as marriage
	 */
	public void flagDivorceLocation( boolean debug, String source, String rmtype )
	{
		long threadId = Thread.currentThread().getId();

		String[] queries =
			{
				"UPDATE person_c, registration_c"
					+ " SET"
					+ " person_c.divorce_location_flag = 1"
					+ " WHERE person_c.id_source = " + source
					+ " AND person_c.registration_maintype = 4"
					+ " AND ( ( person_c.role = 4 ) || ( person_c.role = 7 ) )"
					+ " AND person_c.divorce_location IS NOT NULL"
					+ " AND person_c.id_registration = registration_c.id_registration",

				"UPDATE person_c, registration_c"
					+ " SET"
					+ " person_c.divorce_location_flag = 2,"
					+ " person_c.divorce_location = registration_c.registration_location_no"
					+ " WHERE person_c.id_source = " + source
					+ " AND person_c.registration_maintype = 4"
					+ " AND ( ( person_c.role = 4 ) || ( person_c.role = 7 ) )"
					+ " AND person_c.divorce_location IS NULL"
					+ " AND person_c.id_registration = registration_c.id_registration"
			};

		int n = 0;
		for( String query : queries )
		{
			try
			{
				if ( ! rmtype.isEmpty() ) { query += " AND person_c.registration_maintype = " + rmtype; }
				if( debug ) { showMessage( query, false, true ); }

				int rowsAffected = dbconCleaned.executeUpdate( query );
				String msg = "";
				if( n == 0 )
				{ msg = String.format( "Thread id %02d; flag = 1 # of rows affected: %d", threadId, rowsAffected ); }
				else
				{ msg = String.format( "Thread id %02d; flag = 2 # of rows affected: %d", threadId, rowsAffected ); }

				showMessage( msg, false, true );
				n++;
			}
			catch( Exception ex ) {
				String msg = String.format( "Thread id %02d; Exception in flagDivorceLocation: %s", threadId, ex.getMessage() );
				showMessage( msg, false, true );
				showMessage( query, false, true );
				ex.printStackTrace( new PrintStream( System.out ) );
			}
		}
	} // flagDivorceLocation


	/**
	 * flagDeathLocation()
	 * @param debug
	 * @param source
	 * @param rmtype
	 */
	public void flagDeathLocation( boolean debug, String source, String rmtype )
	{
		long threadId = Thread.currentThread().getId();

		String[] queries =
			{
				"UPDATE person_c, registration_c"
					+ " SET"
					+ " person_c.death_location_flag = 1"
					+ " WHERE person_c.id_source = " + source
					+ " AND person_c.registration_maintype = 3"
					+ " AND person_c.role = 10"
					+ " AND person_c.death_location IS NOT NULL"
					+ " AND person_c.id_registration = registration_c.id_registration",

				"UPDATE person_c, registration_c "
					+ " SET "
					+ " person_c.death_location_flag = 2,"
					+ " person_c.death_location = registration_c.registration_location_no"
					+ " WHERE person_c.id_source = " + source
					+ " AND person_c.registration_maintype = 3"
					+ " AND person_c.role = 10"
					+ " AND person_c.death_location IS NULL"
					+ " AND person_c.id_registration = registration_c.id_registration"
			};

		int n = 0;
		for( String query : queries )
		{
			try
			{
				if ( ! rmtype.isEmpty() ) { query += " AND person_c.registration_maintype = " + rmtype; }
				if( debug ) { showMessage( query, false, true ); }

				int rowsAffected = dbconCleaned.executeUpdate( query );
				String msg = "";
				if( n == 0 )
				{ msg = String.format( "Thread id %02d; flag = 1 # of rows affected: %d", threadId, rowsAffected ); }
				else
				{ msg = String.format( "Thread id %02d; flag = 2 # of rows affected: %d", threadId, rowsAffected ); }

				showMessage( msg, false, true );
				n++;
			}
			catch( Exception ex ) {
				String msg = String.format( "Thread id %02d; Exception in flagDeathLocation: %s", threadId, ex.getMessage() );
				showMessage( msg, false, true );
				showMessage( query, false, true );
				ex.printStackTrace( new PrintStream( System.out ) );
			}
		}
	} // flagDeathLocation


	/*---< Flag Bad Registration Recs >-------------------------------------*/

	/**
	 * doFlagRegistrations()
	 * @param debug
	 * @param go
	 * @param source
	 * @param rmtype
	 * @throws Exception
	 */
	private void doFlagRegistrations( boolean debug, boolean go, String source, String rmtype )
		throws Exception
	{
		long threadId = Thread.currentThread().getId();

		String funcname = String.format( "Thread id %02d; doFlagRegistrations for source: %s, rmtype: %s", threadId, source, rmtype );

		if( !go ) {
			if( showskip ) { showMessage( "Skipping " + funcname, false, true ); }
			return;
		}

		long timeStart = System.currentTimeMillis();

		String msg = String.format( "Thread id %02d; Clear Previous Registration Flags for source: %s, rmtype: %s ...", threadId, source, rmtype );
		showMessage( msg, false, true );
		clearFlagsRegsLinksbase( debug, source, rmtype );

		msg = String.format( "Thread id %02d; Flagging Duplicate Registrations (components) for source: %s, rmtype: %s ...", threadId, source, rmtype );
		showMessage( msg, false, true );
		flagDuplicateRegsComps( debug, source, rmtype );

		msg = String.format( "Thread id %02d; Flagging Duplicate Registrations (_id_persist) for source: %s, rmtype: %s ...", threadId, source, rmtype );
		showMessage( msg, false, true );
		flagDuplicateRegsPersist( debug, source, rmtype );

		msg = String.format( "Thread id %02d; Flagging Empty Date Registrations for source: %s, rmtype: %s ...", threadId, source, rmtype );
		showMessage( msg, false, true );
		flagEmptyDateRegs( debug, source, rmtype );

		msg = String.format( "Thread id %02d; Flagging Empty Days since begin Regs for source: %s, rmtype: %s ...", threadId, source, rmtype );
		showMessage( msg, false, true );
		flagEmptyDaysSinceBegin( debug, source, rmtype );

		elapsedShowMessage( funcname, timeStart, System.currentTimeMillis() );
		showMessage_nl();
	} // doFlagRegistrations


	/**
	 * clearFlagsRegsLinksbase()
	 * @param debug
	 * @param source
	 * @param rmtype
	 * @throws Exception
	 */
	private void clearFlagsRegsLinksbase( boolean debug, String source, String rmtype )
		throws Exception
	{
		long threadId = Thread.currentThread().getId();

		// Clear previous flag values for given source
		String clearQuery_r = "UPDATE registration_c SET not_linksbase = NULL WHERE id_source = " + source;

		if ( ! rmtype.isEmpty() ) { clearQuery_r += " AND registration_maintype = " + rmtype; }
		if( debug ) { System.out.println( clearQuery_r ); }

		//String clearQuery_r = "UPDATE registration_c SET not_linksbase = '0001' WHERE id_source = " + source + ";";   // test updates
		int nrec = dbconCleaned.executeUpdate( clearQuery_r );

		String msg = String.format( "Thread id %02d; Number of flags cleared: %d", threadId, nrec );
		showMessage( msg, false, true );
	} // clearFlagsRegsLinksbase


	/**
	 * flagDuplicateRegsComps()
	 * @param debug
	 * @param source
	 * @param rmtype
	 * @throws Exception
	 */
	private void flagDuplicateRegsComps( boolean debug, String source, String rmtype )
		throws Exception
	{
		long threadId = Thread.currentThread().getId();

		showMessage( String.format( "Thread id %02d; flagDuplicateRegsComps for source %s", threadId, source ), false, true );
		showMessage( String.format( "Thread id %02d; Notice: the familyname prefix is not used for comparisons", threadId ), false , true );

		int min_cnt = 2;    // in practice we see double, triples and quadruples

		// The GROUP_CONCAT on id_registration is needed to get the different registration ids corresponding to the count.
		// And we require that the 4 grouping variables have normal values.
		String query_r = "SELECT GROUP_CONCAT(id_registration), registration_maintype, registration_location_no, registration_date, registration_seq, COUNT(*) AS cnt"
			+ " FROM registration_c"
			+ " WHERE id_source = " + source
			+ " AND registration_maintype IS NOT NULL AND registration_maintype <> 0"
			+ " AND registration_location_no IS NOT NULL AND registration_location_no <> 0"
			+ " AND registration_date IS NOT NULL AND registration_date <> ''"
			+ " AND registration_seq IS NOT NULL AND registration_seq <> ''";

		if ( ! rmtype.isEmpty() ) { query_r += " AND registration_maintype = " + rmtype; }
		if( debug ) { showMessage( query_r, false, true ); }

		query_r += " GROUP BY registration_maintype, registration_location_no, registration_date, registration_seq";
		query_r += " HAVING cnt >= " + min_cnt;
		query_r += " ORDER BY cnt DESC;";

		int nDuplicates = 0;

		try( ResultSet rs_r = dbconCleaned.executeQuery( query_r ) )
		{
			int nflagRegist = 0;
			//int ndeletePerson = 0;

			int row = 0;
			while( rs_r.next() )        // process all groups
			{
				row++;

				String registrationIds_str = rs_r.getString( "GROUP_CONCAT(id_registration)" );
				String registration_date   = rs_r.getString( "registration_date" );
				String registration_seq    = rs_r.getString( "registration_seq" );

				int registration_maintype    = rs_r.getInt( "registration_maintype" );
				int registration_location_no = rs_r.getInt( "registration_location_no" );

				if( debug ) {
					String msg = String.format( "reg_maintype: %d, reg_location_no: %d, registration_date: %s, reg_loc_no: %s",
						registration_maintype, registration_location_no, registration_date, registration_location_no );
					System.out.println( msg );
				}

				String registrationIdsStrs[] = registrationIds_str.split( "," );
				Vector< Integer > registrationIds = new Vector< Integer >();
				for( String registrationId : registrationIdsStrs ) {
					registrationIds.add( Integer.parseInt( registrationId ) );
				}

				if( debug ) { showMessage( registrationIds.toString(), false, true ); }
				Collections.sort( registrationIds );

				if( debug ) {
					showMessage( registrationIds.toString(), false, true );
					showMessage( "Id group of " + registrationIds.size() + ": " + registrationIds.toString(), false, true );
				}

				if( registrationIds.size() > 2 )   // useless registrations, flag them all
				{
					for( int id_regist : registrationIds )
					{
						// get current flags string
						String getFlagQuery_r = "SELECT not_linksbase FROM registration_c WHERE id_registration = " + id_regist;

						try( ResultSet rs = dbconCleaned.executeQuery( getFlagQuery_r ) )
						{
							String old_flags = "";
							while( rs.next() )
							{ old_flags = rs.getString( "not_linksbase" ); }

							int countRegist = 0;
							String new_flags = "";

							if( old_flags == null || old_flags.isEmpty() ) { new_flags = "00100"; }
							else
							{
								// is the flag already set?
								int flag_idx = 2;       // 3rd position for the flag
								if( ! old_flags.substring( flag_idx, flag_idx + 1 ).equals( "1" ) )
								{
									// preserve other flags, and set new flag
									StringBuilder sb = new StringBuilder( old_flags );
									sb.setCharAt( flag_idx,'1' );
									new_flags = sb.toString();
								}
							}

							if( ! new_flags.isEmpty() )
							{
								String flagQuery_r = "UPDATE registration_c SET not_linksbase = '" + new_flags + "'";
								flagQuery_r += " WHERE id_registration = " + id_regist + ";";
								//System.out.println(flagQuery_r);

								countRegist = dbconCleaned.executeUpdate( flagQuery_r );
							}
						}
					}
				}
				else    // test pairs
				{
					int rid1 = 0;
					int rid2 = 1;
					boolean isDuplicate = compare2Registrations( debug, rid1, rid2, registrationIds, registration_maintype );
					if( isDuplicate ) { nDuplicates++; }
				}

				for( int rid1 = 0; rid1 < registrationIdsStrs.length; rid1++ )
				{
					for( int rid2 = rid1 + 1; rid2 < registrationIdsStrs.length; rid2++ )
					{
						boolean isDuplicate = compare2Registrations( debug, rid1, rid2, registrationIds, registration_maintype );
						if( isDuplicate ) { nDuplicates++; }
					}
				}

				// free
				registrationIds.clear();
				registrationIds = null;
			}

			String msg = String.format( "Thread id %02d; Number of duplicate regs flagged from duplicate tuples: %d", threadId, nflagRegist );
			showMessage( msg, false, true );

			msg = String.format( "Thread id %02d; Number of duplicate regs flagged from duplicate pairs: %d", threadId, nDuplicates );
			showMessage( msg, false, true );
		}
		catch( Exception ex ) {
			String msg = String.format( "Thread id %02d; Exception in flagDuplicateRegsComps: %s", threadId, ex.getMessage() );
			showMessage( msg, false, true );
			ex.printStackTrace( new PrintStream( System.out ) );
		}
	} // flagDuplicateRegsComps


	/**
	 * compare2Registrations()
	 * @param debug
	 * @param rid1
	 * @param rid2
	 * @param registrationIds
	 * @param registration_maintype
	 * @return
	 * @throws Exception
	 */
	private boolean compare2Registrations( boolean debug, int rid1, int rid2, Vector< Integer > registrationIds, int registration_maintype )
		throws Exception
	{
		if( debug ) {
			System.out.println( String.format( "rid1: %d, rid2: %d", rid1, rid2 ) );
			System.out.println( String.format( "registrationIds.size(): %d", registrationIds.size() ) );
			for( int rid : registrationIds ) { System.out.println( rid ); }
		}

		int id_registration1 = registrationIds.get( rid1 );
		int id_registration2 = registrationIds.get( rid2 );

		if( debug ) { showMessage( "Comparing, " + rid1 + ": " + id_registration1 + ", " + rid2 + ": " + id_registration2, false, true ); }

		String id_source1 = "";
		String id_source2 = "";

		if( registration_maintype == 1 )
		{
			String query_p1 = "SELECT id_source, role, firstname, prefix, familyname FROM person_c WHERE id_registration = " + id_registration1 + " AND role = 1";
			if( debug ) { System.out.println( query_p1 ); }

			String newborn_id_source1  = "";
			String newborn_firstname1  = "";
			String newborn_prefix1     = "";
			String newborn_familyname1 = "";

			try( ResultSet rs_p1 = dbconCleaned.executeQuery( query_p1 ) )
			{
				while( rs_p1.next() )
				{
					int role = rs_p1.getInt( "role" );

					newborn_id_source1  = rs_p1.getString( "id_source" );
					newborn_firstname1  = rs_p1.getString( "firstname" );
					newborn_prefix1     = rs_p1.getString( "prefix" );
					newborn_familyname1 = rs_p1.getString( "familyname" );

					if( newborn_id_source1  == null ) { newborn_id_source1  = ""; }
					if( newborn_firstname1  == null ) { newborn_firstname1  = ""; }
					if( newborn_prefix1     == null ) { newborn_prefix1     = ""; }
					if( newborn_familyname1 == null ) { newborn_familyname1 = ""; }

					id_source1 = newborn_id_source1;

					//if( debug ) { System.out.printf( "role: %d, familyname: %s, prefix: %s, firstname: %s\n", role, familyname, prefix, firstname ); }
				}
			}

			String query_p2 = "SELECT id_source, role, firstname, prefix, familyname FROM person_c WHERE id_registration = " + id_registration2 + " AND role = 1";
			if( debug ) { System.out.println( query_p2 ); }

			String newborn_id_source2  = "";
			String newborn_firstname2  = "";
			String newborn_prefix2     = "";
			String newborn_familyname2 = "";

			try( ResultSet rs_p2 = dbconCleaned.executeQuery( query_p2 ) )
			{
				while( rs_p2.next() )
				{
					int role = rs_p2.getInt( "role" );

					newborn_id_source2  = rs_p2.getString( "id_source" );
					newborn_firstname2  = rs_p2.getString( "firstname" );
					newborn_prefix2     = rs_p2.getString( "prefix" );
					newborn_familyname2 = rs_p2.getString( "familyname" );

					if( newborn_id_source2  == null ) { newborn_id_source2  = ""; }
					if( newborn_firstname2  == null ) { newborn_firstname2  = ""; }
					if( newborn_prefix2     == null ) { newborn_prefix2     = ""; }
					if( newborn_familyname2 == null ) { newborn_familyname2 = ""; }

					id_source2 = newborn_id_source2;
				}
			}

			if( newborn_firstname1.equals( newborn_firstname2 ) && newborn_familyname1.equals( newborn_familyname2 ) )
			{
				if( debug ) {
					showMessage_nl();
					String msg = String.format( "Duplicate registrations, ids, %d: %d, %d: %d (registration_maintype: %d)",
						rid1, id_registration1, rid2, id_registration2, registration_maintype );
					showMessage( msg, false, true );

					showMessage( "newborn_familyname1: " + newborn_familyname1 + ", newborn_prefix1: " + newborn_prefix1 + ", newborn_firstname1: " + newborn_firstname1, false, true );
					showMessage( "newborn_familyname2: " + newborn_familyname2 + ", newborn_prefix2: " + newborn_prefix2 + ", newborn_firstname2: " + newborn_firstname2, false, true );
				}

				int delcnt = flagDuplicateReg( debug, registrationIds, id_source1, id_source2, id_registration1, id_registration2, registration_maintype );
				if( delcnt > 0 ) { return true; }
			}
		}


		else if( registration_maintype == 2 )   // marriage
		{
			String query_p1 = "SELECT id_source, role, firstname, prefix, familyname FROM person_c WHERE id_registration = " + id_registration1 + " AND (role = 4 OR role = 7)";
			if( debug ) { System.out.println( query_p1 ); }

			String bride_id_source1  = "";
			String bride_firstname1  = "";
			String bride_prefix1     = "";
			String bride_familyname1 = "";

			String groom_id_source1  = "";
			String groom_firstname1  = "";
			String groom_prefix1     = "";
			String groom_familyname1 = "";

			try( ResultSet rs_p1 = dbconCleaned.executeQuery( query_p1 ) )
			{
				while( rs_p1.next() )
				{
					int role = rs_p1.getInt( "role" );

					if( role == 4 )
					{
						bride_id_source1  = rs_p1.getString( "id_source" );
						bride_firstname1  = rs_p1.getString( "firstname" );
						bride_prefix1     = rs_p1.getString( "prefix" );
						bride_familyname1 = rs_p1.getString( "familyname" );

						if( bride_id_source1  == null ) { bride_id_source1  = ""; }
						if( bride_firstname1  == null ) { bride_firstname1  = ""; }
						if( bride_prefix1     == null ) { bride_prefix1     = ""; }
						if( bride_familyname1 == null ) { bride_familyname1 = ""; }

						id_source1 = bride_id_source1;
					}
					else    // role == 7
					{
						groom_id_source1  = rs_p1.getString( "id_source" );
						groom_firstname1  = rs_p1.getString( "firstname" );
						groom_prefix1     = rs_p1.getString( "prefix" );
						groom_familyname1 = rs_p1.getString( "familyname" );

						if( groom_id_source1  == null ) { groom_id_source1  = ""; }
						if( groom_firstname1  == null ) { groom_firstname1  = ""; }
						if( groom_prefix1     == null ) { groom_prefix1     = ""; }
						if( groom_familyname1 == null ) { groom_familyname1 = ""; }

						id_source1 = groom_id_source1;
					}
				}
			}

			String query_p2 = "SELECT id_source, role, firstname, prefix, familyname FROM person_c WHERE id_registration = " + id_registration2 + " AND (role = 4 OR role = 7)";
			if( debug ) { System.out.println( query_p2 ); }

			String bride_id_source2  = "";
			String bride_firstname2  = "";
			String bride_prefix2     = "";
			String bride_familyname2 = "";

			String groom_id_source2  = "";
			String groom_firstname2  = "";
			String groom_prefix2     = "";
			String groom_familyname2 = "";

			try( ResultSet rs_p2 = dbconCleaned.executeQuery( query_p2 ) )
			{
				while( rs_p2.next() )
				{
					int role = rs_p2.getInt( "role" );

					if( role == 4 )
					{
						bride_id_source2  = rs_p2.getString( "id_source" );
						bride_firstname2  = rs_p2.getString( "firstname" );
						bride_prefix2     = rs_p2.getString( "prefix" );
						bride_familyname2 = rs_p2.getString( "familyname" );

						if( bride_id_source2  == null ) { bride_id_source2  = ""; }
						if( bride_firstname2  == null ) { bride_firstname2  = ""; }
						if( bride_prefix2     == null ) { bride_prefix2     = ""; }
						if( bride_familyname2 == null ) { bride_familyname2 = ""; }

						id_source2 = bride_id_source2;
					}
					else    // role == 7
					{
						groom_id_source2  = rs_p2.getString( "id_source" );
						groom_firstname2  = rs_p2.getString( "firstname" );
						groom_prefix2     = rs_p2.getString( "prefix" );
						groom_familyname2 = rs_p2.getString( "familyname" );

						if( groom_id_source2  == null ) { groom_id_source2  = ""; }
						if( groom_firstname2  == null ) { groom_firstname2  = ""; }
						if( groom_prefix2     == null ) { groom_prefix2     = ""; }
						if( groom_familyname2 == null ) { groom_familyname2 = ""; }

						id_source2 = groom_id_source2;
					}
				}
			}

			if( bride_firstname1.equals( bride_firstname2 ) && bride_familyname1.equals( bride_familyname2 ) &&
				groom_firstname1.equals( groom_firstname2 ) && groom_familyname1.equals( groom_familyname2 ) )
			{
				if( debug ) {
					showMessage_nl();
					String msg = String.format( "Duplicate registrations, ids, %d: %d, %d: %d (registration_maintype: %d)",
						rid1, id_registration1, rid2, id_registration2, registration_maintype );
					showMessage( msg, false, true );

					showMessage( "bride_familyname1: " + bride_familyname1 + ", bride_prefix1: " + bride_prefix1 + ", bride_firstname1: " + bride_firstname1, false, true );
					showMessage( "bride_familyname2: " + bride_familyname2 + ", bride_prefix2: " + bride_prefix2 + ", bride_firstname2: " + bride_firstname2, false, true );

					showMessage( "groom_familyname1: " + groom_familyname1 + ", groom_prefix1: " + groom_prefix1 + ", groom_firstname1: " + groom_firstname1, false, true );
					showMessage( "groom_familyname2: " + groom_familyname2 + ", groom_prefix2: " + groom_prefix2 + ", groom_firstname2: " + groom_firstname2, false, true );
				}

				int delcnt = flagDuplicateReg( debug, registrationIds, id_source1, id_source2, id_registration1, id_registration2, registration_maintype );
				if( delcnt > 0 ) { return true; }
			}
		}


		else if( registration_maintype == 4 )   // divorce
		{
			String query_p1 = "SELECT id_source, role, firstname, prefix, familyname FROM person_c WHERE id_registration = " + id_registration1 + " AND (role = 4 OR role = 7)";
			if( debug ) { System.out.println( query_p1 ); }

			String bride_id_source1  = "";
			String bride_firstname1  = "";
			String bride_prefix1     = "";
			String bride_familyname1 = "";

			String groom_id_source1  = "";
			String groom_firstname1  = "";
			String groom_prefix1     = "";
			String groom_familyname1 = "";

			try( ResultSet rs_p1 = dbconCleaned.executeQuery( query_p1 ) )
			{
				while( rs_p1.next() )
				{
					int role = rs_p1.getInt( "role" );

					if( role == 4 )
					{
						bride_id_source1  = rs_p1.getString( "id_source" );
						bride_firstname1  = rs_p1.getString( "firstname" );
						bride_prefix1     = rs_p1.getString( "prefix" );
						bride_familyname1 = rs_p1.getString( "familyname" );

						if( bride_id_source1  == null ) { bride_id_source1  = ""; }
						if( bride_firstname1  == null ) { bride_firstname1  = ""; }
						if( bride_prefix1     == null ) { bride_prefix1     = ""; }
						if( bride_familyname1 == null ) { bride_familyname1 = ""; }

						id_source1 = bride_id_source1;
					}
					else    // role == 7
					{
						groom_id_source1  = rs_p1.getString( "id_source" );
						groom_firstname1  = rs_p1.getString( "firstname" );
						groom_prefix1     = rs_p1.getString( "prefix" );
						groom_familyname1 = rs_p1.getString( "familyname" );

						if( groom_id_source1  == null ) { groom_id_source1  = ""; }
						if( groom_firstname1  == null ) { groom_firstname1  = ""; }
						if( groom_prefix1     == null ) { groom_prefix1     = ""; }
						if( groom_familyname1 == null ) { groom_familyname1 = ""; }

						id_source1 = groom_id_source1;
					}
				}
			}

			String query_p2 = "SELECT id_source, role, firstname, prefix, familyname FROM person_c WHERE id_registration = " + id_registration2 + " AND (role = 4 OR role = 7)";
			if( debug ) { System.out.println( query_p2 ); }

			String bride_id_source2  = "";
			String bride_firstname2  = "";
			String bride_prefix2     = "";
			String bride_familyname2 = "";

			String groom_id_source2  = "";
			String groom_firstname2  = "";
			String groom_prefix2     = "";
			String groom_familyname2 = "";

			try( ResultSet rs_p2 = dbconCleaned.executeQuery( query_p2 ) )
			{
				while( rs_p2.next() )
				{
					int role = rs_p2.getInt( "role" );

					if( role == 4 )
					{
						bride_id_source2  = rs_p2.getString( "id_source" );
						bride_firstname2  = rs_p2.getString( "firstname" );
						bride_prefix2     = rs_p2.getString( "prefix" );
						bride_familyname2 = rs_p2.getString( "familyname" );

						if( bride_id_source2  == null ) { bride_id_source2  = ""; }
						if( bride_firstname2  == null ) { bride_firstname2  = ""; }
						if( bride_prefix2     == null ) { bride_prefix2     = ""; }
						if( bride_familyname2 == null ) { bride_familyname2 = ""; }

						id_source2 = bride_id_source2;
					}
					else    // role == 7
					{
						groom_id_source2  = rs_p2.getString( "id_source" );
						groom_firstname2  = rs_p2.getString( "firstname" );
						groom_prefix2     = rs_p2.getString( "prefix" );
						groom_familyname2 = rs_p2.getString( "familyname" );

						if( groom_id_source2  == null ) { groom_id_source2  = ""; }
						if( groom_firstname2  == null ) { groom_firstname2  = ""; }
						if( groom_prefix2     == null ) { groom_prefix2     = ""; }
						if( groom_familyname2 == null ) { groom_familyname2 = ""; }

						id_source2 = groom_id_source2;
					}
				}
			}

			if( bride_firstname1.equals( bride_firstname2 ) && bride_familyname1.equals( bride_familyname2 ) &&
				groom_firstname1.equals( groom_firstname2 ) && groom_familyname1.equals( groom_familyname2 ) )
			{
				if( debug ) {
					showMessage_nl();
					String msg = String.format( "Duplicate registrations, ids, %d: %d, %d: %d (registration_maintype: %d)",
						rid1, id_registration1, rid2, id_registration2, registration_maintype );
					showMessage( msg, false, true );

					showMessage( "bride_familyname1: " + bride_familyname1 + ", bride_prefix1: " + bride_prefix1 + ", bride_firstname1: " + bride_firstname1, false, true );
					showMessage( "bride_familyname2: " + bride_familyname2 + ", bride_prefix2: " + bride_prefix2 + ", bride_firstname2: " + bride_firstname2, false, true );

					showMessage( "groom_familyname1: " + groom_familyname1 + ", groom_prefix1: " + groom_prefix1 + ", groom_firstname1: " + groom_firstname1, false, true );
					showMessage( "groom_familyname2: " + groom_familyname2 + ", groom_prefix2: " + groom_prefix2 + ", groom_firstname2: " + groom_firstname2, false, true );
				}

				int delcnt = flagDuplicateReg( debug, registrationIds, id_source1, id_source2, id_registration1, id_registration2, registration_maintype );
				if( delcnt > 0 ) { return true; }
			}
		}


		else if( registration_maintype == 3 )   // death
		{
			String query_p1 = "SELECT id_source, role, firstname, prefix, familyname FROM person_c WHERE id_registration = " + id_registration1 + " AND role = 10";
			if( debug ) { System.out.println( query_p1 ); }

			String deceased_id_source1  = "";
			String deceased_firstname1  = "";
			String deceased_prefix1     = "";
			String deceased_familyname1 = "";

			try( ResultSet rs_p1 = dbconCleaned.executeQuery( query_p1 ) )
			{
				while( rs_p1.next() )
				{
					int role = rs_p1.getInt( "role" );

					deceased_id_source1  = rs_p1.getString( "id_source" );
					deceased_firstname1  = rs_p1.getString( "firstname" );
					deceased_prefix1     = rs_p1.getString( "prefix" );
					deceased_familyname1 = rs_p1.getString( "familyname" );

					if( deceased_id_source1  == null ) { deceased_id_source1  = ""; }
					if( deceased_firstname1  == null ) { deceased_firstname1  = ""; }
					if( deceased_prefix1     == null ) { deceased_prefix1     = ""; }
					if( deceased_familyname1 == null ) { deceased_familyname1 = ""; }

					id_source1 = deceased_id_source1;
				}
			}

			String query_p2 = "SELECT id_source, role, firstname, prefix, familyname FROM person_c WHERE id_registration = " + id_registration2 + " AND role = 10";
			if( debug ) { System.out.println( query_p2 ); }

			String deceased_id_source2  = "";
			String deceased_firstname2  = "";
			String deceased_prefix2     = "";
			String deceased_familyname2 = "";

			try( ResultSet rs_p2 = dbconCleaned.executeQuery( query_p2 ) )
			{
				while( rs_p2.next() )
				{
					int role = rs_p2.getInt( "role" );

					deceased_id_source2  = rs_p2.getString( "id_source" );
					deceased_firstname2  = rs_p2.getString( "firstname" );
					deceased_prefix2     = rs_p2.getString( "prefix" );
					deceased_familyname2 = rs_p2.getString( "familyname" );

					if( deceased_id_source2  == null ) { deceased_id_source2  = ""; }
					if( deceased_firstname2  == null ) { deceased_firstname2  = ""; }
					if( deceased_prefix2     == null ) { deceased_prefix2     = ""; }
					if( deceased_familyname2 == null ) { deceased_familyname2 = ""; }

					id_source2 = deceased_id_source2;
				}
			}

			if( deceased_firstname1.equals( deceased_firstname2 ) && deceased_familyname1.equals( deceased_familyname2 ) )
			{
				if( debug ) {
					showMessage_nl();
					String msg = String.format( "Duplicate registrations, ids, %d: %d, %d: %d (registration_maintype: %d)",
						rid1, id_registration1, rid2, id_registration2, registration_maintype );
					showMessage( msg, false, true );

					showMessage( "deceased_familyname1: " + deceased_familyname1 + ", deceased_prefix1: " + deceased_prefix1 + ", deceased_firstname1: " + deceased_firstname1, false, true );
					showMessage( "deceased_familyname2: " + deceased_familyname2 + ", deceased_prefix2: " + deceased_prefix2 + ", deceased_firstname2: " + deceased_firstname2, false, true );
				}

				int delcnt = flagDuplicateReg( debug, registrationIds, id_source1, id_source2, id_registration1, id_registration2, registration_maintype );
				if( delcnt > 0 ) { return true; }
			}
		}

		return false;
	} // compare2Registrations


	/**
	 * flagDuplicateReg()
	 * @param debug
	 * @param registrationIds
	 * @param id_source1
	 * @param id_source2
	 * @param id_registration1
	 * @param id_registration2
	 * @param registration_maintype
	 * @return
	 * @throws Exception
	 */
	private int flagDuplicateReg( boolean debug, Vector< Integer > registrationIds, String id_source1, String id_source2,
								  int id_registration1, int id_registration2, int registration_maintype )
		throws Exception
	{
		if( debug ) {
			showMessage_nl();
			showMessage( "Duplicate in Id group of " + registrationIds.size() + ": " + registrationIds.toString(), false, true );
		}

		int id_reg_keep = 0;
		int id_reg_flag = 0;
		String id_source_flag = "";

		// keep the smallest id_reg
		if( id_registration2 > id_registration1 )
		{
			id_reg_keep    = id_registration1;
			id_reg_flag    = id_registration2;
			id_source_flag = id_source2;
		}
		else
		{
			id_reg_keep    = id_registration2;
			id_reg_flag    = id_registration1;
			id_source_flag = id_source1;
		}

		if( debug ) {
			String msg = "keep id: " + id_reg_keep + ", flag: " + id_reg_flag + " (registration_maintype: " + registration_maintype + ")";
			//System.out.println( msg );
			showMessage( msg, false, true );
		}

		// write error msg with EC=1
		if( id_source_flag.isEmpty() ) { id_source_flag = "0"; }    // it must be a valid integer string for the log table
		String value = "";      // nothing to add
		addToReportRegistration( id_reg_flag, id_source_flag, 1, value );       // warning 1

		// get current flags string
		String getFlagQuery_r = "SELECT not_linksbase FROM registration_c WHERE id_registration = " + id_reg_flag;

		String old_flags = "";
		try( ResultSet rs = dbconCleaned.executeQuery( getFlagQuery_r ) )
		{
			while( rs.next() )
			{ old_flags = rs.getString( "not_linksbase" ); }
		}

		int countRegist = 0;
		String new_flags = "";

		if( old_flags == null || old_flags.isEmpty() ) { new_flags = "01000"; }
		else
		{
			// is the flag already set?
			int flag_idx = 1;       // 2nd position for the flag
			if( ! old_flags.substring( flag_idx, flag_idx + 1 ).equals( "1" ) )
			{
				// preserve other flags, and set new flag
				StringBuilder sb = new StringBuilder( old_flags );
				sb.setCharAt( flag_idx,'1' );
				new_flags = sb.toString();
			}
		}

		if( ! new_flags.isEmpty() )
		{
			// Flag second member of duplicate pair from registration_c
			String flagQuery_r = "UPDATE registration_c SET not_linksbase = '" + new_flags + "'";
			flagQuery_r += " WHERE id_registration = " + id_reg_flag + ";";
			//System.out.println(flagQuery_r);

			countRegist = dbconCleaned.executeUpdate( flagQuery_r );
		}

		return countRegist;
	} // flagDuplicateReg


	/**
	 * flagDuplicateRegsPersist()
	 * @param debug
	 * @param source
	 * @param rmtype
	 * @throws Exception
	 */
	private void flagDuplicateRegsPersist( boolean debug, String source, String rmtype )
		throws Exception
	{
		long threadId = Thread.currentThread().getId();

		showMessage( String.format( "Thread id %02d; flagDuplicateRegsPersist for source %s...", threadId, source ), false, true );

		String query_c = "SELECT COUNT(*) AS cnt FROM registration_c WHERE id_source = " + source;
		query_c += " AND ( id_persist_registration IS NULL OR INSTR( id_persist_registration, ' ' ) > 0 )";

		if ( ! rmtype.isEmpty() ) { query_c += " AND registration_maintype = " + rmtype; }
		if( debug ) { showMessage( query_c, false, true ); }
		if( debug ) { System.out.println( query_c ); }

		try( ResultSet rs_c = dbconCleaned.executeQuery( query_c ) )
		{
			rs_c.first();
			int cnt = rs_c.getInt( "cnt" );
			if( cnt != 0 ) {
				String msg = String.format( "Thread id %02d; # of regs with invalid id_persist_registration: %d (not flagged)", threadId, cnt );
				showMessage( msg, false, true );
			}
		}

		// The GROUP_CONCAT on id_registration is needed to get the different registration ids corresponding to the count.
		// And we require that the 4 grouping variables have normal values.
		int min_cnt = 2;    // in practice we see double, triples and quadruples

		String query_r = "SELECT GROUP_CONCAT(id_registration), id_persist_registration, COUNT(*) AS cnt";
		query_r += " FROM registration_c";
		query_r += " WHERE id_source = " + source;
		query_r += " AND id_persist_registration IS NOT NULL AND INSTR(id_persist_registration, ' ') = 0";

		if ( ! rmtype.isEmpty() ) { query_r += " AND registration_maintype = " + rmtype; }

		query_r +=  " GROUP BY id_persist_registration";
		query_r +=  " HAVING cnt >= " + min_cnt;
		query_r +=  " ORDER BY id_registration ASC, cnt DESC;";

		if( debug ) { showMessage( query_r, false, true ); }
		if( debug ) { System.out.println( query_r ); }

		int stepstate = count_step;
		try( ResultSet rs_r = dbconCleaned.executeQuery( query_r ) )
		{
			rs_r.last();
			int total = rs_r.getRow();
			rs_r.beforeFirst();

			int nflagRegist = 0;

			int ndups2 = 0;
			int ndups3 = 0;
			int ndups4 = 0;
			int ndups5 = 0;
			int ndupsx = 0;

			int count = 0;
			while( rs_r.next() )        // process all groups
			{
				count++;
				if( count == stepstate ) {
					long pct = Math.round( 100.0 * (float)count / (float)total );
					String msg = String.format( "Thread id %02d, flagDuplicateRegsPersist %d-of-%d (%d%%)", threadId, count, total, pct );
					showMessage( msg, true, true );
					stepstate += count_step;
				}

				String registrationIds_str = rs_r.getString( "GROUP_CONCAT(id_registration)" );
				if( registrationIds_str == null ) { registrationIds_str = ""; }
				String id_persist_registration = rs_r.getString( "id_persist_registration" );

				String registrationIdsStrs[] = registrationIds_str.split( "," );
				int ndups = registrationIdsStrs.length;
				switch( ndups )
				{
					case 2:
						ndups2++; break;
					case 3:
						ndups3++; break;
					case 4:
						ndups4++; break;
					case 5:
						ndups5++; break;
					default:
						ndupsx++;
				}

				if( debug ) { plog.show( String.format( "Thread id %02d; flagDuplicateRegsPersist guid: %s, ndups: %d, idregs: %s", threadId, id_persist_registration, ndups, registrationIds_str ) ); }

				flagIdPersistDuplicates( debug, source, rmtype, registrationIdsStrs );
			}

			if( ndups2 != 0 )
			{ showMessage( String.format( "Thread id %02d; flagDuplicateRegsPersist # of dup2: %d", threadId, ndups2 ), false, true ); }
			else if( ndups3 != 0 )
			{ showMessage( String.format( "Thread id %02d; flagDuplicateRegsPersist # of dup3: %d", threadId, ndups3 ), false, true ); }
			else if( ndups4 != 0 )
			{ showMessage( String.format( "Thread id %02d; flagDuplicateRegsPersist # of dup4: %d", threadId, ndups4 ), false, true ); }
			else if( ndups5 != 0 )
			{ showMessage( String.format( "Thread id %02d; flagDuplicateRegsPersist # of dup5: %d", threadId, ndups5 ), false, true ); }
			else if( ndupsx != 0 )
			{ showMessage( String.format( "Thread id %02d; flagDuplicateRegsPersist # of dupx: %d", threadId, ndupsx ), false, true ); }
		}
		catch( Exception ex ) {
			String msg = String.format( "Thread id %02d; Exception in flagDuplicateRegsPersist: %s", threadId, ex.getMessage() );
			showMessage( msg, false, true );
			ex.printStackTrace( new PrintStream( System.out ) );
		}
	} // flagDuplicateRegsPersist


	/**
	 * flagIdPersistDuplicates()
	 * @param debug
	 * @param source
	 * @param rmtype
	 * @param registrationIdsStrs
	 * @throws Exception
	 */
	private void flagIdPersistDuplicates( boolean debug, String source, String rmtype, String registrationIdsStrs[] )
		throws Exception
	{
		long threadId = Thread.currentThread().getId();

		try
		{
			Vector< Integer > registrationIds = new Vector< Integer >();
			for( String registrationId : registrationIdsStrs ) {
				registrationIds.add( Integer.parseInt( registrationId ) );
			}
			Collections.sort( registrationIds );    // inplace sorting

			int r = 0;
			for( int id_registration : registrationIds )
			{
				if( r == 0 )
				{   // keep lowest id_registration
					plog.show( String.format( "Thread id %02d; flagDuplicateRegsPersist keep id_reg: %d", threadId, id_registration ) );
				}
				else
				{
					// flag
					plog.show( String.format( "Thread id %02d; flagDuplicateRegsPersist flag id_reg: %d", threadId, id_registration ) );

					String query = "SELECT not_linksbase FROM links_cleaned.registration_c";
					query += String.format( " WHERE id_source = %s", source );
					query += String.format( " AND id_registration = %s", id_registration );

					if( debug ) { showMessage( query, false, true ); }
					//System.out.println( query );

					String old_flags = "";
					try( ResultSet rs = dbconCleaned.executeQuery( query ) )
					{
						rs.first();
						old_flags = rs.getString( "not_linksbase" );
					}

					String new_flags = "";

					if( old_flags == null || old_flags.isEmpty() ) { new_flags = "10000"; }
					else
					{
						// is the flag already set?
						int flag_idx = 0;       // 1st position for the flag
						if( ! old_flags.substring( flag_idx, flag_idx + 1 ).equals( "1" ) )
						{
							// preserve other flags, and set new flag
							StringBuilder sb = new StringBuilder( old_flags );
							sb.setCharAt( flag_idx,'1' );
							new_flags = sb.toString();
						}
					}

					if( ! new_flags.isEmpty() )
					{
						String flagQuery_r = "UPDATE registration_c SET not_linksbase = '" + new_flags + "'";
						flagQuery_r += " WHERE id_registration = " + id_registration + ";";
						if( debug ) { System.out.println(flagQuery_r); }

						int count = dbconCleaned.executeUpdate( flagQuery_r );
					}
				}
				r++;
			}

			registrationIds.clear();
		}
		catch( Exception ex ) {
			String msg = String.format( "Thread id %02d; Exception in flagDuplicateRegsPersist: %s", threadId, ex.getMessage() );
			showMessage( msg, false, true );
			ex.printStackTrace( new PrintStream( System.out ) );
		}
	} // flagIdPersistDuplicates


	/**
	 * flagEmptyDateRegs()
	 * @param debug
	 * @param source
	 * @param rmtype
	 * @throws Exception
	 *
	 * Flag registrations with empty registration dates from links_cleaned
	 */
	private void flagEmptyDateRegs( boolean debug, String source, String rmtype )
		throws Exception
	{
		long threadId = Thread.currentThread().getId();

		showMessage( String.format( "Thread id %02d; flagEmptyDateRegs for source %s", threadId, source ), false, true );

		String query_r = "SELECT id_registration, id_source, registration_date FROM registration_c"
			+ " WHERE id_source = " + source
			+ " AND ( registration_date IS NULL OR registration_date = '' )";

		if ( ! rmtype.isEmpty() ) { query_r += " AND registration_maintype = " + rmtype; }
		if( debug ) { showMessage( query_r, false, true ); }

		int nNoRegDate = 0;
		int stepstate = count_step;

		try( ResultSet rs_r = dbconCleaned.executeQuery( query_r ) )
		{
			int row = 0;

			while( rs_r.next() )        // process all results
			{
                /*
                row++;
                if( row == stepstate ) {
                    showMessage( "" + row, true, true );
                    stepstate += count_step;
                }

                int id_registration      = rs_r.getInt( "id_registration" );
                int id_source            = rs_r.getInt( "id_source" );
                String registration_date = rs_r.getString( "registration_date" );

                if( registration_date == null   ||
                        registration_date.isEmpty() ||
                        registration_date.equals( "null" ) )
                {
                    nNoRegDate++;

                    // Flag records with this registration
                    String deleteRegist = "DELETE FROM registration_c WHERE id_registration = " + id_registration;
                    String deletePerson = "DELETE FROM person_c WHERE id_registration = " + id_registration;

                    if( debug ) {
                        showMessage( "Deleting id_registration without date: " + id_registration, false, true );
                        showMessage( deleteRegist, false, true );
                        showMessage( deletePerson, false, true );
                    }

                    String id_source_str = Integer.toString( id_source );
                    addToReportRegistration( id_registration, id_source_str, 2, "" );       // warning 2

                    dbconCleaned.runQuery( deleteRegist );
                    dbconCleaned.runQuery( deletePerson );
                }
                */

				int id_regist = rs_r.getInt( "id_registration" );

				// get current flags string
				String getFlagQuery_r = "SELECT not_linksbase FROM registration_c WHERE id_registration = " + id_regist;

				String old_flags = "";
				try( ResultSet rs = dbconCleaned.executeQuery( getFlagQuery_r ) )
				{
					while( rs.next() )
					{ old_flags = rs.getString( "not_linksbase" ); }
				}

				int countRegist = 0;
				String new_flags = "";

				if( old_flags == null || old_flags.isEmpty() ) { new_flags = "00010"; }
				else
				{
					// is the flag already set?
					int flag_idx = 3;       // 4th position for the flag
					if( ! old_flags.substring( flag_idx, flag_idx + 1 ).equals( "1" ) )
					{
						// preserve other flags, and set new flag
						StringBuilder sb = new StringBuilder( old_flags );
						sb.setCharAt( flag_idx,'1' );
						new_flags = sb.toString();
					}
				}

				if( ! new_flags.isEmpty() )
				{
					String flagQuery_r = "UPDATE registration_c SET not_linksbase = '" + new_flags + "'";
					flagQuery_r += " WHERE id_registration = " + id_regist + ";";
					if( debug ) { System.out.println(flagQuery_r); }

					countRegist = dbconCleaned.executeUpdate( flagQuery_r );
					nNoRegDate += countRegist;
				}
			}

			String msg =  String.format( "Thread id %02d; Number of registrations without date: %d", threadId, nNoRegDate );
			showMessage( msg, false, true );
		}
		catch( Exception ex ) {
			if( ex.getMessage() != "After end of result set" ) {
				String msg = String.format( "Thread id %02d; Exception in flagEmptyDateRegs: %s", threadId, ex.getMessage() );
				showMessage( msg, false, true );
				ex.printStackTrace( new PrintStream( System.out ) );
			}
		}
	} // flagEmptyDateRegs


	/**
	 * flagEmptyDaysSinceBegin()
	 * @param debug
	 * @param source
	 * @param rmtype
	 * @throws Exception
	 */
	private void flagEmptyDaysSinceBegin( boolean debug, String source, String rmtype )
		throws Exception
	{
		long threadId = Thread.currentThread().getId();

		showMessage( String.format( "Thread id %02d; flagEmptyDaysSinceBegin for source %s", threadId, source ), false, true );

		String query_r = "SELECT id_registration, id_source, registration_days FROM registration_c"
			+ " WHERE id_source = " + source
			+ " AND ( registration_days IS NULL OR registration_days = '' )";

		if ( ! rmtype.isEmpty() ) { query_r += " AND registration_maintype = " + rmtype; }
		if( debug ) { showMessage( query_r, false, true ); }

		int nNoRegDate = 0;
		//int stepstate = count_step;

		try( ResultSet rs_r = dbconCleaned.executeQuery( query_r ) )
		{
			int row = 0;

			while( rs_r.next() )        // process all results
			{
				int id_regist = rs_r.getInt( "id_registration" );

				// get current flags string
				String getFlagQuery_r = "SELECT not_linksbase FROM registration_c WHERE id_registration = " + id_regist;

				String old_flags = "";
				try( ResultSet rs = dbconCleaned.executeQuery( getFlagQuery_r ) )
				{
					while( rs.next() )
					{ old_flags = rs.getString( "not_linksbase" ); }
				}

				int countRegist = 0;
				String new_flags = "";

				if( old_flags == null || old_flags.isEmpty() ) { new_flags = "00001"; }
				else
				{
					// is the flag already set?
					int flag_idx = 4;       // 5th position for the flag
					if( ! old_flags.substring( flag_idx, flag_idx + 1 ).equals( "1" ) )
					{
						// preserve other flags, and set new flag
						StringBuilder sb = new StringBuilder( old_flags );
						sb.setCharAt( flag_idx,'1' );
						new_flags = sb.toString();
					}
				}

				if( ! new_flags.isEmpty() )
				{
					String flagQuery_r = "UPDATE registration_c SET not_linksbase = '" + new_flags + "'";
					flagQuery_r += " WHERE id_registration = " + id_regist + ";";
					if( debug ) { System.out.println(flagQuery_r); }

					countRegist = dbconCleaned.executeUpdate( flagQuery_r );
					nNoRegDate += countRegist;
				}
			}

			String msg =  String.format( "Thread id %02d; Number of registrations without days since begin: %d", threadId, nNoRegDate );
			showMessage( msg, false, true );
		}
		catch( Exception ex ) {
			if( ex.getMessage() != "After end of result set" ) {
				String msg = String.format( "Thread id %02d; Exception in flagEmptyDaysSinceBegin: %s", threadId, ex.getMessage() );
				showMessage( msg, false, true );
				ex.printStackTrace( new PrintStream( System.out ) );
			}
		}
	} // flagEmptyDaysSinceBegin


	/*---< Flag Bad Person Recs >-------------------------------------------*/

	/**
	 * doFlagPersonRecs()
	 * @param debug
	 * @param go
	 * @param source
	 * @param rmtype
	 * @throws Exception
	 */
	private void doFlagPersonRecs( boolean debug, boolean go, String source, String rmtype ) throws Exception
	{
		long threadId = Thread.currentThread().getId();

		String funcname = String.format( "Thread id %02d; doFlagPersonRecs for source: %s, rmtype: %s", threadId, source, rmtype );

		if( !go ) {
			if( showskip ) { showMessage( "Skipping " + funcname, false, true ); }
			return;
		}

		long timeStart = System.currentTimeMillis();

		String msg = String.format( "Thread id %02d; Clear Previous Person Flags for source: %s, rmtype: %s ...", threadId, source, rmtype );
		showMessage( msg, false, true );
		clearFlagsPersonBasetable( debug, source, rmtype );

		msg = String.format( "Thread id %02d; Flagging Person recs without familynames for source: %s, rmtype: %s ...", threadId, source, rmtype );
		showMessage( msg, false, true );
		flagEmptyFamilynameRecs( debug, source, rmtype );

		msg = String.format( "Thread id %02d; Flagging Person recs without role for source: %s, rmtype: %s ...", threadId, source, rmtype );
		showMessage( msg, false, true );
		flagEmptyRoleRecs( debug, source, rmtype );

		elapsedShowMessage( funcname, timeStart, System.currentTimeMillis() );
		showMessage_nl();
	} // doFlagPersonRecs


	/**
	 * clearFlagsPersonBasetable()
	 * @param debug
	 * @param source
	 * @param rmtype
	 * @throws Exception
	 */
	private void clearFlagsPersonBasetable( boolean debug, String source, String rmtype )
		throws Exception
	{
		long threadId = Thread.currentThread().getId();

		// Clear previous flag values for given source
		String query_r = "UPDATE person_c SET not_linksbase_p = NULL WHERE id_source = " + source + "";

		if ( ! rmtype.isEmpty() ) { query_r += " AND registration_maintype = " + rmtype; }
		if( debug ) { System.out.println( query_r ); }

		int nrec = dbconCleaned.executeUpdate( query_r );

		String msg = String.format( "Thread id %02d; Number of flags cleared: %d", threadId, nrec );
		showMessage( msg, false, true );
	} // clearFlagsPersonBasetable


	/**
	 * flagEmptyFamilynameRecs()
	 * @param debug
	 * @param source
	 * @param rmtype
	 * @throws Exception
	 */
	private void flagEmptyFamilynameRecs( boolean debug, String source, String rmtype )
		throws Exception
	{
		long threadId = Thread.currentThread().getId();

		showMessage( String.format( "Thread id %02d; flagEmptyFamilynameRecs for source %s", threadId, source ), false, true );

		String query_r = "SELECT id_person, role FROM person_c"
			+ " WHERE id_source = " + source
			+ " AND ( familyname IS NULL OR familyname = '' )";

		if ( ! rmtype.isEmpty() ) { query_r += " AND registration_maintype = " + rmtype; }
		if( debug ) { showMessage( query_r, false, true ); }

		int nNoRole = 0;
		//int stepstate = count_step;

		try( ResultSet rs_r = dbconCleaned.executeQuery( query_r ) )
		{
			int row = 0;

			while( rs_r.next() )        // process all registrations
			{
				int id_person = rs_r.getInt( "id_person" );

				// get current flags string
				String getFlagQuery_r = "SELECT not_linksbase_p FROM person_c WHERE id_person = " + id_person;

				String old_flags = "";
				try( ResultSet rs = dbconCleaned.executeQuery( getFlagQuery_r ) )
				{
					while( rs.next() )
					{ old_flags = rs.getString( "not_linksbase_p" ); }
				}

				int countRegist = 0;
				String new_flags = "";

				if( old_flags == null || old_flags.isEmpty() ) { new_flags = "10"; }
				else
				{
					// is the flag already set?
					int flag_idx = 0;       // 1st position for the flag
					if( ! old_flags.substring( flag_idx, flag_idx + 1 ).equals( "1" ) )
					{
						// preserve other flags, and set new flag
						StringBuilder sb = new StringBuilder( old_flags );
						sb.setCharAt( flag_idx,'1' );
						new_flags = sb.toString();
					}
				}

				if( ! new_flags.isEmpty() )
				{
					String flagQuery_r = "UPDATE person_c SET not_linksbase_p = '" + new_flags + "'";
					flagQuery_r += " WHERE id_person = " + id_person + ";";
					if( debug ) { System.out.println(flagQuery_r); }

					countRegist = dbconCleaned.executeUpdate( flagQuery_r );
					nNoRole += countRegist;
				}
			}

			String msg = String.format( "Thread id %02d; Number of person records with missing familyname flagged: %d", threadId, nNoRole );
			showMessage( msg, false, true );
		}
		catch( Exception ex ) {
			String msg = String.format( "Thread id %02d; Exception in flagEmptyFamilynameRecs: %s", threadId, ex.getMessage() );
			showMessage( msg, false, true );
			ex.printStackTrace( new PrintStream( System.out ) );
		}
	} // flagEmptyFamilynameRecs


	/**
	 * flagEmptyRoleRecs()
	 * @param debug
	 * @param source
	 * @param rmtype
	 * @throws Exception
	 *
	 * Flag person_c records with empty person roles from links_cleaned
	 */
	private void flagEmptyRoleRecs( boolean debug, String source, String rmtype )
		throws Exception
	{
		long threadId = Thread.currentThread().getId();

		showMessage( String.format( "Thread id %02d; FlagEmptyRoleRecs for source %s", threadId, source ), false, true );

		String query_r = "SELECT id_person, role FROM person_c"
			+ " WHERE id_source = " + source
			+ " AND ( role IS NULL OR role = 0 )";

		if ( ! rmtype.isEmpty() ) { query_r += " AND registration_maintype = " + rmtype; }
		if( debug ) { showMessage( query_r, false, true ); }

		int nNoRole = 0;
		//int stepstate = count_step;

		try( ResultSet rs_r = dbconCleaned.executeQuery( query_r ) )
		{
			int row = 0;

			while( rs_r.next() )        // process all registrations
			{
                /*
                row++;
                if( row == stepstate ) {
                    showMessage( "" + row, true, true );
                    stepstate += count_step;
                }

                row++;
                int id_registration       = rs_r.getInt( "id_registration" );
                int id_source             = rs_r.getInt( "id_source" );
                int registration_maintype = rs_r.getInt( "registration_maintype" );

                String query_p = "SELECT id_person, role FROM person_c WHERE id_registration = " + id_registration;
                if ( debug ) { showMessage( query_p, false, true ); }

                ResultSet rs_p = dbconCleaned.executeQuery( query_p );

                boolean norole = false;
                while (rs_p.next())        // process the persons of this registration
                {
                    int id_person = rs_p.getInt( "id_person" );
                    String role   = rs_p.getString( "role" );

                    if( role == null || role.isEmpty() || role.equals( "null" ) ) {
                        norole =  true;

                        if( debug ) {
                            String msg = String.format( "No role: id_registration: %d, id_person: %d, registration_maintype: %d, role: %d",
                               id_registration, id_person, registration_maintype );
                            System.out.println( msg ); showMessage( msg, false, true );
                        }
                        break;  // Kees: all persons of a registration must have a role, so we are done for this reg
                    }
                }
                */
                /*
                if( norole )
                {
                    nNoRole++;

                    // Delete records with this registration
                    String deleteRegist = "DELETE FROM registration_c WHERE id_registration = " + id_registration;
                    String deletePerson = "DELETE FROM person_c WHERE id_registration = " + id_registration;

                    if( debug ) {
                        showMessage( "Deleting id_registration without role: " + id_registration, false, true );
                        showMessage( deleteRegist, false, true );
                        showMessage( deletePerson, false, true );
                    }

                    String id_source_str = Integer.toString( id_source );
                    addToReportRegistration( id_registration, id_source_str, 3, "" );       // warning 3

                    dbconCleaned.runQuery( deleteRegist );
                    dbconCleaned.runQuery( deletePerson );
                }
                */

				int id_person = rs_r.getInt( "id_person" );

				// get current flags string
				String getFlagQuery_r = "SELECT not_linksbase_p FROM person_c WHERE id_person = " + id_person;
				ResultSet rs = dbconCleaned.executeQuery( getFlagQuery_r );
				String old_flags = "";
				while( rs.next() )
				{ old_flags = rs.getString( "not_linksbase_p" ); }

				int countRegist = 0;
				String new_flags = "";

				if( old_flags == null || old_flags.isEmpty() ) { new_flags = "01"; }
				else
				{
					// is the flag already set?
					int flag_idx = 1;       // 2nd position for the flag
					if( ! old_flags.substring( flag_idx, flag_idx + 1 ).equals( "1" ) )
					{
						// preserve other flags, and set new flag
						StringBuilder sb = new StringBuilder( old_flags );
						sb.setCharAt( flag_idx,'1' );
						new_flags = sb.toString();
					}
				}

				if( ! new_flags.isEmpty() )
				{
					String flagQuery_r = "UPDATE person_c SET not_linksbase_p = '" + new_flags + "'";
					flagQuery_r += " WHERE id_person = " + id_person + ";";
					if( debug ) { System.out.println(flagQuery_r); }

					countRegist = dbconCleaned.executeUpdate( flagQuery_r );
					nNoRole += countRegist;
				}
			}

			String msg = String.format( "Thread id %02d; Number of person records with missing role flagged: %d", threadId, nNoRole );
			showMessage( msg, false, true );
		}
		catch( Exception ex ) {
			String msg = String.format( "Thread id %02d; Exception in flagEmptyRoleRecs: %s", threadId, ex.getMessage() );
			showMessage( msg, false, true );
			ex.printStackTrace( new PrintStream( System.out ) );
		}
	} // flagEmptyRoleRecs


	/*---< Scan Remarks >-----------------------------------------------------*/

	/**
	 * doScanRemarks()
	 * @param debug
	 * @param go
	 * @param source
	 * @param rmtype
	 * @throws Exception
	 */
	private void doScanRemarks( boolean debug, boolean go, String source, String rmtype )
		throws Exception
	{
		long threadId = Thread.currentThread().getId();

		String funcname = String.format( "Thread id %02d; doScanRemarks for source: %s, rmtype: %s", threadId, source, rmtype );

		if( !go ) {
			if( showskip ) { showMessage( "Skipping " + funcname, false, true ); }
			return;
		}

		long timeStart = System.currentTimeMillis();
		String msg = funcname + " ...";
		showMessage( msg, false, true );

		scanRemarks( debug, source, rmtype );

		elapsedShowMessage( funcname, timeStart, System.currentTimeMillis() );
		showMessage_nl();
	} // doScanRemarks


	/**
	 * scanRemarks()
	 * @param debug
	 * @param source
	 * @param rmtype
	 * @throws Exception
	 */
	private void scanRemarks( boolean debug, String source, String rmtype ) throws Exception
	{
		long threadId = Thread.currentThread().getId();

		showMessage( String.format( "Thread id %02d; scanRemarks()", threadId ), false, true );

		// Clear previous values for given source
		showMessage( String.format( "Thread id %02d; clear previous remarks values: extract", threadId ), false, true );
		String clearQuery_r = "UPDATE registration_c SET extract = NULL WHERE id_source = " + source;
		if ( ! rmtype.isEmpty() ) { clearQuery_r += " AND registration_maintype = " + rmtype; }
		dbconCleaned.executeUpdate( clearQuery_r );

		showMessage( String.format( "Thread id %02d; clear previous remarks values: status_mother", threadId ), false, true );
		String clearQuery_p1 = "UPDATE person_c SET status_mother = NULL WHERE id_source = " + source;
		if ( ! rmtype.isEmpty() ) { clearQuery_p1 += " AND registration_maintype = " + rmtype; }
		dbconCleaned.executeUpdate( clearQuery_p1 );

		showMessage( String.format( "Thread id %02d; clear previous remarks values: stillbirth", threadId ), false, true );
		String clearQuery_p2 = "UPDATE person_c SET stillbirth = NULL WHERE stillbirth = 'y-r' AND id_source = " + source;
		if ( ! rmtype.isEmpty() ) { clearQuery_p2 += " AND registration_maintype = " + rmtype; }
		dbconCleaned.executeUpdate( clearQuery_p2 );

		showMessage( String.format( "Thread id %02d; clear previous remarks values: divorce", threadId ), false, true );
		String clearQuery_p3 = "UPDATE person_c SET divorce_text = NULL, divorce_day = NULL, divorce_month = NULL, divorce_year = NULL, divorce_location = NULL WHERE id_source = " + source;
		if ( ! rmtype.isEmpty() ) { clearQuery_p3 += " AND registration_maintype = " + rmtype; }
		dbconCleaned.executeUpdate( clearQuery_p3 );

		// load the table data from links_general.scan_remarks
		String selectQuery_r = "SELECT * FROM scan_remarks ORDER BY id_scan";
		if( debug ) {
			System.out.printf( "\n%s\n", selectQuery_r );
			showMessage( selectQuery_r, false, true );
		}

		Vector< Remarks > ref_remarksVec = new Vector();    // Remarks class from dataset

		showMessage( String.format( "Thread id %02d; loading remarks reference table", threadId ), false, true );

		HikariConnection dbconRef = new HikariConnection( dsReference.getConnection() );

		try( ResultSet rs_r = dbconRef.executeQuery( selectQuery_r ) )
		{
			int nrecord = 0;
			while( rs_r.next() )
			{
				int id_scan  = rs_r.getInt( "id_scan" );
				int maintype = rs_r.getInt( "maintype" );
				int role     = rs_r.getInt( "role" );

				String string_1   = rs_r.getString( "string_1" );
				String string_2   = rs_r.getString( "string_2" );
				String string_3   = rs_r.getString( "string_3" );
				String not_string = rs_r.getString( "not_string" );
				String name_table = rs_r.getString( "name_table" );
				String name_field = rs_r.getString( "name_field" );
				String value      = rs_r.getString( "value" );

				if( string_1   != null ) { string_1   = string_1  .toLowerCase(); }
				if( string_2   != null ) { string_2   = string_2  .toLowerCase(); }
				if( string_3   != null ) { string_3   = string_3  .toLowerCase(); }
				if( not_string != null ) { not_string = not_string.toLowerCase(); }
				if( name_table != null ) { name_table = name_table.toLowerCase(); }
				if( name_field != null ) { name_field = name_field.toLowerCase(); }
				if( value      != null ) { value      = value     .toLowerCase(); }

				Remarks ref_remarks = new Remarks();

				ref_remarks.setIdScan(    id_scan );
				ref_remarks.setMaintype(  maintype );
				ref_remarks.setRole(      role );
				ref_remarks.setString_1(  string_1 );
				ref_remarks.setString_2(  string_2 );
				ref_remarks.setString_3(  string_3 );
				ref_remarks.setNotString( not_string );
				ref_remarks.setNameTable( name_table );
				ref_remarks.setNameField( name_field );
				ref_remarks.setValue(     value );

				ref_remarksVec.add( ref_remarks  );

				if( debug ) { System.out.printf( "%2d, id_scan: %3d, maintype: %d, role : %2d,  string_1: %s,  string_2: %s,  string_3: %s,  not_string: %s,  name_table: %s, name_field: %s, value: %s\n",
					nrecord, id_scan, maintype, role, string_1, string_2, string_3, not_string, name_table, name_field, value ); }

				nrecord++;
			}
			if( debug ) { System.out.println( "" ); }
		}
		catch( Exception ex ) {
			String msg = String.format( "Thread id %02d; Exception 1 in scanRemarks: %s", threadId, ex.getMessage() );
			showMessage( msg, false, true );
			ex.printStackTrace( new PrintStream( System.out ) );
		}

		dbconRef.close();

		// loop through the remarks from registration_o
		String selectQuery_o = "SELECT id_registration , registration_maintype , remarks FROM registration_o";
		selectQuery_o += " WHERE id_source = " + source;

		if ( ! rmtype.isEmpty() ) { selectQuery_o += " AND registration_maintype = " + rmtype; }

		selectQuery_o += " ORDER BY id_registration";

		if( debug ) {
			System.out.printf( "%s\n\n", selectQuery_o );
			showMessage( selectQuery_o, false, true );
		}

		try( ResultSet rs_o = dbconOriginal.executeQuery( selectQuery_o ) )
		{
			int nupdates  = 0;
			int ndivorces = 0;

			while( rs_o.next() )
			{
				int id_registration       = rs_o.getInt( "id_registration" );
				int registration_maintype = rs_o.getInt( "registration_maintype" );

				String remarks_str = rs_o.getString( "remarks" );

				if( remarks_str == null || remarks_str.isEmpty() ) { continue; }
				else { remarks_str = remarks_str.toLowerCase(); }

				//if( id_registration == 8244763 ) { debug = true; }
				//else { debug = false; continue; }

				int record = 0;
				// loop through all remark entries from the ref table
				for( Remarks ref_remarks : ref_remarksVec )
				{
					int id_scan  = ref_remarks.getIdScan();                         // only for debugging purposes
					int maintype = ref_remarks.getMaintype();
					int role     = ref_remarks.getRole();

					String string_1   = ref_remarks.getString_1();
					String string_2   = ref_remarks.getString_2();
					String string_3   = ref_remarks.getString_3();
					String not_string = ref_remarks.getNotString();
					String name_table = ref_remarks.getNameTable();
					String name_field = ref_remarks.getNameField();
					String value      = ref_remarks.getValue();

					boolean found = false;
					if( registration_maintype ==  maintype )                    // maintype should match
					{
						if( string_2 == null || string_2.isEmpty() )
						{
							if( remarks_str.indexOf( string_1 ) != -1 )         // string_1 found in remarks
							{ found = true; }
						}
						else        // string_2 not empty
						{
							if( string_3 == null || string_3.isEmpty() )
							{
								if( remarks_str.indexOf( string_1 ) != -1 &&
									remarks_str.indexOf( string_2 ) != -1 )     // first 2 found in remarks
								{ found = true; }
							}
							else    // string_3 not empty
							{
								if( remarks_str.indexOf( string_1 ) != -1 &&
									remarks_str.indexOf( string_2 ) != -1 &&
									remarks_str.indexOf( string_3 ) != -1 )     // all 3 found in remarks
								{ found = true; }
							}
						}
					}

					if( found )
					{
						if( debug ) { System.out.println( String.format("id_registration: %d: 1:|%s| 2:|%s| 3:|%s| ~:|%s|", id_registration, string_1, string_2, string_3, not_string )); }

						if( not_string == null || not_string.isEmpty() || remarks_str.indexOf( not_string ) == -1 )
						{
							nupdates++;

							if( string_1.equals( "echtscheiding" ) )
							{
								ndivorces++;
								int p = remarks_str.indexOf( "echtscheiding" );
								//if( debug ) { System.out.println( "p: " + p ); }

								if( p != -1 )
								{
									if( debug ) { System.out.println( String.format("%d: '%s'", id_registration, remarks_str )); }
									String divorce_str = remarks_str.substring( p );    // skip until "echtscheiding"
									scanRemarksDivorce( debug, nupdates, remarks_str, id_scan, id_registration, role, name_table, name_field, divorce_str );
								}
							}
							else { scanRemarksUpdate( debug, nupdates, remarks_str, id_scan, id_registration, role, name_table, name_field, value ); }
						}
						else    // not_string not empty
						{
							if( remarks_str.indexOf( not_string ) == -1 )   // but not found
							{ scanRemarksUpdate( debug, nupdates, remarks_str, id_scan, id_registration, role, name_table, name_field, value ); }
						}
					}
				}
			}

			ref_remarksVec.clear();     // free
			ref_remarksVec = null;

			String msg = String.format( "Thread id %02d; scanRemarks: Number of updates: %d, of which divorces: %d", threadId, nupdates, ndivorces );
			showMessage( msg, false, true );
		}
		catch( Exception ex ) {
			String msg = String.format( "Thread id %02d; Exception 2 in scanRemarks: %s", threadId, ex.getMessage() );
			showMessage( msg, false, true );
			ex.printStackTrace( new PrintStream( System.out ) );
		}
	} // scanRemarks


	/**
	 * scanRemarksDivorce()
	 * @param debug
	 * @param nupdates
	 * @param remarks_str
	 * @param id_scan
	 * @param id_registration
	 * @param role
	 * @param name_table
	 * @param name_field
	 * @param divorceStr
	 */
	private void scanRemarksDivorce( boolean debug, int nupdates, String remarks_str, int id_scan, int id_registration, int role, String name_table, String name_field, String divorceStr )
	{
		long threadId = Thread.currentThread().getId();
		String query_u = "";

		//if( debug ) { System.out.println( "scanRemarksDivorce " + name_table );}

		if( name_table.equals( "person_c" ) )
		{
			String mysqlStr = divorceStr;
			mysqlStr = mysqlStr.replace( "'",  "\\'" );       // escape single quotes
			mysqlStr = mysqlStr.replace( "\"", "\\\"" );      // escape quotes quotes

			query_u = String.format( "UPDATE links_cleaned.person_c SET %s = '%s' WHERE id_registration = %d AND role = %d",
				name_field, mysqlStr, id_registration, role );

			try {
				int rowsAffected = dbconCleaned.executeUpdate( query_u );
				if( debug ) { System.out.println( String.format( "%d %s", rowsAffected, query_u ) ); }
			}
			catch( Exception ex ) {
				String msg = String.format( "Thread id %02d; Exception 1 in scanRemarksDivorce: %s", threadId, ex.getMessage() );
				showMessage( msg, false, true );
				showMessage( "Query: " + query_u, false, true );
				ex.printStackTrace( new PrintStream( System.out ) );
			}

			// input may contain 0, 1 or 2 dates; we want the first, determined by finding the first year: 4 digits in a row.
			String patternStr = "\\d{4}";                       // 4 digits in a row: year
			Pattern pattern = Pattern.compile( patternStr );    // create a Pattern object
			Matcher matcher = pattern.matcher( divorceStr );    // create Matcher object.

			String dateStr = "";
			if( matcher.find() )
			{
				String year = matcher.group(0);
				int pos = divorceStr.indexOf( year );
				String dateStrLong = divorceStr.substring( pos-6, pos+4 );
				//if( debug ) { System.out.println( String.format( "dateStrLong: %s", dateStrLong ) ); }

				char c0 = dateStrLong.charAt(0);
				char c1 = dateStrLong.charAt(1);

				if( ! Character.isDigit( c0 ) ) {
					if( ! Character.isDigit( c1 ) ) { dateStr = dateStrLong.substring( 2 ); }   //skip 2
					else { dateStr = dateStrLong.substring( 1 ); }  // skip 1
				}
				else { dateStr = dateStrLong; }     // skip 0

				//if( debug ) { System.out.println(String.format("year: %s, dateStr: |%s|", year, dateStr )); }
			}
			else {      // unusable
				//if( debug ) { System.out.println( String.format( "matcher.find() false" ) ); }
				return;
			}

			// The divorce string may contain 0, 1 or 2 dates of the form "dd%MM%yyyy" where % can be ' ', '-' or '/',
			// but "dd" may also be "d", and "MM" may also be "M". We want the first date.
			DateTimeFormatter dateFormatter = DateTimeFormatter.ofPattern(""
				+ "[dd MM yyyy]"
				+ "[d MM yyyy]"
				+ "[dd M yyyy]"
				+ "[d M yyyy]"

				+ "[dd-MM-yyyy]"
				+ "[d-MM-yyyy]"
				+ "[dd-M-yyyy]"
				+ "[d-M-yyyy]"

				+ "[dd/MM/yyyy]"
				+ "[d/MM/yyyy]"
				+ "[dd/M/yyyy]"
				+ "[d/M/yyyy]"

				+ "[dd.MM.yyyy]"
				+ "[d.MM.yyyy]"
				+ "[dd.M.yyyy]"
				+ "[d.M.yyyy]"
			);
			// so, e.g., "dd MM yy" will give an exception

            /*
            if( debug ) { System.out.println( String.format( "id_registration: %d, divorceStr: |%s|", id_registration, divorceStr) ); }
            String divorceStrClean = divorceStr.replaceAll( "[^0-9 -/]", "" );    // keep digits plus separators
            if( debug ) { System.out.println( String.format( "id_registration: %d, divorceStrClean rep1: |%s|", id_registration, divorceStrClean) ); }

            // somehow the regexp keeps more than we want??
            divorceStrClean = divorceStrClean.replaceAll( "[',()]", "" );
            if( debug ) { System.out.println( String.format( "id_registration: %d, divorceStrClean rep2: |%s|", id_registration, divorceStrClean) ); }

            divorceStrClean = divorceStrClean.trim();       // zap leading and trailing spaces
            if( debug ) { System.out.println( String.format( "id_registration: %d, divorceStrClean trim: |%s|", id_registration, divorceStrClean) ); }

            // there can be 2 dates, also marriage, we use the first
            if( divorceStrClean.length() > 10 ) {
                divorceStrClean = divorceStrClean.substring( 0, 10 );
                if( debug ) { System.out.println( String.format( "id_registration: %d, divorceStrClean chop: |%s|", id_registration, divorceStrClean) ); }

                divorceStrClean = divorceStrClean.trim();   // zap leading and trailing spaces
                if( debug ) { System.out.println( String.format( "id_registration: %d, divorceStrClean trim: |%s|", id_registration, divorceStrClean) ); }
            }
            dateStr = divorceStrClean;
            */

			if( ! dateStr.isEmpty() && dateStr.length() >= 8 && dateStr.length() <= 10 )
			{
				int day = 0, month = 0, year = 0;
				try
				{
					LocalDate localDate = LocalDate.parse( dateStr, dateFormatter );

					day   = localDate.getDayOfMonth();
					month = localDate.getMonthValue();
					year  = localDate.getYear();
					if( debug ) { System.out.println( String.format( "day: %d, month: %d, year: %d", day, month, year ) ); }

					// TODO try to extract the divorce location
				}
				catch( DateTimeParseException ex )
				{
                    /*
                    String msg = String.format( "Thread id %02d; Exception: %s", threadId, ex.getMessage() );
                    System.out.println( msg );
                    System.out.println( String.format( "id_registration: %d, divorceStr: |%s|", id_registration, divorceStr ) );
                    System.out.println( String.format( "id_registration: %d, dateStr: |%s|", id_registration, dateStr ) );

                    nattyDateParser( debug, divorceStr );       // give it a try
                    */
					return;     // forget it
				}

				try
				{
					// divorce year must be grater than marriage year
					query_u = String.format( "UPDATE links_cleaned.person_c SET divorce_day = %d, divorce_month = %d, divorce_year = %d WHERE id_registration = %d AND role = %d AND mar_year IS NOT NULL AND mar_year <> 0 AND %d > mar_year;",
						day, month, year, id_registration, role, year );
					if( debug ) { System.out.println( query_u ); }
					int rowsAffected = dbconCleaned.executeUpdate( query_u );
				}
				catch( Exception ex )  {
					String msg = String.format( "Thread id %02d; Exception 2 in scanRemarksDivorce: %s", threadId, ex.getMessage() );
					showMessage( msg, false, true );
					showMessage( "Query: " + query_u, false, true );
					ex.printStackTrace( new PrintStream( System.out ) );
				}
			}
		}
	} // scanRemarksDivorce


	/**
	 * @param debug
	 * @param inputStr
	 * @throws Exception
	 */
    /*
    private void nattyDateParser( boolean debug, String inputStr )
    {
        java.util.List<java.util.Date> dates = new Parser().parse( inputStr ).get( 0 ).getDates();
        int ndates = dates.size();
        for( int i = 0; i < ndates; i++ ) { System.out.println( "Natty date: " + dates.get( i ) ); }
    } // nattyDateParser
    */

	/**
	 * scanRemarksUpdate()
	 * @param debug
	 * @param nupdates
	 * @param remarks_str
	 * @param id_scan
	 * @param id_registration
	 * @param role
	 * @param name_table
	 * @param name_field
	 * @param value
	 */
	private void scanRemarksUpdate( boolean debug, int nupdates, String remarks_str, int id_scan, int id_registration, int role, String name_table, String name_field, String value )
	{
		long threadId = Thread.currentThread().getId();

		String query_u = "";

		if( name_table.equals( "registration_c" ) ) {
			query_u = String.format( "UPDATE links_cleaned.registration_c SET %s = '%s' WHERE id_registration = %d",
				name_field, value, id_registration );
		}
		else if( name_table.equals( "person_c" ) ) {
			query_u = String.format( "UPDATE links_cleaned.person_c SET %s = '%s' WHERE id_registration = %d AND role = %d",
				name_field, value, id_registration, role );
		}

		if( debug ) {
			System.out.printf( "Update: %d, based on id_scan: %d, and id_registration: %d, and remarks: %s\n", nupdates, id_scan, id_registration, remarks_str );
			System.out.printf("%s\n", query_u);
		}

		try { dbconCleaned.executeUpdate( query_u ); }
		catch( Exception ex ) {
			String msg = String.format( "Thread id %02d; Exception 2 in scanRemarksUpdate: %s", threadId, ex.getMessage() );
			showMessage( msg, false, true );
			showMessage( "Query: " + query_u, false, true );
			ex.printStackTrace( new PrintStream( System.out ) );
		}
	} // scanRemarksUpdate

	/*---< end Cleaning options >-------------------------------------------*/

}
