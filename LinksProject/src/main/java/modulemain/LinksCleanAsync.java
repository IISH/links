package modulemain;

import dataset.ThreadManager;

import general.PrintLogger;

import java.io.PrintStream;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

import java.time.LocalDateTime;

import javax.swing.JTextArea;
import javax.swing.JTextField;

import com.zaxxer.hikari.HikariDataSource;

import connectors.HikariConnection;

import dataset.Options;

import general.Functions;

/**
 * @author Fons Laan
 *
 * FL-30-Sep-2019 Created
 * FL-08-Oct-2019
 */
public class LinksCleanAsync extends Thread
{
	private Options opts;
	private ThreadManager tm;
	private String source;
	private String rmtype;
	private PrintLogger plog;

	private JTextField guiLine;
	private JTextArea guiArea;

	private String endl = ". OK.";              // ".";
	private boolean showskip;

	private HikariDataSource dsOriginal;
	private HikariDataSource dsCleaned;

	private HikariConnection dbconOriginal = null;
	private HikariConnection dbconCleaned = null;


public LinksCleanAsync
	(
		ThreadManager tm,
		Options opts,
		JTextField guiLine,
		JTextArea guiArea,
		String source,
		String rmtype,
		boolean showskip,
		HikariDataSource dsOriginal,
		HikariDataSource dsCleaned
	)
	{
		this.tm = tm;
		this.opts = opts;
		this.guiLine = guiLine;
		this.guiArea = guiArea;
		this.source = source;
		this.rmtype = rmtype;
		this.plog = opts.getLogger();
		this.showskip = showskip;
		this.dsOriginal = dsOriginal;
		this.dsCleaned = dsCleaned;
	}

	public void run()
	{
		long threadStart = System.currentTimeMillis();

		long threadId = Thread.currentThread().getId();

		try
		{
			elapsedShowMessage( String.format( "Thread id %02d; Pre-loading all reference tables", threadId ), threadStart, System.currentTimeMillis() );

			String msg = String.format("Thread id %02d; CleaningThread/run(): running for source %s, rmtype %s", threadId, source, rmtype);
			plog.show(msg);
			showMessage(msg, false, true);

			// database connections
			dbconOriginal = new HikariConnection( dsOriginal.getConnection() );
			dbconCleaned  = new HikariConnection( dsCleaned.getConnection() );

			doRenewData( opts.isDbgRenewData(), opts.isDoRenewData(), source, rmtype );                     // GUI cb: Remove previous data
			/*
			doPrepieceSuffix(opts.isDbgPrepieceSuffix(), opts.isDoPrepieceSuffix(), source, rmtype);      // GUI cb: Prepiece, Suffix

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

		int count = tm.removeThread();
		msg = String.format("Thread id %02d; Remaining cleaning threads: %d\n", threadId, count);
		showMessage(msg, false, true);
		System.out.println(msg);
	} // run


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


	/*===< functions corresponding to GUI Cleaning options >==================*/

	/*---< Remove previous data >---------------------------------------------*/

	/**
	 * Remove previous data
	 * @param go
	 * @throws Exception
	 */
	private void doRenewData(boolean debug, boolean go, String source, String rmtype)
		throws Exception
	{
		long threadId = Thread.currentThread().getId();

		String funcname = String.format( "Thread id %02d; doRenewData for source: %s, rmtype: %s", threadId, source, rmtype );

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

		dbconCleaned.runQuery( deleteRegist );
		dbconCleaned.runQuery( deletePerson );

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

			dbconCleaned.runQuery( auincRegist );
			dbconCleaned.runQuery( auincPerson );
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

		elapsedShowMessage( funcname, timeStart, System.currentTimeMillis() );
		showMessage_nl();
	} // doRenewData


}
