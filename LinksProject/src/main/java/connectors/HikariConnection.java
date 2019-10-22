package connectors;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

/**
 * @author Fons Laan
 *
 * Take an java.sql.Connection object (from an Hikari DataSource), and add several convenience methods.
 * We call this class HikariConnection because we use it it in combination with Hikari Connection Pooling.
 *
 * FL-08-Oct-2019 Created
 * FL-22-Oct-2019 Changed
 */

public class HikariConnection
{
	private Connection connection;

	/**
	 * Constructor
	 * @param connection
	 */
	public HikariConnection( Connection connection )
	{
		this.connection = connection;
	}


	/**
	* @throws SQLException
	*/
	public void close() throws SQLException
	{
		connection.close();
	}


	/**
	 * Prepare Statement
	 * @throws SQLException
	 */
	public PreparedStatement prepareStatement( String query )
	throws SQLException
	{
		return connection.prepareStatement( query );
	}


	/**
	 * Use this method to insert data into a certain table
	 * @param tableName Table name
	 * @param fields fields
	 * @param data field data
	 * @throws SQLException When insertion fails
	 */
	public void insertIntoTable( String tableName, String[] fields, String[] data )
	throws SQLException
	{
		//Generate SQL query
		String query = "";

		query += "INSERT INTO " + tableName + "(";

		for( int i = 0; i < fields.length; i++ )
		{
			if( i == ( fields.length - 1 ) ) { query += fields[ i ] + ") VALUES("; }
			else { query += fields[ i ] + ","; }
		}

		for( int i = 0; i < data.length; i++ )
		{
			if( i == ( data.length - 1 ) ) { query += "'" + data[i] + "')"; }
			else { query += "'" + data[i] + "',"; }
		}

		runQuery( query );
	}


	/**
	 * Use this method to insert data into a certain table, ignoring duplicates
	 *
	 * @param tableName Table name
	 * @param fields fields
	 * @param data field data
	 * @throws SQLException When insertion fails
	 */
	public void insertIntoTableIgnore( String tableName, String[] fields, String[] data )
	throws SQLException
	{
		// Ignore duplicates when writing to a table with UNIQUE key[s]

		String query = "INSERT IGNORE INTO " + tableName + "(";

		for( int i = 0; i < fields.length; i++ )
		{
			if( i == ( fields.length - 1 ) ) { query += fields[ i ] + ") VALUES("; }
			else { query += fields[ i ] + ","; }
		}

		for( int i = 0; i < data.length; i++ )
		{
			if( i == ( data.length - 1 ) ) { query += "'" + data[i] + "')"; }
			else { query += "'" + data[i] + "',"; }
		}

		runQuery( query );
	}


	/**
	 * @param query
	 * @throws SQLException
	 */
	public void runQuery( String query )
	throws SQLException
	{
		// PreparedStatement and ResultSet with auto-close syntax
		try( PreparedStatement pstmt = connection.prepareStatement( query ) )
		{
			try( ResultSet rs = pstmt.executeQuery() ) { ; }
		}
	}


	/**
	 * @param query
	 * @return
	 * @throws SQLException
	 */
	public int runQueryUpdate( String query )
	throws SQLException
	{
		// PreparedStatement and ResultSet with auto-close syntax
	  	int count;
		try( PreparedStatement pstmt = connection.prepareStatement( query ) )
		{
			try( ResultSet rs = pstmt.executeQuery() ) { count = pstmt.getUpdateCount(); }
		}
		return count;
	}


	/**
	 * @param query
	 * @return
	 * @throws SQLException
	 * PreparedStatement auto closed
	 * ResultSet returned open
	 */
	public ResultSet runQueryWithResult( String query )
	throws SQLException
	{
		// PreparedStatement with auto-close syntax
		// ResultSet returned open (of course)
		ResultSet rs;
		try( PreparedStatement pstmt = connection.prepareStatement( query ) )
		{
			rs = pstmt.executeQuery();
		}
		return rs;
	}

}

// [eof]
