package connectors;

import java.sql.*;

/**
 * @author Fons Laan
 *
 * Take an java.sql.Connection object (from an Hikari DataSource), and add several convenience methods.
 * We call this class HikariConnection because we use it it in combination with Hikari Connection Pooling.
 *
 * FL-08-Oct-2019 Created
 * FL-28-Oct-2019 Changed
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
		//this.connection.commit();
	}


	/**
	* @throws SQLException
	*/
	public void close()
	throws SQLException
	{
		connection.close();
	}

	/**
	 * @throws SQLException
	 */
	public void showMetaData( String connectionName )
	throws SQLException
	{
		System.out.printf( "showMetaData() %s\n", connectionName );
		DatabaseMetaData meta = connection.getMetaData();

		System.out.printf( "DatabaseProductName: %s\n", meta.getDatabaseProductName() );
		System.out.printf( "DatabaseProductVersion: %s\n", meta.getDatabaseProductVersion() );
		System.out.printf( "URL: %s\n", meta.getURL() );
		System.out.printf( "ReadOnly: %s\n", meta.isReadOnly() );

		int networkTimeout = connection.getNetworkTimeout();
		System.out.printf( "NetworkTimeout: %d (milliseconds)\n", networkTimeout );
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
	 * INSERT INTO table
	 * @param tableName Table name
	 * @param fields fields
	 * @param data field data
	 * @throws SQLException When insertion fails
	 */
	public int insertIntoTable( String tableName, String[] fields, String[] data )
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

		int count = runQueryUpdate( query );
		return count;
	}


	/**
	 * INSERT INTO table, ignoring duplicates
	 *
	 * @param tableName Table name
	 * @param fields fields
	 * @param data field data
	 * @throws SQLException When insertion fails
	 */
	public int insertIntoTableIgnore( String tableName, String[] fields, String[] data )
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

		int count = runQueryUpdate( query );
		return count;
	}


	/**
	 * TODO rename to executeQuery
	 * @param query
	 * @throws SQLException
	 */
	/*
	public void runQuery( String query )
	throws SQLException
	{
		// PreparedStatement and ResultSet with auto-close syntax
		System.out.printf( "runQuery: %s\n", query );
		try( PreparedStatement pstmt = connection.prepareStatement( query ) )
		{
			try( ResultSet rs = pstmt.executeQuery() ) { ; }
		}
	}
	*/

	/**
	 * TODO rename to executeUpdate
	 * executeUpdate for INSERT , UPDATE or DELETE ... queries
	 * @param query
	 * @return
	 * @throws SQLException
	 */
	public int runQueryUpdate( String query )
	throws SQLException
	{
		// PreparedStatement and ResultSet with auto-close syntax
		System.out.printf( "runQueryUpdate: %s\n", query );
	  	int count;
		try( PreparedStatement pstmt = connection.prepareStatement( query ) )
		{
			count = pstmt.executeUpdate();
		}
		return count;
	}


	/**
	 * TODO rename to executeQueryWithResult
	 * executeQuery for SELECT queries
	 * @param query
	 * @return
	 * @throws SQLException
	 */
	public ResultSet runQueryWithResult( String query )
	throws SQLException
	{
		// PreparedStatement with auto-close syntax
		// ResultSet returned open (of course)
		System.out.printf( "runQueryWithResult: %s\n", query );
		ResultSet rs;
		try( PreparedStatement pstmt = connection.prepareStatement( query ) )
		{
			rs = pstmt.executeQuery();
		}
		return rs;
	}

}

// [eof]
