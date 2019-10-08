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


	public void runQuery( String query ) throws SQLException
	{
		try( PreparedStatement pstmt = connection.prepareStatement( query ) )
		{
			try( ResultSet rs = pstmt.executeQuery() ) {;}
		}
	}


	public int runQueryUpdate( String query ) throws SQLException
	{
	  	int count;
		try( PreparedStatement pstmt = connection.prepareStatement( query ) )
		{
			try( ResultSet rs = pstmt.executeQuery() ) { count = pstmt.getUpdateCount(); }
		}
		return count;
	}


	public ResultSet runQueryWithResult( String query ) throws SQLException
	{
		ResultSet resultSet;
		try( PreparedStatement pstmt = connection.prepareStatement( query ) )
		{
			try( ResultSet rs = pstmt.executeQuery() ) {
				resultSet = rs;
			}
		}
		return resultSet;
	}

}

// [eof]
