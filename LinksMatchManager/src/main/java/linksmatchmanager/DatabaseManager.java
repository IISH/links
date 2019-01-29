

package linksmatchmanager;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;

import java.text.SimpleDateFormat;

import java.util.Arrays;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.mchange.v2.c3p0.ComboPooledDataSource;       // c3p0 - JDBC3 Connection and Statement Pooling


/**
 * @author Fons Laan
 *
 * FL-08-Jan-2018
 * FL-06-Mar-2018 again lost connection, so try again autoReconnect=true
 * started with the code from:
 * https://stackoverflow.com/questions/4059004/why-is-java-sql-drivermanager-getconnection-hanging
 * FL-07-Jan-2019
 * Again "Communications link failure", changed connection options.
 * https://stackoverflow.com/questions/6865538/solving-a-communications-link-failure-with-jdbc-and-mysql/21717674
 */
public class DatabaseManager
{
    /*
    private static final String DEFAULT_DRIVER   = "org.postgresql.Driver";
    private static final String DEFAULT_URL      = "jdbc:postgresql://localhost:5432/party";
    private static final String DEFAULT_USERNAME = "";
    private static final String DEFAULT_PASSWORD = "";
    */

    //private static final String DEFAULT_DRIVER   = "org.gjt.mm.mysql.Driver";   // old
    private static final String DEFAULT_DRIVER   = "com.mysql.jdbc.Driver";     // current
    private static final String DEFAULT_HOST     = "localhost";
    private static final String DEFAULT_PORT     = "3306";
    //private static final String DEFAULT_DBNAME   = "party";
    //private static final String DEFAULT_URL      = "jdbc:mysql://localhost:3306/party";
    //private static final String DEFAULT_URL      = "jdbc:mysql://" + DEFAULT_HOST + ":" + DEFAULT_PORT + "/" + DEFAULT_DBNAME;
    //private static final String DEFAULT_USERNAME = "";
    //private static final String DEFAULT_PASSWORD = "";

    /*
    public static void main(String[] args)
    {
        long begTime = System.currentTimeMillis();

        String driver = ((args.length > 0) ? args[0] : DEFAULT_DRIVER);
        String url = ((args.length > 1) ? args[1] : DEFAULT_URL);
        String username = ((args.length > 2) ? args[2] : DEFAULT_USERNAME);
        String password = ((args.length > 3) ? args[3] : DEFAULT_PASSWORD);

        Connection connection = null;

        try
        {
            connection = createConnection(driver, url, username, password);
            DatabaseMetaData meta = connection.getMetaData();
            System.out.println(meta.getDatabaseProductName());
            System.out.println(meta.getDatabaseProductVersion());

            String sqlQuery = "SELECT PERSON_ID, FIRST_NAME, LAST_NAME FROM PERSON ORDER BY LAST_NAME";
            System.out.println("before insert: " + query(connection, sqlQuery, Collections.EMPTY_LIST));

            connection.setAutoCommit(false);
            String sqlUpdate = "INSERT INTO PERSON(FIRST_NAME, LAST_NAME) VALUES(?,?)";
            List parameters = Arrays.asList( "Foo", "Bar" );
            int numRowsUpdated = update(connection, sqlUpdate, parameters);
            connection.commit();

            System.out.println("# rows inserted: " + numRowsUpdated);
            System.out.println("after insert: " + query(connection, sqlQuery, Collections.EMPTY_LIST));
        }
        catch (Exception e)
        {
            rollback(connection);
            e.printStackTrace();
        }
        finally
        {
            close(connection);
            long endTime = System.currentTimeMillis();
            System.out.println("wall time: " + (endTime - begTime) + " ms");
        }
    }
    */


    /**
     * @param format
     * @return
     */
    public static String getTimeStamp2( String format ) {
        Calendar cal = Calendar.getInstance();
        SimpleDateFormat sdf = new SimpleDateFormat( format );
        return sdf.format( cal.getTime() );
    }

    /*
    public static Connection createConnection( String driver, String url, String username, String password )
    throws ClassNotFoundException, SQLException
    {
        Class.forName( driver );

        if( (username == null ) || ( password == null ) || ( username.trim().length() == 0 ) || ( password.trim().length() == 0 ) )
        {
            return DriverManager.getConnection( url );
        }
        else
        {
            return DriverManager.getConnection( url, username, password );
        }
    }
    */

    public static Connection getConnection( String db_host, String db_name, String db_user, String db_pass )
    throws ClassNotFoundException, SQLException
    {
        /*
        dontTrackOpenResources
        The JDBC specification requires the driver to automatically track and close resources, however if your application doesn't do a good job of explicitly calling close() on statements or result sets, this can cause memory leakage. Setting this property to true relaxes this constraint, and can be more memory efficient for some applications. Also the automatic closing of the Statement and current ResultSet in Statement.closeOnCompletion() and Statement.getMoreResults ([Statement.CLOSE_CURRENT_RESULT | Statement.CLOSE_ALL_RESULTS]), respectively, ceases to happen. This property automatically sets holdResultsOpenOverStatementClose=true.
        Default: false
        Since version: 3.1.7

        autoReconnect
        Should the driver try to re-establish stale and/or dead connections? If enabled the driver will throw an exception for a queries issued on a stale or dead connection, which belong to the current transaction, but will attempt reconnect before the next query issued on the connection in a new transaction. The use of this feature is not recommended, because it has side effects related to session state and data consistency when applications don't handle SQLExceptions properly, and is only designed to be used when you are unable to configure your application to handle SQLExceptions resulting from dead and stale connections properly. Alternatively, as a last option, investigate setting the MySQL server variable "wait_timeout" to a high value, rather than the default of 8 hours.
        Default: false
        Since version: 1.1

        maxReconnects
        Maximum number of reconnects to attempt if autoReconnect is true, default is '3'.
        Default: 3
        Since version: 1.1
        */

        Class.forName( DEFAULT_DRIVER );

        long threadId = Thread.currentThread().getId();

        // 06-Mar-2018: again lost connection, so try again autoReconnect=true
        //String options = "?dontTrackOpenResources=true&autoReconnect=true";
        // 07-Jan-2019: again lost connection with autoReconnect=true; change options string, see:
        //https://stackoverflow.com/questions/6865538/solving-a-communications-link-failure-with-jdbc-and-mysql/21717674
        String options = "?autoReconnect=true&failOverReadOnly=false&maxReconnects=10";

        //String db_url = "jdbc:mysql://" + db_host + ":" + DEFAULT_PORT + "/" + db_name;
        String db_url = "jdbc:mysql://" + db_host + ":" + DEFAULT_PORT + "/" + db_name + options;

        //String format = "HH:mm:ss";
        String format = "yyyy.MM.dd HH:mm:ss";
        String ts = getTimeStamp2( format );
        String msg = String.format( "%s Thread id %02d; db_url: %s", ts, threadId, db_url );
        System.out.println( msg );

        Connection connection;

        if( ( db_user == null ) || ( db_pass == null ) || ( db_user.trim().length() == 0 ) || ( db_pass.trim().length() == 0 ) )
        { connection = DriverManager.getConnection( db_url ); }
        else
        { connection = DriverManager.getConnection( db_url, db_user, db_pass ); }

        ts = getTimeStamp2( format );
        msg = String.format( "%s Thread id %02d; connection created", ts, threadId );
        System.out.println( msg );

        return connection;
    }


    public static void close( Connection connection )
    {
        try
        {
            if( connection != null )
            {
                connection.close();
            }
        }
        catch( SQLException ex )
        {
            ex.printStackTrace();
        }
    }


    public static void close( Statement st )
    {
        try
        {
            if( st != null )
            {
                st.close();
            }
        }
        catch( SQLException ex )
        {
            ex.printStackTrace();
        }
    }


    public static void close( ResultSet rs )
    {
        try
        {
            if( rs != null )
            {
                rs.close();
            }
        }
        catch( SQLException ex )
        {
            ex.printStackTrace();
        }
    }


    public static void rollback( Connection connection )
    {
        try
        {
            if( connection != null )
            {
                connection.rollback();
            }
        }
        catch( SQLException ex )
        {
            ex.printStackTrace();
        }
    }


    public static List< Map<String, Object> > map( ResultSet rs )
    throws SQLException
    {
        List< Map<String, Object> > results = new ArrayList< Map<String, Object> >();

        try
        {
            if( rs != null )
            {
                ResultSetMetaData meta = rs.getMetaData();
                int numColumns = meta.getColumnCount();
                while( rs.next() )
                {
                    Map<String, Object> row = new HashMap<String, Object>();
                    for( int i = 1; i <= numColumns; ++i )
                    {
                        String name  = meta.getColumnName( i );
                        Object value = rs.getObject( i );
                        row.put( name, value );
                    }
                    results.add( row );
                }
            }
        }
        finally
        {
            close( rs );
        }

        return results;
    }


    public static List< Map<String, Object> > query( Connection connection, String sql, List<Object> parameters )
    throws SQLException
    {
        List< Map<String, Object> > results = null;

        PreparedStatement ps = null;
        ResultSet rs = null;

        try
        {
            ps = connection.prepareStatement( sql );

            int i = 0;
            for( Object parameter : parameters )
            {
                ps.setObject( ++i, parameter );
            }

            rs = ps.executeQuery();
            results = map( rs );
        }
        finally
        {
            close( rs );
            close( ps );
        }

        return results;
    }


    public static int update( Connection connection, String sql, List<Object> parameters )
    throws SQLException
    {
        int numRowsUpdated = 0;

        PreparedStatement ps = null;

        try
        {
            ps = connection.prepareStatement( sql );

            int i = 0;
            for( Object parameter : parameters )
            {
                ps.setObject( ++i, parameter );
            }

            numRowsUpdated = ps.executeUpdate();
        }
        finally
        {
            close( ps );
        }

        return numRowsUpdated;
    }
}

// [eof]
