

package linksmatchmanager;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;

import java.util.Arrays;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.mchange.v2.c3p0.ComboPooledDataSource;       // c3p0 - JDBC3 Connection and Statement Pooling


/**
 * @author Fons Laan
 *
 * FL-08-Jan-2018
 * started with the code from:
 * https://stackoverflow.com/questions/4059004/why-is-java-sql-drivermanager-getconnection-hanging
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


    public static Connection getConnection( String db_host, String db_name, String db_user, String db_pass )
    throws ClassNotFoundException, SQLException
    {
        Class.forName( DEFAULT_DRIVER );

        String db_url = "jdbc:mysql://" + db_host + ":" + DEFAULT_PORT + "/" + db_name + "?dontTrackOpenResources=true&autoReconnect=true";

        if( ( db_user == null ) || ( db_pass == null ) || ( db_user.trim().length() == 0 ) || ( db_pass.trim().length() == 0 ) )
        {
            return DriverManager.getConnection( db_url );
        }
        else
        {
            return DriverManager.getConnection( db_url, db_user, db_pass );
        }
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
