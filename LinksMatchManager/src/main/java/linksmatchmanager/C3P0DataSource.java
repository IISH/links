package linksmatchmanager;

import java.beans.PropertyVetoException;
import java.sql.Connection;
import java.sql.SQLException;

import com.mchange.v2.c3p0.ComboPooledDataSource;

/**
 * @author Fons Laan
 *
 * FL-08-Jan-2018
 * https://www.developer.com/java/data/understanding-jdbc-connection-pooling.html
 *
 * If you need to connect to many different databases at the same time then you should use a different connection pool
 * for each database. It just doesn't make any sense to use the same pool for different databases as a connection to
 * one database will never be able to be re-used for another database.
 */
public class C3P0DataSource
{
    private static C3P0DataSource dataSource;
    private ComboPooledDataSource comboPooledDataSource;

    private C3P0DataSource()
    {
        try
        {
            comboPooledDataSource = new ComboPooledDataSource();
            comboPooledDataSource.setDriverClass( "com.mysql.jdbc.Driver" );
            comboPooledDataSource.setJdbcUrl( "jdbc:mysql://localhost:3306/testdb" );
            comboPooledDataSource.setUser( "root" );
            comboPooledDataSource.setPassword( "secret" );
        }
        catch( PropertyVetoException ex ) { ex.printStackTrace(); }
    }


    public static C3P0DataSource getInstance()
    {
        if ( dataSource == null ) { dataSource = new C3P0DataSource(); }
        return dataSource;
    }


    public Connection getConnection()
    {
        Connection connection = null;
        try { connection = comboPooledDataSource.getConnection(); }
        catch( SQLException ex ) { ex.printStackTrace(); }
        return connection;
   }
}

// [eof]
