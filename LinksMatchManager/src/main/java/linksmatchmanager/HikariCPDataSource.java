package linksmatchmanager;

import java.sql.Connection;
import java.sql.SQLException;

import java.util.logging.Logger;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

/**
 * @author Fons Laan
 *
 * https://www.developer.com/java/data/understanding-jdbc-connection-pooling.html
 *
 * If you need to connect to many different databases at the same time then you should use a different connection pool
 * for each database. It just doesn't make any sense to use the same pool for different databases as a connection to
 * one database will never be able to be re-used for another database.
 *
 * FL-08-Mar-2019
 * FL-10-Mar-2019
 */
public class HikariCPDataSource
{
    private static final String DEFAULT_DRIVER = "com.mysql.jdbc.Driver";     // current
    private static final String DEFAULT_HOST   = "localhost";
    private static final String DEFAULT_PORT   = "3306";

    private static Logger logger = Logger.getLogger( HikariCPDataSource.class.getName() );

    private static HikariDataSource ds;                 // general purpose
    private static HikariDataSource ds_links_match;
    private static HikariDataSource ds_links_prematch;
    private static HikariDataSource ds_links_temp;

    private static int ds_count = 0;
    private static int ds_links_match_count = 0;
    private static int ds_links_prematch_count = 0;
    private static int ds_links_temp_count = 0;

    // Notice: in case of the mysql error "ERROR 1040 (08004): Too many connections",
    // - increase the value of the mysql variable max_connections (default 151 in our case) in the config file, or
    // - decrease the requested number of connections, via the HikariCPD maximumPoolSize (default 10)
    // max_connections; 151 => 200
    // maximumPoolSize: // default: 10 => 24 => 48 => 64 => 32
    private static int maximumPoolSize = 32;

    /*
    See: https://github.com/brettwooldridge/HikariCP/wiki/MySQL-Configuration
    and: https://dev.mysql.com/doc/connector-j/8.0/en/connector-j-reference-configuration-properties.html

    A typical MySQL configuration for HikariCP might look something like this:
    jdbcUrl=jdbc:mysql://localhost:3306/simpsons
    user = test
    password = test

    dataSource.cachePrepStmts = true
    dataSource.prepStmtCacheSize = 250
    dataSource.prepStmtCacheSqlLimit = 2048
    dataSource.useServerPrepStmts = true
    dataSource.useLocalSessionState = true
    dataSource.rewriteBatchedStatements = true
    dataSource.cacheResultSetMetadata = true
    dataSource.cacheServerConfiguration = true
    dataSource.elideSetAutoCommits = true
    dataSource.maintainTimeStats = false
    */

    public static Connection getConnection( String db_host, String db_name, String db_user, String db_pass )
    throws ClassNotFoundException, SQLException
    {
        String fname = "HikariCPDataSource/getConnection()";
        logger.info( fname );
        //String msg = String.format( "%s() db_user: %s, db_pass: %s ", fname, db_user, db_pass );

        //https://stackoverflow.com/questions/6865538/solving-a-communications-link-failure-with-jdbc-and-mysql/21717674
        String options = "?autoReconnect=true&failOverReadOnly=false&maxReconnects=10";

        String jdbc_url = "jdbc:mysql://" + db_host + ":" + DEFAULT_PORT + "/" + db_name + options;
        logger.info( jdbc_url );

        HikariConfig config = new HikariConfig();

        config.setJdbcUrl( jdbc_url );
        config.setUsername( db_user );
        config.setPassword( db_pass );

        config.addDataSourceProperty( "cachePrepStmts",           true );
        config.addDataSourceProperty( "cacheResultSetMetadata",   true );
        config.addDataSourceProperty( "cacheServerConfiguration", true );
        config.addDataSourceProperty( "elideSetAutoCommits",      true );
        config.addDataSourceProperty( "maintainTimeStats",        false );
        config.addDataSourceProperty( "maximumPoolSize",          maximumPoolSize );
        config.addDataSourceProperty( "prepStmtCacheSize",        250 );
        config.addDataSourceProperty( "prepStmtCacheSqlLimit",    2048 );
        config.addDataSourceProperty( "rewriteBatchedStatements", true );
        config.addDataSourceProperty( "useServerPrepStmts",       true );
        config.addDataSourceProperty( "useLocalSessionState",     true );

        Connection connection = null;

        if( db_name.equals( "links_match" ) )
        {
            ds_links_match = new HikariDataSource( config );
            connection = ds_links_match.getConnection();
            ds_links_match_count += 1;
            logger.info( String.format( "ds_links_match_count: %d", ds_links_match_count ) );
        }
        else if( db_name.equals( "links_prematch" ) )
        {
            ds_links_prematch = new HikariDataSource( config );
            connection = ds_links_prematch.getConnection();
            ds_links_prematch_count += 1;
            logger.info( String.format( "ds_links_prematch_count: %d", ds_links_prematch_count ) );
        }

        else if( db_name.equals( "links_temp" ) )
        {
            ds_links_temp = new HikariDataSource( config );
            connection = ds_links_match.getConnection();
            ds_links_temp_count += 1;
            logger.info( String.format( "ds_links_temp_count: %d", ds_links_temp_count ) );
        }
        else
        {
            ds = new HikariDataSource( config );
            connection = ds.getConnection();
            ds_count += 1;
            logger.info( String.format( "ds_count: %d", ds_count ) );
        }

        if( connection == null ) {
            String msg = String.format( "%s: No connection", fname );
            logger.severe( fname );
        }

        return connection;
    }
}

// [eof]
