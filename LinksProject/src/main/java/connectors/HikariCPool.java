package connectors;

//import java.util.Enumeration;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import java.util.logging.Logger;

//import java.util.Properties;

/**
 * @author Fons Laan
 *
 * https://www.developer.com/java/data/understanding-jdbc-connection-pooling.html
 *
 * If you need to connect to many different databases at the same time then you should use a different connection pool
 * for each database. It just doesn't make any sense to use the same pool for different databases as a connection to
 * one database will never be able to be re-used for another database.
 *
 * FL-12-Mar-2019 Created
 * FL-15-Apr-2019 Now using mariadb-java-client instead of mysql-connector-java
 * FL-06-Aug-2019
 */
public class HikariCPool
{
    private static final String DEFAULT_DRIVER = "org.mariadb.jdbc.Driver";
    private static final String DEFAULT_HOST   = "localhost";
    private static final String DEFAULT_PORT   = "3306";

    private static HikariDataSource ds;                     // general purpose
    private static HikariDataSource poolLog;
    private static HikariDataSource poolRefRead;
    private static HikariDataSource poolRefWrite;
    private static HikariDataSource poolOriginal;
    private static HikariDataSource poolCleaned;

    private static int ds_count = 0;
    private static int ds_links_match_count = 0;
    private static int ds_links_prematch_count = 0;
    private static int ds_links_temp_count = 0;

    private static int maximumPoolSize = 10;                // default
    private static String configPathname = null;

    private static String db_host = DEFAULT_HOST;
    private static String db_port = DEFAULT_PORT;
    private static String db_user = null;
    private static String db_pass = null;

    private static Logger logger = Logger.getLogger( HikariCPool.class.getName() );

    public HikariCPool( int maximumPoolSize, String configPathname, String db_host, String db_user, String db_pass )
    {
        String fname = "HikariCP/HikariCPool()";
        logger.info( fname );
        if( configPathname != null && ! configPathname.isEmpty() ) { logger.info( configPathname ); }

        this.maximumPoolSize = maximumPoolSize;
        this.configPathname  = configPathname;

        this.db_host = db_host;
        this.db_user = db_user;
        this.db_pass = db_pass;
    }


    public static HikariConfig getConfig( String db_name )
    {
        //String fname = "HikariCP/getConfig()";
        //logger.info( fname );

        //https://stackoverflow.com/questions/6865538/solving-a-communications-link-failure-with-jdbc-and-mysql/21717674
        //String options = "?autoReconnect=true&failOverReadOnly=false&maxReconnects=10";
        // brettwooldridge: Don't use autoReconnect, it is not meant for pools.

        //String jdbc_url = "jdbc:mysql://" + db_host + ":" + db_port + "/" + db_name + options;
        //String jdbc_url = "jdbc:mysql://" + db_host + ":" + db_port + "/" + db_name;      // Maven: mysql-connector-java
        String jdbc_url = "jdbc:mariadb://" + db_host + ":" + db_port + "/" + db_name;      // Maven: mariadb-java-client
        logger.info( jdbc_url );

        // Does not yet work...?
        //HikariConfig config = new HikariConfig( configFilename );

        HikariConfig config = new HikariConfig();

        config.setDriverClassName( DEFAULT_DRIVER );    // Maven: mariadb-java-client
        config.setJdbcUrl( jdbc_url );
        config.setUsername( db_user );
        config.setPassword( db_pass );
        config.setAutoCommit( false );                  // https://mariadb.com/kb/en/library/about-mariadb-connector-j/

        String poolName = "HikariPool-" + db_name;
        config.setPoolName( poolName );

        config.setMaximumPoolSize( maximumPoolSize );
        // timings in milliseconds
        //config.setMinimumIdle( 0 );
        config.setIdleTimeout( 60000 );             // 1 min

        //config.setConnectionTimeout( 3600000 );     // 1 hour
        //config.setConnectionTimeout( 7200000 );     // 2 hours
        //config.setConnectionTimeout( 86400000 );     // 1 day
        config.setConnectionTimeout( 604800000 );     // 1 week

        // Debug problems:
        // https://github.com/brettwooldridge/HikariCP/issues/1111
        config.setLeakDetectionThreshold( 60 * 1000 );

        // See: https://github.com/brettwooldridge/HikariCP/wiki/MySQL-Configuration           // defaults:
        // and: https://dev.mysql.com/doc/connector-j/8.0/en/connector-j-reference-configuration-properties.html
        config.addDataSourceProperty( "cachePrepStmts",           true );   // false
        config.addDataSourceProperty( "cacheResultSetMetadata",   true );   // false
        config.addDataSourceProperty( "cacheServerConfiguration", true );   // false
        config.addDataSourceProperty( "elideSetAutoCommits",      true );   // false
        config.addDataSourceProperty( "maintainTimeStats",        false );  // true
        config.addDataSourceProperty( "prepStmtCacheSize",        250 );    // 25
        config.addDataSourceProperty( "prepStmtCacheSqlLimit",    2048 );   // 256
        config.addDataSourceProperty( "rewriteBatchedStatements", true );   // false
        config.addDataSourceProperty( "useServerPrepStmts",       true );   // false
        config.addDataSourceProperty( "useLocalSessionState",     true );   //  false

        config.addDataSourceProperty( "tcpKeepAlive", true);

        return config;
    }


    public static void showConfig( HikariConfig config )
    {
        //String fname = "HikariCP/showConfig()";
        //logger.info( fname );

        String poolName = config.getPoolName();
        String msg = String.format( "DataSource PoolName: %s", poolName );
        logger.info( msg );

        int maximumPoolSize = config.getMaximumPoolSize();
        msg = String.format( "maximumPoolSize: %d", maximumPoolSize );
        logger.info( msg );
        // ... other params from config

        /*
        Properties properties = config.getDataSourceProperties();
        Enumeration<?> enumeration = properties.propertyNames();
        while( enumeration.hasMoreElements() ) {
            Object object = enumeration.nextElement();
            String name = object.toString();
            String value = properties.getProperty( name );      // why only null values?
            msg = String.format( "%s: %d", name, value );
            logger.info( msg );
        }
        */
    }


    public static HikariDataSource getDataSource( String db_name )
    {
        //String fname = "HikariCP/getDataSource()";
        //logger.info( fname );

        HikariConfig config = getConfig( db_name );

        HikariDataSource dataSource = new HikariDataSource( config );

        showConfig( config );

        return dataSource;
    }

}

// [eof]
