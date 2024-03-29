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
 * FL-30-Dec-2019 Removing most instances of static!
 */
public class HikariCPool
{
    private static final String DEFAULT_DRIVER = "org.mariadb.jdbc.Driver";
    private static final String DEFAULT_HOST   = "localhost";
    private static final String DEFAULT_PORT   = "3306";

    private int maximumPoolSize = 10;				// default
	private boolean autoCommit = true;				// default: true. Since 2.2.0
    private String configPathname = null;

    private String db_host = DEFAULT_HOST;
    private String db_port = DEFAULT_PORT;
    private String db_user = null;
    private String db_pass = null;

    private Logger logger = Logger.getLogger( HikariCPool.class.getName() );

	/**
	 * Constructor
	 * @param maximumPoolSize
	 * @param autoCommit
	 * @param configPathname
     * @param poolName
	 * @param db_host
	 * @param db_user
	 * @param db_pass
	 */
    public HikariCPool( int maximumPoolSize, boolean autoCommit, String configPathname, String poolName, String db_host, String db_user, String db_pass )
    {
        String fname = "HikariCP/HikariCPool()";
        String msg = String.format( "%s: poolName: %s, db_host: %s", fname, poolName, db_host );
        logger.info( msg );

        if( configPathname != null && ! configPathname.isEmpty() ) { logger.info( configPathname ); }

        this.maximumPoolSize = maximumPoolSize;
        this.autoCommit = autoCommit;
        this.configPathname  = configPathname;

        this.db_host = db_host;
        this.db_user = db_user;
        this.db_pass = db_pass;
    }


    public HikariConfig getConfig( String db_name )
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

        // Notice: with "config.setAutoCommit( false )",  the "pstmt.executeUpdate();" in HikariConnection.java did nothing.
        // Then apparently we need an additional "connection.commit()" after executeUpdate()
        config.setAutoCommit( autoCommit );			// Default: true. Since 2.2.0; see https://mariadb.com/kb/en/library/about-mariadb-connector-j/

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
        config.addDataSourceProperty( "useLocalSessionState",     true );   // false

        config.addDataSourceProperty( "tcpKeepAlive", true);

        return config;
    }


    public void showConfig( HikariConfig config )
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


    public HikariDataSource getDataSource( String db_name )
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
