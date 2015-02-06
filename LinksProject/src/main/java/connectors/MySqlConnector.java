package connectors;

/*
Copyright (C) IISH (www.iisg.nl)

This program is free software; you can redistribute it and/or modify
it under the terms of version 3.0 of the GNU General Public License as
published by the Free Software Foundation.


This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program; if not, write to the Free Software
Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
 */


import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;

/**
 * This class contains functions to connect and 
 * export data to a MySql Database.
 * 
 * @author Omar Azouguagh
 * @author Fons Laan
 *
 * FL-30-Jun-2014 Imported from OA backup
 * FL-06-Feb-2015 Latest change
 */
public class MySqlConnector
{
    private Connection conn;    // Connection Object

    /**
     * Contructor,
     * This constructor connects to the given databas
     * @param url Database Location
     * @param db Database Name
     * @param user Database Username
     * @param pass Database Password
     * @throws Exception When connection fails
     */
    public MySqlConnector(String url, String db, String user, String pass)
            throws Exception {
        conn = connect(url, db, user, pass);
        
        // Set readonly
        conn.isReadOnly();
    }

    /**
     * This method returns a connection object
     * for further tasks
     * @param url Database Location
     * @param db Database Name
     * @param user Database Username
     * @param pass Database Password
     * @return Database object that can be used for database tasks
     * @throws Exception When Connection fails
     */
    private Connection connect(String url, String db, String user, String pass) throws Exception {
        String driver = "org.gjt.mm.mysql.Driver";
        String loc = "jdbc:mysql://" + url + "/" + db + "?dontTrackOpenResources=true";
        String username = user;
        String password = pass;

        Class.forName(driver);
        // Class.forName("externalModules.jdbcDriver.Driver").newInstance();
        return DriverManager.getConnection(loc, username, password);
    }

    /**
     * Close connection to MySQL
     * @throws Exception If connection closing fails
     */
    public void close() throws Exception {
        conn.close();
    }

    /**
     * Create table with one ID field and custom columns (varchar)
     * @param tableName Table name
     * @param fields Field to create
     * @param maxVarcharSize Max size of field
     */
    public void createTable(
            String tableName, String[] fields, int maxVarcharSize)
            throws Exception {

        /**
         * Generate SQL query
         */
        String query = "";

        query += "CREATE TABLE `" + tableName.trim() + "` ("
                + "`EXID` INTEGER NOT NULL AUTO_INCREMENT,";

        for (int i = 0; i < fields.length; i++) {
            query += "`" + fields[i].trim() + "` "
                    + "VARCHAR(" + maxVarcharSize + "),";
        }

        query += "PRIMARY KEY (`EXID`)";
        query += ")";
        query += "ENGINE = MyISAM ";


        /*
         * Run query on Database
         */
        conn.createStatement().execute(query);
        conn.createStatement().close();

    }


    /**
     * Use this method to insert data into a certain table
     * @param tableName Table name
     * @param fields fields
     * @param data field data
     * @throws Exception When insertion fails
     */
    public void insertIntoTable( String tableName, String[] fields, String[] data )
    throws Exception
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

        // Execute Query
        conn.createStatement().execute(query);
        conn.createStatement().close();
    }


    /**
     * Use this method to insert data into a certain table, ignoring duplicates
     *
     * @param tableName Table name
     * @param fields fields
     * @param data field data
     * @throws Exception When insertion fails
     */
    public void insertIntoTableIgnore( String tableName, String[] fields, String[] data )
    throws Exception
    {
        // Ignore duplicates when writing to a table with UNIQUE key[s]

        // Generate SQL query
        String query = "";

        query += "INSERT IGNORE INTO " + tableName + "(";

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

        // Execute Query
        conn.createStatement().execute(query);
        conn.createStatement().close();
    }


    /**
     * Run query without ResultSet
     * @param query
     * @throws Exception
     */
    public void runQuery(String query) throws Exception {
        conn.createStatement().execute(query);
        conn.createStatement().close();
    }

    /**
     * This method Executes a query 
     * and returns the ResultSet
     * @param query Query to run on database
     * @return ResultSet ResultSet with results
     * @throws Exception When execution of query fails
     */
    public ResultSet runQueryWithResult(String query) throws Exception {
        ResultSet rs = conn.createStatement().executeQuery(query);
        conn.createStatement().close();
        return rs;
    }

    /**
     * This method checks if a table excests
     * @param TableName Table name
     * @return True if table does exists
     * @throws Exception When check goes wrong
     */
    private boolean tableExsists(String TableName) throws Exception {

        // Get meta data from database
        DatabaseMetaData dbm = conn.getMetaData();

        ResultSet rs = dbm.getTables(null, null, TableName, null);

        if (rs.next()) {
            return true;
        }

        return false;
    }
}