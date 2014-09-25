package dataset;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Set;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.HashMultiset;

import connectors.MySqlConnector;

import modulemain.LinksSpecific;

/**
 * @author Fons Laan
 *
 * FL-25-Sep-2014 Latest change
 */
public class TabletoArrayListMultimap
{
    private boolean debug = false;

    private boolean check_duplicates  = false;
    private boolean delete_duplicates = false;   // only used with check_duplicates = true

    // A hashed multimap to store key and values for a reference table for fast lookups.
    private Multimap< String, String > oldMap;

    // A hashed set to store new originals.
    private HashMultiset< String > newSet;

    private String tableName;
    private String keyColumn;           // column name used as index
    private String standardColumn;      // column name used as standard (mostly "standard", but "location_no" for ref_location

    ArrayList< String > columnNames;
    ArrayList< String > valueNames;

    private int numColumns;
    private int numRows;

    int originalIdx;
    int standardIdx;
    int standardCodeIdx;

    private MySqlConnector conn_read;
    private MySqlConnector conn_write;

    ResultSet rs;

    /**
     * Constructor to load a reference table into an ArrayListMultiMap
     *
     * "con_write" is only used in the 2 functions updateTable() and updateTableWithCode().
     *
     * @param conn_read         // "conn_read"  is normally conGeneral, i.e. the local links_general db
     * @param conn_write        // "conn_write" is normally conOr, i.e. the remote links_general db (on node-030)
     * @param tableName
     * @param keyColumn
     */
    public TabletoArrayListMultimap
    (
            MySqlConnector conn_read,
            MySqlConnector conn_write,
            String tableName,
            String keyColumn,
            String standardColumn
    )
    throws Exception
    {
        this.conn_read      = conn_read;
        this.conn_write     = conn_write;
        this.tableName      = tableName;
        this.keyColumn      = keyColumn;
        this.standardColumn = standardColumn;

        if( debug ) { System.out.println( "TabletoArrayListMultimap, table name: " +
            tableName + " , index column: " + keyColumn + ", standard column: " + standardColumn ); }

        oldMap = ArrayListMultimap.create();
        newSet = HashMultiset.create();

        columnNames = new ArrayList();
        valueNames  = new ArrayList();

        // invalid values
        originalIdx     = -1;
        standardIdx     = -1;
        standardCodeIdx = -1;

        /*
        ref_role:
        +-----------------+------------------+------+-----+---------+----------------+
        | Field           | Type             | Null | Key | Default | Extra          |  ct
        +-----------------+------------------+------+-----+---------+----------------+
        | id_role         | int(10) unsigned | NO   | PRI | NULL    | auto_increment |  4
        | original        | varchar(60)      | YES  | MUL | NULL    |                |  12
        | standard        | varchar(60)      | YES  | MUL | NULL    |                |  12
        | role_nr         | int(10) unsigned | YES  | MUL | NULL    |                |  4
        | standard_code   | char(1)          | YES  |     | NULL    |                |  1
        | standard_source | varchar(20)      | YES  |     | NULL    |                |  12
        +-----------------+------------------+------+-----+---------+----------------+

        Be aware that getColumnType just returns an integer, so you'll need to know what that integer means.
        According to the JDK docs, the integer returned corresponds to generic SQL data types as follows:
              -7    BIT
              -6    TINYINT
              -5    BIGINT
              -4    LONGVARBINARY
              -3    VARBINARY
              -2    BINARY
              -1    LONGVARCHAR
               0    NULL
               1    CHAR
               2    NUMERIC
               3    DECIMAL
               4    INTEGER
               5    SMALLINT
               6    FLOAT
               7    REAL
               8    DOUBLE
              12    VARCHAR
              91    DATE
              92    TIME
              93    TIMESTAMP
            1111    OTHER

        i: 1, ct: 4, cn: id_role
        i: 2, ct: 12, cn: original
        i: 3, ct: 12, cn: standard
        i: 4, ct: 4, cn: role_nr
        i: 5, ct: 1, cn: standard_code
        i: 6, ct: 12, cn: standard_source
        */

        // check column types
        /*
        for( int i = 1; i <= numCols; i++ )     // process each column
        {
            int ct = rsmd.getColumnType( i );
            String cn = rsmd.getColumnName( i );
            System.out.println( "i: " + i + ", ct: " + ct + ", cn: " + cn );
        }
        */

        String query = "";
        if( check_duplicates ) { query = "SELECT * FROM `" + tableName + "` ORDER BY `" + keyColumn + "` ASC"; }
        else { query = "SELECT * FROM `" + tableName + "`"; }

        if( debug ) { System.out.println( "TabletoArrayListMultimap, query: " + query ); }
        ResultSet rs = conn_read.runQueryWithResult( query );

        ResultSetMetaData rs_md = rs.getMetaData();
        numColumns = rs_md.getColumnCount();
        columnNames = new ArrayList();              // excluding the key column !
        int skip = 0;

        for( int i = 0; i < numColumns; i++ )
        {
            int c = i + 1;                          // MySQL starts at 1

            String columnName = rs_md.getColumnName( c );
            if( columnName.equals( "original" ) ) { skip = 1; }
            else { valueNames.add( columnName ); }

            columnNames.add( columnName );

            if( columnName.equals( "original" ) )      { originalIdx     = i - skip; }
            if( columnName.equals( standardColumn ) )  { standardIdx     = i - skip; }
            if( columnName.equals( "standard_code" ) ) { standardCodeIdx = i - skip; }
        }

        //System.out.println( "originalIdx: " + originalIdx );
        //System.out.println( "standardIdx: " + standardIdx );
        //System.out.println( "stdCodeIdx:  " + standardCodeIdx );

        if( check_duplicates ) {
            if( debug ) { tableInfo(); }
            store_check( rs, rs_md );
        }
        else { store( rs, rs_md ); }

        //contentsOld();

    } // TabletoArrayListMultiMap


    /**
     * table and column names
     */
    public void tableInfo() {
        System.out.printf( "\ntable name: %s, key: %s\n", tableName, keyColumn );

        System.out.printf( "value names:\n" );
        for ( String columnName : columnNames ) { System.out.printf( "%s ", columnName ); }
        System.out.println( "\n" );

        System.out.printf( "standard offset: %s, standard_code offset: %s\n", standardIdx, standardCodeIdx );
    }


    /**
     * store table in multimap, and check for duplicates
     */
    public int store_check( ResultSet rs, ResultSetMetaData rs_md )
    throws Exception
    {
        //System.out.println( "TabletoArrayListMultimap/store_check()" );

        int ndups = 0;
        boolean isdup = false;
        numRows = 0;

        while( rs.next() )          // process each index value
        {
            /*
            int  id_role           = rs.getInt(    1 );
            String original        = rs.getString( 2 );     // index
            String standard        = rs.getString( 3 );
            int role_nr            = rs.getInt(    4 );
            String standard_code   = rs.getString( 5 );
            String standard_source = rs.getString( 6 );
            */

            numRows++;
            String key = "";
            String original = "";
            ArrayList< String > values = new ArrayList();

            for( int i = 0; i < numColumns; i++ )   // process each column
            {
                int c = i + 1;                      // MySQL starts at 1

                int ct = rs_md.getColumnType( c );

                String strValue = "";

                if( ct == 4 ) {
                    int intValue = rs.getInt( c );
                    strValue = Integer.toString( intValue );
                }
                else if( ct == 1 || ct ==12 ) {
                    strValue = rs.getString( c );
                }
                else { throw new Exception( "TabletoArrayListMultimap: unhandled column type: " + ct ); }

                String columnName = columnNames.get( i );

                //if( columnName.equals( keyColumn ) )
                if( originalIdx == i )      // faster than string compare
                {
                    original = strValue;
                    // toLowerCase() is needed for Location keys; the others are already lowercase
                    key = original.toLowerCase();

                    if( check_duplicates && oldMap.containsKey( key ) ) { isdup = true; }
                    else { isdup = false; }
                }
                else { values.add( strValue ); }
            }

            if( check_duplicates && isdup ) {
                ndups++;
                Collection< String > collection = oldMap.get( key );
                String[] map_values = collection.toArray( new String[ collection.size() ] );

                System.out.println( "multimap  original: " + key + " : " + Arrays.toString( map_values ) );
                System.out.println( "duplicate original: " + original + " : " + values );

                if( delete_duplicates ) {
                    int id;
                    int mapId = Integer.parseInt( map_values[ 0 ] );
                    int dupId = Integer.parseInt( values.get( 0 ) );
                    //System.out.println( "mapId: " + mapId + ", dupId: " + dupId );

                    // delete highest (latest) id
                    if( dupId > mapId ) { id = dupId; }
                    else { id = mapId; }

                    String column = columnNames.get( 0 );

                    String query = "DELETE FROM `" + tableName + "` WHERE " + column + " = " + Integer.toString( id );
                    System.out.println( query );

                    try { conn_read.runQuery( query ); }
                    catch( SQLException sex )
                    { System.out.println( "SQLException while deleting duplicate: " + sex.getMessage() ); }
                    catch( Exception jex )
                    { System.out.println( "Exception while deleteing duplicate: " + jex.getMessage() ); }
                }
            }
            else
            {
                for( String value : values ) { oldMap.put ( key, value ); }
            }

            values = null;
        }

        if( check_duplicates ) { System.out.printf( "%d duplicate originals\n\n", ndups ); }

        return ndups;
    }


    /**
     * store table in multimap
     */
    public void store( ResultSet rs, ResultSetMetaData rs_md )
    throws Exception
    {
        //System.out.println( "TabletoArrayListMultimap/store()" );

        numRows = 0;

        while( rs.next() )          // process each index value
        {
            /*
            int  id_role           = rs.getInt(    1 );
            String original        = rs.getString( 2 );     // index
            String standard        = rs.getString( 3 );
            int role_nr            = rs.getInt(    4 );
            String standard_code   = rs.getString( 5 );
            String standard_source = rs.getString( 6 );
            */

            numRows++;
            String key = "";
            String original = "";
            ArrayList< String > values = new ArrayList();

            for( int i = 0; i < numColumns; i++ )     // process each column
            {
                int c = i + 1;

                int ct = rs_md.getColumnType( c );

                String strValue = "";

                if( ct == 4 ) {
                    int intValue = rs.getInt( c );
                    strValue = Integer.toString( intValue );
                }
                else if( ct == 1 || ct ==12 ) {
                    strValue = rs.getString( c );
                }
                else { throw new Exception( "TabletoArrayListMultimap: unhandled column type: " + ct ); }

                String columnName = columnNames.get( i );

                //if( columnName.equals( keyColumn ) )
                if( originalIdx == i )      // faster than string compare
                {
                    original = strValue;
                    // toLowerCase() is needed for Location keys; the others are already lowercase
                    key = original.toLowerCase();
                }
                else { values.add( strValue ); }
            }

            for( String value : values ) { oldMap.put ( key, value ); }

            values = null;
        }
    }


    /**
     * Number of keys in the reference table multimap;
     * that number is identical to the number of originals in the reference table
     * But it is NOT the value of the size() function:
     * size() gives the number of key-value pairs.
     * So: size() = num_keys * num values per key.
     */
    public int numkeys()
    {
        //return oldMap.size();       // WRONG !

        Set< String > keys = oldMap.keySet();
        return keys.size();
    }


    /**
     * Sometimes the entries in the original column of the table are not unique;
     * then numkeys != numrows.
     * duplicate originals in the ref tables should be removed.
     * @return
     */
    public int numrows()
    {
        return numRows;
    }


    /**
     * size of new set (to be added to the reference table)
     */
    public int newcount() { return newSet.size(); }


    /**
     * check entry in map and set
     */
    public boolean contains( String entry )
    {
        boolean tf = false;

        try { tf = oldMap.containsKey( entry ); }
        catch( Exception ex ) { System.out.println( ex.getMessage() ); }

        if( ! tf ) {
            try { tf = newSet.contains( entry ); }
            catch( Exception ex ) { System.out.println(ex.getMessage()); }
        }

        return tf;
    }


    /**
     * return standard for existing key, else empty string
     */
    public String standard( String key )
    {
        String standard = "";

        if( oldMap.containsKey( key ) ) {
            Collection< String > collection = oldMap.get( key );
            String[] values = collection.toArray( new String[ collection.size() ] );

            standard = values [ standardIdx ];
        }
        //System.out.println( "standard: " + standard );
        return standard;
    }


    /**
     * return value of specified column for existing key, else empty string
     */
    public String value( String column, String key )
    {
        //System.out.println( "value(): key: " + key + ", column: " + column );
        String value = "";

        if( oldMap.containsKey( key ) ) {
            Collection< String > collection = oldMap.get( key );
            String[] values = collection.toArray( new String[ collection.size() ] );

            for( int i = 0; i < numColumns; i++ ) {
                String columnName = columnNames.get( i );
                //System.out.println( column );
                if( column.equals( columnName ) ) { value = values[ i ]; }
            }
        }

        return value;
    }


    /**
     * return location_id for existing key, else empty string
     * this is only used for ref_location.
     * one can also use value() to be more generic
     */
    public String locationno( String key )
    {
        String location_no = "";

        if( oldMap.containsKey( key ) ) {
            Collection< String > collection = oldMap.get( key );
            String[] values = collection.toArray( new String[ collection.size() ] );

            location_no = values [ standardIdx ];
        }

        return location_no;
    }


    /**
     * return standard code for existing key
     * return "x" for entry of new set
     */
    public String code( String key )
    {
        String sc = "";     // we know nothing

        if( oldMap.containsKey( key ) ) {
            Collection< String > collection = oldMap.get( key );
            String[] values = collection.toArray( new String[ collection.size() ] );

            sc = values[ standardCodeIdx ];
        }
        else if( newSet.contains( key ) ) {
            sc = "x";
        }
        return sc;
    }


    /**
     *
     */
    public void contentsNew()
    {
        System.out.println( "\n" + newSet.size() + " new entries:");
        int num = 0;
        for( String entry : newSet.elementSet() ) {
            num++;
            System.out.printf( "%d %s\n", num, entry );
        }
    }


    /**
     *
     */
    public void contentsOld()
    {
        /*
        if (tableName == "ref_role") {
            System.out.println("========================================================================");
            System.out.println("     # ir                  original                  standard nr sc  ss");
            System.out.println("------------------------------------------------------------------------");
        }
            //Object valuesObj = values.toArray();
            //System.out.printf( "%06d %25s %2d %25s %2s %2s %3s\n", new Object[] { nrow, key, valuesObj } );
            // nrow, id_role, original, standard, role_nr, standard_code, standard_source );

        if( tableName == "ref_role" ) {
            System.out.println( "========================================================================" );
        }
        }
        */

        Set< String > keys = oldMap.keySet();
        int nkeys = keys.size();

        System.out.println( "Contents of " + tableName + ", standard: " + standardColumn + " [entries: " + nkeys + "]:" );
        System.out.printf("original: ");
        for( String col : columnNames ) {
            if( !col.equals( "original" ) ) { System.out.printf( "%s ", col ); }
        }
        System.out.println( "" );

        int nrow = 0;
        for( String key : keys )
        {
            nrow++;
            Collection< String > collection = oldMap.get( key );
            String[] values = collection.toArray( new String[ collection.size() ] );

            System.out.printf( "%d %s : ", nrow, key );
            System.out.println( "" + Arrays.toString( values ) );
        }
    }


    /**
     * Add an entry to the new set
     */
    public void add( String entry )
    {
        newSet.add( entry );
        //System.out.println( "set size: " + newSet.size() );
    }


    /**
     * Insert the new set entries into the reference table
     */
    public void updateTable()
    throws Exception
    {
        //System.out.println( "updateTable" );

        int num = 0;
        for( String entry : newSet.elementSet() ) {
            //num++;
            //System.out.printf( "%d %s\n", num, entry );
            String[] fields = { "original", "standard_code" };

            String[] values = { LinksSpecific.funcPrepareForMysql( entry ), "x" };

            conn_write.insertIntoTable( tableName, fields, values );
        }
    }


    /**
     * Work for the garbage collector
     */
    public void free() {
        oldMap = null;
        newSet = null;
        columnNames = null;
        valueNames  = null;
    }
}
