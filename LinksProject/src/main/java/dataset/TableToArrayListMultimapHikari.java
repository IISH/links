package dataset;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.HashMultiset;
import com.google.common.collect.Multimap;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

//import connectors.MySqlConnector;
import connectors.HikariConnection;

import modulemain.LinksSpecific;
/**
 * @author Fons Laan
 *
 * FL-06-Apr-2016 AtomicBoolean update_busy
 * FL-05-Jul-2017 optional extra column for ref_registration
 * FL-22-Oct-2019 Hikari connection variant
 */
public class TableToArrayListMultimapHikari
{
    private boolean debug = false;

    private AtomicBoolean update_busy = new AtomicBoolean( false );

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
    private int numValues;      // excluding index column
    private int numRows;

    int keyColOff;              // offset for key column (mostly "original") in column name list
    int standardValOff;         // offset for "standard" value in value list
    int standardCodeValOff;     // offset for "standard_code" value in value list

    // conn_read only used in constructor
    // writing only used in update()
    private HikariConnection conn_read  = null;

    ResultSet rs;

    /**
     * Constructor to load a reference table into an ArrayListMultiMap
     *
     * "con_write" is only used in the 2 functions updateTable() and updateTableWithCode().
     *
     * @param conn_read         // "conn_read"  is normally conGeneral, i.e. the local links_general db
     * @param tableName
     * @param keyColumn
     * @param standardColumn
     */
    public TableToArrayListMultimapHikari
    (
        HikariConnection conn_read,
        String tableName,
        String keyColumn,
        String standardColumn
    )
    throws Exception
    {
        this.conn_read      = conn_read;
        this.tableName      = tableName;
        this.keyColumn      = keyColumn;
        this.standardColumn = standardColumn;

        if( debug ) { System.out.println( "TableToArrayListMultimap, table name: " +
            tableName + " , index column: " + keyColumn + ", standard column: " + standardColumn ); }

        update_busy.set( false );

        //MySqlConnector dbconRefRead = new MySqlConnector( url, "links_general", user, pass );

        oldMap = ArrayListMultimap.create();
        newSet = HashMultiset.create();

        columnNames = new ArrayList();
        valueNames  = new ArrayList();

        // invalid values
        keyColOff          = -1;
        standardValOff     = -1;
        standardCodeValOff = -1;

        /*
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
        */

        String query = "";
        if( check_duplicates ) { query = "SELECT * FROM links_general.`" + tableName + "` ORDER BY `" + keyColumn + "` ASC"; }
        else { query = "SELECT * FROM links_general.`" + tableName + "`"; }

        if( debug ) { System.out.println( "TableToArrayListMultimap, query: " + query ); }
        ResultSet rs = conn_read.runQueryWithResult( query );

        ResultSetMetaData rs_md = rs.getMetaData();
        numColumns = rs_md.getColumnCount();
        numValues  = numColumns - 1;
        if( debug ) { System.out.println( "numColumns: " + numColumns ); }
        columnNames = new ArrayList();              // excluding the key column !
        int skip1 = 0;

        for( int i = 0; i < numColumns; i++ )
        {
            int c = i + 1;                          // MySQL starts at 1

            String columnName = rs_md.getColumnName( c );
            columnNames.add( columnName );

            if( columnName.equals( keyColumn ) ) {
                keyColOff = i;
                skip1 = 1;
            }
            else { valueNames.add( columnName ); }

            if( columnName.equals( standardColumn ) )  { standardValOff = i - skip1; }
            else if( columnName.equals( "standard_code" ) ) { standardCodeValOff = i - skip1; }
        }

        if( debug ) {
            System.out.println( "keyColOff: "      + keyColOff );
            System.out.println( "standardValOff: " + standardValOff );
            System.out.println( "stdCodeValOff:  " + standardCodeValOff );
        }

        if( check_duplicates ) {
            if( debug ) { tableInfo(); }
            store_check( rs, rs_md );
        }
        else { store( rs, rs_md ); }

        if( debug ) { tableInfo(); }
        //contentsOld();

    } // TableToArrayListMultiMap


    /**
     * table and column names
     */
    public void tableInfo() {
        System.out.printf( "\ntable name: %s, key: %s\n", tableName, keyColumn );

        System.out.printf( "column names:\n" );
        for ( String columnName : columnNames ) { System.out.printf( "%s ", columnName ); }
        System.out.println( "\n" );
        System.out.printf( "value names (without the key column):\n" );
        for ( String valueName : valueNames ) { System.out.printf( "%s ", valueName ); }
        System.out.println( "\n" );

        System.out.printf( "standard value offset: %s, standard_code value offset: %s\n", standardValOff, standardCodeValOff );
    } // tableInfo


    /**
     * store table in multimap, and check for duplicates
     */
    public int store_check( ResultSet rs, ResultSetMetaData rs_md )
    throws Exception
    {
        //System.out.println( "TableToArrayListMultimap/store_check()" );

        int ndups = 0;
        boolean isdup = false;
        numRows = 0;

        while( rs.next() )          // process each index value
        {
            numRows++;
            String key = "";
            String original = "";
            ArrayList< String > values = new ArrayList();

            for( int i = 0; i < numColumns; i++ )   // process each column
            {
                int c = i + 1;                      // MySQL starts at 1

                int ct = rs_md.getColumnType( c );

                String strValue = "";

                if( ct == -6 || ct == 4 || ct == 5 ) {
                    int intValue = rs.getInt( c );
                    strValue = Integer.toString( intValue );
                }
                else if( ct == 1 || ct == 12 ) {
                    strValue = rs.getString( c );
                    if( strValue != null ) { strValue = strValue.toLowerCase(); }
                }
                else { throw new Exception( "TableToArrayListMultimap: unhandled column type: " + ct ); }

                String columnName = columnNames.get( i );

                if( columnName.equals( keyColumn ) )
                //if( keyColOff == i )      // faster than string compare
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
    } // store_check


    /**
     * store table in multimap
     */
    public void store( ResultSet rs, ResultSetMetaData rs_md )
    throws Exception
    {
        //System.out.println( "TableToArrayListMultimap/store()" );

        /*
        -7  BIT
        -6  TINYINT
        -5  BIGINT
        -4  LONGVARBINARY
        -3  VARBINARY
        -2  BINARY
        -1  LONGVARCHAR
         0  NULL
         1  CHAR
         2  NUMERIC
         3  DECIMAL
         4  INTEGER
         5  SMALLINT
         6  FLOAT
         7  REAL
         8  DOUBLE
        12  VARCHAR
        91  DATE
        92  TIME
        93  TIMESTAMP
        1111    OTHER
        */

        int nskipped = 0;
        numRows = 0;

        while( rs.next() )          // process each index value
        {
            String id = "";
            String key = "";
            String original = "";
            String standard_code = "";
            ArrayList< String > values = new ArrayList();

            for( int i = 0; i < numColumns; i++ )     // process each column
            {
                int c = i + 1;

                int ct = rs_md.getColumnType( c );

                String strValue = "";

                if( ct == -6 || ct == 4 || ct == 5 ) {
                    int intValue = rs.getInt( c );
                    strValue = Integer.toString( intValue );
                }
                else if( ct == 1 || ct ==12 ) {
                    strValue = rs.getString( c );
                    if( strValue != null )
                    { strValue = strValue.toLowerCase(); }
                }
                else { throw new Exception( "TableToArrayListMultimap: unhandled column type: " + ct ); }

                if( i == 0 ) { id = strValue; }
                String columnName = columnNames.get( i );

                if( columnName.equals( keyColumn ) )
                //if( originalColOff == i )      // faster than string compare
                {
                    original = strValue;
                    // toLowerCase() is needed for Location keys; the others are already lowercase
                    if( original == null ) { key = original; }
                    else { key = original.toLowerCase(); }
                }
                else {
                    if( columnName.equals( "standard_code" ) ) { standard_code = strValue; }

                    values.add( strValue );
                }
            }

            //System.out.println( String.format( "%d %s %s", numRows, id, key ) );

            if( standard_code == null ) {
                System.out.println( String.format( "TableToArrayListMultimap/store: Warning: Standard code is null in table %s for key: %s", tableName, key ) );
                for( String value : values ) { System.out.printf( "%s, ", value); } System.out.println( "" );
            }

            // does key already exist?
            if( contains( key ) ) {
                nskipped++;
                //System.out.println( String.format( "TableToArrayListMultimap/store: in ref table: %s, id: %s, key already exists: %s (but case may be different)",
                //tableName, id, key ) );
            }
            else { for( String value : values ) { oldMap.put ( key, value ); } }

            values = null;

            numRows++;
        }

        if( nskipped != 0 ) { System.out.println( String.format( "Skipped %d duplicates in table %s", nskipped, tableName ) ); }
    } // store


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
    } // numkeys


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

        //System.out.println( "contains: " + entry + ", " + tf );
        return tf;
    } // contains


    /**
     * return standard for existing key, else empty string
     */
    public String standard( String key )
    {
        String standard = "";

        if( oldMap.containsKey( key ) ) {
            Collection< String > collection = oldMap.get( key );
            if( collection.size() != numValues ) {
                System.out.println( String.format( "TableToArrayListMultimap: collection size %d does not match numValues %d ?", collection.size(), numValues ) );
            }

            String[] values = collection.toArray( new String[ collection.size() ] );

            standard = values [ standardValOff ];

            /*
            if( standard == null ) {
                System.out.println( "length: " + values.length );
                System.out.println( "key: " + key );
                System.out.println( "standard: " + standard );
                for( String val : values ) { System.out.printf("'%s' ", val); }
                System.out.println( "" );
            }
            */
        }
        //System.out.println( "standard: " + standard );

        return standard;
    } // standard


    /**
     * return value of specified column for existing key, else empty string
     */
    public String value( String column, String key )
    {
        if( debug ) { System.out.println( "value(): key: " + key + ", column: " + column ); }
        String value = "";

        if( oldMap.containsKey( key ) ) {
            Collection< String > collection = oldMap.get( key );
            if( collection.size() != numValues ) {
                System.out.println( String.format( "TableToArrayListMultimap: collection size %d does not match numValues %d ?", collection.size(), numValues ) );
            }

            String[] values = collection.toArray( new String[ collection.size() ] );

            for( int i = 0; i < valueNames.size(); i++ ) {
                String columnName = valueNames.get( i );
                if( debug ) { System.out.printf( "%s ", columnName ); }
                if( column.equals( columnName ) ) {
                    value = values[ i ];
                    if( debug ) { System.out.println( column + ", value: " + value ); }
                    break;
                }
            }
        }

        if( value == null ) {
            value = "";
            if( debug ) { System.out.println( "TableToArrayListMultimap/value(): null value for: key: " + key + ", column: " + column ); }
        }
        return value;
    } // value


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
            if( collection.size() != numValues ) {
                System.out.println( String.format( "TableToArrayListMultimap: collection size %d does not match numValues %d ?", collection.size(), numValues ) );
            }

            String[] values = collection.toArray( new String[ collection.size() ] );

            location_no = values [ standardValOff ];
        }

        return location_no;
    } // locationno


    /**
     *
     */
    public Set keySet()
    {
        return oldMap.keySet();
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

            /*
            System.out.println( "code() contains: " + key );
            System.out.println( "standardCodeValOff: " + standardCodeValOff + ", len: " + values.length );
            for( String value : values ) {
                System.out.println( value + " " );
            }
            */

            sc = values[ standardCodeValOff ];
        }
        else if( newSet.contains( key ) ) {
            sc = "x";
        }

        return sc;
    } // code


    /**
     *
     */
    public void contentsOld()
    {
        Set< String > keys = oldMap.keySet();
        int nkeys = keys.size();

        System.out.println( "Contents of " + tableName + ", standard: " + standardColumn + " [entries: " + nkeys + "]:" );
        System.out.printf("# %s: ", keyColumn );
        for( String col : columnNames ) {
            if( !col.equals( keyColumn ) ) { System.out.printf( "%s ", col ); }
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
    } // contentsOld


    /**
     *
     */
    public void contentsNew()
    {
        Set< String > newset = newSet.elementSet();
        int size = newset.size();

        System.out.println( "\n" + tableName + ": " + size + " new entries:");
        int num = 0;
        for( String entry : newSet.elementSet() ) {
            num++;
            System.out.printf( "%d %s\n", num, entry );
        }
    } // contentsNew


    /**
     * Add an entry to the new set
     */
    public void add( String entry )
    {
        newSet.add( entry );
        //System.out.println( "set size: " + newSet.size() );
    } // add


    /**
     * Insert the new set entries into the reference table
     *
     * Multi-threaded consideration:
     * Do not call this function when the flag update_busy is true.
     * We ignore the update request if another thread already has the update in progress.
     * Beware of: java.util.ConcurrentModificationException
     */
    public boolean updateTable( HikariConnection conn )
    throws Exception
    {
        // try to prevent: java.util.ConcurrentModificationException
        if( update_busy.get() ) { return false; }   // i.e. fail

        update_busy.set( true );    // if we were not busy, now we are...

        //System.out.println( "updateTable" );

        int num = 0;
        for( String entry : newSet.elementSet() )
        {
            //num++;
            //System.out.printf( "%d %s\n", num, entry );
            String[] fields = { "original", "standard_code" };

            String[] values = { LinksSpecific.prepareForMysql( entry ), "x" };

            // insertIntoTableIgnore: ignore duplicates for UNIQUE keys
            conn.insertIntoTableIgnore( tableName, fields, values );
        }

        update_busy.set( false );

        return true;    // i.e. success
    } // updateTable

    /**
     * Insert the new set entries into the reference table
     *
     * Multi-threaded consideration:
     * Do not call this function when the flag update_busy is true.
     * We ignore the update request if another thread already has the update in progress.
     * Beware of: java.util.ConcurrentModificationException
     */
    public boolean updateTable( HikariConnection conn, String extra_col, String delimiter )
    throws Exception
    {
        // try to prevent: java.util.ConcurrentModificationException
        if( update_busy.get() ) { return false; }   // i.e. fail

        update_busy.set( true );    // if we were not busy, now we are...

        if( debug ) { System.out.println( "updateTable" ); }

        if( debug ) { System.out.printf( "extra_col: %s, delimiter: %s\n", extra_col, delimiter ); }
        if( debug ) { System.out.println( String.format( "number of new entries: %d", newSet.size() ) ); }

        int num = 0;
        String[] fields = { "original", extra_col, "standard_code" };
        for( String entry : newSet.elementSet() )
        {
            //num++;
            if( debug ) { System.out.printf( "num: %d, entry: %s\n", num, entry ); }

            String[] comps   = entry.split( delimiter );
            String original  = comps[ 0 ];
            String extra_val = comps[ 1 ];

            if( debug ) { System.out.println( String.format( "original: %s, %s: %s", original, extra_col, extra_val ) ); }

            //String[] values = { LinksSpecific.prepareForMysql( entry ), "x" };
            String[] values = { LinksSpecific.prepareForMysql( original ), LinksSpecific.prepareForMysql( extra_val ), "x" };

            // insertIntoTableIgnore: ignore duplicates for UNIQUE keys
            conn.insertIntoTableIgnore( tableName, fields, values );
        }

        update_busy.set( false );

        return true;    // i.e. success
    } // updateTable


    public AtomicBoolean isBusy() { return update_busy; }



    /**
     * Work for the garbage collector
     */
    public void free() {
        oldMap = null;
        newSet = null;
        columnNames = null;
        valueNames  = null;
    } // free
}
