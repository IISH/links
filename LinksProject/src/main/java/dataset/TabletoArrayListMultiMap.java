package dataset;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Set;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;

import connectors.MySqlConnector;

/**
 * @author Fons Laan
 *
 * FL-15-Sep-2014 Latest change
 */
public class TabletoArrayListMultiMap
{
    private boolean debug = true;

    // create multimaps to store key and values for reference table and new values (to be added to ref table)
    private Multimap< String, String > multiMapOld = ArrayListMultimap.create();
    private Multimap< String, String > multiMapNew = ArrayListMultimap.create();

    private String tableName;
    private String keyColumn;             // column name used as index
    ArrayList< String > columnNames = new ArrayList();
    ArrayList< String > valueNames  = new ArrayList();
    private int numColumns;

    int originalIdx;
    int standardIdx;
    int standardCodeIdx;

    private MySqlConnector con;
    private MySqlConnector con_or;

    ResultSet rs;

    /**
     * Constructor to load a reference table into an ArrayListMultiMap
     *
     * "con_or" is only used in the 2 functions updateTable() and updateTableWithCode().
     * As updateTableWithCode() is nowhere used by the project, we commented it out.
     *
     * @param con           // "con" is normally conGeneral, i.e. the local links_general db
     * @param con_or        // "con_or" is normally conOr, i.e. the remote links_general db (on node-030)
     * @param keyColumn
     * @param tableName
     */
    public TabletoArrayListMultiMap( MySqlConnector con, MySqlConnector con_or, String tableName, String keyColumn )
    throws Exception
    {
        this.con = con;
        this.con_or = con_or;
        this.tableName = tableName;
        this.keyColumn = keyColumn;

        if( debug ) { System.out.println( "TabletoArrayListMultiMap, table name: " + tableName + " , index column: " + keyColumn ); }

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


        String query = "SELECT * FROM `" + tableName + "` ORDER BY `" + keyColumn + "` ASC";
        if( debug ) { System.out.println( "TabletoArrayListMultiMap, query: " + query ); }
        ResultSet rs = con.runQueryWithResult( query );

        ResultSetMetaData rs_md = rs.getMetaData();
        numColumns = rs_md.getColumnCount();
        columnNames = new ArrayList();              // excluding the key column !
        int skip = 0;

        for( int i = 1; i <= numColumns; i++ ) {
            String columnName = rs_md.getColumnName(i);
            if( columnName.equals( "original" ) ) { skip = 1; }
            else { valueNames.add( columnName ); }

            columnNames.add( columnName );

            if( columnName.equals( "original" ) )      { originalIdx     = i - 1 - skip; }
            if( columnName.equals( "standard" ) )      { standardIdx     = i - 1 - skip; }
            if( columnName.equals( "standard_code" ) ) { standardCodeIdx = i - 1 - skip; }
        }

        if( debug ) { tableInfo(); }

        int nrow = 0;
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

            nrow++;
            String key = "";
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
                else { throw new Exception( "TabletoArrayListMultiMap: unhandled column type: " + ct ); }

                //if( strValue == null ) { strValue = ""; }

                String columnName = columnNames.get( i );
                if( columnName.equals( keyColumn ) ) { key = strValue; }
                else { values.add( strValue ); }
            }

            for( String value : values ) { multiMapOld.put ( key, value ); }
            values = null;
        }

        //tableContents( multiMapOld );

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
     * size existing reference table map
     */
    public int sizeOld() {
        return multiMapOld.size();
    }

    /**
     * size of new map (to be added to reference table)
     */
    public int sizeNew() {
        return multiMapNew.size();
    }


    /**
     * check key in either map
     */
    public boolean containsKey( String key )
    {
        boolean old = multiMapOld.containsKey( key );
        if( old ) return true;

        return multiMapNew.containsKey( key );
    }


    /**
     * return standard for existing key
     */
    public String standard( String key )
    {
        Collection< String > collection = multiMapOld.get( key );
        String[] values = collection.toArray( new String[ collection.size() ] );

        return values [ standardIdx ];
    }


    /**
     * return standard code for existing key
     */
    public String standardCode( String key )
    {
        Collection< String > collection = multiMapOld.get( key );
        String[] values = collection.toArray( new String[ collection.size() ] );

        return values [ standardCodeIdx ];
    }


    /**
     *
     */
    public void contentsOld()
    {
        int size = multiMapOld.size();
        if( size == 0 ) { System.out.println( "multiMapOld is empty" ); }
        else { tableContents( "old", multiMapOld ); }
    }


    /**
     *
     */
    public void contentsNew()
    {
        int size = multiMapNew.size();
        if( size == 0 ) { System.out.println( "multiMapNew is empty" ); }
        else { tableContents( "new", multiMapNew ); }
    }


    /**
     *
     */
    public void tableContents( String source, Multimap< String, String > mmap ) {
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

        int size = mmap.size();
        System.out.println( "Contents " + source + " (size: " + size + "):");
        Set< String > keys = mmap.keySet();

        int nrow = 0;
        for( String key : keys ) {
            nrow++;
            Collection< String > collection = mmap.get( key );
            String[] values = collection.toArray( new String[ collection.size() ] );

            System.out.printf( "%6d %25s : ", nrow, key );
            System.out.println( "" + Arrays.toString( values ) );
            nrow++;
        }
    }


    /**
     *
     */
    public void add( String original )
    {
        String key = "original";
        ArrayList< String > values = new ArrayList();

        String str = "";
        for( int i = 0; i < numColumns; i++ )     // process each column
        {
            str = "";
            if( i == originalIdx )    { str = original; }
            if( i == standardCodeIdx) { str = "x"; }
        }
        values.add( str );

        for( String value : values ) { multiMapNew.put ( key, value ); }
    }


    /**
     *
     */
    public void free() {
        multiMapOld = null;
        multiMapNew = null;
        columnNames = null;
        valueNames  = null;
    }
}
