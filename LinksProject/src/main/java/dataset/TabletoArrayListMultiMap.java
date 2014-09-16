package dataset;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
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
     * @param indexColumn
     * @param tableName
     */
    public TabletoArrayListMultiMap( MySqlConnector con, MySqlConnector con_or, String tableName, String indexColumn )
    throws Exception
    {
        this.tableName = tableName;
        this.con = con;
        this.con_or = con_or;

        if( debug ) { System.out.println( "TabletoArrayListMultiMap, table name: " + tableName + " , index column: " + indexColumn ); }

        String query = query = "SELECT * FROM " + tableName + " ORDER BY " + indexColumn + " ASC";

        if( debug ) { System.out.println( "TabletoArrayListMultiMap, query: " + query ); }

        rs = this.con.runQueryWithResult( query );
        ResultSetMetaData rsmd = rs.getMetaData();

        int numCols = rsmd.getColumnCount();
        if( debug ) { System.out.println( "TabletoArrayListMultiMap, numCols: " + numCols ); }

        if( debug ) {
            System.out.printf( "column names: " );
            for( int i = 1; i <= numCols; i++ ) { System.out.printf("%s ", rsmd.getColumnName(i)); }
            System.out.println("\n");
        }

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
        if( this.tableName == "ref_role" )
        {
            //System.out.println( "========================================================================" );
            //System.out.println( "     # ir                  original                  standard nr sc  ss" );
            //System.out.println( "------------------------------------------------------------------------" );

            int nrow = 0;
            while( rs.next() )          // process each row
            {
                /*
                int  id_role           = rs.getInt(    1 );
                String original        = rs.getString( 2 );     // index
                String standard        = rs.getString( 3 );
                int role_nr            = rs.getInt(    4 );
                String standard_code   = rs.getString( 5 );
                String standard_source = rs.getString( 6 );
                */
                for( int i = 1; i <= numCols; i++ )     // process each column
                {
                    int ct = rsmd.getColumnType(i);

                    String strValue = "";

                    if( ct == 4 ) {
                        int intValue = rs.getInt( i );
                        strValue = Integer.toString( intValue );
                    }
                    else if( ct == 1 || ct ==12 ) {
                        strValue = rs.getString( i );

                    }
                    else {
                        throw new Exception( "TabletoArrayListMultiMap: unhandles column type: " + ct );
                    }

                    this.multiMapOld.put ( "original", strValue );
                }

                //System.out.println( "row: " + nrow + ", id_role: " + id_role + ", original: " + original + ", "
                //    + "standard: " + standard + ", role_nr: " + role_nr + ", sc: " + standard_code + ", ss: " + standard_source );

                //System.out.printf( "%06d %2d %25s %25s %2s %2s %3s\n",
                //        nrow, id_role, original, standard, role_nr, standard_code, standard_source );

                nrow++;
            }
            //System.out.println( "========================================================================" );

            // get all the set of keys
            Set< String>  keys = this.multiMapOld.keySet();

            // iterate through the key set and display key and values
            for( String key : keys ) {
                System.out.println( "Key = " + key );
                System.out.println( "Values = " + this.multiMapOld.get( key ) );
            }
        }


        /*
        // put values into map for A
        this.multiMapNew.put ("A", "Apple" );
        this.multiMapNew.put( "A", "Aeroplane" );

        // put values into map for B
        this.multiMapNew.put( "B", "Bat" );
        this.multiMapNew.put( "B", "Banana" );

        // put values into map for C
        this.multiMapNew.put( "C", "Cat" );
        this.multiMapNew.put( "C", "Car" );

        // retrieve and display values
        System.out.println("Fetching Keys and corresponding [Multiple] Values n");

        // get all the set of keys
        Set< String>  keys = this.multiMapNew.keySet();

        // iterate through the key set and display key and values
        for( String key : keys ) {
            System.out.println( "Key = " + key );
            System.out.println( "Values = " + this.multiMapNew.get( key ) );
        }
        */
    }
}
