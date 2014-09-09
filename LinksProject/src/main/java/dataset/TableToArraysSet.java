/*
 * Version
 * Copyright
 *
 */
package dataset;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;

import java.util.ArrayList;
import java.util.Collections;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import connectors.MySqlConnector;

import modulemain.LinksSpecific;

/**
 * @author Omar Azouguagh
 * @author Fons Laan
 *
 * <p/>
 * FL-30-Jun-2014 Imported from OA backup
 * FL-13-Aug-2014 Cor: ArrayList<ArrayListNonCase> is a case-insensitive variant of the standard ArrayList
 *                created by Omar.
 * FL-28-Aug-2014 Comment out some unused functions
 * FL-09-Sep-2014 Latest change
 */
public class TableToArraysSet
{
    boolean debug = false;
    boolean extra = true;

    private ArrayList< ArrayListNonCase > column     = new ArrayList<ArrayListNonCase>();
    private ArrayList <ArrayListNonCase > columnCopy = new ArrayList<ArrayListNonCase>();         // used for Shuffle
    private ArrayList< ArrayListNonCase > columnNew  = new ArrayList<ArrayListNonCase>();

    private ArrayList< String > columnName    = new ArrayList< String >();
    private ArrayList< String > columnNameNew = new ArrayList<String>();
    
    private String tableName;

    private MySqlConnector con;
    private MySqlConnector con_or;

    ResultSet rs;

    /**
     * Constructor to load a reference table into ArrayLists
     *
     * "con_or" is only used in the 2 functions updateTable() and updateTableWithCode().
     * As updateTableWithCode() is nowhere used by the project, we commented it out.
     *
     * @param con           // "con" is normally conGeneral, i.e. the local links_general db
     * @param con_or        // "con_or" is normally conOr, i.e. the remote links_general db (on node-030)
     * @param IndexField
     * @param tableName
     */
    public TableToArraysSet( MySqlConnector con, MySqlConnector con_or, String IndexField, String tableName )
    throws Exception
    {
        this.tableName = tableName;
        this.con = con;
        this.con_or = con_or;

        if( debug ) { System.out.println( "TableToArraysSet, table: " + tableName + " , index: " + IndexField ); }

        String query = "";
        if( IndexField.isEmpty() )
        { query = "SELECT * FROM ref_" + tableName; }
        else
        { query = "SELECT * FROM ref_" + tableName + " ORDER BY " + IndexField + " ASC"; }

        if( debug ) { System.out.println( "TableToArraysSet, query: " + query ); }

        rs = this.con.runQueryWithResult( query );
        ResultSetMetaData rsmd = rs.getMetaData();

        int numCols = rsmd.getColumnCount();
        if( debug ) { System.out.println( "TableToArraysSet, numCols: " + numCols ); }

        if( debug ) {
            System.out.printf( "column names: " );
            for( int i = 1; i <= numCols; i++ ) { System.out.printf("%s, ", rsmd.getColumnName(i)); }
            System.out.println("\n");
        }

        for( int i = 1; i <= numCols; i++ )     // process each column
        {
            int ct = rsmd.getColumnType( i );
            String cn = rsmd.getColumnName( i );

            boolean isIntFlag;

            // int arraylist
            if( (ct == -6) || (ct == -5) || (ct == 4) || (ct == 5) )
            {
                ArrayListNonCase< Integer > al     = new ArrayListNonCase<Integer>();
                ArrayListNonCase< Integer > alCopy = new ArrayListNonCase<Integer>();
                ArrayListNonCase< Integer > alNew  = new ArrayListNonCase<Integer>();
                
                column.add( al );
                columnCopy.add( alCopy );
                columnNew.add( alNew );

                // add name
                columnName.add( cn );
                columnNameNew.add( cn );

                isIntFlag = true;                // set int flag
            }
            else        // else a String arraylist
            {

                ArrayListNonCase< String > al     = new ArrayListNonCase<String>();
                ArrayListNonCase< String > alCopy = new ArrayListNonCase<String>();
                ArrayListNonCase< String > alNew  = new ArrayListNonCase<String>();
                
                column.add( al );
                columnCopy.add( alCopy );
                columnNew.add( alNew );

                // add name
                columnName.add( cn );
                columnNameNew.add( cn );

                isIntFlag = false;          // set int flag
            }

            // Fill array with table
            int j = 0;
            while( rs.next() )              // process each row
            {
                j += 1;
                if( isIntFlag )            // int
                {
                    column.get( (i - 1) ).add( rs.getInt( i ) );
                    columnCopy.get( (i - 1) ).add( rs.getInt( i ) );
                }
                else                        // string
                {
                    String ts = rs.getString( i );

                    if( ts != null && !ts.isEmpty() ) {
                        column.get( (i - 1) ).add( ts.toLowerCase() );
                        columnCopy.get( (i - 1) ).add( ts.toLowerCase() );
                    }
                    else
                    {
                        column.get( (i - 1) ).add( "" );
                        columnCopy.get( (i - 1) ).add( "" );
                    }
                }
            }

            //System.out.println( "rows: " + j);
            rs.beforeFirst();            // set Iterator
        }

        rs.close();        // Close ResultSet
        rs = null;

        if( debug ) { showContents( "id_" + tableName ); }

        //if( !extra ) {
        //    System.out.println( "SKIPPING EXTRA SORT" );
        //    return;
        //}

        if( !IndexField.isEmpty() )             // Do extra sort
        {
            System.out.println( "TableToArraysSet: extra sort" );
            // Sort by Java 
            Collections.sort( column.get( columnName.indexOf( "original" ) ) );

            // Sort other arrays
            for( int i = 0; i < column.get( columnName.indexOf( "original" ) ).size(); i++ )
            {
                int index = columnCopy.get( columnName.indexOf( "original" ) ).indexOf( column.get( columnName.indexOf( "original" ) ).get( i ) ) ;
                
                // Check if original is replaced
                if( i != index )
                {
                    // Shuffle
                    for( int j = 0 ; j < columnName.size() ; j++ )
                    {
                        // Shuffle only originals
                        if( columnName.get( j ) != "original" ) {
                            column.get( j ).set( i, columnCopy.get( j ).get( index ) );
                        }
                    }
                }
            }
        }
        //System.out.println("TableToArraysSet: end");
    } // TableToArraysSet


    /**
     *
     */
    public void showContents( String ref_name )
    {
        int id_limit = 0;
        //int id_limit = 666000;       // occupation

        int ncolumns = this.column.size();

        System.out.println( "TableToArraysSet/showContents" );
        System.out.printf("Table name: %s\n", this.tableName);
        System.out.printf( "colums: %d\n", ncolumns );

        if( ncolumns != 5 ) {
            System.out.println( "unexpected number of columns" );
            return;
        }

        //id_occupation, original, standard, standard_code, standard_source
        int nrows = column.get( columnName.indexOf( "original" ) ).size();

        ArrayListNonCase alnc_name = this.column.get( 0 );
        ArrayListNonCase alnc_orig = this.column.get( 1 );
        ArrayListNonCase alnc_stan = this.column.get( 2 );
        ArrayListNonCase alnc_code = this.column.get( 3 );
        ArrayListNonCase alnc_sour = this.column.get( 4 );

        System.out.println( "============================================================================================================" );
        System.out.printf ( "     #  %s  original                 standard                 standard_code standard_source\n", ref_name );
        System.out.println( "------------------------------------------------------------------------------------------------------------" );
        for( int c = 0; c < ncolumns; c ++ )
        {
            for( int r = 0; r < nrows; r++)
            {
                String name = alnc_name.get( r ).toString();
                int id_name = Integer.parseInt(name );

                int lmax = 40;
                String orig = alnc_orig.get( r ).toString();
                if( orig.length() > lmax ) { orig = orig.substring( 0, lmax ); }

                String stan = alnc_stan.get( r ).toString();
                if( stan.length() > lmax ) { stan = stan.substring( 0, lmax ); }

                //if( orig.startsWith( "beroep" ) ) {
                if( orig.contains( "eroep" ) ) {
                //if( id_name > id_limit ) {
                    System.out.printf("%06d: %6s %40s %40s %s %9s\n", r,
                            alnc_name.get(r), orig, stan, alnc_code.get(r), alnc_sour.get(r));
                }

                //if (r > 10) { break; }
            }
        }
        System.out.println( "============================================================================================================" );
    } // showContents


    /**
     *
     * @param newValue
     */
    public void addOriginal( String newValue )
    {
        //System.out.println( "TableToArraysSet/addOriginal" );

        // add standardcode

        for (int i = 0; i < columnNew.size(); i++) {

            if (columnNameNew.get(i).equalsIgnoreCase("original")) {

                // Check if original exists
                if (!originalExists(newValue)) {

                    int index = Collections.binarySearch(columnNew.get(i), newValue);

                    //set all the columns, now we know the index where is has to be put
                    for (int j = 0; j < columnNew.size(); j++) {

                        if (j == 0) {
                            columnNew.get(0).add(-1);
                        } else if (columnNameNew.get(j).equalsIgnoreCase("original")) {
                            columnNew.get(j).add(-index - 1, newValue);
                        } else if (columnNameNew.get(j).equalsIgnoreCase("standard_code")) {
                            columnNew.get(j).add(-index - 1, "x");
                        } else {
                            columnNew.get(j).add(-index - 1, "");
                        }
                    }

                }
            }
        }
    } // addOriginal


    /**
     *
     * @param newValue
     * @param standardCode
     */
    public void addOriginalWithCode( String newValue, String standardCode )
    {
        //System.out.println( "TableToArraysSet/addOriginalWithCode" );

        // add nieuwcode
        for (int i = 0; i < columnNew.size(); i++) {

            if (i == 0) {
                columnNew.get(0).add(-1);
            } else if (columnNameNew.get(i).equalsIgnoreCase("original")) {
                columnNew.get(i).add(newValue.toLowerCase());
            } else if (columnNameNew.get(i).equalsIgnoreCase("standard_code")) {
                columnNew.get(i).add(standardCode);
            } else {
                columnNew.get(i).add("");
            }
        }
    } // addOriginalWithCode


    /**
     *
     * @param value
     * @return
     */
    public boolean originalExists( String value )
    {
        //System.out.println( "TableToArraysSet/originalExists" );

        int index = Collections.binarySearch(column.get(columnName.indexOf("original")), value);

        if (index > -1) {
            return true;
        }

        // Test new list
        index = Collections.binarySearch(columnNew.get(columnNameNew.indexOf("original")), value);

        if (index > -1) {
            return true;
        }

        return false;
    } // originalExists


    /**
     *
     * @param value
     * @return
     */
    public String getStandardCodeByOriginal( String value )
    throws Exception
    {
        if( debug ) { System.out.println( "TableToArraysSet/getStandardCodeByOriginal, value: " + value ); }

        return getColumnByOriginal( "standard_code", value );
    } // getStandardCodeByOriginal


    /**
     *
     * @param value
     * @return
     */
    public String getStandardByOriginal( String value )
    throws Exception
    {
        if( debug ) { System.out.println( "TableToArraysSet/getStandardByOriginal" ); }

        return getColumnByOriginal( "standard", value );
    } // getStandardByOriginal


    /**
     *
     * @param name
     * @param value
     * @return
     */
    public String getColumnByOriginal( String name, String value )
    throws Exception
    {
        //System.out.println( "TableToArraysSet/getColumnByOriginal" );

        int index = Collections.binarySearch( column.get(columnName.indexOf( "original" ) ), value );
        //System.out.printf( "TableToArraysSet/getColumnByOriginal: index = %d\n", index );

        if( index > -1 ) {
            return column.get( columnName.indexOf( name.toLowerCase() ) ).get( index ).toString();
        }

        int index2 = Collections.binarySearch( columnNew.get( columnNameNew.indexOf( "original" ) ), value );
        //System.out.printf( "TableToArraysSet/getColumnByOriginal: index2 = %d\n", index2 );

        if( index2 > -1 ) {

            // TODO: work around
            if( name.equalsIgnoreCase( "standard_code" ) ) {
                //return "x";
                return "y";     // FL-29-Aug-2014
            }

            return columnNew.get( columnNameNew.indexOf( name ) ).get( index2 ).toString();
        }

        if( debug ) {
            System.out.printf( "TableToArraysSet/getColumnByOriginal: name: %s, value: %s\n" );
            System.out.printf( "TableToArraysSet/getColumnByOriginal: index1: %d, index2: %d\n" );
        }

        throw new Exception( "Original Index Error" );
    } // getColumnByOriginal


    /**
     *
     * @param columnToGet
     * @param name
     * @param value
     * @return
     */
    /*
    public String getColumnByColumn( String columnToGet, String name, String value )
    {
        //System.out.println( "TableToArraysSet/getColumnByColumn" );

        String valuel = value.toLowerCase();
        int io = columnName.indexOf(columnToGet);
        int in = columnName.indexOf(name);

        return column.get(in).get(column.get(io).indexOf(valuel)).toString();
    } // getColumnByColumn
    */

    public String getColumnByColumnInt( String columnToGet, String name, int value )
    {
        //System.out.println( "TableToArraysSet/getColumnByColumnInt" );

        int io = columnName.indexOf(columnToGet);
        int in = columnName.indexOf(name);

        return column.get(in).get(column.get(io).indexOf(value)).toString();
    } // getColumnByColumnInt


    /**
     *
     * @param index
     * @return
     */
    /*
    public String getOriginalByIndex( int index )
    {
        //System.out.println( "TableToArraysSet/getOriginalByIndex" );

        return column.get(columnName.indexOf("original")).get(index).toString();
    } // getOriginalByIndex
    */

    /**
     *
     * @param cName
     * @return
     */
    public ArrayListNonCase getArray( String cName )
    {
        //System.out.println( "TableToArraysSet/getArray" );

        return column.get(columnName.indexOf(cName));
    } // getArray


    /**
     *
     * @return
     */
    /*
    public int countRows()
    {
        //System.out.println( "TableToArraysSet/countRows" );

        return column.get(columnName.indexOf("original")).size();
    } // countRows
    */

    /**
     *
     * @throws Exception
     */
    public void updateTable()
    throws Exception
    {
        //System.out.println( "TableToArraysSet/updateTable" );

        for (int i = 0; i < columnNew.get(columnNameNew.indexOf("original")).size(); i++) {
            String[] fields = {"original", "standard_code"};
            String[] values = {LinksSpecific.funcPrepareForMysql(columnNew.get(columnNameNew.indexOf("original")).get(i).toString()), "x"};
            this.con_or.insertIntoTable("ref_" + tableName, fields, values);
        }
    } // updateTable


    /**
     *
     * @throws Exception
     */
    /*
    public void updateTableWithCode()
    throws Exception {
        //System.out.println( "TableToArraysSet/updateTableWithCode" );

        for (int i = 0; i < columnNew.get(0).size(); i++) {
            String[] fields = {"original", "standard_code"};
            String[] values = {LinksSpecific.funcPrepareForMysql(
                columnNew.get(columnNameNew.indexOf("original")).get(i).toString()),
                columnNew.get(columnNameNew.indexOf("standard_code")).get(i).toString()};
            this.con_or.insertIntoTable("ref_" + tableName, fields, values);
        }
    } // updateTableWithCode
    */

    /**
     * This function clears the used ArrayLists in this Class
     */
    public void free()
    {
        //System.out.println( "TableToArraysSet/free" );

        for (int i = 0; i < column.size(); i++) {
            column.get(i).clear();
            columnNew.get(i).clear();
        }

        column.clear();
        columnName.clear();
        columnNew.clear();
        columnNameNew.clear();
        
        columnCopy.clear();
    } // free
}

