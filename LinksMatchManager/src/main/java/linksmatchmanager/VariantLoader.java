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
package linksmatchmanager;

import java.sql.Connection;
import java.sql.ResultSet;

import java.util.ArrayList;

import linksmatchmanager.DataSet.NameType;

/**
 * Use this class to convert Prematch tables to multidimensional arrays
 * @author Omar Azouguagh
 * @author Fons Laan
 *
 * FL-16-Feb-2015 Latest change
 *
 */
public class VariantLoader
{
    private String url;
    private String user;
    private String pass;

    Connection dbconPrematch;

    PrintLogger plog;

    /**
     * Constructor
     * @param dbconPrematch
     * @param plog
     */
    public VariantLoader( Connection dbconPrematch, PrintLogger plog )
    {
        this.dbconPrematch = dbconPrematch;

        this.plog = plog;
    }

    /**
     * This method returns a multidimensional Array.
     * @param tableName table to load. This will work as long as the table field name requirements are met
     * @param value max accepted Levenshtein value
     * @return multidimensional representation of the prematch table
     * @throws Exception
     */
    public int[][] loadNames( String tableName, int value )
    throws Exception
    {
        plog.show( "VariantLoader/loadNames() db: links_prematch, tableName: " + tableName + ", Levenshtein value: " + value );

        int limit = 200000000;      // 200.000.000
        //String query =  "SELECT length_1 , length_2 FROM " + tableName
        //    + " WHERE value <= " + value + " ORDER BY length_1, length_2 ASC limit 0, " + limit + ";";

        // need to inspect the string names for debugging
        String query =  "SELECT name_1 , name_2, length_1 , length_2, value FROM " + tableName
            + " WHERE value <= " + value + " ORDER BY length_1, length_2 ASC limit 0, " + limit + ";";

        System.out.println( query );
        plog.show( query );

        ResultSet rs = dbconPrematch.createStatement().executeQuery( query );

        // Create an multi-dimensional array large enough for the biggest number
        rs.last();
        int max = rs.getRow();
        int[][] names = new int[ max ][];
        rs.beforeFirst(); // Put cursor back

        System.out.println( "VariantLoader/loadNames: number of records = " + max );
        plog.show( "VariantLoader/loadNames: number of records = " + max );

        // Counter to detect if the main name is changed
        int currentName = -1;

        ArrayList< String >  tempStrings = new ArrayList<String>();
        ArrayList< Integer > tempNames   = new ArrayList<Integer>();      // Create an ArrayList for the temporary names

        // Loop through the records from the lv_ table
        int r = 0;
        while( rs.next() )
        {
            String name_1 = rs.getString( "name_1" );
            String name_2 = rs.getString( "name_2" );

            int n1 = rs.getInt( "length_1" );
            int n2 = rs.getInt( "length_2" );

            int Ldis = rs.getInt( "value" );

            // If the if statement is true, then the n1 name is changed
            // We must now process the found names
            if( n1 != currentName )
            {
                // count the names
                int countNamesInTemp = tempNames.size();

                // This check is because of the first time
                if( countNamesInTemp > 0 )
                {
                    // Create new array
                    int[] namesArray = new int[ countNamesInTemp ];

                    // Load names from temporary Arraylist into array
                    System.out.printf( "%d variants for entry %d:\n",  namesArray.length, currentName );

                    for( int j = 0; j < countNamesInTemp; j++ ) {
                        namesArray[ j ] = tempNames.get( j );
                        System.out.printf( "'%s' ", tempStrings.get( j ) );
                    }
                    System.out.println( "" );

                    // load complete array into multidimensional array
                    names[ currentName ] = namesArray;
                }

                // Clear temporary array
                tempStrings.clear();
                tempNames.clear();

                tempStrings.add( name_2 );
                tempNames.add( n2 );

                // Change current number 
                currentName = n1;
            }
            else
            {
                // In this case the name must be loaded into temporary array
                tempNames.add( n2 );
                tempStrings.add( name_2 );
            }

            System.out.printf( "r: %d: %s (%d) %s (%d) %d\n", r, name_1, n1, name_2, n2, Ldis );

            r++;
        }

        // Load last batch
        if( !tempNames.isEmpty() )
        {
            // count the names
            int countNamesInTemp = tempNames.size();

            // Create new array
            int[] namesArray = new int[ countNamesInTemp ];

            // Load names from temporary Arraylist into array
            System.out.printf( "%d variants:\n",  namesArray.length );

            for( int j = 0; j < countNamesInTemp; j++ ) {
                namesArray[ j ] = tempNames.get( j );
                System.out.printf( "'%s' ", tempStrings.get( j ) );
            }
            System.out.println( "" );

            // load complete array into multidimensional array
            names[ currentName ] = namesArray;
        }

        // Close
        tempStrings.clear();
        tempStrings = null;

        tempNames.clear();
        tempNames = null;

        rs.beforeFirst();
        rs.close();
        rs = null;

        System.out.println( "Summary:" );
        for( int i = 0; i < max; i ++ ) {
            int[] v = names[ i ];
            if( v != null ) {
                int nvariants = v.length;
                System.out.printf( "i: %d, variants: %d\n", i, nvariants );
            }
        }

        return names;        // Return multidimensional array
    } // loadNames


    public int[][] loadRootNames( String tableName ) throws Exception
    {
        // Get the root from table
        String query =  "SELECT length_1 , length_2 FROM " + tableName + " ORDER BY length_1;";
        ResultSet rs = dbconPrematch.createStatement().executeQuery( query );

        // Create an array large enough for the biggest number
        rs.last();
        int max = rs.getInt( "length_1" );
        int[][] names = new int[ max + 1 ][];
        rs.beforeFirst();       // Put cursor back

        // Loop through the records from the lv table
        while( rs.next() )
        {
            int n1 = rs.getInt( "length_1" );
            String n2 = "";
            n2 = rs.getString( "length_2" );

            if( n2 != null && !n2.isEmpty() && n2.contains( "," ) )
            {
                // remove commas
                n2 = n2.substring( 1, ( n2.length() - 1 ) );

                String[] n2Split = n2.split( "," );
                
                int[] nl = new int[ n2Split.length ];

                for( int i = 0; i < n2Split.length; i++ )
                {
                    try { nl[ i ] = Integer.parseInt( n2Split[ i ] ); }
                    catch( Exception ex ) { nl[ i ] = 0; }
                }
                names[ n1 ] = nl;
            }
        }

        // prevent null errors
        for( int i = 0; i < names.length; i++ ) {
            if( names[ i ] == null ) { names[ i ] = new int[ 0 ];
            }
        }

        rs.close();
        rs = null;

        // Return multidimensional array
        return names;

    } // loadRootNames

    /*
    public String[] loadRootNames(String tableName) throws Exception
    {
        // Get the root from table
        ResultSet rs = dbconPrematch.createStatement().executeQuery(
                "SELECT n1 , n2 FROM " + tableName + " ORDER BY n1;");

        // Create an array large enough for the biggest number
        rs.last();
        int max = rs.getInt("n1");
        String[] names = new String[max + 1];
        rs.beforeFirst(); // Get cursos back

        // Loop through the records from the lv table
        while (rs.next()) {

            int n1 = rs.getInt("n1");
            String n2 = "";
            n2 = rs.getString("n2");

            names[n1] = n2;
        }

        // prevent null errors
        for (int i = 0; i < names.length; i++) {

            if(names[i] == null){
                names[i] = "";
            }
        }

        rs.close();
        rs = null;

        // Return multidimensional array
        return names;

    } // loadRootNames
    */
}
