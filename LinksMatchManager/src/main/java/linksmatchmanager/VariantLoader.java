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
 * FL-13-Feb-2015 Latest change
 *
 */
public class VariantLoader
{
    private String url;
    private String user;
    private String pass;
    PrintLogger plog;

    /**
     * Constructor
     * @param url  URL of the database
     * @param user Username for the database
     * @param pass Password for the database
     */
    public VariantLoader( String url, String user, String pass, PrintLogger plog )
    {
        this.url  = url;
        this.user = user;
        this.pass = pass;
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
        plog.show("VariantLoader/loadNames() db: links_prematch, tableName: " + tableName + ", Levenshtein value: " + value);

        Connection con = General.getConnection( url, "links_prematch", user, pass );
        con.setReadOnly( true );

        String query =  "SELECT length_1 , length_2 FROM " + tableName
            + " WHERE value <= " + value + " ORDER BY length_1, length_2 ASC limit 0, 200000000;";
        System.out.println( query );
        plog.show( query );

        ResultSet rs = con.createStatement().executeQuery( query );

        // Create an multi-dimensional array large enough for the biggest number
        rs.last();
        int max = rs.getRow();
        int[][] names = new int[ max ][];
        rs.beforeFirst(); // Put cursor back

        System.out.println( "VariantLoader/loadNames: number of records = " + max );
        plog.show( "VariantLoader/loadNames: number of records = " + max );

        // Counter to detect if the main name is changed
        int currentName = -1;

        // Create an ArrayList for the temporary names
        ArrayList< Integer > tempNames = new ArrayList<Integer>();

        // Loop through the records from the lv_ table
        int r = 0;
        while( rs.next() )
        {
            //System.out.println( "r: " + r );

            int n1 = rs.getInt( "length_1" );
            int n2 = rs.getInt( "length_2" );

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
                    for( int j = 0; j < countNamesInTemp; j++ ) {
                        namesArray[ j ] = tempNames.get( j );
                    }

                    // load complete array into multidimensional array
                    System.out.println( "r: " + r + ", " + namesArray.length );
                    names[ currentName ] = namesArray;
                }

                // Clear temporary array
                tempNames.clear();

                tempNames.add( n2 );

                // Change current number 
                currentName = n1;
            }
            else {
                // In this case the name must be loaded into temporary array
                tempNames.add( n2 );
            }

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
            for( int j = 0; j < countNamesInTemp; j++ ) {
                namesArray[ j ] = tempNames.get( j );
            }

            // load complete array into multidimensional array
            System.out.println( "r: " + currentName + ", " + namesArray.length );
            names[ currentName ] = namesArray;
        }

        // Close
        tempNames.clear();
        tempNames = null;

        rs.beforeFirst();
        rs.close();
        rs = null;

        con.createStatement().close();
        con.close();
        con = null;

        /*
        for( int r = 0; r < max; r ++ ) {
            //int[] n = names[ r ][ 0 ];
            System.out.printf( "r: ", r );
        }
        */

        return names;        // Return multidimensional array
    } // loadNames


    public int[][] loadRootNames( String tableName ) throws Exception
    {
        Connection con = General.getConnection( url, "links_prematch", user, pass );
        con.setReadOnly( true );

        // Get the root from table
        String query =  "SELECT length_1 , length_2 FROM " + tableName + " ORDER BY length_1;";
        ResultSet rs = con.createStatement().executeQuery( query );

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

        con.createStatement().close();
        con.close();
        con = null;

        // Return multidimensional array
        return names;

    } // loadRootNames

    /*
    public String[] loadRootNames(String tableName) throws Exception
    {
        Connection con = General.getConnection(url, "links_prematch", user, pass);
        con.setReadOnly(true);


        // Get the root from table
        ResultSet rs = con.createStatement().executeQuery(
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

        con.createStatement().close();
        con.close();
        con = null;

        // Return multidimensional array
        return names;

    } // loadRootNames
    */
}
