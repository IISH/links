package general;

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

import dataset.*;
import java.sql.*;
import java.util.*;
import connectors.MySqlConnector;

/**
 * Use this software to load reference tables into the memory
 * @author oaz
 */
public class RefTableManager {

    ResultSet rs;
    private String tableName;
    private MySqlConnector conn;
    private ArrayList<String> columnName = new ArrayList<String>();
    private ArrayList<String> columnNameNew = new ArrayList<String>();
    private ArrayList<ArrayListNonCase> column = new ArrayList<ArrayListNonCase>();
    private ArrayList<ArrayListNonCase> columnNew = new ArrayList<ArrayListNonCase>();
    
    /**
     * Constructor to load table into ArrayLists
     * @param MySQL Connector Object
     * @param indexField Field to index
     * @param tableName WITHOUT REF_
     */
    public RefTableManager(MySqlConnector conn, String IndexField, String tableName) throws Exception {

        this.tableName = tableName;
        this.conn = conn;


        if (IndexField.isEmpty()) {
            rs = this.conn.runQueryWithResult("SELECT * FROM ref_" + tableName + ";");
        } else {
            rs = this.conn.runQueryWithResult("SELECT * FROM ref_" + tableName + " ORDER BY " + IndexField + " ASC;");
        }

        ResultSetMetaData rsmd = rs.getMetaData();
        int numCols = rsmd.getColumnCount();

        for (int i = 1; i <= numCols; i++) {

            int ct = rsmd.getColumnType(i);
            String cn = rsmd.getColumnName(i);

            boolean isIntFlag;

            // int
            if ((ct == -6) || (ct == -5) || (ct == 4) || (ct == 5)) {

                ArrayListNonCase<Integer> al = new ArrayListNonCase<Integer>();
                ArrayListNonCase<Integer> alNew = new ArrayListNonCase<Integer>();
                column.add(al);
                columnNew.add(alNew);

                // add name
                columnName.add(cn);
                columnNameNew.add(cn);

                // set flag
                isIntFlag = true;

            } // else a String arraylist
            else {

                ArrayListNonCase<String> al = new ArrayListNonCase<String>();
                ArrayListNonCase<String> alNew = new ArrayListNonCase<String>();
                column.add(al);
                columnNew.add(alNew);

                // add name
                columnName.add(cn);
                columnNameNew.add(cn);

                // set flag
                isIntFlag = false;
            }

            // Fill array with table
            while (rs.next()) {

                if (isIntFlag) {
                    column.get((i - 1)).add(rs.getInt(i));
                } else { // string
                    String ts = rs.getString(i);

                    if (ts != null && !ts.isEmpty()) {
                        column.get((i - 1)).add(ts.toLowerCase());
                    } else {
                        column.get((i - 1)).add("");
                    }
                }
            }

            // set Iterator
            rs.beforeFirst();
        }

        // Close ResultSet
        rs.close();
        rs = null;
        
        // Do extra sort
        if(!IndexField.isEmpty()){
        Collections.sort( column.get(columnName.indexOf(IndexField)) );
        }
    }

    /**
     * Add an original value, with x
     * @param newValue
     */
    public void addOriginal(String newValue) {

        for (int i = 0; i < columnNew.size(); i++) {
            if (columnNameNew.get(i).equalsIgnoreCase("original")) {

                // Check if original exists
                if (!originalExists(newValue)) {

                    int index = Collections.binarySearch(columnNew.get(i), newValue);

                    //set al the columns, now we know the index where is has to be put
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
    }

    /**
     * Add original with own code
     * @param newValue
     * @param standardCode
     */
    public void addOriginalWithCode(String newValue, String standardCode) {
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
    }

    /**
     * Check if original exists
     * @param value original value
     * @return
     */
    public boolean originalExists(String value) {
        
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
    }

    /**
     * Get standard code of the given orginal value
     * @param value original value
     * @return
     */
    public String getStandardCodeByOriginal(String value) throws Exception {
        return getColumnByOriginal("standard_code", value);
    }

    /**
     * Get standard value of the given
     * @param value original value
     * @return 
     */
    public String getStandardByOriginal(String value) throws Exception {
        return getColumnByOriginal("standard", value);
    }

    /**
     * 
     * @param name name of the column you want
     * @param value vanlue of original
     * @return value in the specified column
     */
    public String getColumnByOriginal(String name, String value) throws Exception {
        // return getColumnByColumn("original", name, value);

        int index = Collections.binarySearch(column.get(columnName.indexOf("original")), value);

        if (index > -1) {
            return column.get(columnName.indexOf(name.toLowerCase())).get(index).toString();
        } 

        int index2 = Collections.binarySearch(columnNew.get(columnNameNew.indexOf("original")), value);

        if (index2 > -1) {

            // TODO: work arround
            if (name.equalsIgnoreCase("standard_code")) {

                return "x";

            }

            return columnNew.get(columnNameNew.indexOf(name)).get(index2).toString();
        } 
        throw new Exception("Original Index Error");
    }

    /**
     *
     * @param columnToGet
     * @param name
     * @param value
     * @return
     */
    public String getColumnByColumn(String columnToGet, String name, String value) {

        String valuel = value.toLowerCase();
        int io = columnName.indexOf(columnToGet);
        int in = columnName.indexOf(name);

        return column.get(in).get(column.get(io).indexOf(valuel)).toString();
    }

    public String getColumnByColumnInt(String columnToGet, String name, int value) {

        int io = columnName.indexOf(columnToGet);
        int in = columnName.indexOf(name);

        return column.get(in).get(column.get(io).indexOf(value)).toString();
    }

    /**
     * Get the original value in the given index
     * @param index 
     * @return
     */
    public String getOriginalByIndex(int index) {
        return column.get(columnName.indexOf("original")).get(index).toString();
    }

    /**
     * Get a ArrayListrepresentation of a given column name
     * @param cName column name
     * @return ArrayListNonCase
     */
    public ArrayListNonCase getArray(String cName) {
        return column.get(columnName.indexOf(cName));
    }

    /**
     * Get amount of rows in table
     * @return amount of rows in table
     */
    public int countRows() {
        return column.get(columnName.indexOf("original")).size();
    }

    /**
     * Use this function to update the table with new code 'x'
     * @throws Exception when fails
     */
    public void updateTable() throws Exception {
        for (int i = 0; i < columnNew.get(columnNameNew.indexOf("original")).size(); i++) {
            String[] fields = {"original", "standard_code"};
            String[] values = {Functions.funcPrepareForMysql(columnNew.get(columnNameNew.indexOf("original")).get(i).toString()), "x"};
            this.conn.insertIntoTable("ref_" + tableName, fields, values);
        }
    }

    /**
     * Use this function when you used another new code that 'x' 
     * @throws Exception when fails
     */
    public void updateTableWithCode() throws Exception {
        for (int i = 0; i < columnNew.get(0).size(); i++) {
            String[] fields = {"original", "standard_code"};
            String[] values = {Functions.funcPrepareForMysql(
                columnNew.get(columnNameNew.indexOf("original")).get(i).toString()),
                columnNew.get(columnNameNew.indexOf("standard_code")).get(i).toString()};
            this.conn.insertIntoTable("ref_" + tableName, fields, values);
        }
    }

    /**
     * This function cleans the used Memory
     */
    public void free() {

        for (int i = 0; i < column.size(); i++) {
            column.get(i).clear();
            columnNew.get(i).clear();
        }

        column.clear();
        columnName.clear();
        columnNew.clear();
        columnNameNew.clear();
    }
}