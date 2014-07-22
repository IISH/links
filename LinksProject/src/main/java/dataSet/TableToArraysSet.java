/*
 * Version
 * Copyright
 *
 */
package dataSet;

import java.sql.*;
import connectors.*;
import java.util.*;
import moduleMain.LinksSpecific;

public class TableToArraysSet {

    private ArrayList<ArrayListNonCase> column = new ArrayList<ArrayListNonCase>();
    private ArrayList<String> columnName = new ArrayList<String>();
    
    // used for Shuffle
    private ArrayList<ArrayListNonCase> columnCopy = new ArrayList<ArrayListNonCase>();
    
    private ArrayList<ArrayListNonCase> columnNew = new ArrayList<ArrayListNonCase>();
    private ArrayList<String> columnNameNew = new ArrayList<String>();
    
    private String tableName;
    private MySqlConnector con;
    private MySqlConnector con_or;
    ResultSet rs;

    /**
     * Constructor to load table into ArrayLists
     * @param conn
     * @param tableName
     */
    public TableToArraysSet(MySqlConnector con, MySqlConnector con_or, String IndexField, String tableName) throws Exception {

        this.tableName = tableName;
        this.con = con;
        this.con_or = con_or;


        if (IndexField.isEmpty()) {
            rs = this.con.runQueryWithResult("SELECT * FROM ref_" + tableName);
        } else {
            rs = this.con.runQueryWithResult("SELECT * FROM ref_" + tableName + " ORDER BY " + IndexField + " ASC");
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
                ArrayListNonCase<Integer> alCopy = new ArrayListNonCase<Integer>();
                ArrayListNonCase<Integer> alNew = new ArrayListNonCase<Integer>();
                
                column.add(al);
                columnCopy.add(alCopy);
                columnNew.add(alNew);

                // add name
                columnName.add(cn);
                columnNameNew.add(cn);

                // set flag
                isIntFlag = true;

            } // else a String arraylist
            else {

                ArrayListNonCase<String> al = new ArrayListNonCase<String>();
                ArrayListNonCase<String> alCopy = new ArrayListNonCase<String>();
                ArrayListNonCase<String> alNew = new ArrayListNonCase<String>();
                
                column.add(al);
                columnCopy.add(alCopy);
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
                    columnCopy.get((i - 1)).add(rs.getInt(i));
                } else { // string
                    String ts = rs.getString(i);

                    if (ts != null && !ts.isEmpty()) {
                        column.get((i - 1)).add(ts.toLowerCase());
                        columnCopy.get((i - 1)).add(ts.toLowerCase());
                    } else {
                        column.get((i - 1)).add("");
                        columnCopy.get((i - 1)).add("");
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
        if (!IndexField.isEmpty()) {

            // Sort by Java 
            Collections.sort(column.get(columnName.indexOf("original")));

            // Sort other arrays
            for (int i = 0; i < column.get(columnName.indexOf("original")).size(); i++) {

                int index = columnCopy.get(columnName.indexOf("original")).indexOf(column.get(columnName.indexOf("original")).get(i)) ;
                
                // Check if original is replaced
                if(i != index){
                    
                    // Shuffle
                    for (int j = 0 ; j < columnName.size() ; j++) {
                        
                        // Shuffle only originals
                        if( columnName.get(j) != "original" ){
                            
                            column.get(j).set(i, columnCopy.get(j).get( index ) );
                            
                        }
                    }
                }
            }
        }
    }

    /**
     *
     * @param newValue
     */
    public void addOriginal(String newValue) {

        // add standardcode


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
     *
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
     *
     * @param value
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
     *
     * @param value
     * @return
     */
    public String getStandardCodeByOriginal(String value) throws Exception {
        return getColumnByOriginal("standard_code", value);
    }

    /**
     *
     * @param value
     * @return
     */
    public String getStandardByOriginal(String value) throws Exception {
        return getColumnByOriginal("standard", value);
    }

    /**
     *
     * @param name
     * @param value
     * @return
     */
    public String getColumnByOriginal(String name, String value) throws Exception {

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
     *
     * @param index
     * @return
     */
    public String getOriginalByIndex(int index) {
        return column.get(columnName.indexOf("original")).get(index).toString();
    }

    /**
     *
     * @param cName
     * @return
     */
    public ArrayListNonCase getArray(String cName) {
        return column.get(columnName.indexOf(cName));
    }

    /**
     *
     * @return
     */
    public int countRows() {
        return column.get(columnName.indexOf("original")).size();
    }

    /**
     *
     * @throws Exception
     */
    public void updateTable() throws Exception {
        for (int i = 0; i < columnNew.get(columnNameNew.indexOf("original")).size(); i++) {
            String[] fields = {"original", "standard_code"};
            String[] values = {LinksSpecific.funcPrepareForMysql(columnNew.get(columnNameNew.indexOf("original")).get(i).toString()), "x"};
            this.con_or.insertIntoTable("ref_" + tableName, fields, values);
        }
    }

    /**
     *
     * @throws Exception
     */
    public void updateTableWithCode() throws Exception {
        for (int i = 0; i < columnNew.get(0).size(); i++) {
            String[] fields = {"original", "standard_code"};
            String[] values = {LinksSpecific.funcPrepareForMysql(
                columnNew.get(columnNameNew.indexOf("original")).get(i).toString()),
                columnNew.get(columnNameNew.indexOf("standard_code")).get(i).toString()};
            this.con_or.insertIntoTable("ref_" + tableName, fields, values);
        }
    }

    /**
     * This function cleanes the used ArrayLists in this Class
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
        
        columnCopy.clear();
    }
}
