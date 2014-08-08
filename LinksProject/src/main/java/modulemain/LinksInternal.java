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
package modulemain;

import connectors.*;
import javax.swing.JTextArea;
import javax.swing.JTextField;
import java.io.FileInputStream;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import javax.swing.JProgressBar;
import enumdefinitions.IndexType;
import java.io.FileWriter;

/**
 * Use this sub-module to load CSV into a Mysql database 
 * @author oaz
 */
public class LinksInternal extends Thread {

    private String[] csvFiles;
    private String dbLocation;
    private String dbName;
    private String dbUser;
    private String dbPass;
    private String dbPreFix;
    private JTextField minUpdate;
    private JTextField lineNumbers;
    private JTextArea maxUpdate;
    private JProgressBar pTotal;
    private JProgressBar pFile;
    private int delay;
    private IndexType it;

    public LinksInternal(
            String[] csvFiles,
            String dbLocation,
            String dbName,
            String dbUser,
            String dbPass,
            String dbPreFix,
            JTextField minUpdate,
            JTextField lineNumbers,
            JTextArea maxUpdate,
            JProgressBar pTotal,
            JProgressBar pFile,
            int delay,
            IndexType it) {

        this.csvFiles = csvFiles;
        this.dbLocation = dbLocation;
        this.dbName = dbName;
        this.dbUser = dbUser;
        this.dbPass = dbPass;
        this.dbPreFix = dbPreFix + "_";
        this.minUpdate = minUpdate;
        this.maxUpdate = maxUpdate;
        this.lineNumbers = lineNumbers;
        this.pTotal = pTotal;
        this.pFile = pFile;
        this.delay = delay;
        this.it = it;

    }

    @Override
    public void run() {

        try {

            /**
             * Check Delay
             */
            if (delay > 0) {
                addToLog("Delay activated.", false);
                Thread.sleep(delay * 60000);
            }

            /**
             * Set GUI step skip
             */
            int skip = 1000000;
            int step = skip;
            long begintime = System.currentTimeMillis();

            /**
             * Use Csv Connector for the headers
             */
            CsvConnector ic = new CsvConnector();

            // Add To Log
            addToLog("Begin process.", false);

            /**
             * Connect to Database
             */
            MySqlConnector etm = new MySqlConnector(dbLocation, dbName, dbUser, dbPass);

            addToLog("Connected to database.", false);

            /**
             * Loop through the CSV files
             */
            for (int i = 0; i < csvFiles.length; i++) {

                // Set tableName
                String tableName = dbPreFix
                        + csvFiles[i].substring(
                        (csvFiles[i].lastIndexOf(LinksSpecific.osSlash()) + 1),
                        (csvFiles[i].length() - 4));

                // Set fileName
                String fileName = csvFiles[i].substring(
                        (csvFiles[i].lastIndexOf(LinksSpecific.osSlash()) + 1),
                        (csvFiles[i].length() - 4));


                /**
                 * Get CSV Header
                 */
                String[] headers = ic.importHeader(csvFiles[i]);

                // set subfile
                int counter = 0;

                /**
                 * Create temporary file 
                 */
                FileWriter fw = new FileWriter(tableName + counter);

                /**
                 * Create Log file
                 */
                FileWriter fwLog = new FileWriter(tableName + ".log");

                /**
                 * Write first part of query to file
                 */
                fw.write("INSERT INTO " + tableName + "(");

                // set table fields
                for (int j = 0; j < (headers.length - 1); j++) {
                    fw.write(headers[j] + " , ");
                }

                fw.write(headers[ headers.length - 1] + " ) VALUES \n");

                /**
                 * Create table by using the prefix + 
                 * the name of the file without ".csv"
                 */
                etm.createTable(tableName, headers, 500);

                /**
                 * Inform user
                 */
                addToLog("Created table " + (i + 1) + " of " + csvFiles.length
                        + " from csv file: " + csvFiles[i], false);

                /**
                 * Start reading file
                 */
                addToLog("Reading file : " + fileName + " to database.", false);


                /**
                 * Count the lines to inform the user
                 */
                addToLog("File: " + (i + 1) + " of " + csvFiles.length, false);

                /**
                 * Create buffered reader object to read lines
                 */
                BufferedReader readCount = new BufferedReader(
                        new java.io.FileReader(csvFiles[i]));


                // Counter
                int countLines = 0;

                /**
                 * Counting lines
                 */
                while (readCount.readLine() != null) {
                    countLines++;
                }

                // close BufferReader
                readCount.close();

                /**
                 * Create buffered reader object to read lines 
                 * in Extended ASCII mode, latin 1
                 */
                BufferedReader reader =
                        new BufferedReader(
                        new InputStreamReader(
                        new FileInputStream(
                        csvFiles[i]),
                        "UTF-8"));
                        //"ISO-8859-1"));

                // Set counter
                int lineCounter = 1;

                /**
                 * Reset counters
                 */
                step = skip;
                pFile.setValue(0);
                lineCounter = 1;

                /**
                 * Skip the first line
                 */
                reader.readLine();

                String rawLine = null;

                boolean firstLine = true;

                /**
                 * Loop through the file, line by line
                 */
                while ((rawLine = reader.readLine()) != null) {

                    /**
                     * Replace controlcharacters
                     * 
                     * In the case of genlias, we have to transform some
                     * control characters to diacritical characters
                     * 
                     * For other sources, please do not forget to comment
                     * this part.
                     * 
                     */
                    // rawLine = LinksSpecific.funcToDiacritic(rawLine);
                    /**
                     * Split Rawline by the comma
                     */
                    String[] serperatedLine = rawLine.split(",");

                    /**
                     * Check the amount of fields
                     * 
                     * When there are too many fields, than the last
                     * fields are merged. Please reconsider this
                     * technique when using another source than genlias.
                     * 
                     * We merge the fields because in the case of 
                     * too many fields we expect the remarks field
                     * contains comma's
                     * 
                     * Incase of not enough fields, the line wil be skipped
                     */
                    if (serperatedLine.length < headers.length) {

                        /**
                         * Write error to log file
                         */
                        fwLog.write(
                                "line: " + lineCounter + " in "
                                + fileName
                                + " has not enough fields -> "
                                + rawLine + "\n");

                    } else { //Amount is correct, or more than the fields count

                        if (serperatedLine.length > headers.length) {

                            String[] tempSer = new String[headers.length];

                            for (int j = 0; j < serperatedLine.length; j++) {

                                if (j >= headers.length) {
                                    tempSer[headers.length - 1] += ","
                                            + serperatedLine[j];
                                } else {
                                    tempSer[j] = serperatedLine[j];
                                }
                            }
                            serperatedLine = tempSer;
                        }

                        /**
                         * Remove character that can cause errors
                         */
                        try {

                            // replace chars
                            String[] specialSerperatedLine =
                                    specialChars(serperatedLine);

                            // Write komma AFTER first line
                            if (firstLine) {
                                fw.write("(");
                                firstLine = false;
                            } else {
                                fw.write(",\n(");
                            }

                            // Write fields content
                            for (int j = 0; j < (specialSerperatedLine.length - 1); j++) {
                                fw.write("'" + specialSerperatedLine[j] + "',");
                            }

                            fw.write("'" + specialSerperatedLine[ specialSerperatedLine.length - 1] + "')");

                        } catch (Exception e) {

                            // Write error to log
                            fwLog.write(
                                    "Error in line no: "
                                    + lineCounter
                                    + " , line data: "
                                    + rawLine
                                    + " Error message: "
                                    + e.getMessage() + "\n");

                        }
                    }

                    // Increase counter
                    lineCounter++;

                    /**
                     * Stepcounter to show data on gui
                     * 
                     * Because the showing of data is a 
                     * Resource Expensive operation
                     * We only inform the the user 
                     * ecery n line of code
                     */
                    if (lineCounter == step) {
                        lineNumbers.setText(
                                lineCounter
                                + " of "
                                + countLines
                                + " lines moved to database from file "
                                + (i + 1)
                                + " of "
                                + csvFiles.length);

                        double lineCounterD = lineCounter;
                        double countLinesD = countLines;
                        double percentage1 = (lineCounterD / countLinesD) * 100;
                        int percentage2 = (int) percentage1;
                        pFile.setValue(percentage2);
                        step += skip;

                        /**
                         * 
                         * File operations
                         * 
                         */
                        // Close file writer
                        fw.close();

                        counter++;

                        // Create new file
                        fw = new FileWriter(tableName + counter);


                        /**
                         * Write query header to file
                         */
                        fw.write("INSERT INTO " + tableName + "(");

                        // Set table fields
                        for (int j = 0; j < (headers.length - 1); j++) {
                            fw.write(headers[j] + " , ");
                        }

                        fw.write(headers[ headers.length - 1] + " ) VALUES \n");

                        // Set first line prevent syntax errors in query file
                        firstLine = true;
                    }
                }

                /**
                 * The proces is succesfully done
                 * Inform the user
                 */
                addToLog("Table " + tableName + " is done. ", false);

                /**
                 * The amount of lines the file had
                 */
                addToLog(lineCounter + " lines processed.", false);


                /**
                 * Set the progresbar
                 */
                double filesD = csvFiles.length;
                double fileNumberB = i;
                double perc = (filesD / fileNumberB) * 100;
                pTotal.setValue((int) perc);

                // Close file
                fw.close();

                /**
                 * 
                 * Run a bash command to run SQL query file
                 * to insert the data into the database
                 * 
                 */
                addToLog("Loading files into database.", false);

                // Create Runtime Object
                Runtime runtime = Runtime.getRuntime();

                for (int j = 0; j <= counter; j++) {


                    // Set command to run
                    Process process = runtime.exec(new String[]{
                                "/bin/bash",
                                "-c",
                                "mysql source_internal --user=" + dbUser + " --password=" + dbPass + " < " + tableName + j});

                    // Wait for response from bash
                    int exitValue = process.waitFor();

                    // Show ecit value
                    addToLog("File " + j + " loaded with exit value: " + exitValue, false);

                }


                addToLog("Loading files into database.", false);

                //

                addToLog("Removing temporary files.", false);

                for (int j = 0; j <= counter; j++) {


                    // Set command to run
                    Process process = runtime.exec(new String[]{
                                "/bin/bash",
                                "-c",
                                "rm " + tableName + j});

                    // Wait for response from bash
                    int exitValue = process.waitFor();

                    // Show ecit value
                    addToLog("File " + j + " removed with exit value: " + exitValue, false);

                }

                addToLog("Removing temporary files is done.", false);
            }

            /**
             * Begin with indexing of the fields
             * and change the datatype in the tables
             * to speed up the proces later on
             * 
             * Please reconsider the type of indexes for every new source
             */
            if (it == IndexType.GENLIAS) {

                addToLog("Creating GENLIAS specific indexes.", false);

                // Run index function
                try {
                    indexGenlias();
                    addToLog("Creating GENLIAS specific indexes done.", false);
                } catch (Exception e) {

                    addToLog(
                            "An error occured while creating indexes: "
                            + e.getMessage(), false);

                }
            }

            /**
             * Computing the time the proces used
             */
            long timeExpand = System.currentTimeMillis() - begintime;
            int iTimeEx = (int) (timeExpand / 1000);

            // Inform user
            addToLog("Inserting is done in " + LinksSpecific.stopWatch(iTimeEx), false);

            /**
             * Proces done, Stop thread
             */
            this.stop();

        } catch (Exception e) {

            // General Error
            addToLog("Error: " + e.getMessage(), false);

            // Stop thread
            this.stop();
        }
    }

    /**
     * Use this function to log texts
     * @param logText tekst to log
     * @param isMinOnly if true the text is only showed
     */
    private void addToLog(String logText, boolean isMinOnly) {
        minUpdate.setText(logText);
        if (!isMinOnly) {
            maxUpdate.append(logText + "\r\n");
        }
    }

    /**
     * Clean error sensitive chars in an array of strings
     * @param serperatedLine
     * @return 
     */
    private String[] specialChars(String[] serperatedLine) {
        String[] TempSerperatedLine = serperatedLine;

        // Loop through array
        for (int i = 0; i < serperatedLine.length; i++) {
            TempSerperatedLine[i] = LinksSpecific.funcPrepareForMysql(
                    LinksSpecific.funcCleanSides(serperatedLine[i]));
        }

        return TempSerperatedLine;
    }

    /**
     * This function indexes and changes
     * the column of the tables built from the 
     * genlias data.
     * 
     * @throws Exception when database error
     */
    private void indexGenlias() throws Exception {

        MySqlConnector etm = new MySqlConnector(dbLocation, dbName, dbUser, dbPass);

        /**
         * Run the queries
         */
        etm.runQuery(LinksSpecific.getSqlQuery("GenliasIndexes/GenliasIndexes_q01").replaceAll("<DBPREFIX>", dbPreFix));
        etm.runQuery(LinksSpecific.getSqlQuery("GenliasIndexes/GenliasIndexes_q02").replaceAll("<DBPREFIX>", dbPreFix));
        etm.runQuery(LinksSpecific.getSqlQuery("GenliasIndexes/GenliasIndexes_q03").replaceAll("<DBPREFIX>", dbPreFix));
        etm.runQuery(LinksSpecific.getSqlQuery("GenliasIndexes/GenliasIndexes_q04").replaceAll("<DBPREFIX>", dbPreFix));
        etm.runQuery(LinksSpecific.getSqlQuery("GenliasIndexes/GenliasIndexes_q05").replaceAll("<DBPREFIX>", dbPreFix));
        etm.runQuery(LinksSpecific.getSqlQuery("GenliasIndexes/GenliasIndexes_q06").replaceAll("<DBPREFIX>", dbPreFix));
        etm.runQuery(LinksSpecific.getSqlQuery("GenliasIndexes/GenliasIndexes_q07").replaceAll("<DBPREFIX>", dbPreFix));
    }
}