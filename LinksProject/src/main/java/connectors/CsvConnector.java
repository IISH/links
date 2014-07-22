/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package connectors;

/**
 *
 * @author oaz
 */

import javax.swing.JTextArea;
import javax.swing.JTextField;
import java.io.*;

public class CsvConnector extends Thread{

    //tijdelijk
    BufferedWriter writer;
    
    private String path;
    private String OutPutCsvS;
    private JTextField infoField;
    private JTextField dataField;
    private JTextArea outputFieldArea;
    private String filterText;
    private int indexNr;

    /**
     * Basic constructor
     */
    public CsvConnector() {
        // Do nothing
    }

    //TODO: Tijdelijk zeeland drenthe
    public CsvConnector(String path, String OutPutCsv, String filterText, int indexNr, JTextField infoField, JTextField dataField, JTextArea outputFieldArea){
        this.path = path;
        this.OutPutCsvS = OutPutCsv;
        this.infoField = infoField;
        this.dataField = dataField;
        this.outputFieldArea = outputFieldArea;
        this.filterText = filterText;
        this.indexNr = indexNr;
        
        try {
            writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(OutPutCsvS), "ISO-8859-1"));
        } catch (Exception e) {
        }
    }
    
    @Override
    public void run(){

        try {
            long timeExpand = 0;
            long begintime = System.currentTimeMillis();
            int teller = 0;
            int hits = 0;
            int mil = 1000;
            String RawLine;

            //java.io.BufferedReader reader = new BufferedReader(new java.io.FileReader(path));
            BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(path), "ISO-8859-1" ));
            // first read and write header
            String header = reader.readLine();
            appendText(header);

            while ((RawLine = reader.readLine())!= null ) {

                // Teller
                if (teller == mil) {
                    infoField.setText(teller + " lines - HITS: " + hits);
                    mil += 1000;
                }

                // nextLine[] is an array of values from the line
                String[] serperatedLine = RawLine.split(",");

                // filtering
                if ( (serperatedLine.length > indexNr) && (serperatedLine[indexNr].contains(filterText) )) {
                    String lineToAppend = "";
                    hits++;

                    for (int i = 0; i < serperatedLine.length; i++) {
                        lineToAppend += serperatedLine[i] + ",";
                    }

                    lineToAppend = lineToAppend.substring(0, lineToAppend.length()-1);
                    appendText(lineToAppend);
                }

                teller++;
            }

            // Close reader and writer
            reader.close();
            writer.close();

            // TODO: Werkelijke aantallen er bij zetten

            //tijd berekenen
            timeExpand = System.currentTimeMillis() - begintime;
            int iTimeEx = (int)(timeExpand / 1000);

            dataField.setText("DONE IN " + general.Functions.stopWatch(iTimeEx));

            //krijgt 2 sec om daarna hardhandig gestopt te worden
            this.stop();
        }
        catch (Exception e) {
            //do nothing a.t.m.
        }
    }

    public String[] importHeader(String CsvPath) {
        try {
            externalModules.openCsv.CSVReader cr = new externalModules.openCsv.CSVReader(new FileReader(CsvPath));
            return cr.readNext();
        } catch (Exception e) {
            // do nothing
        }
        return null;
    }

    /**
     *
     * @param CsvPath contains the
     * @param serperator contains the serperator char, is it is not a comma
     * @return
     */
    public String[] importHeader(String CsvPath, char serperator) {
        try {
            externalModules.openCsv.CSVReader cr = new externalModules.openCsv.CSVReader(new FileReader(CsvPath),serperator);
            return cr.readNext();
        } catch (Exception e) {
            // do nothing
        }
        return null;
    }

    // Append text to file
    private void appendText(String text){
        try {
            writer.write(text);
            writer.newLine();
        } catch (Exception e) {
        }
    }
}