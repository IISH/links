import dataSet.*;
import java.util.*;
import java.io.File;
import javax.swing.*;
import java.util.regex.*;
import javax.xml.parsers.*;
import java.io.IOException;
import org.w3c.dom.Document;
import javax.xml.transform.*;
import javax.swing.JTextArea;
import javax.swing.JTextField;
import java.io.FileInputStream;
import javax.xml.transform.dom.*;
import java.io.BufferedInputStream;
import javax.xml.transform.stream.*;
import javax.swing.filechooser.FileFilter;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.swing.filechooser.FileNameExtensionFilter;

/**
 *
 * @author oaz
 */
public class Functions {

    /**
     * 
     * @param lineToRepare
     * @return
     */
//    public final static String funcToDiacritic(String lineToRepare){
//        lineToRepare = lineToRepare.
//            replaceAll("\\x00", "Ç").
//            replaceAll("\\x01", "ü").
//            replaceAll("\\x02", "é").
//            replaceAll("\\x03", "â").
//            replaceAll("\\x04", "ä").
//            replaceAll("\\x05", "à").
//            replaceAll("\\x06", "å").
//            replaceAll("\\x07", "ç").
//            replaceAll("\\x08", "ê").
//            replaceAll("\\x09", "ë").
//            replaceAll("\\x0B", "ï").
//            replaceAll("\\x0C", "î").
//            replaceAll("\\x0E", "Ä").
//            replaceAll("\\x0F", "Å").
//            replaceAll("\\x10", "é").
//            replaceAll("\\x11", "æ").
//            replaceAll("\\x12", "Æ").
//            replaceAll("\\x13", "ô").
//            replaceAll("\\x14", "ö").
//            replaceAll("\\x15", "ò").
//            replaceAll("\\x16", "û").
//            replaceAll("\\x17", "ù").
//            replaceAll("\\x18", "ÿ").
//            replaceAll("\\x19", "Ö").
//            replaceAll("\\x1A", "Ü").
//            replaceAll("\\x1B", "ç");
//        return lineToRepare;
//    }

    /**
     *
     * @param line
     * @return
     */
//    public static String funcPrepareForMysql(String line){
//        String line1 = line.replaceAll("\\\\", "\\\\\\\\");
//        String line2 = line1.replaceAll("'", "\\\\'");
//        //String line3 = line2.replaceAll("\"", "\\\\\"");
//        return line2;
//    }

    public static String funcPrepareForMysql(String line){
        return line.replaceAll("'", "''");
    }


    /**
     * 
     * @param line
     * @return
     */
    public static String funcCleanSides(String line){
        while(  (line.length() > 0) && (line.substring(0,1).equals("\"") ||
                line.substring(0,1).equals(" ") ||
                line.substring(line.length()-1,line.length()).equals("\"") ||
                line.substring(line.length()-1,line.length()).equals( " " ) ) ){
            
            String begin = line.substring(0,1);
            String end = line.substring(line.length()-1,line.length());
            
            if( begin.equals("\"") || begin.equals(" ") ){
                line = line.substring( 1 , line.length() );
            }
            else if( end.equals("\"") ||end.equals(" ") ){
                line = line.substring( 0 , ( line.length() -1 ) );
            }
        }
        return line;
    }



    /**
     *
     * @return
     */
    public static Document createDomDocument() {
        try {
            return DocumentBuilderFactory.newInstance().newDocumentBuilder().newDocument();
        }
        catch (Exception e) {
            // Kan nooit fout gaan
        }
        return null;
    }



    /**
     *
     * @param path
     * @return
     * @throws Exception
     */
    public static Document LoadXmlFromfile(String path) throws Exception{
        File file = new File(path);
        DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
        DocumentBuilder db = dbf.newDocumentBuilder();
        Document doc = db.parse(file);
        doc.getDocumentElement().normalize();
        return doc;
    }

    public static String SelectFileWithCheck(String extension){
        JFileChooser fileopen = new JFileChooser();
        FileFilter filter = new FileNameExtensionFilter( extension.toUpperCase() + " File (." + extension + ")", extension);
        fileopen.addChoosableFileFilter(filter);

        int ret = fileopen.showDialog(null, "Select/Create file");

        if (ret == JFileChooser.APPROVE_OPTION) {
            File file = fileopen.getSelectedFile();

            String extention = "";

            if(!(file.getPath().substring((file.getPath().length() -4 ), file.getPath().length())).toLowerCase().equals("." + extension)){
                extention = "." + extension;
            }

            return file.getPath() + extention;
        }
        return "";
    }



    /**
     * This method writes a DOM document to a file
     * @param doc
     * @param filename
     */
    public static void writeXmlToFile(Document doc, String filename) {
        try {
            // Prepare the DOM document for writing
            Source source = new DOMSource(doc);

            // Prepare the output file
            File file = new File(filename);
            Result result = new StreamResult(file);

            // Write the DOM document to the file
            Transformer xformer = TransformerFactory.newInstance().newTransformer();
            xformer.transform(source, result);
        } 
        catch (TransformerConfigurationException e) {} 
        catch (TransformerException e) {}
    }

    
    
    /**
     *
     * @param extension The extension to filter, leave empty for none-filtering
     * @param multiple True if you want to select more than one file.
     * @return Path of selected file.
     */
    public static String OpenFile(String extension, boolean multiple){
        JFileChooser fileopen = new JFileChooser();
        FileFilter filter = new FileNameExtensionFilter( extension.toUpperCase() + " File (." + extension + ")", extension);
        fileopen.addChoosableFileFilter(filter);
        
        if(multiple){fileopen.setMultiSelectionEnabled(true);}

        int ret = fileopen.showDialog(null, "Open file");

        if (ret == JFileChooser.APPROVE_OPTION) {
            if(multiple){
                File[] file = fileopen.getSelectedFiles();
                String fileLocations = "";
                for (int i = 0; i < file.length; i++) {
                    if (i != (file.length-1)) {
                        fileLocations += file[i].getPath() + ",";
                    }
                    else {
                        fileLocations += file[i].getPath();
                    }
                }
                return fileLocations;
            }
            else {
                File file = fileopen.getSelectedFile();
                return file.getPath();
            }
        }
        return "";
    }

    
    
    /**
     *
     * @return true als het om ene Windows OS gaat
     */
    public static boolean isWindows(){
        String os = System.getProperty("os.name").toLowerCase();
        return (os.indexOf( "win") >=0);
    }
    
    
    /**
     * Get OS version
     * @return true if OS is linux, or unix
     */
    private static boolean isUnix() {
        String os = System.getProperty("os.name").toLowerCase();
        return (os.indexOf("nix") >= 0 || os.indexOf("nux") >= 0);
    }

    public static String functieOpmerkingenWaardeNaDubbelePunt(String opmerking) {
        return funcCleanSides(opmerking.split(":")[1].replaceAll(";", ""));
    }

    public static void addLogToGui(String logText, boolean isMinOnly, JTextField tb, JTextArea ta){
        tb.setText(logText);
        if(!isMinOnly){
            ta.append(logText + "\r\n");
        }
    }

    
    
    /**
     * 
     * @return
     */
    public static String getTimeStamp(){
        Calendar cal = Calendar.getInstance();
        java.text.SimpleDateFormat sdf = new java.text.SimpleDateFormat("yyyyMMddHHmm");
        return sdf.format(cal.getTime());
    }

    
    
    /**
     * 
     * @param seconds
     * @return
     */
    public static String stopWatch(int seconds){
        int minutes = seconds / 60;
        int restsec = seconds % 60;
        int uren = minutes / 60;
        int restmin = minutes % 60;

        String urenText = "";
        String minutenText = "";
        String secondenText = "";

        if(uren < 10 ) urenText = "0";
        if(restmin < 10 ) minutenText = "0";
        if(restsec < 10 ) secondenText = "0";

        return urenText + uren + ":" + minutenText + restmin + ":" + secondenText + restsec;
    }



    /**
     * Gebruik deze functie om een datum te controleren en te splitsen
     * De datum kan splitchars bevatten als -, / \ [] etc.
     * Formaat kan zijn: d-M-yyyy or dd-M-yyyy or d-MM-yyyy or dd-MM-yyyy
     * @param Te controleren datum
     * @return een DateYearMonthDay object met datum en evt. fouten
     */
    public static DateYearMonthDaySet devideCheckDate(String date){

        dataSet.DateYearMonthDaySet dymd = new dataSet.DateYearMonthDaySet();

        if(date == null || date.isEmpty() ){
            dymd.setYear( 0 ) ;
            dymd.setMonth( 0 ) ;
            dymd.setDay( 0 ) ;
            dymd.setReportYear( "ERROR" ) ;
            dymd.setReportMonth( "ERROR" ) ;
            dymd.setReportDay( "ERROR" ) ;

            return dymd;
        }

        // eerst de data er uit met regex
        Pattern regex = Pattern.compile("[0-9]+");
        Matcher m = regex.matcher(date);

        //
        int day = 0;
        int month = 0;
        int year = 0;
        
        // day
        if(m.find()){
            day = Integer.parseInt(m.group());
        }

        //month
        if(m.find()){
            month = Integer.parseInt(m.group());
        }

        //year
        if(m.find()){
            year = Integer.parseInt(m.group());
        }

        // they are devides, now we check the value on errors
        if( (year > 1680) && ( year < 1960 ) ){
            dymd.setYear(year);
        }
        else {
            // MELDING
            dymd.setReportYear("ERROR");
            dymd.setYear(0);
        }

         // they are devides, now we check the value on errors
        if( (month > 0) && ( month < 13 ) ){
            dymd.setMonth(month);
        }
        else {
            // MELDING
            dymd.setReportMonth("ERROR");
            dymd.setMonth(0);
        }

        // check day number
        if( ( day < 1 ) && ( day > 31 ) ){
            // MELDING
            dymd.setReportDay("ERROR");
            dymd.setDay(0);
        }

        //check if day is 31 in wrong month
        else if( ( day == 31 ) && ( ( month == 2 ) || ( month == 4 ) || ( month == 6 ) || ( month == 9 ) || ( month == 11 ) ) ){
            // MELDING
            dymd.setReportDay("ERROR");
            dymd.setDay(0);
        }
        // Check if februari has 30 days
        else if( ( month == 2 ) && ( day == 30 ) ){
            // MELDING
            dymd.setReportDay("ERROR");
            dymd.setDay(0);
        }
        // leap year calculation
        else if ( ( month == 2 ) && ( day == 29 ) ){
            if( ( year % 4 == 0 ) && ( year % 100 != 0 ) || ( year % 400 == 0 ) ) { // is leapyear
                dymd.setDay(day);
            }
            else {
                // MELDING
                dymd.setReportDay("ERROR");
                dymd.setDay(0);
            }
        }
        else { //februari with 28 or lesser days
            dymd.setDay(day);
        }
        return dymd;
    }
    
     /**
     * Decide wich slash to use
     * @return slash to use
     */
    public static String osSlash() {
        if (isUnix()) {
            return ("/");
        } else {
            return ("\\");
        }
    }
    
        /**
     *
     * @param file
     * @return
     * @throws java.io.IOException
     */
    public static String getSqlQuery(String file) throws java.io.IOException {
        byte[] buffer = new byte[(int) new File("resources/SqlQuery/" + file + ".sql").length()];
        
        BufferedInputStream f = null;
        
        try {
            f = new BufferedInputStream(new FileInputStream("resources/SqlQuery/" + file + ".sql"));
            f.read(buffer);
        } finally {
            if (f != null) {
                try {
                    f.close();
                } catch (IOException ignored) {
                }
            }
        }
        return new String(buffer);
    }
    
    /**
     * (Temporary) disabled functions
     */
}