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

import java.io.FileWriter;

/**
 *  This Class contains logging procedures 
 * @author oaz
 */
public class PrintLogger {

    private String fileName;
    private FileWriter fw;
    private boolean fileIsLocked;
    

    /**
     * Construtor creates a log file
     * with the following structure
     * [datetime].log
     */
    public PrintLogger() throws Exception {
        // Get filename
        fileName = General.getTimeStamp() + ".log";
        fileIsLocked = false;
    }

    /**
     * This method will log the text in
     * a log file, with the following structure
     * [datetime].log
     * @param textToLog Text to log 
     */
    public void show(String textToLog) throws Exception {

        // Write to logfile

        try {
            
            // While file is locked sleep half second
            while(fileIsLocked){
                //Thread.sleep(2000 + new java.util.Random().nextInt(2000));
                
                Thread.sleep(1000);
            }
            
            fileIsLocked = true;
            {
                fw = new FileWriter(fileName, true);
                fw.write(textToLog);
                fw.close();
            }
            fileIsLocked = false;
            
        } catch (Exception e) {
            throw new Exception("Could not write to file " + fileName, e);
        }

        // Print to screen
        System.out.println(textToLog);
    }
}
