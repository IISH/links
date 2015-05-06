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

package general;

import java.io.FileWriter;

import com.google.common.base.Strings;

import modulemain.LinksSpecific;

/**
 * This Class contains logging procedures
 * @author Omar Azouguagh
 * @author Fons Laan
 * FL-06-May-2015 Latest Change
 */
public class PrintLogger
{
    private String fileName;
    private String pathName;
    private FileWriter fw;
    private boolean fileIsLocked;
    private static String intextPrevious;

    /**
     * Construtor creates a log file
     * with the following structure
     * [datetime].log
     */
    public PrintLogger() throws Exception
    {
        // create log filename
        //String format = "yyyy-MM-dd'T'HH:mm:ssz";
        String format = "yyyy-MM-dd_HH:mm:ss";
        String ts = LinksSpecific.getTimeStamp2( format );
        fileName = "LDM-" + ts + ".log";
        pathName = "log/" + fileName;
        fileIsLocked = false;
        intextPrevious = "";
        System.out.printf( "Log filename: %s\n", pathName );
    }


    /**
     * This method will log the text in a log file
     * @param intext Text to log
     */
    public void show( String intext ) throws Exception
    {
        // why do i get all intext strings twice ??
        if( intext == intextPrevious ) { return; }

        String text = intext;
        // empty line without timestamp
        if( ! Strings.isNullOrEmpty( intext ) && ! intext.trim().isEmpty() ) {
            String ts = LinksSpecific.getTimeStamp2( "HH:mm:ss" );
            text = ts + " " + text;
        }

        try
        {
            // While file is locked sleep a bit
            while( fileIsLocked ) {
                //Thread.sleep(2000 + new java.util.Random().nextInt(2000));
                Thread.sleep( 1000 );
            }
            
            fileIsLocked = true;
            {
                fw = new FileWriter( pathName, true );
                fw.write( text  + "\r\n" );
                fw.close();
            }
            fileIsLocked = false;
            
        }
        catch( Exception ex ) {
            String msg = String.format( "Could not write to file: %s,\n%s", pathName, intext );
            throw new Exception( "Could not write to file " + pathName, ex );
        }

        intextPrevious = intext;
    }
}
