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
package regexreplacer;

import java.text.SimpleDateFormat;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.*;
import java.io.*;

/**
 *
 * @author oaz
 */
public class General {
    
    /**
     * 
     * @return 
     */
    public static String getTimeStamp(){
        Calendar cal = Calendar.getInstance();
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmm");
        return sdf.format(cal.getTime());
    }
    
    /**
     * 
     * @param seconds
     * @return 
     */
    public static String stopWatch(int seconds){
        
        // Devide the seconds to get hours and minutes
        int minutes = seconds / 60;
        int restsec = seconds % 60;
        int uren = minutes / 60;
        int restmin = minutes % 60;

        String urenText = "";
        String minutenText = "";
        String secondenText = "";

        // Add zero if a digit is lower than 10
        if(uren < 10 ) urenText = "0";
        if(restmin < 10 ) minutenText = "0";
        if(restsec < 10 ) secondenText = "0";

        // Return clean String
        return 
                urenText + uren + ":" + 
                minutenText + restmin + ":" + 
                secondenText + restsec ;
    }
    
    /**
     * 
     * @param url
     * @param user
     * @param pass
     * @return
     * @throws Exception 
     */
    public static Connection getConnection(String url, String db, String user, String pass) 
            throws Exception {
        String driver = "org.gjt.mm.mysql.Driver";
        String longUrl = "jdbc:mysql://" + url + "/" + db + "?dontTrackOpenResources=true";

        Class.forName(driver);
        return DriverManager.getConnection(longUrl, user, pass);
    }
    
   public static String prepareForMysql(String line){
        String line1 = line.replaceAll("\\\\", "\\\\\\\\");
        String line2 = line1.replaceAll("'", "\\\\'");
        return line2;
    }
   
   public static ArrayList FileToArray(String fileName) throws Exception {
        String line = "";
        ArrayList<String> data = new ArrayList<String>();

        FileReader fr = new FileReader(fileName);
        BufferedReader br = new BufferedReader(fr);
        while ((line = br.readLine()) != null) {
            data.add(line);
        }
        return data;
    }
}