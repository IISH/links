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

import java.text.SimpleDateFormat;

import java.util.Calendar;

//import java.sql.Connection;
//import java.sql.DriverManager;

/**
 * @author Omar Azouguagh
 * @author Fons Laan
 *
 * FL-14-Jul-2017 autoReconnect=true to prevent connection timeouts
 */
public class General {
    
    /**
     * 
     * @return 
     */
    public static String getTimeStamp() {
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
    /*
    public static Connection getConnection(String url, String db, String user, String pass) 
            throws Exception {
        String driver = "org.gjt.mm.mysql.Driver";

        //String longUrl = "jdbc:mysql://" + url + "/" + db + "?dontTrackOpenResources=true";
        String longUrl = "jdbc:mysql://" + url + "/" + db + "?dontTrackOpenResources=true&autoReconnect=true";

        Class.forName(driver);
        return DriverManager.getConnection(longUrl, user, pass);
    }
    */

    /**
     *
     * @return
     */
    public static String getTimeStamp2( String format ) {
        Calendar cal = Calendar.getInstance();
        SimpleDateFormat sdf = new SimpleDateFormat( format );
        return sdf.format(cal.getTime());
    }

}
