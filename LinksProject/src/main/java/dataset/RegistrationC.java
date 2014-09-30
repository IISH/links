package dataset;

import modulemain.LinksSpecific;

/**
 *
 * @author oaz
 */
public class RegistrationC {
    public static String table = "registration_c";
    public static String idp   = "id_registration";


    /**
     *
     * @param field
     * @param value
     * @param id
     * @return
     */
    public static String updateQuery( String field , String value , int id ){
        return  "UPDATE IGNORE " + table
        + " SET " + field + " = '" + LinksSpecific.prepareForMysql( value ) + "'"
        + " WHERE " + idp + " = " + id;
    }


    /**
     * 
     * @param field
     * @param value
     * @param id
     * @return
     */
    public static String updateIntQuery( String field , String value , int id ){
        return  "UPDATE " + table
        + " SET " + field + " = " + value
        + " WHERE " + idp + " = " + id;
    }
}
