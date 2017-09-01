package dataset;

import modulemain.LinksSpecific;

/**
 * @author Omar Azouguagh
 * @author Fons Laan
 *
 * FL-01-Sep-2017
 */
public class RegistrationC
{
    /**
     * @param field
     * @param value
     * @param id
     * @return
     */
    public static String updateQuery( String field , String value , int id )
    {
        return  "UPDATE IGNORE registration_c"
        + " SET " + field + " = '" + LinksSpecific.prepareForMysql( value ) + "'"
        + " WHERE id_registration = " + id;
    }


    /**
     * 
     * @param field
     * @param value
     * @param id
     * @return
     */
    public static String updateIntQuery( String field , String value , int id )
    {
        return  "UPDATE registration_c"
        + " SET " + field + " = " + value
        + " WHERE id_registration = " + id;
    }
}
