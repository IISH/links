package dataset;

import modulemain.LinksSpecific;

/**
 * @author Omar Azouguagh
 * @author Fons Laan
 *
 * FL-01-Sep-2017
 */
public class PersonC {
    public static String table = "person_c";
    public static String idp = "id_person";
    public static String idr = "id_registration";


    /**
     * @param field
     * @param value
     * @param idp
     * @return
     */
    public static String updateQuery( String field, String value , int idp )
    {
        return  "UPDATE " + table
        + " SET " + field + " = '" + LinksSpecific.prepareForMysql( value ) + "'"
        + " WHERE " + idp + " = " + idp;
    }

    /**
     * @param field
     * @param value
     * @param idr
     * @return
     */
    public static String updateQueryByReg( String field, String value , int idr )
    {
        return  "UPDATE " + table
        + " SET " + field + " = '" + LinksSpecific.prepareForMysql( value ) + "'"
        + " WHERE " + idr + " = " + idr;
    }


     /**
     * @param field
     * @param value
     * @param idp
     * @return
     */
    public static String updateIntQuery( String field, String value , int idp )
    {
        return  "UPDATE " + table
        + " SET " + field + " = " + LinksSpecific.prepareForMysql( value )
        + " WHERE " + idp + " = " + idp;
    }


     /**
     * @param id
     * @param value
     * @return
     */
    public static String insertTempFamilyname( int id , String value )
    {
        return  "insert into familyname_t( person_id , familyname ) " +
        " VALUES( " + id + ", '" + LinksSpecific.prepareForMysql( value ) + "' );";
    }
}
