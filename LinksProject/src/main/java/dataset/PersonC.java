package dataset;

import modulemain.LinksSpecific;

/**
 * @author Omar Azouguagh
 * @author Fons Laan
 *
 * FL-01-Sep-2017
 */
public class PersonC
{
    /**
     * @param field
     * @param value
     * @param id
     * @return
     */
    public static String updateQuery( String field, String value , int id )
    {
        return  "UPDATE person_c"
        + " SET " + field + " = '" + LinksSpecific.prepareForMysql( value ) + "'"
        + " WHERE id_person = " + id;
    }


    /**
     * @param field
     * @param value
     * @param id
     * @return
     */
    public static String updateQueryByReg( String field, String value , int id )
    {
        return  "UPDATE person_c"
        + " SET " + field + " = '" + LinksSpecific.prepareForMysql( value ) + "'"
        + " WHERE id_registration = " + id;
    }


     /**
     * @param field
     * @param value
     * @param id
     * @return
     */
    public static String updateIntQuery( String field, String value , int id )
    {
        return  "UPDATE person_c"
        + " SET " + field + " = " + LinksSpecific.prepareForMysql( value )
        + " WHERE id_person = " + id;
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
