package dataset;

import modulemain.LinksSpecific;

/**
 *
 * @author oaz
 */
public class PersonC {
    public static String table = "person_c";
    public static String idp = "id_person";


    /**
     *
     * @param field
     * @param value
     * @param id
     * @return
     */
    public static String updateQuery( String field, String value , int id){
        return  "UPDATE " + table
        + " SET " + field + " = '" + LinksSpecific.prepareForMysql(value) + "'"
        + " WHERE " + idp + " = " + id;
    }

    public static String updateIntQuery( String field, String value , int id){
        return  "UPDATE " + table
        + " SET " + field + " = " + LinksSpecific.prepareForMysql(value)
        + " WHERE " + idp + " = " + id;
    }
    
    public static String insertTempFamilyname( int id , String value ){
        return  "insert into familyname_t( person_id , familyname ) " +
        " VALUES( " + id + ", '" + LinksSpecific.prepareForMysql(value) + "' );";
    }
}
