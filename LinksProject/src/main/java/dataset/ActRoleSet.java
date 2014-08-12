package dataset;

import java.util.*;
import java.sql.*;

public class ActRoleSet {

    ArrayList<String> ActRoleList = new ArrayList<String>();
    ArrayList<String> relationList = new ArrayList<String>();

    
    
    /**
     *
     * @param role1
     * @param role2
     * @param persoon1
     * @param persoon2
     */
    public void addToList( String role1 , String role2 , String sex1 , String sex2 , String relation ){

        String akteRolTemp = "";
        
        akteRolTemp += role1.toLowerCase() ;
        akteRolTemp += role2.toLowerCase() ;
        
        // evt. nulls en empties opvangen
        if( sex1 == null || sex1.isEmpty() ){
            // Do nothing
        }
        else {
            akteRolTemp += sex1.toLowerCase() ;
        }
        
        if( sex2 == null || sex2.isEmpty() ){
            // do nothing
        }
        else {
            akteRolTemp += sex2.toLowerCase() ;
        }

        // Vul arraylists
        ActRoleList.add( akteRolTemp );
        relationList.add( relation ) ;
    }
    
    
    /**
     * 
     * @param rs
     */
    public void addRessultSetToList( ResultSet rs ) throws Exception{

        while( rs.next() ){

            String rol1 = rs.getString("role_person_1");
            String rol2 = rs.getString("role_person_2");
            String geslacht1 = rs.getString("sex_person_1");
            String geslacht2 = rs.getString("sex_person_2");
            String relatie = rs.getString("relationship");

            addToList( rol1 , rol2 , geslacht1 , geslacht2 , relatie );
            
        }
    }
    
    
    
    /**
     * 
     * @param role1
     * @param role2
     * @param sex1
     * @param sex2
     * @return
     */
    public String getRelatie( String role1 , String role2 , String sex1 , String sex2 ){
        
        String workingRole1 ;
        String workingRole2 ;
        String workingSex1 ;
        String workingSex2 ;
        
        // null problem
        if(role1 == null || role2 == null){
            return "";
        }
        
        workingRole1 = role1.toLowerCase();
        workingRole2 = role2.toLowerCase();
        
        // catch nulls and empties
        if( sex1 == null || sex1.isEmpty() ){
            workingSex1 = "" ;
        }
        else {
            workingSex1 = sex1.toLowerCase() ;
        }
        
        if( sex2 == null || sex2.isEmpty() ){
            workingSex2 = "" ;
        }
        else {
            workingSex2 = sex2.toLowerCase() ;
        }
        
        /**
         * Find record
         */
        String withSex = workingRole1 + workingRole2 + workingSex1 + workingSex2 ;
        String withoutSex = workingRole1 + workingRole2 ;
        
        if( ActRoleList.contains( withSex ) ){
        
            return relationList.get( ActRoleList.indexOf( withSex ) );
        
        }
        
        else if( ActRoleList.contains( withoutSex ) ){
        
            return relationList.get( ActRoleList.indexOf( withoutSex ) );
        
        }
        
        // Nothing found
        return "" ;
    }
}
