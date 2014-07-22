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
package linksmatchmanager.DataSet;

/**
 *
 * @author oaz
 */
public class NameAndTypeSet {

    public int name;
    public int frequency;
    public NameType nameType;

    /**
     * 
     * @param name
     * @param frequency
     * @param nameType 
     */
    public NameAndTypeSet(int name, int frequency, NameType nameType) {
        
        this.name = name;
        this.frequency = frequency;
        this.nameType = nameType;
    
    }
    
    /**
     * 
     * @param name
     * @param nameType 
     */
    public NameAndTypeSet(int name, NameType nameType) {
      
        this.name = name;
        this.nameType = nameType;
    
    }
    
    /**
     * 
     * @param name
     * @param frequency 
     */
    public NameAndTypeSet(int name, int frequency ) {
        
        this.name = name;
        this.frequency = frequency;
    
    }
    
}
