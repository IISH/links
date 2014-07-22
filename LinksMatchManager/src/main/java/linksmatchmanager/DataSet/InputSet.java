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

// Imports
import java.util.ArrayList;

/**
 * This iclass contains an ArrayList
 * of QueryGroupSets. This class
 * is the root class that contains 
 * a structure of queries
 * 
 * @author oaz
 */
public class InputSet {

    /**
     * We use an ArrayList because of 
     * its posibilities to add new items
     * 
     */
    private ArrayList<QueryGroupSet> al = new ArrayList<QueryGroupSet>();

    /**
     * 
     * @param index
     * @return 
     */
    public QueryGroupSet get(int index) {
        return al.get(index);
    }

    /**
     * 
     * @param qgs 
     */
    public void add(QueryGroupSet qgs) {
        al.add(qgs);
    }

    /**
     * 
     * @return 
     */
    public int getSize() {
        return al.size();
    }
}
