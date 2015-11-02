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

import java.util.ArrayList;

/**
 * This Class contains an array of query objects that are generated from one
 * record in the match_process table
 * 
 * @author oaz
 */
public class QueryGroupSet
{
    // We use an ArrayList because of its possibilities to add new items

    private ArrayList< QuerySet > al = new ArrayList< QuerySet >();

    /**
     * Use this method to get an individual QuerySet object
     * 
     * @param index index of the QuerySet in the ArrayList
     * @return QuerySet object
     */
    public QuerySet get( int index ) {
        return al.get( index );
    }

    /**
     * Use this method to add a QuerySet object to the array
     *
     * @param qs QuerySet object to add 
     */
    public void add( QuerySet qs ) {
        al.add( qs );
    }

    /**
     * Returns the size of the ArrayList
     *
     * @return size
     */
    public int getSize() {
        return al.size();
    }
}
