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
 * @author Omar Azouguagh
 * @author Fons Laan
 *
 * FL-28-Apr-2015 Latest change
 */
public class QuerySet
{
    public String  query1;              // query to get s1 data set from links_base
    public String  query2;              // query to get s1 data set from links_base

    public int s1_days_low  = 0;
    public int s2_days_low  = 0;
    public int s1_days_high = 0;
    public int s2_days_high = 0;

    public int id;                      // id from match_process table

    public boolean use_mother  = false;
    public boolean use_father  = false;
    public boolean use_partner = false;

    public int     method = 0;
    public boolean ignore_sex    = false;
    public boolean ignore_minmax = false;
    public int     firstname = 0;

    public String prematch_familyname;
    public int    prematch_familyname_value;
    public String prematch_firstname;
    public int    prematch_firstname_value;

    //public String use_familyname;
    //public String use_firstname;
    //public String use_minmax;
    
    public int int_familyname_e;
    public int int_familyname_m;
    public int int_familyname_f;
    public int int_familyname_p;
           
    public int int_firstname_e;
    public int int_firstname_m;
    public int int_firstname_f;
    public int int_firstname_p;
            
    public int int_minmax_e;
    public int int_minmax_m;
    public int int_minmax_f;
    public int int_minmax_p;
}
