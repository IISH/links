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
 * FL-16-Aug-2017 Add chk_* flags (for date range min/max purpose)
 */
public class QuerySet
{
    public String  s1_query;            // query to get s1 data set from links_base
    public String  s2_query;            // query to get s2 data set from links_base

    public int id;                      // id from match_process table

    public int s1_record_count = 0;
    public int s2_record_count = 0;

    public int s1_maintype  = 0;
    public int s2_maintype  = 0;

    public int s1_days_low  = 0;
    public int s2_days_low  = 0;

    public int s1_days_high = 0;
    public int s2_days_high = 0;

    public boolean use_mother  = false;
    public boolean use_father  = false;
    public boolean use_partner = false;
    public boolean ignore_minmax = false;

    //public boolean ignore_sex    = false;
    // "y", "n", "m", "f"; "m": Ego = "male" + Partner = "female", "f": Ego = "female" + Partner = "m"ale
    public String ignore_sex = "y";

    public int     method = 0;
    public int     firstname_method = 0;

    public String prematch_familyname;
    public int    lvs_dist_max_familyname;
    public String prematch_firstname;
    public int    lvs_dist_max_firstname;

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

    // these 4 minmax inputs from the match_process table
    public int int_minmax_e;
    public int int_minmax_m;
    public int int_minmax_f;
    public int int_minmax_p;

    // these derived chk minmax flags are easier to use (computed once)
    public boolean chk_ego_birth    = false;
    public boolean chk_ego_marriage = false;
    public boolean chk_ego_death    = false;

    public boolean chk_mother_birth    = false;
    public boolean chk_mother_marriage = false;
    public boolean chk_mother_death    = false;

    public boolean chk_father_birth    = false;
    public boolean chk_father_marriage = false;
    public boolean chk_father_death    = false;

    public boolean chk_partner_birth    = false;
    public boolean chk_partner_marriage = false;
    public boolean chk_partner_death    = false;

    public int date_range = 0;      // moving date window counter

    public int s1_offset = 0;       // s1 query OFFSET
    public int s2_offset = 0;       // s2 query OFFSET

    public int s1_limit = 0;        // s1 query LIMIT
    public int s2_limit = 0;        // s2 query LIMIT


    public QuerySet copyQuerySet()
    {
        QuerySet qs = new QuerySet();

        qs.s1_query = s1_query;
        qs.s2_query = s2_query;

        qs.id = id;

        qs.s1_record_count = s1_record_count;     // in using the copy, must be replaced by actual COUNT(*) value
        qs.s2_record_count = s2_record_count;     // in using the copy, must be replaced by actual COUNT(*) value

        qs.date_range = date_range;

        qs.s1_maintype  = s1_maintype;
        qs.s2_maintype  = s2_maintype;

        qs.s1_days_low  = s1_days_low;
        qs.s2_days_low  = s2_days_low;

        qs.s1_days_high = s1_days_high;
        qs.s2_days_high = s2_days_high;

        qs.use_mother  = use_mother;
        qs.use_father  = use_father;
        qs.use_partner = use_partner;

        qs.method           = method;
        qs.ignore_sex       = ignore_sex;
        qs.ignore_minmax    = ignore_minmax;
        qs.firstname_method = firstname_method;

        qs.prematch_familyname     = prematch_familyname;
        qs.lvs_dist_max_familyname = lvs_dist_max_familyname;
        qs.prematch_firstname      = prematch_firstname;
        qs.lvs_dist_max_firstname  = lvs_dist_max_firstname;

        //public String use_familyname;
        //public String use_firstname;
        //public String use_minmax;

        qs.int_familyname_e = int_familyname_e;
        qs.int_familyname_m = int_familyname_m;
        qs.int_familyname_f = int_familyname_f;
        qs.int_familyname_p = int_familyname_p;

        qs.int_firstname_e = int_firstname_e;
        qs.int_firstname_m = int_familyname_m;
        qs.int_firstname_f = int_firstname_f;
        qs.int_firstname_p = int_firstname_p;

        qs.int_minmax_e = int_minmax_e;
        qs.int_minmax_m = int_minmax_m;
        qs.int_minmax_f = int_minmax_f;
        qs.int_minmax_p = int_minmax_p;

        qs.chk_ego_birth    = chk_ego_birth;
        qs.chk_ego_marriage = chk_ego_marriage;
        qs.chk_ego_death    = chk_ego_death;

        qs.chk_mother_birth    = chk_mother_birth;
        qs.chk_mother_marriage = chk_mother_marriage;
        qs.chk_mother_death    = chk_mother_death;

        qs.chk_father_birth    = chk_father_birth;
        qs.chk_father_marriage = chk_father_marriage;
        qs.chk_father_death    = chk_father_death;

        qs.chk_partner_birth    = chk_partner_birth;
        qs.chk_partner_marriage = chk_partner_marriage;
        qs.chk_partner_death    = chk_partner_death;

        qs.s1_offset = s1_offset;
        qs.s2_offset = s2_offset;

        qs.s1_limit = s1_limit;
        qs.s2_limit = s2_limit;

        return qs;
    }
}

// [eof]
