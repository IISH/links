/*
Copyright (C) 2009-present IISH (www.iisg.nl)

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
package dataset;

import general.PrintLogger;

/**
 * @author Omar Azouguagh
 * @author Fons Laan
 *
 * <p/>
 * FL-30-Jun-2014 Imported from OA backup
 * FL-10-Oct-2014 Occupation added
 * FL-10-Oct-2014 Renamed DoSet -> Options
 * FL-14-Oct-2014 DB opts
 * FL-12-Nov-2014 dbg
 * FL-22-Jan-2015 int sourceId -> String sourceIds
 * FL-05-Feb-2015 RemoveDuplicates
 * FL-09-Feb-2015 Latest change
 */
public class Options
{
    PrintLogger plog;

    private String sourceIds;

    // db options
    private String db_ref_url;
    private String db_ref_user;
    private String db_ref_pass;
    private String db_ref_db;
    private String db_url;
    private String db_user;
    private String db_pass;

    // cleaning options
    private boolean doDebug;

    private boolean doRenewData;
    private boolean dbgRenewData;

    private boolean doPrepieceSuffix;
    private boolean dbgPrepieceSuffix;

    private boolean doFirstnames;
    private boolean dbgFirstnames;

    private boolean doFamilynames;
    private boolean dbgFamilynames;

    private boolean doLocations;
    private boolean dbgLocations;

    private boolean doStatusSex;
    private boolean dbgStatusSex;

    private boolean doRegType;
    private boolean dbgRegType;

    private boolean doOccupation;
    private boolean dbgOccupation;

    private boolean doAge;
    private boolean dbgAge;

    private boolean doRole;
    private boolean dbgRole;

    private boolean doDates;
    private boolean dbgDates;

    private boolean doMinMaxMarriage;
    private boolean dbgMinMaxMarriage;

    private boolean doPartsToFullDate;
    private boolean dbgPartsToFullDate;

    private boolean doDaysSinceBegin;
    private boolean dbgDaysSinceBegin;

    private boolean doPostTasks;
    private boolean dbgPostTasks;

    private boolean doRemoveDuplicates;
    private boolean dbgRemoveDuplicates;

    private boolean doPrematch;
    private boolean dbgPrematch;

    private boolean doMatch;
    private boolean dbgMatch;


    public void setLogger( PrintLogger plog ) { this.plog = plog; }

    public PrintLogger getLogger() { return plog; }


    public void setSourceIds( String sourceIds ) { this.sourceIds = sourceIds; }

    public String getSourceIds() { return sourceIds; }


    // db options
    public void setDb_ref_url( String db_ref_url ) { this.db_ref_url = db_ref_url; }

    public String getDb_ref_url() { return db_ref_url; }


    public void setDb_ref_user( String db_ref_user ) { this.db_ref_user = db_ref_user; }

    public String getDb_ref_user() { return db_ref_user; }


    public void setDb_ref_pass( String db_ref_pass ) { this.db_ref_pass = db_ref_pass; }

    public String getDb_ref_pass() { return db_ref_pass; }


    public void setDb_ref_db( String db_ref_db ) { this.db_ref_db = db_ref_db; }

    public String getDb_ref_db() { return db_ref_db; }


    public void setDb_url( String db_url ) { this.db_url = db_url; }

    public String getDb_url() { return db_url; }


    public void setDb_user( String db_user ) { this.db_user = db_user; }

    public String getDb_user() { return db_user; }


    public void setDb_pass( String db_pass ) { this.db_pass = db_pass; }

    public String getDb_pass() { return db_pass; }


    // cleaning options
    public void setDoDebug( boolean doDebug ) { this.doDebug = doDebug; }

    public boolean isDoDebug() { return doDebug; }


    public boolean  isDoRenewData() { return  doRenewData; }
    public boolean isDbgRenewData() { return dbgRenewData; }

    public void  setDoRenewData( boolean  doRenewData ) { this.doRenewData  =  doRenewData; }
    public void setDbgRenewData( boolean dbgRenewData ) { this.dbgRenewData = dbgRenewData; }


    public boolean  isDoPrepieceSuffix() { return  doPrepieceSuffix; }
    public boolean isDbgPrepieceSuffix() { return dbgPrepieceSuffix; }

    public void  setDoPrepieceSuffix( boolean  doPrepieceSuffix ) { this.doPrepieceSuffix  =  doPrepieceSuffix; }
    public void setDbgPrepieceSuffix( boolean dbgPrepieceSuffix ) { this.dbgPrepieceSuffix = dbgPrepieceSuffix; }


    public boolean  isDoFirstnames() { return  doFirstnames; }
    public boolean isDbgFirstnames() { return dbgFirstnames; }

    public void  setDoFirstnames( boolean  doFirstnames ) { this.doFirstnames  = doFirstnames; }
    public void setDbgFirstnames( boolean dbgFirstnames ) { this.dbgFirstnames = dbgFirstnames; }


    public boolean  isDoFamilynames() { return  doFamilynames; }
    public boolean isDbgFamilynames() { return dbgFamilynames; }

    public void  setDoFamilynames( boolean  doFamilynames ) { this.doFamilynames  = doFamilynames; }
    public void setDbgFamilynames( boolean dbgFamilynames ) { this.dbgFamilynames = dbgFamilynames; }


    public boolean  isDoLocations() { return  doLocations; }
    public boolean isDbgLocations() { return dbgLocations; }

    public void  setDoLocations( boolean  doLocations ) { this.doLocations  =  doLocations; }
    public void setDbgLocations( boolean dbgLocations ) { this.dbgLocations = dbgLocations; }


    public boolean  isDoStatusSex() { return  doStatusSex; }
    public boolean isDbgStatusSex() { return dbgStatusSex; }

    public void  setDoStatusSex( boolean  doStatusSex ) { this.doStatusSex  =  doStatusSex; }
    public void setDbgStatusSex( boolean dbgStatusSex ) { this.dbgStatusSex = dbgStatusSex; }


    public boolean  isDoRegType() { return  doRegType; }
    public boolean isDbgRegType() { return dbgRegType; }

    public void  setDoRegType( boolean  doRegType ) { this.doRegType  =  doRegType; }
    public void setDbgRegType( boolean dbgRegType ) { this.dbgRegType = dbgRegType; }


    public boolean  isDoOccupation() { return  doOccupation; }
    public boolean isDbgOccupation() { return dbgOccupation; }

    public void  setDoOccupation( boolean  doOccupation ) { this.doOccupation  =  doOccupation; }
    public void setDbgOccupation( boolean dbgOccupation ) { this.dbgOccupation = dbgOccupation; }


    public boolean  isDoAge() { return  doAge; }
    public boolean isDbgAge() { return dbgAge; }

    public void  setDoAge( boolean  doAge ) { this.doAge  =  doAge; }
    public void setDbgAge( boolean dbgAge ) { this.dbgAge = dbgAge; }


    public boolean  isDoRole() { return  doRole; }
    public boolean isDbgRole() { return dbgRole; }

    public void  setDoRole( boolean  doRole ) { this.doRole  =  doRole; }
    public void setDbgRole( boolean dbgRole ) { this.dbgRole = dbgRole; }


    public boolean  isDoDates() { return  doDates; }
    public boolean isDbgDates() { return dbgDates; }

    public void  setDoDates( boolean  doDates ) { this.doDates  =  doDates; }
    public void setDbgDates( boolean dbgDates ) { this.dbgDates = dbgDates; }


    public boolean  isDoMinMaxMarriage() { return  doMinMaxMarriage; }
    public boolean isDbgMinMaxMarriage() { return dbgMinMaxMarriage; }

    public void  setDoMinMaxMarriage( boolean  doMinMaxMarriage ) { this.doMinMaxMarriage  =  doMinMaxMarriage; }
    public void setDbgMinMaxMarriage( boolean dbgMinMaxMarriage ) { this.dbgMinMaxMarriage = dbgMinMaxMarriage; }


    public boolean  isDoPartsToFullDate() { return  doPartsToFullDate; }
    public boolean isDbgPartsToFullDate() { return dbgPartsToFullDate; }

    public void  setDoPartsToFullDate( boolean  doPartsToFullDate ) { this.doPartsToFullDate  =  doPartsToFullDate; }
    public void setDbgPartsToFullDate( boolean dbgPartsToFullDate ) { this.dbgPartsToFullDate = dbgPartsToFullDate; }


    public boolean  isDoDaysSinceBegin() { return  doDaysSinceBegin; }
    public boolean isDbgDaysSinceBegin() { return dbgDaysSinceBegin; }

    public void  setDoDaysSinceBegin( boolean  doDaysSinceBegin ) { this.doDaysSinceBegin  =  doDaysSinceBegin; }
    public void setDbgDaysSinceBegin( boolean dbgDaysSinceBegin ) { this.dbgDaysSinceBegin = dbgDaysSinceBegin; }


    public boolean  isDoPostTasks() { return doPostTasks; }
    public boolean isDbgPostTasks() { return dbgPostTasks; }

    public void setDoPostTasks(  boolean  doPostTasks ) { this.doPostTasks  =  doPostTasks; }
    public void setDbgPostTasks( boolean dbgPostTasks ) { this.dbgPostTasks = dbgPostTasks; }


    public boolean  isDoRemoveDuplicates() { return doRemoveDuplicates; }
    public boolean isDbgRemoveDuplicates() { return dbgRemoveDuplicates; }

    public void setDoRemoveDuplicates(  boolean  doRemoveDuplicates ) { this.doRemoveDuplicates  = doRemoveDuplicates; }
    public void setDbgRemoveDuplicates( boolean dbgRemoveDuplicates ) { this.dbgRemoveDuplicates = dbgRemoveDuplicates; }


    public boolean  isDoPrematch() { return doPrematch; }
    public boolean isDbgPrematch() { return dbgPrematch; }

    public void  setDoPrematch( boolean  doPrematch ) { this.doPrematch  = doPrematch; }
    public void setDbgPrematch( boolean dbgPrematch ) { this.dbgPrematch = dbgPrematch; }


    public boolean  isDoMatch() { return doMatch; }
    public boolean isDbgMatch() { return dbgMatch; }

    public void  setDoMatch( boolean  doMatch ) { this.doMatch  = doMatch; }
    public void setDbgMatch( boolean dbgMatch ) { this.dbgMatch = dbgMatch; }
}

