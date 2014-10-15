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
 */
public class Options
{
     PrintLogger plog;

     private int sourceId;

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
    //private boolean doPreBasicNames;
    private boolean doNames;
    private boolean doLocations;
    private boolean doStatusSex;
    //private boolean doSequence;
    //private boolean doRelation;
    private boolean doRegType;
    private boolean doOccupation;
    //private boolean doAgeYear;
    //private boolean doRole;
    private boolean doDates;
    //private boolean doMinMaxDate;
    //private boolean doRemarks;
    private boolean doMinMaxMarriage;
    private boolean doPartsToFullDate;
    private boolean doDaysSinceBegin;
    private boolean doPostTasks;
    private boolean doPrematch;


    public void setLogger( PrintLogger plog ) { this.plog = plog; }

    public PrintLogger getLogger() { return plog; }


    public void setSourceId( int sourceId ) { this.sourceId = sourceId; }

    public int getSourceId() { return sourceId; }


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


    public boolean isDoRenewData() { return doRenewData; }

    public void setDoRenewData( boolean doRenewData ) {
        this.doRenewData = doRenewData;
    }


    //public void setDoPreBasicNames(boolean doPreBasicNames) { this.doPreBasicNames = doPreBasicNames; }
    //public boolean isDoPreBasicNames() { return doPreBasicNames; }


    public boolean isDoNames() { return doNames; }

    public void setDoNames( boolean doNames ) { this.doNames = doNames; }


    public boolean isDoLocations() { return doLocations; }

    public void setDoLocations( boolean doLocations ) { this.doLocations = doLocations; }


    public boolean isDoStatusSex() { return doStatusSex; }

    public void setDoStatusSex( boolean doStatusSex ) { this.doStatusSex = doStatusSex; }


    //public boolean isDoSequence() { return doSequence; }
    //public void setDoSequence( boolean doSequence ) { this.doSequence = doSequence; }


    //public boolean isDoRelation() { return doRelation; }
    //public void setDoRelation( boolean doRelation ) { this.doRelation = doRelation; }


    public boolean isDoRegType() { return doRegType; }

    public void setDoRegType( boolean doRegType ) { this.doRegType = doRegType; }


    public boolean isDoOccupation() { return doOccupation; }

    public void setDoOccupation( boolean doOccupation ) { this.doOccupation = doOccupation; }


    //public boolean isDoAgeYear() { return doAgeYear; }
    //public void setDoAgeYear( boolean doAgeYear ) { this.doAgeYear = doAgeYear; }


    //public boolean isDoRole() { return doRole; }
    //public void setDoRole( boolean doRole ) { this.doRole = doRole; }


    public boolean isDoDates() { return doDates; }

    public void setDoDates( boolean doDates ) { this.doDates = doDates; }


    //public boolean isDoMinMaxDate() { return doMinMaxDate; }
    //public void setDoMinMaxDate( boolean doMinMaxDate ) { this.doMinMaxDate = doMinMaxDate; }


    //public boolean isDoRemarks() { return doRemarks; }
    //public void setDoRemarks( boolean doRemarks ) { this.doRemarks = doRemarks; }


    public boolean isDoMinMaxMarriage() { return doMinMaxMarriage; }

    public void setDoMinMaxMarriage( boolean doMinMaxMarriage ) { this.doMinMaxMarriage = doMinMaxMarriage; }


    public boolean isDoPartsToFullDate() { return doPartsToFullDate; }

    public void setDoPartsToFullDate( boolean doPartsToFullDate ) { this.doPartsToFullDate = doPartsToFullDate; }


    public boolean isDoDaysSinceBegin() { return doDaysSinceBegin; }

    public void setDoDaysSinceBegin( boolean doDaysSinceBegin ) { this.doDaysSinceBegin = doDaysSinceBegin; }


    public boolean isDoPostTasks() { return doPostTasks; }

    public void setDoPostTasks( boolean doPostTasks ) { this.doPostTasks = doPostTasks; }


    public boolean isDoPrematch() { return doPrematch; }

    public void setDoPrematch( boolean doPrematch ) { this.doPrematch = doPrematch; }

}

