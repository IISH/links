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

/**
 * @author Omar Azouguagh
 * @author Fons Laan
 *
 * <p/>
 * FL-30-Jun-2014 Imported from OA backup
 * FL-10-Oct-2014 Occupation added
 */
public class DoSet
{
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


    /**
     * @param doDebug
     */
    public void setDoDebug( boolean doDebug ) { this.doDebug = doDebug; }

    /**
     * @return
     */
    public boolean isDoDebug() { return doDebug; }


    /**
     * @return
     */
    public boolean isDoRenewData() {
        return doRenewData;
    }

    /**
     * @param doRenewData
     */
    public void setDoRenewData( boolean doRenewData ) {
        this.doRenewData = doRenewData;
    }


    //public void setDoPreBasicNames(boolean doPreBasicNames) { this.doPreBasicNames = doPreBasicNames; }
    //public boolean isDoPreBasicNames() { return doPreBasicNames; }


    /**
     * @return
     */
    public boolean isDoNames() { return doNames; }

    /**
     * @param doNames
     */
    public void setDoNames( boolean doNames ) { this.doNames = doNames; }


    /**
     * @return
     */
    public boolean isDoLocations() { return doLocations; }

    /**
     * @param doLocations
     */
    public void setDoLocations( boolean doLocations ) { this.doLocations = doLocations; }


    /**
     *
     * @return
     */
    public boolean isDoStatusSex() { return doStatusSex; }

    /**
     *
     * @param doStatusSex
     */
    public void setDoStatusSex( boolean doStatusSex ) { this.doStatusSex = doStatusSex; }


    //public boolean isDoSequence() { return doSequence; }
    //public void setDoSequence( boolean doSequence ) { this.doSequence = doSequence; }


    //public boolean isDoRelation() { return doRelation; }
    //public void setDoRelation( boolean doRelation ) { this.doRelation = doRelation; }


    /**
     * @return
     */
    public boolean isDoRegType() { return doRegType; }

    /**
     * @param doRegType
     */
    public void setDoRegType( boolean doRegType ) { this.doRegType = doRegType; }


    /**
     * @return
     */
    public boolean isDoOccupation() { return doOccupation; }

    /**
     * @param doOccupation
     */
    public void setDoOccupation( boolean doOccupation ) { this.doOccupation = doOccupation; }


    //public boolean isDoAgeYear() { return doAgeYear; }
    //public void setDoAgeYear( boolean doAgeYear ) { this.doAgeYear = doAgeYear; }


    //public boolean isDoRole() { return doRole; }
    //public void setDoRole( boolean doRole ) { this.doRole = doRole; }


    /**
     * @return
     */
    public boolean isDoDates() { return doDates; }

    /**
     * @param doDates
     */
    public void setDoDates( boolean doDates ) { this.doDates = doDates; }


    //public boolean isDoMinMaxDate() { return doMinMaxDate; }
    //public void setDoMinMaxDate( boolean doMinMaxDate ) { this.doMinMaxDate = doMinMaxDate; }


    //public boolean isDoRemarks() { return doRemarks; }
    //public void setDoRemarks( boolean doRemarks ) { this.doRemarks = doRemarks; }


    /**
     * @return
     */
    public boolean isDoMinMaxMarriage() { return doMinMaxMarriage; }

    /**
     * @param doMinMaxMarriage
     */
    public void setDoMinMaxMarriage( boolean doMinMaxMarriage ) { this.doMinMaxMarriage = doMinMaxMarriage; }


    /**
     * @return
     */
    public boolean isDoPartsToFullDate() { return doPartsToFullDate; }

    /**
     * @param doPartsToFullDate
     */
    public void setDoPartsToFullDate( boolean doPartsToFullDate ) { this.doPartsToFullDate = doPartsToFullDate; }


    /**
     * @return
     */
    public boolean isDoDaysSinceBegin() { return doDaysSinceBegin; }

    /**
     * @param doDaysSinceBegin
     */
    public void setDoDaysSinceBegin( boolean doDaysSinceBegin ) { this.doDaysSinceBegin = doDaysSinceBegin; }


    /**
     * @return
     */
    public boolean isDoPostTasks() { return doPostTasks; }

    /**
     * @param doPostTasks
     */
    public void setDoPostTasks( boolean doPostTasks ) { this.doPostTasks = doPostTasks; }


    /**
     * @return
     */
    public boolean isDoPrematch() { return doPrematch; }

    /**
     * @param doPrematch
     */
    public void setDoPrematch( boolean doPrematch ) { this.doPrematch = doPrematch; }

}

