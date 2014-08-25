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
 * FL-25-Aug-2014 Occupation added
 */
public class DoSet
{
    private boolean doOccupation;
    private boolean doRenewData;
    private boolean doRemarks;
    private boolean doStatusSex;
    private boolean doAgeYear;
    private boolean doLocations;
    private boolean doRegType;
    private boolean doSequence;
    private boolean doPartsToFullDate;
    private boolean doDaysSinceBegin;
    private boolean doMinMaxDate;
    private boolean doMinMaxMarriage;
    private boolean doRelation;
    private boolean doRole;
    private boolean doPostTasks;
    private boolean doPrematch;
    private boolean doPreBasicNames;
    private boolean doNames;
    private boolean doDates;


    /**
     * @return
     */
    public boolean isDoOccupation() {
        return doOccupation;
    }

    /**
     * @param doOccupation
     */
    public void setDoOccupation(boolean doOccupation) {
        this.doOccupation = doOccupation;
    }


    /**
     * @return
     */
    public boolean isDoAgeYear() {
        return doAgeYear;
    }

    /**
     * @param doAgeYear
     */
    public void setDoAgeYear(boolean doAgeYear) {
        this.doAgeYear = doAgeYear;
    }


    /**
     * @return
     */
    public boolean isDoDates() {
        return doDates;
    }

    /**
     * @param doDates
     */
    public void setDoDates(boolean doDates) {
        this.doDates = doDates;
    }


    /**
     * @return
     */
    public boolean isDoLocations() {
        return doLocations;
    }

    /**
     * @param doLocations
     */
    public void setDoLocations(boolean doLocations) {
        this.doLocations = doLocations;
    }


    /**
     * @return
     */
    public boolean isDoDaysSinceBegin() {
        return doDaysSinceBegin;
    }

    /**
     * @param doDaysSinceBegin
     */
    public void setDoDaysSinceBegin(boolean doDaysSinceBegin) {
        this.doDaysSinceBegin = doDaysSinceBegin;
    }


    /**
     * @return
     */
    public boolean isDoNames() {
        return doNames;
    }

    /**
     * @param doNames
     */
    public void setDoNames(boolean doNames) {
        this.doNames = doNames;
    }


    /**
     * @return
     */
    public boolean isDoMinMaxDate() {
        return doMinMaxDate;
    }

    /**
     * @param doMinMaxDate
     */
    public void setDoMinMaxDate(boolean doMinMaxDate) {
        this.doMinMaxDate = doMinMaxDate;
    }


    /**
     * @return
     */
    public boolean isDoMinMaxMarriage() {
        return doMinMaxMarriage;
    }

    /**
     * @param doMinMaxMarriage
     */
    public void setDoMinMaxMarriage(boolean doMinMaxMarriage) {
        this.doMinMaxMarriage = doMinMaxMarriage;
    }


    /**
     * @return
     */
    public boolean isDoPartsToFullDate() {
        return doPartsToFullDate;
    }

    /**
     * @param doPartsToFullDate
     */
    public void setDoPartsToFullDate(boolean doPartsToFullDate) {
        this.doPartsToFullDate = doPartsToFullDate;
    }


    /**
     * @return
     */
    public boolean isDoRelation() {
        return doRelation;
    }

    /**
     * @param doRelation
     */
    public void setDoRelation(boolean doRelation) {
        this.doRelation = doRelation;
    }


    /**
     * @return
     */
    public boolean isDoRemarks() {
        return doRemarks;
    }

    /**
     * @param doRemarks
     */
    public void setDoRemarks(boolean doRemarks) {
        this.doRemarks = doRemarks;
    }


    /**
     * @return
     */
    public boolean isDoRenewData() {
        return doRenewData;
    }

    /**
     * @param doRenewData
     */
    public void setDoRenewData(boolean doRenewData) {
        this.doRenewData = doRenewData;
    }


    /**
     * @return
     */
    public boolean isDoSequence() {
        return doSequence;
    }

    /**
     * @param doSequence
     */
    public void setDoSequence(boolean doSequence) {
        this.doSequence = doSequence;
    }


    /**
     *
     * @return
     */
    public boolean isDoStatusSex() {
        return doStatusSex;
    }

    /**
     *
     * @param doStatusSex
     */
    public void setDoStatusSex(boolean doStatusSex) {
        this.doStatusSex = doStatusSex;
    }


    /**
     * @return
     */
    public boolean isDoRegType() {
        return doRegType;
    }

    /**
     * @param doRegType
     */
    public void setDoRegType(boolean doRegType) { this.doRegType = doRegType; }


    /**
     * @return
     */
    public boolean isDoRole() {
        return doRole;
    }

    /**
     * @param doRole
     */
    public void setDoRole(boolean doRole) {
        this.doRole = doRole;
    }


    /**
     * @return
     */
    public boolean isDoPostTasks() {
        return doPostTasks;
    }

    /**
     * @param doPostTasks
     */
    public void setDoPostTasks(boolean doPostTasks) {
        this.doPostTasks = doPostTasks;
    }


    /**
     * @return
     */
    public boolean isDoPrematch() {
        return doPrematch;
    }

    /**
     * @param doPrematch
     */
    public void setDoPrematch(boolean doPrematch) {
        this.doPrematch = doPrematch;
    }


    /**
     * @param doPreBasicNames 
     */
    public void setDoPreBasicNames(boolean doPreBasicNames) {
        this.doPreBasicNames = doPreBasicNames;
    }

    /**
     * @return 
     */
    public boolean isDoPreBasicNames() {
        return doPreBasicNames;
    }
}

