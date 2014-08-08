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
package modulemain;

import connectors.*;
import javax.swing.JTextField;
import javax.swing.JTextArea;
import linksManager.ManagerGui;

/**
 * 
 * @author oaz
 */
public class LinksOriginal extends Thread {

    private String dbLocation;
    private String dbUser;
    private String dbPass;
    private String projectName;
    private JTextField minUpdate;
    private JTextArea maxUpdate;
    private int delay;
    private String deleteSource;
    private String tempTableName;
    private ManagerGui jf;
    private boolean doCleaned;

    /**
     * 
     * @param dbLocation
     * @param dbUser
     * @param dbPass
     * @param projectName
     * @param minUpdate
     * @param maxUpdate
     * @param delay
     * @param deleteSource
     * @param jf
     * @param doCleaned 
     */
    public LinksOriginal(
            String dbLocation,
            String dbUser,
            String dbPass,
            String projectName,
            JTextField minUpdate,
            JTextArea maxUpdate,
            int delay,
            String deleteSource,
            ManagerGui jf,
            boolean doCleaned) {
        this.dbLocation = dbLocation;
        this.dbUser = dbUser;
        this.dbPass = dbPass;
        this.projectName = projectName;
        this.minUpdate = minUpdate;
        this.maxUpdate = maxUpdate;
        this.delay = delay;
        this.deleteSource = deleteSource;
        this.jf = jf;
        this.doCleaned = doCleaned;
    }

    /**
     * 
     */
    @Override
    public void run() {

        try {

            // Generate Temptable
            tempTableName = LinksSpecific.getTimeStamp();

            // Connect
            addToLog("Connecting to database", false);
            MySqlConnector etm = new MySqlConnector(dbLocation, "links_original", dbUser, dbPass);

            // Check if DELETE source field is filled
            if (!deleteSource.isEmpty()) {

                addToLog("Deleting source number " + deleteSource + " from table Registratie...", false);
                String query = "DELETE FROM registration_o where id_source=" + deleteSource + ";";
                etm.runQuery(query);

                addToLog("Deleting source number " + deleteSource + " from table Persoon...", false);
                query = "DELETE FROM person_o where id_source=" + deleteSource + ";";
                etm.runQuery(query);
            }

            String query = "";


            /**
             * Begin Moving data
             */
            addToLog("Moving 'ana_id' , 'aty_id' to registration_o...", false);
            {
                query = ""
                        + "INSERT INTO " + "links_original.registration_o( id_source , id_orig_registration , registration_maintype ) "
                        + "SELECT CONCAT('1',CAST(arl_id AS CHAR)),ana_id,aty_id FROM " + "source_internal" + "." + projectName + "_analyse_personen GROUP BY ana_id;";

                etm.runQuery(query);

            }

            addToLog("Moving 'registration_location' to registration_o...", false);
            {
                query = ""
                        + "UPDATE " + "links_original.registration_o , " + "source_internal" + "." + projectName + "_analyse_items "
                        + "SET " + "links_original.registration_o.registration_location = " + "source_internal" + "." + projectName + "_analyse_items.waarde "
                        + "WHERE " + "source_internal" + "." + projectName + "_analyse_items.ana_id = " + "links_original.registration_o.id_orig_registration "
                        + "AND " + "source_internal" + "." + projectName + "_analyse_items.ite_id = 115;";

                etm.runQuery(query);
            }

            addToLog("Moving registration_type to registration_o...", false);
            {
                query = ""
                        + "UPDATE " + "links_original.registration_o, " + "source_internal" + "." + projectName + "_analyse_items "
                        + "SET " + "links_original.registration_o.registration_type = " + "source_internal" + "." + projectName + "_analyse_items.waarde "
                        + "WHERE " + "source_internal" + "." + projectName + "_analyse_items.ana_id = " + "links_original.registration_o.id_orig_registration "
                        + "AND " + "source_internal" + "." + projectName + "_analyse_items.ite_id = 118;";

                etm.runQuery(query);
            }


            addToLog("Moving registration_seq to registration_o...", false);
            {
                query = ""
                        + "UPDATE " + "links_original.registration_o, " + "source_internal" + "." + projectName + "_analyse_items"
                        + " SET " + "links_original.registration_o.registration_seq = " + "source_internal" + "." + projectName + "_analyse_items.waarde"
                        + " WHERE " + "source_internal" + "." + projectName + "_analyse_items.ana_id = " + "links_original.registration_o.id_orig_registration"
                        + " AND " + "source_internal" + "." + projectName + "_analyse_items.ite_id = 116;";

                etm.runQuery(query);
            }


            addToLog("Moving 'registration_date' to registration_o...", false);
            {
                query = ""
                        + "UPDATE " + "links_original.registration_o, " + "source_internal" + "." + projectName + "_analyse_items "
                        + "SET " + "links_original.registration_o.registration_date = " + "source_internal" + "." + projectName + "_analyse_items.waarde "
                        + "WHERE " + "source_internal" + "." + projectName + "_analyse_items.ana_id = " + "links_original.registration_o.id_orig_registration "
                        + "AND " + "source_internal" + "." + projectName + "_analyse_items.ite_id = 117;";

                etm.runQuery(query);
            }


            addToLog("Moving remarks to registration_o...", false);
            {
                query = ""
                        + "UPDATE " + "links_original.registration_o, " + "source_internal" + "." + projectName + "_analyse_items "
                        + "SET " + "links_original.registration_o.remarks = " + "source_internal" + "." + projectName + "_analyse_items.waarde "
                        + "WHERE " + "source_internal" + "." + projectName + "_analyse_items.ana_id = " + "links_original.registration_o.id_orig_registration "
                        + "AND " + "source_internal" + "." + projectName + "_analyse_items.ite_id = 107;";

               etm.runQuery(query);
            }


            addToLog("Creating temporary person table...", false);
            {
                query = ""
                        + "CREATE  TABLE IF NOT EXISTS `" + "links_temp" + "`.`" + tempTableName + "_person` ( "
                        + "`id_person` INT NOT NULL AUTO_INCREMENT , "
                        + "`id_registration` INT NULL , "
                        + "`id_source` DECIMAL(10) NULL , "
                        + "`ana_id` DECIMAL(10) NULL ,"
                        + "`aty_id` DECIMAL(10) NULL ,"
                        + "`atr_volg_nr` DECIMAL(10) NULL , "
                        + "`firstname` VARCHAR(50) NULL , "
                        + "`patronyme` VARCHAR(30) NULL , "
                        + "`prefix` VARCHAR(15) NULL , "
                        + "`familyname` VARCHAR(110) NULL , "
                        + "`birth_location` VARCHAR(50) NULL , "
                        + "`birth_date` VARCHAR(20) NULL , "
                        + "`age_year` VARCHAR(3) NULL , "
                        + "`sex` VARCHAR(20) NULL , "
                        + "`foundling` VARCHAR(3) NULL , "
                        + "`death_location` VARCHAR(50) NULL , "
                        + "`death_date` VARCHAR(20) NULL , "
                        + "`civil_status` VARCHAR(30) NULL , "
                        + "`role` VARCHAR(30) NULL , "
                        + "PRIMARY KEY (`id_person`) , "
                        + "INDEX `defaultindex` (`ana_id` ASC) ) "
                        + "DEFAULT CHARACTER SET = latin1;";

               etm.runQuery(query);
            }


            addToLog("Moving 'id_person' , 'aty_id' , 'atr_volg_nr' , 'firstname' , 'patronyme' , 'prefix' , 'familyname' to temporary person table...", false);
            {
                query = ""
                        + "INSERT IGNORE INTO " + "links_temp" + "." + tempTableName + "_person(ana_id, aty_id, atr_volg_nr, firstname, patronyme, prefix, familyname ) "
                        + "SELECT ana_id, aty_id, atr_volg_nr, voornaam, patronym, tussenvoegsel, familienaam FROM " + "source_internal" + "." + projectName + "_analyse_personen;";

               etm.runQuery(query);
            }


            addToLog("Moving 'id_registration' , id_source to temporary person table...", false);
            {
                query = ""
                        + "UPDATE " + "links_temp" + "." + tempTableName + "_person, " + "links_original.registration_o "
                        + "SET " + "links_temp" + "." + tempTableName + "_person.id_registration = " + "links_original.registration_o.id_registration, "
                        + "links_temp" + "." + tempTableName + "_person.id_source = " + "links_original.registration_o.id_source "
                        + "WHERE " + "links_temp" + "." + tempTableName + "_person.ana_id = " + "links_original.registration_o.id_orig_registration;";

                etm.runQuery(query);
            }

//            runItemsValue(etm, 120, "birth_location"); // problem
           runItemsValue(etm, 119, "birth_date");
            runItemsValue(etm, 113, "age_year");
            runItemsValue(etm, 121, "sex");
            runItemsValue(etm, 123, "foundling");
            runItemsValue(etm, 134, "death_location");
           runItemsValue(etm, 135, "death_date");




            addToLog("Moving 'role' to temporary person table...", false);
            {
                query = ""
                        + "UPDATE " + "links_temp" + "." + tempTableName + "_person, " + "source_internal" + "." + projectName + "_rollen, " + "source_internal" + "." + projectName + "_analyse_type_rollen "
                        + "SET " + "links_temp" + "." + tempTableName + "_person.role = " + "source_internal" + "." + projectName + "_rollen.naam "
                        + "WHERE " + "source_internal" + "." + projectName + "_rollen.id = " + "source_internal" + "." + projectName + "_analyse_type_rollen.ROL_ID AND "
                        + "links_temp" + "." + tempTableName + "_person.aty_id = " + "source_internal" + "." + projectName + "_analyse_type_rollen.ATY_ID AND "
                        + "links_temp" + "." + tempTableName + "_person.atr_volg_nr = " + "source_internal" + "." + projectName + "_analyse_type_rollen.VOLG_NR;";

                etm.runQuery(query);
            }

            // civil status works different
            runCivilStatus(etm);

            addToLog("Moving data from temporary person table to person_o...", false);
            {
                query = ""
                        + "INSERT IGNORE INTO links_original.person_o(id_registration, id_source, id_person_o, firstname, patronyme, prefix, familyname, birth_location, birth_date, age_year, sex, foundling, death_location, death_date, civil_status , role) "
                        + "SELECT id_registration, id_source, CONCAT(ana_id, atr_volg_nr), firstname, patronyme, prefix, familyname, birth_location, birth_date, age_year, sex, foundling, death_location, death_date, civil_status, role from " + "links_temp" + "." + tempTableName + "_person;";

                etm.runQuery(query);
            }


            addToLog("Deleting temporary person table table...", false);

            query = "DROP TABLE " + "links_temp" + "." + tempTableName + "_person;";

            etm.runQuery(query);

            addToLog("DONE", false);


            if (doCleaned) {

                // start cleaned
                jf.fireCleaned();

            }
        } catch (Exception e) {
            addToLog("Error: " + e , false);
        }
    }

    private void addToLog(String logText, boolean isMinOnly) {
        minUpdate.setText(logText);
        if (!isMinOnly) {
            maxUpdate.append(logText + "\r\n");
        }
    }

    /**
     * 
     * @param etm
     * @param ite_id
     * @param field
     */
    private void runItemsValue(MySqlConnector etm, int ite_id, String field) {

        addToLog("Moving '" + field + "' to temporary person table...", false);
        {
            String query = ""
                    + "UPDATE  " + "links_temp" + "." + tempTableName + "_person, " + "source_internal" + "." + projectName + "_analyse_persoon_items "
                    + "SET " + "links_temp" + "." + tempTableName + "_person." + field + " = " + "source_internal" + "." + projectName + "_analyse_persoon_items.waarde "
                    + "WHERE " + "links_temp" + "." + tempTableName + "_person.ana_id = " + "source_internal" + "." + projectName + "_analyse_persoon_items.ana_id AND "
                    + "source_internal" + "." + projectName + "_analyse_persoon_items.ite_id = " + ite_id + " "
                    + "AND " + "links_temp" + "." + tempTableName + "_person.atr_volg_nr = " + "source_internal" + "." + projectName + "_analyse_persoon_items.atr_volg_nr;";

            try {
                etm.runQuery(query);
                addToLog("Moving '" + field + "' to temporary person table...DONE", false);
            } catch (Exception e) {
                addToLog("Moving '" + field + "' to temporary person table...FAILED: " + e.getMessage(), false);
                this.stop();
                return;
            }
        }
    }

    /**
     * 
     * @param etm
     * @param ite_id
     * @param field
     */
    private void runCivilStatus(MySqlConnector etm) {

        addToLog("Moving 'civil_status' to temporary person table...", false);
        {
            String query = ""
                    + "UPDATE  " + "links_temp" + "." + tempTableName + "_person, " + "source_internal" + "." + projectName + "_analyse_persoon_items "
                    + "SET " + "links_temp" + "." + tempTableName + "_person.civil_status = " + "source_internal" + "." + projectName + "_analyse_persoon_items.waarde "
                    + "WHERE " + "links_temp" + "." + tempTableName + "_person.ana_id = " + "source_internal" + "." + projectName + "_analyse_persoon_items.ana_id AND "
                    + "source_internal" + "." + projectName + "_analyse_persoon_items.ite_id = 136 AND "
                    + "source_internal" + "." + projectName + "_analyse_persoon_items.aty_id = 3 AND "
                    + "links_temp" + "." + tempTableName + "_person.role = 'Overledene';";

            try {
                etm.runQuery(query);
                addToLog("Moving 'civil_status' to temporary person table...DONE", false);
            } catch (Exception e) {
                addToLog("Moving 'civil_status' to temporary person table...FAILED: " + e.getMessage(), false);
                this.stop();
            }
        }
    }

    // check delay
    private void delay(int d) throws Exception {
        if (delay > 0) {
            addToLog("Delay activated", false);

            Thread.sleep(delay * 60000);

        }
    }
}
