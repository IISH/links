package modulemain;

import dataset.Options;
import general.Functions;
import general.PrintLogger;

import java.net.InetAddress;
import java.util.Properties;

public class LinksClean {
    public static void main(String[] args) throws Exception {
        long threadId = Thread.currentThread().getId();
        String hostname = InetAddress.getLocalHost().getHostName();

        PrintLogger plog = new general.PrintLogger(true);
        plog.show(String.format("Thread id %02d; Running on host: %s", threadId, hostname));
        plog.show(String.format("Thread id %02d; ManagerGui/main()", threadId));
        plog.show("");

        String timestamp1 = "22-Jun-2021 14:52";
        String timestamp2 = LinksSpecific.getTimeStamp2("yyyy.MM.dd-HH:mm:ss");

        plog.show(String.format("Thread id %02d; Links Data Manager 2.0 timestamp: %s", threadId, timestamp1));
        plog.show(String.format("Thread id %02d; Start at: %s", threadId, timestamp2));
        plog.show(String.format("Thread id %02d; LinksClean/main/run()", threadId));

        String version = System.getProperty("java.version");
        String msg = String.format("Thread id %02d; Java version: %s", threadId, version);
        plog.show(msg);
        plog.show("");

        Properties properties = Functions.getProperties();
        Options opts = loadProperties(plog, properties);

        LinksCleanMain linksCleaned = new LinksCleanMain(opts, null, null, null);
        linksCleaned.start();
    }

    public static Options loadProperties(PrintLogger plog, Properties properties) {
        Options opts = new Options();
        opts.setLogger(plog);

        // max number of simultaneous cleaning threads
        String max_threads_simul = properties.getProperty("max_threads_simul");
        if (max_threads_simul != null) {
            int max_threads_simul_int = 1;
            try {
                max_threads_simul_int = Integer.parseInt(max_threads_simul);
            } catch (NumberFormatException ex) {
                System.out.println(ex.getMessage());
            }
            opts.setMaxThreadsSimul(max_threads_simul_int);
        }

        String use_links_logs_str = properties.getProperty("use_links_logs");
        if (use_links_logs_str == null || use_links_logs_str.isEmpty()) {
            use_links_logs_str = "true";
        }
        opts.setUseLinksLogs(use_links_logs_str.equals("true"));

        String sourceId = properties.getProperty("tbLOLCSourceId");
        if (sourceId == null) {
            sourceId = "";
        }
        opts.setSourceIds(sourceId);

        String RMtypes = properties.getProperty("tbLOLCrmtype");
        if (RMtypes == null) {
            RMtypes = "";
        }
        opts.setRMtypes(RMtypes);

        String ref_url = properties.getProperty("mysql_hsnref_hosturl");
        String ref_user = properties.getProperty("mysql_hsnref_username");
        String ref_pass = properties.getProperty("mysql_hsnref_password");
        String ref_db = properties.getProperty("mysql_hsnref_dbname");

        opts.setDb_ref_url(ref_url);
        opts.setDb_ref_user(ref_user);
        opts.setDb_ref_pass(ref_pass);
        opts.setDb_ref_db(ref_db);

        String url = properties.getProperty("mysql_links_hosturl");
        String user = properties.getProperty("mysql_links_username");
        String pass = properties.getProperty("mysql_links_password");

        opts.setDb_url(url);
        opts.setDb_user(user);
        opts.setDb_pass(pass);

        // Enable cleaning checkboxes ?
        String noRefreshData = properties.getProperty("noRefreshData");
        if (noRefreshData != null) {
            opts.setDoRefreshData(!noRefreshData.equals("true"));
        }

        // Remove previous data ?
        String doRefreshData = properties.getProperty("doRefreshData");
        if (doRefreshData != null) {
            opts.setDoRefreshData(doRefreshData.equals("true"));
        }

        // PrepieceSuffix
        String doPrepieceSuffix = properties.getProperty("doPrepieceSuffix");
        if (doPrepieceSuffix != null) {
            opts.setDoPrepieceSuffix(doPrepieceSuffix.equals("true"));
        }

        // Firstnames
        String doFirstnames = properties.getProperty("doFirstnames");
        if (doFirstnames != null) {
            opts.setDoFirstnames(doFirstnames.equals("true"));
        }

        // Familynames
        String doFamilynames = properties.getProperty("doFamilynames");
        if (doFamilynames != null) {
            opts.setDoFamilynames(doFamilynames.equals("true"));
        }

        // Locations
        String doLocations = properties.getProperty("doLocations");
        if (doLocations != null) {
            opts.setDoLocations(doLocations.equals("true"));
        }

        // Status and sex
        String doStatusSex = properties.getProperty("doStatusSex");
        if (doStatusSex != null) {
            opts.setDoStatusSex(doStatusSex.equals("true"));
        }

        // RegType
        String doRegType = properties.getProperty("doRegType");
        if (doRegType != null) {
            opts.setDoRegType(doRegType.equals("true"));
        }

        // Occupation
        String doOccupation = properties.getProperty("doOccupation");
        if (doOccupation != null) {
            opts.setDoOccupation(doOccupation.equals("true"));
        }

        // Age
        String doAge = properties.getProperty("doAge");
        if (doAge != null) {
            opts.setDoAge(doAge.equals("true"));
        }

        // Role
        String doRole = properties.getProperty("doRole");
        if (doRole != null) {
            opts.setDoRole(doRole.equals("true"));
        }

        // Dates
        String doDates = properties.getProperty("doDates");
        if (doDates != null) {
            opts.setDoDates(doDates.equals("true"));
        }

        // Min Max Marriage
        String doMinMaxMarriage = properties.getProperty("doMinMaxMarriage");
        if (doMinMaxMarriage != null) {
            opts.setDoMinMaxMarriage(doMinMaxMarriage.equals("true"));
        }

        // Parts to Full Date
        String doPartsToFullDate = properties.getProperty("doPartsToFullDate");
        if (doPartsToFullDate != null) {
            opts.setDoPartsToFullDate(doPartsToFullDate.equals("true"));
        }

        // Days since begin
        String doDaysSinceBegin = properties.getProperty("doDaysSinceBegin");
        if (doDaysSinceBegin != null) {
            opts.setDoDaysSinceBegin(doDaysSinceBegin.equals("true"));
        }

        // Post Tasks
        String doPostTasks = properties.getProperty("doPostTasks");
        if (doPostTasks != null) {
            opts.setDoPostTasks(doPostTasks.equals("true"));
        }

        // Remove Empty Date Regs
        String doFlagPersons = properties.getProperty("doFlagPersons");
        if (doFlagPersons != null) {
            opts.setDoFlagPersons(doFlagPersons.equals("true"));
        }

        // Remove Duplicate Regs
        String doFlagDuplicateRegs = properties.getProperty("doFlagDuplicateRegs");
        if (doFlagDuplicateRegs != null) {
            opts.setDoFlagRegistrations(doFlagDuplicateRegs.equals("true"));
        }

        // Scan Remarks in Regs
        String doScanRemarks = properties.getProperty("doScanRemarks");
        if (doScanRemarks != null) {
            opts.setDoScanRemarks(doScanRemarks.equals("true"));
        }

        String dbgRefreshData = properties.getProperty("dbgRefreshData");
        String dbgPrepieceSuffix = properties.getProperty("dbgPrepieceSuffix");
        String dbgFirstnames = properties.getProperty("dbgFirstnames");
        String dbgFamilynames = properties.getProperty("dbgFamilynames");
        String dbgLocations = properties.getProperty("dbgLocations");
        String dbgStatusSex = properties.getProperty("dbgStatusSex");
        String dbgRegType = properties.getProperty("dbgRegType");
        String dbgOccupation = properties.getProperty("dbgOccupation");
        String dbgAge = properties.getProperty("dbgAge");
        String dbgRole = properties.getProperty("dbgRole");
        String dbgDates = properties.getProperty("dbgDates");
        String dbgMinMaxMarriage = properties.getProperty("dbgMinMaxMarriage");
        String dbgPartsToFullDate = properties.getProperty("dbgPartsToFullDate");
        String dbgDaysSinceBegin = properties.getProperty("dbgDaysSinceBegin");
        String dbgPostTasks = properties.getProperty("dbgPostTasks");
        String dbgFlagRegistrations = properties.getProperty("dbgFlagRegistrations");
        String dbgFlagPersons = properties.getProperty("dbgFlagPersons");
        String dbgScanRemarks = properties.getProperty("dbgScanRemarks");

        if (dbgRefreshData != null) {
            opts.setDbgRefreshData(dbgRefreshData.equals("true"));
        }

        if (dbgPrepieceSuffix != null) {
            opts.setDbgPrepieceSuffix(dbgPrepieceSuffix.equals("true"));
        }

        if (dbgFirstnames != null) {
            opts.setDbgFirstnames(dbgFirstnames.equals("true"));
        }

        if (dbgFamilynames != null) {
            opts.setDbgFamilynames(dbgFamilynames.equals("true"));
        }

        if (dbgLocations != null) {
            opts.setDbgLocations(dbgLocations.equals("true"));
        }

        if (dbgStatusSex != null) {
            opts.setDbgStatusSex(dbgStatusSex.equals("true"));
        }

        if (dbgRegType != null) {
            opts.setDbgRegType(dbgRegType.equals("true"));
        }

        if (dbgOccupation != null) {
            opts.setDbgOccupation(dbgOccupation.equals("true"));
        }

        if (dbgAge != null) {
            opts.setDbgAge(dbgAge.equals("true"));
        }

        if (dbgRole != null) {
            opts.setDbgRole(dbgRole.equals("true"));
        }

        if (dbgDates != null) {
            opts.setDbgDates(dbgDates.equals("true"));
        }

        if (dbgMinMaxMarriage != null) {
            opts.setDbgMinMaxMarriage(dbgMinMaxMarriage.equals("true"));
        }

        if (dbgPartsToFullDate != null) {
            opts.setDbgPartsToFullDate(dbgPartsToFullDate.equals("true"));
        }

        if (dbgDaysSinceBegin != null) {
            opts.setDbgDaysSinceBegin(dbgDaysSinceBegin.equals("true"));
        }

        if (dbgPostTasks != null) {
            opts.setDbgPostTasks(dbgPostTasks.equals("true"));
        }

        if (dbgFlagRegistrations != null) {
            opts.setDbgFlagRegistrations(dbgFlagRegistrations.equals("true"));
        }

        if (dbgFlagPersons != null) {
            opts.setDbgFlagPersons(dbgFlagPersons.equals("true"));
        }

        if (dbgScanRemarks != null) {
            opts.setDbgScanRemarks(dbgScanRemarks.equals("true"));
        }

        return opts;
    }
}