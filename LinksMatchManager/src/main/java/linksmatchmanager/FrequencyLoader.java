package linksmatchmanager;

import java.util.Map;
import java.util.HashMap;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.sql.Connection;
import java.util.Collections;
import linksmatchmanager.DataSet.NameType;
import linksmatchmanager.DataSet.NameAndTypeSet;

public class FrequencyLoader {

    private Connection con;
    private boolean load = false;
    
    private final String TABLE_FIRSTNAME = "firstname";
    private final String TABLE_FAMILYNAME = "familyname";
    private final String ID = "id";
    private final String FREQUENCY = "frequency";
    private ArrayList<Integer> firstname;
    private ArrayList<Integer> familyname;
    private ArrayList<Integer> firstnameFreq;
    private ArrayList<Integer> familynameFreq;
    private Exception invalidNameType;
    private Exception nameNotFound;
    private Exception namesNotLoaded;
    
    FrequencyLoader(String url, String user, String pass) throws Exception {

        this.con = General.getConnection(url, "links_frequency", user, pass);
        this.con.setReadOnly(true);
        this.familyname = new ArrayList<Integer>();
        this.familynameFreq = new ArrayList<Integer>();
        this.firstname = new ArrayList<Integer>();
        this.firstnameFreq = new ArrayList<Integer>();
        this.invalidNameType = new Exception("Invalid NameType.");
        this.nameNotFound = new Exception("No frequency found.");
        this.namesNotLoaded = new Exception("Names not loaded.");
    }

    public void load() throws Exception {

        /**
         * Run query
         */
        ResultSet fam = con.createStatement().executeQuery(
                "SELECT " + ID + " , " + FREQUENCY + " FROM " + TABLE_FAMILYNAME);
        con.createStatement().close();


        ResultSet fir = con.createStatement().executeQuery(
                "SELECT " + ID + " , " + FREQUENCY + " FROM " + TABLE_FIRSTNAME);
        
        // Close to prevent memory problems
        con.createStatement().close();

        /**
         * Load ResultSets into Arrays
         */
        // Family name
        while (fam.next()) {
            familyname.add(fam.getInt(ID));
            familynameFreq.add(fam.getInt(FREQUENCY));
        }

        // First name
        while (fir.next()) {
            firstname.add(fir.getInt(ID));
            firstnameFreq.add(fir.getInt(FREQUENCY));
        }

        /* Close resultSets and connection */
        fam.close();
        fir.close();
        con.close();

        /* Set load flag when loading is done */
        load = true;
    }

    public int returnNameFrequency(int name, NameType type) throws Exception {

        /**
         * Check if names are loeaded
         * If not, throw exception
         */
        if (!load) {
            throw namesNotLoaded;
        }

        /**
         * Use binary search to get index of name
         */
        int result = -1;

        // Search into Familyname
        if (type == NameType.FAMILYNAME) {
            result = Collections.binarySearch(familyname, name);
        } // Search into First name
        else if (type == NameType.FIRSTNAME) {
            result = Collections.binarySearch(firstname, name);
        }

        /**
         * If result is negative then there 
         * is no frequency for this name
         */
        if (result < 0) {
            throw nameNotFound;
        }

        /**
         * Return Frequency of founded name
         */
        // Familyname
        if (type == NameType.FAMILYNAME) {
            return familynameFreq.get(result);
        } // First name
        else if (type == NameType.FIRSTNAME) {
            return firstnameFreq.get(result);
        }

        /**
         * NameType is not FIRSTNAME OR FAMILYNAME
         * Throw Exception
         */
        throw invalidNameType;

    }

    public NameAndTypeSet[] lowestFrequency(NameAndTypeSet[] names) throws Exception {

        /**
         * Create new Map to put the array into it
         */
        Map<Integer, Integer> map = new HashMap<Integer, Integer>();

        /**
         * Put names and their frequency into hashtable 
         */
        for (int i = 0; i < names.length; i++) {
            map.put(names[i].name,
                    returnNameFrequency(names[i].name, names[i].nameType));
        }

        // Temporary variable
        Map.Entry< Integer, Integer> min = null;

        /**
         * Sorting the names
         */
        int s = map.size();

        /**
         * Create NameAndTypeSet array for the min to max names
         */
        NameAndTypeSet[] namesSort = new NameAndTypeSet[s];

        for (int i = 0; i < s; i++) {

            /**
             * Find lowest value in the Map
             */
            for (Map.Entry<Integer, Integer> entry : map.entrySet()) {

                /**
                 * If null, than put into map because of first time
                 * If value is smaller than min, punt it into min
                 */
                if (min == null || min.getValue() < entry.getValue()) {
                    min = entry;
                }

            }

            /**
             * Add the minimal value to array
             * And remove it from the map
             */
            namesSort[i] = new NameAndTypeSet(min.getKey(), min.getValue());

            // Remove
            map.remove(min.getKey());

            // Reset min
            min = null;


        }

        /**
         * Return sorted array with names
         */
        return namesSort;
    }
}