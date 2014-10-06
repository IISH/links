/**
 * @author Fons Laan
 *
 * obsolete functions removed from LinksCleaned.java
 *
 * FL-17-Sep-2014 Latest change
 */

    // old links_base
    // hp* objects directly used by 2 functions:
    //      returnAgeCentralFigure()  called by minMaxDate()
    //      fillMinMaxArrays()        called by doMinMaxDate()
    /*
    private ArrayList<Integer> hpChildRegistration    = new ArrayList<Integer>();
    private ArrayList<Integer> hpChildAge             = new ArrayList<Integer>();
    private ArrayList<Integer> hpChildMonth           = new ArrayList<Integer>();
    private ArrayList<Integer> hpChildWeek            = new ArrayList<Integer>();
    private ArrayList<Integer> hpChildDay             = new ArrayList<Integer>();

    private ArrayList<Integer> hpBrideRegistration    = new ArrayList<Integer>();
    private ArrayList<Integer> hpBrideAge             = new ArrayList<Integer>();
    private ArrayList<Integer> hpBrideMonth           = new ArrayList<Integer>();
    private ArrayList<Integer> hpBrideWeek            = new ArrayList<Integer>();
    private ArrayList<Integer> hpBrideDay             = new ArrayList<Integer>();

    private ArrayList<Integer> hpGroomRegistration    = new ArrayList<Integer>();
    private ArrayList<Integer> hpGroomAge             = new ArrayList<Integer>();
    private ArrayList<Integer> hpGroomMonth           = new ArrayList<Integer>();
    private ArrayList<Integer> hpGroomWeek            = new ArrayList<Integer>();
    private ArrayList<Integer> hpGroomDay             = new ArrayList<Integer>();

    private ArrayList<Integer> hpDeceasedRegistration = new ArrayList<Integer>();
    private ArrayList<Integer> hpDeceasedAge          = new ArrayList<Integer>();
    private ArrayList<Integer> hpDeceasedMonth        = new ArrayList<Integer>();
    private ArrayList<Integer> hpDeceasedWeek         = new ArrayList<Integer>();
    private ArrayList<Integer> hpDeceasedDay          = new ArrayList<Integer>();
    */



/**
 * Basic names temp
 * @param go
 * @throws Exception
 */
    /*
    private void doPreBasicNames( boolean go, int sourceId ) throws Exception
    {
        String funcname = "doPreBasicNames";
        if( !go ) {
            if( showskip ) { showMessage( "Skipping " + funcname, false, true ); }
            return;
        }

        long timeStart = System.currentTimeMillis();
        showMessage( funcname + "...", false, true );

        // load the ref tables
        showMessage( "Loading reference tables: firstname/familyname/prepiece/suffix...", false, true );
        {
            //ttalFirstname  = new TableToArraysSet( conGeneral, "original", "firstname" );
            //ttalFamilyname = new TableToArraysSet( conGeneral, "original", "familyname" );
            ttalPrepiece = new TableToArraysSet(conGeneral, conOr, "original", "prepiece");
            ttalSuffix = new TableToArraysSet(conGeneral, conOr, "original", "suffix");
            ttalAlias = new TableToArraysSet(conGeneral, conOr, "original", "alias");
        }

        // Firstname
        if (doesTableExist(conTemp, "links_temp", "firstname_t")) {
            showMessage("Deleting table links_temp.firstname_t", false, true);
            dropTable(conTemp, "links_temp", "firstname_t");
        }

        createTempFirstnameTable();
        createTempFirstnameFile();
        ttalFirstname = new TableToArraysSet(conGeneral, conOr, "original", "firstname");

        //runMethod("standardFirstname");
        for( int i : sourceList ) {
            showMessage( "Processing standardFirstname for source: " + i + "...", false, true );
            standardFirstname( "" + i );
        }

        ttalFirstname.updateTable();
        ttalFirstname.free();
        writerFirstname.close();
        loadFirstnameToTable();
        updateFirstnameToPersonC();
        removeFirstnameFile();
        removeFirstnameTable();

        // Familyname
        if (doesTableExist(conTemp, "links_temp", "familyname_t")) {
            showMessage("Deleting table links_temp.familyname_t", false, true);
            dropTable(conTemp, "links_temp", "familyname_t");
        }

        createTempFamilynameTable();
        createTempFamilynameFile();
        ttalFamilyname = new TableToArraysSet( conGeneral, conOr, "original", "familyname" );

        //runMethod("standardFamilyname");
        for( int i : sourceList ) {
            showMessage( "Processing standardFamilyname for source: " + i + "...", false, true );
            standardFamilyname( "" + i );
        }

        ttalFamilyname.updateTable();
        ttalFamilyname.free();
        writerFamilyname.close();
        loadFamilynameToTable();
        updateFamilynameToPersonC();
        removeFamilynameFile();
        removeFamilynameTable();

        // Update other tables
        ttalPrepiece.updateTable();
        ttalSuffix.updateTable();
        ttalPrepiece.free();
        ttalSuffix.free();

        showMessage("Converting names to lowercase", false, false);
        {
            String qLower = "UPDATE links_cleaned.person_c SET firstname = LOWER(firstname),  familyname = LOWER(familyname);";
            conCleaned.runQuery(qLower);
        }
        showMessage(endl, false, true);

        // Prematch
        LinksPrematch lpm = new LinksPrematch(url, user, pass, taLOLCoutput, tbLOLClatestOutput);

        // temp
        showMessage("Splitting names", false, false);
        {
            lpm.doSplitName();
        }
        showMessage(endl, false, true);

        showMessage("Creating unique name tables", false, false);
        {
            lpm.doUniqueNameTablesTemp();
        }

        showMessage("Basic names tables", false, false);
        {
            lpm.doBasicName();
        }
        showMessage(endl, false, true);

        elapsedShowMessage( funcname, timeStart, System.currentTimeMillis() );
    } // doPreBasicNames
    */


/**
 * Sequence
 * @param go
 * @throws Exception
 */
    /*
    private void doSequence( boolean go, String source ) throws Exception
    {
        String funcname = "doSequence";
        if( !go ) {
            if( showskip ) { showMessage( "Skipping " + funcname, false, true ); }
            return;
        }

        long timeStart = System.currentTimeMillis();
        showMessage( funcname + "...", false, true );

        standardSequence( source );

        elapsedShowMessage( funcname, timeStart, System.currentTimeMillis() );
    } // doSequence
    */

/**
 * Relation
 * @param go
 * @throws Exception
 */
    /*
    private void doRelation( boolean go, String source ) throws Exception
    {
        String funcname = "doRelation";
        if( !go ) {
            if( showskip ) { showMessage( "Skipping " + funcname, false, true ); }
            return;
        }

        long timeStart = System.currentTimeMillis();
        showMessage( funcname + "...", false, true );

        funcRelation( source );

        elapsedShowMessage( funcname, timeStart, System.currentTimeMillis() );
    } // doRelation
    */


/**
 * @param source
 * @throws Exception
 */
    /*
public void relation( String source ) throws Exception
        {
        int counter = 0;
        int step = 1000;
        int stepstate = step;

        try {

        String selectQuery = "SELECT id_registration , id_person, role, sex FROM person_c WHERE id_source = " + source + " ORDER BY id_registration";

        ResultSet rsactRoleRef = conGeneral.runQueryWithResult( "SELECT * FROM ref_relation" );

        // create searchable list
        ActRoleSet ars = new ActRoleSet();
        ars.addRessultSetToList( rsactRoleRef );

        // Run person query
        ResultSet rsPersons = conCleaned.runQueryWithResult( selectQuery );

        int currentId = -1;

        ArrayList< RelationSet > rsList = new ArrayList< RelationSet >();

        while( rsPersons.next() )
        {
        counter++;

        if( counter == stepstate ) {
        showMessage( counter + "", true, true );
        stepstate += step;
        }

        int id_registration = rsPersons.getInt( "id_registration" );
        int id_person       = rsPersons.getInt( "id_person" );
        String role         = rsPersons.getString( "role" );
        String sex          = rsPersons.getString( "sex" );

        if( currentId == id_registration )                 // It is the same id
        {
        RelationSet rs = new RelationSet();

        // add
        rs.setIdPerson( id_person );
        rs.setRole( role );
        rs.setSex( sex );

        rsList.add( rs );
        }
        else     // new id, process old list
        {
        // we must save old list
        ArrayList< RelationSet > rsWorkingList = new ArrayList< RelationSet >( rsList );

        Collections.copy( rsWorkingList, rsList );

        // clear old list
        rsList.clear();

        // old list will be used nou
        RelationSet rs = new RelationSet();

        // add
        rs.setIdPerson( id_person );
        rs.setRole( role );
        rs.setSex( sex );

        rsList.add( rs );

        // Proces the new list
        // Only if id is not -1
        // otherwise is is the first time

        if( currentId > -1 )
        {
        for( int i = 0; i < rsWorkingList.size(); i++ )         // walk through list
        {
        for (int j = 0; j < rsWorkingList.size(); j++)      // second walk
        {
        if (i != j)
        {
        int id_person1 = rsWorkingList.get(i).getIdPerson();
        int id_person2 = rsWorkingList.get(j).getIdPerson();
        String role1   = rsWorkingList.get(i).getRole();
        String role2   = rsWorkingList.get(j).getRole();
        String sex1    = rsWorkingList.get(i).getSex();
        String sex2    = rsWorkingList.get(j).getSex();

        String relation = ars.getRelatie( role1, role2, sex1, sex2 );     // Get relation

        // check is relation is fileld
        if( relation.isEmpty() ) {
        addToReportPerson( id_person1, source, 101, id_person2 + "" );    // EC 101
        }
        else {
        // add to relation_c
        String query = ""
        + "INSERT INTO relation_c( id_person1 , id_person2 , relation ) "
        + "values( '" + id_person1 + "' , '" + id_person2 + "' , '" + relation + "' )";

        //conCleaned.runQuery(query);
        }
        }
        }
        }

        currentId = id_registration;

        } // Current ID is -1
        // Must change
        else {
        currentId = id_registration;
        }
        }
        }
        }
        catch( Exception ex ) {
        showMessage( counter + " Exception while running Relation: " + ex.getMessage(), false, true );
        ex.printStackTrace( new PrintStream( System.out ) );
        }
        } // relation
    */

/**
 * @param SourceNo
 * @throws Exception
 */
    /*
public void standardSequence( String SourceNo) throws Exception
        {
        int counter = 0;
        int step = 1000;
        int stepstate = step;

        try {

        String selectQuery = ""
        + "SELECT "
        + "id_registration , "
        + "registration_maintype , "
        + "registration_location_no , "
        + "registration_year , "
        + "registration_month , "
        + "registration_seq , "
        + "id_source "
        + "FROM "
        + "links_cleaned.registration_c "
        + "WHERE id_source = " + SourceNo + " AND "
        + "registration_location_no is not null AND "
        + "registration_year     is not null AND "
        + "registration_month    is not null "
        + "ORDER BY "
        + "registration_maintype , "
        + "registration_location_no , "
        + "registration_year , "
        + "registration_month , "
        + "registration_seq";

        ResultSet rs = conCleaned.runQueryWithResult( selectQuery );

        // Read first entry
        if (rs.next() == false) {
        return;
        }

        int previousId = rs.getInt("id_registration");
        int previousNo = -1;
        int previousMt = rs.getInt("registration_maintype");
        int previousLc = rs.getInt("registration_location_no");
        int previousYr = rs.getInt("registration_year");
        int previousMn = rs.getInt("registration_month");
        String id_source = rs.getString("id_source");

        if (rs.getString("registration_seq") == null || rs.getString("registration_seq").isEmpty()) {
        // EC 111
        addToReportRegistration( previousId, id_source, 111, "" );
        } else { // Present
        // Is is numeric
        try {

        previousNo = Integer.parseInt(rs.getString("registration_seq"));

        }
        catch ( Exception ex ) {
        // EC 112
        addToReportRegistration( previousId, id_source, 112, rs.getString( "registration_seq" ) );
        ex.printStackTrace( new PrintStream( System.out ) );
        }
        }
        while (rs.next()) {
        counter++;
        if (counter == stepstate) {
        showMessage(counter + "", true, true);
        stepstate += step;
        }

        int nummer;

        // Is reg_seq present ?
        if (rs.getString("registration_seq") == null || rs.getString("registration_seq").isEmpty()) {

        // EC 111
        addToReportRegistration( rs.getInt("id_registration"), id_source, 111, "" );
        continue;
        }
        // Is is numeric ?
        try {
        nummer = Integer.parseInt(rs.getString("registration_seq"));
        }
        catch( Exception ex ) {
        // EC 112
        addToReportRegistration( rs.getInt("id_registration"), id_source, 112, rs.getString( "registration_seq" ) );

        // Set values
        previousId = rs.getInt("id_registration");
        previousMt = rs.getInt("registration_maintype");
        previousLc = rs.getInt("registration_location_no");
        previousYr = rs.getInt("registration_year");
        previousMn = rs.getInt("registration_month");

        continue;
        }

        int verschil = nummer - previousNo;

        if (verschil == 0 && (previousYr == rs.getInt("registration_year")) && (previousMt == rs.getInt("registration_maintype")) && (previousLc == rs.getInt("registration_location_no"))) {
        // EC 113
        addToReportRegistration( rs.getInt("id_registration"), id_source, 113, rs.getString("registration_seq") );
        } else if (verschil > 1 && (previousYr == rs.getInt("registration_year")) && (previousMt == rs.getInt("registration_maintype")) && (previousLc == rs.getInt("registration_location_no"))) {
        // EC 114
        addToReportRegistration( rs.getInt("id_registration"), id_source, 114, rs.getString("registration_seq") );
        }

        // Set values
        previousId = rs.getInt("id_registration");
        previousNo = nummer;
        previousMt = rs.getInt("registration_maintype");
        previousLc = rs.getInt("registration_location_no");
        previousYr = rs.getInt("registration_year");
        previousMn = rs.getInt("registration_month");
        }
        }
        catch( Exception ex ) {
        showMessage( counter + " Exception while checking sequence: " + ex.getMessage(), false, true );
        ex.printStackTrace( new PrintStream( System.out ) );
        }
        } // standardSequence
    */

/**
 * Parse remarks
 * @param go
 * @throws Exception
 */
    /*
    private void doRemarks( boolean go, int sourceId ) throws Exception
    {
        String funcname = "doRemarks";
        if( !go ) {
            if( showskip ) { showMessage( "Skipping " + funcname, false, true ); }
            return;
        }

        long timeStart = System.currentTimeMillis();
        showMessage( funcname + "...", false, true );

        // load all refs used by remarks Parser
        showMessage( "Loading reference tables: location/occupation...", false, true );
        {
            //ttalLocation = new TableToArraysSet(conGeneral, "original", "location");
            //ttalOccupation = new TableToArraysSet(conGeneral, "original", "occupation");
        }

        //runMethod("scanRemarks");
        for( int i : sourceList ) {
            showMessage( "Processing scanRemarks for source: " + i + "...", false, true );
            scanRemarks( "" + i );
        }

        showMessage( "Updating reference tables: " + "location/occupation" + "...", false, true );
        {
            //ttalLocation.updateTable();
            //ttalOccupation.updateTable();
        }

        elapsedShowMessage( funcname, timeStart, System.currentTimeMillis() );
    } // doRemarks
    */




/**
 * @param MethodName
 * @throws Exception
 */
    /*
    private void runMethod( String MethodName ) throws Exception
    {
        Class[] partypes = new Class[1];
        Object[] argList = new Object[1];

        partypes[0] = String.class;

        for( int i : sourceList ) {
            showMessage( "Running " + MethodName + " for source: " + i + "...", false, true );

            argList[ 0 ] = i + "";
            Method m = this.getClass().getMethod( MethodName, partypes );

            // Call method
            m.invoke( this, argList );
        }

    } // runMethod
    */

/**************************************************************************/

/**
 * calls functieOpmerkingenBeroep(),
 *      calls functieVeldBeroep,
 *          uses ttalOccupation
 *
 * @param rs
 * @param rsScanStrings
 * @return
 * @throws Exception
 */
    /*
    private HashMap functieParseRemarks(ResultSet rs, ResultSet rsScanStrings) throws Exception {

        // Hashmap voor de overgebleven
        HashMap cache = new HashMap();

        // Counter
        teller = 0;

        // Stappen instellen
        int step = 10000;

        // Door de opmerkingen heen lopen
        while (rs.next()) {
            teller++;

//            if(teller>634646){
//                int iets = 0;
//                iets++;
//            }
//            else{
//                continue;
//            }

            // Get Opmerking
            String id_registratie = rs.getString("id_registration");
            String registratie_hoofdtype = rs.getString("registration_maintype");
            String bron = rs.getString("id_source");
            String opmerking = rs.getString("remarks");

            // Controleren of de opmerking null is
            if (opmerking == null) {
                continue;
            }

            // Controleren of de opmerking leeg is
            if (opmerking.isEmpty()) {
                continue;
            }

            // Controleren of de opmerking al in de cache voorkomt
            if (cache.containsKey(registratie_hoofdtype + ":" + opmerking)) {
                Object o = cache.get(registratie_hoofdtype + ":" + opmerking).toString();
                int oToInt = Integer.parseInt(o.toString());
                int newValue = oToInt + 1;
                cache.put(registratie_hoofdtype + ":" + opmerking, newValue);
            } else {
                // TODO: Skip werkt niet helemaal goed
                int skipGroup = 0;

                // Gebruiker op de hoogte stellen
                if (teller > step) {
                    showMessage( (teller - 1) + "", true, true );
                    step += 10000;

                    // Clean memory
                    if (((teller - 1) % 50000) == 0) {
                        showMessage( "Cleaning unused memory...", true, false );
                        r.gc();
                        showMessage( "DONE!", true, true );
                    }
                }

                // Set matchfound boolean
                boolean matchFound = false;

                // Opmerking strippen aan de hand van de table
                // We lopen hier door alle regexen uit de table
                while (rsScanStrings.next()) {

                    // Er is net iets gevonden
                    if (matchFound) {

                        // Controleer of deze al in de cache zit
                        if (cache.containsKey(registratie_hoofdtype + ":" + opmerking)) {
                            break;
                        }

                        // Reset matchfound
                        matchFound = false;
                    }

                    // Haal regex uit de table
                    int aktenummer =
                            Integer.parseInt(rsScanStrings.getString("maintype"));

                    int groepnummer =
                            Integer.parseInt(rsScanStrings.getString("group_no"));

                    String scan_string = rsScanStrings.getString("scan_string");
                    String scan_waarde = rsScanStrings.getString("scan_value");
                    int role = rsScanStrings.getInt("role");
                    String veld = rsScanStrings.getString("field");

                    // Controleren of er iets verwijderd moet worden
                    // 99 betekent verwijderen
                    if (groepnummer == 99) {

                        opmerking = opmerking.replaceAll(scan_string, "");

                        continue;

                    }

                    // Controleren of dit de juiste groep is
                    if ((aktenummer != Integer.parseInt(registratie_hoofdtype))
                            && (aktenummer != 0)) {
                        continue;
                    }

                    // Reguliere expressie gaat gebruikt worden
                    Pattern regex = Pattern.compile(scan_string);

                    // Voer expressie uit op de opmerking
                    Matcher m = regex.matcher(opmerking);

                    // Controleer of er iets gevonden is
                    if (m.find()) {

                        // Controleer of deze groep overgeslagen moet worden
                        skipGroup = groepnummer;

                        // Opmerking strippen
                        opmerking = opmerking.replaceAll(scan_string, "");

                        // Destilleer het benodigde stukje op uit de opmerking
                        String currentPart = m.group();

                        HashMap insertValues = new HashMap();

                        // We controleren of de gebruiker een functie aanroept,
                        // of een expressie wil uitvoeren
                        if (scan_waarde.toLowerCase().contains("functieopmerkingen")) {

                            // regex veld bevat een functie
                            // Nu zoeken we uit om welke functie het gaat

                            if (scan_waarde.toLowerCase().contains("functieopmerkingenwaardenadubbelepunt")) {
                                String tempValue = LinksSpecific.functieOpmerkingenWaardeNaDubbelePunt(currentPart);
                                insertValues.put(veld, tempValue);
                            } else if (scan_waarde.toLowerCase().contains("functieopmerkingenberoep")) {
                                String tempValue = functieOpmerkingenBeroep(id_registratie, bron, currentPart.toLowerCase());
                                insertValues.put(veld, tempValue);
                            } else if (scan_waarde.toLowerCase().contains("functieopmerkingenlocatie")) {
                                String tempValue = functieOpmerkingenLocatie(currentPart);
                                insertValues.put(veld, tempValue);
                            } else if (scan_waarde.toLowerCase().contains("functieopmerkingenlocatieendatum")) {
                                insertValues = functieOpmerkingenLocatieEnDatum(currentPart);
                            } else if (scan_waarde.toLowerCase().contains("functieopmerkingengebdatoverl")) {
                                insertValues = functieOpmerkingenGebDatOverl(currentPart);
                            } else if (scan_waarde.toLowerCase().contains("functieopmerkingenlevenloos")) {
                                insertValues.put(veld, "ja");
                            } else if (scan_waarde.toLowerCase().contains("functieopmerkingentelocatie")) { // no prob
                                String tempValue = functieOpmerkingenTeLocatie(currentPart);
                                insertValues.put(veld, tempValue);
                            } else if (scan_waarde.toLowerCase().contains("functieopmerkingenoudbijna")) { // no prob
                                insertValues.put(veld, "1");
                            } else if (scan_waarde.toLowerCase().contains("functieopmerkingeneldersoverleden")) { // no prob

                                // Update register.extract
                                String query = "UPDATE registration_c"
                                        + " SET registration_c.extract = 'y'"
                                        + " WHERE registration_c.id_registration = " + id_registratie;

                                // Voer query uit
                                conCleaned.runQuery(query);

                                // Clean datatype
                                insertValues.clear();
                            } // Extract met locatie
                            else if (scan_waarde.toLowerCase().contains("functieopmerkingenextract")) { // no prob

                                // Haal locatie op
                                String location = currentPart.replaceAll("(Extract|[U|u]ittreksel)[ ]*overlijdensregister[ ]*", "");

                                // Clean locatie
                                String cleanLocation = LinksSpecific.funcCleanSides(location);

                                // Schone locatie
                                String returnedLocation = functieVeldLocatie(cleanLocation);

                                // Maak query
                                String query = "UPDATE registration_c"
                                        + " SET registration_c.extract = 'y'"
                                        + " WHERE registration_c.id_registration = " + id_registratie;

                                // Voer query uit
                                conCleaned.runQuery(query);

                                // Geef locatie door
                                insertValues.put("death_location", returnedLocation);
                            } // Leeftijd jaar
                            else if (scan_waarde.toLowerCase().contains("functieopmerkingenleeftijdjaar")) {
                                Pattern regexval = Pattern.compile("[0-9]+");
                                Matcher mval = regexval.matcher(currentPart);

                                if (mval.find()) {
                                    // Er is een leeftijd gevonden

                                    int leeftijd = 0;

                                    try {
                                        leeftijd = Integer.parseInt(mval.group());
                                    } catch (Exception we) {
                                        // We doen hier niets mee omdat het nooit zal gebeuren
                                        // dat de expressie [0-9]+ een nietgetal zal opleveren
                                        // We hebben wel een catch voor de 0% uitzondering
                                    }

                                    // Geldige leeftijd valt tussen de 0 en 115
                                    if ((leeftijd > 0) && (leeftijd < 115)) {
                                        insertValues.put(veld, leeftijd);
                                    } else {
                                        // TODO: message invalid age

                                        // Leeg de dataset
                                        insertValues.clear();
                                    }
                                }

                                // Er is niets gevonden
                                insertValues.clear();
                            } else if (scan_waarde.toLowerCase().contains("functieopmerkingengeboreninjaartal")) {
                                String jaartal = currentPart.replaceAll("Geboren[ ]*in[ ]*", "");

                                int intJaartal = 0;

                                try {
                                    intJaartal = Integer.parseInt(jaartal);
                                } catch (Exception qa) {
                                    // We doen hier niets mee omdat het nooit zal gebeuren
                                    // dat de expressie [0-9]+ een nietgetal zal opleveren
                                    // We hebben wel een catch voor de 0% uitzondering
                                }

                                // Controleren op geldigheid jaartal
                                if ((intJaartal > 1680)
                                        && (intJaartal < 1960)) {

                                    insertValues.put(veld, intJaartal);
                                } else {

                                    // TODO: messahe invaled year
                                    insertValues.clear();

                                }
                            }
                        } else if (scan_waarde.toLowerCase().contains("functieopmerkingenwoonplaats")) {
                            String location = currentPart.replaceAll("[W|w]oonpl[a]?[a]?[t]?[s]?[ ]*", "");

                            String cleanLocation = LinksSpecific.funcCleanSides(location);

                            String returnedLocation = functieVeldLocatie(cleanLocation);

                            insertValues.put(veld, returnedLocation);
                        } else if (scan_waarde.toLowerCase().contains("functieopmerkingenweduw")) {
                            insertValues.put(veld, "verweduwd");
                        } else if (scan_waarde.toLowerCase().contains("functieopmerkingengeboortelocatiehuw")) {
                            String location = currentPart.replaceAll("[G|g]eboortepl[a]?[a]?[t]?[s]?[ ]*bruid[ ]+", "").replaceAll("[G|g]eboortepl[a]?[a]?[t]?[s]?[ ]*bruidegom[ ]+", "");

                            String cleanLocation = LinksSpecific.funcCleanSides(location);

                            String returnedLocation = functieVeldLocatie(cleanLocation);

                            insertValues.put(veld, returnedLocation);
                        } else if (scan_waarde.toLowerCase().contains("functieopmerkingenleeftijduren")) {
                            Pattern regexval = Pattern.compile("[0-9]+");
                            Matcher mval = regexval.matcher(currentPart);

                            if (mval.find()) {
                                int days = 0;

                                try {
                                    days = Integer.parseInt(mval.group());
                                } catch (Exception we) {
                                    // We doen hier niets mee omdat het nooit zal gebeuren
                                    // dat de expressie [0-9]+ een nietgetal zal opleveren
                                    // We hebben wel een catch voor de 0% uitzondering
                                }

                                if ((days > 0) && (days < 36)) {
                                    insertValues.put(veld, "1");
                                } else {
                                    // TODO: Onduidelijke melding

                                    insertValues.clear();
                                }
                            }

                            insertValues.clear();
                        } else if (scan_waarde.toLowerCase().contains("functieopmerkingendoopplaats")) { // no prob
                            String tempValue = functieOpmerkingenTeLocatie(currentPart);
                            insertValues.put(veld, tempValue);
                        }
                        // Het gaat om ene expressie en geen functie
                        else {

                            // Expressie wordt uitgevoerd
                            Pattern regexval = Pattern.compile(scan_waarde);
                            Matcher mval = regexval.matcher(currentPart);

                            // Als er wat gevonden, wordt dit toegevoegd
                            if (mval.find()) {

                                insertValues.put(veld, mval.group());

                            }

                        }

                        // Verwerkingsfase:
                        // Controleer of er iets te verwerken valt
                        if (!insertValues.isEmpty()) {

                            // Maak een iterator aan om door de set te loopen
                            Set keySet = insertValues.keySet();
                            Iterator keySetIterator = keySet.iterator();

                            // Itereer door de dataset heen
                            while (keySetIterator.hasNext()) {

                                Object key = keySetIterator.next();
                                Object value = insertValues.get(key);

                                // Controleer op null
                                String valueQ = "";

                                if (value != null) {

                                    valueQ =
                                            LinksSpecific.funcPrepareForMysql(value.toString());

                                }

                                // Maak query aan
                                String query = "UPDATE person_c"
                                        + " SET person_c." + key.toString() + " = '" + valueQ + "'"
                                        + " WHERE person_c.id_registration = " + id_registratie
                                        + " AND person_c.role = " + role;

                                // Voer query uit
                                conCleaned.runQuery(query);
                            }

                            // Zet flag op true
                            // Nu wordt er eerst in cache gekeken
                            // alvorens er verder wordt gestript
                            matchFound = true;
                        }
                    }
                }

                // De resultset Iterator weer terugzetten
                rsScanStrings.beforeFirst();

                // We zijn door de opmerking heen
                // Controleer of de 'rest' opmerking ana de cache toegevoegd wordt
                if (cache.containsKey(registratie_hoofdtype + ":" + opmerking)) {

                    int newValue = Integer.parseInt(
                            cache.get(registratie_hoofdtype + ":" + opmerking).toString()) + 1;

                    cache.put(registratie_hoofdtype + ":" + opmerking, newValue);
                } else {

                    cache.put(registratie_hoofdtype + ":" + opmerking, 1);
                }
            }
        }

        return cache;
    } // functieParseRemarks
    */

/**
 * calls functieVeldBeroep()
 *
 * @return
 * @throws Exception
 */
    /*
    private String functieOpmerkingenBeroep(String id_registratie, String bron, String value) throws Exception
    {
        String beroep = LinksSpecific.functieOpmerkingenWaardeNaDubbelePunt(value);

        // verwerken, gaat via een generieke methode, ergens anders hebben we geen waarde na opmerking
        String nieuweBeroep = functieVeldBeroep(id_registratie, bron, beroep);

        // return event. bewerkte beroep
        return nieuweBeroep;
    } // functieOpmerkingenBeroep
    */

    /*
    private String functieOpmerkingenTeLocatie(String currentPart) {
        String[] locationRaw = currentPart.split("te");

        // to prevent null pointer exception
        if (locationRaw.length > 1) {
            String location = locationRaw[1];
            String cleanLocation = LinksSpecific.funcCleanSides(location);
            return cleanLocation;
        } else {
            return "";
        }
    } // functieOpmerkingenTeLocatie
    */

    /*
    private String functieOpmerkingenLocatie(String value) throws Exception {
        String locatie = LinksSpecific.functieOpmerkingenWaardeNaDubbelePunt(value);

        // verwerken, gaat via een generieke methode, ergens anders hebben we geen waarde na opmerking
        String nieuweLocatie = functieVeldLocatie(locatie);

        // return event. bewerkte beroep
        return nieuweLocatie;
    } // functieOpmerkingenLocatie
    */

    /*
    private HashMap functieOpmerkingenGebDatOverl(String currentPart) {
        String cleanValue = currentPart.replaceAll("[G|g]eboren", "");

        String cleanDate = LinksSpecific.funcCleanSides(cleanValue);

        // create hashmap to put values into
        HashMap values = new HashMap();

        values.put("birth_date", cleanDate);

        // date has to be devided
        DateYearMonthDaySet dymd = LinksSpecific.divideCheckDate( cleanDate );

        //add location
        values.put("birth_day", dymd.getDay());
        values.put("birth_month", dymd.getMonth());
        values.put("birth_year", dymd.getYear());

        // TODO: Verwerk logbestanden
        if (!dymd.getReportDay().isEmpty()) {
        }
        if (!dymd.getReportMonth().isEmpty()) {
        }
        if (!dymd.getReportYear().isEmpty()) {
        }

        //return
        return values;
    } // functieOpmerkingenGebDatOverl
    */

    /*
    private HashMap functieOpmerkingenLocatieEnDatum(String currentPart) throws Exception {
        String cleanValue = LinksSpecific.functieOpmerkingenWaardeNaDubbelePunt(currentPart);

        // functie datum scheiding
        String[] devidedValueDate = divideValueDate( cleanValue ).split( "&" );

        HashMap values = new HashMap();

        //add location, via verwijzing
        values.put("birth_location", functieVeldLocatie(LinksSpecific.funcCleanSides(devidedValueDate[0])));


        String date = LinksSpecific.funcCleanSides(devidedValueDate[1]);

        values.put("birth_date", date);

        // date has to be devided
        DateYearMonthDaySet dymd = LinksSpecific.divideCheckDate( date );

        //add location
        values.put("birth_day", dymd.getDay());
        values.put("birth_month", dymd.getMonth());
        values.put("birth_year", dymd.getYear());

        // TODO: Verwerk logbestanden
        if (!dymd.getReportDay().isEmpty()) {
        }
        if (!dymd.getReportMonth().isEmpty()) {
        }
        if (!dymd.getReportYear().isEmpty()) {
        }

        //return
        return values;
    } // functieOpmerkingenLocatieEnDatum
    */

/**
 * Verwerk in de lijst, en geef evt. ander beroep terug
 * uses ttalOccupation
 *
 * @return
 * @throws Exception
 */
    /*
    private String functieVeldBeroep(String id_registratie, String id_bron, String beroepTemp) throws Exception
    {
        if (beroepTemp != null) {

            // String beroep = beroepTemp.toLowerCase();
            String beroep = beroepTemp;

            if (ttalOccupation.originalExists(beroep)) {
                String nieuwCode = ttalOccupation.getStandardCodeByOriginal(beroep);

                if (nieuwCode == null ? SC_X == null : (nieuwCode.equals(SC_X))) {
                    // melding 41
                    try {
                        //addToLogTableRegistratie(id_registratie, id_bron, ErrorCode.WA, 41, "Beroep: " + LinksSpecific.functieParserVerbeterCharVoorMysql(beroepTemp) + " , geen standaard aanwezig, nieuwcode x");
                    } catch (Exception e) {
                    }

                    return beroep;
                } else if (nieuwCode == null ? SC_N == null : nieuwCode.equals(SC_N)) {
                    // melding 43
                    try {
                        //addToLogTableRegistratie(id_registratie, id_bron, ErrorCode.FT, 43, "Beroep: " + LinksSpecific.functieParserVerbeterCharVoorMysql(beroepTemp) + " , Ongeldig, geen standaard aanwezig, nieuwcode n");
                    } catch (Exception e) {
                    }

                    return "";
                } else if (nieuwCode == null ? SC_U == null : nieuwCode.equals(SC_U)) {
                    // melding 43
                    try {
                        //addToLogTableRegistratie(id_registratie, id_bron, ErrorCode.WA, 45, "Beroep: " + LinksSpecific.functieParserVerbeterCharVoorMysql(beroepTemp) + " , Ongeldig, wel standaard aanwezig, nieuwcode n");
                    } catch (Exception e) {
                    }

                    String beroepFromTable = ttalOccupation.getStandardByOriginal(beroep);
                    return beroepFromTable;
                } else if (nieuwCode == null ? SC_Y == null : nieuwCode.equals(SC_Y)) {
                    String beroepFromTable = ttalOccupation.getStandardByOriginal(beroep);
                    return beroepFromTable;
                } else {
                    return "";
                }
            } else {
                // melding 41
                try {
                    // addToLogTableRegistratie(id_registratie, id_bron, ErrorCode.WA, 41, "Beroep: " + LinksSpecific.functieParserVerbeterCharVoorMysql(beroepTemp) + " , geen standaard aanwezig, nieuwcode x");
                } catch (Exception e) {
                }

                // No beroep in Origineel
                ttalOccupation.addOriginal(beroep);
                return beroep;
            }
        }
        return "";
    } // functieVeldBeroep
    */

// Verwerk in de lijst, en geef evt. ander locatie terug
    /*
    private String functieVeldLocatie(String locatieTemp) throws Exception {
        if (locatieTemp != null) {

            // String locatie = locatieTemp.toLowerCase();
            String locatie = locatieTemp;

            if (ttalLocation.originalExists(locatie)) {
                String nieuwCode = ttalLocation.getStandardCodeByOriginal(locatie);

                if (nieuwCode == null ? SC_X == null : (nieuwCode.equals(SC_X))) {
                    // melding 32
                    return "";
                } else if (nieuwCode == null ? SC_N == null : nieuwCode.equals(SC_N)) {
                    // melding 33
                    return "";
                } else if (nieuwCode == null ? SC_U == null : nieuwCode.equals(SC_U)) {
                    // melding 34
                    String locatieFromTable = ttalLocation.getColumnByOriginal("location_no", locatie);
                    return locatieFromTable;
                } else if (nieuwCode == null ? SC_Y == null : nieuwCode.equals(SC_Y)) {
                    String locatieFromTable = ttalLocation.getColumnByOriginal("location_no", locatie);
                    return locatieFromTable;
                } else {
                    return "";
                }
            } else {
                ttalLocation.addOriginal(locatie);
            }
        }
        return "";
    } // functieVeldLocatie
    */

    /*
    private String divideValueDate( String valueToDevide )
    {
        Pattern regex = Pattern.compile( "[0-9]+-[0-9]+-[0-9]+" );
        Matcher m = regex.matcher( valueToDevide );

        String date = m.group();

        String onlyData = valueToDevide.replaceAll( "[0-9]+-[0-9]+-[0-9]+", "" );

        return onlyData + "$" + date;
    } // divideValueDate
    */

/**
 * calls functieParseRemarks()
 *
 * @throws Exception
 */
    /*
    public void scanRemarks(String bronnr) throws Exception
    {
        // Lees Scan instellingen in
        showMessage("Preparing remarks parsing...", false, false);
        ResultSet rsScanStrings = conGeneral.runQueryWithResult(
                "SELECT * FROM scan_remarks ORDER BY maintype, group_no, priority_no");

        String query;

        if (bronnr.isEmpty()) {
            query = "SELECT id_registration , id_source , registration_maintype , remarks FROM registration_o" + sourceFilter;
        } else {
            query = "SELECT id_registration , id_source , registration_maintype , remarks FROM registration_o WHERE id_source = " + bronnr;
        }


        // Lees Opmerkingen in
        ResultSet rs = conOriginal.runQueryWithResult( query );
        showMessage( endl, false, true );

        // Parsing Opmerkingen
        showMessage("Parsing remarks...", false, false);
        HashMap cache;
        try {
            cache = functieParseRemarks(rs, rsScanStrings);
        } catch (Exception e) {
            showMessage(teller + " ERROR:" + e.getMessage(), false, false);
            return;
        }
        showMessage( endl, false, true );

        // Maak log table aan met resterende opmekingen
        String createQuery = ""
                + "CREATE TABLE IF NOT EXISTS `links_logs`.`log_rest_remarks_" + sourceId + bronnr + "_" + logTableName + "` (  "
                + "`id_log` INT NOT NULL AUTO_INCREMENT , "
                + "`registration_maintype` VARCHAR(3) NULL , "
                + "`content` VARCHAR(500) NULL , "
                + "`frequency` INT NULL , "
                + "PRIMARY KEY (`id_log`) , "
                + "INDEX `defaultindex` (`id_log` ASC) ) "
                + "DEFAULT CHARACTER SET = utf8;";

        // Voer query uit
        conLog.runQuery( createQuery );

        // Cache overzetten
        Set keySet = cache.keySet();
        Iterator keySetIterator = keySet.iterator();

        showMessage( "Writing rest remarks tot database...", false, false );

        // Loop door de resterende opmerkingen heen
        // while (keySetIterator.hasNext()) {
        //
        // Object key = keySetIterator.next();
        // Object value = cache.get(key);
        //
        // String[] velden = {"registration_maintype", "content", "frequency"};
        //
        // // eventuele quotes vervangen
        // String cleanKey = LinksSpecific.funcPrepareForMysql(key.toString());
        // String[] data = {cleanKey.substring(0, cleanKey.indexOf(":")), cleanKey.substring((cleanKey.indexOf(":") + 1)), value.toString()};
        // conLog.insertIntoTable("log_rest_remarks_" + sourceId + bronnr + "_" + logTableName, velden, data);
        // }

        rs.close();
        rs = null;

        showMessage( endl, false, true );
    } // scanRemarks
    */


/**
 *
 */
    /*
    private void setSourceFilters()
    {
        showMessage( "Set source filters for: " + sourceId, false, true );

        //sourceFilter = " WHERE id_source = " + sourceId;

        //sourceFilterCleanPers = " WHERE person_c.id_source = "       + sourceId;
        //sourceFilterOrigPers  = " WHERE person_o.id_source = "       + sourceId;
        //sourceFilterCleanReg  = " WHERE registration_c.id_source = " + sourceId;
        //sourceFilterOrigReg   = " WHERE registration_o.id_source = " + sourceId;

    } // setSourceFilters
    */



// ---< Functions using hp* objects >---------------------------------------

/**
 * @param id_registration
 * @param registration_maintype
 * @param role
 * @return
 */
    /*
    private Ages returnAgeCentralFigure( int id_registration, int registration_maintype, int role )
    {
        Ages ages = new Ages();

        // age of central figure
        if( registration_maintype == 1 )
        {
            // int indexNr = hpChildRegistration.indexOf(id_registration);
            int indexNr = Collections.binarySearch( hpChildRegistration, id_registration );

            // Check if number excists in list
            // add age
            if( indexNr > -1 ) {
                ages.setYear(  hpChildAge.get(   indexNr ) );
                ages.setMonth( hpChildMonth.get( indexNr ) );
                ages.setWeek(  hpChildWeek.get(  indexNr) );
                ages.setDay(   hpChildDay.get(   indexNr ) );
            }
        }

        else if( (registration_maintype == 2) && ((role == 5) || (role == 6)) )
        {
            // int indexNr = hpBrideRegistration.indexOf(id_registration);
            int indexNr = Collections.binarySearch( hpBrideRegistration, id_registration );

            // check is number exists
            // add age
            if( indexNr > -1 ) {
                ages.setYear(  hpBrideAge.get(   indexNr ) );
                ages.setMonth( hpBrideMonth.get( indexNr ) );
                ages.setWeek(  hpBrideWeek.get(  indexNr ) );
                ages.setDay(   hpBrideDay.get(   indexNr ) );
            }
        }

        else if( (registration_maintype == 2) && ((role == 8) || (role == 9)) )
        {
            // int indexNr = hpGroomRegistration.indexOf(id_registration);
            int indexNr = Collections.binarySearch( hpGroomRegistration, id_registration );

            // check existence of number in list
            // add age
            if( indexNr > -1 ) {
                ages.setYear(  hpGroomAge.get(   indexNr ) );
                ages.setMonth( hpGroomMonth.get( indexNr ) );
                ages.setWeek(  hpGroomWeek.get(  indexNr ) );
                ages.setDay(   hpGroomDay.get(   indexNr ) );
            }
        }

        else if( registration_maintype == 3 )
        {
            // int indexNr = hpDeceasedRegistration.indexOf(id_registration);
            int indexNr = Collections.binarySearch( hpDeceasedRegistration, id_registration );

            // check number if exists
            // add age
            if( indexNr > -1 ) {
                ages.setYear(  hpDeceasedAge.get(   indexNr ) );
                ages.setMonth( hpDeceasedMonth.get( indexNr ) );
                ages.setWeek(  hpDeceasedWeek.get(  indexNr ) );
                ages.setDay(   hpDeceasedDay.get(   indexNr) ) ;
            }
        }

        return ages;
    } // returnAgeCentralFigure
    */

/**
 * @param sourceNo
 * @throws Exception
 */
    /*
    private void fillMinMaxArrays( String sourceNo ) throws Exception
    {
        hpChildRegistration.clear();
        hpChildAge.clear();
        hpBrideRegistration.clear();
        hpBrideAge.clear();
        hpGroomRegistration.clear();
        hpGroomAge.clear();
        hpDeceasedRegistration.clear();
        hpDeceasedAge.clear();

        String query1 = "SELECT id_registration , age_year , age_month , age_week , age_day FROM person_c WHERE role =  '1' AND id_source = " + sourceNo + " ORDER BY id_registration ASC";
        String query2 = "SELECT id_registration , age_year , age_month , age_week , age_day FROM person_c WHERE role =  '7' AND id_source = " + sourceNo + " ORDER BY id_registration ASC";
        String query3 = "SELECT id_registration , age_year , age_month , age_week , age_day FROM person_c WHERE role =  '4' AND id_source = " + sourceNo + " ORDER BY id_registration ASC";
        String query4 = "SELECT id_registration , age_year , age_month , age_week , age_day FROM person_c WHERE role = '10' AND id_source = " + sourceNo + " ORDER BY id_registration ASC";

        ResultSet rs1 = conCleaned.runQueryWithResult( query1 );
        ResultSet rs2 = conCleaned.runQueryWithResult( query2 );
        ResultSet rs3 = conCleaned.runQueryWithResult( query3 );
        ResultSet rs4 = conCleaned.runQueryWithResult( query4 );

        while( rs1.next() ) {
            hpChildRegistration.add( rs1.getInt( "id_registration" ) );
            hpChildAge.add(   rs1.getInt( "age_year" ) );
            hpChildMonth.add( rs1.getInt( "age_month" ) );
            hpChildWeek.add(  rs1.getInt( "age_week" ) );
            hpChildDay.add(   rs1.getInt( "age_day" ) );
        }

        while( rs2.next() ) {
            hpGroomRegistration.add( rs2.getInt( "id_registration" ) );
            hpGroomAge.add(   rs2.getInt( "age_year" ) );
            hpGroomMonth.add( rs2.getInt( "age_month" ) );
            hpGroomWeek.add(  rs2.getInt( "age_week" ) );
            hpGroomDay.add(   rs2.getInt( "age_day" ) );
        }

        while( rs3.next() ) {
            hpBrideRegistration.add( rs3.getInt( "id_registration" ) );
            hpBrideAge.add(   rs3.getInt( "age_year" ) );
            hpBrideMonth.add( rs3.getInt( "age_month" ) );
            hpBrideWeek.add(  rs3.getInt( "age_week" ) );
            hpBrideDay.add(   rs3.getInt( "age_day" ) );
        }

        while( rs4.next() ) {
            hpDeceasedRegistration.add( rs4.getInt( "id_registration" ) );
            hpDeceasedAge.add(   rs4.getInt( "age_year" ) );
            hpDeceasedMonth.add( rs4.getInt( "age_month" ) );
            hpDeceasedWeek.add(  rs4.getInt( "age_week" ) );
            hpDeceasedDay.add(   rs4.getInt( "age_day" ) );
        }
    } // fillMinMaxArrays
    */

