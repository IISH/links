/**
 * @author Fons Laan
 *
 * obsolete functions removed from LinksCleanThread.java
 *
 * FL-13-Oct-2014 Latest change
 */

    // Table -> ArraysSet
    //private TableToArraysSet ttalPrepiece;            // Names
    //private TableToArraysSet ttalSuffix;              // Names
    //private TableToArraysSet ttalAlias;               // Names
    //private TableToArraysSet ttalFirstname;           // Names
    //private TableToArraysSet ttalFamilyname;          // names
    //private TableToArraysSet ttalLocation;            // Location
    //private TableToArraysSet ttalOccupation;          // Occupation
    //private TableToArraysSet ttalRegistration;        // formerly used in standardType()
    //private TableToArraysSet ttalReport;              // Report warnings
    //private TableToArraysSet ttalRole;                // Role
    //private TableToArraysSet ttalStatusSex;           // Civilstatus & Gender

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

    // WHERE [...]id_source = ...   shortcuts
    //private String sourceFilter = "";
    //private String sourceFilterCleanPers = "";
    //private String sourceFilterOrigPers = "";
    //private String sourceFilterCleanReg = "";
    //private String sourceFilterOrigReg = "";

    //private int teller = 0;


/**
 * @param id
 * @param id_source
 * @param errorCode
 * @param value
 * @throws Exception
 */
    /*
    private void addToReportRegistration_old( int id, String id_source, int errorCode, String value )
    {
        if( ! use_links_logs ) { return; }

        boolean debug = false;
        if( debug ) { showMessage( "addToReportRegistration()", false, true ); }

        String errorCodeStr = Integer.toString( errorCode );

        String cla = almmReport.value( "class",   errorCodeStr );
        String con = almmReport.value( "content", errorCodeStr );

        if( debug ) { System.out.println( "cla: " + cla + ", con: " + con ); }

        // WORKAROUND
        // replace error chars
        value = value.replaceAll( "\\\\", "" );
        value = value.replaceAll( "\\$", "" );
        value = value.replaceAll( "\\*", "" );

        con = con.replaceAll( "<.*>", value );
        //con = LinksSpecific.prepareForMysql( con );
        con = con.replace( "'",  "\\'" );             // escape single quotes
        con = con.replace( "\"", "\\\"" );            // escape double quotes

        // get registration values from links_original.registration_o
        String location  = "";
        String reg_type  = "";
        String date      = "";
        String sequence  = "";
        String guid      = "";

        String selectQuery = "SELECT registration_location , registration_type , registration_date , registration_seq , id_persist_registration"
            + " FROM registration_o WHERE id_registration = " + id;

        if( debug ) {
            System.out.println( selectQuery );
            showMessage( selectQuery, false, true );
        }

        try {
            ResultSet rs = dbconOriginal.runQueryWithResult( selectQuery );

            rs.first();
            location = rs.getString( "registration_location" );
            reg_type = rs.getString( "registration_type" );
            date     = rs.getString( "registration_date" );
            sequence = rs.getString( "registration_seq" );
            guid     = rs.getString( "id_persist_registration" );
        }
        catch( Exception ex ) {
            showMessage( selectQuery, false, true );
            showMessage( ex.getMessage(), false, true );
            ex.printStackTrace( new PrintStream( System.out ) );
        }

        if( location == null) { location = ""; }
        if( reg_type == null) { reg_type = ""; }
        if( date     == null) { date     = ""; }
        if( sequence == null) { sequence = ""; }
        if( guid     == null) { guid     = ""; }

        location = location.replace( "'",  "\\'" );     // escape single quotes
        location = location.replace( "\"", "\\\"" );    // escape double quotes
        location = location.replace( "\\", "\\\\" );    // escape backslash

        reg_type = reg_type.replace( "'",  "\\'" );     // escape single quotes
        reg_type = reg_type.replace( "\"", "\\\"" );    // escape double quotes
        reg_type = reg_type.replace( "\\", "\\\\" );    // escape backslash

        date = date.replace( "'",  "\\'" );             // escape single quotes
        date = date.replace( "\"", "\\\"" );            // escape double quotes
        date = date.replace( "\\", "\\\\" );            // escape backslash

        sequence = sequence.replace( "'",  "\\'" );     // escape single quotes
        sequence = sequence.replace( "\"", "\\\"" );    // escape double quotes
        sequence = sequence.replace( "\\", "\\\\" );    // escape backslash

        guid = guid.replace( "'",  "\\'" );             // escape single quotes
        guid = guid.replace( "\"", "\\\"" );            // escape double quotes
        guid = guid.replace( "\\", "\\\\" );            // escape backslash

        String s = "INSERT INTO links_logs.`" + logTableName + "`"
            + " ( reg_key , id_source , report_class , report_type , content ,"
            + " date_time , location , reg_type , date , sequence , guid )"
            + " VALUES ( %d , \"%s\" , \"%s\" , \"%s\" , \"%s\" , NOW() , \"%s\" , \"%s\" , \"%s\" , \"%s\" , \"%s\" ) ;";

        if( debug ) {
            System.out.println( s );
            showMessage( s, false, true );
        }

        String insertQuery = "";
        try {
            insertQuery = String.format ( s,
                id, id_source, cla.toUpperCase(), errorCode, con, location, reg_type, date, sequence, guid );
            if( debug ) {
                System.out.println( insertQuery );
                showMessage( insertQuery, false, true );
            }
        }
        catch( Exception ex )
        {
            System.out.println( "reg_key     : " + id );
            System.out.println( "id_source   : " + id_source );
            System.out.println( "report_class: " + cla.toUpperCase() );
            System.out.println( "report_type : " + errorCode );
            System.out.println( "content     : " + con );
            System.out.println( "date_time   : " + "NOW()" );
            System.out.println( "location    : " + location );
            System.out.println( "reg_type    : " + reg_type );
            System.out.println( "date        : " + date );
            System.out.println( "sequence    : " + sequence );
            System.out.println( "guid        : " + guid );

            showMessage( s, false, true );
            showMessage( insertQuery, false, true );
            showMessage( ex.getMessage(), false, true );
            ex.printStackTrace();
        }

        try { dbconLog.runQuery( insertQuery ); }
        catch( Exception ex ) {
            showMessage( "source: " + id_source + ", query: " + insertQuery, false, true );
            showMessage( ex.getMessage(), false, true );
            ex.printStackTrace();
        }
    } // addToReportRegistration_old
    */


/**
 * @param id
 * @param id_source
 * @param errorCode
 * @param value
 * @throws Exception
 */
    /*
    private void addToReportPerson_old( int id, String id_source, int errorCode, String value )
    {
        if( ! use_links_logs ) { return; }

        boolean debug = false;
        if( debug ) { showMessage( "addToReportPerson()", false, true ); }

        String errorCodeStr = Integer.toString( errorCode );

        String cla = almmReport.value( "class",   errorCodeStr );
        String con = almmReport.value( "content", errorCodeStr );

        if( debug ) { System.out.println( "cla: " + cla + ", con: " + con ); }

        // WORKAROUND
        // replace error chars
        value = value.replaceAll( "\\\\", "" );
        value = value.replaceAll( "\\$", "" );
        value = value.replaceAll( "\\*", "" );

        con = con.replaceAll( "<.*>", value );
        //con = LinksSpecific.prepareForMysql( con );
        con = con.replace( "'",  "\\'" );             // escape single quotes
        con = con.replace( "\"", "\\\"" );            // escape double quotes

        // get id_registration from links_original.person_o
        String id_registration = "";
        String role            = "";

        String selectQuery1 = "SELECT id_registration, role FROM person_o WHERE id_person = " + id;
        try {
            ResultSet rs = dbconOriginal.runQueryWithResult( selectQuery1 );
            rs.next();
            id_registration = rs.getString( "id_registration" );
            role            = rs.getString( "role" );
        }
        catch( Exception ex ) {
            showMessage( ex.getMessage(), false, true );
            ex.printStackTrace( new PrintStream( System.out ) );
        }

        // get registration values from links_original.registration_o
        String location  = "";
        String reg_type  = "";
        String date      = "";
        String sequence  = "";
        String guid      = "";

        if( !id_registration.isEmpty() )
        {
            String selectQuery2 = "SELECT registration_location , registration_type , registration_date , registration_seq , id_persist_registration"
                + " FROM registration_o WHERE id_registration = " + id_registration;

            if( debug ) { showMessage( selectQuery2, false, true ); }

            try {
                ResultSet rs = dbconOriginal.runQueryWithResult( selectQuery2 );
                rs.next();
                location = rs.getString( "registration_location" );
                reg_type = rs.getString( "registration_type" );
                date     = rs.getString( "registration_date" );
                sequence = rs.getString( "registration_seq" );
                guid     = rs.getString( "id_persist_registration" );
            }
            catch( Exception ex ) {
                showMessage( "source: " + id_source + "query: " + selectQuery2, false, true );
                showMessage( ex.getMessage(), false, true );
                ex.printStackTrace( new PrintStream( System.out ) );
            }
        }

        if( location == null) { location = ""; }
        if( reg_type == null) { reg_type = ""; }
        if( date     == null) { date     = ""; }
        if( sequence == null) { sequence = ""; }
        if( guid     == null) { guid     = ""; }

        location = location.replace( "'",  "\\'" );     // escape single quotes
        location = location.replace( "\"", "\\\"" );    // escape quotes quotes

        reg_type = reg_type.replace( "'",  "\\'" );     // escape single quotes
        reg_type = reg_type.replace( "\"", "\\\"" );    // escape quotes quotes

        date = date.replace( "'",  "\\'" );             // escape single quotes
        date = date.replace( "\"", "\\\"" );            // escape quotes quotes

        sequence = sequence.replace( "'",  "\\'" );     // escape single quotes
        sequence = sequence.replace( "\"", "\\\"" );    // escape quotes quotes

        guid = guid.replace( "'",  "\\'" );             // escape single quotes
        guid = guid.replace( "\"", "\\\"" );            // escape quotes quotes

        // to prevent: Data truncation: Data too long for column 'sequence'
        if( sequence != null && sequence.length() > 20 )
        { sequence = sequence.substring( 0, 20 ); }

        String s = "INSERT INTO links_logs.`" + logTableName + "`"
            + " ( pers_key , id_source , report_class , report_type , content ,"
            + " date_time , location , reg_type , date , sequence , role, reg_key, guid )"
            + " VALUES ( %d , \"%s\" , \"%s\" , \"%s\" , \"%s\" , NOW() , \"%s\" , \"%s\" , \"%s\" , \"%s\" , \"%s\" , \"%s\" , \"%s\" ) ;";

        if( debug ) {
            System.out.println( s );
            showMessage( s, false, true );
        }

        String insertQuery = "";
        try {
            insertQuery = String.format ( s,
                id, id_source, cla.toUpperCase(), errorCode, con, location, reg_type, date, sequence, role, id_registration, guid );
            if( debug ) {
                System.out.println( insertQuery );
                showMessage( insertQuery, false, true );
            }
        }
        catch( Exception ex )
        {
            System.out.println( "pers_key        : " + id );
            System.out.println( "id_source       : " + id_source );
            System.out.println( "report_class    : " + cla.toUpperCase() );
            System.out.println( "report_type     : " + errorCode );
            System.out.println( "content         : " + con );
            System.out.println( "date_time       : " + "NOW()" );
            System.out.println( "location        : " + location );
            System.out.println( "reg_type        : " + reg_type );
            System.out.println( "date            : " + date );
            System.out.println( "sequence        : " + sequence );
            System.out.println( "role            : " + role );
            System.out.println( "id_registration : " + id_registration );
            System.out.println( "guid            : " + guid );

            showMessage( s, false, true );
            showMessage( insertQuery, false, true );
            showMessage( ex.getMessage(), false, true );
            ex.printStackTrace();
        }

        try { dbconLog.runQuery( insertQuery ); }
        catch( Exception ex ) {
            showMessage( "source: " + id_source + ", query: " + insertQuery, false, true );
            showMessage( ex.getMessage(), false, true );
            ex.printStackTrace();
        }
    } // addToReportPerson_old
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


// begin Obsolete Dates stuff

        // deleted from doDates
        /*
        // superfluous, remove
        standardFlaggedDate( "birth" );
        standardFlaggedDate( "mar" );
        standardFlaggedDate( "death" );
        */

        // FL-04-Sep-2014
        // commented out function calls below: probably overlapping functionality with doMinMaxDate ?!

        //showMessage( "Running completeMinMaxBirth...", false, true );
        //completeMinMaxBirth();

        //showMessage( "Running completeMinMaxMar...", false, true );
        //completeMinMaxMar();

        // no function completeMinMaxDeath() : NO, not needed

        /*
        if( showskip ) { showMessage( "Skipping registration_c updates", false, true ); }
        showMessage( "Running update queries...", false, true );
        // extra function to correct registration data
        String q1 = "UPDATE links_cleaned.registration_c AS r, links_cleaned.person_c AS p SET r.registration_date = p.birth_date WHERE r.registration_date IS NULL AND r.id_registration = p.id_registration AND r.registration_maintype = 1 AND p.role = 1;";
        String q2 = "UPDATE links_cleaned.registration_c AS r, links_cleaned.person_c AS p SET r.registration_date = p.mar_date   WHERE r.registration_date IS NULL AND r.id_registration = p.id_registration AND r.registration_maintype = 2 AND p.role = 4;";
        String q3 = "UPDATE links_cleaned.registration_c AS r, links_cleaned.person_c AS p SET r.registration_date = p.mar_date   WHERE r.registration_date IS NULL AND r.id_registration = p.id_registration AND r.registration_maintype = 2 AND p.role = 7;";
        String q4 = "UPDATE links_cleaned.registration_c AS r, links_cleaned.person_c AS p SET r.registration_date = p.death_date WHERE r.registration_date IS NULL AND r.id_registration = p.id_registration AND r.registration_maintype = 3 AND p.role = 10;";
        String q5 = "UPDATE links_cleaned.registration_c AS r, links_cleaned.person_c AS p SET r.registration_date = p.death_date WHERE r.registration_date IS NULL AND r.id_registration = p.id_registration AND r.registration_maintype = 7 AND p.role = 10;";

        conCleaned.runQuery( q1 );
        conCleaned.runQuery( q2 );
        conCleaned.runQuery( q3 );
        conCleaned.runQuery( q4 );
        conCleaned.runQuery( q5 );
        */

        // the remains are copied from doMinMaxDate()
        //minMaxValidDate();              // for all sources ? NO, per source, -> change


/**
 * @param type
 */
    /*
    public void standardFlaggedDate( String type )
    {
        // Step vars
        int counter = 0;
        int step = 10000;
        int stepstate = step;

        try {
            String startQuery;

            startQuery = "SELECT id_person , id_source , " + type + "_date FROM person_c WHERE ( ( " + type + "_date_flag = 2 ) OR ( " + type + "_date_flag = 3 ) ) AND " + type + "_date is not null";

            ResultSet rs = conCleaned.runQueryWithResult( startQuery );

            while( rs.next() )
            {
                // GUI info
                counter++;
                if (counter == stepstate) {
                    showMessage(counter + "", true, true);
                    stepstate += step;
                }

                int id_person = rs.getInt("id_person");
                int id_source = rs.getInt("id_source");
                String date = rs.getString(type + "_date");

                if (date.isEmpty()) {
                    continue;
                }

                DateYearMonthDaySet dymd = LinksSpecific.divideCheckDate( date );

                if (dymd.isValidDate()) {
                    String query = ""
                            + "UPDATE person_c "
                            + "SET person_c." + type + "_date = '" + date + "' , "
                            + "person_c." + type + "_day = " + dymd.getDay() + " , "
                            + "person_c." + type + "_month = " + dymd.getMonth() + " , "
                            + "person_c." + type + "_year = " + dymd.getYear() + " , "
                            + "person_c." + type + "_date_valid = 1 "
                            + "WHERE person_c.id_person = " + id_person;

                    conCleaned.runQuery(query);
                }
                else {

                    // EC 211
                    addToReportPerson(id_person, id_source + "", 211, dymd.getReports());

                    String query = ""
                            + "UPDATE person_c "
                            + "SET person_c." + type + "_date = '" + date + "' , "
                            + "person_c." + type + "_day = " + dymd.getDay() + " , "
                            + "person_c." + type + "_month = " + dymd.getMonth() + " , "
                            + "person_c." + type + "_year = " + dymd.getYear() + " "
                            + "WHERE person_c.id_person = " + id_person;

                    conCleaned.runQuery(query);
                }
            }
            rs = null;
        }
        catch (Exception ex) {
            showMessage(counter + " Exception while cleaning " + type + " flagged date: " + ex.getMessage(), false, true);
            ex.printStackTrace( new PrintStream( System.out ) );
        }
    } // standardFlaggedDate
    */


/**
 * @throws Exception
 */
    /*
    private void completeMinMaxBirth() throws Exception
    {
        String q1 = ""
                + " UPDATE links_cleaned.person_c, links_general.ref_date_minmax"
                + " SET"
                + " mar_day_min     = birth_day ,"
                + " mar_day_max     = birth_day ,"
                + " mar_month_min   = birth_month ,"
                + " mar_month_max   = birth_month ,"
                + " mar_date_min    = CONCAT( birth_day , '-' , birth_month , '-' ,  birth_year + min_year ) ,"
                + " mar_date_max    = CONCAT( birth_day , '-' , birth_month , '-' ,  birth_year + max_year ) ,"
                + " mar_year_min    = birth_year + min_year ,"
                + " mar_year_max    = birth_year + max_year ,"
                + " mar_date_valid = 1"
                + " WHERE"
                + " links_cleaned.person_c.role             = 1 AND"
                + " links_cleaned.person_c.birth_date_valid = 1 AND"
                + " links_general.ref_date_minmax.role      = 1 AND"
                + " links_general.ref_date_minmax.maintype  = 1 AND"
                + " links_general.ref_date_minmax.date_type = 'marriage_date'";

        String q2 = ""
                + " UPDATE links_cleaned.person_c, links_general.ref_date_minmax"
                + " SET"
                + " death_day_min   = birth_day ,"
                + " death_day_max   = birth_day ,"
                + " death_month_min = birth_month ,"
                + " death_month_max = birth_month ,"
                + " death_date_min  = CONCAT( birth_day , '-' , birth_month , '-' ,  birth_year + min_year ) ,"
                + " death_date_max  = CONCAT( birth_day , '-' , birth_month , '-' ,  birth_year + max_year ) ,"
                + " death_year_min  = birth_year + min_year ,"
                + " death_year_max  = birth_year + max_year ,"
                + " death_date_valid = 1"
                + " WHERE"
                + " links_cleaned.person_c.role             = 1 AND"
                + " links_cleaned.person_c.birth_date_valid = 1 AND"
                + " links_general.ref_date_minmax.role      = 1 AND"
                + " links_general.ref_date_minmax.maintype  = 1 AND"
                + " links_general.ref_date_minmax.date_type = 'death_date'";

        conCleaned.runQuery( q1 );
        conCleaned.runQuery( q2 );
    } // completeMinMaxBirth
    */

/**
 * @throws Exception
 */
    /*
    private void completeMinMaxMar() throws Exception
    {
        String q1 = ""
                + " UPDATE links_cleaned.person_c, links_general.ref_date_minmax"
                + " SET"
                + " birth_day_min     = mar_day ,"
                + " birth_day_max     = mar_day ,"
                + " birth_month_min   = mar_month ,"
                + " birth_month_max   = mar_month ,"
                + " birth_date_min    = CONCAT( mar_day , '-' , mar_month , '-' ,  (mar_year - age_year) + min_year ) ,"
                + " birth_date_max    = CONCAT( mar_day , '-' , mar_month , '-' ,  (mar_year - age_year) + max_year ) ,"
                + " birth_year_min    = (mar_year - age_year) + min_year ,"
                + " birth_year_max    = (mar_year - age_year) + max_year ,"
                + " birth_date_valid  = 1"
                + " WHERE"
                + " ( (links_cleaned.person_c.age_year is not null ) AND ( links_cleaned.person_c.age_year <> '') ) AND"
                + " links_cleaned.person_c.role = 4 AND"
                + " links_cleaned.person_c.mar_date_valid       = 1 AND"
                + " links_cleaned.person_c.birth_date_valid     = 0 AND"
                + " links_general.ref_date_minmax.role          = 4 AND"
                + " links_general.ref_date_minmax.maintype      = 2 AND"
                + " links_general.ref_date_minmax.age_reported  = 'y' AND"
                + " links_general.ref_date_minmax.date_type     = 'birth_date'";

        String q2 = ""
                + " UPDATE links_cleaned.person_c, links_general.ref_date_minmax"
                + " SET"
                + " birth_day_min     = mar_day ,"
                + " birth_day_max     = mar_day ,"
                + " birth_month_min   = mar_month ,"
                + " birth_month_max   = mar_month ,"
                + " birth_date_min    = CONCAT( mar_day , '-' , mar_month , '-' ,  (mar_year - age_year) + min_year ) ,"
                + " birth_date_max    = CONCAT( mar_day , '-' , mar_month , '-' ,  (mar_year - age_year) + max_year ) ,"
                + " birth_year_min    = (mar_year - age_year) + min_year ,"
                + " birth_year_max    = (mar_year - age_year) + max_year ,"
                + " birth_date_valid  = 1"
                + " WHERE"
                + " ( (links_cleaned.person_c.age_year is not null ) AND ( links_cleaned.person_c.age_year <> '') ) AND"
                + " links_cleaned.person_c.role = 7 AND"
                + " links_cleaned.person_c.mar_date_valid       = 1 AND"
                + " links_cleaned.person_c.birth_date_valid     = 0 AND"
                + " links_general.ref_date_minmax.role          = 7 AND"
                + " links_general.ref_date_minmax.maintype      = 2 AND"
                + " links_general.ref_date_minmax.age_reported  = 'y' AND"
                + " links_general.ref_date_minmax.date_type     = 'birth_date'";

        String q3 = ""
                + " UPDATE links_cleaned.person_c, links_general.ref_date_minmax"
                + " SET"
                + " death_day_min     = mar_day ,"
                + " death_day_max     = mar_day ,"
                + " death_month_min   = mar_month ,"
                + " death_month_max   = mar_month ,"
                + " death_date_min    = CONCAT( mar_day , '-' , mar_month , '-' ,  mar_year ) ,"
                + " death_date_max    = CONCAT( mar_day , '-' , mar_month , '-' ,  mar_year + ( max_year - age_year ) ) ,"
                + " death_year_min    = mar_year ,"
                + " death_year_max    = mar_year + ( max_year - age_year ) ,"
                + " death_date_valid  = 1"
                + " WHERE"
                + " ( (links_cleaned.person_c.age_year is not null ) AND ( links_cleaned.person_c.age_year <> '') ) AND"
                + " links_cleaned.person_c.role = 4 AND"
                + " links_cleaned.person_c.mar_date_valid       = 1 AND"
                + " links_general.ref_date_minmax.role          = 4 AND"
                + " links_general.ref_date_minmax.maintype      = 2 AND"
                + " links_general.ref_date_minmax.age_reported  = 'y' AND"
                + " links_general.ref_date_minmax.date_type     = 'death_date'";

        String q4 = ""
                + " UPDATE links_cleaned.person_c, links_general.ref_date_minmax"
                + " SET"
                + " death_day_min     = mar_day ,"
                + " death_day_max     = mar_day ,"
                + " death_month_min   = mar_month ,"
                + " death_month_max   = mar_month ,"
                + " death_date_min    = CONCAT( mar_day , '-' , mar_month , '-' ,  mar_year ) ,"
                + " death_date_max    = CONCAT( mar_day , '-' , mar_month , '-' ,  mar_year + ( max_year - age_year ) ) ,"
                + " death_year_min    = mar_year ,"
                + " death_year_max    = mar_year + ( max_year - age_year ) ,"
                + " death_date_valid  = 1 "
                + " WHERE"
                + " ( (links_cleaned.person_c.age_year is not null ) AND ( links_cleaned.person_c.age_year <> '') ) AND"
                + " links_cleaned.person_c.role = 7 AND"
                + " links_cleaned.person_c.mar_date_valid       = 1 AND"
                + " links_general.ref_date_minmax.role          = 7 AND"
                + " links_general.ref_date_minmax.maintype      = 2 AND"
                + " links_general.ref_date_minmax.age_reported  = 'y' AND"
                + " links_general.ref_date_minmax.date_type     = 'death_date'";

        conCleaned.runQuery( q1 );
        conCleaned.runQuery( q2 );
        conCleaned.runQuery( q3 );
        conCleaned.runQuery( q4 );
    } // completeMinMaxMar
    */

// end Obsolete Dates stuff


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



/**
 * @param source
 * @return
 * @throws Exception
 */
/*
private ArrayList< MarriageYearPersonsSet > setMarriageYear( boolean debug, String source )
        throws Exception
        {
        String query = ""
        + " SELECT "
        + " person_c.id_person ,"
        + " person_c.id_registration ,"
        + " person_c.registration_maintype ,"
        + " person_c.role ,"
        + " person_c.mar_day_min ,"
        + " person_c.mar_day_max ,"
        + " person_c.mar_month_min ,"
        + " person_c.mar_month_max ,"
        + " person_c.mar_year_min ,"
        + " person_c.mar_year_max"
        + " FROM person_c"
        + " WHERE id_source = " + source;

        if( debug ) { showMessage( "setMarriageYear() query: " + query, false , true ); }

        ResultSet minmaxjaarRs = dbconCleaned.runQueryWithResult( query );

        ArrayList< MarriageYearPersonsSet > mypsList = new ArrayList< MarriageYearPersonsSet >();

        while( minmaxjaarRs.next() )
        {
        MarriageYearPersonsSet myps = new MarriageYearPersonsSet();

        myps.setIdRegistration( minmaxjaarRs.getInt( "id_registration" ) );
        myps.setRegistrationMainType( minmaxjaarRs.getInt( "registration_maintype" ) );
        myps.setIdPerson( minmaxjaarRs.getInt( "id_person" ) );
        myps.setRole( minmaxjaarRs.getInt( "role" ) );

        myps.setMarriageDayMin(   minmaxjaarRs.getInt( "mar_day_min" ) );
        myps.setMarriageDayMax(   minmaxjaarRs.getInt( "mar_day_max" ) );
        myps.setMarriageMonthMin( minmaxjaarRs.getInt( "mar_month_min" ) );
        myps.setMarriageMonthMax( minmaxjaarRs.getInt( "mar_month_max" ) );
        myps.setMarriageYearMin(  minmaxjaarRs.getInt( "mar_year_min" ) );
        myps.setMarriageYearMax(  minmaxjaarRs.getInt( "mar_year_max" ) );

        mypsList.add( myps );
        }

        return mypsList;
        } // setMarriageYear
*/

/**
 * @param mypsList
 * @param refMinMaxMarriageYear
 * @throws Exception
 */
/*
private void minMaxMarriageYear
        (
        boolean debug,
        ArrayList< MarriageYearPersonsSet > mypsList,
        ResultSet refMinMaxMarriageYear
        )
        throws Exception
        {
        int counter = 0;
        int step = 1000;
        int stepstate = step;

        if( debug ) { showMessage( "minMaxMarriageYear() : " + mypsList.size() + " hits", false, true ); }

        // Loop through all persons
        for( int i = 0; i < mypsList.size(); i++ )
        {
        counter++;

        if( counter == stepstate ) {
        showMessage( counter + "", true, true );
        stepstate += step;
        }

        // walk through
        refMinMaxMarriageYear.beforeFirst();

        boolean role1Found = false;
        int role1 = 0;      // role1 not used ?
        int role2 = 0;

        while( refMinMaxMarriageYear.next() )
        {
        int tempRht   = refMinMaxMarriageYear.getInt( "maintype" );
        int tempRole1 = refMinMaxMarriageYear.getInt( "role1" );
        int tempRole2 = refMinMaxMarriageYear.getInt( "role2" );

        //if( debug ) { showMessage( "ref_minmax_marriageyear: maintype: " + tempRht + ", role1: " + tempRole1 + ", role2: " + tempRole2, false, true ); }

        if( ( tempRole1 == mypsList.get( i ).getRole() ) &&
        tempRht == ( mypsList.get( i ).getRegistrationMainType() ) ) {

        role1Found = true;
        role1      = tempRole1;
        role2      = tempRole2;

        break;      // role1 found
        }
        }

        // check if role 1
        if( ! role1Found )
        { showMessage( "minMaxMarriageYear() role 1 not found", false, true ); }
        else        // role1 found; search for role 2
        {
        boolean role2Found = false;
        int role2Id = 0;

        int role2MarYearMin  = 0;
        int role2MarYearMax  = 0;
        int role2MarMonthMin = 0;
        int role2MarMonthMax = 0;
        int role2MarDayMin   = 0;
        int role2MarDayMax   = 0;

        // walk trough all persons of registration
        for( int j = (((i - 7) > 0) ? i - 7 : 0); j < ((i + 7) > mypsList.size() ? mypsList.size() : i + 7); j++ )
        {
        if( (role2 == mypsList.get( j ).getRole()) &&
        (mypsList.get( i ).getIdRegistration() == mypsList.get( j ).getIdRegistration()) )
        {
        // Role 2 found
        role2Found       = true;
        role2Id          = mypsList.get( j ).getIdPerson();
        role2MarYearMin  = mypsList.get( j ).getMarriageYearMin();
        role2MarYearMax  = mypsList.get( j ).getMarriageYearMax();
        role2MarMonthMin = mypsList.get( j ).getMarriageMonthMin();
        role2MarMonthMax = mypsList.get( j ).getMarriageMonthMax();
        role2MarDayMin   = mypsList.get( j ).getMarriageDayMin();
        role2MarDayMax   = mypsList.get( j ).getMarriageDayMax();

        break;
        }
        }

        // check is role 2
        if( ! role2Found )
        { showMessage( "minMaxMarriageYear() role 2 not found", false, true ); }
        else
        {
        int role1Id          = mypsList.get( i ).getIdPerson();
        int role1MarYearMax  = mypsList.get( i ).getMarriageYearMax();
        int role1MarYearMin  = mypsList.get( i ).getMarriageYearMin();
        int role1MarMonthMax = mypsList.get( i ).getMarriageMonthMax();
        int role1MarMonthMin = mypsList.get( i ).getMarriageMonthMin();
        int role1MarDayMax   = mypsList.get( i ).getMarriageDayMax();
        int role1MarDayMin   = mypsList.get( i ).getMarriageDayMin();

        // role1 min > role2 min
        if( dateLeftIsGreater( role1MarYearMin, role1MarMonthMin, role1MarDayMin, role2MarYearMin, role2MarMonthMin, role2MarDayMin ) )
        {
        String query = ""
        + " UPDATE person_c"
        + " SET"
        + " mar_year_min = "  + mypsList.get( i ).getMarriageYearMin() + ","
        + " mar_month_min = " + mypsList.get( i ).getMarriageMonthMin() + ","
        + " mar_day_min = "   + mypsList.get( i ).getMarriageDayMin()
        + " WHERE"
        + " id_person = " + role2Id;

        if( debug ) { showMessage( "role1 min > role2 min: " + query, false, true ); }
        dbconCleaned.runQuery( query );
        }

        // role2 max > role1 max
        if( dateLeftIsGreater( role2MarYearMax, role2MarMonthMax, role2MarDayMax, role1MarYearMax, role1MarMonthMax, role1MarDayMax ) )
        {
        String query = ""
        + " UPDATE person_c"
        + " SET"
        + " mar_year_max = "  + mypsList.get( i ).getMarriageYearMax() + ","
        + " mar_month_max = " + mypsList.get( i ).getMarriageMonthMax() + ","
        + " mar_day_max = "   + mypsList.get( i ).getMarriageDayMax()
        + " WHERE"
        + " id_person = " + role2Id;

        if( debug ) { showMessage( "role2 max > role1 max: " + query, false, true ); }
        dbconCleaned.runQuery( query );
        }

        // role2 min > role1 min
        if( dateLeftIsGreater( role2MarYearMin, role2MarMonthMin, role2MarDayMin, role1MarYearMin, role1MarMonthMin, role1MarDayMin ) )
        {
        // Query
        String query = "UPDATE person_c"
        + " SET"
        + " mar_year_min = "  + role2MarYearMin + ","
        + " mar_month_min = " + role2MarMonthMin + ","
        + " mar_day_min = "   + role2MarDayMin
        + " WHERE"
        + " id_person = " + role1Id;

        if( debug ) { showMessage( "role2 min > role1 min: " + query, false, true ); }
        dbconCleaned.runQuery( query );
        }

        // role1 max > role2 max
        if( dateLeftIsGreater( role1MarYearMax, role1MarMonthMax, role1MarDayMax, role2MarYearMax, role2MarMonthMax, role2MarDayMax ) )
        {
        // Query
        String query = "UPDATE person_c"
        + " SET"
        + " mar_year_max = "  + role2MarYearMax + ","
        + " mar_month_max = " + role2MarMonthMax + ","
        + " mar_day_max = "   + role2MarDayMax
        + " WHERE"
        + " id_person = " + role1Id;

        if( debug ) { showMessage( "role1 max > role2 max: " + query, false, true ); }
        dbconCleaned.runQuery( query );
        }
        }
        }
        }
        } // minMaxMarriageYear
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

