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

