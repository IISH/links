package dataset;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.DateTimeParseException;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author Omar Azouguagh
 * @author Fons Laan
 *
 * FL-20-Nov-2018 new function validateDateString()
 * FL-04-Dec-2018 Latest change
 */
public final class DateYearMonthDaySet
{
    boolean debug = false;

    private boolean dateIsValid       = false;
    private boolean dateIsFixed       = false;      // fixed to valid from invalid input string
    private boolean dateIsReformatted = false;      // input string was not in default format

    private String localDateStr = "";

    private int year;
    private int month;
    private int day;

    private String reportsDate  = "";
    private String reportsYear  = "";
    private String reportsMonth = "";
    private String reportsDay   = "";

    // default formatter for day-month-year
    static DateTimeFormatter dd_MM_yyyy = DateTimeFormatter.ofPattern( "dd-MM-yyyy" );

    // parser for multiple patterns
    static DateTimeFormatter parser = new DateTimeFormatterBuilder()
        .appendOptional( dd_MM_yyyy )     // optional "dd-MM-yyyy"
        .appendOptional( DateTimeFormatter.ofPattern( "d-MM-yyyy" ) )
        .appendOptional( DateTimeFormatter.ofPattern( "dd-M-yyyy" ) )
        .appendOptional( DateTimeFormatter.ofPattern( "d-M-yyyy" ) )

        .appendOptional( DateTimeFormatter.ofPattern( "dd/MM/yyyy" ) )
        .appendOptional( DateTimeFormatter.ofPattern( "d/MM/yyyy" ) )
        .appendOptional( DateTimeFormatter.ofPattern( "dd/M/yyyy" ) )
        .appendOptional( DateTimeFormatter.ofPattern( "d/M/yyyy" ) )

        .appendOptional( DateTimeFormatter.ofPattern( "yyyy-MM-dd" ) )
        .appendOptional( DateTimeFormatter.ofPattern( "yyyy-MM-d" ) )
        .appendOptional( DateTimeFormatter.ofPattern( "yyyy-M-dd" ) )
        .appendOptional( DateTimeFormatter.ofPattern( "yyyy-M-d" ) )

        .appendOptional( DateTimeFormatter.ofPattern( "yyyy/MM/dd" ) )
        .appendOptional( DateTimeFormatter.ofPattern( "yyyy/MM/d" ) )
        .appendOptional( DateTimeFormatter.ofPattern( "yyyy/M/dd" ) )
        .appendOptional( DateTimeFormatter.ofPattern( "yyyy/M/d" ) )

        //.appendOptional( DateTimeFormatter.ISO_LOCAL_DATE )      // optional built-in formatter
        .toFormatter();                 // create formatter


    /**
     * @param reportsDate
     */
    public DateYearMonthDaySet( String reportsDate )
    {
        this.reportsDate = reportsDate;
        //this.dateIsValid = true;      // Omar

        dateIsValid = validateDateString( reportsDate );

        // try to replace with its numeric pieces, ignoring junk
        if( ! dateIsValid )
        {
            dateIsValid = validateDatePieces( reportsDate );
        }
    }


    /**
     * @param dateString
     */
    public boolean validateDateString( String dateString )
    {
        // could add a length condition based on parser when dateString not null
        if( dateString == null ) { dateIsValid = false; }
        else    // parse input string
        {
            try
            {
                LocalDate ld = LocalDate.parse( dateString, parser );
                localDateStr = dd_MM_yyyy.format( ld );
                dateIsValid = true;
                if( debug ) { System.out.println( "validateDateString(): " + localDateStr ); }

                // Notice that LocalDate.parse() may silently ignore leading 0's which "officially" fall outside the
                // specified format patterns! => compare input and parsed strings.
                // Notice that if a valid input string has a different format that the default format, we also mark it
                // as "fixed" so that the default format is used for links_cleaned.
                if( dateString.equals( localDateStr ) ) { dateIsReformatted = false; }
                else { dateIsReformatted = true; }
            }
            catch( DateTimeParseException ex )
            {
                dateIsValid = false;
                dateIsReformatted = false;
                if( debug ) { System.out.println( "validateDateString(): " + ex.getMessage() ); }
            }
        }

        return dateIsValid;
    } // validateDateString


    /**
     * @param dateString
     */
    private boolean validateDatePieces( String dateString )
    {
        // FL-27-Nov-2018 hack: try to replace invalid date string with its numeric pieces, if any.
        if( dateString == null )
        {
            dateIsValid = false;
            dateIsFixed = false;
        }
        else
        {
            Pattern regex = Pattern.compile( "[0-9]+" );
            Matcher m = regex.matcher( dateString );

            int day   = 0;
            int month = 0;
            int year  = 0;

            if( m.find() ) { day   = Integer.parseInt( m.group() ); }   // day
            if( m.find() ) { month = Integer.parseInt( m.group() ); }   // month
            if( m.find() ) { year  = Integer.parseInt( m.group() ); }   // year

            String dateReplace = String.format( "%d-%d-%d", day, month, year );
            try
            {
                LocalDate ld = LocalDate.parse( dateReplace, parser );
                dateIsValid = true;
                dateIsFixed = true;
                localDateStr = dd_MM_yyyy.format( ld );
                if( debug ) { System.out.println( "validateDatePieces(): " + localDateStr ); }
            }
            catch( DateTimeParseException ex )
            {
                dateIsValid = false;
                dateIsFixed = false;
                if( debug ) { System.out.println( "validateDatePieces(): " + ex.getMessage() ); }
            }
        }

        return dateIsValid;
    } // validateDatePieces


    /**
     * @return
     */
    public boolean isValidDate()
    { return this.dateIsValid; }

    /**
     * @param valid
     */
    public void setValidDate( boolean valid )
    { this.dateIsValid = valid; }


    /**
     * @return
     */
    public boolean isFixedDate()
    { return this.dateIsFixed; }


    /**
     * @return
     */
    public boolean isReformattedDate()
    { return this.dateIsReformatted; }


    /**
     * @param value
     */
    public void setReportYear( String value )
    {
        this.reportsYear = value;
        this.dateIsValid = false;
    }


    /**
     * @param value
     */
    public void setReportMonth( String value )
    {
        this.reportsMonth = value;
        this.dateIsValid  = false;
    }


    /**
     * @param value
     */
    public void setReportDay( String value )
    {
        this.reportsDay  = value;
        this.dateIsValid = false;
    }


    /**
     * @param value
     */
    public void setReportDate( String value ) {
        this.reportsDate  = value;
    }


    /**
     * @param value
     */
    public void setYear( int value ) {
        this.year = value;
    }


    /**
     * @param value
     */
    public void setMonth( int value ){
        this.month = value;
    }


    /**
     * @param value
     */
    public void setDay( int value ){
        this.day = value;
    }


    /**
     *
     * @return
     */
    public String getReportYear(){
        return reportsYear;
    }


    /**
     * @return
     */
    public String getReportMonth(){
        return reportsMonth;
    }


    /**
     * @return
     */
    public String getReportDay(){
        return reportsDay;
    }


    /**
     * @return
     */
    public String getReports()
    {
        //  getReports() used for addToReportRegistration()
        if( reportsDate == null || reportsDate.isEmpty() )
        { return "[ Day: " + reportsDay + " ; Month: " + reportsMonth + " ; Year: " + reportsYear + " ]"; }
        else
        { return "[ Date: " + reportsDate + " ]"; }
    }

    /**
     * @return
     */
    public String getDate() { return localDateStr; }

    /**
     * @return
     */
    public int getYear() { return year; }

    /**
     * @return
     */
    public int getMonth() { return month; }

    /**
     * @return
     */
    public int getDay() { return day; }
}

