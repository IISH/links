package dataset;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.DateTimeParseException;

/**
 * @author Omar Azouguagh
 * @author Fons Laan
 *
 * FL-20-Nov-2018 new function validateDateString()
 * FL-26-Nov-2018 Latest change
 */
public final class DateYearMonthDaySet
{
    boolean debug = false;

    private boolean dateIsValid;

    private int year;
    private int month;
    private int day;

    private String reportsDate  = "";

    private String reportsYear  = "";
    private String reportsMonth = "";
    private String reportsDay   = "";


    /**
     * @param dateString
     */
    public boolean validateDateString( String dateString )
    {
        boolean valid = false;

        // parser/formatter for day-month-year
        DateTimeFormatter dd_MM_yyyy = DateTimeFormatter.ofPattern( "dd-MM-yyyy" ); // default

        // parser for multiple patterns
        // TODO Create static parser once finished: it does not change dynamically
        DateTimeFormatter parser = new DateTimeFormatterBuilder()
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

        // parse input string
        try {
            LocalDate ld = LocalDate.parse( dateString, parser );
            valid = true;
            if( debug ) { System.out.println( "validateDateString(): " + dd_MM_yyyy.format( ld ) ); }
        }
        catch( DateTimeParseException ex )
        {
            valid = false;
            if( debug ) { System.out.println( "validateDateString(): " + ex.getMessage() ); }
        }

        return valid;
    } // validateDateString


    /**
     *
     * @param reportsDate
     */
    public DateYearMonthDaySet( String reportsDate )
    {
        this.reportsDate = reportsDate;

        //this.dateIsValid = true;      // Omar
        //this.dateIsValid = false;     // Fons

        this.dateIsValid = validateDateString( reportsDate );
    }

    /**
     *
     * @return
     */
    public boolean isValidDate() {
        return this.dateIsValid;
    }

    /**
     *
     * @param valid
     */
    public void setValidDate( boolean valid )
    {
        this.dateIsValid = valid;
    }


    /**
     *
     * @param value
     */
    public void setReportYear( String value )
    {
        this.reportsYear = value;
        this.dateIsValid = false;
    }


    /**
     *
     * @param value
     */
    public void setReportMonth( String value )
    {
        this.reportsMonth = value;
        this.dateIsValid  = false;
    }


    /**
     *
     * @param value
     */
    public void setReportDay( String value )
    {
        this.reportsDay  = value;
        this.dateIsValid = false;
    }


    /**
     *
     * @param value
     */
    public void setReportDate( String value ) {
        this.reportsDate  = value;
    }


    /**
     *
     * @param value
     */
    public void setYear( int value ) {
        this.year = value;
    }


    /**
     *
     * @param value
     */
    public void setMonth( int value ){
        this.month = value;
    }


    /**
     *
     * @param value
     */
    public void setDay( int value ){
        this.day = value;
    }


    /**
     *
     * @return
     */
    public String getReportYear( ){
        return reportsYear;
    }


    /**
     *
     * @return
     */
    public String getReportMonth( ){
        return reportsMonth;
    }


    /**
     *
     * @return
     */
    public String getReportDay( ){
        return reportsDay;
    }


    /**
     *
     * @return
     */
    public String getReports( )
    {
        if( reportsDate == null || reportsDate.isEmpty() )
        { return "[ Day: " + reportsDay + " ; Month: " + reportsMonth + " ; Year: " + reportsYear + " ]"; }
        else
        { return "[ Date: " + reportsDate + " ]"; }
    }


    /**
     *
     * @return
     */
    public int getYear( ){
        return year;
    }


    /**
     *
     * @return
     */
    public int getMonth( ){
        return month;
    }


    /**
     *
     * @return
     */
    public int getDay( ){
        return day;
    }
}

