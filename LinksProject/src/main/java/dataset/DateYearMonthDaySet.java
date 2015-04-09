package dataset;

/**
 * @author Omar Azouguagh
 * @author Fons Laan
 *
 * FL-09-Apr-2015 Latest change
 */
public final class DateYearMonthDaySet
{
    private int year;
    private int month;
    private int day;

    private boolean dateIsValid = true;

    private String reportsYear  = "";
    private String reportsMonth = "";
    private String reportsDay   = "";

    private String reportsDate  = "";


    public DateYearMonthDaySet( String reportsDate )
    {
        this.reportsDate = reportsDate;
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
     * @param value
     */
    public void setReportYear( String value ) {
        this.reportsYear = value;
        this.dateIsValid = false;
    }


    /**
     *
     * @param value
     */
    public void setReportMonth( String value ) {
        this.reportsMonth = value;
        this.dateIsValid  = false;
    }


    /**
     *
     * @param value
     */
    public void setReportDay( String value ) {
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
    public String getReports( ) {
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

