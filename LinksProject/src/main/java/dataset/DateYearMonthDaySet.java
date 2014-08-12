package dataset;

/**
 *
 * @author oaz
 */
public final class DateYearMonthDaySet {

    /**
     *
     */
    private int year;
    private int month;
    private int day;



    /**
     *
     */
    private boolean dateIsValid = true;

    
    
    /**
     *
     */
    private String reportsYear = "";
    private String reportsMonth = "";
    private String reportsDay = "";



    /**
     *
     * @return
     */
    public boolean isValidDate( ) {
        return this.dateIsValid;
    }



    /**
     *
     * @param meldingText
     */
    public void setReportYear( String value ) {
        this.reportsYear = value;
        this.dateIsValid = false;
    }



    /**
     *
     * @param meldingText
     */
    public void setReportMonth( String value ) {
        this.reportsMonth = value;
        this.dateIsValid = false;
    }



    /**
     *
     * @param meldingText
     */
    public void setReportDay( String value ) {
        this.reportsDay = value;
        this.dateIsValid = false;
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
    public String getReports( ){
        return "[ DAG: " + reportsDay + " ; MAAND: " + reportsMonth + " ; JAAR: " + reportsYear + " ]";
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