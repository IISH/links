/**
 * DataSet
 */
package dataset;

/**
 * @author Omar Azouguagh
 * @author Fons Laan
 *
 * FL-16-Sep-2014 Latest change
 */
public final class DivideMinMaxDatumSet
{
    private int MinDay   = 0;
    private int MinMonth = 0;
    private int MinYear  = 0;

    private int MaxDay   = 0;
    private int MaxMonth = 0;
    private int MaxYear  = 0;

    public void nonnegative() {
        if( this.MinYear  < 0 ) { this.MinYear  = 0; }
        if( this.MinMonth < 0 ) { this.MinMonth = 0; }
        if( this.MinDay   < 0 ) { this.MinDay   = 0; }
        if( this.MaxYear  < 0 ) { this.MaxYear  = 0; }
        if( this.MaxMonth < 0 ) { this.MaxMonth = 0; }
        if( this.MaxDay   < 0 ) { this.MaxDay   = 0; }
    }

    public String getMinDate()  { return String.format ( "%04d.%02d.%02d", this.MinYear, this.MinMonth, this.MinDay ); }

    public String getMaxDate()  { return String.format ( "%04d.%02d.%02d", this.MaxYear, this.MaxMonth, this.MaxDay ); }

    public String getMinMaxDate()  { return "min/max date: " + getMinDate() + "-" + getMaxDate(); }


    public int getMinDay()   { return this.MinDay; }

    public int getMinMonth() { return this.MinMonth; }

    public int getMinYear()  { return this.MinYear; }

    public int getMaxDay()   { return this.MaxDay; }

    public int getMaxMonth() { return this.MaxMonth; }

    public int getMaxYear()  { return this.MaxYear; }


    public void setMinDay(   int value ) { this.MinDay   = value; }

    public void setMinMonth( int value ) { this.MinMonth = value; }

    public void setMinYear(  int value ) { this.MinYear  = value; }

    public void setMaxDay(   int value ) { this.MaxDay   = value; }

    public void setMaxMonth( int value ) { this.MaxMonth = value; }

    public void setMaxYear(  int value ) { this.MaxYear  = value; }

}
