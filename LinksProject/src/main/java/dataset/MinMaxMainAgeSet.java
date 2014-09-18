package dataset;

/**
 * @author Fons Laan
 *
 * FL-18-Sep-2014 Latest change
 */
public class MinMaxMainAgeSet {
    private String function;
    private String min_age_0;
    private String max_age_0;
    private int age;
    private int min_year;
    private int max_year;

    public void MinMaxMainAgeSet() {
        function  = "";
        min_age_0 = "";
        max_age_0 = "";
        age  = 0;
        min_year = 0;
        max_year = 0;
    }

    public String getFunction() {  return this.function; }

    public void setFunction( String function ) { this.function = function; }


    public String getMinAge0() { return this.min_age_0; };

    public void setMinAge0( String min_age_0 ) { this.min_age_0 = min_age_0; }


    public String getMaxAge0() {  return this.max_age_0; }

    public void setMaxAge0( String max_age_0 ) { this.max_age_0 = max_age_0; }


    public int getAgeYear() { return this.age; }

    public void setAgeYear( int age ) { this.age = age; }


    public int getMinYear() { return this.min_year; }

    public void setMinYear( int min_year ) { this.min_year = min_year; }


    public int getMaxYear() { return this.max_year; }

    public void setMaxYear( int max_year ) { this.max_year = max_year; }
}
