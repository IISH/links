package dataset;

/**
 * @author Fons Laan
 *
 * FL-16-Sep-2014 Latest change
 */
public class MinMaxMainAgeSet {
    private int age_year;
    private int main_role;
    private String min_age_0;
    private String max_age_0;


    public int getAgeYear() { return this.age_year; }

    public void setAgeYear( int age_year ) { this.age_year = age_year; }


    public int getMainRole() {  return this.main_role; }

    public void setMainRole( int main_role ) { this.main_role = main_role; }


    public String getMinAge0() { return this.min_age_0; };

    public void setMinAge0( String min_age_0 ) { this.min_age_0 = min_age_0; }


    public String getMaxAge0() {  return this.max_age_0; }

    public void setMaxAge0( String max_age_0 ) { this.max_age_0 = max_age_0; }

}
