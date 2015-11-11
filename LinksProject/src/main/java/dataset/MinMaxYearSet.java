package dataset;

/**
 * MinMaxYear class
 * Used to return a min and and max Year
 *
 * @author Omar Azouguagh
 * @author Fons Laan
 *
 * FL-10-Nov-2015 Latest change
 */
public class MinMaxYearSet
{
    private int minYear ;
    private int maxYear ;

    /**
     * @return minYear
     */
    public int getMinYear( ) { return this.minYear ; }


    /**
     * @return maxYear
     */
    public int getMaxYear( ) { return this.maxYear ; }


    /**
     * @param minYear
     */
    public void setMinYear( int minYear ) { this.minYear = minYear ; }


    /**
     * @param maxYear
     */
    public void setMaxYear( int maxYear ) { this.maxYear = maxYear ; }
}
