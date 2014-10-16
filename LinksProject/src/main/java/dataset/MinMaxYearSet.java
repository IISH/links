package dataset;

/**
 * MinMaxJaar dataset
 * Used to return a min and an max Year
 *
 * @author Omar Azouguagh
 * @author Fons Laan
 *
 * FL-16-Oct-2014 Latest change
 */
public class MinMaxYearSet {

    // Private vars
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
