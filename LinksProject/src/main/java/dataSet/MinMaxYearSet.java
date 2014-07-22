package dataSet;

/**
 * MinMaxJaar dataset
 * Used to return a min and an max Jaar
 * @author oaz
 */
public class MinMaxYearSet {

    // Private vars
    private int MinYear ;
    private int MaxYear ;



    /**
     * Use this method to get MinYear
     * @return MinYear
     */
    public int GetMinYear( ){

        // Return var
        return this.MinYear ;

    }



    /**
     * Use this method to get MaxYear
     * @return MaxYear
     */
    public int GetMaxYear( ){

        // Return var
        return this.MaxYear ;

    }



    /**
     * Use this method to set MinYear
     * @param value value to set MinYear
     */
    public void SetMinYear( int value ){

        // Set var with value
        this.MinYear = value ;

    }



    /**
     * Use this method to set MaxYear
     * @param value value to set MaxYear
     */
    public void SetMaxYear( int value ){

        // Set var with value
        this.MaxYear = value ;

    }

}