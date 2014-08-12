/**
 * DataSet
 */
package dataset;

/**
 * DevinedMinMaxDatum dataset
 * Used to return a set of data
 * @author oaz
 */
public final class DevinedMinMaxDatumSet {

    // Private vars
    private int MinDay ;
    private int MinMonth ;
    private int MinYear ;

    private int MaxDay ;
    private int MaxMonth ;
    private int MaxYear ;



    /**
     * Use this method to get MinDay
     * @return MinDay
     */
    public int getMinDay( ){

        return this.MinDay ;

    }



    /**
     * Use this method to get MinMonth
     * @return MinMonth
     */
    public int getMinMonth( ){

        return this.MinMonth ;

    }



    /**
     * Use this method to get MinYear
     * @return MinYear
     */
    public int getMinYear( ){

        return this.MinYear ;

    }



    /**
     * Use this method to get MaxDay
     * @return MaxDay
     */
    public int getMaxDay( ){

        return this.MaxDay ;

    }



    /**
     * Use this method to get MaxMonth
     * @return MaxMonth
     */
    public int getMaxMonth( ){

        return this.MaxMonth ;

    }



    /**
     * Use this method to get MaxYear
     * @return MaxYear
     */
    public int getMaxYear( ){

        return this.MaxYear ;

    }



    /**
     * Use this method to set MinDay
     * @param value value to set MinDay
     */
    public void setMinDay( int value ){

        this.MinDay = value ;

    }



    /**
     * Use this method to set MinMonth
     * @param value value to set MinMonth
     */
    public void setMinMonth( int value ){

        this.MinMonth = value ;

    }



    /**
     * Use this method to set MinYear
     * @param value value to set MinYear
     */
    public void setMinYear( int value ){

        this.MinYear = value ;

    }



    /**
     * Use this method to set MaxDay
     * @param value value to set MaxDay
     */
    public void setMaxDay( int value ){

        this.MaxDay = value ;

    }



    /**
     * Use this method to set MaxMonth
     * @param value value to set MaxMonth
     */
    public void setMaxMonth( int value ){

        this.MaxMonth = value ;

    }



    /**
     * Use this method to set MaxYear
     * @param value value to set MaxYear
     */
    public void setMaxYear( int value ){

        this.MaxYear = value ;

    }

}