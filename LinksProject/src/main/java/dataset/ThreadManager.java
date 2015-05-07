package dataset;

/**
 * @author Fons Laan
 *
 * FL-07-May-2015 Latest change
 */
public class ThreadManager
{
    int threadCount;
    int maxCount;

    /**
     *
     */
    public ThreadManager() {
        threadCount = 0;
        this.maxCount = maxCount;
    }

    /**
     *
     * @param maxCount
     */
    public ThreadManager( int maxCount ) {
        threadCount = 0;
        this.maxCount = maxCount;
    }

    /**
     *
     * @return
     */
    public boolean allowNewThread() {

        if( threadCount < maxCount ) {
            return true;
        }

        return false;
    }

    /**
     *
     * @return
     */
    public int addThread() {
        threadCount++;
        return threadCount;
    }

    /**
     *
     * @return
     */
    public int removeThread() {
        threadCount--;
        return threadCount;
    }

    /**
     *
     * @return
     */
    public int getThreadCount() {
        return threadCount;
    }
}
