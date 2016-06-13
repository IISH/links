package linksmatchmanager.DataSet;

/**
 * @author Fons Laan
 *
 * An NameLvsVariants object takes a name (firstname or familyname), plus the used Levenshtein table.
 * The Levenshtein variants of the name are collected, plus a bunch bookkeeping variables for matching.
 *
 * FL-09-Jun-2016 Created
 * FL-09-Jun-2016 Latest change
 */
public class NameLvsVariants
{
    private boolean debug = true;

    private long creatorId;
    private long userId;

    private int name_int;
    private String name_str;

    private String lvs_table;
    private int lvs_dist;



    /**
     * Constructor to collect the Levenshtein variants of the supplied name
     *
     */
    public NameLvsVariants()
    throws Exception
    {
        long creatorId = Thread.currentThread().getId();

        if( debug ) { System.out.println( String.format( "Thread id %02d; NameLvsVariants", creatorId ) ); }


    } // NameLvsVariants


    /**
     * @param threadId
     * @param name_int
     * @param name_str
     * @param lvs_table
     * @param lvs_dist
     */
    public void init
    (
        long threadId,
        int name_int,
        String name_str,
        String lvs_table,
        int lvs_dist
    )
    {
        // new parameters?
        if( ! ( this.name_int == name_int && this.lvs_table.equals( lvs_table ) ) )
        { reset(); }

        this.userId    = threadId;
        this.name_int  = name_int;
        this.name_str  = name_str;
        this.lvs_table = lvs_table;
        this.lvs_dist  = lvs_dist;

        String msg = String.format( "threadId: %02d; NameLvsVariants/init: name_int: %d, name_str: %s, lvs_table: %s, lvs_dist: %d",
            threadId, name_int, name_str, lvs_table, lvs_dist );
        System.out.println( msg );

        // reset buffers
        // ...

    } // init


    /**
      * reset buffers
      */
    public void reset()
    {
        String msg = String.format( "threadId: %02d; NameLvsVariants/reset", userId );
        System.out.println( msg );
    }


     /**
      * free buffers
      */
    public void free()
    {
        String msg = String.format( "threadId: %02d; NameLvsVariants/reset", userId );
        System.out.println( msg );
    }
}
