package linksmatchmanager.DataSet;

import java.util.Comparator;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author Fons Laan
 *
 * FL-05-Nov-2015 Created
 * FL-20-Jan-2016 keySet function not finished
 *
 * Needed Map to store name frequensies: -> HashMap
 * But must be thread save: -> ConcurrentHashMap
 * And want to sort by values (not keys, so not a ConcurrentSkipListMap): -> this OrderedValueHashMap
 *
 * See: http://stackoverflow.com/questions/10592002/sorting-a-concurrent-hash-map-by-value
 * The above solution is not optimal if you are calling the methods keySet/entrySet very often as it
 * sorts entries on each call. You may want to cache these results to avoid re-computation while the
 * internal state of the map has not changed.
 */

public class OrderedValueHashMap< K,V extends Comparable< V > > extends ConcurrentHashMap< K, V >
{
    @Override
    public Set< Map.Entry< K, V > > entrySet()
    {
        Set< Map.Entry< K, V > > orderedValueEntrySet = new TreeSet< Map.Entry< K, V > > (
            new Comparator< Map.Entry< K, V > >()
        {
            @Override
            public int compare( java.util.Map.Entry< K, V > o1, java.util.Map.Entry< K, V > o2 )
            { return o1.getValue().compareTo( o2.getValue() ); }
        }
        );

        orderedValueEntrySet.addAll( super.entrySet() );

        return orderedValueEntrySet;
    }
    /*
    @Override
    public Set< K > keySet()
    {
        Set< K > orderedKeySet = new LinkedHashSet< K >();

        for ( Map.Entry< K, V > e : entrySet() )
        { orderedKeySet.add( e.getKey() ); }

        return orderedKeySet;
    }
    */
}
