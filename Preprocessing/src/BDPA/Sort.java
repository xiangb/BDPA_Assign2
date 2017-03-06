package BDPA;

import java.util.*;


public class Sort {
	
	
	public static <K, V extends Comparable<? super V>> LinkedHashSet<String> 
    sortByValue( Map<K, V> map )
{
    List<Map.Entry<K, V>> list = new LinkedList<>( map.entrySet() );
    
    Collections.sort( list, new Comparator<Map.Entry<K, V>>()
    {
        @Override
        public int compare( Map.Entry<K, V> o1, Map.Entry<K, V> o2 )
        {
            return o1.getValue().compareTo(o2.getValue());
        }
    } );
    
    

    LinkedHashSet<String> result = new LinkedHashSet<String>();

    for (Map.Entry<K, V> entry : list)
    {
        result.add(entry.getKey().toString());
    }
    
    return result;
}
}
