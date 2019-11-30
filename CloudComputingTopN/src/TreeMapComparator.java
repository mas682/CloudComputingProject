import java.util.Comparator;

// this class is used for the sorted map to get the right
// order of the values

public class TreeMapComparator implements Comparator<Integer> {

	public int compare(Integer a, Integer b)
	{	
		return a.compareTo(b);
	}
}
