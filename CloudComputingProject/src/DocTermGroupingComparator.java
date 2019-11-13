import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class DocTermGroupingComparator 
	extends WritableComparator {
	
	public DocTermGroupingComparator() {
		super(DocTermPair.class, true);
	}

	@Override
	public int compare(WritableComparable wc1, WritableComparable wc2)
	{
		DocTermPair pair = (DocTermPair)wc1;
		DocTermPair pair2 = (DocTermPair)wc2;
		int value = 0;
		return pair.getTerm().toString().compareTo(pair2.getTerm().toString());
	}
}
