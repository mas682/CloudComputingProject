import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class DocTermPartitioner
extends Partitioner<DocTermPair, IntWritable> {

	@Override
	public int getPartition(DocTermPair pair, IntWritable freq, int numberOfPartitions)
	{
		return Math.abs(pair.getTerm().hashCode() % numberOfPartitions);
	}
}
