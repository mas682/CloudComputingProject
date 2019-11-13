
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.util.GenericOptionsParser;



public class SearchTerm {

  public static class SearchTermMapper
       extends Mapper<Object, Text, NullWritable, Text>{
	
	private String searchTerm = "";

    @Override
    public void setup(Context context) throws IOException,
        InterruptedException {
      Configuration conf = context.getConfiguration();
      String [] searchTerms;
      searchTerms= conf.getStrings("SearchTerm.term");
      if(searchTerms.length > 0)
      {
    	  searchTerm = searchTerms[0];
      }
      else
      {
    	  searchTerm = "";
      }
    }

    @Override
    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      StringTokenizer itr = new StringTokenizer(value.toString(), " \t");
      String token = "";
      String term = "";
      while (itr.hasMoreTokens()) {
    	token = itr.nextToken();
    	term = token;
    	System.out.println("TERM: " + token);
    	break;
      }
      if(term.contentEquals(searchTerm))
      {
    	  context.write(NullWritable.get(), value);
      }
    }
  }
 

  public static void main(String[] args) throws Exception {
	long startTime = System.currentTimeMillis();
	long endTime = 0;
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "SearchTerm");
    job.setJarByClass(SearchTerm.class);
    job.setMapperClass(SearchTermMapper.class);
    //job.setReducerClass(TopNReducer.class);
    job.setMapOutputKeyClass(NullWritable.class);
    job.setMapOutputValueClass(Text.class);
    job.setNumReduceTasks(0);
    job.setOutputKeyClass(NullWritable.class);
    job.setOutputValueClass(Text.class);
    GenericOptionsParser optionParser = new GenericOptionsParser(conf, args);
    String[] remainingArgs = optionParser.getRemainingArgs();
    List<String> otherArgs = new ArrayList<String>();
	for (int i=0; i < remainingArgs.length; ++i) 
	{
		if("-T".contentEquals(remainingArgs[i])) 
		{
			job.getConfiguration().setStrings("SearchTerm.term", remainingArgs[i+1]);
		}
        otherArgs.add(remainingArgs[i]);
    }
    FileInputFormat.addInputPath(job, new Path(otherArgs.get(0)));
    FileOutputFormat.setOutputPath(job, new Path(otherArgs.get(1)));
    if(job.waitForCompletion(true))
    {
    	endTime = System.currentTimeMillis();
    	System.out.println("TOTAL EXECUTION TIME: " + ((endTime - startTime)/1000.0) + " seconds"); // in milliseconds
    	System.exit(0);
    }
    else {
    	System.exit(1);
    }
  }
}