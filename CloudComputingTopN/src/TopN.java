import java.util.TreeMap;
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
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.util.GenericOptionsParser;



public class TopN {

  public static class TopNMapper
       extends Mapper<Object, Text, NullWritable, Text>{
	
	private TreeMap<Integer, Text> topNwords = new TreeMap<Integer, Text>(new TreeMapComparator());
	private int N = 10;

    @Override
    public void setup(Context context) throws IOException,
        InterruptedException {
      Configuration conf = context.getConfiguration();
      N = conf.getInt("TopN.terms", 10);
    }

    @Override
    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      StringTokenizer itr = new StringTokenizer(value.toString(), " \t");
      String token = "";
      String term = "";
      int val = 0;
      int counter = 0;
      int freq = 0;
      while (itr.hasMoreTokens()) {
    	token = itr.nextToken();
    	if(counter == 0)
    	{
    		term = token;
    		System.out.println("TERM: " + token);
    	}
    	else if(counter%2 == 0)
    	{
    		freq = Integer.parseInt(token);
    		val += freq;
    	}
    	counter++;
      }
      // then need to add the counter to the sorted map?
      System.out.println("VALUE: " + val);
      Text tempVal = new Text(term + " " + val);
      value.set(term + " " + val);
      topNwords.put(val, tempVal);
      System.out.println(tempVal.toString());
      if(topNwords.size() > N)
      {
    	  // remove the element with the smalles frequency
    	  topNwords.remove(topNwords.firstKey());
      }
    }
    
    /**
     * cleanup() function will be executed once at the end of each mapper
     * Here we set up the "words top N list" as topNwords
     */
    @Override
    public void cleanup(Context context) throws IOException, InterruptedException
    {
    	for(Text term: topNwords.values() ) {
    		context.write(NullWritable.get(), term);
    	}
    }
  }
 
  public static class TopNReducer
       extends Reducer<NullWritable,Text,NullWritable,Text> {
	private int N = 10;

	@Override
	public void setup(Context context) throws IOException,
     InterruptedException {
		Configuration conf = context.getConfiguration();
		N = conf.getInt("TopN.terms", 10);
	}

    public void reduce(NullWritable key, Iterable<Text> values,
                       Context context
                       ) throws IOException, InterruptedException {
    	TreeMap<Integer, Text> finalTopN = new TreeMap<Integer, Text>(new TreeMapComparator());
    	int freq = 0;
    	
    	// aggregate all local top N words
    	String text = "";
    	for(Text word: values)
    	{
    		text = word.toString();
    		String[] tokens = text.split(" ");
    		try {
    			freq = Integer.parseInt(tokens[1]);
    			finalTopN.put(freq, new Text(text));
    		} catch(ArrayIndexOutOfBoundsException e) {
    			System.out.println("INDEX OUT OF BOUNDS EXCEPTION!!!!!!!!!!!!!!!!");
    			System.out.println("ON LINE: " + text);
    			System.out.println("Tokens length:" + tokens.length);
    			
    		}
    		
    		if(finalTopN.size() > N)
    		{
    			finalTopN.remove(finalTopN.firstKey());
    		}
    	}
    	
    	// emit final top N list
    	while(finalTopN.size() != 0)
    	{
    		int topKey = finalTopN.lastKey();
    		context.write(NullWritable.get(), finalTopN.remove(topKey));
    	}
    }
  }

  public static void main(String[] args) throws Exception {
	long startTime = System.currentTimeMillis();
	long endTime = 0;
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "TopN");
    job.setJarByClass(TopN.class);
    job.setMapperClass(TopNMapper.class);
    job.setReducerClass(TopNReducer.class);
    job.setMapOutputKeyClass(NullWritable.class);
    job.setMapOutputValueClass(Text.class);
    job.setNumReduceTasks(1);
    job.setOutputKeyClass(NullWritable.class);
    job.setOutputValueClass(Text.class);
    GenericOptionsParser optionParser = new GenericOptionsParser(conf, args);
    String[] remainingArgs = optionParser.getRemainingArgs();
    List<String> otherArgs = new ArrayList<String>();
	for (int i=0; i < remainingArgs.length; ++i) 
	{
		if("-N".contentEquals(remainingArgs[i])) 
		{
			job.getConfiguration().setInt("TopN.terms", Integer.parseInt(remainingArgs[i+1]));
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