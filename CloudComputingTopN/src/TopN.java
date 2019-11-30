import java.util.TreeMap;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;



public class TopN {

  public static class TopNMapper
       extends Mapper<Object, Text, NullWritable, Text>{
	
	private static final Log LOG = LogFactory.getLog(TopNMapper.class);  
	private TreeMap<Integer, Text> topNwords = new TreeMap<Integer, Text>(new TreeMapComparator());
	private int N = 0;
	private boolean LOGENABLED;
	static enum Mapper{ InvalidValue, TermsProcessed }

    @Override
    public void setup(Context context) throws IOException,
        InterruptedException {
      Configuration conf = context.getConfiguration();
      N = conf.getInt("TopN.terms", 0);
      LOG.info("The value of N received by the mapper: " + N);
      LOGENABLED=conf.getBoolean("LOG", false);
    }

    @Override
    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      // the mapper receives lines from the inverted index output
      // terms are seperated by tabs from the file and frequencies
      StringTokenizer itr = new StringTokenizer(value.toString(), " \t");
      // if the number of tokens is less than 2, then there is some issue in the input
      if(itr.countTokens() < 2)
      {
    	  if(LOGENABLED)
    		  LOG.info("\"" + value.toString() + "\" is in the wrong format");
    	  context.getCounter(Mapper.InvalidValue).increment(1);
      }
      // used to hold current token
      String token = "";
      // used to hold the value of the term
      String term = "";
      // used to hold the frequency of a given term
      int val = 0;
      // used to determine which token is currently being looked at
      int counter = 0;
      // holds the frequency of a single file
      int freq = 0;
      // iterate through the tokens
      while (itr.hasMoreTokens()) {
    	// get a token
    	token = itr.nextToken();
    	// if this is the first token, then this should be the term
    	if(counter == 0)
    	{
    		term = token;
    		//LOG.info("TERM: " + token);
    	}
    	// if the token count is divisible by 2, this should be a frequency of the term
    	else if(counter%2 == 0)
    	{
    		// get the frequency, which is a string, so converted to a integer
    		freq = Integer.parseInt(token);
    		// increment the current frequency associated with the term in all documents
    		val += freq;
    	}
    	counter++;
      }
      // display the result in the log
      if(LOGENABLED)
    	  LOG.info("Term: " + term + " Frequency: "+ val);
      // create text that will be the value stored in the sorted map
      Text tempVal = new Text(term + " " + val);
      // place the results in the sorted map
      // val is the frequency, tempVal consists of the term and the frequency
      topNwords.put(val, tempVal);
      // if the number of pairs in the map is greater than the number requested, remove the smallest value pair
      if(topNwords.size() > N)
      {
    	  // remove the element with the smallest frequency
    	  topNwords.remove(topNwords.firstKey());
      }
      context.getCounter(Mapper.TermsProcessed).increment(1);
    }
    
    /**
     * cleanup() function will be executed once at the end of each mapper
     * Here we set up the "words top N list" as topNwords
     */
    @Override
    public void cleanup(Context context) throws IOException, InterruptedException
    {
    	// If the size of the sorted map for this mapper is too big, output this
    	if(topNwords.size() > N)
    	{
    		if(LOGENABLED)
    			LOG.info("The size of the sorted map is too big: " + topNwords.size());
    	}
    	for(Text term: topNwords.values() ) {
    		context.write(NullWritable.get(), term);
    	}
    }
  }
 
  public static class TopNReducer
       extends Reducer<NullWritable,Text,NullWritable,Text> {
	  
	private static final Log LOG = LogFactory.getLog(TopNReducer.class);
	private int N = 0;
	private boolean LOGENABLED;
	static enum Reducer{ TermsProcessed, InvalidFormat, OutOfBounds }
	
	@Override
	public void setup(Context context) throws IOException,
     InterruptedException {
		Configuration conf = context.getConfiguration();
		// get the number of top terms to get, set to 0 by default
		N = conf.getInt("TopN.terms", 0);
		LOGENABLED=conf.getBoolean("LOG", false);
		if(LOGENABLED)
			LOG.info("The value of N received by the reducer: " + N);
	}

    public void reduce(NullWritable key, Iterable<Text> values,
                       Context context
                       ) throws IOException, InterruptedException {
    	TreeMap<Integer, Text> finalTopN = new TreeMap<Integer, Text>(new TreeMapComparator());
    	// temporary variable for the frequency of a term
    	int freq = 0;
    	// temporarily holds the input value as a string
    	String text = "";
    	// for each of term,freq pairs that come in
    	for(Text word: values)
    	{
    		// convert the input value to a string
    		text = word.toString();
    		// seperate the term from the frequency
    		String[] tokens = text.split(" ");
    		// if there are less than 2 tokens, the input is in the wrong format
    	    if(tokens.length < 2)
    	    {
    	    	if(LOGENABLED)
    	    		LOG.info("\"" + values.toString() + "\" is in the wrong format");
    	    	context.getCounter(Reducer.InvalidFormat).increment(1);
    	    }
    		try {
    			// the requency of the term should be the token at index 1
    			freq = Integer.parseInt(tokens[1]);
    			// place the frequency, and the (term frequency) into the sorted map
    			finalTopN.put(freq, new Text(text));
    			if(LOGENABLED)
    				LOG.info(text + " added to the sorted map");
    		} catch(ArrayIndexOutOfBoundsException e) {
    			if(LOGENABLED)
    			{
    				LOG.info("INDEX OUT OF BOUNDS EXCEPTION!");
    				LOG.info("ON THE INPUT LINE: " + text);
    				LOG.info("Tokens length:" + tokens.length);
    			}
    			context.getCounter(Reducer.OutOfBounds).increment(1);
    		}
    		// if the size of the sorted map is greater than n, remove the smallest value
    		if(finalTopN.size() > N)
    		{
    			finalTopN.remove(finalTopN.firstKey());
    		}
    		context.getCounter(Reducer.TermsProcessed).increment(1);
    	}
    	
    	// emit final top N list
    	while(finalTopN.size() != 0)
    	{
    		// get the highest value from the map
    		int topKey = finalTopN.lastKey();
    		// get the term frequency pair associated with the value
    		// this is removed from the map on output
    		context.write(NullWritable.get(), finalTopN.remove(topKey));
    	}
    }
  }

  public static void main(String[] args) throws Exception {
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
		// set TopN.terms to the number of terms being looked for
		if("-N".contentEquals(remainingArgs[i])) 
		{
			job.getConfiguration().setInt("TopN.terms", Integer.parseInt(remainingArgs[i+1]));
			i++;
		}
		if("-D".contentEquals(remainingArgs[i]))
		{
			job.setProfileEnabled(true);
			job.setProfileTaskRange(true, "0");
			i++;
		}
		if("-LOG".contentEquals(remainingArgs[i]))
		{
			job.getConfiguration().setBoolean("LOG", true);
		}
		else
		{
			otherArgs.add(remainingArgs[i]);
		}
    }
    FileInputFormat.addInputPath(job, new Path(otherArgs.get(0)));
    FileOutputFormat.setOutputPath(job, new Path(otherArgs.get(1)));
    if(job.waitForCompletion(true))
    {
    	System.exit(0);
    }
    else {
    	System.exit(1);
    }
  }
}