
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
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;



public class SearchTerm {

  public static class SearchTermMapper
       extends Mapper<Object, Text, NullWritable, Text>{
	
	private String searchTerm = "";
	private boolean LOGENABLED;
	static enum Mapper { InvalidTerm, TermsRead, MatchingTerms };
	private static final Log LOG = LogFactory.getLog(SearchTermMapper.class); 

    @Override
    public void setup(Context context) throws IOException,
        InterruptedException {
      // get the configuration to get the term to search for
      Configuration conf = context.getConfiguration();
      // variable to hold value of search term
      String [] searchTerms;
      // get the search term from the configuration
      searchTerms= conf.getStrings("SearchTerm.term");
      LOGENABLED = conf.getBoolean("LOG", false);
      // make sure there is a term to search for
      if(searchTerms.length > 0)
      {
    	  searchTerm = searchTerms[0];
    	  if(LOGENABLED)
    		  LOG.info("The term to search for is: " + searchTerm);
      }
      else
      {
    	  // if no term found, the search term is simply set to a empty string
    	  if(LOGENABLED)
    		  LOG.info("Map function given no term to search for");
    	  searchTerm = "";
      }
    }

    @Override
    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      // tokenizer to seperate term from output of inverted index(doc id, frequency)
      StringTokenizer itr = new StringTokenizer(value.toString(), " \t");
      // temporary variable to hold the current token
      String token = "";
      // holds the actual term if it is found in the file
      String term = "";
      // if less than 2 tokens, the input must be in invalid form
      if(itr.countTokens() < 2)
      {
    	  context.getCounter(Mapper.InvalidTerm).increment(1);
    	  if(LOGENABLED)
    		  LOG.info("Invalid input given to the map function");
      }
      while (itr.hasMoreTokens()) {
    	// get the first token
    	token = itr.nextToken();
    	// set the term to the token
    	term = token;
    	if(LOGENABLED)
    		LOG.info("TERM: " + token);
    	context.getCounter(Mapper.TermsRead).increment(1);
    	// only need to get first token, so break out of loop
    	// while loop just to make sure there are tokens
    	break;
      }
      // if the term matches the term that is being search for, output it
      if(term.contentEquals(searchTerm))
      {
    	  context.write(NullWritable.get(), value);
    	  context.getCounter(Mapper.MatchingTerms).increment(1);
      }
    }
  }
 

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "SearchTerm");
    job.setJarByClass(SearchTerm.class);
    job.setMapperClass(SearchTermMapper.class);
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