import java.util.Hashtable;
import java.util.Enumeration;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


public class InvertedIndexing {

  public static class TokenizerMapper
       extends Mapper<Object, Text, DocTermPair, IntWritable>{

	private boolean LOGENABLED;
    static enum Mapper { NumTerms }
    private static final Log LOG = LogFactory.getLog(TokenizerMapper.class); 
    
    // holds the terms and the frequency of them
    private Hashtable<String, Integer> hash;
    // used to hold the name of the file where the data came from
    private String filename;
    // used to hold the hash of the filename
    private int filenum;

    @Override
    public void setup(Context context) throws IOException,
        InterruptedException {
      Configuration conf = context.getConfiguration();
      // initiialize the hash function
      hash = new Hashtable<String, Integer>();
      // initialize the file name
      filename = null;
      // initialize the file number
      filenum = 0;
      LOGENABLED = conf.getBoolean("LOG", false);
    }

    @Override
    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      // convert the input value to a string
      String line = value.toString();
      // replace all the punctuation of the file with a space
      line = line.replaceAll("[\\p{Punct}*&&[^']]", " ");
      // if the file name has not been set yet, do it now
      if(filename == null)
      {
    	  // get the filesplit
    	  FileSplit fileSplit = (FileSplit)context.getInputSplit();
    	  // get the file name
    	  filename = fileSplit.getPath().getName();
    	  // get the hashcode of the filename
    	  filenum = filename.hashCode();
    	  if(LOGENABLED)
    		  LOG.info("Input file name: " + filename + " Hash of file name: " + filenum);
      }
      // get a string tokenizer, passing it the input
      StringTokenizer itr = new StringTokenizer(line);
      // temporary variable to hold the current token
      String token = "";
      // used to hold frequency of the associated term in the hash table
      // it is a object so it can hold null, but cast to a int later
      Object count = null;
      // used to temporarily hold the frequency of a term
      int val = 0;
      // loop while there are more tokens to process
      while (itr.hasMoreTokens()) {
    	// get the term
    	token = itr.nextToken();
    	// see if there is a frequency associated with it already
    	count = hash.get(token);
    	// if there is a frequency do this
    	if(count != null)
    	{
    		// convert the Object into a integer
    		val = (int)count;
    		if(LOGENABLED)
    			LOG.info("Term: " + token + " Value: " + val);
    		// increment the frequency of the term
    		val++;
    		// place the term and frequency back into the hash table
    		hash.replace(token, val);
    	}
    	else
    	{
    		// if here, this is the first time the term was found in the document
    		hash.put(token,1);
    		if(LOGENABLED)
    			LOG.info("First occurence of Term: " + token + " Value: 1");
    	}
      }
    }
    
    @Override
    public void cleanup(Context context) throws IOException, InterruptedException
    {
    	// holds a term temporarily
    	String token = "";
    	// this is the composite key that will be used
    	DocTermPair wordPair = new DocTermPair();
    	// get an enumerator over the keys of the hash
    	Enumeration<String> e = hash.keys();
    	// do this for each key in the hash table
        while(e.hasMoreElements())
        {
        	// get term
      	  	token = e.nextElement();
      	  	// create the composite key, consiting of the filenumber, the term itself, and the frequency of the term
      	  	wordPair.set(filename, token, hash.get(token));
      	  	// output the composite key, along with the value of the terms
      	  	context.write(wordPair, new IntWritable(hash.get(token)));
      	  	if(LOGENABLED)
      	  		LOG.info("Map output key: " + wordPair.toText().toString() + " Value: "+ hash.get(token));
      	  	context.getCounter(Mapper.NumTerms).increment(1);
        }
    }
  }

/*
 * do not think I need this combiner but if so, change result to a array list..
public static class IntSumCombiner
  extends Reducer<DocTermPair,IntWritable,DocTermPair,IntWritable> {

	//private static final Log LOG = LogFactory.getLog(IntSumCombiner.class);
	  private CustomMapWritable result = new CustomMapWritable();
	  private DocTermPair term;
	  @Override
	  public void setup(Context context) throws IOException,
	        InterruptedException {
		  result = new CustomMapWritable();
		  term = null;
	  }
	
	public void reduce(DocTermPair key, Iterable<IntWritable> values,
                  Context context
                  ) throws IOException, InterruptedException {
		if(term != null)
		{
	    	  if(!(term.getTerm().toString().equals(key.getTerm().toString())))
	    	  {
	    		  System.out.println("New Term!");
	    		  Hashtable<Integer, Integer> list = result.getHash();
	    		  DocTermPair wordPair = new DocTermPair();
	    		  Enumeration<Integer> e = list.keys();
	    		  while(e.hasMoreElements())
	    		  {
	    			  int docNum = e.nextElement();
	    			  wordPair.set(docNum, term.getTerm().toString(),list.get(docNum));
	    			  context.write(term, new IntWritable(list.get(docNum)));
	    		      //	LOG.debug("Combiner output: " + key + " Value: "+ list.get(docNum));
	    		  }
	    		  result = new CustomMapWritable();
	    	  }
		}
	   // System.out.println("KEY: " + key);
		for (IntWritable val : values) {
			result.add(key.getDoc().get(), val.get());
			System.out.println("DOC: " + key.getDoc() + " value: " + val.get());
		}
		if(term == null)
	    {
			term = new DocTermPair();
	    }
	    term.set(key.getDoc().get(), key.getTerm().toString(), key.getFreq().get());
	}
    @Override
    public void cleanup(Context context) throws IOException, InterruptedException
    {
    	if(term != null)
    	{
    		Hashtable<Integer, Integer> list = result.getHash();
  		  	DocTermPair wordPair = new DocTermPair();
  		  	Enumeration<Integer> e = list.keys();
  		  	while(e.hasMoreElements())
  		  	{
  		  		int docNum = e.nextElement();
  		  		wordPair.set(docNum, term.getTerm().toString(),list.get(docNum));
  		  		context.write(term, new IntWritable(list.get(docNum)));
  		      //	LOG.debug("Combiner output: " + key + " Value: "+ list.get(docNum));
  		  	}
    	}
    }
}
*/
  

  public static class IntSumReducer
       extends Reducer<DocTermPair,IntWritable,DocTermPair,Text> {
	  
	  static enum Reducer { NumTerms, OutOfOrder, ZeroFrequencies, TooManyDocs }
	  private static final Log LOG = LogFactory.getLog(IntSumReducer.class); 
	  // used to hold the current term that is being processed
	  private DocTermPair term;
	  // holds the frequencies, doc ID, and term of each unique term
	  private ArrayList<DocFreqPair> result;
	  private boolean LOGENABLED;
	  
	  @Override
	  public void setup(Context context) throws IOException,
	        InterruptedException {
		  Configuration conf = context.getConfiguration();
		  // initilize the arraylist to an empty arraylist
		  result = new ArrayList<DocFreqPair>();
		  // set the term to null, as want to skip the first if in reduce
		  // if this is the first term
		  term = null;
		  LOGENABLED = conf.getBoolean("LOG", false);
	  }
	  
    public void reduce(DocTermPair key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
      if(term != null)
      {
    	  // if the term coming in does not match the previous term
    	  // the terms will come in sorted based off of the term, and the frequencies
    	  // of which they were found in the document
    	  int prevFreq = -1;
    	  if(!(term.getTerm().toString().equals(key.getTerm().toString())))
    	  {
    		  if(LOGENABLED)
    			  LOG.info("New term: " + key.getTerm());
    		  // used to temporarily hold the output
    		  String output = "";
    		  // used to temporarily hold the DocFreqPair from the arraylist
    		  DocFreqPair temp = null;
    		  if(result.size() < 1)
      		  {
      		  	context.getCounter(Reducer.ZeroFrequencies).increment(1);
      		  	if(LOGENABLED)
      		  		LOG.info("There are no frequencies associated with the term: " + term);
      		  }
    		  else if(result.size() > 3)
    		  {
    			  context.getCounter(Reducer.TooManyDocs).increment(1);
    			  if(LOGENABLED)
    				  LOG.info("There are too many doc/frequency pairs found for the term: " + term);
    		  }
    		  // loop through the arraylist for each docID, Frequency pair
    		  for(int i = 0; i < result.size(); i++)
    		  {
    			  // get the first doc and frequency for the term
    			  // these are already in order
    			  temp = result.get(i);
    			  // add to the output the docID and the frequency, seperated by spaces
    			  output = output.concat(temp.getDoc() + " " + temp.getFreq() + " ");
    			  if(prevFreq == -1)
    			  {
    				  prevFreq = temp.getFreq();
    			  }
    			  else
    			  {
    				  if(temp.getFreq() > prevFreq)
    				  {
    					  context.getCounter(Reducer.OutOfOrder).increment(1);
    					  if(LOGENABLED)
    					  {
    						  LOG.info("The term " + term.getTerm() + " has frequencies out of order");
    						  LOG.info(term.getTerm() + " " + output);
    					  }
    				  }
    				  prevFreq = temp.getFreq();
    			  }
    		  }
    		  // output the result
    		  context.write(term,  new Text(output));
    		  context.getCounter(Reducer.NumTerms).increment(1);
    		  if(LOGENABLED)
    			  LOG.info("Output: " +term.toString() + "\t" + output);
    		  // reset the arraylist to a empty list as a new term is going to get processed
    		  result = new ArrayList<DocFreqPair>();
    	  }
      }
      
      for (IntWritable val : values) {
    	  // set the pair to the docID and the frequency of the term in the doc
    	  DocFreqPair temp = new DocFreqPair(key.getDoc().toString(), val.get());
    	  // append this to the next available position in the array list
    	  // the values should come into the reducer in order
    	  result.add(temp);
      }
      // if this is the first term processed by the reducer
      if(term == null)
      {
    	  // initialize the term
    	  term = new DocTermPair();
      }
      // update the term
      // may not want to do this every time as wasting time?
      term.set(key.getDoc().toString(), key.getTerm().toString(), key.getFreq().get());
    }
    
    
    // this method has to execute once at the end as there will be one term that
    // still has to be output
    @Override
    public void cleanup(Context context) throws IOException, InterruptedException
    {
    	// if there is a term to output
    	if(term != null)
    	{
    		// the string to output
    		String output = "";
    		// temporarily holds a doc, term pair
  		  	DocFreqPair temp = null;
  		  	int prevFreq = -1;
  		  	// if there are values still in the results array list
  		  	if(result.size() < 1)
  		  	{
  		  		context.getCounter(Reducer.ZeroFrequencies).increment(1);
  		  		if(LOGENABLED)
  		  			LOG.info("There are no frequencies associated with the term: " + term);
  		  	}
  		  	else if(result.size() > 3)
  		  	{
  		  		context.getCounter(Reducer.TooManyDocs).increment(1);
  		  		if(LOGENABLED)
  		  			LOG.info("There are too many doc/frequency pairs found for the term: " + term);
  		  	}
  		  	for(int i = 0; i < result.size(); i++)
  		  	{
  		  		temp = result.get(i);
  		  		output = output.concat(temp.getDoc() + " " + temp.getFreq() + " ");
  		  		if(prevFreq == -1)
  		  		{
  		  			prevFreq = temp.getFreq();
  		  		}	
  		  		else
  		  		{
  		  			if(temp.getFreq() > prevFreq)
  		  			{
  		  				context.getCounter(Reducer.OutOfOrder).increment(1);
  		  				if(LOGENABLED)
  		  					LOG.info("The term " + term + " has frequencies out of order");
  		  			}
  		  			prevFreq = temp.getFreq();
  		  		}
  		  	}
  		  	context.getCounter(Reducer.NumTerms).increment(1);
  		  	context.write(term,  new Text(output));
    	}
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    GenericOptionsParser optionParser = new GenericOptionsParser(conf, args);
    String[] remainingArgs = optionParser.getRemainingArgs();

    Job job = Job.getInstance(conf, "Inverted Indexing");
    job.setJarByClass(InvertedIndexing.class);
    job.setMapperClass(TokenizerMapper.class);
    //job.setCombinerClass(IntSumCombiner.class);
    job.setReducerClass(IntSumReducer.class);
    job.setMapOutputKeyClass(DocTermPair.class);
    job.setMapOutputValueClass(IntWritable.class);
    job.setPartitionerClass(DocTermPartitioner.class);
    job.setGroupingComparatorClass(DocTermGroupingComparator.class);
    job.setOutputKeyClass(DocTermPair.class);
    job.setOutputValueClass(Text.class);

    List<String> otherArgs = new ArrayList<String>();
    for (int i=0; i < remainingArgs.length; ++i) {
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