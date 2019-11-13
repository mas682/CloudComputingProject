import java.io.BufferedReader;
import java.util.Hashtable;
import java.util.Enumeration;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.commons.logging.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.StringUtils;


public class InvertedIndexing {

  public static class TokenizerMapper
       extends Mapper<Object, Text, DocTermPair, IntWritable>{

    static enum CountersEnum { INPUT_WORDS }
   // private int LOG_COUNTER = 0;
   // private static final Log LOG = LogFactory.getLog(TokenizerMapper.class);

    private Hashtable<String, Integer> hash;
    private String filename;
    private int filenum;

    @Override
    public void setup(Context context) throws IOException,
        InterruptedException {
      hash = new Hashtable<String, Integer>();
      filename = null;
      filenum = 0;
    }

    @Override
    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      String line = value.toString();
      line = line.replaceAll("[\\p{Punct}*&&[^']]", " ");
      if(filename == null)
      {
    	  FileSplit fileSplit = (FileSplit)context.getInputSplit();
    	  filename = fileSplit.getPath().getName();
    	  filenum = filename.hashCode();
      }
      //LOG.info("Filename: " + filename + " File hash: " + filenum);
      StringTokenizer itr = new StringTokenizer(line);
      String token = "";
      Object count = null;
      int val = 0;
      while (itr.hasMoreTokens()) {
    	token = itr.nextToken();
    	count = hash.get(token);
    	if(count != null)
    	{
    		val = (int)count;
    		//LOG.debug("Term: " + token + " Value: " + val);
    		val++;
    		hash.replace(token, val);
    	}
    	else
    	{
    		hash.put(token,1);
    			//LOG.debug("Term: " + token + " Value: " + val);
    	}
      }
    }
    
    @Override
    public void cleanup(Context context) throws IOException, InterruptedException
    {
    	String token = "";
    	DocTermPair wordPair = new DocTermPair();
    	Enumeration<String> e = hash.keys();
        while(e.hasMoreElements())
        {
      	  token = e.nextElement();
      	  wordPair.set(filenum, token, hash.get(token));
      	  context.write(wordPair, new IntWritable(hash.get(token)));
      	  //LOG.debug("Map output key: " + wordPair.toText().toString() + " Value: "+ hash.get(token));
          //Counter counter = context.getCounter(CountersEnum.class.getName(),
          //          CountersEnum.INPUT_WORDS.toString());
           // counter.increment(1);
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
	  
	  private DocTermPair term;
	  private ArrayList<DocFreqPair> result;
	  
	  @Override
	  public void setup(Context context) throws IOException,
	        InterruptedException {
		  result = new ArrayList<DocFreqPair>();
		  term = null;
	  }

	//private static final Log LOG = LogFactory.getLog(IntSumReducer.class); 
	  
    public void reduce(DocTermPair key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
      if(term != null)
      {
    	  if(!(term.getTerm().toString().equals(key.getTerm().toString())))
    	  {
    		  //System.out.println("New Term!");
    		  String output = "";
    		  DocFreqPair temp = null;
    		  for(int i = 0; i < result.size(); i++)
    		  {
    			  temp = result.get(i);
    			  output = output.concat(temp.getDoc() + " " + temp.getFreq() + " ");
    		  }
    		  context.write(term,  new Text(output));
    		  result = new ArrayList<DocFreqPair>();
    	  }
      }
      for (IntWritable val : values) {
    	 // System.out.println(key.getTerm().toString() + " " + key.getDoc().get() + " " + val.get());
    	  DocFreqPair temp = new DocFreqPair(key.getDoc().get(), val.get());
    	  result.add(temp);
      }
      if(term == null)
      {
    	  term = new DocTermPair();
      }
      term.set(key.getDoc().get(), key.getTerm().toString(), key.getFreq().get());
      //System.out.println("TERM SET TO " + term.getTerm().toString());
      //context.write(key, new Text(result.toString()));
  	  //  LOG.debug("Reducer output: " + key + " Value: "+ result.toString());
    }
    
    @Override
    public void cleanup(Context context) throws IOException, InterruptedException
    {
    	if(term != null)
    	{
    		context.write(term, new Text(result.toString()));
    	}
    }
  }

  public static void main(String[] args) throws Exception {
	long startTime = System.currentTimeMillis();
	long endTime = 0;
    Configuration conf = new Configuration();
    GenericOptionsParser optionParser = new GenericOptionsParser(conf, args);
    String[] remainingArgs = optionParser.getRemainingArgs();
    if ((remainingArgs.length != 2) && (remainingArgs.length != 4)) {
      System.err.println("Usage: Inverted Indexing <in> <out> [-skip skipPatternFile]");
      System.exit(2);
    }
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