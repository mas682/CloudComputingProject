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
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.StringUtils;


public class InvertedIndexing {

  public static class TokenizerMapper
       extends Mapper<Object, Text, DocTermPair, IntWritable>{

    static enum CountersEnum { INPUT_WORDS }

    DocTermPair wordPair = new DocTermPair();

    private boolean caseSensitive;
    private Set<String> patternsToSkip = new HashSet<String>();

    private Configuration conf;
    private BufferedReader fis;

    @Override
    public void setup(Context context) throws IOException,
        InterruptedException {
      conf = context.getConfiguration();
      caseSensitive = conf.getBoolean("wordcount.case.sensitive", true);
      if (conf.getBoolean("wordcount.skip.patterns", false)) {
        URI[] patternsURIs = Job.getInstance(conf).getCacheFiles();
        for (URI patternsURI : patternsURIs) {
          Path patternsPath = new Path(patternsURI.getPath());
          String patternsFileName = patternsPath.getName().toString();
          parseSkipFile(patternsFileName);
        }
      }
    }

    private void parseSkipFile(String fileName) {
      try {
        fis = new BufferedReader(new FileReader(fileName));
        String pattern = null;
        while ((pattern = fis.readLine()) != null) {
          patternsToSkip.add(pattern);
        }
      } catch (IOException ioe) {
        System.err.println("Caught exception while parsing the cached file '"
            + StringUtils.stringifyException(ioe));
      }
    }

    @Override
    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      Hashtable<String, Integer> hash = new Hashtable<String, Integer>();
      String line = (caseSensitive) ?
          value.toString() : value.toString().toLowerCase();
      for (String pattern : patternsToSkip) {
        line = line.replaceAll(pattern, "");
      }
      line = line.replaceAll("[\\p{Punct}*&&[^']]", " ");
      FileSplit fileSplit = (FileSplit)context.getInputSplit();
      String filename = fileSplit.getPath().getName();
      int filenum = filename.hashCode();
      System.out.println("File: " + filename);
      System.out.println("Filenum: " + filenum);
      System.out.println("Value " + value);
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
    		val++;
    		hash.replace(token, val);
    	}
    	else
    	{
    		hash.put(token,1);
    	}
      }
      Enumeration<String> e = hash.keys();
      while(e.hasMoreElements())
      {
    	  token = e.nextElement();
    	  // think this should be a object containing doc,key instead of string
    	 wordPair.set(filenum, token, hash.get(token));
    	  // not sure what this stuff below des exactly..
    	  context.write(wordPair, new IntWritable(hash.get(token)));
          Counter counter = context.getCounter(CountersEnum.class.getName(),
                  CountersEnum.INPUT_WORDS.toString());
          counter.increment(1);
      }
    }
  }
  
public static class IntSumCombiner
  extends Reducer<DocTermPair,IntWritable,DocTermPair,IntWritable> {

	public void reduce(DocTermPair key, Iterable<IntWritable> values,
                  Context context
                  ) throws IOException, InterruptedException {
		CustomMapWritable result = new CustomMapWritable();
		for (IntWritable val : values) {
			result.add(key.getDoc().get(), val.get());
		}
		Hashtable<Integer, Integer> list = result.getHash();
		DocTermPair wordPair = new DocTermPair();
		Enumeration<Integer> e = list.keys();
		while(e.hasMoreElements())
		{
			int docNum = e.nextElement();
			wordPair.set(docNum, key.getTerm().toString(),0);
			context.write(key, new IntWritable(list.get(docNum)));
		}
	}
}
  

  public static class IntSumReducer
       extends Reducer<DocTermPair,IntWritable,DocTermPair,Text> {

    public void reduce(DocTermPair key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
      CustomMapWritable result = new CustomMapWritable();
      for (IntWritable val : values) {
    	  result.add(key.getDoc().get(), val.get());
      }
      context.write(key, new Text(result.toString()));
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
    job.setCombinerClass(IntSumCombiner.class);
    job.setReducerClass(IntSumReducer.class);
    job.setMapOutputKeyClass(DocTermPair.class);
    job.setMapOutputValueClass(IntWritable.class);
    job.setPartitionerClass(DocTermPartitioner.class);
    job.setOutputKeyClass(DocTermPair.class);
    job.setOutputValueClass(Text.class);

    List<String> otherArgs = new ArrayList<String>();
    for (int i=0; i < remainingArgs.length; ++i) {
      if ("-skip".equals(remainingArgs[i])) {
        job.addCacheFile(new Path(remainingArgs[++i]).toUri());
        job.getConfiguration().setBoolean("wordcount.skip.patterns", true);
      } else {
        otherArgs.add(remainingArgs[i]);
      }
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