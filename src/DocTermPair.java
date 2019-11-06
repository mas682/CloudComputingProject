import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

public class DocTermPair
	implements Writable, WritableComparable<DocTermPair> {
  // not sure if this should be text or a integer?
  private Text term = new Text();
  private IntWritable freq = new IntWritable();
  private IntWritable doc = new IntWritable();
  
  public DocTermPair()
  {
	  
  }
  @Override
  /* This comparator controls the sort order of the keys. */
  public int compareTo(DocTermPair pair) {
	  int compareValue = this.term.compareTo(pair.getTerm());
	  if(compareValue == 0) {
		 //compareValue = doc.compareTo(pair.getDoc());
		  return compareValue;
	  }
	  return -1*compareValue;
  }
  
  /*
  public int compareToDoc(DocTermPair pair) {
	  int compareValue = doc.compareTo(pair.getDoc());
	  if(compareValue == 0) {
		  return compareValue;
	  }
	  return -1*compareValue;
  }
  */
  public Text getTerm()
  {
	  return this.term;
  }
  
  public IntWritable getDoc()
  {
	  return this.doc;
  }
  
  public void set(int docNum, String word, int frequency)
  {
	  term.set(word);
	  doc.set(docNum);
	  freq.set(frequency);
  }
  
  public IntWritable getFrequency()
  {
	  return this.freq;
  }
  
  public void write(DataOutput out) throws IOException 
  {
	  this.term.write(out);
	  this.freq.write(out);
	  this.doc.write(out);
  }
  
  public void readFields(DataInput in) throws IOException 
  {
	  term.readFields(in);
	  freq.readFields(in);
	  doc.readFields(in);
  }
  
  public Text toText()
  {
	  Text temp = new Text();
	  temp.set(this.doc + " " + this.term);
	  return temp;
  }
  

  public String toString()
  {
	  return this.term.toString();
  }
  
}