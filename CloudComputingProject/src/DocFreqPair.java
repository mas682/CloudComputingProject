
public class DocFreqPair {
	
	private String doc;
	private int freq;
	
	public DocFreqPair(String docNum, int freqNum)
	{
		doc = docNum;
		freq = freqNum;
	}
	
	public String getDoc()
	{
		return doc;
	}
	
	public int getFreq()
	{
		return freq;
	}
}
