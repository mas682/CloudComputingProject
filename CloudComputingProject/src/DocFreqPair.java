
public class DocFreqPair {
	
	private int doc;
	private int freq;
	
	public DocFreqPair(int docNum, int freqNum)
	{
		doc = docNum;
		freq = freqNum;
	}
	
	public int getDoc()
	{
		return doc;
	}
	
	public int getFreq()
	{
		return freq;
	}
}
