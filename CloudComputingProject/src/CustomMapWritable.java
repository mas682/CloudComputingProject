import java.util.Hashtable;
import java.util.Enumeration;


public class CustomMapWritable{
	
	// switched these to ?,?
	private Hashtable<Integer,Integer> values;
	
	public CustomMapWritable()
	{
		values = new Hashtable<Integer, Integer>();
	}
	
	
	public void add(int key, int value)
	{
		int val = 0;
		try {
			val = values.get(key);
			val = (int)val;
            val += value;
            values.replace(key, val);
		} catch(NullPointerException e)
		{
			values.put(key, value);
        }
	}
	
	
	@Override
	public String toString()
	{
		Enumeration<Integer> e = values.keys();
		String output = "";
		while(e.hasMoreElements())
		{
			int key = e.nextElement();
			output = output.concat(key + " " + values.get(key) + " ");
		}
		return output;
	}
	
	public Hashtable<Integer, Integer> getHash()
	{
		return (Hashtable<Integer, Integer>)values.clone();
	}

}
