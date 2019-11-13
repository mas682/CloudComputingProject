
import java.util.*;

public class HadoopClient extends Client {


	public HadoopClient(String name, int port)
	{
		connect(name, port);
	}


	//this method is ONLY to be used on initialization
	public Boolean initialize()
	{
		Envelope message = null, response = null;
		try
		{
			message = new Envelope("INIT");
			output.writeObject(message);

			response = (Envelope)input.readObject();
			if(response.getMessage().equals("INITIALIZED"))
			{
				return true;
			}
			return false;
		}
		catch(Exception e)
		{
			System.err.println("Error: " + e.getMessage());
			e.printStackTrace(System.err);
			return false;
		}
	}

	 public String search(String term)
	 {
		 try
			{
				Envelope message = null, response = null;
				//Tell the server to create a user
				message = new Envelope("SEARCH");
				message.addObject(term); //Add user name string
				output.writeObject(message);

				response = (Envelope)input.readObject();

				//If server indicates success, return true
				if(response.getMessage().equals("RESULTS"))
				{
					return (String)response.getObjContents().get(0);
				}

				return null;
			}
			catch(Exception e)
			{
				System.err.println("Error: " + e.getMessage());
				e.printStackTrace(System.err);
				return null;
			}
	 }

	public ArrayList<String> getTopN(int num)
	{
		try
		   {
			   Envelope message = null, response = null;
			   //Tell the server to create a user
			   message = new Envelope("TOPN");
			   message.addObject(num); //Add user name string
			   output.writeObject(message);

			   response = (Envelope)input.readObject();

			   //If server indicates success, return true
			   if(response.getMessage().equals("RESULTS"))
			   {
				   return (ArrayList<String>)response.getObjContents().get(0);
			   }
			   return null;
		   }
		   catch(Exception e)
		   {
			   System.err.println("Error: " + e.getMessage());
			   e.printStackTrace(System.err);
			   return null;
		   }
	}

}
