/* This thread does all the work. It communicates with the client through Envelopes.
 * This will be ran on the hadoop cluster
 */
import java.lang.Thread;
import java.net.Socket;
import java.io.*;
import java.util.*;

public class HadoopThread extends Thread
{

	private final Socket socket;
	private HadoopServer my_hs;

	public HadoopThread(Socket _socket, HadoopServer _hs)
	{
		socket = _socket;
		my_hs = _hs;
	}

	public void run()
	{
		boolean proceed = true;

		try
		{
			//Announces connection and opens object streams
			System.out.println("*** New connection from " + socket.getInetAddress() + ":" + socket.getPort() + "***");
			final ObjectInputStream input = new ObjectInputStream(socket.getInputStream());
			final ObjectOutputStream output = new ObjectOutputStream(socket.getOutputStream());

			do
			{
				Envelope message = (Envelope)input.readObject();
				System.out.println("Request received: " + message.getMessage());
				Envelope response = null;

				if(message.getMessage().equals("INIT")) //used to initialize the system
				{
					//set response to fail
					response = new Envelope("FAIL");
					// will want to do the algorithm to generate the indexes here
					output.writeObject(response);//ouput either done or fail
				}
				else if(message.getMessage().equals("SEARCH"))
				{
					//set response to fail
					response = new Envelope("FAIL");
					String term = (String)message.getObjContents().get(0);
					boolean results = false;
					// do something on hadoop to get term
					try {
						// below code is used to execute a script which runs the hadoop program
						String[] cmd = ["sh", "hadoop.sh"];
						Process p = Runtime.getRuntime().exec(cmd);
						p.waitFor();
						// below is to read the output
						//BufferedReader reader=new BufferedReader(new InputStreamReader(p.getInputStream()));
						//String line;
						//while((line = reader.readLine()) != null) {
						//    System.out.println(line);
						//}
}
						System.out.println("Results in!");
					} catch (IOException e) {
						System.out.println(e);
					} catch (InterruptedException e) {
						System.out.println(e);
					}
					results = searchTerm(term);
					if(results)
					{
						response = new Envelope("RESULTS");
						// may want to add the results to the envelope
					}
					output.writeObject(response);  // output either results or fail
				}
				else if(message.getMessage().equals("TOPN"))
				{
					//set response to fail
					response = new Envelope("FAIL");
					// make sure the message has a string to search
					if(message.getObjContents().size() > 1)
					{
						// make sure the string to search for is not null
						if(message.getObjContents().get(0) != null)
						{
							int num = (int)message.getObjContents().get(0);
							boolean results = false;
							// do something on hadoop to get the top n
							results = topN(num);
							if(results)
							{
								response = new Envelope("RESULTS");
								// may want to add the results to the envelope
							}
						}
					}
					output.writeObject(response);  // output either results or fail
				}
			}while(proceed);
		}
		catch(Exception e)
		{
			System.err.println("Error: " + e.getMessage());
			e.printStackTrace(System.err);
		}
	}

	// method to handle getting the search term results
	// will want to return something other than a boolean
	private boolean searchTerm(String term)
	{
		return false;
	}

	// method to handle getting the top n terms
	// will want to return something other than a boolean
	private boolean topN(int num)
	{
		return false;
	}

}
