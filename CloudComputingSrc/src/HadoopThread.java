/* This thread does all the work. It communicates with the client through Envelopes.
 * This will be ran on the hadoop cluster
 */
import java.lang.Thread;

import java.net.Socket;
import java.io.*;
import java.lang.Exception;
import java.util.*;
import java.util.ArrayList;

public class HadoopThread extends Thread
{

	private final Socket socket;

	public HadoopThread(Socket _socket, HadoopServer _hs)
	{
		socket = _socket;
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
	// need to update hadoop.sh before letting this go thorugh
					/*
					System.out.println("Initializing the inverted indices..");
					try {
						// below code is used to execute a script which runs the hadoop program
						String cmd[] = {"sh", "hadoop.sh"};
						Process p = Runtime.getRuntime().exec(cmd);
						p.waitFor();
						System.out.println("System initialized.");
						response = new Envelope("INITIALIZED");
					} catch (IOException e) {
						System.out.println(e);
					} catch (InterruptedException e) {
						System.out.println(e);
					}
					*/
					output.writeObject(response); //ouput either done or fail
				}
				else if(message.getMessage().equals("SEARCH"))
				{
					//set response to fail
					response = new Envelope("FAIL");
					String results = "";
					if(message.getObjContents().get(0) != null)
					{
						String term = (String)message.getObjContents().get(0);
						System.out.println("The term to search for is: " + term);
						try {
							// below code is used to execute a script which runs the hadoop program
							String cmd[] = {"sh", "hadoopSearch.sh", term};
							Process p = Runtime.getRuntime().exec(cmd);
							p.waitFor();
							System.out.println("Search Results are in!");
							results = searchTerm();
							response = new Envelope("RESULTS");
							response.addObject(results);
						} catch (IOException e) {
							System.out.println(e);
						} catch (InterruptedException e) {
							System.out.println(e);
						}
					}
					output.writeObject(response);  // output either results or fail
				}
				else if(message.getMessage().equals("TOPN"))
				{
					//set response to fail
					response = new Envelope("FAIL");

					// make sure the terms to search for is not null
					if(message.getObjContents().get(0) != null)
					{
						int num = (int)message.getObjContents().get(0);
						System.out.println("Got the number: " + num);
						try {
							// below code is used to execute a script which runs the hadoop program
							String[] cmd = {"sh", "hadoopTopN.sh", Integer.toString(num)};
							// for top n, need to do export TERMS=num terms to string
							Process p = Runtime.getRuntime().exec(cmd);
							p = Runtime.getRuntime().exec(cmd);
							p.waitFor();
							System.out.println("Results in!");
							ArrayList<String> results = topN(num);
							response = new Envelope("RESULTS");
							response.addObject(results);
						} catch (IOException e) {
							System.out.println(e);
						} catch (InterruptedException e) {
							System.out.println(e);
						}
					}
					output.writeObject(response);  // output either results or fail
				}
				else if(message.getMessage().equals("DISCONNECT"))
				{
					input.close();
					output.close();
					System.out.println("*** Connection closed to client " + socket.getInetAddress() + ":" + socket.getPort() + "***");
					proceed = false;
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
	private String searchTerm()
	{
		String results = "";
		BufferedReader br = null;
		String line = "";
		File file = new File("SearchResults");
		int counter = 0;
		try {
			br = new BufferedReader(new FileReader(file));
		} catch (FileNotFoundException e) {
			System.out.println(e);
			return null;
		}
		try {
			line = br.readLine();
		} catch (IOException e) {
			System.out.println(e);
		}
		while(line != null)
		{
			// should only be one result in the file..
			// unless not doing case sensitive in which
			// multiple lines could be there and would need to return a array of strings...
			if(counter >= 1)
			{
				results = "";
			}
			results = line;
			try {
				line = br.readLine();
			} catch (IOException e) {

			}
			counter++;
		}
		try {
			br.close();
		} catch(IOException e) {
			
		}
		return results;
	}

	// method to handle getting the top n terms
	// returns an arraylist holding the top n results
	private ArrayList<String> topN(int num)
	{
		ArrayList<String> results = new ArrayList<String>(num);
		BufferedReader br = null;
		String line = "";
		File file = new File("TopNResults");
		try {
			br = new BufferedReader(new FileReader(file));
		} catch (FileNotFoundException e) {
			System.out.println(e);
			return null;
		}
		try {
			line = br.readLine();
		} catch (IOException e) {
			System.out.println(e);
		}
		while(line != null)
		{
			results.add(line);
			try {
				line = br.readLine();
			} catch (IOException e) {

			}
		}
		try {
			br.close();
		} catch(IOException e) {
			
		}
		return results;
	}

}
