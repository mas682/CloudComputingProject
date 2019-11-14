//Matt Stropkey
//CS1699 Project

import java.io.*;
import java.util.*;
/**
 * MyClientApp is a application for simulating a group file server.
 * It is a client of the GroupClient and File classes.
 */
public class UserApp {

    // a scanner to get input from the user
    private static Scanner input = new Scanner(System.in);
	private static HadoopClient hadoopServ;

    public static void main(String args[]) {
		//will need to shares admins public key with server before attempting to login

        boolean connected = false;
		if(args.length > 0)
		{
			hadoopServ = new HadoopClient(args[0], Integer.parseInt(args[1]));
			connected = hadoopServ.isConnected();
			if(!connected)
			{
				System.out.println("Connection to Hadoop Server failed!");
			}
		}
		else
		{
			hadoopServ = new HadoopClient("localhost", HadoopServer.SERVER_PORT);
			connected = hadoopServ.isConnected();
			if(!connected)
			{
				System.out.println("Connection to Hadoop Server failed!");
			}
		}

		int selection = -1;
        while (selection != 0) {
            System.out.println();
            System.out.println("Hadoop App Main Menu");
            System.out.println("1. Construct Inverted Indicies");
            System.out.println("0. Quit");
            System.out.print("Selection: ");

            try {
                selection = input.nextInt();
            } catch (NoSuchElementException e) {
                selection = -1;
            } catch (IllegalStateException e) {
                selection = -1;
            }
            input.nextLine();

            switch (selection) {
                case 1:
                    selection = constructIndicies();
                    break;
                case 0:
                    quit();
                    break;
                default:
                    // Invalid, just ignore and let loop again
                    break;
            }
        }
    }


	public static int constructIndicies()
	{
		int selection = -1;
		System.out.println("Please wait while the Inverted Indicies are constructed...");
        boolean initialized = hadoopServ.initialize();
        if(initialized)
        {
        	System.out.println("Engine was loaded and ");
        	System.out.println("Inverted indicies were constructed successfully!\n");
        }
        else
        {
        	System.out.println("Unable to construct the Inverted Indicies.");
        	System.out.println("Please try again or press 0 to quit.\n");
        	return -1;
        }
        while (selection != 0) {
            System.out.println();
            System.out.println("Your options: ");
            System.out.println("1. Search for a term");
            System.out.println("2. Top-N");
            System.out.println("0. Quit");
            System.out.print("Selection: ");

            try {
                selection = input.nextInt();
            } catch (NoSuchElementException e) {
                selection = -1;
            } catch (IllegalStateException e) {
                selection = -1;
            }
            input.nextLine();

            switch (selection) {
                case 1:
                    searchTerm();
                    break;
				case 2:
					topN();
					break;
                case 0:
                    quit();
                    break;
                default:
                    // Invalid, just ignore and let loop again
                    break;
            }//end of switch
        }//end of while loop
        return selection;

	}//end of userOptions method



	/**
	 * Searches the files for a specific term
	 */
	public static void searchTerm()
	{
		boolean validInput = false;
		String term = "";
		while(!validInput)
		{
			System.out.print("Enter the term to search for: ");
			term = input.nextLine();
			// a simple check to see if multiple terms were entered
			// to keep the system from crashing
			String [] check = term.split(" ");
			if(check.length > 1)
			{
				System.out.println("You can only search for one term in this application.");
				validInput = false;
			}
			else
			{
				validInput = true;
			}
		}
		System.out.println("Please wait while your results are gathered...\n");
		String results = hadoopServ.search(term);
		if(results == null || results.equals(""))
		{
			System.out.println("The term did not exist within the files");
		}
		else
		{
			System.out.println("The results were generated");
			System.out.println(results);
		}
	}

	/**
	 * Gets the top-N results of the files
	 */
	 public static void topN()
	 {
		 int n = -1;
		 while (n <= 0) {
			 System.out.print("Enter a number for the top N terms you would like: ");
			 try {
				 n = input.nextInt();
			 } catch (NoSuchElementException e) {
				 System.out.println("You must enter a number greater than 0");
				 n= -1;
	         } catch (IllegalStateException e) {
	        	 System.out.println("You must enter a number greater than 0");
	        	 n = -1;
	         }
			 input.nextLine();
		 }
         System.out.println("Please wait while your results are gathered...\n");
		 ArrayList<String> results = hadoopServ.getTopN(n);
		 if(results == null || results.size() == 0)
		 {
			 System.out.println("Unable to get the Top-" + n + " results.");
		 }
		 else
		 {
			 System.out.println("The Top-" + n + " results have been generated:");
             for(int i = 0; i < n; i++)
             {
                System.out.println("\t" + (i+1) + ". " + results.get(i));
             }
		 }
	 }

	/**
	 * Allows a user to exit the system.
	 */
	public static void quit()
	{
		hadoopServ.disconnect();
		System.out.println("Goodbye!");

	}//end of the quit method

}//end of class
