

import java.net.ServerSocket;
import java.net.Socket;
import java.io.*;
import java.util.*;


public class HadoopServer extends Server {

	public static final int SERVER_PORT = 8765;

	public HadoopServer() {
		super(SERVER_PORT, "ALPHA");
	}

	public HadoopServer(int _port) {
		super(_port, "ALPHA");
	}

	public void start() {
		Scanner console = new Scanner(System.in);
		ObjectInputStream userStream;
		ObjectInputStream groupStream;

		//This block listens for connections and creates threads on new connections
		try
		{
			final ServerSocket serverSock = new ServerSocket(port);

			Socket sock = null;
			Thread thread = null;
			while(true)
			{
				sock = serverSock.accept();
				thread = new HadoopThread(sock, this);
				thread.start();
			}
		}
		catch(Exception e)
		{
			System.err.println("Error: " + e.getMessage());
			e.printStackTrace(System.err);
		}

	}

}
