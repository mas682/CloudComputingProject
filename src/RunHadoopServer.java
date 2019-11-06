/* Driver program for Hadoop Server */

public class RunHadoopServer {

	public static void main(String[] args) {
		if (args.length> 0) {
			try {
				HadoopServer server = new HadoopServer(Integer.parseInt(args[0]));
				server.start();
			}
			catch (NumberFormatException e) {
				System.out.printf("Enter a valid port number or pass no arguments to use the default port (%d)\n", HadoopServer.SERVER_PORT);
			}
		}
		else {
			HadoopServer server = new HadoopServer();
			server.start();
		}
	}
}
