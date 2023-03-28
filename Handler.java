import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;

public abstract class Handler implements Runnable {
	protected final Socket socket;
	protected final PrintWriter writer;
	protected final BufferedReader reader;
	
	public Handler(Socket socket) throws IOException {
		this.socket = socket;
		this.writer = new PrintWriter(socket.getOutputStream(), true);
		this.reader = new BufferedReader( new InputStreamReader(socke.getInputStream()));
	}
}
