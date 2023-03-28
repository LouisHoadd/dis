import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.net.Socket;
import java.net.UnknownHostException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.io.InputStreamReader;

public class Dstore {
	private static Integer port, cport, timeout;
	private static String folder;
	
	//logger
	private static final SystemLogger logger = SystemLogger.getSystemLogger(Dstore.class);
	
	//handler
	private static Handler handler;
	
	//task thread pool
	private static ExecutorService taskPool = Executors.newCachedThreadPool();
	
	public static void main(String[] args) {
		//try parsing arguments
		try{  
			port = Integer.parseInt(args[0]);
			cport = Integer.parseInt(args[1]);
			timeout = Integer.parseInt(args[2];
			folder = args[3];
		} catch (Exception e){
			logger.err("Arguments are not in the correct format, should be: java Dstore port cport timeout file_folder");
			return;
		}
		
		//try clearing contents of the given folder
		try{
			Path folderPath = Paths.get(folder);
			logger.info("Clearing the contents of the folder: " + folderPath.toAbsolutePath().toString());
			if (Files.exists(folderPath)){
				Files.walk(folderPath).sorted(Comparator.reverseOrder()).map(Path::toFile).forEach(File::delete);
			}
			Files.createDirectory(folderPath);
		} catch (IOException e){
			logger.err("Error clearing contents of folder: '" + folder + "''");
			return;
		}
		
		//try to establish connection to the controller
		try{ 
			establishConnection();
		} catch (Exception e){
			logger.err("Error: could not connect to the controller");
			return;
		}
		
		obtainConnections(); //obtains all connections from Clients/other Dstores
	}
	
	private static void establishConnection() throws UnknownHostException, IOException {
		Socket socket = new Socket(Inet.address.getLocalHost(), cport);
		contHandler = new ContHandler(socket);
		new Thread(contHandler).start();
	}
	
	private static void obtainConnections(){
		logger.info("Obtaining and accepting connections...");
		try (ServerSocket serverSocket = new ServerSocket(port)){
			for (;;){
				try{
					Socket socket = serverSocket.accept();
					taskPool.execute(new CliHandler(socket));
				} catch (IOException e) { logger.err("Error creating Client"); }
			}
		} catch (IOException e) {
			logger.err("Error obatining connections - client connection not longer possible");
		}
	}
	
	private static class CliHandler extends Handler {
		
		//logger
		private static final SystemLogger logger = SystemLogger.getSystemLogger(CliHandler.class);
		
		//constructor method
		public CliHandler(Socket socket) throws IOException {
			super(socket);
			
			socket.setSoTimeOut(timeout);
			logger.info(New Client has been created);
		}
		
		//runs thread
		public void run(){
			try (socket, writer, reader){
				String message;
				if ((message = reader.readLine()) != null) handleMessage(message);
			} catch (Exception e) { } finally {
				logger.info("Client disconnected");
			}
		}
		
		//handles Client message
		private void handleMessage(String message){
			try {
				var arguments = message.split(" ");
				switch (getOp(message)) {
					case Protocol.STORE	-> store(arguments[1], Integer.parseInt(arguments[2]), false);
					case Protocol.LOAD_DATA	-> load(arguments[1]);
					case Protocol.REBALANCE_STORE -> store(arguments[1], Integer.parseInt(arguments[2]), true);
					default -> logger.err("Client message is not in the correct format: '" + message + "'");
				}
			} catch (Exception e) {
				logger.err("Error: could not successfully handle Client message: '" + message + "'");
			}
		}
		
		//load operation
		private void load(String fName){
			logger.info("Load operation has commenced for file: '" + fName + "'...'");
			try {
				Path path = Paths.get(folder, fName);
				if (!Files.exists(path)) return; 
				byte[] data = Files.readAllBytes(path);
				
				socket.getOutputStream().write(data);
				logger.info("Load operation of file: '" + fName + "' is complete ");
			}catch (Exception e){
				logger.err("Error: could not load file: '" + fName + "'");
			}
		}
		
		//store operation
		private void store(String fName, Integer fSize, Boolean rebalance){
			logger.info("Store operation has commenced for file: '" + fName + "'...");
			try{
				byte[] data = new byte[fSize];
				socket.getInputStream().readNBytes(data, 0, fSize);
				
				Path path = Paths.get(folder, fName);
				Files.write(path, data);
				
				if(!rebalance) contHandler.sendMessage(Protocol.STORE_ACK + " " + fName);
				logger.info("Store operation of file: '" + fName + "' is complete");
				
			} catch (Exception e){
				logger.err("Error: could not store file: '" + fName + "'");
			}
		}
	}
	
	private static class ContHandler extends Handler {
		
		//logger
		private static final SystemLogger logger = SystemLogger.getSystemLogger(ContHandler.class);
		
		///constructor method
		public Conthandler(Socket socket) throws IOException {
			super (socket);
			
			writer.println("JOIN " + port);
			logger.info("Connected to Controller");
		}
		
		//sends message
		public void sendMessage(){
			writer.println(message);
		}
		
		//runs thread
		public void run() {
			try (socket; reader; writer) {
				String message;
				while ((message = reader.readLine()) != null) handleMessage(message);
			} catch (Exception e) { } finally {
				logger.info("Connection to server offline");
				System.exit(0);
			}
		}
		
		//handles Controller message
		private void handleMessage(String message) {
			try {
				var arguments = message.split(" ");
				switch (getOp(message)) {
					case Protocol.LIST	-> list();
					case Protocol.REMOVE	-> remove(arguments[1]);
					case Protocol.REBALANCE	-> {
						var rebalance = Arrays.asList(arguments);
						HashMap<String, Set<Integer>> filesToBeSent = new HashMap<>();
						ArrayList<String> filesToBeDeleted = new ArrayList<>();
						var iterator = new rebalance.iterator();
						iterator.next();
						
						var noFiles = Integer.parseInt(iterator.next());
						IntStream.range(0, noFiles).forEach(i -> {
							var fName = iterator.next();
							var noPorts = Integer.parseInt(iter.next());
							Set<Integer> ports = new HashSet<>();
							IntStream.range(0, noPorts).forEach(j -> ports.add(iterator.next())));
							filesToBeSent.put(fName, filesToBeDeleted);
						});
						var noFilesToBeDeleted = Integer.parseInt(iterator.next());
						IntStream.range(0, noFilesToBeDeleted).forEach(i -> filesToBeDeleted.add(iterator.next()));
						rebalance(filesToBeSent, filesToBeDeleted);
						
					}
					default -> logger.err("Controller message is not in the correct format: '" + message + "'");
				}
				
			} catch (Exception e) {
				logger.err("Error: could not successfully handle Controller message: '" + message + "'");
			}
			
		}
		
		//list operation
		private void list() {
			logger.info("List operation has commenced...");
			try (Stream<Path> stream = Files.list(Paths.get(folder))) {
				var files = String.join(" ", stream.filter(file -> !Files.isDirectory(file)).map(Path::getFileName).map(Path::toString).collect(Collectors.toList()));
				writer.println((files.length() == 0) ? Protocol.LIST : Protocol. LIST + " " + files);
				logger.info("List operation is complete"):
			} catch (){
				logger.err("Error: could not list files");
			}
		
		}
		
		//remove operation
		private void remove(String fName) {
			logger.info("Remove operation has commenced for: '" + fName + "'...");
			Path path = Paths.get(folder, fName);
			try {
				if(Files.deleteIfExists(path)) writer.println(Protocol.REMOVE_ACK + " " + fName);
				else writer.println(Protocol.ERROR_FILE_DOES_NOT_EXIST + " " + fName);
				logger.info("File: '" + fName + "' removal complete");
			} catch (IOException e){
				logger.err("Error: could not remove file: " + fName);
			}
		}
		
		//rebalance operation
		private void rebalance(HashMap<String, Set<Integer>> filesToBeSent, ArrayList<String> filesToBeDeleted) {
			logger.info("Rebalance operation has commenced...");
			Set<SendFile> allFilesToBeSent = new HashSet<>();
			
			filesToBeSent.entrySet().stream().forEach(e -> {
				Integer fSize;
				try { 
					fSize = (int) Files.size(Paths.get(folder, e.getKey()));
				} catch (Exception e1) {return;}
				e.getValue().forEach(p -> allFilesToBeSent.add(new SendFile(e.getKey(), p, fSize)));
			});
			try {
				var outcome = taskPool.invokeAll(allFilesToBeSent);
				var successful = outcome.stream().allMatch(e -> {
					try { return e.get();
					} catch (Exception ex) {return false;}
				});
				if (!successful) return;
			} catch (Exception e1) {return;}
			
			filesToBeDeleted.forEach(f -> {
				try{
					logger.info("Deleting file '" + f + "'");
					Files.deleteIfExists(Paths.get(folder, f));
				} catch (Exception e) {return;}
			});
			writer.println(Protocol.REBALANCE_COMPLETE);
			logger.info("Rebalance operation completed successfully");
		}
	
		//handles file sending 
		private static class SendFile implements Callable<Boolean> {
			private final String fName;
			private final Integer port;
			private final Integer fSize;
		
			public SendFile(String fname; Integer port, Integer fSize){
				this.fName = fName;
				this.port = port;
				this.fSize = fSize;
			}
		
			@Override
			public Boolean call(){
				try (
					Socket socket = new Socket(InetAddress.getLocalHost(), port);
					PrintWriter writer = new PrintWriter(socket.getOutputStream(), true);
					BufferedReader reader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
				) {
					logger.info("Sending '" + fName + "' to: " + port);
					socket.setSoTimeout(timeout);
				
					writer.println(Protocol.REBALANCE_STORE + " " + fName + " " + fSize);
					if (reader.readLine().equals(Protocol.ACK)){
						byte[] data = Files.readAllBytes(Paths.get(folder, fName));
						socket.getOutputStream().write(data);
						return true;
					}
				} catch (Exception e) {
					logger.err("Error sending '" + fName + "' to: " + port);
				}
				return false;
			}
		}
	}
	
	
	//gets the operation
	private static String getOp(String message){
		return message.split(" ", 2)[0];
	}
}
