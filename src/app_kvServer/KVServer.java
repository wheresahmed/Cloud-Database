package app_kvServer;

import java.net.BindException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.io.IOException;

import logger.LogSetup;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;


public class KVServer implements IKVServer {
	/**
	 * Start KV Server at given port
	 * @param port given port for storage server to operate
	 * @param cacheSize specifies how many key-value pairs the server is allowed
	 *           to keep in-memory
	 * @param strategy specifies the cache replacement strategy in case the cache
	 *           is full and there is a GET- or PUT-request on a key that is
	 *           currently not contained in the cache. Options are "FIFO", "LRU",
	 *           and "LFU".
	 */

	private static Logger logger = Logger.getRootLogger();
	
	private int port;
	private int cacheSize;
	private String strategy;
    private ServerSocket serverSocket;
	private boolean running;

	// @TODO: figure out what is this ClientConnection?
	private ArrayList<ClientConnection> connections;
	// @TODO: initialize cache and persistent storage

	
	public KVServer(int port, int cacheSize, String strategy) {
		// TODO Auto-generated method stub
		this.port = port;
		this.cacheSize = cacheSize;
		this.strategy = strategy;
	}
	
	@Override
	public int getPort(){
		// TODO Auto-generated method stub
		return this.port;
	}

	@Override
    public String getHostname(){
		// TODO Auto-generated method stub
		if (serverSocket != null) {
			return serverSocket.getInetAddress().getHostName();
		}
		logger.error("Error: Host is unknown!\n");
		return null;
	}

	@Override
    public CacheStrategy getCacheStrategy(){
		// TODO Auto-generated method stub
		if (strategy.contains("LRU")) {
			return IKVServer.CacheStrategy.LRU;
		} else if (strategy.contains("LFU")) {
			return IKVServer.CacheStrategy.LFU;
		} else if (strategy.contains("FIFO")) {
			return IKVServer.CacheStrategy.FIFO;
		} else {
			return IKVServer.CacheStrategy.None;
		}
	}

	@Override
    public int getCacheSize(){
		// TODO Auto-generated method stub
		return this.cacheSize;
	}

	@Override
    public boolean inStorage(String key){
		// TODO Auto-generated method stub
		
		// call getKV in persistent storage and return true if found
		return false;
	}

	@Override
    public boolean inCache(String key){
		// TODO Auto-generated method stub

		// call getKV in cache and return true if found
		return false;
	}

	@Override
    public String getKV(String key) throws Exception{
		// TODO Auto-generated method stub
		
		// try to get value in cache, if not found, try to get value
		// in persistent storage
		return "";
	}

	@Override
    public void putKV(String key, String value) throws Exception{
		// TODO Auto-generated method stub
		
		// put in persistent storage and in cache based on policy
	}

	@Override
    public void clearCache(){
		// TODO Auto-generated method stub
		logger.info("Clearing cache");
		// if (cache != null){
		// 	cache.clear();
		// }
	}

	@Override
    public void clearStorage(){
		// TODO Auto-generated method stub
		// try {
		// 	pStore.clear();
		// 	logger.info("Clearing storage");
		// 	if (cache != null){
		// 		cache.clear();
		// 	}
		// } catch (IOException e) {
        //     logger.error("Error: Exception in clear storage");
        // }
	}

	private boolean isRunning() {
        return this.running;
	}
	
	private boolean initializeServer() {
    	logger.info("Initialize server ...");
    	try {
            serverSocket = new ServerSocket(port);
            logger.info("Server listening on port: " 
            		+ serverSocket.getLocalPort()); 
			
            return true;
        
        } catch (IOException e) {
        	logger.error("Error! Cannot open server socket:");
            if(e instanceof BindException){
            	logger.error("Port " + port + " is already bound!");
            }
            return false;
        }
	}
	
	private void setupServer() {
		logger.info("Setting up the server...");
		connections = new ArrayList<ClientConnection>();

		// setup cache strategy
		if (this.strategy.contains("LRU")) {
			logger.info("Using cache strategy: LRU");
			// @TODO: setup LRU cache
		} else if (this.strategy.contains("LFU")) {
			logger.info("Using cache strategy: LFU");
			// @TODO: setup LFU cache
		} else if (this.strategy.contains("FIFO")) {
			logger.info("Using cache strategy: FIFO");
			// @TODO: setup FIFO cache
		} else {
			// no cache strategy
			logger.info("No cache strategy specified.");
		}
		
		// @TODO: initialize persistent storage
	}

	@Override
    public void run(){
		// TODO Auto-generated method stub

		running = initializeServer();
		//setupServer();

		if (serverSocket != null) {
	        while(isRunning()){
	            try {
	                Socket client = serverSocket.accept();                
	                ClientConnection connection = 
	                		new ClientConnection(client);
					new Thread(connection).start();
					
					this.connections.add(connection);
	                
	                logger.info("Connected to " 
	                		+ client.getInetAddress().getHostName() 
	                		+  " on port " + client.getPort());
	            } catch (IOException e) {
	            	logger.error("Error! " +
	            			"Unable to establish connection. \n", e);
	            }
	        }
		}
		logger.info("Server stopped!");
	}

	@Override
    public void kill(){
		// TODO Auto-generated method stub
		running = false;
        try {
			logger.info("Killing server.");
			// @TODO: Loop through all connections and stop each thread
			serverSocket.close();
		} catch (IOException e) {
			logger.error("Error! " +
					"Unable to close socket on port: " + port, e);
		}
	}

	@Override
    public void close(){
		// TODO Auto-generated method stub
		running = false;
        try {
			logger.info("Closing server.");
			// @TODO: Loop through all connections and stop each thread
			serverSocket.close();
		} catch (IOException e) {
			logger.error("Error! " +
					"Unable to close socket on port: " + port, e);
		}
	}

	public static void main(String[] args) {
		try {
			new LogSetup("logs/server.log", Level.ALL);
			if(args.length != 3) {
				System.out.println("Error! Invalid number of arguments!");
				System.out.println("Usage: Server <port>!");
			} else {
				// KVServer(port, cacheSize, cache replacement strategy)
				KVServer server = new KVServer(Integer.parseInt(args[0]), Integer.parseInt(args[1]), args[2]);

				new TServer(server).start();
			}
		} catch (IOException e) {
			System.out.println("Error! Unable to initialize logger!");
			e.printStackTrace();
			System.exit(1);
		} catch (NumberFormatException nfe) {
			System.out.println("Error! Invalid argument <port>! Not a number!");
			System.out.println("Usage: Server <port>!");
			System.exit(1);
		}
	}
}
