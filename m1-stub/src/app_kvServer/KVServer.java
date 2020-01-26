package app_kvServer;

import java.net.BindException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;

import javax.naming.NameNotFoundException;

import java.io.IOException;
import java.util.Map;
import java.util.Collections;
import logger.LogSetup;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import java.util.NoSuchElementException;
import java.util.stream.*;


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
	private static KVServer server;
        private Map<String,String>cache;
	// @TODO: figure out what is this ClientConnection?
	private ArrayList<ClientConnection> connections;
	// @TODO: initialize cache and persistent storage

	
	public KVServer(int port, int cacheSize, String strategy) {
		// TODO Auto-generated method stub
		this.port = port;
		this.cacheSize = cacheSize;
      		this.strategy = strategy;
	        if (strategy.equalsIgnoreCase("LRU")){
		  cache=Collections.synchronizedMap(new lru_cache(cacheSize));
	       }
	       else if (strategy.equalsIgnoreCase("FIFO")){
		  cache=Collections.synchronizedMap(new fifo_cache(cacheSize));

	       }
	       else if (strategy.equalsIgnoreCase("LFU")){
		  cache=Collections.synchronizedMap(new lfu_cache(cacheSize, 0.5f));
	       }
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
		if(key.isEmpty() || key == null) return false;

		// System.out.println("Finding key");
		String value = persistentDb.find(key);

		if (value == null)  return false;
		// System.out.println("Found key");
		return true;
	}

	@Override
    public boolean inCache(String key){
		// TODO Auto-generated method stub

		// call getKV in cache and return true if found
		 if (cache != null) {
		 	if (cache.containsKey(key)) return true;
		 }
		return false;
	}

	@Override
    public String getKV(String key) throws Exception{
		// TODO Auto-generated method stub
		
		// try to get value in cache, if not found, try to get value
		// in persistent storage

		String value = "";
		if (inCache(key)) {
		      value = cache.get(key);
		} else if (inStorage(key)) {
			value = persistentDb.find(key);
		} else {
			throw new NameNotFoundException();
		}
		return value;
	}

	@Override
    public void putKV(String key, String value) throws Exception{
		// TODO Auto-generated method stub
		
		// put in persistent storage and in cache based on policy
		// System.out.println("In putKV : " + key + " : " + value);
		if (inCache(key) && value.equals("") && value.equals("null") || value == null) {
			 cache.remove(key);
		} else {
			persistentDb.add(key, value);
			cache.put(key, value);
		}
	}

	@Override
    public void clearCache(){
		// TODO Auto-generated method stub
		logger.info("Clearing cache");
		if (cache != null){
		 	cache.clear();
		}
	}

	@Override
    public void clearStorage(){
		// TODO Auto-generated method stub
		persistentDb.clearDb();
		logger.info("Clearing storage");
		if (cache != null){
			cache.clear();
		}
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

		// initialize persistent storage
		persistentDb.initializeDb();

		// setup cache strategy
		if (this.strategy.compareToIgnoreCase("LRU") == 0) {
			logger.info("Using cache strategy: LRU");
			// @TODO: setup LRU cache
		} else if (this.strategy.compareToIgnoreCase("LFU") == 0) {
			logger.info("Using cache strategy: LFU");
			// @TODO: setup LFU cache
		} else if (this.strategy.compareToIgnoreCase("FIFO") == 0) {
			logger.info("Using cache strategy: FIFO");
			// @TODO: setup FIFO cache
		} else {
			// no cache strategy
			logger.info("No cache strategy specified.");
		}
	}

	@Override
    public void run(){
		// TODO Auto-generated method stub

		running = initializeServer();
		setupServer();

		if (serverSocket != null) {
	        while(isRunning()){
	            try {
	                Socket client = serverSocket.accept();                
	                ClientConnection connection = 
	                		new ClientConnection(client, server);
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
				server = new KVServer(Integer.parseInt(args[0]), Integer.parseInt(args[1]), args[2]);

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
