package app_kvServer;

import java.net.BindException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.Iterator; 

import javax.naming.NameNotFoundException;

import java.io.IOException;
import java.util.Map;
import java.util.Collections;
import java.util.HashMap;

import logger.LogSetup;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import java.util.NoSuchElementException;
import java.util.concurrent.CountDownLatch;
import java.util.stream.*;

import java.security.*;
import java.io.*;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.Stat;


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
	private ArrayList<ClientConnection> connections;

	private ServerStateType serverStatus = ServerStateType.STOPPED;
	private boolean lockWrite = false;
	private ZooKeeper zookeeper;
	private Map<String, String[]> metaData = new HashMap<>();

	public KVServer(int port, int cacheSize, String strategy) {
		// TODO Auto-generated method stub
		this.port = port;
		this.cacheSize = cacheSize;
		this.strategy = strategy;

		if (strategy.equalsIgnoreCase("LRU")){
		  cache = Collections.synchronizedMap(new lru_cache(cacheSize));
		}
		else if (strategy.equalsIgnoreCase("FIFO")){
		  cache = Collections.synchronizedMap(new fifo_cache(cacheSize));
		}
		else if (strategy.equalsIgnoreCase("LFU")){
		  cache = Collections.synchronizedMap(new lfu_cache(cacheSize, 0.5f));
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
		// persistentDb.clearDb();
		logger.info("Clearing storage");
		if (cache != null){
			cache.clear();
		}
	}

	private boolean isRunning() {
        return this.running;
	}

	private void connectZookeeper(){
		final CountDownLatch connectedSignal = new CountDownLatch(1);

		try {
			this.zookeeper = new ZooKeeper("127.0.0.1:2181", 2000, new Watcher(){
				public void process (WatchedEvent we){
					if(we.getState() == KeeperState.SyncConnected) {
						connectedSignal.countDown();
					}
				}
			});
		} catch(IOException e) {
			logger.error("ERROR: Connection to Zookeeper failed");
		}

		try {
			connectedSignal.await();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	private void disconnectZookeeper() {
		try {
			if (zookeeper != null) {
				zookeeper.close();
			} 
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	private String getData(String path){
		try{
			byte[] b = zookeeper.getData(path, false,null);
			String data = new String(b, "UTF-8");
			return data;
		}catch(Exception e){
			System.out.println(e.getMessage()); 
		}
		return "";
	}

	public void loadMetadataFromZookeeper() {
		this.metaData.clear();

		try {
			String data = new String (zookeeper.getData("/server", false, null), "UTF-8");
			String[] token = data.split("\\s+");
			String key = "";

			for(int i = 0; i < token.length; i++) {
				if (i % 2 == 0) {
					// key
					key = token[i];
				} else {
					// value, store with key
					this.metaData.put(key, token[i].split("-"));
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
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

		// connect to zookeeper
		connectZookeeper();

		// load metadata from Zookeeper
		loadMetadataFromZookeeper();

		// setup cache strategy
		if (this.strategy.compareToIgnoreCase("LRU") == 0) {
			logger.info("Using cache strategy: LRU");
			cache = Collections.synchronizedMap(new lru_cache(cacheSize));
		} else if (this.strategy.compareToIgnoreCase("LFU") == 0) {
			logger.info("Using cache strategy: LFU");
			cache = Collections.synchronizedMap(new lfu_cache(cacheSize, 0.5f));
		} else if (this.strategy.compareToIgnoreCase("FIFO") == 0) {
			logger.info("Using cache strategy: FIFO");
			cache = Collections.synchronizedMap(new fifo_cache(cacheSize));
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
	                		new ClientConnection(client, this);
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
		clearStorage();
		running = false;
        try {
			logger.info("Closing server.");
			// @TODO: Loop through all connections and stop each thread
			disconnectZookeeper();
			serverSocket.close();
		} catch (IOException e) {
			logger.error("Error! " +
					"Unable to close socket on port: " + port, e);
		}
	}

	public ServerStateType getServerState() {
		return this.serverStatus;
	}

	public boolean isWriterLocked() {
		return this.lockWrite;
	}

	public String getMetaData() {
		String meta = "";
		for (Map.Entry <String,String[]> pair : this.metaData.entrySet()) {
			meta += pair.getKey() + " " + pair.getValue()[0] +  "-" +  pair.getValue()[1] + " ";
		}
		return meta;
	}

	public boolean isCorrectServer(String data) {
		boolean res = false;

		try {
			byte[] bytesOfMessage = data.getBytes("UTF-8");
			MessageDigest md = MessageDigest.getInstance("MD5");
			byte[] thedigest = md.digest(bytesOfMessage);
			StringBuffer sb = new StringBuffer();
			for (int i = 0; i < thedigest.length; ++i) {
				sb.append(Integer.toHexString((thedigest[i] & 0xFF) | 0x100).substring(1,3));
			}

			String hash = sb.toString();

			String lowerBound = metaData.get("127.0.0.1:" + Integer.toString(this.port))[0];
			String upperBound = metaData.get("127.0.0.1:" + Integer.toString(this.port))[1];

			if (hash.compareTo(lowerBound) > 0 && hash.compareTo(upperBound) <= 0) {
				res = true;
			} else if (lowerBound.compareTo(upperBound) >= 0 && (hash.compareTo(lowerBound) > 0 || hash.compareTo(upperBound) <= 0)) {
				res = true;
			} else if (lowerBound.compareTo(upperBound) == 0) {
				res = true;
			}
		} catch (Exception e) {
			e.printStackTrace();
		}

		return res;
	}

	

	public List<List<String>> moveData(String[] range) {

		String lowerBound = range[0];
		String upperBound = range[1];
		List<String> movedDataKeys = new ArrayList<String>();
		List<String> movedDataValues = new ArrayList<String>();

		// createSocket(server.split(":")[0], Integer.parseInt(server.split(":")[1]));

		Iterator it = this.cache.entrySet().iterator();
		while (it.hasNext()) {
			Map.Entry<String, String> pair = (Map.Entry<String, String>)it.next();

			String key = pair.getKey();
			String hash = "";
			try {
				// convert to hash
				byte[] bytesOfMessage = key.getBytes("UTF-8");
				MessageDigest md = MessageDigest.getInstance("MD5");
				byte[] thedigest = md.digest(bytesOfMessage);
				StringBuffer sb = new StringBuffer();
				for (int i = 0; i < thedigest.length; ++i) {
					sb.append(Integer.toHexString((thedigest[i] & 0xFF) | 0x100).substring(1,3));
				}

				hash = sb.toString();
			} catch (Exception e) {
				e.printStackTrace();
			}

			boolean res = false;
			// Does key fall in lowerBound and upperBound range?
			if (hash.compareTo(lowerBound) > 0 && hash.compareTo(upperBound) <= 0) {
				res = true;
			} else if (lowerBound.compareTo(upperBound) >= 0 && (hash.compareTo(lowerBound) > 0 || hash.compareTo(upperBound) <= 0)) {
				res = true;
			}

			if(res) {
				movedDataKeys.add(key);
				movedDataValues.add(pair.getValue());
			}
			it.remove(); // avoids a ConcurrentModificationException
		}

		for (String s : movedDataKeys) {
            cache.remove(s);
		}
		
		List<List<String>> listOfLists = new ArrayList<List<String>>();
		listOfLists.add(movedDataKeys);
		listOfLists.add(movedDataValues);

		return listOfLists;
	}

	public void start() {
		this.serverStatus = ServerStateType.STARTED;
	}

	public void stop() {
		this.serverStatus = ServerStateType.STOPPED;
	}

	public void shutdown() {
		close();
	}

	public void lockWrite() {
		this.lockWrite = true;		
	}

	public void unlockWrite() {
		this.lockWrite = false;
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
