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
	// @TODO: figure out what is this ClientConnection?
	private ArrayList<ClientConnection> connections;
	// @TODO: initialize cache and persistent storage
	private ServerStateType serverStatus = ServerStateType.STOPPED;
	private boolean lockWrite = false;
	private ZooKeeper zookeeper;
	private Map<String, String[]> metaData = new HashMap<>();
	private static final int BUFFER_SIZE = 1024;
	private static final int DROP_SIZE = 128 * BUFFER_SIZE;

	private Socket socket;
	private OutputStream output;
	private InputStream input;

	public static final String LOCAL_HOST = "127.0.0.1";
	public static final String ZK_HOST = LOCAL_HOST;
	public static final String ZK_PORT = "2181";
	public static final String ZK_CONN = ZK_HOST + ":" + ZK_PORT;
	public static final int ZK_TIMEOUT = 2000;

	
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
		String data = "";
		try{
			byte[] b = zookeeper.getData(path, false,null);
			data = new String(b, "UTF-8");

		}catch(Exception e){
			System.out.println(e.getMessage()); 
		}
		return data;
	}

	public void loadMetadataFromZookeeper() {
		// this.metaData.clear();
		// try {
		// 	String data = new String (zookeeper.getData("/server", false, null), "UTF-8");
		// 	String[] token = data.split("\\s+");
		// 	String key = "";

		// 	for(int i = 0; i < token.length; i++) {
		// 		if (i % 2 == 0) {
		// 			// key
		// 			key = token[i];
		// 		} else {
		// 			// value, store with key
		// 			this.metaData.put(key, token[i].split("-"));
		// 		}
		// 	}
		// } catch (Exception e) {
		// 	e.printStackTrace();
		// }
		String data = getData("/server");
		String[] dataPieces = data.split("\\s+");
		this.metaData.clear(); 
		for (int i = 0; i < dataPieces.length; i +=2){

			String[] value = dataPieces[i+1].split("-");
			this.metaData.put(dataPieces[i],value);
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

	public String convertToMD5(String md5) {
		try {
			MessageDigest md = MessageDigest.getInstance("MD5");
			byte[] array = md.digest(md5.getBytes());
			StringBuffer sb = new StringBuffer();
			for (int i = 0; i < array.length; ++i) {
				sb.append(Integer.toHexString((array[i] & 0xFF) | 0x100).substring(1,3));
			}
			return sb.toString();
		} catch (NoSuchAlgorithmException e) {
		}	      
		return null;
	}  

	public boolean isCorrectServer(String data) {
		// boolean res = false;

		// try {
		// 	byte[] bytesOfMessage = data.getBytes("UTF-8");
		// 	MessageDigest md = MessageDigest.getInstance("MD5");
		// 	byte[] thedigest = md.digest(bytesOfMessage);
		// 	//@TODO: I dont convert space to 0 or something

		// 	String hash = thedigest.toString();
		// 	String lowerBound = metaData.get("127.0.0.1:" + Integer.toString(this.port))[0];
		// 	String upperBound = metaData.get("127.0.0.1:" + Integer.toString(this.port))[1];

		// 	if (hash.compareTo(lowerBound) > 0 && hash.compareTo(upperBound) <= 0) {
		// 		res = true;
		// 	} else if (lowerBound.compareTo(upperBound) >= 0 && (hash.compareTo(lowerBound) > 0 || hash.compareTo(upperBound) <= 0)) {
		// 		res = true;
		// 	} else if (lowerBound.compareTo(upperBound) == 0) {
		// 		res = true;
		// 	}
		// } catch (Exception e) {
		// 	e.printStackTrace();
		// }

		// return res;

		String keyHash = convertToMD5(data);

		String[] hash_range = metaData.get(LOCAL_HOST+":"+Integer.toString(this.port));
		
		String start = hash_range[0];
		String end = hash_range[1];

		//wrap around case: start is bigger than end
		if (start.compareTo(end) >= 0){
			if( keyHash.compareTo(start) >0 || keyHash.compareTo(end) <= 0 ){
				return  true;
			}			
		}
		else {

			//if start and end are the same => only 1 server
			if (start.compareTo(end) == 0) {
				return true;
			}	      
			//			System.out.println ("non wrap around case");
			if( keyHash.compareTo(start) >0 && keyHash.compareTo(end) <= 0 ){
				return  true;
			}
		}
		return false;
	}

	public void moveData(String[] range, String server) {
		String start = range[0];
		String end = range[1];	
		String[] host_port = server.split(":");
		String host = host_port[0];
		int port = Integer.parseInt(host_port[1]);

		ArrayList<String> keys_to_delete = new ArrayList<String>();

		//create socket to talk to server to transfer data to
		try {
			socket =  new Socket (host, port);
			output = socket.getOutputStream();
			input = socket.getInputStream();
		} catch(Exception e){
			logger.error("Connection could not be established!");
		}

		//loop through cache and transfer key,value
		//delete from current server
		for (Map.Entry <String, String> pair : this.cache.entrySet()){
			String key = pair.getKey();
			String keyHash = convertToMD5(key);
			// String keyHash = "";

			// try {
			// 	// convert to hash
			// 	byte[] bytesOfMessage = key.getBytes("UTF-8");
			// 	MessageDigest md = MessageDigest.getInstance("MD5");
			// 	byte[] thedigest = md.digest(bytesOfMessage);
			// 	//@TODO: I dont convert space to 0 or something

			// 	keyHash = thedigest.toString();
			// } catch (Exception e) {
			// 	e.printStackTrace();
			// }

			boolean move_key=false;

			//Check if this key falls in the range we want to move
			if (start.compareTo(end) > 0){
				if( keyHash.compareTo(start) >0 || keyHash.compareTo(end) <= 0 ){
					move_key=true;
				}			
			}
			else {
				if( keyHash.compareTo(start) >0 && keyHash.compareTo(end) <= 0 ){
					move_key=true;
				}
			}

			if (!move_key){
				continue;
			}

			String value = pair.getValue();

			//delete from current cache
			keys_to_delete.add(key);
			//send to new server
			try{
				sendMessage(new TextMessage("transfer " + key + " " + value));
				TextMessage latestMsg= receiveMessage();
			}
			catch(Exception e){
				System.out.println(e.getMessage());
			}
		}

		for (int i = 0; i < keys_to_delete.size(); i++){
			cache.remove(keys_to_delete.get(i));
		}

		//close the socket
		try{
			input.close();
			output.close();
			socket.close();
		}catch(IOException e){
			e.printStackTrace();
		}

		socket = null; 
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

	/**
	 * Method sends a TextMessage using this socket.
	 * @param msg the message that is to be sent.
	 * @throws IOException some I/O error regarding the output stream 
	 */
	public void sendMessage(TextMessage msg) throws IOException {
		byte[] msgBytes = msg.getMsgBytes();
		output.write(msgBytes, 0, msgBytes.length);
		output.flush();
		logger.info("SEND \t<" 
				+ socket.getInetAddress().getHostAddress() + ":" 
				+ socket.getPort() + ">: '" 
				+ msg.getMsg() +"'");
	}

	private TextMessage receiveMessage() throws IOException {
		int index = 0;
		byte[] msgBytes = null, tmp = null;
		byte[] bufferBytes = new byte[BUFFER_SIZE];

		/* read first char from stream */
		byte read = (byte) input.read();	
		boolean reading = true;

		while(/*read != 13  && */ read != 10 && read !=-1 && reading) {/* CR, LF, error */
			/* if buffer filled, copy to msg array */
			if(index == BUFFER_SIZE) {
				if(msgBytes == null){
					tmp = new byte[BUFFER_SIZE];
					System.arraycopy(bufferBytes, 0, tmp, 0, BUFFER_SIZE);
				} else {
					tmp = new byte[msgBytes.length + BUFFER_SIZE];
					System.arraycopy(msgBytes, 0, tmp, 0, msgBytes.length);
					System.arraycopy(bufferBytes, 0, tmp, msgBytes.length,
							BUFFER_SIZE);
				}

				msgBytes = tmp;
				bufferBytes = new byte[BUFFER_SIZE];
				index = 0;
			} 

			/* only read valid characters, i.e. letters and constants */
			bufferBytes[index] = read;
			index++;

			/* stop reading is DROP_SIZE is reached */
			if(msgBytes != null && msgBytes.length + index >= DROP_SIZE) {
				reading = false;
			}

			/* read next char from stream */
			read = (byte) input.read();
		}

		if(msgBytes == null){
			tmp = new byte[index];
			System.arraycopy(bufferBytes, 0, tmp, 0, index);
		} else {
			tmp = new byte[msgBytes.length + index];
			System.arraycopy(msgBytes, 0, tmp, 0, msgBytes.length);
			System.arraycopy(bufferBytes, 0, tmp, msgBytes.length, index);
		}

		msgBytes = tmp;

		/* build final String */
		TextMessage msg = new TextMessage(msgBytes);
		logger.info("RECEIVE \t<" 
				+ socket.getInetAddress().getHostAddress() + ":" 
				+ socket.getPort() + ">: '" 
				+ msg.getMsg() + "'");
		return msg;
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
