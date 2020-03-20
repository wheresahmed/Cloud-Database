package app_kvServer;

import java.io.InputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;

import org.apache.log4j.*;

import java.util.ArrayList;
import java.util.List;

/**
 * Represents a connection end point for a particular client that is 
 * connected to the server. This class is responsible for message reception 
 * and sending. 
 * The class also implements the echo functionality. Thus whenever a message 
 * is received it is going to be echoed back to the client.
 */
public class ClientConnection implements Runnable {

	private static Logger logger = Logger.getRootLogger();
	
	private boolean isOpen;
	private static final int BUFFER_SIZE = 1024;
	private static final int DROP_SIZE = 128 * BUFFER_SIZE;
	
	private Socket clientSocket;
	private InputStream input;
	private OutputStream output;

	private Socket socket;
	private OutputStream outputS;
	private InputStream inputS;

	private KVServer server = null;
	
	/**
	 * Constructs a new CientConnection object for a given TCP socket.
	 * @param clientSocket the Socket object for the client connection.
	 */
	public ClientConnection(Socket clientSocket, KVServer server) {
		this.clientSocket = clientSocket;
		this.isOpen = true;
		this.server = server;
	}
	
	/**
	 * Initializes and starts the client connection. 
	 * Loops until the connection is closed or aborted by the client.
	 */
	public void run() {
		try {
			output = clientSocket.getOutputStream();
			input = clientSocket.getInputStream();
		
			while(isOpen) {
				try {
					TextMessage latestMsg = receiveMessage(true);
					String msg = "";

					// parse msg and take action accordingly
					String[] token = null;
					token = latestMsg.getMsg().trim().split("\\s+");

					if (token[0].equalsIgnoreCase("transfer")) {
						if(token.length >= 3){
							if (server.inCache(token[1])){
								msg = "TRANSFER_UPDATE < ";
							}else{
								msg = "TRANSFER_SUCCESS < ";
							}

							String value = "";

							for (int i = 2; i < token.length; i++) {
								value += token[i] + " ";
							}

							value.trim();

							try {
								server.putKV(token[1], value);
							} catch (Exception e) {
								logger.error("TRANSFER ERROR! Error in PUT function");
							}

							msg += token[1] + " , " + value + " >";
						}else {
							msg = "TRANSFER ERROR: Invalid format";
						}
					} else if (token[0].equalsIgnoreCase("start")) {
						server.start();
						msg = "Server is started";
					} else if (token[0].equalsIgnoreCase("stop")) {
						server.stop();
						msg = "Server is stopped";
					} else if (token[0].equalsIgnoreCase("shutdown")) {
						server.shutdown();
						msg = "Server is shutdown";
					} else if (token[0].equalsIgnoreCase("lockWrite")) {
						createSocket(token[1].split(":")[0], Integer.parseInt(token[1].split(":")[1]));
						List<List<String>> movedData = server.moveData(token[2].split("-"));
						transferData(movedData);
						try {
							if (socket != null) {
								inputS.close();
								outputS.close();
								socket.close();
							}
						} catch (IOException ioe) {
							logger.error("Error! Unable to tear down connection!", ioe);
						}
						socket = null;
						server.lockWrite();
						msg = "Locked write at " + server.getPort() + " and moved data...";
					} else if (token[0].equalsIgnoreCase("unlockWrite")) {
						server.unlockWrite();
						msg = "Unlocked write at " + server.getPort() + "...";
					} else if (token[0].equalsIgnoreCase("update_metadata")) {
						server.loadMetadataFromZookeeper();
						msg = "Updating metadata on " + server.getPort();
					} else if (server.getServerState() == ServerStateType.STOPPED) {
						msg = "SERVER_STOPPED";
					} else if (token[0].equalsIgnoreCase("get")) {
						logger.info("Message received with GET request."); 
						boolean append = true;
						String value = "";
						
						if (token.length == 2 && server.isCorrectServer(token[1]) && server.inStorage(token[1])) {
							try {
								value = server.getKV(token[1]);
								msg = "GET_SUCCESS < ";
							} catch (Exception e) {
								logger.error("GET_ERROR! Could not find key in DB.");
							}
						} else if (token.length != 2) {
							msg = "GET_ERROR < ";
						} else if (!server.isCorrectServer(token[1])) {
							msg = "SERVER_NOT_RESPONSIBLE " + server.getMetaData();
							append = false;
						} else {
							msg = "GET_ERROR < ";
						}

						if (append) {
							msg += token[1] + ", " + value + " >";
						}
					} else if (server.isWriterLocked()) {
						msg = "SERVER_WRITE_LOCK";
					} else if(token[0].equalsIgnoreCase("put")) {
						logger.info("Message received with PUT request.");

						if (!(token.length >= 2)) {
							msg = "INVALID_PUT";
						} else if (server.isCorrectServer(token[1])) {
							if ((token.length == 3 && token[2].equalsIgnoreCase("null")) || token.length == 2) {
								// delete operation
								if (server.inStorage(token[1])) {
									msg = "DELETE_SUCCESS < "; 
								} else {
									msg = "DELETE_ERROR < "; 
								}
							} else if (token.length > 2) {
								if (server.inStorage(token[1])) {
									msg = "PUT_UPDATE < ";
								} else {
									msg = "PUT_SUCCESS < ";
								}
							}

							String value = "";

							for (int i = 2; i < token.length; i++) {
								value += token[i] + " ";
							}

							value.trim();

							try {
								server.putKV(token[1], value);
							} catch (Exception e) {
								logger.error("PUT ERROR! Error in PUT function");
							}

							msg += token[1] + " , " + value + " >";
						} else {
							msg = "SERVER_NOT_RESPONSIBLE " + server.getMetaData();
						}
					} else {
						msg = latestMsg.toString();
					} 

					sendMessage(new TextMessage(msg), true);
					
				/* connection either terminated by the client or lost due to 
				 * network problems*/	
				} catch (IOException ioe) {
					logger.error("Error! Connection lost!");
					isOpen = false;
				}				
			}
			
		} catch (IOException ioe) {
			logger.error("Error! Connection could not be established!", ioe);
			
		} finally {
			
			try {
				if (clientSocket != null) {
					input.close();
					output.close();
					clientSocket.close();
				}
			} catch (IOException ioe) {
				logger.error("Error! Unable to tear down connection!", ioe);
			}
		}
	}

	public void createSocket(String host, int port) {
		try {
			socket = new Socket(host, port);
			outputS = socket.getOutputStream();
			inputS = socket.getInputStream();
		} catch(Exception e) {
			logger.error("Error! Cannot establish connection.");
		}
	}

	public void transferData(List<List<String>> movedData) {
		List<String> movedDataKeys = movedData.get(0);
		List<String> movedDataValues = movedData.get(1);
		
		for (String s : movedDataKeys) {
			try{
				sendMessage(new TextMessage("transfer " + s + " " + movedDataValues.get(movedDataKeys.indexOf(s))), false);
				TextMessage latestMsg = receiveMessage(false);
			}
			catch(Exception e){
				System.out.println(e.getMessage());
			}
		}
	}
	
	/**
	 * Method sends a TextMessage using this socket.
	 * @param msg the message that is to be sent.
	 * @throws IOException some I/O error regarding the output stream 
	 */
	public void sendMessage(TextMessage msg, boolean isClient) throws IOException {
		byte[] msgBytes = msg.getMsgBytes();
		if(isClient) {
			output.write(msgBytes, 0, msgBytes.length);
			output.flush();
			logger.info("SEND \t<" 
					+ clientSocket.getInetAddress().getHostAddress() + ":" 
					+ clientSocket.getPort() + ">: '" 
					+ msg.getMsg() +"'");
		} else {
			outputS.write(msgBytes, 0, msgBytes.length);
			outputS.flush();
			logger.info("SEND \t<" 
					+ socket.getInetAddress().getHostAddress() + ":" 
					+ socket.getPort() + ">: '" 
					+ msg.getMsg() +"'");
		}
    }
	
	
	private TextMessage receiveMessage(boolean isClient) throws IOException {
		int index = 0;
		byte[] msgBytes = null, tmp = null;
		byte[] bufferBytes = new byte[BUFFER_SIZE];
		
		/* read first char from stream */
		byte read = (byte) input.read();	
		boolean reading = true;
		
//		logger.info("First Char: " + read);
//		Check if stream is closed (read returns -1)
//		if (read == -1){
//			TextMessage msg = new TextMessage("");
//			return msg;
//		}

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
		if(isClient) {
			logger.info("RECEIVE \t<" 
					+ clientSocket.getInetAddress().getHostAddress() + ":" 
					+ clientSocket.getPort() + ">: '" 
					+ msg.getMsg().trim() + "'");
		} else {
			logger.info("RECEIVE \t<" 
					+ socket.getInetAddress().getHostAddress() + ":" 
					+ socket.getPort() + ">: '" 
					+ msg.getMsg().trim() + "'");
		}
		return msg;
    }
	

	
}