package app_kvServer;

import java.io.InputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;

import org.apache.log4j.*;

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
		
			// sendMessage(new TextMessage(
			// 		"Connection to MSRG Echo server established: " 
			// 		+ clientSocket.getLocalAddress() + " / "
			// 		+ clientSocket.getLocalPort()));
			
			while(isOpen) {
				try {
					TextMessage latestMsg = receiveMessage();
					String msg = "";

					// parse msg and take action accordingly
					String[] token = null;
					token = latestMsg.getMsg().trim().split("\\s+");
					// System.out.println("MSG: " + latestMsg.getMsg() + "\n");
					// System.out.println("TOKEN[0]: " + token[0]);
					// System.out.println("TOKEN LEN: " + token.length);

					if(token[0].equals("put")) {
						logger.info("Message received with PUT request.");

						if (server.getServerState() == ServerStateType.STOPPED) {
							msg = "SERVER_STOPPED";
						} else if (server.isWriterLocked()) {
							msg = "SERVER_WRITE_LOCK";
						} else{
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
						}

						// sendMessage(new TextMessage(msg));
					} else if (token[0].equals("get")) {
						// System.out.println("In GET");
						logger.info("Message received with GET request."); 
						// System.out.println("Key : " + token[1]);
						if (server.getServerState() == ServerStateType.STOPPED) {
							msg = "SERVER_STOPPED";
						} else {
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
							} else {
								msg = "GET_ERROR < ";
							}

							msg += token[1] + ", " + value + " >";
						}
						// sendMessage(new TextMessage(msg));
					} else if (token[0].equals("transfer")){
						transfer(token);
					} else if (token[0].equals("start")) {
						server.start();
						msg = "Server is started";
					} else if (token[0].equals("stop")) {
						server.stop();
						msg = "Server is stopped";
					} else if (token[0].equals("shutdown")) {
						server.shutdown();
						msg = "Server is shutdown";
					} else if (token[0].equals("lockWrite")) {
						server.moveData(token[2].split("-"), token[1]);
						server.lockWrite();
					} else if (token[0].equals("unlockWrite")) {
						server.unlockWrite();
					} else if (token[0].equals("moveData")) {
						
					} else if (token[0].equals("update_metadata")) {
						server.loadMetadataFromZookeeper();
					} else {
						msg = latestMsg.toString();
					} 

					sendMessage(new TextMessage(msg));
					
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

	private void transfer(String[] splitMsg){

		String message = "";

		if(splitMsg.length >= 3){

			if (server.inCache(splitMsg[1])){
				message = "TRANSFER_UPDATE < ";
			}else{
				message = "TRANSFER_SUCCESS < ";
			}

			String value = "";

			for (int i =2; i < splitMsg.length - 1; i++ ){
				value += splitMsg[i];
				value+= " ";
			}

			value += splitMsg[splitMsg.length -1];

			try {
				server.putKV(splitMsg[1],value);
			}
			catch (Exception e){
				logger.error("Transfer Error");

			}
			message += splitMsg[1] + 
					" , " + value + " >";

		}
		else{
			message = "ERROR: Invlaid format";
		}
		//Send message back to the client 
		try {
			sendMessage(new TextMessage(message));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

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
				+ clientSocket.getInetAddress().getHostAddress() + ":" 
				+ clientSocket.getPort() + ">: '" 
				+ msg.getMsg() +"'");
    }
	
	
	private TextMessage receiveMessage() throws IOException {
		
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
		logger.info("RECEIVE \t<" 
				+ clientSocket.getInetAddress().getHostAddress() + ":" 
				+ clientSocket.getPort() + ">: '" 
				+ msg.getMsg().trim() + "'");
		return msg;
    }
	

	
}