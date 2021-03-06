package client;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.util.HashSet;
import java.util.Set;
import java.util.Map;
import java.util.HashMap;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import org.apache.log4j.Logger;
import java.net.UnknownHostException;

import shared.messages.KVMessage;
import shared.messages.Message;
import app_kvClient.ClientSocketListener;
import app_kvClient.TextMessage;


public class KVStore extends Thread implements KVCommInterface, ClientSocketListener {
	/**
	 * Initialize KVStore with address and port of KVServer
	 * @param address the address of the KVServer
	 * @param port the port of the KVServer
	 */

	public String address;
	public int port;

	private boolean running;

	public Logger logger = Logger.getRootLogger();
	private Set<ClientSocketListener> listeners;
	private Socket clientSocket;
	private OutputStream output;
	private InputStream input;

	private static final String PROMPT = "M1_Client> ";

	private static final int MAXKEYLENGTH = 20;
	private static final int MAXVALUELENGTH = 1024 * 120;
	private static final int BUFFER_SIZE = 1024;
	private static final int DROP_SIZE = 1024 * BUFFER_SIZE;

	// Metadata Structure - "ip:port", range of hash values
	private Map<String,String[]> metadata = new HashMap<>();

	public KVStore(String address, int port) {

		this.address = address;
		this.port = port;

		String key = address + ":" + Integer.toString(port);
		String[] value = {"00000000000000000000000000000000","ffffffffffffffffffffffffffffffff"};
		
		this.metadata.put(key, value);
	}

	// Manage Connection to Server

	@Override
	public void connect() throws UnknownHostException, IOException {

		clientSocket = new Socket(address, port);
		listeners = new HashSet<ClientSocketListener>();
		setRunning(true);

		print("Connection established to server at " + address + " / " + port);
		logger.info("Connection established");
		listeners.add(this);

		try {
			output = clientSocket.getOutputStream();
			input = clientSocket.getInputStream();
		} catch (Exception e) {
			logger.error("Connection could not be established!");
		}
	}

	private void tearDownConnection() throws IOException {
		setRunning(false);
		logger.info("tearing down the connection ...");
		if (clientSocket != null) {
			input.close();
			output.close();
			clientSocket.close();
			clientSocket = null;
			logger.info("connection closed!");
		}
	}

	@Override
	public void disconnect() {
		logger.info("try to close connection ...");

		if (running) {
			try {
				tearDownConnection();
				for (ClientSocketListener listener : listeners) {
					listener.handleStatus(SocketStatus.DISCONNECTED);
				}
			} catch (IOException ioe) {
				logger.error("Unable to close connection!");
			}
		}
	}

	// Manage Metadata Handling

	private void loadMetadata(String[] data) {

		for (int i = 0; i < data.length; i +=2) {
			String[] value = data[i+1].split("-");
			this.metadata.put(data[i],value);
			// System.out.println("data[i] : " + data[i]);
			// System.out.println("value : " + value[0]);
			// System.out.println("value : " + value[1]);
		}

		// String meta = "";
		// for (Map.Entry <String,String[]> pair : this.metadata.entrySet()) {
		// 	meta += pair.getKey() + " " + pair.getValue()[0] +  "-" +  pair.getValue()[1] + " ";
		// }

		// System.out.println("METADATA AT LOADMETADATA : " + meta);
	}

	// Inspired from: https://stackoverflow.com/questions/415953/how-can-i-generate-an-md5-hash
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

	public String searchKey(String key){
		String keyHash = convertToMD5(key);
		// System.out.println("IN SEARCH KEYYYY");
		// String meta = "";
		// for (Map.Entry <String,String[]> pair : this.metadata.entrySet()) {
		// 	meta += pair.getKey() + " " + pair.getValue()[0] +  "-" +  pair.getValue()[1] + " ";
		// }

		// System.out.println("METADATA AT SEARCHKEY : " + meta);
		
		for (Map.Entry <String,String[]> pair : this.metadata.entrySet()) {
			// System.out.println("IN FOR LOOP");

			String start = pair.getValue()[0];
			String end = pair.getValue()[1];

			if (start.compareTo(end) >= 0) {
				if (keyHash.compareTo(start) >0 || keyHash.compareTo(end) <= 0 ) {
					return  pair.getKey();
				}			
			}
			else {
				if (keyHash.compareTo(start) >0 && keyHash.compareTo(end) <= 0 ) {
					return pair.getKey();
				}
			}
		}

		return "";
	}

	private void receiveMetadata(String[] msg) {
		String[] metadata = new String [msg.length -1];

		for (int i = 1; i < msg.length; i++) {
			metadata[i-1] =  msg[i];
			// System.out.println("Metadata[i-1] : " + metadata[i-1]);
			// System.out.println("msg[i] : " + msg[i]);
		}

		loadMetadata(metadata);
	}

	private void printMetadata() {

		for(Map.Entry <String,String[]> pair : this.metadata.entrySet()) {
			System.out.println(pair.getKey() + " " + pair.getValue()[0] + " to " + pair.getValue()[1] );
		}

	}

	// Put and Get functions - take in the key and value, used by KVClient

	@Override
	public KVMessage put(String key, String value) throws Exception {
		Message msgToClient = null;

		if (isLengthInvalid(key, MAXKEYLENGTH)) {
			printError("Put request was unsuccessful! Key was too large");
			String[] error= {"PUT_ERROR", "<", key, ",", value, ">"};
			msgToClient = new Message(error);
		} else if (!value.equals("") && isLengthInvalid(value, MAXVALUELENGTH)) {
			printError("Put request was unsuccessful! Value was too large");
			String[] error= {"PUT_ERROR", "<", key, ",", value, ">"};
			msgToClient = new Message(error);
		} else {
			StringBuilder sendMsgToServer = new StringBuilder();
			sendMsgToServer.append("put " + key + " " + value);
			sendMessage(new TextMessage(sendMsgToServer.toString()));

			TextMessage latestMsgFromServer = receiveMessage();
			// handleNewMessage(latestMsgFromServer);
			String[] splitMsg = latestMsgFromServer.getMsg().split("\\s+");

			if (splitMsg[0].equals("SERVER_NOT_RESPONSIBLE")) {
				handleNewMessage(new TextMessage(splitMsg[0]));
				receiveMetadata(splitMsg);
			} else {
				handleNewMessage(latestMsgFromServer);
			}

			msgToClient = new Message(splitMsg);
		}
		return msgToClient;
	}

	@Override
	public KVMessage get(String key) throws Exception {
		Message msgToClient = null;

		if (isLengthInvalid(key, MAXKEYLENGTH)) {
			printError("Get request was unsuccessful! Key was too large");
			String[] error= {"GET_ERROR", "<", key};
			msgToClient = new Message(error);
		} else {
			StringBuilder sendMsgToServer = new StringBuilder();
			sendMsgToServer.append("get " + key);
			sendMessage(new TextMessage(sendMsgToServer.toString()));

			TextMessage latestMsgFromServer = receiveMessage();
			// handleNewMessage(latestMsgFromServer);
			String[] splitMsg = latestMsgFromServer.getMsg().split("\\s+");

			if (splitMsg[0].equals("SERVER_NOT_RESPONSIBLE")) {
				handleNewMessage(new TextMessage(splitMsg[0]));
				receiveMetadata(splitMsg);
			} else {
				handleNewMessage(latestMsgFromServer);
			}

			msgToClient = new Message(splitMsg);
		}
		return msgToClient;
	}

	// Sending and Receiving Messages Functionality - used by put and get to send the message and receive the response

	/**
	 * Method sends a TextMessage using this socket.
	 *
	 * @param msg the message that is to be sent.
	 * @throws IOException some I/O error regarding the output stream
	 */
	public void sendMessage(TextMessage msg) throws IOException {
		byte[] msgBytes = msg.getMsgBytes();
		//System.out.println(msg.getMsg());
		output.write(msgBytes, 0, msgBytes.length);
		output.flush();
		logger.info("Send message:\t '" + msg.getMsg() + "'");
	}

	public TextMessage receiveMessage() throws IOException {

		int index = 0;
		byte[] msgBytes = null, tmp = null;
		byte[] bufferBytes = new byte[BUFFER_SIZE];

		/* read first char from stream */
		byte read = (byte) input.read();
		boolean reading = true;

		while (read != 13 && reading) {/* carriage return */
			/* if buffer filled, copy to msg array */
			if (index == BUFFER_SIZE) {
				if (msgBytes == null) {
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

			/* only read valid characters, i.e. letters and numbers */
			if((read > 31 && read < 127)) {
				bufferBytes[index] = read;
				index++;
			}

			// /* stop reading is DROP_SIZE is reached */
			// if(msgBytes != null && msgBytes.length + index >= DROP_SIZE) {
			// 	reading = false;
			// }

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
		logger.info("Receive message:\t '" + msg.getMsg() + "'");
		return msg;
	}

	//Client Socket Listener Functionality

	@Override
	public void handleStatus(SocketStatus status) {
		if(status == SocketStatus.CONNECTED) {

		} else if (status == SocketStatus.DISCONNECTED) {
			logger.debug(PROMPT);
			logger.debug("Connection terminated: "
					+ this.address + " / " + this.port);

		} else if (status == SocketStatus.CONNECTION_LOST) {
			logger.debug("Connection lost: "
					+ this.address + " / " + this.port);
			logger.debug(PROMPT);
		}

	}

	@Override
	public void handleNewMessage(TextMessage msg) {
		System.out.println(PROMPT + msg.getMsg());
	}

	// Error and Formatting Logic

	private void printError(String error) {
		System.out.println(PROMPT + "Error! " + error);
	}

	private void print(String text) {
		System.out.println(PROMPT + text);
	}

	// Helpers

	public boolean isClientRunning() {
		return running;
	}

	public boolean isRunning() {
		return running;
	}

	public void setRunning(boolean run) {
		running = run;
	}

	public boolean isLengthInvalid(String input, int threshold ){
		TextMessage message = new TextMessage(input);
		return message.getMsgBytes().length > threshold;
	}

}
