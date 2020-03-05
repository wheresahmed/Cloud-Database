package app_kvClient;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import logger.LogSetup;

import client.KVCommInterface;
import client.KVStore;
import shared.messages.KVMessage;

public class KVClient implements IKVClient {

    private static Logger logger = Logger.getRootLogger();
    private static final String PROMPT = "M1_Client> ";
    private BufferedReader stdin;
    private boolean stop = false;

    // Metadata Structure - "ip:port", range of hash values
	// private Map<String,String[]> metadata = new HashMap<>();

    private String serverAddress;
    private int serverPort;

    private KVStore store = null;

    public void run() {
        while (!stop) {
            stdin = new BufferedReader(new InputStreamReader(System.in));
            System.out.print(PROMPT);

            try {
                String cmdLine = stdin.readLine();
                this.handleCommand(cmdLine);
            } catch (IOException e) {
                stop = true;
                printError("CLI does not respond - Application terminated ");
            }

        }
    }

    private void handleCommand(String cmdLine) {
        String[] tokens = cmdLine.split("\\s+");

        if (tokens[0].equals("quit")) {
            stop = true;
            if (store != null && store.isClientRunning()) {
                store.disconnect();
            }
            System.out.println(PROMPT + "Application exit!");

        } else if (tokens[0].equals("connect")) {
            if (tokens.length == 3) {
                try {
                    serverAddress = tokens[1];
                    serverPort = Integer.parseInt(tokens[2]);
                    newConnection(serverAddress, serverPort);
                } catch (NumberFormatException nfe) {
                    printError("No valid address. Port must be a number!");
                    logger.info("Unable to parse argument <port>", nfe);
                } catch (UnknownHostException e) {
                    printError("Unknown Host!");
                    logger.info("Unknown Host!", e);
                } catch (IOException e) {
                    printError("Could not establish connection!");
                    logger.warn("Could not establish connection!", e);
                } catch (Exception e) {
                    printError("Unknown exception!");
                    logger.warn("Unknown exception!", e);
                }
            } else {
                printError("Invalid number of parameters!");
            }

        } else if (tokens[0].equals("put")) {
            if (tokens.length >= 2) {
                if (store != null && store.isClientRunning()) {
                    try {
                        String key = tokens[1];
                        String value = "";
                        if (tokens.length > 2) {
                            for (int i = 2; i < tokens.length - 1; i++) {
                                value += tokens[i];
                                value += " ";
                            }
                            value += tokens[tokens.length - 1];
                        }
                        KVMessage reply = retryput(key,value);
                        if (reply.getStatus() == KVMessage.StatusType.PUT_SUCCESS) {
                            printReply("PUT request was successful! Key value tuple has been created");
                        } else if (reply.getStatus() == KVMessage.StatusType.PUT_UPDATE) {
                            printReply("PUT request was successful! Key value tuple has been updated");
                        } else if (reply.getStatus() == KVMessage.StatusType.DELETE_SUCCESS) {
                            printReply("DELETE request was successful! Key value tuple has been deleted");
                        } else if (reply.getStatus() == KVMessage.StatusType.DELETE_ERROR) {
                            printReply("DELETE request was unsuccessful! Key value tuple is not in database");
                        } else if (reply.getStatus() == KVMessage.StatusType.PUT_ERROR) {
                            printError("PUT request encountered an error");
                        } else if (reply.getStatus() == KVMessage.StatusType.SERVER_STOPPED) {
                            printError("Request encountered a server error");
                        } else if (reply.getStatus() == KVMessage.StatusType.SERVER_WRITE_LOCK) {
                            printError("Request encountered a server write error");
                        } else if (reply.getStatus() == KVMessage.StatusType.SERVER_NOT_RESPONSIBLE) {
                            printError("Request encountered a non-server error");
                        } else {
                            printError("PUT request received unknown reply");
                        }
                    } catch (UnknownHostException e) {
                        printError("Unknown Host!");
                        logger.info("Unknown Host!", e);
                    } catch (IOException e) {
                        printError("Could not establish connection!");
                        logger.warn("Could not establish connection!", e);
                    } catch (Exception e) {
                        printError("Unknown exception!");
                    }
                } else {
                    printError("Client not connected!");
                }
            } else {
                printError("Invalid number of parameters!");
            }

        } else if (tokens[0].equals("get")) {
            if (tokens.length == 2) {
                if (store != null && store.isClientRunning()) {
                    try {
                        String key = tokens[1];
                        KVMessage reply = retryget(key);
                        if (reply.getStatus() == KVMessage.StatusType.GET_SUCCESS) {
                            printReply("GET successful, value has been retrieved");
                        } else if (reply.getStatus() == KVMessage.StatusType.GET_ERROR) {
                            printError("GET request encountered an error, the key is not in the database");
                        } else if (reply.getStatus() == KVMessage.StatusType.SERVER_STOPPED) {
                            printError("Request encountered a server error");
                        } else if (reply.getStatus() == KVMessage.StatusType.SERVER_WRITE_LOCK) {
                            printError("Request encountered a server write error");
                        } else if (reply.getStatus() == KVMessage.StatusType.SERVER_NOT_RESPONSIBLE) {
                            printError("Request encountered a non-server error");
                        } else {
                            printError("GET request received unknown reply");
                        }
                    } catch (UnknownHostException e) {
                        printError("Unknown Host!");
                        logger.info("Unknown Host!", e);
                    } catch (IOException e) {
                        printError("Could not establish connection!");
                        logger.warn("Could not establish connection!", e);
                    } catch (Exception e) {
                        printError("Unknown exception!");
                    }
                } else {
                    printError("Client not connected!");
                }
            } else {
                printError("Invalid number of parameters!");
            }

        } else if(tokens[0].equals("disconnect")) {
            if (store != null && store.isClientRunning()) {
                store.disconnect();
            }

        } else if(tokens[0].equals("logLevel")) {
            if(tokens.length == 2) {
                String level = setLevel(tokens[1]);
                if(level.equals(LogSetup.UNKNOWN_LEVEL)) {
                    printError("No valid log level!");
                    printPossibleLogLevels();
                } else {
                    System.out.println(PROMPT +
                            "Log level changed to level " + level);
                }
            } else {
                printError("Invalid number of parameters!");
            }

        } else if(tokens[0].equals("help")) {
            printHelp();
        } else {
            printError("Unknown command");
            printHelp();
        }
    }

    @Override
    public void newConnection(String hostname, int port) throws Exception {

        store = new KVStore(hostname, port);
        store.connect();
    }

    @Override
    public KVCommInterface getStore() {
        return store;
    }

    private KVMessage retryput(String key, String value) throws Exception {
		KVMessage msg;

		do {
			String address = store.searchKey(key);
			if (!address.equals(serverAddress + ":" + Integer.toString(serverPort))) {
				
                if (store!=null && store.isClientRunning()) {
					store.disconnect();
					store = null;
				}

				String [] addrArray= address.split(":");
				newConnection(addrArray[0], Integer.parseInt(addrArray[1]));
			}

			msg = store.put(key, value); 

		} while (msg.getStatus() == KVMessage.StatusType.SERVER_NOT_RESPONSIBLE);

		if (store.port != serverPort || !store.address.equals(serverAddress)) {

			if (store!=null && store.isClientRunning()){
				store.disconnect();
				store=null;
			}

			newConnection(serverAddress, serverPort);
        }
        
        return msg;
	}

	private KVMessage retryget(String key) throws Exception{
		KVMessage msg;

		do {
            // System.out.println("in do loop");
            String address = store.searchKey(key);
            // System.out.println("address : " + address );            
			if (!address.equals(serverAddress + ":" + Integer.toString(serverPort))) {
                // System.out.println("address does not equal serverAddress, so switch");
				if (store!=null && store.isClientRunning()){
					store.disconnect();
					store=null;
				}

				String [] addrArray= address.split(":");
				newConnection(addrArray[0], Integer.parseInt(addrArray[1]));
			}

			msg = store.get(key);
		} while (msg.getStatus() == KVMessage.StatusType.SERVER_NOT_RESPONSIBLE);
		
		if (store.port != serverPort || !store.address.equals(serverAddress)) {
			
            if (store!=null && store.isClientRunning()){
				store.disconnect();
				store = null;
			}

			newConnection(serverAddress, serverPort);
        }
        
        return msg;
	}

    private void printHelp() {
        StringBuilder sb = new StringBuilder();
        sb.append(PROMPT).append("KV CLIENT HELP (Usage):\n");
        sb.append(PROMPT);
        sb.append("::::::::::::::::::::::::::::::::");
        sb.append("::::::::::::::::::::::::::::::::\n");
        sb.append(PROMPT).append("connect <host> <port>");
        sb.append("\t establishes a connection to a server\n");
        sb.append(PROMPT).append("get <key> ");
        sb.append("\t\t\t gets value associated with key from the server\n");
        sb.append(PROMPT).append("put <key> <value>");
        sb.append("\t\t creates or updates key value pair in the server\n");
        sb.append(PROMPT).append("disconnect");
        sb.append("\t\t\t disconnects from the server \n");

        sb.append(PROMPT).append("logLevel");
        sb.append("\t\t\t changes the logLevel \n");
        sb.append(PROMPT).append("\t\t\t\t ");
        sb.append("ALL | DEBUG | INFO | WARN | ERROR | FATAL | OFF \n");

        sb.append(PROMPT).append("quit");
        sb.append("\t\t\t\t exits the program");
        System.out.println(sb.toString());
    }

    private void printPossibleLogLevels() {
        System.out.println(PROMPT
                + "Possible log levels are:");
        System.out.println(PROMPT
                + "ALL | DEBUG | INFO | WARN | ERROR | FATAL | OFF");
    }

    private String setLevel(String levelString) {

        if(levelString.equals(Level.ALL.toString())) {
            logger.setLevel(Level.ALL);
            store.logger.setLevel(Level.ALL);
            return Level.ALL.toString();
        } else if(levelString.equals(Level.DEBUG.toString())) {
            logger.setLevel(Level.DEBUG);
            store.logger.setLevel(Level.DEBUG);
            return Level.DEBUG.toString();
        } else if(levelString.equals(Level.INFO.toString())) {
            logger.setLevel(Level.INFO);
            store.logger.setLevel(Level.INFO);
            return Level.INFO.toString();
        } else if(levelString.equals(Level.WARN.toString())) {
            logger.setLevel(Level.WARN);
            store.logger.setLevel(Level.WARN);
            return Level.WARN.toString();
        } else if(levelString.equals(Level.ERROR.toString())) {
            logger.setLevel(Level.ERROR);
            store.logger.setLevel(Level.ERROR);
            return Level.ERROR.toString();
        } else if (levelString.equals(Level.FATAL.toString())) {
            logger.setLevel(Level.FATAL);
            store.logger.setLevel(Level.FATAL);
            return Level.FATAL.toString();
        } else if (levelString.equals(Level.OFF.toString())) {
            logger.setLevel(Level.OFF);
            store.logger.setLevel(Level.OFF);
            return Level.OFF.toString();
        } else {
            return LogSetup.UNKNOWN_LEVEL;
        }
    }

    private void print(String text) {
        System.out.println(PROMPT + text);
    }

    private void printReply(String reply) {
        System.out.println(PROMPT + "Reply: " + reply);
    }

    private void printError(String error) {
        System.out.println(PROMPT + "Error: " + error);
    }

    /**
     * Main entry point for the KV Client application.
     *
     * @param args contains the port number at args[0].
     */
    public static void main(String[] args) {
        try {
            new LogSetup("logs/client.log", Level.ALL);
            KVClient client = new KVClient();
            client.run();
        } catch (IOException e) {
            System.out.println("Error! Unable to initialize logger!");
            e.printStackTrace();
            System.exit(1);
        }
    }
}
