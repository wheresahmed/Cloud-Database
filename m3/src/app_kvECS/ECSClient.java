package app_kvECS;

import java.util.Map;
import java.util.Collection;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Scanner;
import java.util.ArrayList;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import logger.LogSetup;

import ecs.ECS;
import ecs.IECSNode;

public class ECSClient implements IECSClient {

    private static Logger logger = Logger.getRootLogger();
    private static final String PROMPT = "ECS_Client> ";

	private BufferedReader stdin;
    private ECS ecs;
	
	private boolean stop = false;
	private boolean done_init = false;

    public void run(){
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

    private void handleCommand(String cmdLine){
       String[] tokens = cmdLine.split("\\s+");
       if (tokens[0].equals("init")) {
	  command_init(tokens);
       } else if (tokens[0].equals("start")) {
	 command_start(tokens);
       } else if (tokens[0].equals("stop")) {
	 command_stop(tokens);
       } else if (tokens[0].equals("shutdown")) {
	 command_shutdown(tokens);
       } else if (tokens[0].equals("addNode")) {
	  command_addnode(tokens);
       } else if (tokens[0].equals("addNodes")) {
	  command_addnodes(tokens);
       } else if (tokens[0].equals("removeNode")) {
	  command_removenode(tokens);
       } else if (tokens[0].equals("logLevel")) {
	  command_loglevel(tokens);
       } else if (tokens[0].equals("quit")) {
	 command_quit(tokens);
       } else if (tokens[0].equals("help")) {
	  printHelp();
       } else {
	  printError("Invalid command");
	  printHelp();
      	}
   }
   private void command_init(String[] tokens){
      if (tokens.length!=4){
	     System.out.println(PROMPT + "Invalid arguments, please use:");
	     System.out.println(PROMPT + "init <numberOfServers> <cacheSize> <cacheStrategy>");
	 }else {
	    if (done_init){
	       System.out.println("Cannot initialize storage server more than once");
	    }
	    if (!done_init) {
	       if (!tokens[3].equalsIgnoreCase("LRU") && !tokens[3].equalsIgnoreCase("LFU") && !tokens[3].equalsIgnoreCase("FIFO")) {
		  System.out.println(PROMPT + "Invalid cache strategy. Cache strategy must be: LRU, LFU, or FIFO");
	       } else {
		  System.out.println(PROMPT + "Initializing Storage Servers.....");
		  ecs = new ECS(Integer.parseInt(tokens[1]), Integer.parseInt(tokens[2]), tokens[3]);
		  System.out.println(PROMPT + "Initialized Storage Service with " + ecs.servers_launched + " server(s).");
		  done_init = true;
	       }
	    }
      }
   }
   private void command_start(String[]tokens){
      if (!done_init) {
	 printError("Please initialize Storage service before starting");
      } else {
	 start();
      }
   }
   private void command_stop(String[]tokens){
      if (!done_init) {
	 printError("Please initialize Storage service before stopping");
      } else {
	 stop();
      }
   }
   private void command_shutdown(String[]tokens){
      if (!done_init) {
	 printError("no servers to shutdown, storage service has not been initialized");
      } else {
	 shutdown();
	 done_init = false;
      } 
   }
   private void command_addnode(String[]tokens){
      if(!done_init) {
	    printError("Please initialize storage service before adding node");
	 }
      else {
	 if (tokens.length==3) {
	    if (!tokens[2].equalsIgnoreCase("LRU") && !tokens[2].equalsIgnoreCase("LFU") && !tokens[2].equalsIgnoreCase("FIFO")) {
	       System.out.println(PROMPT + "Invalid cache strategy. Cache strategy must be LRU, LFU, or FIFO");
	    } else {
	       System.out.println(PROMPT + "New node being added....");
	       addNode(tokens[2], Integer.parseInt(tokens[1]));
	    }
	 } else {
	    System.out.println(PROMPT + "Invalid arguments, please use:");
	    System.out.println(PROMPT + "addNode <cacheSize> <cacheStrategy>");
	 }
      } 

   }
   private void command_addnodes(String[]tokens){
      if(!done_init) {
	 printError("Please initialize storage service before adding nodes");
      }

      else{
	 if (tokens.length==4) {
	    if (!tokens[3].equalsIgnoreCase("LRU") && !tokens[3].equalsIgnoreCase("LFU") && !tokens[3].equalsIgnoreCase("FIFO")) {
	       System.out.println(PROMPT + "Invalid cache strategy. Cache strategy must be LRU, LFU, or FIFO");
	    } else {
	       System.out.println(PROMPT + "New node(s) being added....");
	       addNodes(Integer.parseInt(tokens[1]), tokens[3], Integer.parseInt(tokens[2]));
	    }
	 } else {
	       System.out.println(PROMPT + "Invalid arguments, please use:");
	       System.out.println(PROMPT + "addNodes <count> <cacheSize> <cacheStrategy>");
	    }
      } 
   }
   private void command_removenode(String[]tokens){
      if (done_init){
	 if (tokens.length >= 2) {
	    System.out.println(PROMPT + "Removing node(s).......");
	    ArrayList<String> nodes = new ArrayList<String>();
	    for (int i = 1; i < tokens.length; i++) {
	       nodes.add(tokens[i]);
	    }
	    removeNodes(nodes);
	 } else {
	    System.out.println(PROMPT + "Invalid arguments, please use:");
	    System.out.println(PROMPT + "removeNode <index> ....");
	 }
      } else {
	 printError("initialize storage service to removeNodes");
      }
   }
   private void command_loglevel(String[]tokens){
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
   }
   private void command_quit(String[]tokens){
      if (this.ecs != null) {
	 shutdown();
	 this.ecs = null;
      }
      System.exit(0);
   }
    @Override
    public boolean start() {
        boolean success = ecs.start();
        return success;
    }

    @Override
    public boolean stop() {
        boolean success = ecs.stop();
        return success;
    }

    @Override
    public boolean shutdown() {
        boolean success = ecs.shutdownAll();
        return success;
    }

    @Override
    public IECSNode addNode(String cacheStrategy, int cacheSize) {
        try {	
		    return ecs.addNode(cacheSize, cacheStrategy);
		} catch (Exception e) {
			printError(e.getMessage());
			return null;
		}
    }

    @Override
    public Collection<IECSNode> addNodes(int count, String cacheStrategy, int cacheSize) {
        try {
			return ecs.addNodes(count, cacheStrategy, cacheSize);
		} catch(Exception e) {
			printError(e.getMessage());
			return null;
		}
    }

    @Override
    public Collection<IECSNode> setupNodes(int count, String cacheStrategy, int cacheSize) {
        // TODO
        return null;
    }

    @Override
    public boolean awaitNodes(int count, int timeout) throws Exception {
        // TODO
        return false;
    }

    @Override
    public boolean removeNodes(Collection<String> nodeNames) {
        if (ecs.getNumberofNodes() == 0) {
			
            printError("Remove error: there are no nodes to remove");
			done_init = false;
			return false;

		} else if (nodeNames == null || nodeNames.size() <= 0) {
			return false;
		}

		ArrayList<String> nodes = new ArrayList(nodeNames);
		
        for (int i = 0; i < nodeNames.size(); i++) {
			try {
				ecs.removeNode(Integer.parseInt(nodes.get(i)));
			} catch(Exception e) {
				printError("Exception " + e.getMessage());
				return false;
			}
        }

		return true;
    }

    @Override
    public Map<String, IECSNode> getNodes() {
        // TODO
        return null;
    }

    @Override
    public IECSNode getNodeByKey(String Key) {
        // TODO
        return null;
    }

    private void printError(String error){
		System.out.println(PROMPT + "Error!! " +  error);
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
			return Level.ALL.toString();
		} else if(levelString.equals(Level.DEBUG.toString())) {
			logger.setLevel(Level.DEBUG);
			return Level.DEBUG.toString();
		} else if(levelString.equals(Level.INFO.toString())) {
			logger.setLevel(Level.INFO);
			return Level.INFO.toString();
		} else if(levelString.equals(Level.WARN.toString())) {
			logger.setLevel(Level.WARN);
			return Level.WARN.toString();
		} else if(levelString.equals(Level.ERROR.toString())) {
			logger.setLevel(Level.ERROR);
			return Level.ERROR.toString();
		} else if(levelString.equals(Level.FATAL.toString())) {
			logger.setLevel(Level.FATAL);
			return Level.FATAL.toString();
		} else if(levelString.equals(Level.OFF.toString())) {
			logger.setLevel(Level.OFF);
			return Level.OFF.toString();
		} else {
			return LogSetup.UNKNOWN_LEVEL;
		}
	}

    public static void main(String[] args) {
        try {
			
            new LogSetup("logs/ecs_client.log",  Level.OFF);

		} catch (IOException e) {

			System.out.println("Error! Unable to initialize logger!");
			e.printStackTrace();
			System.exit(1);

		}

		ECSClient ecs_client = new ECSClient();
		ecs_client.run();
    }
}
