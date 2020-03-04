package ecs;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;

import java.io.OutputStream;
import java.net.Socket;

import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.ArrayList;
import java.util.Scanner;
import java.util.concurrent.CountDownLatch;
import java.util.TreeMap;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Collection;
import org.apache.zookeeper.*;
import org.apache.zookeeper.Watcher.Event.KeeperState;

import java.util.concurrent.TimeUnit;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import app_kvClient.TextMessage;
import logger.LogSetup;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.Stat;

public class ECS {

   private static final String PROMPT = "ecs> ";
   private String confFile = "ecs.config";
   public int servers_launched = 0;
   private static int total_servers = 0;
   private static Logger logger = Logger.getRootLogger();
   public static final String LOCAL_HOST = "127.0.0.1";
   public static final String ZK_HOST = LOCAL_HOST;
   public static final String ZK_PORT = "2181";
   public static final String ZK_CONN = ZK_HOST + ":" + ZK_PORT;
   public static final int ZK_TIMEOUT = 2000;

   TreeMap<String, ECSNode> metadata = new TreeMap<>();
   ArrayList<ECSNode> storageNodes = new ArrayList<ECSNode>(); 
   ArrayList<ECSNode> idleNodes = new ArrayList<ECSNode>(); 
   private ZooKeeper zk;
   private static final String dataPath = "/server";
   private int storage_cache_size = 0; 
   private String storage_cache_strategy = "LRU"; 
   private Socket ecsSocket;
   private OutputStream output;
   private InputStream input;
   private static final int BUFFER_SIZE = 1024;
   private static final int DROP_SIZE = 1024 * BUFFER_SIZE;
   
   public ECS(int num_servers,int cache_size, String cache_strategy){
      zk_start();
      zk_connect();
      ecs_nodes_initialize(num_servers, cache_size,cache_strategy);
   }

   public void zk_start(){
     //check if zookeepr is runninging already. if not running, start a new one
      String script="lsof -w -n -i tcp:2181";
      Process process;
      Runtime runtime=Runtime.getRuntime();
      String output="";
      try {
         process=runtime.exec(script);
         process.waitFor();
         BufferedReader buf=new BufferedReader(new InputStreamReader(process.getInputStream()));
         output=buf.readLine();
	   }
      catch(IOException e){
         e.printStackTrace();
      }catch(InterruptedException e){
         e.printStackTrace();
      }
      // not running start zookeeper
      if (output==null) {
         script="zookeeper-3.4.11/bin/zkServer.sh start";
         
         try{
            process=runtime.exec(script);
         }catch(IOException e){
            e.printStackTrace();
         }

         try{
            TimeUnit.MILLISECONDS.sleep(500);
         }catch(InterruptedException e){
            e.printStackTrace();
         }
      }
   }

   private void zk_connect(){
      final CountDownLatch connected_signal=new CountDownLatch(1);
      try {
         zk=new ZooKeeper(ZK_CONN,ZK_TIMEOUT,new Watcher(){
            public void process (WatchedEvent we){
               if (we.getState()==KeeperState.SyncConnected){
                  connected_signal.countDown();
	            }
	         }
         });
      } catch(IOException e){
         logger.error("Failed to connect Client to Zookeeper");
      }

      try {
	      connected_signal.await();
      }catch(InterruptedException e){
	      e.printStackTrace();
      }
   }

   private void ecs_nodes_initialize(int num_servers, int cache_size, String cache_strategy){
      populate_storage_and_idle_servers(num_servers, cache_size, cache_strategy); 
      create_ecsnode_and_zknode();
   }

   private void populate_storage_and_idle_servers(int num_servers, int cache_size, String cache_strategy){
      File file =new File(confFile);
      servers_launched=0;
      try {
         Scanner scanner=new Scanner(file);
         while(scanner.hasNextLine()) {
            String line=scanner.nextLine();
            String[]words=line.split(" ");
            String server_name=words[0];
            String host=words[1];
            String port=words[2];
            ECSNode node=new ECSNode(server_name,host,Integer.parseInt(port));
            if (servers_launched>=num_servers){
               idleNodes.add(node);
            } else {
               storageNodes.add(node);
               launchServer(node,cache_size,cache_strategy);
               //servers_launched++;
               metadata.put(node.getNodeHash(),node);
            }
            total_servers++;
	      }
      } catch(FileNotFoundException e){
	      System.out.println("Could not find file"+ e); 
      }
   }

   private void create_ecsnode_and_zknode(){
      String current_data;
      String data = "";

      for (ECSNode node:storageNodes) {  
         node.setRange(getServerRange(node.getNodeHash()));
         current_data=node.getNodeHost()+":" + node.getNodePort()+" "+node.getNodeHashRange()[0]+"-"+node.getNodeHashRange()[1]+"\n";
         data+=current_data;
	   }
      if (!exists(dataPath)){
         create_zknode(dataPath,data);
      }
	   setData(dataPath,data);
   }

   private void setData(String path, String data){
      try{
	      zk.setData(path,data.getBytes(),zk.exists(path,true).getVersion());
      }catch (Exception e){
	      System.out.println(e.getMessage());
      }
   }

   private void create_zknode(String path, String data){
      try{
	      zk.create(path,data.getBytes(),ZooDefs.Ids.OPEN_ACL_UNSAFE,CreateMode.PERSISTENT);
      }catch(Exception e){
	      System.out.println(e.getMessage());
      }
   }
   
   public ArrayList<String> getServerRange(String hash){
      ArrayList<String>res=new ArrayList<String>();

      if(metadata==null) return res;

      String previous=null;
      boolean found=false;
      int count=0;

      for(Map.Entry<String,ECSNode>entry : this.metadata.entrySet()){
         if(entry.getKey().equals(hash)){
            if (count!=0){
               found = true;
               break;
            }
	      }
         previous=entry.getKey();
         count++;
      }
      
      if(previous!=null){
         res.add(previous);
         res.add(hash);
      }

      return res;
   }

   private boolean exists(String path){
      Stat stat = new Stat();
      try{
	      stat = zk.exists(path,false);
      }catch(Exception e){
	      System.out.println(e.getMessage());
      }
      return stat != null;
   }
   
   public Collection<IECSNode> addNodes(int num_nodes, String replacementStrategy, int cacheSize) throws Exception {
      ArrayList added = new ArrayList<ECSNode>();
      if(storageNodes.size()+num_nodes<=total_servers){
         for (int i = 0; i < num_nodes; i++) {
            added.add(addNode(cacheSize, replacementStrategy));
         }
      }
      else {
	      throw new Exception("Cannot add more nodes than in reserve");
      }
      return added;
   }

   public ECSNode addNode(int cacheSize, String replacementStrategy) throws Exception {
      if (idleNodes == null || idleNodes.size() == 0) {
	      throw new Exception("Cannot add server since all servers are active.");
      }

      ECSNode node= add_idle_node_to_storagenodes();
      System.out.println("Computing key-range of new server...");
      update_node_range_and_metadata(node);
      appendToCurrentPath(node);
      System.out.println("Launching new server...");
      launchServer(node, cacheSize, replacementStrategy);

      try{
         TimeUnit.MILLISECONDS.sleep(500);
      }catch(InterruptedException e){
         e.printStackTrace();
      }

      System.out.println("Updating metadata of all servers...");
      lockwrite_successor_while_servers_update_metadata(node);
      return node; 
   }

   private void lockwrite_successor_while_servers_update_metadata(ECSNode node){
       
      ECSNode successorNode = getSuccessorNode(node); 
      sendLockWriteMessage(successorNode, node); 
      update_servers_metadata();
      sendUnlockWriteMessage(successorNode);

   }

   private ECSNode add_idle_node_to_storagenodes(){
      ECSNode node = idleNodes.get(0);
      storageNodes.add(node);
      idleNodes.remove(0);
      return node;
   }

   private void update_node_range_and_metadata(ECSNode node){
      metadata.put(node.getNodeHash(), node);
      node.setRange(getServerRange(node.getNodeHash()));
      ECSNode successorNode = getSuccessorNode(node); 
      updateMetadata(successorNode);
   }

   private ECSNode getSuccessorNode(ECSNode node){
      ECSNode successorNode = metadata.get(getSuccessorHash(node.getNodeHash())); 
      return successorNode;
   }

   public String getSuccessorHash(String hash){
      if (metadata == null) return null;
      NavigableMap<String, ECSNode> temp = metadata;
      String next;
      if (temp.higherEntry(hash) == null) next = temp.firstKey();
      else next = temp.higherEntry(hash).getKey();
      System.out.println(next);
      return next;
   }

   private void appendToCurrentPath(ECSNode node){
      String data = getData(dataPath);
      String host = node.getNodeHost();
      String port = Integer.toString(node.getNodePort());
      String range_begin = node.getNodeHashRange()[0];
      String range_end = node.getNodeHashRange()[1];
      String currentData = host + ":" + port + " " + range_begin + "-" + range_end + "\n";
      data += currentData;
      setData(dataPath,data);
   }

   private void sendLockWriteMessage(ECSNode node, ECSNode curNode) {
      try {
         ecsSocket = new Socket(node.getNodeHost(), node.getNodePort());
         output = ecsSocket.getOutputStream();
         input = ecsSocket.getInputStream();
         String host=curNode.getNodeHost();
         String port=Integer.toString(curNode.getNodePort());
         String range_begin=curNode.getNodeHashRange()[0]; 
         String range_end=curNode.getNodeHashRange()[1]; 
      
	      sendMessage (new TextMessage("lockwrite " + host+ ":" + port + " "+ range_begin + "-" + range_end));
         try{
            TimeUnit.MILLISECONDS.sleep(500);
         }catch(InterruptedException e){
            e.printStackTrace();
         }
         
         TextMessage latestMsg = receiveMessage();
         System.out.println(PROMPT + latestMsg.getMsg());
         try {
            input.close();
            output.close();
            ecsSocket.close();
         } catch (IOException e) {
            e.printStackTrace();
         }
	      ecsSocket = null;	     
      } catch (Exception e) {
	      e.printStackTrace();
      }
   }

   private void sendUnlockWriteMessage(ECSNode node) {
      try {
         ecsSocket = new Socket(node.getNodeHost(), node.getNodePort());
         output = ecsSocket.getOutputStream();
         input = ecsSocket.getInputStream();
         sendMessage (new TextMessage("unlockwrite"));
         TextMessage latestMsg = receiveMessage();
         System.out.println(PROMPT + latestMsg.getMsg());

         try {
            input.close();
            output.close();
            ecsSocket.close();
            } catch (IOException e) {
               e.printStackTrace();
            }
         ecsSocket = null;	     
      } catch (Exception e) {
         e.printStackTrace();
      }
   }

   private void updateMetadata(ECSNode node) {
      String data = getData(dataPath);
      String host=node.getNodeHost();
      String port=Integer.toString(node.getNodePort());
      String range_begin=node.getNodeHashRange()[0];
      String range_end=node.getNodeHashRange()[1];

      String oldData = host + ":" + port + " " + range_begin + "-"+ range_end + "\n";
      node.setRange(getServerRange(node.getNodeHash()));
      host = node.getNodeHost();
      port = Integer.toString(node.getNodePort());
      range_begin = node.getNodeHashRange()[0];
      range_end = node.getNodeHashRange()[1];
      String currentData = host + ":" + port + " " + range_begin + "-"+ range_end + "\n";
      data = data.replace(oldData, currentData);
      setData(dataPath,data);
   }

   private void update_servers_metadata(){
      try{
         TextMessage latestMsg;
         for (ECSNode node:storageNodes){
            ecsSocket = new Socket (node.getNodeHost(), node.getNodePort());
            output= ecsSocket.getOutputStream();
            input = ecsSocket.getInputStream();
            sendMessage (new TextMessage("update_metadata"));
            latestMsg = receiveMessage();
            System.out.println(PROMPT + latestMsg.getMsg());
            try{
               input.close();
               output.close();
               ecsSocket.close();
            }catch(IOException e){
               e.printStackTrace();
            }
	      }
	   }catch(Exception e){
	      e.printStackTrace();
	   }
   }

   private void launchServer(ECSNode node, int cache_size, String cache_strategy) {
      Process proc;
      Runtime run = Runtime.getRuntime();
      String script;
      String output = "";
      String host = node.getNodeHost();
      int port = node.getNodePort();
      script = "script.sh " + host + " " + Integer.toString(port) + " " + cache_size + " " + cache_strategy;
      try{
         proc = run.exec(script);
      }catch(IOException e){
	      e.printStackTrace();
   	}
	   servers_launched++;
   }

   public void sendMessage(TextMessage msg) throws IOException {
      byte[] msgBytes = msg.getMsgBytes();
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
      while(read != 13 && reading) {
         /* carriage return */
	      /* if buffer filled, copy to msg array */
         if(index == BUFFER_SIZE) {
            if (msgBytes == null){
               tmp = new byte[BUFFER_SIZE];
               System.arraycopy(bufferBytes, 0, tmp, 0, BUFFER_SIZE);
            } else {
               tmp = new byte[msgBytes.length + BUFFER_SIZE];
               System.arraycopy(msgBytes, 0, tmp, 0, msgBytes.length);
               System.arraycopy(bufferBytes, 0, tmp, msgBytes.length,BUFFER_SIZE);
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
      logger.info("Receive message:\t '" + msg.getMsg() + "'");
      return msg;
   }

   public String getData(String path){
      String data = "";
      try{
         byte[] b = zk.getData(path, false,null);
         data = new String(b, "UTF-8");
      }catch(Exception e){
         System.out.println(e.getMessage());
      }
      return data;
   }

   private void removeFromCurrentPath(ECSNode node) {
      String data = getData(dataPath);
      String host=node.getNodeHost();
      String port=Integer.toString(node.getNodePort());
      String range_begin=node.getNodeHashRange()[0];
      String range_end=node.getNodeHashRange()[1];
      String currentData=host+":"+port+" "+range_begin+"-"+range_end+"\n";
      data = data.replace(currentData, "");
      setData(dataPath,data);
   }

   public boolean start(){
      try{
	      TimeUnit.MILLISECONDS.sleep(storageNodes.size() * 1000);
	   } catch(InterruptedException e){
	      e.printStackTrace();
      }

      int count = 0;
      TextMessage latestMsg;

      try{
         for(ECSNode node:storageNodes){
            ecsSocket = new Socket (node.getNodeHost(), node.getNodePort());
            output= ecsSocket.getOutputStream();
            input = ecsSocket.getInputStream();
            sendMessage (new TextMessage("start"));
            latestMsg= receiveMessage();
            if (!latestMsg.equals("")){
               count ++;
               System.out.println("Started node " + node.getNodePort());
            }
            try{
               input.close();
               output.close();
               ecsSocket.close();
            }catch(IOException e){
               e.printStackTrace();
            }

	      }
	   }catch(Exception e){
	      e.printStackTrace();
	   }

      System.out.println(PROMPT + count + " servers have been started" );
      return true;
   }

   public void removeNode(int index) throws Exception {
      if (index < 0 || index >= storageNodes.size()) {
	      throw new Exception("Node to remove not found");
      }		   

<<<<<<< HEAD
      add_storageNode_to_idle_nodes(index);
      ECSNode node = storageNodes.get(index);
      ECSNode successorNode = getSuccessorNode(node); 
      update_metadata_and_shutdown(node, successorNode);
=======
      
      ECSNode node = storageNodes.get(index);
      add_storageNode_to_idle_nodes(index);
      if (storageNodes.size()==0){
	 sendShutdownMessage(node);
      }
      else{
	 ECSNode successorNode = getSuccessorNode(node); 
	 update_metadata_and_shutdown(node, successorNode);
      }
>>>>>>> 5880def4d748feeac44e27fba04fff756f4ca1b1
   }

   private void  add_storageNode_to_idle_nodes(int index){
      ECSNode node = storageNodes.get(index);
      removeFromCurrentPath(node);
      storageNodes.remove(index);
      metadata.remove(node.getNodeHash());
      idleNodes.add(node);
   }
   
   private void update_metadata_and_shutdown(ECSNode node,ECSNode successorNode){
      updateMetadata(successorNode);
      update_servers_metadata();
      sendLockWriteMessage(node, successorNode);
      sendUnlockWriteMessage(node);	
      sendShutdownMessage(node); 
   }

   private void sendShutdownMessage(ECSNode node) {
      TextMessage latestMsg;
      try {
         ecsSocket = new Socket(node.getNodeHost(), node.getNodePort());
         output = ecsSocket.getOutputStream();
         input = ecsSocket.getInputStream();
         sendMessage (new TextMessage("shutdown"));
         latestMsg = receiveMessage();
         System.out.println(PROMPT + latestMsg.getMsg() + ". Node: " + node.getNodePort());
	      try {
            input.close();
            output.close();
            ecsSocket.close();
         } catch (IOException e) {
            e.printStackTrace();
	      }
         ecsSocket = null;	     
      } catch (Exception e) {
         e.printStackTrace();
      }
   }

   private void disconnectfromZK(){
      try{
         if (zk!= null){
            zk.close();
         }
      }catch(InterruptedException e){
	      System.out.println(e.getMessage()); 
	   }
   }

   public boolean stop(){
      TextMessage latestMsg;
      try {
         for(ECSNode node :storageNodes){
            ecsSocket = new Socket (node.getNodeHost(), node.getNodePort());
            output= ecsSocket.getOutputStream();
            input = ecsSocket.getInputStream();
            sendMessage (new TextMessage("stop"));
            latestMsg= receiveMessage();
            System.out.println(PROMPT + latestMsg.getMsg());
            try{
               input.close();
               output.close();
               ecsSocket.close();
            }catch(IOException e){
               e.printStackTrace();
            }
            ecsSocket = null;
	      }
	   }catch(Exception e){
	      e.printStackTrace();
      }
      
	   try{
	      TimeUnit.MILLISECONDS.sleep(storageNodes.size() * 1000);
	   }catch(InterruptedException e){
	      e.printStackTrace();
      }
      
      return true;
   }
   public boolean shutdownAll(){
      if (storageNodes != null){
         for (ECSNode node:storageNodes){
            sendShutdownMessage(node);
         }
         storageNodes.clear();
      }
      if (idleNodes != null){
	      idleNodes.clear();
      }
      if (metadata != null){
	      metadata.clear();
      }
      disconnectfromZK();

      return true;
   }

   public int getNumberofNodes(){
      return this.storageNodes.size();
   }
}
