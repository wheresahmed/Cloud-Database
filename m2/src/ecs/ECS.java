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
   private static final int MAXKEYLENGTH = 20;
   private static final int MAXVALUELENGTH = 1024 * 120;
   private static final int BUFFER_SIZE = 1024;
   private static final int DROP_SIZE = 1024 * BUFFER_SIZE;
   
   public ECS(int num_servers,int cache_size, String cache_strategy){
      zk_start();
      zk_connect();
     // ecs_nodes_initialize(num_servers);
      storage_cache_size=cache_size;
      storage_cache_strategy=cache_strategy;
     // launch_servers(cache_size,cache_strategy);
   }
   public void zk_start(){
     //check if zookeepr is runninging already. if not running, start a new one
      String script="lsof -w -n -i tcp:2181";
      Process process;
      Runtime runtime=Runtime.getRuntime();
      String output="";
      try{
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
      if(output==null){
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
      try{
	 zk=new ZooKeeper(ZK_CONN,ZK_TIMEOUT,new Watcher(){
	    public void process (WatchedEvent we){
	       if (we.getState()==KeeperState.SyncConnected){
		  connected_signal.countDown();
	       }
	    }
	 });
      }catch(IOException e){
	 logger.error("Client connection to Zookeeper failed");
      }
      try{
	 connected_signal.await();
      }catch(InterruptedException e){
	 e.printStackTrace();
      }
   }
}
