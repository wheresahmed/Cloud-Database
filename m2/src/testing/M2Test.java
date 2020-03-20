package testing;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import app_kvServer.KVServer;

import client.KVStore;
import ecs.ECS;

import shared.messages.KVMessage;

import junit.framework.TestCase;

import shared.messages.KVMessage.StatusType;

import app_kvServer.persistentDb;

import shared.messages.KVMessage;

public class M2Test extends TestCase {

	public void setUp(){

	}

	public void tearDown(){
		persistentDb.clearDb();
	}


	@Test
	public void test_ecs_initialize(){

		ECS ecs = new ECS(1, 4, "LRU");

		try{
			TimeUnit.MILLISECONDS.sleep(500);
		}catch(InterruptedException e){

		}

		Exception ex = null;
		try{
			KVStore kvclient = new KVStore("127.0.0.1", 50000);
			kvclient.connect();
		}catch(Exception e){
			ex = e;
		}

		assertTrue(ex==null);


		ecs.shutdownAll();
	}


	@Test
	public void test_addNodes(){

		ECS ecs = new ECS(1, 4, "LFU");
		Exception ex = null;


		try{
			ecs.addNodes(2, "LRU", 4);
			try{
				TimeUnit.MILLISECONDS.sleep(2000);
			}catch(InterruptedException e){

			}
		}catch(Exception e){
			ex = e;
		}

		assertTrue(ex==null);


		try{
			KVStore kc1 = new KVStore("127.0.0.1", 50000);
			KVStore kc2 = new KVStore("127.0.0.1", 50001);
			KVStore kc3 = new KVStore("127.0.0.1", 50002);

			kc1.connect();
			kc2.connect();
			kc3.connect();
		}catch(Exception e){
			ex = e;
		}

		assertTrue(ex==null);

		ecs.shutdownAll();
	}


	@Test
	public void test_add_and_remove_node(){

		ECS ecs = new ECS(1, 4, "LFU");
		Exception ex = null;


		try{
			ecs.addNode(4, "LRU");
			try{
				TimeUnit.MILLISECONDS.sleep(2000);
			}catch(InterruptedException e){

			}
		}catch(Exception e){
			ex = e;
		}

		assertTrue(ex==null);


		try{
			KVStore kc = new KVStore("127.0.0.1", 50000);
			ecs.removeNode(0);

			try{
				TimeUnit.MILLISECONDS.sleep(500);
			}catch(InterruptedException e){

			}


			kc.connect();
		}catch(Exception e){
			ex = e;
		}

		assertTrue(ex!=null);

		ecs.shutdownAll();
	}
	@Test
	public void test_start(){
		ECS ecs = new ECS(1, 4, "LRU");
		Exception ex = null;
		boolean start_status = ecs.start();

		KVMessage response = null;

		try{
			KVStore kc = new KVStore("127.0.0.1", 50000);
			kc.connect();

			response = kc.put("a", "b");

		}catch(Exception e){
			ex = e;
		}
		assertTrue(ex == null && (response.getStatus() == StatusType.PUT_SUCCESS|| response.getStatus()==StatusType.PUT_UPDATE));



		ecs.shutdownAll();	

	}

	 @Test
	public void test_remove(){
		ECS ecs = new ECS(1, 4, "FIFO");
		Exception ex = null;
		boolean start_status = ecs.start();

		KVMessage response = null;

		try{
			KVStore kc = new KVStore("127.0.0.1", 50000);

			ecs.removeNode(0);

			try{
				TimeUnit.MILLISECONDS.sleep(500);
			}catch(InterruptedException e){

			}

			kc.connect();

		}catch(Exception e){
			ex = e;
		}


		assertTrue(ex!=null);


		ecs.shutdownAll();	

	}

	@Test
	public void test_stop(){
		ECS ecs = new ECS(1, 4, "FIFO");
		Exception ex = null;
		boolean start_status = ecs.start();

		KVMessage response = null;

		try{
			KVStore kvclient = new KVStore("127.0.0.1", 50000);
			kvclient.connect();
			ecs.stop();


			response = kvclient.put("a", "b");

		}catch(Exception e){
			ex = e;
		}
		assertTrue(ex == null && response.getStatus() == StatusType.SERVER_STOPPED);

		ecs.shutdownAll();	

	}

	@Test
	public void removeNode_dataTransfer(){
		ECS ecs = new ECS(3, 4, "LRU");
		Exception ex = null;
		boolean start_status = ecs.start();

		KVMessage response1 = null;
		KVMessage response2 = null;

		try{
			KVStore kc = new KVStore("127.0.0.1", 50000);
			KVStore kc2 = new KVStore("127.0.0.1", 50001);
			KVStore kc3 = new KVStore("127.0.0.1", 50002);

			kc.put("1", "2");
			kc2.put("a", "b");

			//remove
			ecs.removeNode(0);
			ecs.removeNode(0);


			try{
				TimeUnit.MILLISECONDS.sleep(2000);
			}catch(InterruptedException e){

			}

			response1= kc3.get("a");
			response2= kc3.get("1");

		}catch(Exception e){
			ex = e;
		}


		assertTrue(ex==null && response1.getStatus()==StatusType.GET_SUCCESS && response2.getStatus()==StatusType.GET_SUCCESS);

		ecs.shutdownAll();	
	}
	
	@Test
	public void basic_metadata(){
		ECS ecs = new ECS(2, 4, "FIFO");
		Exception ex = null;
		boolean start_status = ecs.start();
		String dataPath = "/server";
                String meta1 = "127.0.0.1:50000 dcee0277eb13b76434e8dcd31a387709-358343938402ebb5110716c6e836f5a2\n";
                String meta2 = "127.0.0.1:50001 358343938402ebb5110716c6e836f5a2-dcee0277eb13b76434e8dcd31a387709\n";

		String golden = meta1 + meta2;
		String actual = ecs.getData(dataPath);
		assertTrue(ex==null && actual == golden);
		ecs.shutdownAll();	
	}
  
	@Test
	public void addNode_metadatUpdate(){
		ECS ecs = new ECS(2, 4, "FIFO");
		Exception ex = null;
		boolean start_status = ecs.start();
		String dataPath = "/server";
                String meta1 = "127.0.0.1:50000 dcee0277eb13b76434e8dcd31a387709-358343938402ebb5110716c6e836f5a2\n";
                String meta2 = "127.0.0.1:50001 358343938402ebb5110716c6e836f5a2-dcee0277eb13b76434e8dcd31a387709\n";

                String meta2_after = "127.0.0.1:50001 b3638a32c297f43aa37e63bbd839fc7e-dcee0277eb13b76434e8dcd31a387709\n";
                String meta3 = "127.0.0.1:50002 358343938402ebb5110716c6e836f5a2-b3638a32c297f43aa37e63bbd839fc7e\n";
		String beforeAdd_golden = meta1 + meta2;
		String afterAdd_golden = meta1 + meta2_after + meta3;
		String beforeAdd_actual = ecs.getData(dataPath); 
		assertTrue(ex==null && beforeAdd_actual == beforeAdd_golden);
                String afterAdd_actual = null;

		try{
			ecs.addNode(4, "FIFO");
			afterAdd_actual = ecs.getData(dataPath);
			try{
				TimeUnit.MILLISECONDS.sleep(2000);
			}catch(InterruptedException e){

			}
		}catch(Exception e){
			ex = e;
		}

		assertTrue(ex==null && afterAdd_actual == afterAdd_golden);


		ecs.shutdownAll();	
	}

	@Test
	public void removeNode_metadatUpdate(){
		ECS ecs = new ECS(3, 4, "FIFO");
		Exception ex = null;
		boolean start_status = ecs.start();
		String dataPath = "/server";
                String meta1 = "127.0.0.1:50000 dcee0277eb13b76434e8dcd31a387709-358343938402ebb5110716c6e836f5a2\n";
                String meta2 = "127.0.0.1:50001 b3638a32c297f43aa37e63bbd839fc7e-dcee0277eb13b76434e8dcd31a387709\n";
                String meta3 = "127.0.0.1:50002 358343938402ebb5110716c6e836f5a2-b3638a32c297f43aa37e63bbd839fc7e\n";
		String beforeRemove_golden = meta1 + meta2 + meta3;
                String meta1_after = "127.0.0.1:50000 b3638a32c297f43aa37e63bbd839fc7e-358343938402ebb5110716c6e836f5a2\n";
                String meta3_after = "127.0.0.1:50002 358343938402ebb5110716c6e836f5a2-b3638a32c297f43aa37e63bbd839fc7e\n";
		String afterRemove_golden = meta1_after + meta3_after;
		String beforeRemove_actual = ecs.getData(dataPath); 
		assertTrue(ex==null && beforeRemove_actual == beforeRemove_golden);
                String afterRemove_actual = null;

		try{
			ecs.removeNode(1); // Remove the second node added
			afterRemove_actual = ecs.getData(dataPath);
			try{
				TimeUnit.MILLISECONDS.sleep(2000);
			}catch(InterruptedException e){

			}
		}catch(Exception e){
			ex = e;
		}

		assertTrue(ex==null && afterRemove_actual == afterRemove_golden);


		ecs.shutdownAll();	
	}

/*
	///////PERFORMANCE TESTING /////////////////

	private String readFile(String path){

		String file = "";

		try{
			FileReader in = new FileReader(path);

			BufferedReader br = new BufferedReader(in);

			String line = "";


			line = br.readLine();


			while ( line != null) {
				file +=line;
				line = br.readLine();

			}

			in.close();
		}catch(IOException e){
			e.printStackTrace();
		}

		return file;
	}

	private void retryput(KVStore store, String serverAddress,int serverPort, String key,String value) throws Exception {
		//Retry if necessary
		KVMessage msg;

		do{
			String address = store.searchKey(key);


			if (!address.equals(serverAddress + ":" + Integer.toString(serverPort))){

				//disconnect from current server
				if (store!=null && store.isClientRunning()){
					store.disconnect();
					store=null;
				}

				//logger.info("Disconnecting ...");

				String [] addrArray= address.split(":");

				store = new KVStore(addrArray[0], Integer.parseInt(addrArray[1]));
				//logger.info("Connecting to " + addrArray[0] + ":" + addrArray[1] + "...");
				store.connect();
			}

			msg = store.put(key, value); //put/delete

		}while(msg.getStatus() == KVMessage.StatusType.SERVER_NOT_RESPONSIBLE);


		//Connect back to original server 

		if (store.port != serverPort || !store.address.equals(serverAddress)){
			if (store!=null && store.isClientRunning()){
				store.disconnect();
				store=null;
			}

			store = new KVStore(serverAddress, serverPort);
			//logger.info("Connecting to " + addrArray[0] + ":" + addrArray[1] + "...");
			store.connect();

		}
	}

	private boolean retryget(KVStore store, String serverAddress,int serverPort, String key) throws Exception{
		//Retry if necessary
		KVMessage msg;

		do{
			String address = store.searchKey(key);
			//System.out.println(address);

			//			System.out.println(serverAddress + ":" + Integer.toString(serverPort));
			//			System.out.println(address);

			if (!address.equals(serverAddress + ":" + Integer.toString(serverPort))){

				//disconnect from current server
				if (store!=null && store.isClientRunning()){
					store.disconnect();
					store=null;
				}

				//System.out.println(PROMPT + "Disconnecting ...");

				String [] addrArray= address.split(":");

				//System.out.println(PROMPT + "Connecting to " + addrArray[0] + ":" + addrArray[1] + "...");
				store = new KVStore(addrArray[0], Integer.parseInt(addrArray[1]));
				//logger.info("Connecting to " + addrArray[0] + ":" + addrArray[1] + "...");
				store.connect();
			}

			msg = store.get(key); //put/delete

		}while(msg.getStatus() == KVMessage.StatusType.SERVER_NOT_RESPONSIBLE);


		//Connect back to original server 

		if (store.port != serverPort || !store.address.equals(serverAddress)){
			if (store!=null && store.isClientRunning()){
				store.disconnect();
				store=null;
			}

			store = new KVStore(serverAddress, serverPort);
			//logger.info("Connecting to " + addrArray[0] + ":" + addrArray[1] + "...");
			store.connect();

		}

		return true;
	}


	//Load testing 
	@Test
	//putLoadRatio: ratio of load that are puts
	//getLoadRatio: ratio of load that are gets
	//load: number of combined put and get requests (10000)

	public void loads(int cacheSize, String cacheStrategy, int num_clients, int num_servers){

		int load = 1000;
		int putload = (int) (0.5 * load);
		int getload = (int) (0.5 * load);

		//clear disk
		persistentDb.clearDb();

		KVStore[] clients = new KVStore[num_clients] ;

		//launch the ecs and servers
		ECS ecs = new ECS(num_servers, cacheSize, cacheStrategy);

		//start the servers
		ecs.start();

		String path = "/nfs/ug/homes-0/s/shakerba/ece419/email_dataset/";

		long startTime = 0;
		long endTime = 0;
		long putstart = 0;
		long putend = 0;
		long getstart = 0;
		long getend = 0;

		//connect all clients to same server
		if(num_clients > num_servers ){

			//initilize clients
			for (int i = 0; i < num_clients; i ++){
				clients[i] = new KVStore("127.0.0.1",50000);
				try{
					clients[i].connect();
				}catch(Exception e){
					e.printStackTrace();
				}
			}

			//Spread the gets and puts across all clients

			startTime = System.nanoTime();
			//puts

			putstart = System.nanoTime();
			for (int i = 0 ; i < putload; i ++ ){
				//String value = readFile(path + Integer.toString(i+1) + ".");
				String value = Integer.toString(i);
				int j = i % num_clients;
				if(clients[j] != null && clients[j].isClientRunning()){
					try{
						retryput(clients[j],"127.0.0.1",50000,Integer.toString(i+1),value);
					}catch(Exception e){
						e.printStackTrace();
					}
				}
			}
			putend = System.nanoTime();

			getstart = System.nanoTime();
			//gets
			for(int i = 0; i < getload ; i ++){
				int j = i % num_clients;
				if(clients[j] != null && clients[j].isClientRunning()){
					try{
						retryget(clients[j],"127.0.0.1",50000,Integer.toString(i+1));
					}catch(Exception e){
						e.printStackTrace();
					}
				}
			}

			getend = System.nanoTime();

			endTime = System.nanoTime();

			//spread clients across all servers 
		}else {

			for (int i = 0; i < num_clients; i ++){
				clients[i] = new KVStore("127.0.0.1",50000 + i);
				try{
					clients[i].connect();
				}catch(Exception e){
					e.printStackTrace();
				}
			}

			//Spread the gets and puts across all clients

			startTime = System.nanoTime();
			//puts

			putstart = System.nanoTime();
			for (int i = 0 ; i < putload; i ++ ){
				//String value = readFile(path + Integer.toString(i+1) + ".");
				String value = Integer.toString(i);
				int j = i % num_clients;
				if(clients[j] != null && clients[j].isClientRunning()){
					try{
						retryput(clients[j],"127.0.0.1",50000 + j,Integer.toString(i+1),value);
					}catch(Exception e){
						e.printStackTrace();
					}
				}
			}

			putend = System.nanoTime();

			//gets

			getstart = System.nanoTime();
			for(int i = 0; i < getload ; i ++){
				int j = i % num_clients;
				if(clients[j] != null && clients[j].isClientRunning()){
					try{
						retryget(clients[j],"127.0.0.1",50000 + j ,Integer.toString(i+1));
					}catch(Exception e){
						e.printStackTrace();
					}
				}
			}

			getend = System.nanoTime();

			endTime = System.nanoTime();


		}

		//spit out some metrics 

		long timeElapsedNano = endTime - startTime;
		double timeElapsedSeconds = (timeElapsedNano * 1.0 ) /1000000000 ;
		double putElapsedSeconds = ((putend-putstart)*1.0) / 1000000000;
		double getElapsedSeconds = ((getend-getstart)*1.0) / 1000000000;

		System.out.println("The "+ cacheStrategy + " storage service with " + cacheSize + " cache size throughput is " + load/timeElapsedSeconds + " req/sec, overall latency " 
				+ timeElapsedSeconds + " secs, put throughput " + 
				putload/putElapsedSeconds +" req/sec, and get throughput is " +  getload/getElapsedSeconds + " req/sec for " + num_clients +" clients "+ num_servers + " servers.");


		//disconnect clients from all servers

		for(int i = 0; i < num_clients ; i ++){
			clients[i].disconnect();
			clients[i] = null;
		}

		//shut servers down

		ecs.shutdownAll();

	}


	//Performance testing
	//Using a cache size of 500
	@Test
	public void testPerformance() {

		int[] cacheSize = {50,400};
		String[] strat= {"LRU","LFU","FIFO"};
		int[] clients = {5,5,5,25,25,25};
		int[] servers = {1,15,50,1,25,50};

		for (int i= 0; i < 2; i++){
			for (int j =0 ; j < 3; j ++){
				for(int k = 0; k < 6; k ++){
					loads(cacheSize[i],strat[j],clients[k],servers[k]);
				}
			}
		}

	}


	public void testaddremoveNodeTest(){

		int n = 20;

		persistentDb.clearDb();

		//launch the ecs and servers
		ECS ecs = new ECS(1, 5, "LRU");

		//start the servers
		ecs.start();

		long start = 0;
		long end = 0;

		start = System.nanoTime();
		try{
			ecs.addNodes(n,"LRU", 5);
		}catch (Exception e){
			e.printStackTrace();
		}
		end = System.nanoTime();


		long timeElapsedNano = end - start;
		double timeElapsedSeconds = (timeElapsedNano * 1.0 ) /1000000000 ;

		System.out.println("It takes " + timeElapsedSeconds + " seconds.");


		start = System.nanoTime();

		for(int i = 0; i < n ; i++){
			try{
				ecs.removeNode(0);
			}catch (Exception e){
				e.printStackTrace();
			}
		}

		end = System.nanoTime();

		timeElapsedNano = end - start;
		timeElapsedSeconds = (timeElapsedNano * 1.0 ) /1000000000 ;

		System.out.println("It takes " + timeElapsedSeconds + " seconds.");

		ecs.shutdownAll();



	}





   */



}









