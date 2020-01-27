package testing;

import app_kvServer.TServer;
import org.junit.Test;

import app_kvServer.KVServer;

import client.KVStore;

import shared.messages.KVMessage;

import junit.framework.TestCase;

import shared.messages.KVMessage.StatusType;

import app_kvServer.persistentDb;

public class AdditionalTest extends TestCase {

	private KVStore kvClientLFU;
	private KVStore kvClientLRU;
	private KVStore kvClientFIFO;
	private KVStore kvClientFIFOTwin;

	private KVStore perfClientLFU ;
	private KVStore perfClientLRU ;
	private KVStore perfClientFIFO ;

	public void setUp() {

		try{
			new TServer(new KVServer(60000, 3, "LFU")).start();

		}catch( Exception e){
		}

		try{
			new TServer(new KVServer(40000, 3, "LRU")).start();

		}catch( Exception e){
		}

		try{
			new TServer(new KVServer(50000, 3, "FIFO")).start();

		}catch( Exception e){
		}

		kvClientLFU = new KVStore("localhost", 60000);
		kvClientLRU = new KVStore("localhost", 40000);
		kvClientFIFO = new KVStore("localhost", 50000);
		kvClientFIFOTwin = new KVStore("localhost", 50000);

		try {
			kvClientLFU.connect();
		} catch (Exception e) {
		}

		try {
			kvClientLRU.connect();
		} catch (Exception e) {
		}

		try {
			kvClientFIFO.connect();
		} catch (Exception e) {
		}
		try {
			kvClientFIFOTwin.connect();
		} catch (Exception e) {
		}

	}

	public void tearDown() {
		kvClientLFU.disconnect();
		kvClientLRU.disconnect();
		kvClientFIFO.disconnect();
		kvClientFIFOTwin.disconnect();
		persistentDb.clearDb();
	}

	@Test
	public void testLRU() {
		String[] keys = {"blah","hi","car","flower"};
		String[] values = {"a","b","c","d"};

		KVMessage response = null;
		Exception ex = null;

		for(int i = 0; i < 3; i++){

			try {
				response = kvClientLRU.put(keys[i], values[i]);
			} catch (Exception e) {
				ex = e;
			}

			assertTrue(ex == null && response.getStatus() == StatusType.PUT_SUCCESS);
		}

		for (int i = 0 ; i < 5; i ++){
			try {
				response = kvClientLRU.get(keys[0]);
			} catch (Exception e) {
				ex = e;
			}

			assertTrue(ex == null && response.getStatus() == StatusType.GET_SUCCESS);
		}

		for(int i = 0; i < 3; i++){

			try {
				response = kvClientLRU.get(keys[i]);
			} catch (Exception e) {
				ex = e;
			}

			assertTrue(ex == null && response.getStatus() == StatusType.GET_SUCCESS);
		}

		try {
			response = kvClientLRU.put(keys[3], values[3]);
		} catch (Exception e) {
			ex = e;
		}


		assertTrue(ex == null && response.getStatus() == StatusType.PUT_SUCCESS);

		try {
			response = kvClientLRU.get(keys[0]);
		} catch (Exception e) {
			ex = e;
		}

		assertTrue(ex == null && response.getStatus() == StatusType.GET_SUCCESS);


	}

	@Test
	public void testFIFO() {

		String[] keys = {"blah","hi","car","flower"};
		String[] values = {"a","b","c","d"};

		KVMessage response = null;
		Exception ex = null;

		for(int i = 0; i < 3; i++){

			try {
				response = kvClientFIFO.put(keys[i], values[i]);
			} catch (Exception e) {
				ex = e;
			}

			assertTrue(ex == null && response.getStatus() == StatusType.PUT_SUCCESS);
		}

		for (int i = 0 ; i < 5; i ++){
			try {
				response = kvClientFIFO.get(keys[0]);
			} catch (Exception e) {
				ex = e;
			}

			assertTrue(ex == null && response.getStatus() == StatusType.GET_SUCCESS);
		}

		for(int i = 0; i < 3; i++){

			try {
				response = kvClientFIFO.get(keys[i]);
			} catch (Exception e) {
				ex = e;
			}

			assertTrue(ex == null && response.getStatus() == StatusType.GET_SUCCESS);
		}

		try {
			response = kvClientFIFO.put(keys[3], values[3]);
		} catch (Exception e) {
			ex = e;
		}


		assertTrue(ex == null && response.getStatus() == StatusType.PUT_SUCCESS);

		try {
			response = kvClientFIFO.get(keys[0]);
		} catch (Exception e) {
			ex = e;
		}

		assertTrue(ex == null && response.getStatus() == StatusType.GET_SUCCESS);


	}
	@Test
	public void testLFU() {

		String[] keys = {"blah","hi","car","flower"};
		String[] values = {"a","b","c","d"};

		KVMessage response = null;
		Exception ex = null;

		for(int i = 0; i < 3; i++){

			try {
				response = kvClientLFU.put(keys[i], values[i]);
			} catch (Exception e) {
				ex = e;
			}

			assertTrue(ex == null && response.getStatus() == StatusType.PUT_SUCCESS);
		}


		for(int i = 0; i < 3; i++){

			try {
				response = kvClientLFU.get(keys[i]);
			} catch (Exception e) {
				ex = e;
			}

			assertTrue(ex == null && response.getStatus() == StatusType.GET_SUCCESS);
		}
		for (int i = 0 ; i < 5; i ++){
			try {
				response = kvClientLFU.get(keys[0]);
			} catch (Exception e) {
				ex = e;
			}

			assertTrue(ex == null && response.getStatus() == StatusType.GET_SUCCESS);
		}

		try {
			response = kvClientLFU.put(keys[3], values[3]);
		} catch (Exception e) {
			ex = e;
		}


		assertTrue(ex == null && response.getStatus() == StatusType.PUT_SUCCESS);

		try {
			response = kvClientLFU.get(keys[1]);
		} catch (Exception e) {
			ex = e;
		}

		assertTrue(ex == null && response.getStatus() == StatusType.GET_SUCCESS);


	}


	/* Tests the following cases:
	 * Case 1: All key-value pairs are in storage. Including evicted entries.
	 */
	@Test
	public void testBasicPersistence() {

		String[] keys = {"a", "c", "e", "g", "j"};
		String[] values = {"1", "2", "3", "4", "5"};

		KVMessage response = null;
		Exception ex = null;

		for(int i = 0; i < keys.length; i++){

			try {
				response = kvClientFIFO.put(keys[i], values[i]);
			} catch (Exception e) {
				ex = e;
			}

			assertTrue(ex == null && (response.getStatus() == StatusType.PUT_SUCCESS));
		}
		for(int i = 0; i < keys.length; i++){

			try {
				response = kvClientFIFO.get(keys[i]);
			} catch (Exception e) {
				ex = e;
			}

			assertTrue(ex == null && response.getStatus() == StatusType.GET_SUCCESS);
		}
		assertTrue(persistentDb.find("a") != null);
		assertTrue(persistentDb.find("c") !=null);
		assertTrue(persistentDb.find("e") !=null);
		assertTrue(persistentDb.find("g") !=null);
		assertTrue(persistentDb.find("j") !=null);
	}
	/* Tests the following cases:
	 * Case 2: Entry updated in file correctly
	 */
	@Test
	public void testKeyValueUpdateFile() {

		// File should have all the entries and a's value as z
		String[] keys = {"a", "c", "e", "g", "a"};
		String[] values = {"b", "d", "f", "h", "z"};

		KVMessage response = null;
		Exception ex = null;

		for(int i = 0; i < keys.length; i++){

			try {
				response = kvClientFIFO.put(keys[i], values[i]);
			} catch (Exception e) {
				ex = e;
			}

			assertTrue(ex == null && (response.getStatus() == StatusType.PUT_SUCCESS ||
					response.getStatus() == StatusType.PUT_UPDATE));
		}

		values[0] = "z";
		for(int i = 0; i < keys.length; i++){

			try {
				response = kvClientFIFO.get(keys[i]);
			} catch (Exception e) {
				ex = e;
			}

			assertTrue(ex == null && response.getStatus() == StatusType.GET_SUCCESS
					&& response.getValue().equals(values[i]));
		}
		assertTrue(persistentDb.find("a") != null);

		assertTrue(persistentDb.find("c") != null);
		assertTrue(persistentDb.find("e") != null);
		assertTrue(persistentDb.find("g") != null);
	}


	/* Tests the following cases:
	 * Case 2: File removes key-value pairs for put <Key, "">
	 */
	@Test
	public void testKeyValueRemoveFile() {

		// File should have all the entries and a's value as z
		String[] keys = {"a", "b", "c", "d"};
		String[] values = {"1", "2", "3", "4"};

		KVMessage response = null;
		Exception ex = null;

		for(int i = 0; i < keys.length; i++){

			try {
				response = kvClientFIFO.put(keys[i], values[i]);
			} catch (Exception e) {
				ex = e;
			}

			assertTrue(ex == null && (response.getStatus() == StatusType.PUT_SUCCESS));
		}
		for(int i = 0; i < keys.length; i++){

			try {
				response = kvClientFIFO.get(keys[i]);
			} catch (Exception e) {
				ex = e;
			}

			assertTrue(ex == null && response.getStatus() == StatusType.GET_SUCCESS);
		}
		try {
			response = kvClientFIFO.put(keys[keys.length-1], "");
		} catch (Exception e) {
			ex = e;
		}

		assertTrue(ex == null && (response.getStatus() == StatusType.DELETE_SUCCESS));
		assertTrue(persistentDb.find("a") != null);
		assertTrue(persistentDb.find("b") != null);
		assertTrue(persistentDb.find("c") != null);
		assertTrue(persistentDb.find("d") == null);
	}
	/* Tests the following cases:
	 * Case 2: File removes key-value pairs for put <Key, "">
	 */
	@Test
	public void testNullKeyValueRemoveFile() {

		// File should have all the entries and a's value as z
		String[] keys = {"a", "b", "c", "d"};
		String[] values = {"1", "2", "3", "4"};

		KVMessage response = null;
		Exception ex = null;

		for(int i = 0; i < keys.length; i++){

			try {
				response = kvClientFIFO.put(keys[i], values[i]);
			} catch (Exception e) {
				ex = e;
			}

			assertTrue(ex == null && (response.getStatus() == StatusType.PUT_SUCCESS));
		}
		for(int i = 0; i < keys.length; i++){

			try {
				response = kvClientFIFO.get(keys[i]);
			} catch (Exception e) {
				ex = e;
			}

			assertTrue(ex == null && response.getStatus() == StatusType.GET_SUCCESS);
		}
		try {
			response = kvClientFIFO.put(keys[keys.length-1], "null");
		} catch (Exception e) {
			ex = e;
		}

		assertTrue(ex == null && (response.getStatus() == StatusType.DELETE_SUCCESS));
		assertTrue(persistentDb.find("a") != null);
		assertTrue(persistentDb.find("b") != null);
		assertTrue(persistentDb.find("c") != null);
		assertTrue(persistentDb.find("d") == null);
	}


	/* Tests the following cases:
	 * Case 4: Checks if the keys and values are parsed correctly in the file
	 * Sub-cases:
	 *   1. File Contains Distinct Pairs after put requests: <Key1, Value1>, <Key2, Key1>
	 *   2. File contains <Key3, Value3>. Put <Value3, ""> return error.
	 */
	@Test
	public void testKeyValueParsingInFile() {

		// File should have all the entries and a's value as z
		String[] keys = {"a", "b", "c"};
		String[] values = {"1", "2", "3"};

		KVMessage response = null;
		Exception ex = null;

		for(int i = 0; i < keys.length; i++){

			try {
				response = kvClientFIFO.put(keys[i], values[i]);
			} catch (Exception e) {
				ex = e;
			}

			assertTrue(ex == null && (response.getStatus() == StatusType.PUT_SUCCESS ||
					response.getStatus() == StatusType.PUT_UPDATE));
		}
		assertTrue(persistentDb.find("a") != null);
		assertTrue(persistentDb.find("b") != null);
		assertTrue(persistentDb.find("c") != null);
		for(int i = 0; i < keys.length; i++){

			try {
				response = kvClientFIFO.get(keys[i]);
			} catch (Exception e) {
				ex = e;
			}

			assertTrue(ex == null && response.getStatus() == StatusType.GET_SUCCESS
					&& response.getValue().equals(values[i]));
		}
		try {
			response = kvClientFIFO.put("d", "");
		} catch (Exception e) {
			ex = e;
		}

		assertTrue(ex == null && (response.getStatus() == StatusType.DELETE_ERROR));
	}

	/* Tests the following cases:
	 * Case 5: Multiple clients putting the same key with the same server
	 *
	 */
	@Test
	public void testMultiClientPersistenceTest() {

		// File should have all the entries and a's value as z
		String[] keys = {"1", "2", "3"};
		String[] values = {"1", "2", "3"};

		KVMessage response = null;
		KVMessage response2 = null;
		Exception ex = null;
		Exception ex2 = null;

		for(int i = 0; i < keys.length; i++){

			try {
				if (i % 2 == 0)
					response = kvClientFIFO.put(keys[i], values[i]);
				else
					response = kvClientFIFOTwin.put(keys[i], values[i]);
			} catch (Exception e) {
				ex = e;
			}

			assertTrue(ex == null && (response.getStatus() == StatusType.PUT_SUCCESS ||
					response.getStatus() == StatusType.PUT_UPDATE));
		}
		assertTrue(persistentDb.find("1") != null);
		assertTrue(persistentDb.find("2") != null);
		assertTrue(persistentDb.find("3") != null);

		try {
			response = kvClientFIFO.put(keys[1], "blah");
		} catch (Exception e) {
			ex = e;
		}

		assertTrue(ex == null &&
				(response.getStatus() == StatusType.PUT_SUCCESS || response.getStatus() == StatusType.PUT_UPDATE));
		assertTrue(persistentDb.find("2") !=null);
	}

	//Bad Values
	@Test
	public void testValues() {

		String[] keys = {"a"};

		int MAX = 1024 * 120;
		String[] values = {""};

		for(int i =0 ; i < MAX; i++){
			values[0]+="a";
		}


		KVMessage response = null;
		Exception ex = null;

		for(int i = 0; i < 1; i++){

			try {
				response = kvClientFIFO.put(keys[i], values[i]);
			} catch (Exception e) {
				ex = e;
			}

			//value too large
			assertTrue(ex == null && response.getStatus() == StatusType.PUT_ERROR);
		}

	}

	public KVStore getClient (String cacheType){

		if(cacheType.equals("LRU")){
			return perfClientLRU;
		}else if(cacheType.equals("LFU")){
			return perfClientLFU;
		}else if(cacheType.equals("FIFO")){
			return perfClientFIFO;
		}else{
			//default cacheType is LRU
			return perfClientLRU;
		}

	}


	//Load testing
	@Test
	//putLoadRatio: ratio of load that are puts
	//getLoadRatio: ratio of load that are gets
	//load: number of combined put and get requests (10000)

	public void loads(double putLoadRatio, double getLoadRatio){

		int load = 10000;

		//clear disk
		persistentDb.clearDb();

		//set up new performance server
		try{
			new TServer(new KVServer(60300, 10, "LFU")).start();

		}catch( Exception e){
		}

		try{
			new TServer(new KVServer(40300, 10, "LRU")).start();
		}catch( Exception e){
		}

		try{
			new TServer(new KVServer(50300, 10, "FIFO")).start();

		}catch( Exception e){
		}

		//set up clients to connect to performance servers
		perfClientLFU = new KVStore("localhost", 60300);
		perfClientLRU = new KVStore("localhost", 40300);
		perfClientFIFO = new KVStore("localhost", 50300);


		//set up client-server connections to each server
		try {
			perfClientLFU.connect();
		} catch (Exception e) {
		}

		try {
			perfClientLRU.connect();
		} catch (Exception e) {
		}

		try {
			perfClientFIFO.connect();
		} catch (Exception e) {
		}


		//Performance setup
		KVMessage response = null;
		Exception ex = null;

		int putLoad = (int) (putLoadRatio * load);
		int getLoad = (int) (getLoadRatio * load);
		String[] cacheType = {"FIFO","LRU","LFU"};


		//Get set of keys and values

		int numofKeys = putLoad;

		if (putLoad < getLoad){
			numofKeys = getLoad;
		}

		String keys[] = new String[numofKeys];
		String values[] = new String[numofKeys];

		for(int i = 0; i < numofKeys; i++){
			keys[i] = Integer.toString(i);
			values[i] = Integer.toString(numofKeys - i);
		}


		//cache type
		for (int i = 0 ; i < 3; i ++){
			long startTime = System.nanoTime();

			long getstart = 0;
			long getend = 0;
			long putstart = System.nanoTime();


			//put load
			for (int j = 0; j < putLoad; j ++){

				try {
					response = getClient(cacheType[i]).put(keys[j], values[j]);
				} catch (Exception e) {
					ex = e;
				}

				assertTrue(ex == null && (response.getStatus() == StatusType.PUT_SUCCESS || response.getStatus() == StatusType.PUT_UPDATE));
			}

			long putend = System.nanoTime();


			if( getLoad > putLoad){

				//get load

				getstart = System.nanoTime();

				for (int l = 0; l < 4; l ++){
					for (int k = 0; k < putLoad; k ++){
						try {
							response = getClient(cacheType[i]).get(keys[k]);
						} catch (Exception e) {
							ex = e;
						}

						assertTrue(ex == null && response.getStatus() == StatusType.GET_SUCCESS);
					}
				}

				getend = System.nanoTime();
			}
			else{
				//get load

				getstart = System.nanoTime();

				for (int k = 0; k < getLoad; k ++){
					try {
						response = getClient(cacheType[i]).get(keys[k]);
					} catch (Exception e) {
						ex = e;
					}

					assertTrue(ex == null && response.getStatus() == StatusType.GET_SUCCESS);
				}

				getend = System.nanoTime();
			}

			long endTime = System.nanoTime();

			long timeElapsedNano = endTime - startTime;

			double timeElapsedSeconds = (timeElapsedNano * 1.0 ) /1000000000 ;
			double putElapsedSeconds = ((putend-putstart)*1.0) / 1000000000;
			double getElapsedSeconds = ((getend-getstart)*1.0) / 1000000000;



			System.out.println(cacheType[i] + " throughput is " + load/timeElapsedSeconds + " req/sec, overall latency "
					+ timeElapsedSeconds + " secs, put throughput " +
					putLoad/putElapsedSeconds +" req/sec, and get throughput is " +  getLoad/getElapsedSeconds + " req/sec" +"put load: "+ putLoad+"get load:" +getLoad);

			persistentDb.clearDb();
			getClient(cacheType[i]).disconnect();
		}
	}


	//Performance testing
	//Using a cache size of 500

	@Test
	public void testPerformance() {

		double[] putloads = {0.5,0.8,0.2};
		double[] getloads = {0.5,0.2,0.8};

		for (int i =0 ; i < 3; i ++){
			loads(putloads[i],getloads[i]);
		}


	}





}
