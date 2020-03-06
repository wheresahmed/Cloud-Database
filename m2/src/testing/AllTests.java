package testing;

import java.io.IOException;

import app_kvServer.TServer;
import org.apache.log4j.Level;

import app_kvServer.KVServer;
import junit.framework.Test;
import junit.framework.TestSuite;
import logger.LogSetup;

import ecs.ECS;

import app_kvECS.ECSClient;


public class AllTests {

	static {
		try {
			new LogSetup("logs/testing/test.log", Level.OFF);
			ECS ecs = new ECS(1, 4, "LRU");
			ecs.start();

		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	
	public static Test suite() {
		TestSuite clientSuite = new TestSuite("Basic Storage ServerTest-Suite");
		clientSuite.addTestSuite(ConnectionTest.class);
		clientSuite.addTestSuite(InteractionTest.class); 
		//clientSuite.addTestSuite(AdditionalTest.class); 
		//clientSuite.addTestSuite(M2Test.class);
		return clientSuite;
	}
	
}
