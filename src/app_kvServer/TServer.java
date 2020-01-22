package app_kvServer;

import app_kvServer.KVServer;

public class TServer extends Thread {

    private KVServer kvServer;

    public TServer(KVServer kvServer) {
        this.kvServer = kvServer;
    }

    public void run() {
        //kvServer.clearStorage();
        kvServer.run();
    }
}