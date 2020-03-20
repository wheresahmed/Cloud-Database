package app_kvServer;

public enum ServerStateType {
	IDLE, /* server is idle */
	STARTED, /* server is started */
	SHUT_DOWN, /* server is shut down */
	STOPPED, /* default server status; server is stopped */
}