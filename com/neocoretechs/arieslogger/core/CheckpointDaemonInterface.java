package com.neocoretechs.arieslogger.core;


public interface CheckpointDaemonInterface {
	public int subscribe(LogFactory logFactory);
	public void serviceNow(int client);
	public void unsubscribe(int logFactory);
}