package com.neocoretechs.bigsack.io.cluster;

import java.io.IOException;

import com.neocoretechs.bigsack.io.request.IoRequestInterface;

public interface MasterInterface {

	public void setMasterPort(int port);

	public void setSlavePort(int port);

	/**
	 * Set the prefix name of the remote worker node that this master communicates with
	 * This name plus the tablespace identifies each individual worker node
	 * In test mode, the local host is used for workers and master
	 * @param rname
	 */
	public void setRemoteWorkerName(String rname);

	/**
	 * Send request to remote worker
	 * @param iori
	 */
	public void send(IoRequestInterface iori);

	/**
	 * Open a socket to the remote worker located at 'remoteWorker' with the tablespace appended
	 * so each node is named [remoteWorker]0 [remoteWorker]1 etc
	 * @param fname
	 * @param create
	 * @return
	 * @throws IOException
	 */
	public boolean Fopen(String fname, boolean create) throws IOException;

}