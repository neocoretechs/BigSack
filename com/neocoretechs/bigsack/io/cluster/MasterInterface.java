package com.neocoretechs.bigsack.io.cluster;

import java.io.IOException;

import com.neocoretechs.bigsack.io.request.IoRequestInterface;
/**
 * For those nodes functioning as master, this interface provides the contract to
 * define the remote worker and queue a request
 * @author jg
 *
 */
public interface MasterInterface {


	public void setSlavePort(String port);
	public void setMasterPort(String port);
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