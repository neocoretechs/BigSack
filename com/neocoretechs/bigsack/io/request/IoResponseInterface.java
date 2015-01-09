package com.neocoretechs.bigsack.io.request;

import java.io.Serializable;


/**
 * This interface defines the contract between the remote worker node and the master
 * in the context of a response from the remote UDPWorker to the UDPMaster
 * @author jg
 *
 */
public interface IoResponseInterface extends Serializable {
	/**
	 * In the cases where we have a long value to return as in the offset of a block, 
	 * we can use this to get it stackwise
	 * @return
	 */
	public long getLongReturn();
	/**
	 * In the cases where we have an object to return, as in the case of an actual data block, 
	 * usually set up through the constructor of the request
	 * @return
	 */
	public Object getObjectReturn();
	
	public int getUUID();
	public void setUUID(int id);
}
