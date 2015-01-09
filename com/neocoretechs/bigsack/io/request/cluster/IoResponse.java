package com.neocoretechs.bigsack.io.request.cluster;

import com.neocoretechs.bigsack.io.request.IoRequestInterface;
import com.neocoretechs.bigsack.io.request.IoResponseInterface;
/**
 * An IOResponse is returned from a worker node with the results of that
 * nodes operation identified by the uuid of the response which matches the uuid of the 
 * ConcurrentHashTable of requests in the UDPMaster.
 * The payload portion of the original request is updated with the values to conform
 * with the pattern of the multithreaded IO manager model. Once
 * the original request is updated with the values from the remote processing all
 * further operation should be identical to the basic multithreaded model
 * @author jg
 *
 */
public class IoResponse implements IoResponseInterface {
	private static final long serialVersionUID = 2672078363281627844L;
	private Object responseObject = null;
	private long responseLong = -1L;
	private int uuid;
	public IoResponse() {}
	public IoResponse(IoRequestInterface request) {
		this.responseLong = request.getLongReturn();
		this.responseObject = request.getObjectReturn();
		this.uuid = ((AbstractClusterWork)request).getUUID();
	}
	@Override
	public long getLongReturn() {
		return responseLong;
	}
	@Override
	public Object getObjectReturn() {
		return responseObject;
	}
	@Override
	public int getUUID() {
		return uuid;
	}
	@Override
	public void setUUID(int id) {	
	}

}
