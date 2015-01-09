package com.neocoretechs.bigsack.io.request.cluster;


import java.io.Serializable;


/**
 * Provides access to a UUID to be carried through a request response sequence in the cluster
 * This device lets us serialize and transport a request and identify it once a result returns.
 * There are one of these for each node in the sense that the IDs are monotonic from 0 in each
 * instance of this class.
 * @author jg
 *
 */
public abstract class AbstractClusterWork implements Serializable {
	private static final long serialVersionUID = 2631644867376911342L;
	private int u = 0;
	private boolean response = true;
	public AbstractClusterWork() {}
	public int newUUID() {
		 ++u;
		 return u;
	 }

	@Override
	public boolean equals(Object o) {
			return u == ((AbstractClusterWork)o).getUUID();
	}
	
	public int getUUID() {
		return u;
	}
	/**
	 * Determine whether the master should compute/retain the Id of the request for processing a result.
	 * If no result is expected, it will prevent the necessity of removing the Id of the request form the requestQueue
	 * @return
	 */
	public boolean isResponse() {
		return response;
	}
	
	public void setResponse(boolean resp) { this.response = resp; }

}
