package com.neocoretechs.bigsack.io.request.cluster;


import java.io.Serializable;

import com.neocoretechs.bigsack.io.cluster.ClusterIOManager;


/**
 * Provides access to a UUID to be carried through a request response sequence in the cluster
 * This device lets us serialize and transport a request and identify it once a result returns.
 * There are one of these for each node in the sense that the IDs are monotonic from 0 in each
 * instance of this class.
 * The other function is to determine whether the subclassed message gets propagated to nodes, or is a local operation
 * or a split operation such as commit, where there is a local and remote component of different functionality.
 * @author jg
 *
 */
public abstract class AbstractClusterWork implements Serializable {
	private static final long serialVersionUID = 2631644867376911342L;
	private int u = 0;
	public AbstractClusterWork() {}
	public int newUUID() {
		 u = ClusterIOManager.getNextUUID();
		 return u;
	}
	
	public abstract boolean doPropagate();
	
	@Override
	public boolean equals(Object o) {
			return u == ((AbstractClusterWork)o).getUUID();
	}
	@Override
	public int hashCode() {
		return u;
	}
	public int getUUID() {
		return u;
	}

}
