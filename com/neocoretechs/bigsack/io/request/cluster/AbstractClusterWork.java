package com.neocoretechs.bigsack.io.request.cluster;


import java.io.Serializable;

import com.neocoretechs.bigsack.io.cluster.ClusterIOManager;


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
	public AbstractClusterWork() {}
	public int newUUID() {
		 u = ClusterIOManager.getNextUUID();
		 return u;
	 }

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
