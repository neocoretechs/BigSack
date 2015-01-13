package com.neocoretechs.bigsack.io.cluster;

import com.neocoretechs.bigsack.io.request.IoResponseInterface;

public interface DistributedWorkerResponseInterface {
	public void queueResponse(IoResponseInterface iori);
	public int getMasterPort();
	public int getSlavePort();
}
