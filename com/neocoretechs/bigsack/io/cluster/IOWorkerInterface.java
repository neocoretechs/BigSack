package com.neocoretechs.bigsack.io.cluster;

import com.neocoretechs.bigsack.io.request.IoRequestInterface;
/**
 * This interface defines the contract for an IO worker thread. It really only has to queue requests
 * @author jg
 *
 */
public interface IOWorkerInterface {
	public void queueRequest(IoRequestInterface irf);
	public int getRequestQueueLength();
	//public void FseekAndWrite(long toffset, Datablock tblk) throws IOException;
}
