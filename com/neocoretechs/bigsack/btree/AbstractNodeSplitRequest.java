package com.neocoretechs.bigsack.btree;

import com.neocoretechs.bigsack.io.IoInterface;
import com.neocoretechs.bigsack.io.pooled.ObjectDBIO;
import com.neocoretechs.bigsack.io.request.IoRequestInterface;

public abstract class AbstractNodeSplitRequest implements IoRequestInterface {
	protected boolean wasInserted = false;
	protected BTreeKeyPage oldRoot;
	protected ObjectDBIO globalIO;
	protected Comparable newKey;
	protected Object newData;
	protected int insertPoint;
	protected static int keysToMove = BTreeKeyPage.MAXKEYS/3; // split to even 3 nodes, left, right, parent;
	
	@Override
	public void setIoInterface(IoInterface ioi) {
	}

	@Override
	public void setTablespace(int tablespace) {
	}
	
	public boolean wasNodeInserted() {
		return wasInserted;
	}

	public int getInsertionPoint() {
		return insertPoint;
	}

}
