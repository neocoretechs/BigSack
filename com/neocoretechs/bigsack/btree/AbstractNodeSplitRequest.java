package com.neocoretechs.bigsack.btree;

import com.neocoretechs.bigsack.io.IoInterface;
import com.neocoretechs.bigsack.io.pooled.ObjectDBIO;
import com.neocoretechs.bigsack.io.request.IoRequestInterface;
/**
 * Superclass of node split requests encapsulating common data elements.
 * If a node becomes full, a split operation is performed during the insert operation.
 * The split operation transforms a full node with 2*T-1 elements into two nodes with T-1 elements each
 * and moves the median key of the two nodes into its parent node.
 * The elements left of the median (middle) element of the split node remain in the original node.
 * The new node becomes the child node immediately to the right of the median element that was moved to the parent node.
 * 
 * Example (T = 4):
 * 1.  R = | 1 | 2 | 3 | 4 | 5 | 6 | 7 |
 * 
 * 2.  Add key 8
 *   
 * 3.  R =         | 4 |
 *                 /   \
 *     | 1 | 2 | 3 |   | 5 | 6 | 7 | 8 |
 *
 * @author jg Copyright (C) NeoCoreTechs 2015
 *
 */
public abstract class AbstractNodeSplitRequest implements IoRequestInterface {
	protected boolean wasInserted = false;
	protected BTreeKeyPage oldRoot;
	protected ObjectDBIO globalIO;
	protected Comparable newKey;
	protected Object newData;
	protected int insertPoint;
	protected static int keysToMove = (BTreeKeyPage.MAXKEYS/2); // split to 3 nodes, left, right, original parent, move 1 key up;
	
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
