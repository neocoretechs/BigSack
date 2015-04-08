package com.neocoretechs.bigsack.btree;

import java.io.IOException;

import com.neocoretechs.bigsack.io.pooled.ObjectDBIO;

/**
 * Request sent to NodeSplitThread operating on left nodes. The assumption is that a RightSplitThread
 * request is also in progress with the same data and sharing a synchronization lock on the former parent node.
 * All three are using a CyclicBarrier with 3 participants to determine a barrier synchronization point. 
 * Left and right specific node split requests to handle overflow nodes at intended insertion points to the BTree+.
 * These requests are passed to the NodeSplitThreads simultaneously in a situation where our insertion point is full.
 * We pass the same data to each and let them fight it out over modifying the previous root node via synchronization
 * critical sections. 
 * We tend to favor the nodes as insertion point versus the previous root as it keeps the tree more balanced
 * and delegates insertion logic to these threads. 
 * @author jg Copyright (C) NeoCoreTechs 2015
 *
 */
public final class LeftNodeSplitRequest extends AbstractNodeSplitRequest {
	private static boolean DEBUG = true;
	private BTreeKeyPage newLeft = null;

	/**
	 * The insertion point must be between the values for the key. It may or may not be on this
	 * thread. The data is passed to each of the two split node threads
	 * @param globalIO
	 * @param targetPage
	 * @param newKey
	 * @param newData
	 * @param insertPoint
	 */
	public LeftNodeSplitRequest(ObjectDBIO globalIO, BTreeKeyPage targetPage, Comparable newKey, Object newData, int insertPoint) {
		this.globalIO = globalIO;
		this.oldRoot = targetPage;
		this.newKey = newKey;
		this.newData = newData;
		this.insertPoint = insertPoint;
	}
	
	@Override
	public void process() throws IOException {
		newLeft = BTreeKeyPage.getPageFromPool(globalIO);
		// pull everything from the left nodes and move it down
		synchronized(oldRoot) {
		    // Slot is opened up, insert child node pointers into new parent
	        // parent should point left to new node
			// In this left case, the insert point is the actual point, no offset necessary
			// However, if it is out of range, the no processing happens here.
			int moveOffs = (insertPoint <= keysToMove ? 1 : 0); // if insert is on this node bump counter, if at end just tack it
			for(int i = 0; i < keysToMove+moveOffs; i++) {
				if( i == insertPoint ) {
					newLeft.keyArray[i] = newKey;
					newLeft.dataArray[i] = newData;
					newLeft.dataIdArray[i] = null;
					newLeft.dataUpdatedArray[i] = (newData != null);
					newLeft.nullPageArray(i);
					wasInserted = true;
				} else {
					if( i > insertPoint ) {
						BTreeMain.moveKeyData(oldRoot, i-1, newLeft, i, true);
						BTreeMain.moveChildData(oldRoot, i-1, newLeft, i, true);		
					} else {
						BTreeMain.moveKeyData(oldRoot, i, newLeft, i, true);
						BTreeMain.moveChildData(oldRoot, i, newLeft, i, true);
					}
				}
			}
			newLeft.numKeys = keysToMove + moveOffs;
			// See if leaf node, simply, no pointers out
			newLeft.mIsLeafNode = true;
			for( int i = 0; i < newLeft.numKeys; i++) {
				if( newLeft.pageIdArray[i] != -1L ) {
					newLeft.mIsLeafNode = false;
					break;
				}
			}
			newLeft.setUpdated(true);
			oldRoot.pageIdArray[keysToMove] = newLeft.pageId;
			oldRoot.pageArray[keysToMove] = newLeft;
			oldRoot.setUpdated(true);
		}
		// done with new left, leave synch, write it and leave oldRoot to its fate
		newLeft.putPage(globalIO);
	}
	/**
	 * Send back the pageId of the newly formed left page
	 */
	@Override
	public long getLongReturn() {
		return newLeft.pageId;
	}
	/**
	 * Send back the newly created left page, BTreeKeyPage from freechain
	 */
	@Override
	public Object getObjectReturn() {
		return newLeft;
	}


}
