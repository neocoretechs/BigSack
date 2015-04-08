package com.neocoretechs.bigsack.btree;

import java.io.IOException;

import com.neocoretechs.bigsack.io.pooled.ObjectDBIO;

/**
 * Request sent to NodeSplitThread operating on right nodes. The assumption is that a LeftSplitThread
 * request is also in progress with the same data and sharing a synchronization lock on the former parent node.
 * All three are using a CyclicBarrier with 3 participants to determine a barrier synchronization point. 
 * @author jg Copyright (c) NeoCoreTechs 2015
 *
 */
public final class RightNodeSplitRequest extends AbstractNodeSplitRequest {
	private static boolean DEBUG = true;
	BTreeKeyPage newRight = null;
	private int localInsert;
	// from rootOffs to MAXKEYS is range of keys to extract
	private static int rootOffs = BTreeKeyPage.MAXKEYS - keysToMove; // the place in root to start pulling keys

	public RightNodeSplitRequest(ObjectDBIO globalIO, BTreeKeyPage targetPage, Comparable newKey, Object newData, int insertPoint) {
				this.globalIO = globalIO;
				this.oldRoot = targetPage;
				this.newKey = newKey;
				this.newData = newData;
				this.insertPoint = insertPoint;
			    // Slot is opened up, insert child node pointers into new parent
		        // parent should point right to new node.
				// localInsert is the offset for insertion should the key go here. If its say 2 on the full node
				// then here it should be 0 as slot 2 comes here, so calculate the offset
				localInsert = insertPoint - rootOffs;
				// if localInsert is < 0 no insert here, yet we still have to split
	}
	/**
	 * Attempt insert, we may not have it on this node. A slit is always assumed necessary if we get here.
	 */
	@Override
	public void process() throws IOException {
		newRight = BTreeKeyPage.getPageFromPool(globalIO);

		// if its less then 0, no insert bere
		// pull everything from the right nodes and move it down
		synchronized(oldRoot) {
			// have to decide whether to tack it on the end or move the first element by 1 to offset
			int moveOffs = (localInsert >= 0 && localInsert <= keysToMove ? 1 : 0); // if insert is on this node bump counter, if at end just tack it
			for(int i = 0; i < keysToMove+moveOffs; i++) {
				// if localInsert < 0 no insert, we still have to split though
				if(localInsert >= 0 && i == localInsert ) {
					newRight.keyArray[i] = newKey;
					newRight.dataArray[i] = newData;
					newRight.dataIdArray[i] = null;
					newRight.dataUpdatedArray[i] = (newData != null);
					newRight.nullPageArray(i);
					wasInserted = true;
				} else {
					if( localInsert >= 0 && i > localInsert ) {
						// If we have inserted we have to offset our source by -1 to keep up with target counter
						BTreeMain.moveKeyData(oldRoot, rootOffs+i-1, newRight, i, true);
						BTreeMain.moveChildData(oldRoot, rootOffs+i-1, newRight, i, true);		
					} else {
						BTreeMain.moveKeyData(oldRoot, rootOffs+i, newRight, i, true);
						BTreeMain.moveChildData(oldRoot, rootOffs+i, newRight, i, true);
					}
				}
			}
			// moveOffs is 0 or 1 for new key insertion at this node, or not.
			newRight.numKeys = keysToMove + moveOffs;
			// See if leaf node, simply, no pointers out
			newRight.mIsLeafNode = true;
			for( int i = 0; i < newRight.numKeys; i++) {
				if( newRight.pageIdArray[i] != -1L ) {
					newRight.mIsLeafNode = false;
					break;
				}
			}
			newRight.setUpdated(true);
			oldRoot.pageIdArray[rootOffs] = newRight.pageId;
			oldRoot.pageArray[rootOffs] = newRight;
			oldRoot.setUpdated(true);
		}
		// done with new right, leave synch, write it and leave oldRoot to its fate
		newRight.putPage(globalIO);

	}

	@Override
	public long getLongReturn() {
		return newRight.pageId;
	}

	@Override
	public Object getObjectReturn() {
		return newRight;
	}

}
