package com.neocoretechs.bigsack.btree;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Arrays;

import com.neocoretechs.bigsack.DBPhysicalConstants;
import com.neocoretechs.bigsack.io.MappedBlockBuffer;
import com.neocoretechs.bigsack.io.Optr;
import com.neocoretechs.bigsack.io.pooled.BlockAccessIndex;
import com.neocoretechs.bigsack.io.pooled.BlockStream;
import com.neocoretechs.bigsack.io.pooled.Datablock;
import com.neocoretechs.bigsack.io.pooled.GlobalDBIO;
import com.neocoretechs.bigsack.io.pooled.ObjectDBIO;

/*
* Copyright (c) 2003,2014 NeoCoreTechs
* All rights reserved.
* Redistribution and use in source and binary forms, with or without modification, 
* are permitted provided that the following conditions are met:
*
* Redistributions of source code must retain the above copyright notice, this list of
* conditions and the following disclaimer. 
* Redistributions in binary form must reproduce the above copyright notice, 
* this list of conditions and the following disclaimer in the documentation and/or
* other materials provided with the distribution. 
* Neither the name of NeoCoreTechs nor the names of its contributors may be 
* used to endorse or promote products derived from this software without specific prior written permission. 
* THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED 
* WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A 
* PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE LIABLE FOR 
* ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED
* TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
* HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT 
* (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED 
* OF THE POSSIBILITY OF SUCH DAMAGE.
*
*/
/**
* A key page in the bTree.  Performs operations on its set of keys and
* persistent object locations/data.  Persists itself to the buffer pool as as block stream that appears
* as input and output streams connected to pages in the backing store.
* MAXKEYS are the odd maximum keys without spanning page boundaries, calculated by block payload divided by keysize.
* The 'transient' keyword designations in the class fields are an artifact leftover from serialization, retained to 
* illustrate the items that are not persisted via block streams, as direct stream to backing store is far more efficient.
* 
* Unlike a binary search tree, each node of a B-tree may have a variable number of keys and children.
* The keys are stored in non-decreasing order. Each node either is a leaf node or
* it has some associated children that are the root nodes of subtrees.
* The left child node of a node's element contains all nodes (elements) with keys less than or equal to the node element's key
* but greater than the preceding node element's key.
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
* @author Groff Copyright (C) NeoCoreTechs 2014,2015,2017
*/
public final class BTreeKeyPage {
	private static final boolean DEBUG = false;
	private static final boolean DEBUGPUTKEY = false;
	private static final boolean DEBUGREMOVE = false;
	private static final boolean DEBUGSETNUMKEYS = false;
	private static final boolean DEBUGGETDATA = false;
	private static final boolean DEBUGPUTDATA = false;
	static final long serialVersionUID = -2441425588886011772L;
	public static final int BTREEKEYSIZE = 28; // total size of non-transient recurring fields here
	public static final int BTREEDATASIZE = 13; // total number of non transient single entry fields stored per block (numkeys,page) plus 8 bytes for last BTree page pointer entry
	// number of keys per page; number of instances of the non transient fields of 'this' per DB block.
	// The number of maximum children is MAXKEYS+1 per node.
	// Calculate the maximum number of odd keys that can fit per block.
	public static int MAXKEYS = (
			((int) Math.floor(((DBPhysicalConstants.DATASIZE-BTREEDATASIZE)/BTREEKEYSIZE)) % 2) == 0 ? 
			((int) Math.floor((DBPhysicalConstants.DATASIZE-BTREEDATASIZE)/BTREEKEYSIZE)) - 1 : // even, subtract 1 from total
			((int) Math.floor((DBPhysicalConstants.DATASIZE-BTREEDATASIZE)/BTREEKEYSIZE)) );
	// Non transient number of keys on this page. Adjusted as necessary when inserting/deleting.
	private int numKeys = 0; // 4 bytes, SINGLE ENTRY
	// Transient. The 'id' is really the location this page was retrieved from deep store.
	transient long pageId = -1L;
	@SuppressWarnings("rawtypes")
	// The array of keys, transient, filled from key Ids.
	private transient Comparable[] keyArray;
	// The array of page locations of stored keys as block and offset, used to fill keyArray lazily
	private Optr[] keyIdArray; // 10 bytes, MAXKEYS ENTRIES
	// Array to hold updated key status
	private transient boolean[] keyUpdatedArray;
	// The array of pages corresponding to the pageIds for the child nodes. Transient since we lazily retrieve pages via pageIds
	transient BTreeKeyPage[] pageArray;
	// The array of page ids from which the btree key page array is filled. This data is persisted as virtual page pointers. Since
	// we align indexes on page boundaries we dont need an offset as we do with value data associated with the indexes for maps.
	private long[] pageIdArray; // 8 bytes, MAXKEYS+1 ENTRIES
	// These are the data items for values associated with keys,
	// These are lazily populated from the dataIdArray where an id exsists at that index.
	transient Object[] dataArray;
	// This array is present for maps where values are associated with keys. In sets it is absent or empty.
	// It contains the page and offset of the data item associated with a key. We pack value data on pages, hence
	// we need an additional 2 byte offset value to indicate that.
	private Optr[] dataIdArray; // 10 bytes, MAXKEYS ENTRIES
	// This transient array maintains boolean values indicating whether the data item at that index has been updated
	// and needs written back to deep store.
	transient boolean[] dataUpdatedArray;
	// Global is this leaf node flag.
	private boolean mIsLeafNode = true; //1 byte, SINGLE ENTRY ,We treat as leaf since the logic is geared to proving it not
	// Global page updated flag.
	private transient boolean updated = false; // has the node been updated for purposes of write
	private transient ObjectDBIO sdbio;
	private transient BlockAccessIndex lbai = null; // The page is tied to a block
	
	/**
	 * Initialize the transient arrays, the ones that hold the deserialized instances of those objects
	 * pointed to by the non transient block and offset locations read from backing store
	 */
	public void initTransients() {
		keyArray = new Comparable[MAXKEYS];
		keyUpdatedArray = new boolean[MAXKEYS];
		pageArray = new BTreeKeyPage[MAXKEYS + 1];
		dataArray = new Object[MAXKEYS];
		dataUpdatedArray = new boolean[MAXKEYS];
	}
	
	/**
	 * Construct the page from scratch.
	 * We provide the intended virtual page number, all fields are initialized and
	 * we can either retrieve or store it from there. Typically this starts a BTree for indexing.
	 * If clear is false, we will retrieve the page data
	 * @param globalDBIO 
	 * @param ppos The virtual page id
	 * @param clear True to clear the block and set up a new fresh index, false retrieves old index at loc
	 * @throws IOException 
	 */
	public BTreeKeyPage(ObjectDBIO sdbio, long ppos, boolean clear) throws IOException {
		if( DEBUG ) {
			System.out.println("BTreeKeyPage ctor1 from loc:"+GlobalDBIO.valueOf(ppos)+" for "+MAXKEYS+" keys");
		}
		this.sdbio = sdbio;
		initTransients();
		setupKeyArrays();
		pageId = ppos;
		lbai = sdbio.getIOManager().findOrAddBlockAccess(pageId); // read in block headers etc from presumably freechain block
	
		if( lbai == null )
			throw new IOException("BTreeKeyPage ctor1 retrieval for page "+GlobalDBIO.valueOf(pageId)+" **FAIL**.");
		else {
			if( clear || lbai.getBlk().getBytesused() == 0) { // intentional clear or we may have deleted or rolled back all the way to primordial
				lbai.resetBlock(false); // set up headers without revoking access
				lbai.getBlk().setKeypage((byte) 1); // mark it as keypage
				setAllUpdated(true); // we cleared the block, so all must be written come write time
			} else {
				BlockStream bs = sdbio.getIOManager().getBufferPool().getBlockStream(GlobalDBIO.getTablespace(this.pageId));
				DataInputStream dis = bs.getDBInput();
				readFromDBStream(dis);
			}
			if( DEBUG )
				System.out.println("BTreeKeyPage ctor1 retrieval for page "+pageId+" "+lbai);
		}
	}
	
	/**
	 * This is called from getPageFromPool get set up a new clean node
	 * @param sdbio The database IO main class
	 * @param lbai The BlockAcceesIndex page block holding page data
	 * @param read true to read the contents of the btree key from page, otherwise a new page to be filled
	 */
	public BTreeKeyPage(ObjectDBIO sdbio, BlockAccessIndex lbai, boolean read) throws IOException {
		this.sdbio = sdbio;
		this.lbai = lbai;
		this.pageId = lbai.getBlockNum();
		if( DEBUG ) {
			System.out.println("BTreeKeyPage ctor2 from:"+lbai+" for MAXKEYS "+MAXKEYS);
		}
		initTransients();
		// Pre-allocate the arrays that hold persistent data
		setupKeyArrays();
		if( read && lbai.getBlk().getBytesinuse() > 0) {// intentional clear or we may have deleted or rolled back all the way to primordial
			BlockStream bs = sdbio.getIOManager().getBufferPool().getBlockStream(GlobalDBIO.getTablespace(this.pageId));
			DataInputStream dis = bs.getDBInput();
			readFromDBStream(dis);
		} else {
			// If we are not reading, we must be preparing the block for new key. Really no
			// reason for a new block with unassigned and not updating keys conceptually.
			// Set the appropriate flags to write to associated block when the time comes
			lbai.resetBlock(false); // set up headers without revoking access
			lbai.getBlk().setKeypage((byte) 1); // mark it as keypage
			setAllUpdated(true); // we cleared the block, so all must be written come write time
		}
		
		if( DEBUG )
			System.out.println("BtreeKeyPage ctor2 retrieval for page "+GlobalDBIO.valueOf(pageId)+" "+lbai);
	}
	/**
	 * Initialize the key page NON-TRANSIENT arrays, the part that actually gets written to backing store.
	 */
	public synchronized void setupKeyArrays() {
		// Pre-allocate the arrays that hold persistent data
		keyIdArray = new Optr[MAXKEYS];
		pageIdArray= new long[MAXKEYS + 1];
		dataIdArray= new Optr[MAXKEYS];
		for (int i = 0; i <= MAXKEYS; i++) {
			pageIdArray[i] = -1L;
			if( i != MAXKEYS ) {
				keyIdArray[i] = Optr.emptyPointer;
				keyUpdatedArray[i] = false;
				dataIdArray[i] = Optr.emptyPointer;
				dataUpdatedArray[i] = false;
			}
		}
	}
	/**
	 * Read the keypage using the given DataInputStream
	 * @throws IOException
	 */
	public synchronized void readFromDBStream(DataInputStream dis) throws IOException {
		setmIsLeafNode(dis.readByte() == 0 ? false : true);
		setNumKeys(dis.readInt());
		for(int i = 0; i < MAXKEYS; i++) {
			long sblk = dis.readLong();
			short shblk = dis.readShort();
			//if( DEBUG ) { 
			//	System.out.println("block of key "+i+":"+GlobalDBIO.valueOf(sblk)+" offset of key "+i+":"+shblk);
			//}
			keyIdArray[i] = new Optr(sblk, shblk);
			// data array
			sblk = dis.readLong();
			shblk = dis.readShort();
			//if( DEBUG ) { 
			//	System.out.println("block of data "+i+":"+GlobalDBIO.valueOf(sblk)+" offset of data "+i+":"+shblk);
			//}
			dataIdArray[i] = new Optr(sblk, shblk);
		}
		// pageId
		for(int i = 0; i <= MAXKEYS; i++) {	
			pageIdArray[i] = dis.readLong();
		}
	}
	/**
	 * Move the sourcepage to this as target page. Zero source and set it to write.
	 * The block numbers are preserved.
	 * This is used primarily to replace the root or any internal node if all keys are deleted from it.
	 * @param sourcePage
	 * @throws IOException 
	 */
	public synchronized void replacePage(BTreeKeyPage sourcePage) throws IOException {
		if(DEBUG || DEBUGREMOVE) {
			System.out.println("BTreeKeyPage.replacePage replacing "+this+" with "+sourcePage);
		}
		setmIsLeafNode(sourcePage.getmIsLeafNode());
		setNumKeys(sourcePage.getNumKeys());
		for(int i = 0; i < sourcePage.getNumKeys(); i++) {
			BTreeMain.moveKeyData(sourcePage, i, this, i, false);
			BTreeMain.moveChildData(sourcePage, i, this, i, false);
		}
		//
		BlockAccessIndex tbai = sourcePage.getBlockAccessIndex(); ///source page block
		tbai.resetBlock(false); // set up headers without revoking access, does NOT reset block number
		tbai.getBlk().setKeypage((byte) 0); // mark it as no longer a keypage, its a free block
		tbai.getBlk().setIncore(true);
		tbai.decrementAccesses(); // unlatch it, we are done
		if(DEBUG || DEBUGREMOVE) {
			System.out.println("BTreeKeyPage.replacePage replaced new root, now "+this);
		}
	}
	/**
	 * Set the key Id array, and set the keyUpdatedArray for the key and the general updated flag
	 * @param index
	 * @param optr
	 */
	public synchronized void setKeyIdArray(int index, Optr optr) {
		keyIdArray[index] = optr;
		keyUpdatedArray[index] = true;
		updated = true;
	}
	
	public synchronized Optr getKeyId(int index) {
		return keyIdArray[index];
	}
	/**
	 * Set the Data Id array, and set the dataUpdatedArray for the key and the general updated flag
	 * @param index
	 * @param optr The Vblock and offset within that block of the first data item for the key/value value associated data if any
	 */
	public synchronized void setDataIdArray(int index, Optr optr) {
		dataIdArray[index] = optr;
		dataUpdatedArray[index] = true;
		updated = true;
	}
	
	public synchronized Optr getDataId(int index) {
		return dataIdArray[index];
	}
	/**
	 * Set the Key page nodes Id array, and set the pageUpdatedArray for the key and the general updated flag
	 * @param index
	 * @param optr The long VBlock of the key page, we dont need Optr offset because offset of key pages is always 0
	 */
	public synchronized void setPageIdArray(int index, long optr) {
		pageIdArray[index] = optr;
		updated = true;
	}
	
	public synchronized long getPageId(int index) {
		return pageIdArray[index];
	}
	/**
	* Given a Comparable object, search for that object on this page.
	* The key was found on this page and loc is index of
	* located key if atKey is true. 
	* If atKey was false then the key
	* was not found on this page and index-1 is index of where the
	* key *should* be. The result is always to the right of the target key, therefore
	* to nav left one accesses index-1 key.
	* @param targetKey The target key to retrieve
	* @return TreeSearchResult the insertion point from 0 to MAXKEYS and flag of whether key was found
	 * @throws IOException 
	*/
	@SuppressWarnings({ "rawtypes", "unchecked" })
	synchronized TreeSearchResult search(Comparable targetKey) throws IOException {
		if(targetKey == null)
			System.out.printf("%s.search target key is null", this.getClass().getName());
		assert(keyArray.length > 0) : "BTreeKeyPage.search key array length zero";
		int middleIndex = 1; 
        int leftIndex = 0;
        int rightIndex = getNumKeys() - 1;
        // no keys, call for insert at 0
        if( rightIndex == -1)
        	return new TreeSearchResult(0, false);
        while (leftIndex <= rightIndex) {
        	middleIndex = leftIndex + ((rightIndex - leftIndex) / 2);
    		if(getKey(middleIndex) == null)
    			System.out.printf("%s.search getKey(%d) is null for page:%s%n", this.getClass().getName(),middleIndex,this);
        	int cmpRes = getKey(middleIndex).compareTo(targetKey);
        	if (cmpRes < 0 ) {
        		leftIndex = middleIndex + 1;
        	} else 
        		if (cmpRes > 0 ) {
        			rightIndex = middleIndex - 1;
        		} else {
        			return new TreeSearchResult(middleIndex, true);
        		}
        }
        if( DEBUG )
        	System.out.println("BtreeKeyPage.search falling thru "+middleIndex+" "+leftIndex+" "+rightIndex+" "+this+" target:"+targetKey);
        return new TreeSearchResult(middleIndex, false);
	}
	/**
	 * Remove the key k from this node or the sub-tree rooted with this node preserving BTree properties. 
	 * The following methods are unique to the removal process:
	 * 1) remove
	 * 2) removeFromNonLeaf
	 * 3) getPred
	 * 4) getSucc
	 * 5) borrowFromPrev
	 * 6) borrowFromNext
	 * 7) merge
	 * @see BTreeMain.delete for a description of the logic applied to deletion.
	 * @param targetKey
	 * @throws IOException 
	 */
	synchronized void remove(Comparable targetKey) throws IOException {
	    TreeSearchResult tsr = search(targetKey);
	    //System.out.println("BTreeKeyPage.remove "+targetKey+" search result:"+tsr);
	    int idx = tsr.insertPoint;
	    // The key to be removed is present in this node
	    if (tsr.atKey) {
	        // If the node is a leaf node - delete is called
	        // Otherwise, removeFromNonLeaf method is called
	        if (getmIsLeafNode()) {
				if( DEBUG || DEBUGREMOVE ) System.out.println("BTreeKeyPage.remove LEAF on "+tsr);
	            delete(idx);
	        } else {
				if( DEBUG || DEBUGREMOVE ) System.out.println("BTreeKeyPage.remove NON-LEAF on "+tsr);
	            removeFromNonLeaf(idx);
	        }
	    } else {
	        // If this node is a leaf node, then the key is not present in tree
	        if (getmIsLeafNode()) {
	            if( DEBUG || DEBUGREMOVE )
	            	System.out.println("BTreeKeyPage.remove The key "+targetKey+" does not exist in the tree");
	            return;
	        }
	        // The key to be removed is present in the sub-tree rooted with this node
	        // The flag indicates whether the key is present in the sub-tree rooted
	        // with the last child of this node
	        boolean flag = ( (idx == getNumKeys())? true : false );
	        // If the child where the key is supposed to exist has less that t keys,
	        // we fill that child
	        if (getPage(idx).getNumKeys() < BTreeMain.T)
	            fill(idx);
	        // If the last child has been merged, it must have merged with the previous
	        // child and so we recurse on the (idx-1)th child. Else, we recurse on the
	        // (idx)th child which now has at least t keys
	        if (flag && idx > getNumKeys())
	            getPage(idx-1).remove(targetKey);
	        else
	            getPage(idx).remove(targetKey);
	    }
	    return;
	}
	// A function to remove the idx-th key from this node - which is a non-leaf node
	synchronized void removeFromNonLeaf(int idx) throws IOException {
		// failsafe
		if(getPage(idx) == null) {
			System.out.println("BtreeKeyPage.removeFromNonLeaf requesting null page from index "+idx+" exiting instead.");
			return;
		}
		Comparable targetKey = getKey(idx);
	    // If the child that precedes k (C[idx]) has at least t keys,
	    // find the predecessor 'pred' of k in the subtree rooted at
	    // C[idx]. Replace k by pred. Recursively delete pred
	    // in C[idx]
	    if (getPage(idx).getNumKeys() >= BTreeMain.T) {
	        TreeSearchResult pred = getPred(idx);
	        //keys[idx] = pred;
	        copyKeyAndDataToArray(pred.page, pred.insertPoint, idx);
	        getPage(idx).remove(pred.page.getKey(pred.insertPoint));
	    } else
	    // If the child C[idx] has less that t keys, examine C[idx+1].
	    // If C[idx+1] has atleast t keys, find the successor 'succ' of k in
	    // the subtree rooted at C[idx+1]
	    // Replace k by succ
	    // Recursively delete succ in C[idx+1]
	    	if(getPage(idx+1).getNumKeys() >= BTreeMain.T) {
	    		TreeSearchResult succ = getSucc(idx);
	    		//keys[idx] = succ;
	    		//C[idx+1]->remove(succ);
	            copyKeyAndDataToArray(succ.page, succ.insertPoint, idx);
		        getPage(idx+1).remove(succ.page.getKey(succ.insertPoint));
	    	} else {
	    		// If both C[idx] and C[idx+1] has less that t keys,merge k and all of C[idx+1]
	    		// into C[idx]
	    		// Now C[idx] contains 2t-1 keys
	    		// Free C[idx+1] and recursively delete k from C[idx]
	    		merge(idx);
	    		//C[idx]->remove(k);
	    		getPage(idx).remove(targetKey);
	    	}
	    return;
	}
	// A function to get predecessor of keys[idx]
	synchronized TreeSearchResult getPred(int idx) throws IOException {
	    // Keep moving to the right most node until we reach a leaf
	    BTreeKeyPage cur = getPage(idx);
	    while (!cur.getmIsLeafNode())
	        cur = cur.getPage(cur.getNumKeys());
	    // Return the last key of the leaf
	    return new TreeSearchResult(cur, cur.getNumKeys()-1, false);
	}
	synchronized TreeSearchResult getSucc(int idx) throws IOException {
	    // Keep moving the left most node starting from C[idx+1] until we reach a leaf
	    //BTreeNode *cur = C[idx+1];
	    //while (!cur->leaf)
	    BTreeKeyPage cur = getPage(idx+1);
	    while (!cur.getmIsLeafNode())
	    	//cur = cur->C[0];
	        cur = cur.getPage(0);
	    // Return the first key of the leaf
	    //return cur->keys[0];
	    return new TreeSearchResult(cur, 0, false);
	}
	/**
	 * Fill child C[idx] which has less than t-1 keys
	 * @param idx
	 * @throws IOException 
	 */
	synchronized void fill(int idx) throws IOException {
	    // If the previous child(C[idx-1]) has more than t-1 keys, borrow a key
	    // from that child
	    //if (idx!=0 && C[idx-1]->n>=t)
		if( idx != 0 && getPage(idx-1).getNumKeys() >= BTreeMain.T)
	        borrowFromPrev(idx);
	    // If the next child(C[idx+1]) has more than t-1 keys, borrow a key
	    // from that child
	    else 
	    	//if (idx!=n && C[idx+1]->n>=t)
	    	if(idx != getNumKeys() && getPage(idx+1).getNumKeys() >= BTreeMain.T)
	    		borrowFromNext(idx); 
	    	// Merge C[idx] with its sibling
	    	// If C[idx] is the last child, merge it with with its previous sibling
	    	// Otherwise merge it with its next sibling
	    	else {
	    		if (idx != getNumKeys())
	    			merge(idx);
	    		else
	    			merge(idx-1);
	    	}
	    return;
	}
	// Borrow a key from C[idx-1] and insert it
	// into C[idx]
	synchronized void borrowFromPrev(int idx) throws IOException { 
	    BTreeKeyPage child = getPage(idx);
	    BTreeKeyPage sibling = getPage(idx-1);
	    // The last key from C[idx-1] goes up to the parent and key[idx-1]
	    // from parent is inserted as the first key in C[idx]. Thus, the 
	    // sibling loses one key and child gains one key.
	    // Moving all key in C[idx] one step ahead
	    //for (int i=child->n-1; i>=0; --i)
	    for(int i = child.getNumKeys()-1; i >= 0; --i)
		    //child->keys[i+1] = child->keys[i];
	    	child.copyKeyAndDataToArray(child, i, i+1);
	    // If C[idx] is not a leaf, move all its child pointers one step ahead
	    if (!child.getmIsLeafNode()) {
	        //for(int i=child->n; i>=0; --i)
	    	for(int i = child.getNumKeys(); i >= 0; --i) {
	            //child->C[i+1] = child->C[i];
	    		child.putPageToArray(child.getPage(i), i+1);
	    		child.setPageIdArray(i+1, child.getPageId(i));
	    	}
	    }
	    // Setting child's first key equal to keys[idx-1] from the current node
	    //child->keys[0] = keys[idx-1];
	    child.copyKeyAndDataToArray(this, idx-1, 0);
	    // Moving sibling's last child as C[idx]'s first child
	    if (!getmIsLeafNode()) {
	        //child->C[0] = sibling->C[sibling->n];
	    	child.putPageToArray(sibling.getPage(sibling.getNumKeys()), 0);
	    	child.setPageIdArray(0, sibling.getPageId(sibling.getNumKeys()));
	    }
	    // Moving the key from the sibling to the parent
	    // This reduces the number of keys in the sibling
	    //keys[idx-1] = sibling->keys[sibling->n-1];
	    copyKeyAndDataToArray(sibling, sibling.getNumKeys()-1, idx-1);
	    // increase/reduce key count
	    //child->n += 1;
	    child.setNumKeys(child.getNumKeys()+1);
	    //sibling->n -= 1;
	    sibling.setNumKeys(sibling.getNumKeys()-1);
	    return;
	}
	// A function to borrow a key from the C[idx+1] and place
	// it in C[idx]
	synchronized void borrowFromNext(int idx) throws IOException {
	    BTreeKeyPage child = getPage(idx);
	    BTreeKeyPage sibling = getPage(idx+1);
	    // keys[idx] is inserted as the last key in C[idx]
	    //child->keys[(child->n)] = keys[idx];
	    child.copyKeyAndDataToArray(this, idx, child.getNumKeys());
	    // Sibling's first child is inserted as the last child
	    // into C[idx]
	    if (!child.getmIsLeafNode()) {
	        //child->C[(child->n)+1] = sibling->C[0];
	      	child.putPageToArray(sibling.getPage(0), child.getNumKeys()+1);
	    	child.setPageIdArray(child.getNumKeys()+1, sibling.getPageId(0));
	    }
	    //The first key from sibling is inserted into keys[idx]
	    //keys[idx] = sibling->keys[0];
	    copyKeyAndDataToArray(sibling, 0, idx);
	 
	    // Moving all keys in sibling one step behind
	    for (int i=1; i< sibling.getNumKeys() ; ++i)
	        //sibling->keys[i-1] = sibling->keys[i];
	    	sibling.copyKeyAndDataToArray(sibling, i, i-1);
	 
	    // Moving the child pointers one step behind
	    if (!sibling.getmIsLeafNode()) {
	        for(int i=1; i<=sibling.getNumKeys(); ++i) {
	            //sibling->C[i-1] = sibling->C[i];
	          	sibling.putPageToArray(sibling.getPage(i), i-1);
		    	sibling.setPageIdArray(i-1, sibling.getPageId(i));
	        }
	    }
	    // Increasing and decreasing the key count of C[idx] and C[idx+1]
	    // respectively
	    //child->n += 1;
	    child.setNumKeys(child.getNumKeys()+1);
	    //sibling->n -= 1;
	    sibling.setNumKeys(sibling.getNumKeys()-1);
	    return;
	}
	/**
	 * Merge C[idx] with C[idx+1]. C[idx+1] is freed after merging
	 * @param idx
	 * @throws IOException
	 */
	synchronized void merge(int idx) throws IOException {
	    BTreeKeyPage child = getPage(idx);//C[idx];
	    BTreeKeyPage sibling = getPage(idx+1);// C[idx+1]; 
	    // Pulling a key from the current node and inserting it into (t-1)th
	    // position of C[idx]
	    //child->keys[t-1] = keys[idx];
	    child.copyKeyAndDataToArray(this, idx, BTreeMain.T-1);
	    // Copying the keys from C[idx+1] to C[idx] at the end
	    for (int i=0; i < sibling.getNumKeys(); ++i) {
	        //child->keys[i+t] = sibling->keys[i];
	    	child.copyKeyAndDataToArray(sibling, i, i+BTreeMain.T);
	    }
	    // Copying the child pointers from C[idx+1] to C[idx]
	    if (!child.getmIsLeafNode()) {
	        for(int i=0; i <= sibling.getNumKeys(); ++i) {
	            //child->C[i+t] = sibling->C[i];
	          	child.putPageToArray(sibling.getPage(i), i+BTreeMain.T);
			    child.setPageIdArray(i+BTreeMain.T, sibling.getPageId(i));
	        }
	    }
	    // Moving all keys after idx in the current node one step before -
	    // to fill the gap created by moving keys[idx] to C[idx]
	    for (int i = idx+1; i < getNumKeys(); ++i)
	        //keys[i-1] = keys[i];
	    	copyKeyAndDataToArray(this, i, i-1);
	    // Moving the child pointers after (idx+1) in the current node one
	    // step before
	    for (int i = idx+2; i <= getNumKeys(); ++i) {
	       // C[i-1] = C[i];
          	child.putPageToArray(getPage(i), i-1);
			child.setPageIdArray(i-1, getPageId(i));
	    }
	    // Updating the key count of child and the current node
	    //child->n += sibling->n+1;
	    child.setNumKeys(sibling.getNumKeys()+1);
	    //n--;
	    setNumKeys(getNumKeys()-1);
	    // Freeing the memory occupied by sibling
	    sibling.getBlockAccessIndex().decrementAccesses();
	    return;
	}
	 
	/**
	* Delete the key/data item on this page.
	* Everything on the page is slid left. If there exists a valid pointer to a key or value
	* object, that object is deleted.
	* In the end, numKeys is decremented.
	* @param index the index of the item on this page to delete
	* @throws IOException 
	*/
	synchronized void delete(int index) throws IOException {
		//System.out.println("BTreeKeyPage.delete "+this+" index:"+index);
		if( !keyIdArray[index].equals(Optr.emptyPointer))
			sdbio.delete_object(keyIdArray[index], GlobalDBIO.getObjectAsBytes(getKey(index)).length);
		if( !dataIdArray[index].equals(Optr.emptyPointer))
			sdbio.delete_object(dataIdArray[index], GlobalDBIO.getObjectAsBytes(getData(index)).length);
		// If its the rightmost key ignore move
		if (index < getNumKeys() - 1)
			// Move all up
			for (int i = index;i < (getNumKeys() == MAXKEYS ? MAXKEYS - 1 : getNumKeys()); i++) {
				keyArray[i] = keyArray[i + 1];
				keyIdArray[i] = keyIdArray[i + 1];
				pageArray[i + 1] = pageArray[i + 2];
				pageIdArray[i + 1] = pageIdArray[i + 2];
				dataArray[i] = dataArray[i + 1];
				dataIdArray[i] = dataIdArray[i + 1];
				dataUpdatedArray[i] = dataUpdatedArray[i + 1];
				keyUpdatedArray[i] = keyUpdatedArray[i + 1];
			}

		// Decrement key count and nullify rightmost item on the node
		setNumKeys(getNumKeys() - 1);
		keyArray[getNumKeys()] = null;
		keyIdArray[getNumKeys()] = Optr.emptyPointer;
		keyUpdatedArray[getNumKeys()] = false;
		pageArray[getNumKeys() + 1] = null;
		pageIdArray[getNumKeys() + 1] = -1L;
		dataArray[getNumKeys()] = null;
		dataIdArray[getNumKeys()] = Optr.emptyPointer;
		dataUpdatedArray[getNumKeys()] = false; // we took care of it
		setUpdated(true);
	}
	/**
	 * For a delete to fire, the dataArray at index cant be null and the dtaIdArray Optr at index cant be Empty.
	 * @param index the key position whose data will be deleted
	 * @throws IOException
	 */
	synchronized void deleteData(int index) throws IOException {
		if (dataArray[index] != null && !dataIdArray[index].isEmptyPointer()) {
			if( DEBUG ) {
				System.out.println("Deleting :"+dataArray[index]+" "+dataIdArray[index]);
			}
			//if( Props.DEBUG ) System.out.println(" size "+ilen);
			sdbio.delete_object(dataIdArray[index],  GlobalDBIO.getObjectAsBytes(dataArray[index]).length );
			dataIdArray[index] = Optr.emptyPointer;
			dataUpdatedArray[index] = true;
			setUpdated(true);
		}
	}
	/**
	* Retrieve a page based on an index to this page containing a page.
	* If the pageArray at index is NOT null we dont fetch anything.
	* In effect, this is our lazy initialization of the 'pageArray' and we strictly
	* work in pageArray in this method. If the pageIdArray contains a valid non -1 entry, then
	* we retrieve that virtual block to an entry in the pageArray at the index passed in the params
	* location. If we retrieve an instance we also fill in the transient fields from our current data
	* @param index The index to the page array on this page that contains the virtual record to deserialize.
	* @return The constructed page instance of the page at 'index' on this page.
	* @exception IOException If retrieval fails
	*/
	public synchronized BTreeKeyPage getPage(int index) throws IOException {
		if(DEBUG) {
			System.out.println("BTreeKeyPage.getPage ENTER BTreeKeyPage to retrieve target index:["+index+"]");
			for(int i = 0; i <= numKeys; i++) {
				System.out.println("BTreeKeyPage.getPage initial page index:["+i+"]="+GlobalDBIO.valueOf(pageIdArray[i])+" page:"+pageArray[i]);
			}
		}
		if (pageArray[index] == null && pageIdArray[index] != -1L) {
			// eligible to retrieve page
			if( DEBUG ) {
				System.out.println("BTreeKeyPage.getPage about to retrieve index:["+index+"] loc:"+GlobalDBIO.valueOf(pageIdArray[index]));
			}
			pageArray[index] = getPageFromPool(sdbio, pageIdArray[index]);
			pageArray[index].pageId = pageIdArray[index];
			if( DEBUG ) {
				System.out.println("BTreeKeyPage.getPage RETRIEVED index:"+index+" loc:"+GlobalDBIO.valueOf(pageIdArray[index])+" page:"+pageArray[index]);
			}
		}
		if(DEBUG) {
			System.out.println("BTreeKeyPage.getPage EXIT BTreeKeyPage to retrieve target index:["+index+"]");
			for(int i = 0; i <= numKeys; i++) {
				System.out.println("BTreeKeyPage.getPage final page index:["+i+"]="+GlobalDBIO.valueOf(pageIdArray[i])+" page:"+pageArray[i]);
			}
		}
		if(DEBUG)
		if(pageArray[index] == null) {
			System.out.println("BtreeKeyPage prepared to deliver null page from pageArray index "+index+" pageID:"+pageIdArray[index]);
			for(int i = 0; i < pageArray.length; i++ ) {
				System.out.println("Index:"+i+" Id:"+pageIdArray[i]+" Page:"+pageArray[i]);
			}
		}
		// resolve other pages with this same value
		for(int i = 0; i < numKeys; i++) {
			if( i == index )
				continue;
			if (pageArray[i] == null && pageIdArray[i] == pageIdArray[index]) {
				pageArray[i] = pageArray[index];
			}
		}
		return pageArray[index];
	}

	/**
	* Retrieve a key based on an index to this page.
	* In effect, this is our lazy initialization of the 'keyArray' and we strictly
	* work in keyArray in this method. If the keyIdArray contains a valid non -1 entry, then
	* we retrieve and deserialize that block,offset to an entry in the keyArray at the index passed in the params
	* location.
	* @param sdbio The session database io instance
	* @param index The index to the key array on this page that contains the offset to deserialize.
	* @return The deserialized page instance
	* @exception IOException If retrieval fails
	*/
	public synchronized Comparable getKey(int index) throws IOException {
		if(DEBUG) {
			System.out.println("BTreeKeyPage.getKey Entering BTreeKeyPage to retrieve target index "+index);
		}
		if (keyArray[index] == null && !keyIdArray[index].isEmptyPointer()) {
			// eligible to retrieve page
			if( DEBUG ) {
				System.out.println("BTreeKeyPage.getKey about to retrieve index:"+index+" loc:"+keyIdArray[index]);
			}
			keyArray[index] =
				(Comparable) (sdbio.deserializeObject(keyIdArray[index]));
			if( DEBUG ) {
				System.out.println("BTreeKeyPage.getKey retrieved index:"+index+" loc:"+keyIdArray[index]+" retrieved:"+keyArray[index]);
				for(int i = 0; i < numKeys; i++)System.out.println(i+"="+keyIdArray[i]);
			}
			keyUpdatedArray[index] = false;
		}
		return keyArray[index];
	}

	/**
	* Retrieve value for key based on an index to this page.
	* In effect, this is our lazy initialization of the 'dataArray' and we strictly
	* work in dataArray in this method. If the dataIdArray contains a valid non Optr.Empty entry, then
	* we retrieve and deserialize that block,offset to an entry in the dataArray at the index passed in the params
	* location.
	* @param index The index to the data array on this page that contains the offset to deserialize.
	* @return The deserialized Object instance
	* @exception IOException If retrieval fails
	*/
	public synchronized Object getData(int index) throws IOException {
		if(DEBUG) {
			System.out.println("BTreeKeyPage.getData Entering BTreeKeyPage to retrieve target index "+index+" loc:"+dataIdArray[index]+" "+dataArray[index]);
		}
		if (dataArray[index] == null && !dataIdArray[index].isEmptyPointer()) {
			// eligible to retrieve page
			if( DEBUGGETDATA ) {
				System.out.println("BTreeKeyPage.getData about to retrieve index:"+index+" loc:"+dataIdArray[index]);
			}
			dataArray[index] = sdbio.deserializeObject(dataIdArray[index]);
			if( DEBUG ) {
				System.out.println("BTreeKeyPage.getData retrieved index:"+index+" loc:"+dataIdArray[index]+" retrieved:"+dataArray[index]);
			}
			dataUpdatedArray[index] = false;
		}
		return dataArray[index];
	}

	/**
	* Primarily for getting root at boot where we don't have an index on a page that contains a 
	* location.  Otherwise, we use the overloaded getPage with index. This method
	* can be used to locate a block which has an index record on its boundary and deserialize that.
	* It starts somewhat from baseline as it initializes all the internal BTreeKeyPage structures.
	* No effort is made to guarantee the record being accessed is a viable BtreeKeyPage, that is assumed.
	* A getPage is performed after page setup.
	* @param sdbio The session database io instance
	* @param pos The block containing page
	* @return The deserialized page instance
	* @exception IOException If retrieval fails
	*/
	static BTreeKeyPage getPageFromPool(ObjectDBIO sdbio, long pos) throws IOException {
		assert(pos != -1L) : "Page index invalid in getPage "+sdbio.getDBName();
		BlockAccessIndex bai = sdbio.findOrAddBlock(pos);
		BTreeKeyPage btk = new BTreeKeyPage(sdbio, bai, true);
		if( DEBUG ) 
			System.out.println("BTreeKeyPage.getPageFromPool "+btk+" from block "+bai);
		//for(int i = 0; i <= MAXKEYS; i++) {
		//	btk.pageArray[i] = btk.getPage(sdbio,i);
		//}
		return btk;
	}
	/**
	 * Get a new page from the pool from round
	 *  robin tablespace. used for general inserts.
	 * Call stealblk, create BTreeKeyPage with the page Id of stolen block, set the pageArray
	 * to MAXKEYS+1, the dataArray to MAXKEYS, and the dataUpdatedArray to MAXKEYS
	 * set updated to true, and return the newly formed  
	 * @param sdbio
	 * @return
	 * @throws IOException
	 */
	static BTreeKeyPage getPageFromPool(ObjectDBIO sdbio) throws IOException {
		// Get a fresh block
		BlockAccessIndex lbai = sdbio.stealblk();
		// initialize transients, set page with this block, false=no read, set up for new block instead
		// this will set updated since the block is new
		BTreeKeyPage btk = new BTreeKeyPage(sdbio, lbai, false);
		return btk;
	}
	/**
	 * Serialize this page to deep store on a page boundary.
	 * For data, we reset the new node position. For pages, we don't use
	 * it because they are always on page boundaries (not packed). The data is written
	 * the the blockbuffer, the push to deep store takes place at commit time or
	 * when the buffer fills and it becomes necessary to open a spot.
	 * @param sdbio The ObjectDBIO instance
	 * @exception IOException If write fails
	 */
	public synchronized void putPage() throws IOException {
		if (!isUpdated()) {
			if( DEBUG ) 
				System.out.println("BTreeKeyPage.putPage page NOT updated:"+this);
			return;
		}
		if( DEBUG ) 
			System.out.println("BTreeKeyPage.putPage:"+this);
		// Persist each key that is updated to fill the keyIds in the current page
		// Once this is complete we write the page contiguously
		// Write the object serialized keys out to deep store, we want to do this out of band of writing key page
		for(int i = 0; i < MAXKEYS; i++) {
			if( keyUpdatedArray[i] ) {
				// put the key to a block via serialization and assign KeyIdArray the position of stored key
				putKey(i);	
			}
			if( dataUpdatedArray[i]) {
				putData(i);
			}
		}
		//
		assert (pageId != -1L) : " BTreeKeyPage unlinked from page pool:"+this;
		BlockStream bks = sdbio.getIOManager().getBlockStream(GlobalDBIO.getTablespace(pageId));
		DataOutputStream bs = bks.getDBOutput();
		// write the page to the current block
		lbai.getBlk().setIncore(true);
		lbai.setByteindex((short) 0);
		// Write to the block output stream
		bks.setBlockAccessIndex(lbai);
		if( DEBUG )
			System.out.println("BTreeKeyPage.putPage BlockStream:"+bks);
		bs.writeByte(getmIsLeafNode() ? 1 : 0);
		bs.writeInt(getNumKeys());
		for(int i = 0; i < MAXKEYS; i++) {
			if( keyUpdatedArray[i] ) { // if set, key was processed by putKey[i]
				bs.writeLong(keyIdArray[i].getBlock());
				bs.writeShort(keyIdArray[i].getOffset());
				keyUpdatedArray[i] = false;
			} else { // skip 
				bks.getBlockAccessIndex().setByteindex((short) (bks.getBlockAccessIndex().getByteindex()+10));
			}
			// data array
			if( dataUpdatedArray[i] ) {
				bs.writeLong(dataIdArray[i].getBlock());
				bs.writeShort(dataIdArray[i].getOffset());
				dataUpdatedArray[i] = false;
			} else {
				// skip the data Id for this index as it was not updated, so no need to write anything
				bks.getBlockAccessIndex().setByteindex((short) (bks.getBlockAccessIndex().getByteindex()+10));
			}
		}
		// persist btree key page indexes
		for(int i = 0; i <= MAXKEYS; i++) {
			bs.writeLong(pageIdArray[i]);
		}
		bs.flush();
		if( DEBUG ) {
			System.out.println("BTreeKeyPage.putPage Added Keypage @"+GlobalDBIO.valueOf(pageId)+" block for keypage:"+lbai+" page:"+this);
		}
		setUpdated(false);
	}
	/**
	 * At BTreeKeyPage putPage time, we resolve the lazy elements to actual VBlock,offset
	 * This method puts the values associated with a key/value pair, if using maps vs sets
	 * @param index Index of BTreeKeyPage key and data value array
	 * @throws IOException
	 */
	private synchronized void putData(int index) throws IOException {
		// if it gets nulled or overwritten, delete old data
		if( dataArray[index] != null) {
				byte[] pb = GlobalDBIO.getObjectAsBytes(dataArray[index]);
				// pack the page into this tablespace and within blocks the same tablespace as key
				// the new insert position will attempt to find a block with space relative to established positions
				dataIdArray[index] = sdbio.getIOManager().getNewInsertPosition(dataIdArray, index, getNumKeys(), pb.length);		
				if( DEBUGPUTDATA )
					System.out.println("BTreeKeyPage.putData ADDING NON NULL value "+dataArray[index]+" for key index "+index+" at "+
										GlobalDBIO.valueOf(dataIdArray[index].getBlock())+","+dataIdArray[index].getOffset());
				sdbio.add_object(dataIdArray[index], pb, pb.length);
		} else {
				// null this with an empty Optr.
				dataIdArray[index] = Optr.emptyPointer;
				if( DEBUGPUTDATA )
					System.out.println("BTreeKeyPage.putData ADDING NULL value for key index "+index);
		}
	}
	
	/**
	 * Using fromPage, populate pageArray[index] = fromPage. 
	 * If fromPage is NOT null, pageIdArray[index] = fromPage.pageId
	 * if fromPage IS null, pageIdArray[index] = -1L
	 * set the updated flag to true
	 * @param fromPage
	 * @param index
	 */
	synchronized void putPageToArray(BTreeKeyPage fromPage, int index) {
		pageArray[index] = fromPage;
		if (fromPage != null)
			pageIdArray[index] = fromPage.pageId;
		else
			pageIdArray[index] = -1L;
		setUpdated(true);
	}

	/**
	 * Put the updated keys to the buffer pool at available block space.
	 * The data is written to the BlockAccessIndex, the push to deep store takes place at commit time or
	 * when the buffer fills and it becomes necessary to open a spot.
	 * @param sdbio The ObjectDBIO instance
	 * @exception IOException If write fails
	 */
	private synchronized boolean putKey(int index) throws IOException {
		if(!keyUpdatedArray[index]) {
			if(DEBUG || DEBUGPUTKEY) 
				System.out.println("BTreeKeyPage.putKey("+index+") key not updated, exiting..");
			return false;
		}
		if(keyArray[index] == null || !keyIdArray[index].equals(Optr.emptyPointer)) {
			if(DEBUG || DEBUGPUTKEY) 
				System.out.println("BTreeKeyPage.putKey("+index+") "+ (keyArray[index] == null ? "key is null " : " ")+
						(!keyIdArray[index].equals(Optr.emptyPointer) ? " keyIdArray already contains valid pointer, "+(keyIdArray[index])+" returning.." : "."));
			return false;
		}
		// get first block to write contiguous records for keys
		// We either have a block with some space or one we took from freechain list
		byte[] pb = GlobalDBIO.getObjectAsBytes(keyArray[index]);
		keyIdArray[index] = sdbio.getIOManager().getNewInsertPosition(keyIdArray, index, getNumKeys(), pb.length);
		sdbio.add_object(keyIdArray[index], pb, pb.length);
		if(DEBUG || DEBUGPUTKEY) 
				System.out.println("BTreeKeyPage.putKey Added object:"+keyArray[index]+" @"+keyIdArray[index]+" bytes:"+pb.length);
		return true;
	}
	
	@SuppressWarnings("rawtypes")
	/**
	 * Using key, put keyArray[index] = key
	 * set updated true
	 * @param key
	 * @param index
	 */
	synchronized void putKeyToArray(Comparable key, int index) {
		keyArray[index] = key;
		keyIdArray[index] = Optr.emptyPointer;
		keyUpdatedArray[index] = true;
		setUpdated(true);
	}
	/**
	 * Copy a key from another page to a key index on this one, preserving the pointer.
	 * Using souceKey, put this.keyArray[targetIndex] = sourceKey.keyArray[sourceIndex] and
	 * this.keyIdArray[targetIndex] = sourceKey.keyIdArray[sourceIndex].
	 * SO we copy key, preserving the Id, and set updated true for record and key
	 * @param key
	 * @param sourceIndex
	 * @param targetIndex
	 * @throws IOException 
	 */
	synchronized void copyKeyToArray(BTreeKeyPage sourceKey, int sourceIndex, int targetIndex) throws IOException {
		keyArray[targetIndex] = sourceKey.getKey(sourceIndex); // get the key from pointer from source if not already
		keyIdArray[targetIndex] = sourceKey.getKeyId(sourceIndex);
		keyUpdatedArray[targetIndex] = true;
		setUpdated(true);
	}
	
	synchronized void copyDataToArray(BTreeKeyPage sourceKey, int sourceIndex, int targetIndex) throws IOException {
		dataArray[targetIndex] = sourceKey.getData(sourceIndex);
		dataIdArray[targetIndex] = sourceKey.getDataId(sourceIndex); // takes care of updated fields
		dataUpdatedArray[targetIndex] = true;
		setUpdated(true);
	}
	
	synchronized void copyKeyAndDataToArray(BTreeKeyPage sourceKey, int sourceIndex, int targetIndex) throws IOException {
		copyKeyToArray(sourceKey, sourceIndex, targetIndex);
		copyDataToArray(sourceKey, sourceIndex, targetIndex);
	}

	/**
	 * Set the dataArray[index] to 'data'. Set the dataidArray[index] to empty pointer,
	 * set the dataUpdatedArray[index] to true, set data updated true;
	 * @param data
	 * @param index
	 */
	synchronized void putDataToArray(Object data, int index) {
		dataArray[index] = data;
		dataIdArray[index] = Optr.emptyPointer;
		dataUpdatedArray[index] = true;
		setUpdated(true);
	}
	/**
	 * Set the pageArray[index] and pageIdArray[index] to default null values.
	 * Set updated to true.
	 * @param index The array index to annul
	 */
	synchronized void nullPageArray(int index) {
		pageArray[index] = null;
		pageIdArray[index] = -1L;
		setUpdated(true);
	}
	/**
	 * Set the keyArray and dataArray to null for this index. Set the keyIdArray and dataIdArray to empty locations.
	 * Set the updated flag for the key and data fields, then set updated for the record.
	 * @param index The target index we are updating for both key and data.
	 */
	synchronized void nullKeyAndData(int index) {
        keyArray[index] = null;
        keyIdArray[index] = Optr.emptyPointer;
        keyUpdatedArray[index] = true;
        dataArray[index] = null;
        dataIdArray[index] = Optr.emptyPointer;
        dataUpdatedArray[index] = true;
        setUpdated(true);
	}
	
	public synchronized String toString() {
		int endd;
		StringBuffer sb = new StringBuffer();
		//sb.append("Page ");
		//sb.append(hashCode());
		sb.append("<<<<<<<<<<BTreeKeyPage Id:");
		sb.append(GlobalDBIO.valueOf(pageId));
		sb.append(" Numkeys:");
		sb.append(String.valueOf(getNumKeys()));
		sb.append(" Leaf:");
		sb.append(getmIsLeafNode());
		sb.append(" Updated:");
		sb.append(String.valueOf(updated)+"\r\n");
		
		sb.append("Key Array:\r\n");
		if( keyArray == null ) {
			sb.append("Key ARRAY IS NULL\r\n");
		} else {
			for (int i = 0; i < numKeys /*keyArray.length*/; i++) {
				sb.append(i+"=");
				sb.append(keyIdArray[i]);
				sb.append(",");
				sb.append(keyArray[i]+" updated=");
				sb.append(keyUpdatedArray[i]+"\r\n");
			}
		}
		sb.append("BTree Page Array:\r\n");
		if( pageArray == null ) {
			sb.append("PAGE ARRAY IS NULL\r\n");
		} else {
			int j = 0;
			for (int i = 0 ; i <= numKeys /*pageArray.length*/; i++) {
			//	sb.append(i+"=");
			//	sb.append(pageArray[i]+"\r\n");
				sb.append(i+"=");
				sb.append(pageIdArray[i] == -1 ? "Empty" : GlobalDBIO.valueOf(pageIdArray[i]));
				sb.append(",");
				sb.append(pageArray[i]);
				sb.append("\r\n");
				if(pageIdArray[i] != -1) ++j;
			}
			sb.append("Page ID Array Non null for "+j+" members\r\n");
		}
		sb.append("Data Array:\r\n");
		if(dataArray==null) {
			sb.append(" DATA ARRAY NULL\r\n");
		} else {
			for(int i = 0; i < numKeys; i++) {
				sb.append(i+"=");
				sb.append(dataIdArray[i]+",");
				sb.append(dataArray[i]+" ");
				sb.append("updated=");
				sb.append(dataUpdatedArray[i]);
				sb.append("\r\n");
			}
		}
		sb.append(GlobalDBIO.valueOf(pageId));
		sb.append(" >>>>>>>>>>>>>>End ");
		sb.append("\r\n");
		return sb.toString();
	}

	public synchronized boolean isUpdated() {
		return updated;
	}

	public synchronized void setUpdated(boolean updated) {
		this.updated = updated;
	}
	

	/**
	 * Determine if this key is in the list of array elements for this page
	 * @param key
	 * @return
	 * @throws IOException 
	 */
    synchronized boolean contains(int key) throws IOException {
        return search(key).atKey;
    }               

    /**
     * Find the place in the key array where the target key is less than the key value in the array element.
     * @param key The target key
     * @return The index where target is < array index value, if end, return numKeys
     */
    synchronized int subtreeRootNodeIndex(Comparable key) {
        for (int i = 0; i < getNumKeys(); i++) {                            
                if (key.compareTo(keyArray[i]) < 0) {
                        return i;
                }                               
        }
        return getNumKeys();
    }
    
	public synchronized void setKey(int i, Comparable key) {
		putKeyToArray(key, i);
	}

	/**
	 * Force a full write of all records at write time or turn it off.
	 * @param b
	 */
	public synchronized void setAllUpdated(boolean b) {
		for(int i = 0; i < MAXKEYS; i++) {
			keyUpdatedArray[i] = b;
			dataUpdatedArray[i] = b;
		}
		setUpdated(true);
	}

	public synchronized Datablock getDatablock() {
		return lbai.getBlk();
	}

	public synchronized BlockAccessIndex getBlockAccessIndex() {
		return lbai;
	}

	public synchronized void setKeyUpdatedArray(int targetIndex, boolean b) {
		keyUpdatedArray[targetIndex] = b;	
	}

	/**
	 * @return the mIsLeafNode
	 */
	public synchronized boolean getmIsLeafNode() {
		return mIsLeafNode;
	}

	/**
	 * Sets node updated as this is a persistent param
	 * @param mIsLeafNode the mIsLeafNode to set
	 */
	public synchronized void setmIsLeafNode(boolean mIsLeafNode) {
		this.mIsLeafNode = mIsLeafNode;
		updated = true;
	}

	/**
	 * @return the numKeys
	 */
	public synchronized int getNumKeys() {
		return numKeys;
	}

	/**
	 * @param numKeys the numKeys to set
	 */
	public synchronized void setNumKeys(int numKeys) {
		if(DEBUGSETNUMKEYS)
			System.out.printf("Setting num keys=%d MAX=%d leaf:%b page:%s%n", numKeys, MAXKEYS, mIsLeafNode, GlobalDBIO.valueOf(pageId));
		this.numKeys = numKeys;
		updated = true;
	}

	

}
