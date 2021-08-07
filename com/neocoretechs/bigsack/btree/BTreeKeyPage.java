package com.neocoretechs.bigsack.btree;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.function.Supplier;

import com.neocoretechs.bigsack.DBPhysicalConstants;
import com.neocoretechs.bigsack.hashmap.HTNode;
import com.neocoretechs.bigsack.io.Optr;
import com.neocoretechs.bigsack.io.pooled.BlockAccessIndex;
import com.neocoretechs.bigsack.io.pooled.BlockStream;
import com.neocoretechs.bigsack.io.pooled.Datablock;
import com.neocoretechs.bigsack.io.pooled.GlobalDBIO;
import com.neocoretechs.bigsack.keyvaluepages.KVIteratorIF;
import com.neocoretechs.bigsack.keyvaluepages.KeyPageInterface;
import com.neocoretechs.bigsack.keyvaluepages.KeySearchResult;
import com.neocoretechs.bigsack.keyvaluepages.KeyValue;
import com.neocoretechs.bigsack.keyvaluepages.KeyValueMainInterface;
import com.neocoretechs.bigsack.keyvaluepages.NodeInterface;

/*
* Copyright (c) 2003,2014,2021 NeoCoreTechs
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
* A key page in the bTree. This class functions as a wrapper, or facade pattern as an interface
* between the in-memory BTree object model and the buffer pool/persistent storage disk, non-volatile 
* memory structure. It contains fewer data elements of its own then HMap since ulike HMap a page will always contain a node
* because there are no keys-only pages.<p/>
* The in-memory BTnode/BTree object model Performs operations on its set of keys and
* optional sets of object values. The non-volatile model persists itself to the buffer pool as as block stream that appears
* as input and output streams connected to pages in the backing store.<p/>
* MAXKEYS are the odd maximum keys without spanning page boundaries, calculated by block payload divided by keysize.
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
* @author Jonathan Groff Copyright (C) NeoCoreTechs 2014,2015,2017,2021
*/
public class BTreeKeyPage implements KeyPageInterface {
	private static final boolean DEBUG = true;
	private static final boolean DEBUGPUTKEY = false;
	private static final boolean DEBUGREMOVE = true;
	private static final boolean DEBUGSETNUMKEYS = false;
	private static final boolean DEBUGGETDATA = false;
	private static final boolean DEBUGPUTDATA = false;
	public static final int BTREEKEYSIZE = 28; // total size per key/value 2 Optr for key/value + child node page to the left
	public static final int BTREEDATASIZE = 17; // extra data in key/value page, long number of keys, long last child right node page ID, one byte for leaf
	public static int TOTALKEYS = 
			((int) Math.floor((DBPhysicalConstants.DATASIZE-BTREEDATASIZE)/BTREEKEYSIZE));
	// number of keys per page; number of instances of the non transient fields of 'this' per DB block.
	// The number of maximum children is MAXKEYS+1 per node.
	// Calculate the maximum number of odd keys that can fit per block. Must be odd for splits that leave an even balance of left/right
	public static int MAXKEYS = (TOTALKEYS % 2 == 0 ? TOTALKEYS - 1 : TOTALKEYS);
	// Global is this leaf node flag.
	//private boolean mIsLeafNode = true; //1 byte, SINGLE ENTRY ,We treat as leaf since the logic is geared to proving it not
	// Global page updated flag.
	//private transient boolean updated = false; // has the node been updated for purposes of write
	//
	protected transient BlockAccessIndex lbai = null; // The page is tied to a block
	protected transient KeyValueMainInterface bTreeMain;
	protected transient NodeInterface<Comparable, Object> bTNode = null;
	private long numKeys = 0L;
	/**
	 * This is called from getPageFromPool get set up a new clean node
	 * @param bTMain The database IO main class instance of KeyValueMainInterface
	 * @param lbai The BlockAcceesIndex page block holding page data
	 * @param read true to read the contents of the btree key from page, otherwise a new page to be filled
	 */
	public BTreeKeyPage(KeyValueMainInterface bTree, BlockAccessIndex lbai, boolean read) throws IOException {
		this.bTreeMain = bTree;
		this.lbai = lbai;
		this.bTNode = new BTNode(this);
		if( DEBUG ) 
			System.out.printf("%s ctor1 BlockAccessIndex:%s for MAXKEYS=%d%n",this.getClass().getName(), lbai, MAXKEYS);
		//initTransients();
		// Pre-allocate the arrays that hold persistent data
		//setupKeyArrays();
		if( read && lbai.getBlk().getBytesinuse() > 0) {// intentional clear or we may have deleted or rolled back all the way to primordial
			readFromDBStream(lbai.getDBStream());
		} else {
			// If we are not reading, we must be preparing the block for new key. Really no
			// reason for a new block with unassigned and not updating keys conceptually.
			// Set the appropriate flags to write to associated block when the time comes
			lbai.resetBlock(false); // set up headers without revoking access
		}
		
		if( DEBUG ) 
			System.out.printf("%s ctor1 exit BlockAccessIndex:%s for MAXKEYS=%d%n",this.getClass().getName(), lbai, MAXKEYS);
	}


	/**
	 * Initialize the key page NON-TRANSIENT arrays, the part that actually gets written to backing store.
	 
	public synchronized void setupKeyArrays() {
		// Pre-allocate the arrays that hold persistent data
		setKeyIdArray(new Optr[MAXKEYS]);
		pageIdArray= new long[MAXKEYS + 1];
		dataIdArray= new Optr[MAXKEYS];
		for (int i = 0; i <= MAXKEYS; i++) {
			pageIdArray[i] = -1L;
			if( i != MAXKEYS ) {
				getKeyIdArray()[i] = Optr.emptyPointer;
				getKeyUpdatedArray()[i] = false;
				dataIdArray[i] = Optr.emptyPointer;
				dataUpdatedArray[i] = false;
			}
		}
	}
	*/
	@Override
	public synchronized long getPageId() {
		return lbai.getBlockNum();
	}
	/**
	 * Set the node associated with this page. This will called back from {@link BlockAccessIndex}
	 * static method getPageFromPool to set up a new node.
	 * This action come via BTree create to set the node.
	 * @param btnode
	 */
	@Override
	public synchronized void setNode(NodeInterface btnode) {
		this.bTNode = btnode;
	}
	/**
	 * Read the page using the given DataInputStream
	 * @throws IOException
	 */
	@Override
	public synchronized void readFromDBStream(DataInputStream dis) throws IOException {
		// check for fresh database
		if(dis.available() < 8)
			return;
		setNumKeys((int) dis.readLong()); // size
		setmIsLeafNode(dis.readByte() == 0 ? false : true);
		for(int i = 0; i < getNumKeys(); i++) {
			bTNode.initKeyValueArray(i);
			long sblk = dis.readLong();
			short shblk = dis.readShort();
			//if( DEBUG ) { 
			//	System.out.println("block of key "+i+":"+GlobalDBIO.valueOf(sblk)+" offset of key "+i+":"+shblk);
			//}
			bTNode.getKeyValueArray(i).setKeyOptr(new Optr(sblk, shblk));
			//
			sblk = dis.readLong();
			shblk = dis.readShort();
			//if( DEBUG ) { 
			//	System.out.println("block of data "+i+":"+GlobalDBIO.valueOf(sblk)+" offset of data "+i+":"+shblk);
			//}
			bTNode.getKeyValueArray(i).setValueOptr(new Optr(sblk, shblk));
			// page Id
			((BTNode)bTNode).childPages[i] = dis.readLong();
		}
		// final right pageId
		((BTNode)bTNode).childPages[getNumKeys()] = dis.readLong();
	}
	
	/**
	 * Set the key Id array, and set the keyUpdatedArray for the key and the general updated flag
	 * @param index
	 * @param optr
	 */
	@Override
	public synchronized void setKeyIdArray(int index, Optr optr, boolean update) {
		bTNode.initKeyValueArray(index);
		getKeyValueArray(index).setKeyOptr(optr);
		bTNode.getKeyValueArray(index).setKeyUpdated(update);
		((BTNode)bTNode).setUpdated(update);
	}
	
	@Override
	public synchronized Optr getKeyId(int index) {
		return getKeyValueArray(index).getKeyOptr();
	}
	/**
	 * Set the Data Id array, and set the dataUpdatedArray for the key and the general updated flag
	 * @param index
	 * @param optr The Vblock and offset within that block of the first data item for the key/value value associated data if any
	 */
	@Override
	public synchronized void setDataIdArray(int index, Optr optr, boolean update) {
		getKeyValueArray(index).setValueOptr(optr);
		getKeyValueArray(index).setValueUpdated(update);
		((BTNode)bTNode).setUpdated(update);
	}
	
	@Override
	public synchronized Optr getDataId(int index) {
		return getKeyValueArray(index).getValueOptr();
	}
	/**
	 * Set the Key page nodes Id array, and set the pageUpdatedArray for the key and the general updated flag
	 * @param index
	 * @param optr The long VBlock of the key page, we dont need Optr offset because offset of key pages is always 0
	 */
	@Override
	public synchronized void setPageIdArray(int index, long optr, boolean update) {
		((BTNode)bTNode.getChildNoread(index)).setPageId(optr);
		((BTNode)bTNode).setUpdated(update);
	}
	
	@Override
	public synchronized long getPageId(int index) {
		return ((BTNode)bTNode.getChildNoread(index)).getPageId();
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
	synchronized KeySearchResult search(Comparable targetKey) throws IOException {
		if(targetKey == null)
			System.out.printf("%s.search target key is null", this.getClass().getName());
		int middleIndex = 1; 
        int leftIndex = 0;
        int rightIndex = getNumKeys() - 1;
        // no keys, call for insert at 0
        if( rightIndex == -1)
        	return new KeySearchResult(0, false);
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
        			return new KeySearchResult(middleIndex, true);
        		}
        }
        if( DEBUG )
        	System.out.println("BtreeKeyPage.search falling thru "+middleIndex+" "+leftIndex+" "+rightIndex+" "+this+" target:"+targetKey);
        return new KeySearchResult(middleIndex, false);
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
	 * @see HMapMain.delete for a description of the logic applied to deletion.
	 * @param targetKey
	 * @throws IOException 
	 */
	synchronized void remove(Comparable targetKey) throws IOException {
	
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
		//System.out.println("KeyPageInterface.delete "+this+" index:"+index);
		if( bTNode.getKeyValueArray(index) == null )
			throw new IOException("Node at index "+index+" null for attempted delete");
		if( !bTNode.getKeyValueArray(index).getKeyOptr().equals(Optr.emptyPointer))
			bTreeMain.getIO().delete_object(bTNode.getKeyValueArray(index).getKeyOptr(), GlobalDBIO.getObjectAsBytes(bTNode.getKeyValueArray(index).getmKey()).length);
		if( bTNode.getKeyValueArray(index).getValueOptr() != null && !bTNode.getKeyValueArray(index).getValueOptr().equals(Optr.emptyPointer))
			bTreeMain.getIO().delete_object(bTNode.getKeyValueArray(index).getValueOptr(), GlobalDBIO.getObjectAsBytes(bTNode.getKeyValueArray(index).getmValue()).length);
		// If its the rightmost key ignore move
		setUpdated(true);
	}

	/**
	 * Put the BtreeKeyPage to BlockAccessIndex to deep store.
	 * Put the updated keys to the buffer pool at available block space.
	 * The data is written to the BlockAccessIndex, the push to deep store takes place at commit time or
	 * when the buffer fills and it becomes necessary to open a spot.
	 * @param index
	 * @param keys The list of unique blocks that already contain entries for more efficient clustering. We will try to place new entry in one.
	 * @return
	 * @throws IOException
	 */
	public synchronized boolean putKey(int index, ArrayList<Long> keys) throws IOException {
		if(getKeyValueArray(index).getmKey() == null) {
			if(DEBUG || DEBUGPUTKEY) 
				System.out.printf("%s.putKey index=%d, key=%s Optr=%s%n", this.getClass().getName(),
						index,(getKeyValueArray(index) == null ? "key array index is null": 
						(getKeyValueArray(index).getmKey() == null ? "key is null " : getKeyValueArray(index).getmKey())),
						(getKeyValueArray(index) == null ? "key array index is still null " : getKeyValueArray(index).getKeyOptr()));
			return false;
		}
		// get first block to write contiguous records for keys
		// We either have a block with some space or one we took from freechain list
		byte[] pb = GlobalDBIO.getObjectAsBytes(getKeyValueArray(index).getmKey());
		getKeyValueArray(index).setKeyOptr(lbai.getSdbio().getIOManager().getNewInsertPosition(keys, pb.length));
		lbai.getSdbio().add_object(getKeyValueArray(index).getKeyOptr(), pb, pb.length);
		if(DEBUG || DEBUGPUTKEY) 
				System.out.println("KeyPageInterface.putKey Added object:"+getKeyValueArray(index).getmKey()+" @"+getKeyValueArray(index)+" bytes:"+pb.length);
		return true;
	}
	
	/**
	 * Create a unique list of blocks that have already been populated with values from this node in order to possibly
	 * cluster new entries more efficiently.
	 * @return The list of unique blocks containing entries for this node.
	 */
	public ArrayList<Long> aggregatePayloadBlocks() {
		ArrayList<Long> blocks = new ArrayList<Long>();
		int i = 0;
		for(; i < bTNode.getNumKeys(); i++) {
			if(getKeyValueArray(i) != null) { 
				if(getKeyValueArray(i).getKeyOptr() != null && 
					!blocks.contains(getKeyValueArray(i).getKeyOptr().getBlock()) &&
					!getKeyValueArray(i).getKeyOptr().equals(Optr.emptyPointer) ) {
						blocks.add(getKeyValueArray(i).getKeyOptr().getBlock());
				}
				if(getKeyValueArray(i).getValueOptr() != null && 
					!blocks.contains(getKeyValueArray(i).getValueOptr().getBlock()) &&
					!getKeyValueArray(i).getValueOptr().equals(Optr.emptyPointer) ) {
						blocks.add(getKeyValueArray(i).getKeyOptr().getBlock());
				}
			}
		}
		return blocks;
	}
	
	@Override
	public synchronized void putPage() throws IOException {
		if(bTNode == null) {
			throw new IOException(String.format("%s.putPage BTNode is null:%s%n",this.getClass().getName(),this));
		}
		if(!isUpdated()) {
			if(DEBUG)
				System.out.printf("%s.putPage page NOT updated:%s%n",this.getClass().getName(),this);
			return;
			//throw new IOException("KeyPageInterface.putPage page NOT updated:"+this);
		}
		if( DEBUG ) 
			System.out.printf("%s.putPage:%s%n",this.getClass().getName(),this);
		// hold accumulated insert pages
		ArrayList<Long> currentPayloadBlocks = aggregatePayloadBlocks();
		// Persist each key that is updated to fill the keyIds in the current page
		// Once this is complete we write the page contiguously
		// Write the object serialized keys out to deep store, we want to do this out of band of writing key page
		for(int i = 0; i < getNumKeys(); i++) {
			if( getKeyValueArray(i) != null ) {
				if(getKeyValueArray(i).getKeyUpdated()) {
					// put the key to a block via serialization and assign KeyIdArray the position of stored key
					putKey(i, currentPayloadBlocks);
				}
				if(getKeyValueArray(i).getValueUpdated()) {
					putData(i, currentPayloadBlocks);
				}
			}
		}
		//
		assert (lbai.getBlockNum() != -1L) : " KeyPageInterface unlinked from page pool:"+this;
		// write the page to the current block
		// Write to the block output stream
		DataOutputStream bs = GlobalDBIO.getBlockOutputStream(lbai);
		bs.writeLong(getNumKeys());
		bs.writeByte(getmIsLeafNode() ? 1 : 0);
		for(int i = 0; i < getNumKeys(); i++) {
			if(getKeyValueArray(i) != null && getKeyValueArray(i).getKeyUpdated() ) { // if set, key was processed by putKey[i]
				bs.writeLong(getKeyValueArray(i).getKeyOptr().getBlock());
				bs.writeShort(getKeyValueArray(i).getKeyOptr().getOffset());
				getKeyValueArray(i).setKeyUpdated(false);
				if( DEBUG ) 
					System.out.printf("%s.putPage %d Optr key:%s%n",this.getClass().getName(),i,getKeyValueArray(i));
			} else { // skip 
				lbai.setByteindex((short) (lbai.getByteindex()+10));
				if( DEBUG ) 
					System.out.printf("%s.putPage %d Optr key skipped:%s%n",this.getClass().getName(),i,getKeyValueArray(i));
			}
			// data array
			if(getKeyValueArray(i) != null && getKeyValueArray(i).getValueUpdated()) {
				bs.writeLong(getKeyValueArray(i).getValueOptr().getBlock());
				bs.writeShort(getKeyValueArray(i).getValueOptr().getOffset());
				getKeyValueArray(i).setValueUpdated(false);
				if( DEBUG ) 
					System.out.printf("%s.putPage %d Optr value:%s%n",this.getClass().getName(),i,getKeyValueArray(i));	
			} else {
				// skip the data Id for this index as it was not updated, so no need to write anything
				lbai.setByteindex((short) (lbai.getByteindex()+10));
				if( DEBUG ) 
					System.out.printf("%s.putPage %d value Optr skipped:%s%n",this.getClass().getName(),i,getKeyValueArray(i));
			}
			bs.writeLong(((BTNode)(bTNode.getChild(i))).getPageId());
		}
		bs.writeLong(((BTNode)(bTNode.getChild(getNumKeys()))).getPageId());
		bs.flush();
		bs.close();
		if( DEBUG ) {
			System.out.println("KeyPageInterface.putPage Added Keypage @"+GlobalDBIO.valueOf(getBlockAccessIndex().getBlockNum())+" block for keypage:"+this+" page:"+this);
		}	
	}
	
	/**
	 * At KeyPageInterface putPage time, we resolve the lazy elements to actual VBlock,offset
	 * This method puts the values associated with a key/value pair, if using maps vs sets.
	 * Deletion of previous data has to occur before we get here, as we only have a payload to write, not an old one to remove.
	 * @param index Index of KeyPageInterface key and data value array
	 * @param values The list of unique blocks that already contain entries for more efficient clustering. We will try to place new entry in one.
	 * @throws IOException
	 */
	@Override
	public synchronized boolean putData(int index, ArrayList<Long> values) throws IOException {

		if( getKeyValueArray(index).getmValue() == null ) {
			//|| bTreeKeyPage.getKeyValueArray()[index].getValueOptr().equals(Optr.emptyPointer)) {
			getKeyValueArray(index).setValueOptr(Optr.emptyPointer);
			if( DEBUGPUTDATA )
					System.out.println("KeyPageInterface.putData ADDING NULL value for key index "+index);
			return false;
		}
		// pack the page into this tablespace and within blocks the same tablespace as key
		// the new insert position will attempt to find a block with space relative to established positions
		byte[] pb = GlobalDBIO.getObjectAsBytes(getKeyValueArray(index).getmValue());
		getKeyValueArray(index).setValueOptr(lbai.getSdbio().getIOManager().getNewInsertPosition(values, pb.length));		
		if( DEBUGPUTDATA )
			System.out.println("KeyPageInterface.putData ADDING NON NULL value "+getKeyValueArray(index)+" for key index "+index+" at "+
				getKeyValueArray(index).getValueOptr());
		lbai.getSdbio().add_object(getKeyValueArray(index).getValueOptr(), pb, pb.length);
		return true;
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
	@Override
	public synchronized KeyPageInterface getPage(int index) throws IOException {
		BTreeKeyPage btk = null;
		if(bTNode == null ) {
			System.out.printf("%s.getPage null bTNode trying to rerieve index%d%n",this.getClass().getName(), index);
			return null;
		}
		if(DEBUG) {
			System.out.println("KeyPageInterface.getPage ENTER KeyPageInterface to retrieve BTNode "+bTNode+" target index:["+index+"]");
			//for(int i = 0; i <= bTNode.mCurrentKeyNum; i++) {
			//	System.out.println("KeyPageInterface.getPage initial page index:["+i+"]="+GlobalDBIO.valueOf(bTNode.mChildren[i].pageId)+" page:"+bTNode.mChildren[index]);
			//}
		}
		if(bTNode.getChildNoread(index) == null) {
			if(DEBUG)
				System.out.printf("%s.getPage target index:[%d] child at index null, cant get page..",this.getClass().getName(), index);
			return null;
		}
		if(((BTNode)bTNode.getChildNoread(index)).getPageId() != -1L) {
			// has a key to retrieve page
			if( DEBUG ) {
				System.out.println("KeyPageInterface.getPage about to retrieve index:["+index+"] loc:"+GlobalDBIO.valueOf(((BTNode)bTNode.getChildNoread(index)).getPageId()));
			}
			// this will read the data values for the page
			btk = (BTreeKeyPage) bTreeMain.getIO().getBTreePageFromPool(((BTNode) bTNode.getChildNoread(index)).getPageId());
			btk.bTNode = (BTNode<Comparable, Object>) bTNode.getChild(index);
			if( DEBUG ) {
				System.out.println("KeyPageInterface.getPage RETRIEVED index:"+index+" loc:"+GlobalDBIO.valueOf(((BTNode)bTNode.getChildNoread(index)).getPageId())+" page:"+bTNode.getChildNoread(index));
			}
		}

		// see if other pages have same value
		if(DEBUG) {
			for(int i = 0; i < bTNode.getNumKeys(); i++) {
				if( i == index )
					continue;
				if (bTNode.getChildNoread(i) != null && ((BTNode)bTNode.getChildNoread(i)).getPageId() != -1L && bTNode.getChildNoread(i) == bTNode.getChildNoread(index)) {
					throw new IOException("Duplicate child page encountered");
				}
			}
		}
		return btk;
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
	@Override
	public synchronized Comparable getKey(int index) throws IOException {
		if(DEBUG) {
			System.out.println("KeyPageInterface.getKey Entering KeyPageInterface to retrieve target index "+index);
		}
		bTNode.initKeyValueArray(index);
		if(bTNode.getKeyValueArray(index).getmKey() == null && !bTNode.getKeyValueArray(index).getKeyOptr().isEmptyPointer() && !bTNode.getKeyValueArray(index).getKeyUpdated()) {
			// eligible to retrieve page
			if( DEBUG ) {
				System.out.println("KeyPageInterface.getKey about to retrieve index:"+index+" loc:"+bTNode.getKeyValueArray(index).getKeyOptr());
			}
			bTNode.getKeyValueArray(index).setmKey((Comparable) bTreeMain.getIO().deserializeObject(bTNode.getKeyValueArray(index).getKeyOptr()));
			if( DEBUG ) {
				System.out.println("KeyPageInterface.getKey retrieved index:"+index+" loc:"+bTNode.getKeyValueArray(index).getKeyOptr()+" retrieved:"+bTNode.getKeyValueArray(index).getmKey());
				for(int i = 0; i < getNumKeys(); i++)System.out.println(i+"="+bTNode.getKeyValueArray(index).getmKey());
			}
			bTNode.getKeyValueArray(index).setKeyUpdated(false);
		}
		return bTNode.getKeyValueArray(index).getmKey();
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
	@Override
	public synchronized Object getData(int index) throws IOException {
		if(DEBUG) {
			System.out.println("KeyPageInterface.getKey Entering KeyPageInterface to retrieve target index "+index);
		}
		bTNode.initKeyValueArray(index);
		if(bTNode.getKeyValueArray(index).getmValue() == null && !bTNode.getKeyValueArray(index).getValueOptr().isEmptyPointer() && !bTNode.getKeyValueArray(index).getValueUpdated() ){
			// eligible to retrieve page
			if( DEBUG ) {
				System.out.println("KeyPageInterface.getData about to retrieve index:"+index+" loc:"+bTNode.getKeyValueArray(index).getValueOptr());
			}
			bTNode.getKeyValueArray(index).setmValue((Comparable) bTreeMain.getIO().deserializeObject(bTNode.getKeyValueArray(index).getValueOptr()));
			if( DEBUG ) {
				System.out.println("KeyPageInterface.getData retrieved index:"+index+" loc:"+bTNode.getKeyValueArray(index).getValueOptr()+" retrieved:"+bTNode.getKeyValueArray(index).getmValue());
				for(int i = 0; i < getNumKeys(); i++)System.out.println(i+"="+bTNode.getKeyValueArray(index).getmValue());
			}
			bTNode.getKeyValueArray(index).setValueUpdated(false);
		}
		return bTNode.getKeyValueArray(index).getmValue();
	}

	@SuppressWarnings("rawtypes")
	/**
	 * Using key, put keyArray[index] = key
	 * set updated true
	 * @param key
	 * @param index
	 */
	synchronized void putKeyToArray(Comparable key, int index) {
		bTNode.initKeyValueArray(index);
		bTNode.getKeyValueArray(index).setmKey(key);
		bTNode.getKeyValueArray(index).setKeyOptr(Optr.emptyPointer);
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
		bTNode.initKeyValueArray(targetIndex);
		bTNode.getKeyValueArray(targetIndex).setmKey(sourceKey.getKey(sourceIndex)); // get the key from pointer from source if not already
		bTNode.getKeyValueArray(targetIndex).setKeyOptr(sourceKey.getKeyId(sourceIndex));
		setUpdated(true);
	}
	
	synchronized void copyDataToArray(BTreeKeyPage sourceKey, int sourceIndex, int targetIndex) throws IOException {
		bTNode.initKeyValueArray(targetIndex);
		bTNode.getKeyValueArray(targetIndex).setmValue(sourceKey.getData(sourceIndex)); // get the key from pointer from source if not already
		bTNode.getKeyValueArray(targetIndex).setValueOptr(sourceKey.getDataId(sourceIndex));
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
		bTNode.initKeyValueArray(index);
		bTNode.getKeyValueArray(index).setmValue(data);
		bTNode.getKeyValueArray(index).setValueOptr(Optr.emptyPointer);
		setUpdated(true);
	}
	/**
	 * Set the keyArray and dataArray to null for this index. Set the keyIdArray and dataIdArray to empty locations.
	 * Set the updated flag for the key and data fields, then set updated for the record.
	 * @param index The target index we are updating for both key and data.
	 */
	synchronized void nullKeyAndData(int index) {
       putKeyToArray(null, index);
       putDataToArray(null, index);
       setUpdated(true);
	}
	
	public synchronized String toString() {
		StringBuffer sb = new StringBuffer();
		//sb.append("Page ");
		//sb.append(hashCode());
		sb.append("<<<<<<<<<<KeyPageInterface BlockAccessIndex:");
		sb.append(lbai);
		sb.append("\r\n");
		sb.append("BTNode:");
		sb.append(bTNode);
		sb.append("\r\n");
		return sb.toString();
	}

	@Override
	public synchronized boolean isUpdated() {
		return ((BTNode)bTNode).getUpdated();
	}

	@Override
	public synchronized void setUpdated(boolean updated) {
		((BTNode)bTNode).setUpdated(updated);
	}       

    /**
     * Find the place in the key array where the target key is less than the key value in the array element.
     * @param key The target key
     * @return The index where target is < array index value, if end, return numKeys
     * @throws IOException 
     */
    synchronized int subtreeRootNodeIndex(Comparable key) throws IOException {
        for (int i = 0; i < getNumKeys(); i++) {                            
                if (key.compareTo(getKeyValueArray(i).getmKey()) < 0) {
                        return i;
                }                               
        }
        return getNumKeys();
    }
    /**
     * Set the value of the index at i to the key.
     * @param i the child index into this node.
     * @param key The key to set index i to.
     */
	@Override
	public synchronized void setKey(int i, Comparable key) {
		putKeyToArray(key, i);
	}


	@Override
	public synchronized Datablock getDatablock() {
		return lbai.getBlk();
	}

	@Override
	public synchronized BlockAccessIndex getBlockAccessIndex() {
		return lbai;
	}

	/**
	 * @return the mIsLeafNode
	 */
	public synchronized boolean getmIsLeafNode() {
		return ((BTNode)bTNode).getIsLeaf();
	}

	/**
	 * Sets node updated as this is a persistent param
	 * @param mIsLeafNode the mIsLeafNode to set
	 */
	public synchronized void setmIsLeafNode(boolean mIsLeafNode) {
		((BTNode)bTNode).setmIsLeaf(mIsLeafNode);
		((BTNode)bTNode).setUpdated(true);
	}

	/**
	 * @return the numKeys
	 */
	@Override
	public synchronized int getNumKeys() {
		return (int) numKeys;
	}

	/**
	 * @param numKeys the numKeys to set
	 */
	@Override
	public synchronized void setNumKeys(int numKeys) {
		if(DEBUGSETNUMKEYS)
			System.out.printf("Setting num keys=%d MAX=%d leaf:%b page:%s%n", numKeys, MAXKEYS, ((BTNode)bTNode).getIsLeaf(), GlobalDBIO.valueOf(lbai.getBlockNum()));
		this.numKeys = numKeys;
		setUpdated(true);
		//bTNode.setNumKeys(numKeys);
		//((BTNode)bTNode).setUpdated(true);
	}
	
	/**
	 * Return the structure with all keys, values, and {@link Optr} database object instance pointers.
	 * @return the key/value array {@link KeyValue} from {@link BTNode}
	 */
	@Override
	public KeyValue<Comparable,Object> getKeyValueArray(int index) {
		return bTNode.getKeyValueArray(index);
	}
	
	@Override
	public KeyValueMainInterface getKeyValueMain() {
		return bTreeMain;
	}
	@Override
	public void setRootNode(BlockAccessIndex bai) throws IOException {
		this.lbai = bai;
		bTNode = new BTNode(((BTreeMain)bTreeMain).bTreeNavigator, 0L, true);
	}
	@Override
	public KeyValue<Comparable, Object> readBlockAndGetKV(DataInputStream dis, NodeInterface node) throws IOException {
		long block = dis.readLong();
		short offset = dis.readShort();
		Optr keyPtr = new Optr(block, offset);
		block = dis.readLong();
		offset = dis.readShort();
		Optr dataPtr = new Optr(block, offset);
		KeyValue<Comparable, Object> kv = new KeyValue<Comparable, Object>(node);
		kv.setKeyOptr(keyPtr);
		kv.setValueOptr(dataPtr);
		kv.setKeyUpdated(false);
		kv.setValueUpdated(false);
		return kv;
	}
	@Override
	public void retrieveEntriesInOrder(KVIteratorIF<Comparable, Object> iterImpl) {
		// TODO Auto-generated method stub
		
	}
	@Override
	public int retrieveEntriesInOrder(Supplier<KeyValue<Comparable, Object>> b, int count, int limit) {
		// TODO Auto-generated method stub
		return 0;
	}
	




}
