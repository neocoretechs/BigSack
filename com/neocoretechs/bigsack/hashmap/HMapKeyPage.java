package com.neocoretechs.bigsack.hashmap;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.function.Supplier;
import java.util.stream.Stream.Builder;

import com.neocoretechs.bigsack.DBPhysicalConstants;
import com.neocoretechs.bigsack.io.Optr;
import com.neocoretechs.bigsack.io.pooled.BlockAccessIndex;
import com.neocoretechs.bigsack.io.pooled.Datablock;
import com.neocoretechs.bigsack.io.pooled.GlobalDBIO;

import com.neocoretechs.bigsack.keyvaluepages.KVIteratorIF;
import com.neocoretechs.bigsack.keyvaluepages.KeyPageInterface;
import com.neocoretechs.bigsack.keyvaluepages.KeySearchResult;
import com.neocoretechs.bigsack.keyvaluepages.KeyValue;
import com.neocoretechs.bigsack.keyvaluepages.KeyValueMainInterface;
import com.neocoretechs.bigsack.keyvaluepages.NodeInterface;
import com.neocoretechs.bigsack.keyvaluepages.RootKeyPageInterface;

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
* A key page in the hMap. This class functions as a wrapper, or facade pattern as an interface
* between the in-memory hMap object model and the buffer pool/persistent storage disk, non-volatile 
* memory structure. It contains no appreciable data elements of its own except for the array of boolean flags
* that indicate whether object model elements have changes and should be persisted. Other than that, merely pointers
* to the hTnode htree node and the BlockAccessIndex NVM layout.<p/>
* The in-memory HTnode/HTree object model Performs operations on its set of keys and
* optional sets of object values. The non-volatile model persists itself to the buffer pool as as block stream that appears
* as input and output streams connected to pages in the backing store.<p/>
* The 'transient' keyword designations in the class fields are an artifact leftover from serialization, retained to 
* illustrate the items that are not persisted via block streams.
*
* @author Jonathan Groff Copyright (C) NeoCoreTechs 2021
*/
public final class HMapKeyPage implements KeyPageInterface {
	private static final boolean DEBUG = false;
	private static final boolean DEBUGPUTKEY = false;
	private static final boolean DEBUGREMOVE = false;
	private static final boolean DEBUGSETNUMKEYS = false;
	private static final boolean DEBUGGETDATA = false;
	private static final boolean DEBUGPUTDATA = false;
	public static final int HMAPKEYSIZE = 20; // total size per key/value 2 Optr for key/value
	public static final int HMAPDATASIZE = 16; // extra data in key/value page, long number of keys, long next page page ID
	public static int MAXKEYS = 
			((int) Math.floor((DBPhysicalConstants.DATASIZE-HMAPDATASIZE)/HMAPKEYSIZE));
	protected KeyValueMainInterface hMapMain;
	protected BlockAccessIndex lbai;
	protected NodeInterface<Comparable, Object> hTNode = null;
	protected long nextPageId = -1L; // lazy initialization of the collision space, redundant storage of nextPage.pageId to bootstrap
	protected KeyPageInterface nextPage = null; // next page of collision space
	private long numKeys = 0L;

	/**
	 * This is called from getPageFromPool get set up a node.
	 * @param sdbio The database IO main class
	 * @param lbai The BlockAcceesIndex page block holding page data
	 * @param read true to read the contents of the hMap keys from page already retrieved, otherwise a new page to be filled
	 */
	public HMapKeyPage(HMapMain hMapMain, BlockAccessIndex lbai, boolean read) throws IOException {
		this.hMapMain = hMapMain;
		this.lbai = lbai;
		this.hTNode = new HTNode(this);
		if( DEBUG ) 
			System.out.printf("%s ctor1 BlockAccessIndex:%s%n",this.getClass().getName(), lbai);
		if( read && lbai.getBlk().getBytesinuse() > 0) {// intentional clear or we may have deleted or rolled back all the way to primordial
			readFromDBStream(lbai.getDBStream());
		} else {
			// If we are not reading, we must be preparing the block for new key. Really no
			// reason for a new block with unassigned and not updating keys conceptually.
			// Set the appropriate flags to write to associated block when the time comes
			lbai.resetBlock(false); // set up headers without revoking access
		}		
		if( DEBUG ) 
			System.out.printf("%s ctor1 exit BlockAccessIndex:%s%n",this.getClass().getName(), lbai);
	}
	
	public synchronized long getPageId() {
		return lbai.getBlockNum();
	}
	
	/**
	 * Set the node associated with this page. This will called back from {@link BlockAccessIndex}
	 * static method getPageFromPool to set up a new node.
	 * This action come via BTree create to set the node.
	 * @param btnode
	 */
	public synchronized void setNode(NodeInterface htnode) {
		this.hTNode = htnode;
	}
	
	@Override
	public KeyValueMainInterface getKeyValueMain() {
		return hMapMain;
	}

	@Override
	/**
	 * Does not apply here due to creation of collision space 'root's as a different process
	 */
	public void setRootNode(BlockAccessIndex bai) throws IOException {
	}

	/**
	 * Read the page using the given DataInputStream
	 * @throws IOException
	 */
	public synchronized void readFromDBStream(DataInputStream dis) throws IOException {
		if(dis.available() < 8)
			return; // brand new root page allocated from scratch?
		numKeys = (int) dis.readLong();
		hTNode.setNumKeys((int) numKeys);
		if(DEBUG )
			System.out.printf("%s.readFromDBStream numKeys=%d%n",this.getClass().getName(),numKeys);
		for(int i = 0; i < numKeys; i++) {
			long sblk = dis.readLong();
			short shblk = dis.readShort();
			//if( DEBUG ) { 
			//	System.out.println("block of key "+i+":"+GlobalDBIO.valueOf(sblk)+" offset of key "+i+":"+shblk);
			//}
			hTNode.initKeyValueArray(i);
			hTNode.getKeyValueArray(i).setKeyOptr(new Optr(sblk, shblk));
			//
			sblk = dis.readLong();
			shblk = dis.readShort();
			//if( DEBUG ) { 
			//	System.out.println("block of data "+i+":"+GlobalDBIO.valueOf(sblk)+" offset of data "+i+":"+shblk);
			//}
			hTNode.getKeyValueArray(i).setValueOptr(new Optr(sblk, shblk));
		}
		nextPageId = dis.readLong(); // next page in collision space, if any
		if(nextPageId != -1L) {
			createAndSetNextHTNode(this); // queue it up
		}
	}
	
	@Override
	/**
	 * Set the key Id array, and set the keyUpdatedArray for the key and the general updated flag
	 * @param index
	 * @param optr
	 * @param update status of the node to updated
	 * @param keyState status of the key/value entry in relation to backing store
	 */
	public synchronized void setKeyIdArray(int index, Optr optr, boolean update, KeyValue.synchStates keyState) {
		hTNode.initKeyValueArray(index);
		getKeyValueArray(index).setKeyOptr(optr);
		hTNode.getKeyValueArray(index).keyState = keyState;
		((HTNode)hTNode).setUpdated(update);
	}
	
	public synchronized Optr getKeyId(int index) {
		return getKeyValueArray(index).getKeyOptr();
	}
	
	@Override
	/**
	 * Set the Data Id array, and set the dataUpdatedArray for the key and the general updated flag
	 * @param index
	 * @param optr The Vblock and offset within that block of the first data item for the key/value value associated data if any
	 * @param update status of the node to updated
	 * @param keyState status of the key/value entry in relation to backing store
	 */
	public synchronized void setDataIdArray(int index, Optr optr, boolean update, KeyValue.synchStates valueState) {
		getKeyValueArray(index).setValueOptr(optr);
		getKeyValueArray(index).valueState = valueState;
		((HTNode)hTNode).setUpdated(update);
	}
	
	public synchronized Optr getDataId(int index) {
		return getKeyValueArray(index).getValueOptr();
	}
	/**
	 * Set the Key page nodes Id array, and set the pageUpdatedArray for the key and the general updated flag
	 * @param index
	 * @param optr The long VBlock of the key page, we dont need Optr offset because offset of key pages is always 0
	 */
	public synchronized void setPageIdArray(int index, long optr, boolean update) {
	}
	
	public synchronized long getPageId(int index) {
		return lbai.getBlockNum();
	}
	
	public synchronized void setPageId(long blockNum) throws IOException {
		lbai.setBlockNumber(blockNum);
	}
	
	/**
	* Search this page for a target key.<p/>
	* The object model will be affected to the extent that entries in the pipeline for lazy initialization
	* will be initialized. We are calling getNumKeys() and getKey(index).
	* @throws IOException 
	* @return the KeySearchResult with page index, this page, and whether the key was found at that index, otherwise 0 and/or MaxIndex
	*/
	@SuppressWarnings({ "rawtypes", "unchecked" })
	synchronized KeySearchResult search(Comparable targetKey) throws IOException {
		if(DEBUG)
			System.out.printf("%s.search target key %s page %s%n", this.getClass().getName(), targetKey, this);
		if(targetKey == null) {
			throw new IOException(String.format("%s.search target key is null", this.getClass().getName()));
		}
        int index = 0;
        int maxIndex = (int) numKeys;
        // no keys, call for insert at 0
        if( maxIndex == 0)
        	return new KeySearchResult(this, 0, false);
        while (index < maxIndex) {
    		if(getKey(index) == null) {
    			if(DEBUG)
    				System.out.printf("%s.search getKey(%d) is null for target key:%s page:%s%n", this.getClass().getName(),index,targetKey,this);
    			continue;
    		}
        	int cmpRes = getKey(index).compareTo(targetKey);
        	if (cmpRes != 0 ) {
        		++index;
        	} else 
        		return new KeySearchResult(this, index, true);
        }
        if( DEBUG )
        	System.out.printf("%s.search(%s) falling thru maxIndex:%d for page:%s%n ",this.getClass().getName(),targetKey,index,this);
        return new KeySearchResult(this, index, false);
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
		if( hTNode.getKeyValueArray(index) == null )
			throw new IOException("Node at index "+index+" null for attempted delete");
		if( !hTNode.getKeyValueArray(index).getKeyOptr().equals(Optr.emptyPointer))
			hMapMain.getIO().delete_object(hTNode.getKeyValueArray(index).getKeyOptr(), GlobalDBIO.getObjectAsBytes(hTNode.getKeyValueArray(index).getmKey()).length);
		if( hTNode.getKeyValueArray(index).getValueOptr() != null && !hTNode.getKeyValueArray(index).getValueOptr().equals(Optr.emptyPointer))
			hMapMain.getIO().delete_object(hTNode.getKeyValueArray(index).getValueOptr(), GlobalDBIO.getObjectAsBytes(hTNode.getKeyValueArray(index).getmValue()).length);
		// If its the rightmost key ignore move
		setUpdated(true);
	}
	
	/**
	 * Create a unique list of blocks that have already been populated with values from this node in order to possibly
	 * cluster new entries more efficiently.
	 * @return The list of unique blocks containing entries for this node.
	 */
	public ArrayList<Long> aggregatePayloadBlocks() {
		ArrayList<Long> blocks = new ArrayList<Long>();
		int i = 0;
		for(; i < getNumKeys(); i++) {
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
	

	/**
	 * Serialize this page to deep store on a page boundary.
	 * Key pages are always on page boundaries. The data is written
	 * to the page buffers from the updated node values via the {@link HMapKeyPage} facade.
	 * The push to deep store takes place at commit time or
	 * when the buffer fills and it becomes necessary to open a spot.
	 * @exception IOException If write fails
	 */
	public synchronized void putPage() throws IOException {
		if(hTNode == null) {
			throw new IOException(String.format("%s.putPage HTNode is null:%s%n",this.getClass().getName(),this));
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
		//.map(Map::values)                  // -> Stream<List<List<String>>>
		//.flatMap(Collection::stream)       // -> Stream<List<String>>
		//.flatMap(Collection::stream)       // -> Stream<String>
		//.collect(Collectors.toSet())       // -> Set<String>
		// Persist each key that is updated to fill the keyIds in the current page
		// Once this is complete we write the page contiguously
		// Write the object serialized keys out to deep store, we want to do this out of band of writing key page
		for(int i = 0; i < getNumKeys(); i++) {
			if( getKeyValueArray(i) != null ) {
				if(getKeyValueArray(i).keyState == KeyValue.synchStates.mustWrite || getKeyValueArray(i).keyState == KeyValue.synchStates.mustReplace) {
					// put the key to a block via serialization and assign KeyIdArray the position of stored key
					putKey(i, currentPayloadBlocks);
				}
				if(getKeyValueArray(i).valueState == KeyValue.synchStates.mustWrite ||getKeyValueArray(i).valueState == KeyValue.synchStates.mustReplace) {
					putData(i, currentPayloadBlocks);
				}
			}
		}
		//
		assert (lbai.getBlockNum() != -1L) : " KeyPageInterface unlinked from page pool:"+this;
		// write the page to the current block
		// Write to the block output stream
		DataOutputStream bs = GlobalDBIO.getBlockOutputStream(lbai);
		bs.writeLong(numKeys);
		for(int i = 0; i < numKeys; i++) {
			if(getKeyValueArray(i) != null && getKeyValueArray(i).keyState == KeyValue.synchStates.mustWrite || getKeyValueArray(i).keyState == KeyValue.synchStates.mustReplace ) { // if set, key was processed by putKey[i]
				bs.writeLong(getKeyValueArray(i).getKeyOptr().getBlock());
				bs.writeShort(getKeyValueArray(i).getKeyOptr().getOffset());
				getKeyValueArray(i).keyState = KeyValue.synchStates.upToDate;
				if( DEBUG ) 
					System.out.printf("%s.putPage %d Optr key:%s%n",this.getClass().getName(),i,getKeyValueArray(i));
			} else { // skip 
				lbai.setByteindex((short) (lbai.getByteindex()+10));
				if( DEBUG ) 
					System.out.printf("%s.putPage %d Optr key skipped:%s%n",this.getClass().getName(),i,getKeyValueArray(i));
			}
			// data array
			if(getKeyValueArray(i) != null && getKeyValueArray(i).valueState == KeyValue.synchStates.mustWrite ||getKeyValueArray(i).valueState == KeyValue.synchStates.mustReplace ) {
				bs.writeLong(getKeyValueArray(i).getValueOptr().getBlock());
				bs.writeShort(getKeyValueArray(i).getValueOptr().getOffset());
				getKeyValueArray(i).valueState = KeyValue.synchStates.upToDate;
				if( DEBUG ) 
					System.out.printf("%s.putPage %d Optr value:%s%n",this.getClass().getName(),i,getKeyValueArray(i));	
			} else {
				// skip the data Id for this index as it was not updated, so no need to write anything
				lbai.setByteindex((short) (lbai.getByteindex()+10));
				if( DEBUG ) 
					System.out.printf("%s.putPage %d value Optr skipped:%s%n",this.getClass().getName(),i,getKeyValueArray(i));
			}
		}
		// write the linked collision space page number
		if(nextPage == null)
			bs.writeLong(-1L);
		else
			bs.writeLong(nextPage.getPageId());
		bs.flush();
		bs.close();
		if( DEBUG ) {
			System.out.println("KeyPageInterface.putPage Added Keypage @"+GlobalDBIO.valueOf(getBlockAccessIndex().getBlockNum())+" block for keypage:"+this+" page:"+this);
		}
	}
	
	@Override
	/**
	 * Put the KeyPage to BlockAccessIndex to deep store.
	 * Put the updated keys to the buffer pool at available block space.
	 * The data is written to the BlockAccessIndex, the push to deep store takes place at commit time or
	 * when the buffer fills and it becomes necessary to open a spot.
	 * @param index
	 * @param currentPayloadBlocks 
	 * @return
	 * @throws IOException
	 */
	public synchronized boolean putKey(int index, ArrayList<Long> currentPayloadBlocks) throws IOException {
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
		getKeyValueArray(index).setKeyOptr(hMapMain.getIO().getIOManager().getNewInsertPosition(currentPayloadBlocks, pb.length));
		hMapMain.getIO().add_object(getKeyValueArray(index).getKeyOptr(), pb, pb.length);
		if(DEBUG || DEBUGPUTKEY)
			System.out.printf("%s.putKey ADDED Object for k/v:%s index:%d bytes:%d%n",this.getClass().getName(),getKeyValueArray(index),index,pb.length);
		setUpdated(true);
		return true;
	}
	
	@Override
	/**
	 * At KeyPageInterface putPage time, we resolve the lazy elements to actual VBlock,offset
	 * This method puts the values associated with a key/value pair, if using maps vs sets.
	 * Deletion of previous data has to occur before we get here, as we only have a payload to write, not an old one to remove.
	 * @param index Index of KeyPageInterface key and data value array
	 * @throws IOException
	 */
	public synchronized boolean putData(int index, ArrayList<Long> currentPayloadBlocks) throws IOException {

		if( getKeyValueArray(index).getmValue() == null ) {
			//|| bTreeKeyPage.getKeyValueArray()[index].getValueOptr().equals(Optr.emptyPointer)) {
			getKeyValueArray(index).setValueOptr(Optr.emptyPointer);
			if( DEBUGPUTDATA )
				System.out.printf("%s.putData ADDING NULL value for k/v:%s index:%d%n",this.getClass().getName(),getKeyValueArray(index),index);
			return false;
		}
		// pack the page into this tablespace and within blocks the same tablespace as key
		// the new insert position will attempt to find a block with space relative to established positions
		byte[] pb = GlobalDBIO.getObjectAsBytes(getKeyValueArray(index).getmValue());
		getKeyValueArray(index).setValueOptr(hMapMain.getIO().getIOManager().getNewInsertPosition(currentPayloadBlocks, pb.length));		
		if( DEBUGPUTDATA )
			System.out.printf("%s.putData ADDING NON NULL value for k/v:%s index:%d%n",this.getClass().getName(),getKeyValueArray(index),index);
		hMapMain.getIO().add_object(getKeyValueArray(index).getValueOptr(), pb, pb.length);
		setUpdated(true);
		return true;
	}

	/**
	 * Write the blocknum to the passed BlockAccessIndex at index position of long value.
	 * Only applicable to root page and children of root, not linked node pages of collision space
	 * since we directly set the byteindex.
	 * @param childPages.getBlockAccessIndex()
	 * @param index
	 * @param blocknum
	 * @throws IOException
	 */
	protected synchronized void writeIndex(RootKeyPageInterface childPages, int index, long blocknum) throws IOException {
		DataOutputStream bs = GlobalDBIO.getBlockOutputStream(childPages.getBlockAccessIndex(), (short)((index*8)+4));
		bs.writeLong(blocknum);
		childPages.getBlockAccessIndex().setUpdated();
		bs.flush();
		bs.close();
	}
	
	/**
	 * Set the nextPage field of passed page to new HTNode after creating a new page referencing that new HTNode.<p/>
	 * @param page The parent of the next page, may not be this page but one in the chain of this page
	 * @return the newly created HMapKeyPage
	 * @throws IOException
	 */
	protected synchronized HMapKeyPage createAndSetNextHTNode(RootKeyPageInterface page) throws IOException {
		((HMapKeyPage)page).nextPage = hMapMain.getIO().getHMapPageFromPool(-1L);
		HTNode newhTNode = new HTNode(((HMapKeyPage)page).nextPage);
		((HMapKeyPage)(((HMapKeyPage)page).nextPage)).hTNode = newhTNode;
		setUpdated(true);
		return (HMapKeyPage)((HMapKeyPage)page).nextPage;
	}
	
	@Override
	/**
	* Get the linked list of collision space pages and return the one at index.
	* In a tree, we would look for n'th child vertex with the proper implementation.
	*/
	public synchronized HMapKeyPage getPage(int index) throws IOException {
		if(nextPageId == -1L)
			return null;
		if(nextPage == null)
			nextPage = hMapMain.getIO().getHMapPageFromPool(nextPage.getPageId());
		HMapKeyPage pageList = (HMapKeyPage) nextPage;
		for(int i = 0 ; i < index; i++) {
			if(pageList.nextPageId == -1L) {
				return null;
			}
			if(pageList.nextPage == null)
				pageList.nextPage = hMapMain.getIO().getHMapPageFromPool(pageList.nextPageId);
			pageList = (HMapKeyPage) pageList.nextPage;
		}
		return pageList;
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
			System.out.printf("%s.getKey(%d) Entering KeyPageInterface to retrieve target index numKeys=%d mKey=%s Optr=%s updated=%b%n", this.getClass().getName(),
					index,
					hTNode.getNumKeys(),
					hTNode.getKeyValueArray(index).getmKey(),
					hTNode.getKeyValueArray(index).getKeyOptr(),
					hTNode.getKeyValueArray(index).keyState);
		}
		if(hTNode.getNumKeys() < index+1)
			return null;
		hTNode.initKeyValueArray(index);
		if(hTNode.getKeyValueArray(index).getmKey() == null && !hTNode.getKeyValueArray(index).getKeyOptr().isEmptyPointer() && hTNode.getKeyValueArray(index).keyState == KeyValue.synchStates.mustRead) {
			// eligible to retrieve page
			if( DEBUG ) {
				System.out.println("KeyPageInterface.getKey about to retrieve index:"+index+" loc:"+hTNode.getKeyValueArray(index).getKeyOptr());
			}
			hTNode.getKeyValueArray(index).setmKey((Comparable) hMapMain.getIO().deserializeObject(hTNode.getKeyValueArray(index).getKeyOptr()));
			if( DEBUG ) {
				System.out.println("KeyPageInterface.getKey retrieved index:"+index+" loc:"+hTNode.getKeyValueArray(index).getKeyOptr()+" retrieved:"+hTNode.getKeyValueArray(index).getmKey());
				for(int i = 0; i < getNumKeys(); i++)System.out.println(i+"="+hTNode.getKeyValueArray(index).getmKey());
			}
			hTNode.getKeyValueArray(index).keyState = KeyValue.synchStates.upToDate;
		}
		return hTNode.getKeyValueArray(index).getmKey();
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
			System.out.println("KeyPageInterface.getKey Entering KeyPageInterface to retrieve target index "+index);
		}
		if(hTNode.getNumKeys() < index+1)
			return null;
		hTNode.initKeyValueArray(index);
		if(hTNode.getKeyValueArray(index).getmValue() == null && !hTNode.getKeyValueArray(index).getValueOptr().isEmptyPointer() && hTNode.getKeyValueArray(index).valueState == KeyValue.synchStates.mustRead ){
			// eligible to retrieve page
			if( DEBUG ) {
				System.out.println("KeyPageInterface.getData about to retrieve index:"+index+" loc:"+hTNode.getKeyValueArray(index).getValueOptr());
			}
			hTNode.getKeyValueArray(index).setmValue((Comparable) hMapMain.getIO().deserializeObject(hTNode.getKeyValueArray(index).getValueOptr()));
			if( DEBUG ) {
				System.out.println("KeyPageInterface.getData retrieved index:"+index+" loc:"+hTNode.getKeyValueArray(index).getValueOptr()+" retrieved:"+hTNode.getKeyValueArray(index).getmValue());
				for(int i = 0; i < getNumKeys(); i++)System.out.println(i+"="+hTNode.getKeyValueArray(index).getmValue());
			}
			hTNode.getKeyValueArray(index).valueState = KeyValue.synchStates.upToDate;
		}
		return hTNode.getKeyValueArray(index).getmValue();
	}

	@SuppressWarnings("rawtypes")
	/**
	 * Using key, put keyArray[index] = key
	 * set updated true
	 * @param key
	 * @param index
	 */
	synchronized void putKeyToArray(Comparable key, int index) {
		hTNode.initKeyValueArray(index);
		hTNode.getKeyValueArray(index).setmKey(key);
		hTNode.getKeyValueArray(index).setKeyOptr(Optr.emptyPointer);
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
	synchronized void copyKeyToArray(HMapKeyPage sourceKey, int sourceIndex, int targetIndex) throws IOException {
		hTNode.initKeyValueArray(targetIndex);
		hTNode.getKeyValueArray(targetIndex).setmKey(sourceKey.getKey(sourceIndex)); // get the key from pointer from source if not already
		hTNode.getKeyValueArray(targetIndex).setKeyOptr(sourceKey.getKeyId(sourceIndex));
		setUpdated(true);
	}
	
	synchronized void copyDataToArray(HMapKeyPage sourceKey, int sourceIndex, int targetIndex) throws IOException {
		hTNode.initKeyValueArray(targetIndex);
		hTNode.getKeyValueArray(targetIndex).setmValue(sourceKey.getData(sourceIndex)); // get the key from pointer from source if not already
		hTNode.getKeyValueArray(targetIndex).setValueOptr(sourceKey.getDataId(sourceIndex));
		setUpdated(true);
	}
	
	synchronized void copyKeyAndDataToArray(HMapKeyPage sourceKey, int sourceIndex, int targetIndex) throws IOException {
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
		hTNode.initKeyValueArray(index);
		hTNode.getKeyValueArray(index).setmValue(data);
		hTNode.getKeyValueArray(index).setValueOptr(Optr.emptyPointer);
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
		sb.append("Node:");
		sb.append(hTNode);
		sb.append("\r\n");
		return sb.toString();
	}

	public synchronized boolean isUpdated() {
		return ((HTNode)hTNode).isUpdated();
	}

	public synchronized void setUpdated(boolean updated) {
		((HTNode)hTNode).setUpdated(updated);
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
	public synchronized void setKey(int i, Comparable key) {
		putKeyToArray(key, i);
	}


	public synchronized Datablock getDatablock() {
		return lbai.getBlk();
	}

	public synchronized BlockAccessIndex getBlockAccessIndex() {
		return lbai;
	}


	/**
	 * @return the numKeys. HTNode contains its own numKeys, the imbalance between that one and the one for this page indicates some degree of modification. 
	 */
	public synchronized int getNumKeys() {
		return (int) numKeys;
	}

	/**
	 * @param numKeys the numKeys to set
	 */
	public synchronized void setNumKeys(int numKeys) {
		if(DEBUGSETNUMKEYS)
			System.out.printf("Setting num keys=%d previous numKeys=%d page:%s%n", numKeys, numKeys, GlobalDBIO.valueOf(lbai.getBlockNum()));
		setUpdated(true);
		this.numKeys = numKeys;
	}
	
	/**
	 * Return the structure with all keys, values, and {@link Optr} database object instance pointers.
	 * @return the key/value array {@link KeyValue} from {@link HTNode}
	 */
	public KeyValue<Comparable,Object> getKeyValueArray(int index) {
		return hTNode.getKeyValueArray(index);
	}
	
	@Override
	public void retrieveEntriesInOrder(KVIteratorIF<Comparable, Object> iterImpl) {
		HMapKeyPage nPage = this;//((HMapKeyPage)nextPage);
		while(nPage != null ) {
			for(int i = 0; i < nPage.hTNode.getNumKeys(); i++) {
				KeyValue<Comparable,Object> kv = nPage.hTNode.getKeyValueArray(i);
				try {
					if(iterImpl.item(kv.getmKey(), kv.getmValue()))
						return;
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
			nPage = (HMapKeyPage) nPage.nextPage;
		}	
	}

	public int retrieveEntriesInOrder(Builder<KeyValue<Comparable, Object>> b, int count, int limit) {
		HMapKeyPage nPage = this;//((HMapKeyPage)nextPage);
		while(nPage != null ) {
			for(int i = 0; i < nPage.hTNode.getNumKeys(); i++) {
				KeyValue<Comparable,Object> kv = nPage.hTNode.getKeyValueArray(i);
				try {
					kv.getmKey();
					kv.getmValue();
					b.accept(kv);
					++count;
					if(limit != -1 && count >= limit)
						return count;
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
			nPage = (HMapKeyPage) nPage.nextPage;
		}
		return count;
	}

	@Override
	public int retrieveEntriesInOrder(Supplier<KeyValue<Comparable, Object>> b, int count, int limit) {
		// TODO Auto-generated method stub
		return 0;
	}

}
