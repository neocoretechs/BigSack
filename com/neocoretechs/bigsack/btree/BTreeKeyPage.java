package com.neocoretechs.bigsack.btree;
import java.io.IOException;
import java.io.Serializable;

import com.neocoretechs.bigsack.io.Optr;
import com.neocoretechs.bigsack.io.pooled.BlockAccessIndex;
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
* persistent object locations/data.  Persists itself to the buffer pool as
* serialized instance when necessary.
* MAXKEYS is an attempt to keep keys from spanning page boundaries at the expense of some storage
* a key overflow will cause a page split, at times unavoidable.
* Important to note that the data is stored as arrays serialized out in this class. Related to that
* is the concept of element 0 of those arrays being 'this', hence the special treatment in CRUD.
* Unlike a binary search tree, each node of a B-tree may have a variable number of keys and children.
* The keys are stored in non-decreasing order. Each node either is a leaf node or
* it has some associated children that are the root nodes of subtrees.
* The left child node of a node's element contains all nodes (elements) with keys less than or equal to the node element's key
* but greater than the preceding node element's key.
* If a node becomes full, a split operation is performed during the insert operation.
 * The split operation transforms a full node with 2*T-1 elements into two nodes with T-1 elements each
 * and moves the median key of the two nodes into its parent node.
 * The elements left of the median (middle) element of the splitted node remain in the original node.
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
* @author Groff Copyright (C) NeoCoreTechs 2014,2015
*/
public final class BTreeKeyPage implements Serializable {
	static final boolean DEBUG = false;
	static final long serialVersionUID = -2441425588886011772L;
	// number of keys per page; number of instances of the non transient fields of 'this' per DB block
	// Can be overridden by properties element. the number of maximum children is MAXKEYS+1 per node
	public static int MAXKEYS = 5; 
	// Non transient number of keys on this page. Adjusted as necessary when inserting/deleting.
	int numKeys = 0;
	// Transient. The 'id' is really the location this page was retrieved from deep store.
	transient long pageId = -1L;
	@SuppressWarnings("rawtypes")
	// The array of keys, non transient.
	public Comparable[] keyArray;
	// The array of pages corresponding to the pageIds for the child nodes. Transient since we lazily retrieve pages via pageIds
	transient BTreeKeyPage[] pageArray;
	// The array of page ids from which the page array is filled. This data is persisted as virtual page pointers. Since
	// we align indexes on page boundaries we dont need an offset as we do with value data associated with the indexes for maps.
	long[] pageIdArray;
	// These are the data items for values associated with keys, should this be a map vs set.
	// These are lazily populated from the dataIdArray where an id exsists at that index.
	transient Object[] dataArray;
	// This array is present for maps where values are associated with keys. In sets it is absent or empty.
	// It contains the page and offset of the data item associated with a key. We pack value data on pages, hence
	// we need an additional 2 byte offset value to indicate that.
	Optr[] dataIdArray;
	// This transient array maintains boolean values indicating whether the data item at that index has been updated
	// and needs written back to deep store.
	transient boolean[] dataUpdatedArray;
	// Global is this leaf node flag.
	public boolean mIsLeafNode = true; // We treat as leaf since the logic is geared to proving it not
	// Global page updated flag.
	private transient boolean updated = false; // has the node been updated for purposes of write
	/**
	 * No - arg cons to initialize pageArray to MAXKEYS + 1, this is called on deserialization
	 */
	public BTreeKeyPage() {
		if( DEBUG ) {
			System.out.println("BTreeKeyPage DEFAULT ctor");
		}
		initTransients();
	}
	public void initTransients() {
		pageArray = new BTreeKeyPage[MAXKEYS + 1];
		dataArray = new Object[MAXKEYS];
		dataUpdatedArray = new boolean[MAXKEYS];
	}
	/**
	 * Construct the page from scratch in a non deserialization context.
	 * We provide the intended virtual page number, all fields are initialized and
	 * we can either retrieve or store it from there.
	 * @param ppos The virtual page id
	 */
	public BTreeKeyPage(long ppos) {
		if( DEBUG ) {
			System.out.println("BTreeKeyPage ctor loc:"+GlobalDBIO.valueOf(ppos));
		}
		initTransients();
		// Pre-allocate the arrays that hold persistent data
		pageIdArray = new long[MAXKEYS + 1];
		dataIdArray = new Optr[MAXKEYS];
		for (int i = 0; i <= MAXKEYS; i++) {
			pageIdArray[i] = -1L;
			if( i != MAXKEYS ) {
				dataIdArray[i] = Optr.emptyPointer;
			}
		}
		keyArray = new Comparable[MAXKEYS];
		pageId = ppos; 
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
	*/
	@SuppressWarnings({ "rawtypes", "unchecked" })
	synchronized TreeSearchResult search(Comparable targetKey) {
		assert(keyArray.length > 0) : "BTreeKeyPage.search key array length zero";
		int middleIndex = 1; 
        int leftIndex = 0;
        int rightIndex = numKeys - 1;
        // no keys, call for insert at 0
        if( rightIndex == -1)
        	return new TreeSearchResult(0, false);
        while (leftIndex <= rightIndex) {
        	middleIndex = leftIndex + ((rightIndex - leftIndex) / 2);
        	int cmpRes = keyArray[middleIndex].compareTo(targetKey);
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
	* Delete the key/data item on this page.
	* Everything on the page is slid left.
	* numKeys is decremented
	* @param index the index of the item on this page to delete
	*/
	synchronized void delete(int index) {
		// If its the rightmost key ignore move
		if (index < numKeys - 1)
			// Move all up
			for (int i = index;i < (numKeys == MAXKEYS ? MAXKEYS - 1 : numKeys); i++) {
				keyArray[i] = keyArray[i + 1];
				pageArray[i + 1] = pageArray[i + 2];
				pageIdArray[i + 1] = pageIdArray[i + 2];
				dataArray[i] = dataArray[i + 1];
				dataIdArray[i] = dataIdArray[i + 1];
				dataUpdatedArray[i] = dataUpdatedArray[i + 1];
			}

		// Decrement key count and nullify rightmost item on the node
		--numKeys;
		keyArray[numKeys] = null;
		pageArray[numKeys + 1] = null;
		pageIdArray[numKeys + 1] = -1L;
		dataArray[numKeys] = null;
		dataIdArray[numKeys] = Optr.emptyPointer;
		dataUpdatedArray[numKeys] = false; // we took care of it
		setUpdated(true);
	}

	synchronized void deleteData(ObjectDBIO sdbio, int index) throws IOException {
		if (dataArray[index] != null && !dataIdArray[index].isEmptyPointer()) {
			if( DEBUG ) {
				System.out.print("Deleting :"+dataIdArray[index]+"\r\n");
				System.out.println("Data: "+dataArray[index]+"\r\n");
			}
			//if( Props.DEBUG ) System.out.println(" size "+ilen);
			sdbio.delete_object(dataIdArray[index],  GlobalDBIO.getObjectAsBytes(dataArray[index]).length );
			dataIdArray[index] = Optr.emptyPointer;
			dataUpdatedArray[index] = true;
			setUpdated(true);
		} //else {
			//throw new IOException("Attempt to delete null data index "+index+" for "+this);
		//}
	}
	/**
	* Retrieve a page based on an index to this page containing a page.
	* In effect, this is our lazy initialization of the 'pageArray' and we strictly
	* work in pageArray in this method. If the pageIdArray contains a valid non -1 entry, then
	* we retrieve and deserialize that virtual block to an entry in the pageArray at the index passed in the params
	* location. If we retrieve an instance we also fill in the transient fields from our current data
	* @param sdbio The session database io instance
	* @param index The index to the page array on this page that contains the virtual record to deserialize.
	* @return The deserialized page instance
	* @exception IOException If retrieval fails
	*/
	public synchronized BTreeKeyPage getPage(ObjectDBIO sdbio, int index) throws IOException {
		if(DEBUG) {
			System.out.println("BTreeKeyPage.getPage Entering BTreeKeyPage to retrieve target index "+index);
			for(int i = 0; i < pageIdArray.length; i++) {
				System.out.println(i+"="+GlobalDBIO.valueOf(pageIdArray[i]));
				System.out.println(pageArray[i]);
			}
		}
		if (pageArray[index] == null && pageIdArray[index] != -1L) {
			// eligible to retrieve page
			if( DEBUG ) {
				System.out.println("BTreeKeyPage.getPage index:"+index+" loc:"+GlobalDBIO.valueOf(pageIdArray[index]));
			}
			pageArray[index] =
				(BTreeKeyPage) (sdbio.deserializeObject(pageIdArray[index]));
			// set up all the transient fields
			pageArray[index].initTransients();
			pageArray[index].pageId = pageIdArray[index];
			if( DEBUG ) {
				System.out.println("BTreeKeyPage.getPage index:"+index+" loc:"+GlobalDBIO.valueOf(pageIdArray[index])+" target page ID:"+GlobalDBIO.valueOf(pageArray[index].pageId));
				for(int i = 0; i < pageIdArray.length; i++)System.out.println(i+"="+GlobalDBIO.valueOf(pageIdArray[i]));
			}
		}
		return pageArray[index];
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
		sdbio.findOrAddBlock(pos);
		BTreeKeyPage btk =
			(BTreeKeyPage) (sdbio.deserializeObject(pos));
		// initialize transients
		btk.initTransients();
		btk.pageId = pos;
		if( DEBUG ) System.out.println("BTreeKeyPage.getPageFromPool "+pos);//+" "+btk);
		//for(int i = 0; i <= MAXKEYS; i++) {
		//	btk.pageArray[i] = btk.getPage(sdbio,i);
		//}
		return btk;
	}
	/**
	 * Get a new page from the pool from a random tablespace. used for general inserts.
	 * Call stealblk, create BTreeKeyPage with the page Id of stolen block, set the pageArray
	 * to MAXKEYS+1, the dataArray to MAXKEYS, and the dataUpdatedArray to MAXKEYS
	 * set updated to true, and return the newly formed  
	 * @param sdbio
	 * @return
	 * @throws IOException
	 */
	static BTreeKeyPage getPageFromPool(ObjectDBIO sdbio) throws IOException {
		BlockAccessIndex lbai = sdbio.stealblk();
		long pageId = lbai.getBlockNum();
		// extract tablespace since we steal blocks from any
		//int tablespace = GlobalDBIO.getTablespace(pageId);
		// initialize transients
		BTreeKeyPage btk = new BTreeKeyPage(pageId);
		btk.setUpdated(true);
		//for(int i = 0; i <= MAXKEYS; i++) {
		//	btk.pageArray[i] = btk.getPage(sdbio,i);
		//}
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
	public synchronized void putPage(ObjectDBIO sdbio) throws IOException {
		if (!isUpdated()) {
			if( DEBUG ) 
				System.out.println("BTreeKeyPage.putPage page not updated:"+this);
			return;
		}
		byte[] pb = GlobalDBIO.getObjectAsBytes(this);
		if( DEBUG ) 
			System.out.println("BTreeKeyPage putPage id:"+GlobalDBIO.valueOf(pageId)+" Got "+pb.length+" bytes");
		if (pageId == -1L) {
			BlockAccessIndex lbai = sdbio.stealblk();
			pageId = lbai.getBlockNum();
			// extract tablespace since we steal blocks from any
			int tablespace = GlobalDBIO.getTablespace(pageId);
			if( DEBUG )
				System.out.println("BTreeKeyPage putPage Stole block "+GlobalDBIO.valueOf(pageId));
			sdbio.add_object(tablespace, lbai, pb, pb.length);
		} else {
			sdbio.add_object(Optr.valueOf(pageId), pb, pb.length);
		}
		// Persist the data items associated with the keys
		for (int i = 0; i < numKeys; i++) {
			// put the data item
			if (dataUpdatedArray[i]) {
				dataUpdatedArray[i] = false;
				// if it gets nulled, should probably delete
				if (dataArray[i] != null) {
					// pack the page into this tablespace and within blocks at the last known good position
					dataIdArray[i] = sdbio.getIOManager().getNewNodePosition(GlobalDBIO.getTablespace(pageIdArray[i]));
					pb = GlobalDBIO.getObjectAsBytes(dataArray[i]);
					//System.out.println("ADDING DATA TO INSTANCE:"+dataArray[i]);
					sdbio.add_object(dataIdArray[i], pb, pb.length);
					// set new node position to the current block to pack pages
					//sdbio.setNewNodePosition();
				}
			}
		}
		if( DEBUG ) 
			System.out.println("BTreeKeyPage putPage Added object @"+GlobalDBIO.valueOf(pageId)+" bytes:"+pb.length+" page:"+this);
		setUpdated(false);
	}
	/**
	* Recursively put the pages to deep store.  
	* @param sdbio The BlockDBIO instance
	* @exception IOException if write fails 
	*/
	public synchronized void putPages(ObjectDBIO sdbio) throws IOException {
		for (int i = 0; i <= numKeys; i++) {
			if (pageArray[i] != null) {
				pageArray[i].putPages(sdbio);
				pageIdArray[i] = pageArray[i].pageId;
			}
		}
		putPage(sdbio);
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

	@SuppressWarnings("rawtypes")
	/**
	 * Using key, put keyArray[index] = key
	 * set updated true
	 * @param key
	 * @param index
	 */
	synchronized void putKeyToArray(Comparable key, int index) {
		keyArray[index] = key;
		setUpdated(true);
	}

	/**
	 * Lazy initialization for off-index-page 'value' objects attached to our BTree keys.
	 * Essentially guarantees that if a virtual pointer Id is present in dataIdArray at index, 
	 * you get a valid instance back.
	 * If dataArray[index] is null and dataIdArray[index] is NOT an empty pointer,
	 * set the dataArray[index] to the deserialized object retrieved from deep store via dataIdArray[index].
	 * Set dataUpdatedArray[index] to false since we just retrieved an instance.
	 * return the dataArray[index].
	 * @param sdbio The IO manager
	 * @param index The index to populate if its possible to do so
	 * @return dataArray[index] filled with deep store object
	 * @throws IOException
	 */
	public synchronized Object getDataFromArray(ObjectDBIO sdbio, int index) throws IOException {
		if (dataArray[index] == null && dataIdArray[index] != null && !dataIdArray[index].isEmptyPointer() ) {
			dataArray[index] = sdbio.deserializeObject(dataIdArray[index]);
			dataUpdatedArray[index] = false;
		}
		return dataArray[index];
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
	
	public synchronized String toString() {
		StringBuffer sb = new StringBuffer();
		//sb.append("Page ");
		//sb.append(hashCode());
		sb.append("<<<<<<<<<<BTreeKeyPage Id:");
		sb.append(GlobalDBIO.valueOf(pageId));
		sb.append(" Numkeys:");
		sb.append(String.valueOf(numKeys));
		sb.append(" Leaf:");
		sb.append(mIsLeafNode);
		sb.append(" Updated:");
		sb.append(String.valueOf(updated)+"\r\n");
		
		sb.append("Key Array:\r\n");
		if( keyArray == null ) {
			sb.append("Key ARRAY IS NULL\r\n");
		} else {
			for (int i = 0; i < keyArray.length; i++) {
				sb.append(i+" =");
				sb.append(keyArray[i]+"\r\n");
			}
		}
		
		sb.append("BTree Page Array:\r\n");
		if( pageArray == null ) {
			sb.append("PAGE ARRAY IS NULL\r\n");
		} else {
			int j = 0;
			for (int i = 0 ; i < pageArray.length; i++) {
			//	sb.append(i+"=");
			//	sb.append(pageArray[i]+"\r\n");
				if(pageArray[i] != null) ++j;
			}
			sb.append("Page Array Non null for "+j+" members");
		}
		sb.append("BTree Page IDs:\r\n");
		if( pageIdArray == null ) {
			sb.append(" PAGE ID ARRAY IS NULL\r\n ");
		} else {
			sb.append(" ");
			for (int i = 0; i < pageIdArray.length; i++) {
				sb.append(i+" id=");
				//sb.append(pageArray[i] == null ? null : String.valueOf(pageArray[i].hashCode()));
				sb.append(GlobalDBIO.valueOf(pageIdArray[i]));
				sb.append("\r\n");
			}
		}
		/*
		sb.append("Data Array:\r\n");
		if(dataArray==null) {
			sb.append(" DATA ARRAY NULL\r\n");
		} else {
			for(int i = 0; i < dataArray.length; i++) {
				sb.append(i+"=");
				sb.append(dataArray[i]+"\r\n");
				sb.append("updated=");
				sb.append(dataUpdatedArray[i]);
				sb.append("\r\n");
			}
		}
		sb.append("Data IDs:\r\n");
		if(dataIdArray==null) {
			sb.append(" DATA ID ARRAY NULL\r\n");
		} else {
			for(int i = 0; i < dataIdArray.length; i++) {
				sb.append(i+" id=");
				sb.append(dataIdArray[i]);
				sb.append("\r\n");
			}
		}
		*/
		sb.append(GlobalDBIO.valueOf(pageId));
		sb.append(" >>>>>>>>>>>>>>End ");
		sb.append("\r\n");
		return sb.toString();
	}

	public boolean isUpdated() {
		return updated;
	}

	public void setUpdated(boolean updated) {
		this.updated = updated;
	}
	

	/**
	 * Determine if this key is in the list of array elements for this page
	 * @param key
	 * @return
	 */
    boolean contains(int key) {
        return search(key).atKey;
    }               

    /**
     * Find the place in the key array where the target key is less than the key value in the array element.
     * @param key The target key
     * @return The index where target is < array index value, if end, return numKeys
     */
    int subtreeRootNodeIndex(Comparable key) {
        for (int i = 0; i < numKeys; i++) {                            
                if (key.compareTo(keyArray[i]) < 0) {
                        return i;
                }                               
        }
        return numKeys;
    }
}
