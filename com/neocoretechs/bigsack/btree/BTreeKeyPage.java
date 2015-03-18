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
* is the concept of element 0 of those arrays being 'this', hence the special treatment in CRUD 
* @author Groff
*/
public final class BTreeKeyPage implements Serializable {
	static final boolean DEBUG = false;
	static final long serialVersionUID = -2441425588886011772L;
	static int MAXKEYS = 4;
	int numKeys = 0;

	transient long pageId = -1L;
	@SuppressWarnings("rawtypes")
	Comparable[] keyArray;
	transient BTreeKeyPage[] pageArray;
	long[] pageIdArray;
	transient Object[] dataArray;
	Optr[] dataIdArray;
	transient boolean[] dataUpdatedArray;

	private transient boolean updated = false;

	BTreeKeyPage() {
		// Pre-allocate the arrays
		keyArray = new Comparable[MAXKEYS];
		pageArray = new BTreeKeyPage[MAXKEYS + 1];
		pageIdArray = new long[MAXKEYS + 1];
		dataArray = new Object[MAXKEYS];
		dataIdArray = new Optr[MAXKEYS];
		dataUpdatedArray = new boolean[MAXKEYS];

		for (int i = 0; i <= MAXKEYS; i++) {
			pageIdArray[i] = -1L;
			if( i != MAXKEYS ) {
				dataIdArray[i] = Optr.emptyPointer;
			}
		}
	}

	public BTreeKeyPage(long ppos) {
		this();
		pageId = ppos;
	}
	/**
	* Given a Comparable object, search for that object on this page.
	* If loc >= 0 then the key was found on this page and loc is index of
	* located key. If loc < 0 then the key
	* was not found on this page and abs(loc)-1 is index of where the
	* key *should* be.
	* @param targetKey The target key to retrieve
	* @return loc.
	*/
	@SuppressWarnings({ "rawtypes", "unchecked" })
	synchronized int search(Comparable targetKey) {
		int lo = 0;
		int hi = this.numKeys - 1;
		int mid = 0;
		//if( targetKey == null ) if( Props.DEBUG ) System.out.println("search:Target is null");
		//if( keyArray == null ) if( Props.DEBUG ) System.out.println("search:Keyarray is null");
		//if( keyArray[lo] == null ) if( Props.DEBUG ) System.out.println("search:Key array lo is null");
		if (keyArray[lo].compareTo(targetKey) > 0) {
			return (-1);
		}
		do {
			mid = (lo + hi) / 2;
			if (keyArray[mid].compareTo(targetKey) == 0) {
				return (mid);
			}
			if (keyArray[mid].compareTo(targetKey) > 0)
				hi = mid - 1;
			else
				lo = mid + 1;
		} while (lo <= hi);
		return (-lo - 1);
	}

	/**
	* Insert the Comparable object and data
	* object at index. Everything on page is slid to the right to make space.
	* numKeys is incremented
	* @param newKey The Comparable key to insert
	* @param newData The new data payload to insert
	* @param index The index of key array to begin offset
	*/
	@SuppressWarnings("rawtypes")
	synchronized void insert(Comparable newKey, Object newData, int index) {
		// If adding to right, no moving to do
		if (index < numKeys)
			// move elements down
			for (int i = (numKeys == MAXKEYS ? MAXKEYS - 2 : numKeys - 1); i >= index; i--) {
				keyArray[i + 1] = keyArray[i];
				pageArray[i + 2] = pageArray[i + 1];
				pageIdArray[i + 2] = pageIdArray[i + 1];
				dataArray[i + 1] = dataArray[i];
				dataIdArray[i + 1] = dataIdArray[i];
				dataUpdatedArray[i + 1] = dataUpdatedArray[i];
			}
		// If we're going to overflow, decrement target, otherwise 
		// we insert key here and increment number
		if (index == MAXKEYS)
			--index;
		else
			++numKeys;

		keyArray[index] = newKey;
		dataArray[index] = newData;
		dataIdArray[index] = Optr.emptyPointer;
		dataUpdatedArray[index] = true;
		setUpdated(true);
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
			System.out.println("Entering BTreeKeyPage with target index "+index);
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
			pageArray[index].pageId = pageIdArray[index];
			if( DEBUG ) {
				System.out.println("BTreeKeyPage.getPage index:"+index+" loc:"+GlobalDBIO.valueOf(pageIdArray[index])+" target page ID:"+GlobalDBIO.valueOf(pageArray[index].pageId));
				for(int i = 0; i < pageIdArray.length; i++)System.out.println(i+"="+GlobalDBIO.valueOf(pageIdArray[i]));
			}
			pageArray[index].pageArray = new BTreeKeyPage[MAXKEYS + 1];
			pageArray[index].dataArray = new Object[MAXKEYS];
			pageArray[index].dataUpdatedArray = new boolean[MAXKEYS];
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
		if( DEBUG ) System.out.println("BTreeKeyPage.getPageFromPool "+pos);//+" "+btk);
		// initialize transients
		btk.pageId = pos;
		btk.pageArray = new BTreeKeyPage[MAXKEYS + 1];
		btk.dataArray = new Object[MAXKEYS];
		btk.dataUpdatedArray = new boolean[MAXKEYS];
		//for(int i = 0; i <= MAXKEYS; i++) {
		//	btk.pageArray[i] = btk.getPage(sdbio,i);
		//}
		return btk;
	}
	/**
	 * Serialize this page to deep store on a page boundary.
	 * @param sdbio The ObjectDBIO instance
	 * @exception IOException If write fails
	 */
	public synchronized void putPage(ObjectDBIO sdbio) throws IOException {
		if (!isUpdated()) {
			if( DEBUG ) 
				System.out.println("page not updated, returning from putPage");
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
		if( DEBUG ) 
			System.out.println("BTreeKeyPage putPage Added object @"+GlobalDBIO.valueOf(pageId)+" bytes:"+pb.length+" page:"+this);
		setUpdated(false);
	}
	/**
	* Recursively put the pages to deep store.  If data items are updated
	* persist them as well.
	* For data, we reset the new node position. For pages, we don't use
	* it because they are always on page boundaries (not packed)
	* @param sdbio The BlockDBIO instance
	* @exception IOException if write fails 
	*/
	public synchronized void putPages(ObjectDBIO sdbio) throws IOException {
		for (int i = 0; i <= numKeys; i++) {
			if (pageArray[i] != null) {
				pageArray[i].putPages(sdbio);
				pageIdArray[i] = pageArray[i].pageId;
			}
			// put the data item
			if (i < numKeys && dataUpdatedArray[i]) {
				dataUpdatedArray[i] = false;
				// if it gets nulled, should probably delete
				if (dataArray[i] != null) {
					// pack the page into this tablespace and within blocks at the last known good position
					dataIdArray[i] = sdbio.getIOManager().getNewNodePosition(GlobalDBIO.getTablespace(pageIdArray[i]));
					byte[] pb = GlobalDBIO.getObjectAsBytes(dataArray[i]);
					sdbio.add_object(dataIdArray[i], pb, pb.length);
					// set new node position to the current block to pack pages
					//sdbio.setNewNodePosition();
				}
			}
		}
		putPage(sdbio);
	}

	synchronized void putPageToArray(BTreeKeyPage fromPage, int index) {
		pageArray[index] = fromPage;
		if (fromPage != null)
			pageIdArray[index] = fromPage.pageId;
		else
			pageIdArray[index] = -1L;
		setUpdated(true);
	}

	@SuppressWarnings("rawtypes")
	synchronized void putKeyToArray(Comparable key, int index) {
		keyArray[index] = key;
		setUpdated(true);
	}

	synchronized Object getDataFromArray(ObjectDBIO sdbio, int index) throws IOException {
		if (dataArray[index] == null && !dataIdArray[index].isEmptyPointer() ) {
			dataArray[index] = sdbio.deserializeObject(dataIdArray[index]);
			dataUpdatedArray[index] = false;
		}
		return dataArray[index];
	}

	synchronized void putDataToArray(Object data, int index) {
		dataArray[index] = data;
		dataIdArray[index] = Optr.emptyPointer;
		dataUpdatedArray[index] = true;
		setUpdated(true);
	}

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
}
