package com.neocoretechs.bigsack.keyvaluepages;

import java.io.DataInputStream;
import java.io.IOException;
import java.util.ArrayList;

import com.neocoretechs.bigsack.io.Optr;
import com.neocoretechs.bigsack.io.pooled.BlockAccessIndex;
import com.neocoretechs.bigsack.io.pooled.Datablock;

public interface RootKeyPageInterface {
	long getPageId();

	/**
	 * Read the page using the given DataInputStream
	 * @throws IOException
	 */
	void readFromDBStream(DataInputStream dis) throws IOException;

	/**
	 * Set the Key page nodes Id array, and set the pageUpdatedArray for the key and the general updated flag
	 * @param index The index to the collection of child pages, aligned on page boundary (hence long and not {@link Optr})
	 * @param block The long VBlock of the key page, we dont need Optr offset because offset of key pages is always 0
	 * @throws IOException 
	 */
	void setPageIdArray(int index, long block, boolean update) throws IOException;

	long getPageId(int index);
	
	/**
	 * Flush the buffers to the page if in update mode, prepare to write page to deep store
	 * @throws IOException
	 */
	void putPage() throws IOException;
	
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
	RootKeyPageInterface getPage(int index) throws IOException;

	boolean isUpdated();

	void setUpdated(boolean updated);

	Datablock getDatablock();

	BlockAccessIndex getBlockAccessIndex();

	/**
	 * @return the numKeys
	 */
	int getNumKeys();

	/**
	 * @param numKeys the numKeys to set
	 * @throws IOException 
	 */
	void setNumKeys(int numKeys) throws IOException;

	/**
	 * Calls {@link KeyValueMain}.createRootNode and sets{@link NodeInterface} here to returned value.
	 * @param bai 
	 * @throws IOException
	 */
	void setRootNode(BlockAccessIndex bai) throws IOException;
	
}
