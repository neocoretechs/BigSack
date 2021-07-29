package com.neocoretechs.bigsack.keyvaluepages;

import java.io.DataInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.function.Supplier;
import java.util.stream.Stream.Builder;

import com.neocoretechs.bigsack.btree.BTNode;
import com.neocoretechs.bigsack.io.Optr;
import com.neocoretechs.bigsack.io.pooled.BlockAccessIndex;
import com.neocoretechs.bigsack.io.pooled.Datablock;
/**
 * Contract between the KeyPage that supports the Key/Value store implementation and the
 * {@link BlockAccessIndex} that provides access to the deep store and recovery logs.
 * Differentiate between {@link Optr} composed of long virtual block and short offset within that block
 * and the long Vblock where offset is assumed to be 0. the Vblock is a virtual block where the first
 * 3 bits are the tablespace and the remaining 29 bits are the block (35.184T).
 * @author Jonathan Groff Copyright (C) NeoCoreTechs 2021
 *
 */
public interface KeyPageInterface extends RootKeyPageInterface {

	/**
	 * Return the structure with all keys, values, and {@link Optr} database object instance pointers.
	 * @return the key/value array {@link KeyValue} from {@link BTNode}
	 */
	KeyValue<Comparable, Object> getKeyValueArray(int index);
	
	KeyValueMainInterface getKeyValueMain();
	
	/**
	 * Set the key Id array, and set the keyUpdatedArray for the key and the general updated flag
	 * @param index
	 * @param optr
	 */
	void setKeyIdArray(int index, Optr optr, boolean update);
	/**
	 * Get the pointer to the key/value record based on the index to the collection of record pointers housed on this page.
	 * @param index the index to the pointer on this page
	 * @return
	 */
	Optr getKeyId(int index);

	/**
	 * Set the Data Id array, and set the dataUpdatedArray for the key and the general updated flag
	 * @param index
	 * @param optr The Vblock and offset within that block of the first data item for the key/value value associated data if any
	 */
	void setDataIdArray(int index, Optr optr, boolean update);

	Optr getDataId(int index);
	
	/**
	 * Put the KeyPage to BlockAccessIndex to deep store.
	 * Put the updated keys to the buffer pool at available block space.
	 * The data is written to the BlockAccessIndex, the push to deep store takes place at commit time or
	 * when the buffer fills and it becomes necessary to open a spot.
	 * @param index
	 * @param keys The list of unique blocks that already contain entries for more efficient clustering. We will try to place new entry in one.
	 * @return
	 * @throws IOException
	 */
	boolean putKey(int index, ArrayList<Long> keys) throws IOException;
	
	/**
	 * At {@link KeyPageInterface} putPage time, we resolve the lazy elements to actual VBlock,offset
	 * This method puts the values associated with a key/value pair, if using maps vs sets.
	 * Deletion of previous data has to occur before we get here, as we only have a payload to write, not an old one to remove.
	 * @param index Index of {@link KeyPageInterface} key and data value array
	 * @param values The list of unique blocks that already contain entries for more efficient clustering. We will try to place new entry in one.
	 * @throws IOException
	 */
	boolean putData(int index, ArrayList<Long> values) throws IOException;

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
	Comparable getKey(int index) throws IOException;

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
	Object getData(int index) throws IOException;
	
	/**
	 * Set the value of the index at i to the key.
	 * @param i the child index into this node.
	 * @param key The key to set index i to.
	 */
	void setKey(int i, Comparable key);
	
	/**
	 * Set the node associated with this page. This will called back from {@link BlockAccessIndex}
	 * static method getPageFromPool to set up a new node.
	 * This action come via BTree create to set the node.
	 * @param btnode
	 */
	void setNode(NodeInterface btnode);

	KeyValue<Comparable, Object> readBlockAndGetKV(DataInputStream dis, NodeInterface node) throws IOException;
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

	void retrieveEntriesInOrder(KVIteratorIF<Comparable, Object> iterImpl);

	int retrieveEntriesInOrder(Supplier<KeyValue<Comparable, Object>> b, int count, int limit);


}