package com.neocoretechs.bigsack.session;

import java.io.IOException;
import java.util.Iterator;
import java.util.Stack;
import java.util.stream.Stream;

import com.neocoretechs.bigsack.keyvaluepages.KeySearchResult;
import com.neocoretechs.bigsack.keyvaluepages.KeyValueMainInterface;
import com.neocoretechs.bigsack.keyvaluepages.TraversalStackElement;

interface MapInterface extends SetInterface {


	boolean put(Comparable key, Object o) throws IOException;

	/**
	 * Retrieve an object with this value for first key found to have it.
	 * @param o the object value to seek
	 * @return the object, null if not found
	 * @throws IOException
	 */
	Object getValue(Object o) throws IOException;
	
	/**
	* Not a real subset, returns iterator
	* @return The Iterator over the entrySet
	* @exception IOException If we cannot obtain the iterator
	*/
	Iterator<?> entrySet() throws IOException;
	
	/**
	 * Get a stream of entry set
	 * @return
	 * @throws IOException
	 */
	Stream<?> entrySetStream() throws IOException;
	
	/**
	* Return the keyset Iterator over all elements
	* @return The Iterator over the keySet
	* @exception IOException If we cannot obtain the iterator
	*/
	Iterator<?> keySet() throws IOException;
	
	/**
	 * Get a keyset stream
	 * @return
	 * @throws IOException
	 */
	Stream<?> keySetStream() throws IOException;
		
	/**
	 * Contains a value object
	 * @param o
	 * @return boolean if the value object is found
	 * @throws IOException
	 */
	boolean containsValue(Object o) throws IOException;
	
	/**
	 * Get the first key
	 * @return The Comparable first key in the KVStore
	 * @throws IOException
	 */
	Comparable firstKey(TraversalStackElement tse, Stack stack) throws IOException;

	/**
	 * Get the last key in the KVStore
	 * @return The last, greatest valued key in the KVStore.
	 * @throws IOException
	 */
	Comparable lastKey(TraversalStackElement tse, Stack stack) throws IOException;

	
}