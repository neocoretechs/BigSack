package com.neocoretechs.bigsack.session;

import java.io.IOException;
import java.util.Iterator;
import java.util.stream.Stream;

import com.neocoretechs.bigsack.keyvaluepages.KeySearchResult;
import com.neocoretechs.bigsack.keyvaluepages.KeyValueMainInterface;

interface OrderedKVMapInterface extends MapInterface, OrderedKVSetInterface {
	
	/**
	* Not a real subset, returns iterator vs set.
	* 'from' element inclusive, 'to' element exclusive
	* @param fkey Return from fkey
	* @param tkey Return from fkey to strictly less than tkey
	* @return The KeyValuePair Iterator over the subSet
	* @exception IOException If we cannot obtain the iterator
	*/
	Iterator<?> subSetKV(Comparable fkey, Comparable tkey) throws IOException;
	
	/**
	 * Return a Streamof key/value pairs that delivers the subset of fkey to tkey
	 * @param fkey the from key
	 * @param tkey the to key
	 * @return the stream from which the lambda expression can be utilized
	 * @throws IOException
	 */
	Stream<?> subSetKVStream(Comparable fkey, Comparable tkey) throws IOException;
	
	
	/**
	* Not a real subset, returns Iterator
	* @param tkey return from head to strictly less than tkey
	* @return The KeyValuePair Iterator over the headSet
	* @exception IOException If we cannot obtain the iterator
	*/
	Iterator<?> headSetKV(Comparable tkey) throws IOException;
	
	/**
	 * Get a stream of head set
	 * @param tkey
	 * @return
	 * @throws IOException
	 */
	Stream<?> headSetKVStream(Comparable tkey) throws IOException;
	
	
	/**
	* Not a real subset, returns Iterator
	* @param fkey return from value to end
	* @return The KeyValuePair Iterator over the tailSet
	* @exception IOException If we cannot obtain the iterator
	*/
	Iterator<?> tailSetKV(Comparable fkey) throws IOException;
	
	/**
	 * Return a tail set key/value stream
	 * @param fkey from key of tailset
	 * @return the stream from which the lambda can be utilized
	 * @throws IOException
	 */
	Stream<?> tailSetKVStream(Comparable fkey) throws IOException;
	

	
}