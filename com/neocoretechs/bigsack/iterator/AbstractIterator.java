package com.neocoretechs.bigsack.iterator;

import java.io.IOException;
import java.util.Iterator;

import com.neocoretechs.bigsack.keyvaluepages.KeyValue;
import com.neocoretechs.bigsack.keyvaluepages.KeyValueMainInterface;
/**
 * Provides the superclass for out iterators and drop-in compatibility for java.util.Iterator<> contracts
 * @author jg
 *
 */
public abstract class AbstractIterator implements Iterator<Object> {
	KeyValueMainInterface kvMain;
	protected KeyValue current;
	public AbstractIterator(KeyValueMainInterface kvMain) throws IOException {
		this.kvMain = kvMain;
	}
	public abstract boolean hasNext();
	public abstract Object next();
}
