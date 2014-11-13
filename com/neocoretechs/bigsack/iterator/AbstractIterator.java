package com.neocoretechs.bigsack.iterator;

import java.io.IOException;
import java.util.Iterator;

import com.neocoretechs.bigsack.btree.BTreeMain;
/**
 * Provides the superclass for out iterators and drop-in compatibility for java.util.Iterator<> contracts
 * @author jg
 *
 */
public abstract class AbstractIterator implements Iterator<Object> {
	BTreeMain bTree;
	public AbstractIterator(BTreeMain bTree) throws IOException {
		this.bTree = bTree;
	}
	public abstract boolean hasNext();
	public abstract Object next();
}
