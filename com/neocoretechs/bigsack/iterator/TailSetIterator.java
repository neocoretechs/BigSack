package com.neocoretechs.bigsack.iterator;
import java.io.IOException;
import java.util.ConcurrentModificationException;
import java.util.NoSuchElementException;
import java.util.Stack;

import com.neocoretechs.bigsack.keyvaluepages.KeyPageInterface;
import com.neocoretechs.bigsack.keyvaluepages.KeySearchResult;
import com.neocoretechs.bigsack.keyvaluepages.KeyValueMainInterface;
import com.neocoretechs.bigsack.keyvaluepages.TraversalStackElement;
/*
* Copyright (c) 2003, NeoCoreTechs
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
* Provides a persistent collection iterator greater or equal to 'from' element
* @author Jonathan Groff Copyright (C) NeoCoreTechs 2021
*/
public class TailSetIterator extends AbstractIterator {
	@SuppressWarnings("rawtypes")
	Comparable fromKey, nextKey, retKey;
	TraversalStackElement tracker = new TraversalStackElement(null, 0,0);
	Stack stack = new Stack();
	private static boolean DEBUG = false;
	public TailSetIterator(@SuppressWarnings("rawtypes") Comparable fromKey, KeyValueMainInterface bTree)
		throws IOException {
		super(bTree);
		this.fromKey = fromKey;
		synchronized (bTree) {
			KeySearchResult tsr = bTree.seekKey(fromKey, stack);
			tracker = new TraversalStackElement(tsr);
			current = ((KeyPageInterface)tracker.keyPage).getKeyValueArray(tracker.index);
			if(current == null)
				nextKey = null;
			else
				nextKey = current.getmKey(); // closest key
			if (nextKey == null || nextKey.compareTo(fromKey) < 0) {
				nextKey = null;
				stack.clear();
			}
			//bTree.getIO().deallocOutstanding();
			if( DEBUG )
				System.out.println("TailSetIterator.Nextkey:"+nextKey);
		}
	}
	public boolean hasNext() {
		return (nextKey != null);
	}
	public Object next() {
		synchronized (kvMain) {
			try {
				// move nextelem to retelem, search nextelem, get nextelem
				if (nextKey == null)
					throw new NoSuchElementException("No next iterator element");
				retKey = nextKey;
				if((tracker = kvMain.gotoNextKey(tracker, stack)) != null) {
					current = ((KeyPageInterface)tracker.keyPage).getKeyValueArray(tracker.index);
					if(current == null) {
						nextKey = null;
						stack.clear();
						//throw new ConcurrentModificationException("Next iterator element rendered invalid. Last good key:"+nextKey);
					} else {
						nextKey = current.getmKey();
					}
				} else {
					nextKey = null;
					stack.clear();
				}
				//kvMain.getIO().deallocOutstanding();
				return retKey;
			} catch (IOException ioe) {
				throw new RuntimeException(ioe.toString());
			}
		}
	}
	public void remove() {
		throw new UnsupportedOperationException("No provision to remove from Iterator");
	}
}
