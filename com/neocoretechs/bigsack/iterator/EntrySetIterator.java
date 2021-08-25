package com.neocoretechs.bigsack.iterator;
import java.io.IOException;
import java.util.ConcurrentModificationException;
import java.util.NoSuchElementException;
import java.util.Stack;

import com.neocoretechs.bigsack.keyvaluepages.KeyPageInterface;
import com.neocoretechs.bigsack.keyvaluepages.KeyValueMainInterface;
import com.neocoretechs.bigsack.keyvaluepages.TraversalStackElement;
/*
* Copyright (c) 1997,2003, NeoCoreTechs
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
 * Iterator for entrySet of persistent collection
 * @author Jonathan Groff Copyright (C) NeoCoreTechs 2021
 *
 */
public class EntrySetIterator extends AbstractIterator {
	Object retElem, nextElem;
	@SuppressWarnings("rawtypes")
	Comparable retKey, nextKey;
	TraversalStackElement tracker = new TraversalStackElement(null, 0,0);
	Stack stack = new Stack();
	public EntrySetIterator(KeyValueMainInterface kvMain) throws IOException {
		super(kvMain);
		synchronized (kvMain) {
			current = kvMain.rewind(tracker,stack);
			nextElem = current.getmValue();
			nextKey = current.getmKey();
			kvMain.getIO().deallocOutstanding();
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
				// save for return
				retKey = nextKey;
				retElem = nextElem;
				if((tracker = kvMain.gotoNextKey(tracker, stack)) != null) {
					current = ((KeyPageInterface)tracker.keyPage).getKeyValueArray(tracker.index);
					if(current == null)
						throw new ConcurrentModificationException("Next iterator element rendered invalid. Last good key:"+nextKey);
					nextKey = current.getmKey();
					nextElem = current.getmValue();
				} else {
					nextElem = null;
					nextKey = null;
					stack.clear();
				}
				kvMain.getIO().deallocOutstanding();
				return new Entry(retKey, retElem);
			} catch (IOException ioe) {
				throw new RuntimeException(ioe.toString());
			}
		}
	}
	
	public void remove() {
		throw new UnsupportedOperationException("No provision to remove from Iterator");
	}

}
