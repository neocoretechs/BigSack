package com.neocoretechs.bigsack.iterator;
import java.io.IOException;
import java.util.ConcurrentModificationException;
import java.util.NoSuchElementException;

import com.neocoretechs.bigsack.btree.BTreeMain;
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
 * keySet iterator for persistent collection
 * @author Groff
 */
public class KeySetIterator extends AbstractIterator  {
	@SuppressWarnings("rawtypes")
	Comparable retKey, nextKey;
	public KeySetIterator(BTreeMain bTree) throws IOException {
		super(bTree);
		synchronized (bTree) {
			bTree.rewind();
			bTree.setCurrent();
			nextKey = bTree.getCurrentKey();
			bTree.getIO().deallocOutstanding();
		}
	}
	public boolean hasNext() {
		return (nextKey != null);
	}
	public Object next() {
		synchronized (bTree) {
			try {
				// move nextelem to retelem, search nextelem, get nextelem
				if (nextKey == null)
					throw new NoSuchElementException("No next element in EntrySetIterator");
				// save for return
				retKey = nextKey;
				if (bTree.seek(nextKey) == null)
					throw new ConcurrentModificationException("Next EntrySetIterator element rendered invalid");
				if (bTree.gotoNextKey() == 0) {
					nextKey = bTree.getCurrentKey();
				} else {
					nextKey = null;
				}
				bTree.getIO().deallocOutstanding();
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
