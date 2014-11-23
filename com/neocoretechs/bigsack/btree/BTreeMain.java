package com.neocoretechs.bigsack.btree;
import java.io.IOException;

import com.neocoretechs.bigsack.Props;
import com.neocoretechs.bigsack.io.pooled.ObjectDBIO;
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
* Main bTree class.  Manipulates retrieval stack of bTreeKeyPages and provides access
* to seek/add/delete functions.
* Important to note that the data is stores as arrays serialized out in key pages. Related to that
* is the concept of element 0 of those arrays being 'this', hence the special treatment in CRUD 
* @author Groff
*/
public final class BTreeMain {
	static int BOF = 1;
	static int EOF = 2;
	static int NOTFOUND = 3;
	static int ALREADYEXISTS = 4;
	static int STACKERROR = 5;
	static int TREEERROR = 6;
	static int MAXSTACK = 64;

	private BTreeKeyPage root;
	private long numKeys;
	BTreeKeyPage currentPage;
	int currentIndex;
	@SuppressWarnings("rawtypes")
	private Comparable currentKey;
	private Object currentObject;
	BTreeKeyPage[] keyPageStack;
	int[] indexStack;
	int stackDepth;
	boolean atKey;
	private static boolean DEBUG = false;
	private ObjectDBIO sdbio;

	public BTreeMain(ObjectDBIO sdbio) throws IOException {
		this.sdbio = sdbio;
		keyPageStack = new BTreeKeyPage[MAXSTACK];
		indexStack = new int[MAXSTACK];
		currentPage = setRoot(BTreeKeyPage.getPageFromPool(sdbio, 0L));
		if( DEBUG ) System.out.println("Root BTreeKeyPage: "+currentPage);
		currentIndex = 0;
		// Attempt to retrieve last good key count
		numKeys = 0;//sdbio.getKeycountfile().getKeysCount();
		// Consistency check, also needed to get number of keys
		// If no keys, give it check to make sure log was not compromised
		rewind();
		int i;
		long tim = System.currentTimeMillis();
		while ((i = gotoNextKey()) == 0) {
				//if( Props.DEBUG ) System.out.println("gotoNextKey returned: "+i);
				++numKeys;
		}
		System.out.println("Consistency check for "+sdbio.getDBName()+" returned "+numKeys+" keys in "+(System.currentTimeMillis()-tim)+" ms.");
		sdbio.deallocOutstanding();
		//if( Props.DEBUG ) System.out.println("Records: " + numKeys);
	}

	/**
	* currentPage and currentIndex set.
	* @param targetKey The Comparable key to seek
	* @return data Object if found. Null otherwise.
	* @exception IOException if read failure
	*/
	@SuppressWarnings("rawtypes")
	public Object seek(Comparable targetKey) throws IOException {
		if (search(targetKey)) {
			setCurrent();
			return getCurrentObject();
		} else
			return (null);
	}

	/**
	* Add key/data object to tree.
	* @param newKey The new key to add
	* @param newObject The new data payload to add
	* @return 0 if ok, <> 0 if error
	* @exception IOException If write fails 
	*/
	@SuppressWarnings("rawtypes")
	public int add(Comparable newKey, Object newObject) throws IOException {
		int i, j, k;
		Comparable saveKey = null;
		Object saveObject = null;
		BTreeKeyPage savePagePointer = null;
		BTreeKeyPage leftPagePtr = null;
		BTreeKeyPage rightPagePtr = null;

		atKey = true;
		// If tree is empty, make a root
		if (getNumKeys() == 0) {
			currentPage = getRoot();
			currentIndex = 0;
			currentPage.insert(newKey, newObject, currentIndex);
			++numKeys;
			return 0;
		}
		// Determine whether we need to overwrite existing data
		// we need a search call regardless to set up our target insertion point for later
		if( search(newKey) && atKey ) {
			// found it, delete old data
			//if( Props.DEBUG ) System.out.println("--Add found key "+newKey+" in current page "+currentPage);
			currentPage.deleteData(getIO(), currentIndex);
			currentPage.putDataToArray(newObject, currentIndex);
			// add location of new data to id array
			//if( Props.DEBUG ) System.out.println("--Add ABOUT TO PUT PAGE "+currentPage);
			currentPage.putPages(getIO());
			//if( Props.DEBUG ) System.out.println("--Add finished putPage");
			return 0;
		}

		do {
			// About to insert key. See if the page is going to overflow
			// If the current index is at end, dont insert, proceed to split
			if ((i = currentPage.numKeys) == BTreeKeyPage.MAXKEYS) {
				// Save rightmost key/data/pointer
				--i;
				if (currentIndex == BTreeKeyPage.MAXKEYS) {
					saveKey = newKey;
					saveObject = newObject;
					savePagePointer = rightPagePtr;
				} else {
					saveKey = currentPage.keyArray[i];
					saveObject = currentPage.getDataFromArray(getIO(), i);
					savePagePointer = currentPage.getPage(getIO(), i + 1);
					currentPage.insert(newKey, newObject, currentIndex);
					currentPage.putPageToArray(leftPagePtr, currentIndex);
					currentPage.putPageToArray(rightPagePtr, currentIndex + 1);
				}

				// Split has occurred. Pull the middle key out
				i = BTreeKeyPage.MAXKEYS / 2;
				newKey = currentPage.keyArray[i];
				currentPage.putKeyToArray(null, i);
				newObject = currentPage.getDataFromArray(getIO(), i);
				currentPage.putDataToArray(null, i);
				leftPagePtr = currentPage;
				// Create new page for the right half of the old page and move right half in
				// (Note that since we are dealing with object references, we have
				// to clear them from the old page, or we might leak objects.)
				rightPagePtr = new BTreeKeyPage();
				k = 0;
				for (j = i + 1; j < BTreeKeyPage.MAXKEYS; j++) {
					rightPagePtr.putPageToArray(currentPage.getPage(getIO(), j), k);
					currentPage.nullPageArray(j);
					rightPagePtr.keyArray[k] = currentPage.keyArray[j];
					currentPage.keyArray[j] = null;
					rightPagePtr.putDataToArray(currentPage.getDataFromArray(getIO(), j), k);
					currentPage.putDataToArray(null, j);
					++k;
				}
				rightPagePtr.putPageToArray(
					currentPage.getPage(getIO(), BTreeKeyPage.MAXKEYS),
					k);
				rightPagePtr.keyArray[k] = saveKey;
				rightPagePtr.putDataToArray(saveObject, k);
				rightPagePtr.putPageToArray(savePagePointer, k + 1);

				leftPagePtr.numKeys = i;
				leftPagePtr.setUpdated(true);
				rightPagePtr.numKeys = BTreeKeyPage.MAXKEYS - i;
			} else {
				// Insert key/object at current location
				currentPage.insert(newKey, newObject, currentIndex);
				currentPage.putPageToArray(leftPagePtr, currentIndex);
				currentPage.putPageToArray(rightPagePtr, currentIndex + 1);
				++numKeys;
				return 0;
			}
			// Try to pop. If we can't pop, make a new root
		} while (pop());
		getRoot().pageId = -1L;
		setRoot(currentPage = new BTreeKeyPage(0L));
		currentIndex = 0;
		getRoot().insert(newKey, newObject, currentIndex);
		getRoot().putPageToArray(leftPagePtr, 0);
		getRoot().putPageToArray(rightPagePtr, 1);
		++numKeys;
		return 0;
	}

	/**
	* Add key object to tree. If we locate it return that position, else write it
	* @param newKey The new key to add
	* @return 0 if ok, <> 0 if error
	* @exception IOException If write fails 
	*/
	@SuppressWarnings("rawtypes")
	public int add(Comparable newKey) throws IOException {
		int i, j, k;
		Comparable saveKey = null;
		//Object saveObject = null;
		BTreeKeyPage savePagePointer = null;
		BTreeKeyPage leftPagePtr = null;
		BTreeKeyPage rightPagePtr = null;

		atKey = true;
		// If tree is empty, make a root
		if (getNumKeys() == 0) {
			currentPage = getRoot();
			currentIndex = 0;
			currentPage.insert(newKey, null, currentIndex);
			++numKeys;
			return 0;
		}
		// Determine whether data is present
		// we need a search call regardless to set up our target insertion point for later should we decide to do so
		if( search(newKey) && atKey ) {
			// found it,
			//if( Props.DEBUG ) System.out.println("--Add found key "+newKey+" in current page "+currentPage);
			return 0;
		}

		do {
			// About to insert key. See if the page is going to overflow
			// If the current index is at end, dont insert, proceed to split
			if ((i = currentPage.numKeys) == BTreeKeyPage.MAXKEYS) {
				// Save rightmost key/data/pointer
				--i;
				if (currentIndex == BTreeKeyPage.MAXKEYS) {
					saveKey = newKey;
					//saveObject = null;
					savePagePointer = rightPagePtr;
				} else {
					saveKey = currentPage.keyArray[i];
					//saveObject = currentPage.getDataFromArray(getIO(), i);
					savePagePointer = currentPage.getPage(getIO(), i + 1);
					currentPage.insert(newKey, null, currentIndex);
					currentPage.putPageToArray(leftPagePtr, currentIndex);
					currentPage.putPageToArray(rightPagePtr, currentIndex + 1);
				}

				// Split has occurred. Pull the middle key out
				i = BTreeKeyPage.MAXKEYS / 2;
				newKey = currentPage.keyArray[i];
				currentPage.putKeyToArray(null, i);
				//newObject = currentPage.getDataFromArray(getIO(), i);
				currentPage.putDataToArray(null, i);
				leftPagePtr = currentPage;
				// Create new page for the right half of the old page and move right half in
				// (Note that since we are dealing with object references, we have
				// to clear them from the old page, or we might leak objects.)
				rightPagePtr = new BTreeKeyPage();
				k = 0;
				for (j = i + 1; j < BTreeKeyPage.MAXKEYS; j++) {
					rightPagePtr.putPageToArray(currentPage.getPage(getIO(), j), k);
					currentPage.nullPageArray(j);
					rightPagePtr.keyArray[k] = currentPage.keyArray[j];
					currentPage.keyArray[j] = null;
					rightPagePtr.putDataToArray(currentPage.getDataFromArray(getIO(), j), k);
					currentPage.putDataToArray(null, j);
					++k;
				}
				rightPagePtr.putPageToArray(
					currentPage.getPage(getIO(), BTreeKeyPage.MAXKEYS),
					k);
				rightPagePtr.keyArray[k] = saveKey;
				rightPagePtr.putDataToArray(null, k);
				rightPagePtr.putPageToArray(savePagePointer, k + 1);

				leftPagePtr.numKeys = i;
				leftPagePtr.setUpdated(true);
				rightPagePtr.numKeys = BTreeKeyPage.MAXKEYS - i;
			} else {
				// Insert key/object at current location
				currentPage.insert(newKey, null, currentIndex);
				currentPage.putPageToArray(leftPagePtr, currentIndex);
				currentPage.putPageToArray(rightPagePtr, currentIndex + 1);
				++numKeys;
				return 0;
			}
			// Try to pop. If we can't pop, make a new root
		} while (pop());
		getRoot().pageId = -1L;
		setRoot(currentPage = new BTreeKeyPage(0L));
		currentIndex = 0;
		getRoot().insert(newKey, null, currentIndex);
		getRoot().putPageToArray(leftPagePtr, 0);
		getRoot().putPageToArray(rightPagePtr, 1);
		++numKeys;
		return 0;
	}

	/**
	* Remove key/data object.
	* @param newKey The key to delete
	* @return 0 if ok, <>0 if error
	* @exception IOException if seek or write failure
	*/
	@SuppressWarnings("rawtypes")
	public int delete(Comparable newKey) throws IOException {
		BTreeKeyPage tpage;
		int tindex;
		BTreeKeyPage leftPage;
		BTreeKeyPage rightPage;

		// Is the key there?
		if(!search(newKey))
			return (NOTFOUND);
		if( DEBUG ) System.out.println("--ENTERING DELETE LOOP FOR "+newKey+" with currentPage "+currentPage);
		while (true) {
			// If either left or right pointer
			// is null, we're at a leaf or a
			// delete is percolating up the tree
			// Collapse the page at the key
			// location
			if ((currentPage.getPage(getIO(), currentIndex) == null) || 
				(currentPage.getPage(getIO(), currentIndex + 1) == null)) {
				if( Props.DEBUG ) System.out.println("Delete loop No left/right pointer on "+currentPage);
				// Save non-null pointer
				if (currentPage.pageArray[currentIndex] == null)
					tpage = currentPage.getPage(getIO(), currentIndex + 1);
				else
					tpage = currentPage.getPage(getIO(), currentIndex);
				if( Props.DEBUG ) System.out.println("Delete selected non-null page is "+tpage);
				// At leaf - delete the key/data
				currentPage.deleteData(getIO(), currentIndex);
				currentPage.delete(currentIndex);
				if( Props.DEBUG ) System.out.println("Just deleted "+currentPage+" @ "+currentIndex);
				// Rewrite non-null pointer
				currentPage.putPageToArray(tpage, currentIndex);
				// If we've deleted the last key from the page, eliminate the
				// page and pop up a level. Null the page pointer
				// at the index we've popped to.
				// If we can't pop, all the keys have been eliminated (or, they should have).
				// Null the root, clear the keycount
				if (currentPage.numKeys == 0) {
					// Following guards against leaks
					if( DEBUG ) System.out.println("Delete found numKeys 0");
					currentPage.nullPageArray(0);
					if (pop()) { // Null pointer to node
						// just deleted
						if( DEBUG ) System.out.println("Delete popped "+currentPage+" nulling and decrementing key count");
						currentPage.nullPageArray(currentIndex);
						--numKeys;
						if( DEBUG ) System.out.println("Key count now "+numKeys+" returning");
						// Perform re-seek to re-establish location
						//search(newKey);
						return (0);
					}
					if( DEBUG ) System.out.println("Cant pop, clear root");
					// Can't pop -- clear the root
					// if root had pointer make new root
					if (tpage != null) {
						if( DEBUG ) System.out.println("Delete tpage not null, setting page Id/current page root "+tpage);
						tpage.pageId = 0L;
						setRoot(tpage);
						currentPage = tpage;
					} else {
						// no more keys
						if( DEBUG ) System.out.println("Delete No more keys, setting new root page");
						setRoot(new BTreeKeyPage(0L));
						getRoot().setUpdated(true);
						currentPage = null;
						numKeys = 0;
					}
					atKey = false;
					if( DEBUG ) System.out.println("Delete returning");
					return (0);
				}
				// If we haven't deleted the last key, see if we have few enough
				// keys on a sibling node to coalesce the two. If the
				// keycount is even, look at the sibling to the left first;
				// if the keycount is odd, look at the sibling to the right first.
				if( DEBUG ) System.out.println("Have not deleted last key");
				if (stackDepth == 0) { // At root - no siblings
					if( DEBUG ) System.out.println("Delete @ root w/no siblings");
					--numKeys;
					//search(newKey);
					if( DEBUG ) System.out.println("Delete returning after numKeys set to "+numKeys);
					return (0);
				}
				// Get parent page and index
				if( DEBUG ) System.out.println("Delete get parent page and index");
				tpage = keyPageStack[stackDepth - 1];
				tindex = indexStack[stackDepth - 1];
				if( DEBUG ) System.out.println("Delete tpage now "+tpage+" and index now "+tindex+" with stack depth "+stackDepth);
				// Get sibling pages
				if( DEBUG ) System.out.println("Delete Getting sibling pages");
				if (tindex > 0) {
					leftPage = tpage.getPage(getIO(), tindex - 1);
					if( DEBUG ) System.out.println("Delete tindex > 0 @ "+tindex+" left page "+leftPage);
				} else {
					leftPage = null;
					if( DEBUG ) System.out.println("Delete tindex not > 0 @ "+tindex+" left page "+leftPage);
				}
				if (tindex < tpage.numKeys) {
					rightPage = tpage.getPage(getIO(), tindex + 1);
				} else {
					rightPage = null;
				}
				if( DEBUG ) System.out.println("Delete tindex "+tindex+" tpage.numKeys "+tpage.numKeys+" right page "+rightPage);
				// Decide which sibling
				if( DEBUG ) System.out.println("Delete find sibling from "+leftPage+" -- " + rightPage);
				if (numKeys % 2 == 0)
					if (leftPage == null)
						leftPage = currentPage;
					else
						rightPage = currentPage;
				else 
					if (rightPage == null)
						rightPage = currentPage;
					else
						leftPage = currentPage;
				if( DEBUG ) System.out.println("Delete found sibling from "+leftPage+" -- " + rightPage);

				// assertion check
				if (leftPage == null || rightPage == null) {
					if( DEBUG ) System.out.println("ASSERTION CHECK FAILED, left/right page null in delete");
					return (TREEERROR);
				}
				// Are the siblings small enough to coalesce
				// address coalesce later
				//if (leftPage.numKeys + rightPage.numKeys + 1 > BTreeKeyPage.MAXKEYS) {
					// Coalescing not possible, exit
					--numKeys;
					//search(newKey);
					if( DEBUG ) System.out.println("Cant coalesce, returning with keys="+numKeys);
					return (0);
				/*	
				} else {
					// Coalescing is possible. Grab the parent key, build a new
					// node. Discard the old node.
					// (If sibling is left page, then the new page is left page,
					// plus parent key, plus original page. New page is old left
					// page. If sibling is right page, then the new page
					// is the original page, the parent key, plus the right
					// page.
					// Once the new page is created, delete the key on the
					// parent page. Then cycle back up and delete the parent key.)
					coalesce(tpage, tindex, leftPage, rightPage);
					if( Props.DEBUG ) System.out.println("Coalesced "+tpage+" -- "+tindex+" -- "+leftPage+" -- "+rightPage);
					// Null right page of parent
					tpage.nullPageArray(tindex + 1);
					// Pop up and delete parent
					pop();
					if( Props.DEBUG ) System.out.println("Delete Popped, now continuing loop");
					continue;
				}
				*/
			} else {
				// Not at a leaf. Get a successor or predecessor key.
				// Copy the predecessor/successor key into the deleted key's slot, then "move
				// down" to the leaf and do a delete there.
				// Note that doing the delete could cause a "percolation" up the tree.
				// Save current page and  and index
				if( Props.DEBUG ) System.out.println("Delete Not a leaf, find next key");
				tpage = currentPage;
				tindex = currentIndex;
				if (currentPage.getPage(getIO(), currentIndex) != null) {
					// Get predecessor if possible
					if( DEBUG ) System.out.println("Delete Seeking right tree");
					if (!seekRightTree()) {
						if( DEBUG ) System.out.println("Delete cant seek right tree, returning");
						return (STACKERROR);
					}
				} else { // Get successor
					if (currentPage.getPage(getIO(), currentIndex + 1) == null) {
						if( DEBUG ) System.out.println("Delete cant get successor, returning");
						return (TREEERROR);
					}
					currentIndex++;
					if (!seekLeftTree()) {
						if( DEBUG ) System.out.println("Delete cant seek left tree, returning");
						return (STACKERROR);
					}
				}
				// Replace key/data with successor/predecessor
				tpage.putKeyToArray(currentPage.keyArray[currentIndex], tindex);
				tpage.putDataToArray(
					currentPage.getDataFromArray(getIO(), currentIndex),
					tindex);
				if( DEBUG ) System.out.println("Delete re-entring loop to delete key on leaf of tpage "+tpage);
				// Reenter loop to delete key on leaf
			}
		}
	}

	/** 
	* Coalesce a node
	* Combine the left page, the parent key (at offset index) and the right 
	* page contents.
	* The contents of the right page are nulled. Note that it's the job of the
	* caller to delete the parent.
	* @param parent The parent page
	* @param index The index on the parent page to move to end of left key
	* @param left The left page
	* @param right The right page
	* @exception IOException If seek/writes fail
	*/
	private void coalesce (
		BTreeKeyPage parent,
		int index,
		BTreeKeyPage left,
		BTreeKeyPage right)
		throws IOException {
		int i, j;

		// Append the parent key to the end of the left key page
		left.putKeyToArray(parent.keyArray[index], left.numKeys);
		left.putDataToArray(parent.getDataFromArray(getIO(), index), left.numKeys);

		// Append the contents of the right
		// page onto the left key page
		j = left.numKeys + 1;
		for (i = 0; i < right.numKeys; i++) {
			left.putPageToArray(right.getPage(getIO(), i), j);
			left.putDataToArray(right.getDataFromArray(getIO(), i), j);
			left.keyArray[j] = right.keyArray[i];
			j++;
		}
		left.putPageToArray(
			right.getPage(getIO(), right.numKeys),
			left.numKeys + right.numKeys + 1);
		left.numKeys += right.numKeys + 1;

		// Null the right page (no leaks)
		for (i = 0; i < right.numKeys; i++) {
			right.nullPageArray(i);
			right.keyArray[i] = null;
			right.putDataToArray(null, i);
		}
		right.nullPageArray(right.numKeys);
	}

	/**
	 * Rewind current position to beginning of tree
	 * @exception IOException If read fails
	 */
	public void rewind() throws IOException {
		currentPage = getRoot();
		currentIndex = 0;
		clearStack();
		if (currentPage.getPage(getIO(), currentIndex) != null)
				seekLeftTree();
		atKey = false;
	}

	/**
	 * Set current position to end of tree
	 * @exception IOException If read fails
	 */
	public void toEnd() throws IOException {
		currentPage = getRoot();
		if (getNumKeys() != 0) {
			clearStack();
			currentIndex = currentPage.numKeys;
			if (currentPage.getPage(getIO(), currentIndex) != null) {
				seekRightTree();
				currentIndex++;
			}
		} else
			currentIndex = 0;
		atKey = false;
	}

	/**
	* Seek to location of next key in tree
	* @return 0 if ok, != 0 if error
	* @exception IOException If read fails
	*/
	public int gotoNextKey() throws IOException {

		//if (getNumKeys() == 0)
		//	return (EOF);

		// If we are at a key, then advance the index
		if (atKey)
			currentIndex++;

		// If we are not at a key, then see if the pointer is null.
		if (currentPage.getPage(getIO(), currentIndex) == null)
			// Pointer is null, is it the last one on the page?
			if (currentIndex == currentPage.numKeys) {
				// Last pointer on page. We have to pop up
				while (pop())
					if (currentIndex != currentPage.numKeys) {
						setCurrent();
						return (0);
					}
				atKey = false;
				return (EOF);
			} else { // Not last pointer on page.
				// Skip to next key
				setCurrent();
				return (0);
			}
		// Pointer not null, seek to "leftmost" key in current subtree
		if (seekLeftTree()) {
			setCurrent();
			return (0);
		}
		atKey = false;
		return (EOF);
	}

	/**
	* Go to location of previous key in tree
	* @return 0 if ok, <>0 if error
	* @exception IOException If read fails
	*/
	public int gotoPrevKey() throws IOException {
		if (getNumKeys() == 0)
			return (BOF);

		// If we are at a key, then simply back up the index

		// If we are not at a key, then see if
		// the pointer is null.
		if (currentPage.getPage(getIO(), currentIndex) == null)
			// Pointer is null, is it the first one on the page?
			if (currentIndex == 0) {
				// First pointer on page. We have to pop up
				while (pop())
					if (currentIndex != 0) {
						currentIndex--;
						setCurrent();
						return (0);
					}
				atKey = false;
				return (BOF);
			} else {
				// Not first pointer on page. Skip to previous key
				currentIndex--;
				setCurrent();
				return (0);
			}
		// Pointer not null, seek to "rightmost" key in current subtree
		if (seekRightTree()) {
			setCurrent();
			return (0);
		}
		atKey = false;
		return (EOF);
	}

	/**
	* Set the current object and key based on value of currentPage
	* and currentIndex
	*/
	public void setCurrent() throws IOException {
		atKey = true;
		setCurrentKey(currentPage.keyArray[currentIndex]);
		setCurrentObject(currentPage.getDataFromArray(getIO(), currentIndex));
	}

	/**
	* 
	* search method used by seek, insert, and delete etc.
	* @param targetKey The key to search for
	* @return true if found
	* @exception IOException If read fails
	*/
	@SuppressWarnings("rawtypes")
	public boolean search(Comparable targetKey) throws IOException {
		// File empty?
		if (getNumKeys() == 0) {
			if( DEBUG ) System.out.println("*** NO KEYS! ***");
			return false;
		}
		// Search - start at root
		clearStack();
		currentPage = getRoot();
		do {
			currentIndex = currentPage.search(targetKey);
			if (currentIndex >= 0) // Key found
				break;
			currentIndex = (-currentIndex) - 1;
			if (currentPage.getPage(getIO(), currentIndex) == null) {
				atKey = false;
				return false;
			}
			if (!push()) {
				atKey = false;
				return false;
			}
			currentPage = currentPage.getPage(getIO(), currentIndex);
		} while (true);
		atKey = true;
		return true;
	}

	/**
	* Seeks to leftmost key in current subtree
	*/
	private boolean seekLeftTree() throws IOException {
		while (push()) {
			currentPage = currentPage.getPage(getIO(), currentIndex);
			currentIndex = 0;
			if (currentPage.getPage(getIO(), currentIndex) == null)
				return (true);
		}
		return (false);
	}

	/**
	* Seeks to rightmost key in current subtree
	*/
	private boolean seekRightTree() throws IOException {
		while (push()) {
			currentPage = currentPage.getPage(getIO(), currentIndex);
			currentIndex = currentPage.numKeys;
			if (currentPage.getPage(getIO(), currentIndex) == null) {
				currentIndex--;
				return (true);
			}
		}
		return (false);
	}

	/** Internal routine to push stack */
	private boolean push() {
		if (stackDepth == MAXSTACK)
			return (false);
		keyPageStack[stackDepth] = currentPage;
		indexStack[stackDepth++] = currentIndex;
		return (true);
	}

	/** Internal routine to pop stack */
	private boolean pop() {
		if (stackDepth == 0)
			return (false);
		stackDepth--;
		currentPage = keyPageStack[stackDepth];
		currentIndex = indexStack[stackDepth];
		return (true);
	}

	/**
	* Internal routine to clear references on stack
	*/
	private void clearStack() {
		for (int i = 0; i < MAXSTACK; i++)
			keyPageStack[i] = null;
		stackDepth = 0;
	}

	public Object getCurrentObject() {
		return currentObject;
	}

	public void setCurrentObject(Object currentObject) {
		this.currentObject = currentObject;
	}

	@SuppressWarnings("rawtypes")
	public Comparable getCurrentKey() {
		return currentKey;
	}

	@SuppressWarnings("rawtypes")
	public void setCurrentKey(Comparable currentKey) {
		this.currentKey = currentKey;
	}

	public ObjectDBIO getIO() {
		return sdbio;
	}

	public void setIO(ObjectDBIO sdbio) {
		this.sdbio = sdbio;
	}

	public long getNumKeys() {
		return numKeys;
	}

	public void setNumKeys(long s) {
		this.numKeys = s;
	}

	public BTreeKeyPage getRoot() {
		return root;
	}

	public BTreeKeyPage setRoot(BTreeKeyPage root) {
		this.root = root;
		return root;
	}

}
