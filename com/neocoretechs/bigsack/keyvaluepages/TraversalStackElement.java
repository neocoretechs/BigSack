package com.neocoretechs.bigsack.keyvaluepages;

/**
 * Class used to account for items placed on the stack to affect traversal
 * via recovery of parent indicies, to progressively move through the BTree.
 * Contains page, index of key that generated this, and index of child that is the
 * subtree on the index of the key that generated this in relation to our retrieval.
 * @author Jonathan Groff Copyright(C) NeoCoreTechs 2015,2021
 *
 */
public final class TraversalStackElement {
	public RootKeyPageInterface keyPage;
	public int index;
	public int child;
	public TraversalStackElement(RootKeyPageInterface sourcePage, int index, int child) {
		this.keyPage = sourcePage;
		this.index = index;
		this.child = child;
	}
	/**
	 * Slightly kludgey manner of facilitating transfer of {@link KeySearchResult} to {@link TraversalStackElement}<p/>
	 * Index is set to insertPoint
	 * Note that if we are not 'atKey' in search result, the child is set to -insertPoint otherwise insertPoint (left child),
	 * @param ksr
	 */
	public TraversalStackElement(KeySearchResult ksr) {
		this.keyPage = ksr.page;
		this.child = ksr.atKey ? ksr.insertPoint : -ksr.insertPoint;
		this.index = ksr.insertPoint;
	}
	
	public String toString() {
		return String.format("TraversalStackElement: %s %d %d%n",keyPage, index, index);
	}

}
