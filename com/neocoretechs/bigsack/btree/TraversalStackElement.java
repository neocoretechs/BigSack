package com.neocoretechs.bigsack.btree;
/**
 * Class used to account for items placed on the stack to affect traversal
 * via recovery of parent indicies, to progressively move through the BTree.
 * Contains page, index of key that generated this, and index of child that is the
 * subtree on the index of the key that generated this in relation to our retrieval.
 * @author jg Copyright(C) NeoCoreTechs 2015
 *
 */
public final class TraversalStackElement {
	BTreeKeyPage keyPage;
	int index;
	int child;
	public TraversalStackElement(BTreeKeyPage keyPage, int index, int child) {
		this.keyPage = keyPage;
		this.index = index;
		this.child = child;
	}
}
