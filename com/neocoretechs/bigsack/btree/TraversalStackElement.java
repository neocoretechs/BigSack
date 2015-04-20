package com.neocoretechs.bigsack.btree;
/**
 * Class used to account for items placed on the stack to affect traversal
 * via recovery of parent indicies, to progressively move through the BTree.
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
