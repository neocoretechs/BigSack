package com.neocoretechs.bigsack.btree;
/**
 * Represents the result of a tree search. Page, index, and whether that index is at a key that
 * is a direct math to the target key supplied to generate the instance of this.
 * @author jg Copyright (C) NeoCoreTechs 2015
 *
 */
public final class TreeSearchResult {
	public BTreeKeyPage page = null;
	public boolean atKey = false;
	public int insertPoint = 0;
	public TreeSearchResult(int i, boolean b) {
		insertPoint = i;
		atKey = b;
	}
	public TreeSearchResult(BTreeKeyPage sourcePage, int i, boolean b) {
		this(i,b);
		page = sourcePage;
	}
}
