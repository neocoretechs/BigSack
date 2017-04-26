package com.neocoretechs.bigsack.btree;
/**
 * Represents the result of a tree search. Page, index, and whether that index is at a key that
 * is a direct match to the target key.
 * @author jg Copyright (C) NeoCoreTechs 2015
 *
 */
public final class TreeSearchResult {
	public BTreeKeyPage page = null;
	public boolean atKey = false;
	public int insertPoint = 0;
	/**
	 * Form a free search result unattached to a key page
	 * @param i BTree insert point index
	 * @param b true if key already exists and index points to it
	 */
	public TreeSearchResult(int i, boolean b) {
		insertPoint = i;
		atKey = b;
	}
	/**
	 * Form a search result attached to a key page
	 * @param sourcePage The key page object
	 * @param i  BTree insert point index
	 * @param b true if key already exists and index points to it
	 */
	public TreeSearchResult(BTreeKeyPage sourcePage, int i, boolean b) {
		this(i,b);
		page = sourcePage;
	}
	/**
	 * Get the target key at the stopping point of the search. Use insertPoint-1
	 * as the location of the last good key that the search found immediately preceding the
	 * item searched for.
	 * @return The Comparable key at the search location or null if insertPoint was 0
	 */
	public Comparable getTargetKey() {
		if( insertPoint == 0 )
			return null;
		return page.keyArray[insertPoint-1];
	}
	
	public String toString() {
		return "TreeSearchResult atKey:"+atKey+" insert point:"+insertPoint+" page:"+page;
	}
}
