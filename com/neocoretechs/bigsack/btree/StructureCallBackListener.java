package com.neocoretechs.bigsack.btree;
/**
 * Called back from BTreeMain traversal traverseStructure on new database page, 
 * delivers level, parent page, first and last keys for analysis.
 * @author groff
 *
 */
public interface StructureCallBackListener {

	public void call(int level, long block, long parent, Comparable key, Comparable key2, boolean isLeaf, int numKeys);
}