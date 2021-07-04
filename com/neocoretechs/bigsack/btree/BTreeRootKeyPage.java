package com.neocoretechs.bigsack.btree;

import java.io.IOException;

import com.neocoretechs.bigsack.hashmap.HMapMain;
import com.neocoretechs.bigsack.hashmap.HTNode;
import com.neocoretechs.bigsack.io.pooled.BlockAccessIndex;
import com.neocoretechs.bigsack.io.pooled.GlobalDBIO;
import com.neocoretechs.bigsack.keyvaluepages.KeyValueMainInterface;
import com.neocoretechs.bigsack.keyvaluepages.RootKeyPageInterface;

public class BTreeRootKeyPage extends BTreeKeyPage implements RootKeyPageInterface {
	
	public BTreeRootKeyPage(KeyValueMainInterface bTree, BlockAccessIndex lbai, boolean read) throws IOException {
		super(bTree, lbai, read);
	}

	/**
	 * This is called from getPageFromPool get set up a new clean node
	 * @param sdbio The database IO main class
	 * @param lbai The BlockAcceesIndex page block holding page data
	 * @param read true to read the contents of the btree key from page, otherwise a new page to be filled
	 */
	public BTreeRootKeyPage(KeyValueMainInterface bTree, BlockAccessIndex lbai, BTNode btNode, boolean read) throws IOException {
		super(bTree, lbai, btNode, read);
	}
	/**
	 * Calls {@link BTreeMain}.createRootNode and sets bTNode here to returned value.
	 * @param bai 
	 * @throws IOException
	 */
	@Override
	public void setRootNode(BlockAccessIndex bai) throws IOException {
		this.lbai = bai;
		int tablespace = GlobalDBIO.getTablespace(bai.getBlockNum());
		bTNode = new BTNode((BTree) bTree, true);
	}
	


}
