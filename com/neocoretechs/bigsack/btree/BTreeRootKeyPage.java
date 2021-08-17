package com.neocoretechs.bigsack.btree;

import java.io.IOException;

import com.neocoretechs.bigsack.io.pooled.BlockAccessIndex;
import com.neocoretechs.bigsack.keyvaluepages.KeyValueMainInterface;
import com.neocoretechs.bigsack.keyvaluepages.RootKeyPageInterface;
/**
 * Unlike the hash implementation, the BTree root page shares enough in common with the standard page to be a subclass of it
 * although intuitively its a bit confusing. The pages are identical except for the fact that the root page is locked to 
 * tablespace 0, block 0 and occurs once.
 * @author Jonathan Groff Copyright (C) NeoCoreTechs 2021
 *
 */
public class BTreeRootKeyPage extends BTreeKeyPage implements RootKeyPageInterface {
	public static boolean DEBUG = false;
	public BTreeRootKeyPage(KeyValueMainInterface bTree, BlockAccessIndex lbai, boolean read) throws IOException {
		super(bTree, lbai, read);
	}

	/**
	 * Calls {@link BTreeMain}.createRootNode and sets bTNode here to returned value.
	 * @param bai 
	 * @throws IOException
	 */
	@Override
	public void setRootNode(BlockAccessIndex bai) throws IOException {
		this.lbai = bai;
		bTNode = new BTNode(((BTreeMain)bTreeMain).bTreeNavigator, 0L, true);
		if(DEBUG)
			System.out.printf("%s.setRootNode block=%s, node=%s%n", this.getClass().getName(),bai,bTNode);
	}
	
}
