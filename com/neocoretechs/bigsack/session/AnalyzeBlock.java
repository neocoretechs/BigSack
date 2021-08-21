package com.neocoretechs.bigsack.session;

import com.neocoretechs.bigsack.DBPhysicalConstants;
import com.neocoretechs.bigsack.btree.BTreeKeyPage;
import com.neocoretechs.bigsack.io.pooled.BlockAccessIndex;
import com.neocoretechs.bigsack.io.pooled.Datablock;
import com.neocoretechs.bigsack.io.pooled.GlobalDBIO;
import com.neocoretechs.bigsack.keyvaluepages.KeyPageInterface;
import com.neocoretechs.bigsack.keyvaluepages.KeyValueMainInterface;

/**
 * Read a key page and dump it
 * AnalyzeBlock <dbname> <tablepace> <block> 
 * Root key page would be: AnalyzeBlock /home/db/TestDB 0 0
 * @author jg
 *
 */
public class AnalyzeBlock {
	public static void main(String[] args) throws Exception {
		if( args == null || args.length != 4) {
			System.out.println("usage: java com.neocoretechs.bigsack.test.AnalyzeBlock <database> <class> <tablespace> <block>");
			System.exit(1);
		}
		BigSackAdapter.setTableSpaceDir(args[0]);
		BufferedTreeSet bts = BigSackAdapter.getBigSackTreeSet(Class.forName(args[1]));
		BigSackSession bss = bts.getSession();
		KeyValueMainInterface bTree = bss.getKVStore();
		long vblock = GlobalDBIO.makeVblock(Integer.parseInt(args[2]), Long.parseLong(args[3]) );	
		KeyPageInterface btk = bTree.getIO().getBTreePageFromPool(vblock);
		System.out.printf("--Keypage from BlockAccessIndex:%s%n",btk);
		System.out.printf("Node: %s%n", ((BTreeKeyPage)btk).getNodeInterface());
		System.out.printf("--Raw Block:%d = %s%n",vblock,btk.getBlockAccessIndex().getBlk());
	}
}
