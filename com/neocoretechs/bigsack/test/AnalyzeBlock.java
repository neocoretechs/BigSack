package com.neocoretechs.bigsack.test;

import com.neocoretechs.bigsack.btree.BTreeKeyPage;
import com.neocoretechs.bigsack.io.pooled.GlobalDBIO;
import com.neocoretechs.bigsack.io.pooled.ObjectDBIO;
/**
 * Read a key page and dump it
 * AnalyzeBlock <dbname> <tablepace> <block> 
 * Root key page would be: AnalyzeBlock /home/db/TestDB 0 0
 * @author jg
 *
 */
public class AnalyzeBlock {
	public static void main(String[] args) throws Exception {
		if( args == null || args.length != 3) {
			System.out.println("usage: java com.neocoretechs.bigsack.test.AnalyzeBlock <database> <tablespace> <offset of page boundary in tablespace file>");
			System.exit(1);
		}
		ObjectDBIO gdb = new ObjectDBIO(args[0],null,false,0); // name,remote name, no create, trans id 0
		int tablespace = Integer.valueOf(args[1]);
		long blk = Long.valueOf(args[2]);
		// should set up block access index in bufferpool and blockstream
		gdb.getIOManager().objseek(GlobalDBIO.makeVblock(tablespace, blk));
		System.out.println("Raw Block="+gdb.getIOManager().getBlockStream(tablespace).getBlockAccessIndex());
		BTreeKeyPage btk = new BTreeKeyPage(gdb, gdb.getIOManager().getBlockStream(tablespace).getBlockAccessIndex(), true);
		System.out.println("Keypage from raw block ="+btk);
		//btk = new BTreeKeyPage(gdb,GlobalDBIO.makeVblock(tablespace, blk), false );
		//System.out.println("Keypage from location ="+btk);
	}
}
