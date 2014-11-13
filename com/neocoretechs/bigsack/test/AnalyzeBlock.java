package com.neocoretechs.bigsack.test;

import com.neocoretechs.bigsack.io.pooled.BlockDBIO;
import com.neocoretechs.bigsack.io.pooled.GlobalDBIO;
import com.neocoretechs.bigsack.session.BigSackSession;
import com.neocoretechs.bigsack.session.BufferedTreeSet;
import com.neocoretechs.bigsack.session.SessionManager;

public class AnalyzeBlock {
	public static void main(String[] args) throws Exception {
		BlockDBIO gdb = new BlockDBIO(args[0],false,0); // name, no create, trans id 0
		int tablespace = Integer.valueOf(args[1]);
		long blk = Long.valueOf(args[2]);
		gdb.objseek(GlobalDBIO.makeVblock(tablespace, blk));
		System.out.println(gdb.getDatablock());
	}
}
