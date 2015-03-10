package com.neocoretechs.bigsack.test;

import com.neocoretechs.bigsack.io.pooled.BlockDBIO;
import com.neocoretechs.bigsack.io.pooled.GlobalDBIO;

public class AnalyzeBlock {
	public static void main(String[] args) throws Exception {
		BlockDBIO gdb = new BlockDBIO(args[0],args[3],false,0); // name,remote name, no create, trans id 0
		int tablespace = Integer.valueOf(args[1]);
		long blk = Long.valueOf(args[2]);
		gdb.objseek(GlobalDBIO.makeVblock(tablespace, blk));
		System.out.println(gdb.getDatablock());
	}
}
