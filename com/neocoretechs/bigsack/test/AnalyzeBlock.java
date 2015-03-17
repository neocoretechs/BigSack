package com.neocoretechs.bigsack.test;

import com.neocoretechs.bigsack.io.pooled.GlobalDBIO;
import com.neocoretechs.bigsack.io.pooled.ObjectDBIO;

public class AnalyzeBlock {
	public static void main(String[] args) throws Exception {
		ObjectDBIO gdb = new ObjectDBIO(args[0],args[3],false,0); // name,remote name, no create, trans id 0
		int tablespace = Integer.valueOf(args[1]);
		long blk = Long.valueOf(args[2]);
		gdb.getIOManager().objseek(GlobalDBIO.makeVblock(tablespace, blk));
		System.out.println(gdb.getIOManager().getBlockStream(tablespace).getLbai().getBlk());
	}
}
