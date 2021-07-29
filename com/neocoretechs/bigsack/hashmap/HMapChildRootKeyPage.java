package com.neocoretechs.bigsack.hashmap;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;


import com.neocoretechs.bigsack.io.pooled.BlockAccessIndex;
import com.neocoretechs.bigsack.io.pooled.Datablock;
import com.neocoretechs.bigsack.io.pooled.GlobalDBIO;
import com.neocoretechs.bigsack.io.stream.PageIteratorIF;
import com.neocoretechs.bigsack.keyvaluepages.ChildRootKeyPageInterface;
import com.neocoretechs.bigsack.keyvaluepages.KeyPageInterface;
import com.neocoretechs.bigsack.keyvaluepages.KeyValue;
import com.neocoretechs.bigsack.keyvaluepages.KeyValueMainInterface;
import com.neocoretechs.bigsack.keyvaluepages.NodeInterface;
import com.neocoretechs.bigsack.keyvaluepages.RootKeyPageInterface;

public class HMapChildRootKeyPage implements ChildRootKeyPageInterface {
	private static boolean DEBUG = false;
	public final static int MAXKEYSCHILD = 512; // 9 bits (0-511) times 8 bytes long page pointer
	protected BlockAccessIndex lbai = null; // The page is tied to a block
	protected KeyValueMainInterface hMapMain;
	protected long numKeys;
	protected long[] childKeys = new long[MAXKEYSCHILD];

	public HMapChildRootKeyPage(HMapMain hMap, BlockAccessIndex lbai, boolean read) throws IOException {
		this.hMapMain = hMap;
		this.lbai = lbai;
		for(int i = 0; i < MAXKEYSCHILD; i++)
			childKeys[i] = -1L;
		if(read) {	
			readFromDBStream(GlobalDBIO.getBlockInputStream(lbai));
		}
	}
	@Override
	public synchronized long getPageId() {
		return lbai.getBlockNum();
	}
	
	@Override
	public synchronized void putPage() throws IOException {
		DataOutputStream dos = GlobalDBIO.getBlockOutputStream(lbai);
		dos.writeLong(numKeys);
		for(int i = 0; i < numKeys; i++) {
			dos.writeLong(childKeys[i]);
		}
		dos.flush();
		dos.close();
		setUpdated(true); // underlying page is updated
	}
	
	@Override
	public synchronized void readFromDBStream(DataInputStream dis) throws IOException {
		if(dis.available() < 8)
			return;
		numKeys = dis.readLong();
		if(DEBUG )
			System.out.printf("%s.readFromDBStream numKeys=%d%n",this.getClass().getName(),numKeys);
		for(int i = 0; i < numKeys; i++) {
			childKeys[i] = dis.readLong();	
		}
	}

	@Override
	public synchronized void setPageIdArray(int index, long block, boolean update) throws IOException {
		if(index >= numKeys) {
			numKeys = index+1;
		}
		childKeys[index] = block;
		setUpdated(true);
	}

	@Override
	public synchronized long getPageId(int index) {
		if(index >= numKeys)
			return -1L;
		return childKeys[index];
	}


	@Override
	public synchronized RootKeyPageInterface getPage(int index) throws IOException {
		if(childKeys[index] != -1L)
			return GlobalDBIO.getHMapChildRootPageFromPool(hMapMain.getIO(), childKeys[index]);
		return null;
	}

	@Override
	public synchronized boolean isUpdated() {
		return lbai.isUpdated();
	}

	@Override
	public synchronized void setUpdated(boolean updated) {
		// updated not optional for this mode, but may be in others
		lbai.setUpdated();
	}

	@Override
	public synchronized Datablock getDatablock() {
		return lbai.getBlk();
	}

	@Override
	public synchronized BlockAccessIndex getBlockAccessIndex() {
		return lbai;
	}

	@Override
	public synchronized int getNumKeys() {
		return (int) numKeys;
	}

	@Override
	public synchronized void setNumKeys(int numKeys) {
		this.numKeys = numKeys;
		setUpdated(true);
	}

	@Override
	public synchronized void setRootNode(BlockAccessIndex bai) throws IOException {
		lbai = bai;
	}
	 
	 @Override
	 public String toString() {
		 return String.format("%s %s%n", this.getClass().getName(),lbai);
	 }
}
