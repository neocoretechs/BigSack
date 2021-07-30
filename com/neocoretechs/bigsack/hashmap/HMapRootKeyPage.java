package com.neocoretechs.bigsack.hashmap;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Arrays;

import com.neocoretechs.bigsack.io.Optr;
import com.neocoretechs.bigsack.io.pooled.BlockAccessIndex;
import com.neocoretechs.bigsack.io.pooled.Datablock;
import com.neocoretechs.bigsack.io.pooled.GlobalDBIO;
import com.neocoretechs.bigsack.io.stream.PageIteratorIF;
import com.neocoretechs.bigsack.keyvaluepages.ChildRootKeyPageInterface;
import com.neocoretechs.bigsack.keyvaluepages.KVIteratorIF;
import com.neocoretechs.bigsack.keyvaluepages.KeyPageInterface;
import com.neocoretechs.bigsack.keyvaluepages.KeySearchResult;
import com.neocoretechs.bigsack.keyvaluepages.KeyValue;
import com.neocoretechs.bigsack.keyvaluepages.KeyValueMainInterface;
import com.neocoretechs.bigsack.keyvaluepages.NodeInterface;
import com.neocoretechs.bigsack.keyvaluepages.RootKeyPageInterface;
/**
 * The {@link KeyPageInterface} {@link HMapKeyPage} instance containing the {@link BlockAccessIndex}
 * is the bottom collision space page with actual k/v entries of {@link KeyValue} in the {@link HTNode} instance.
 * The haskey is a 32 bit integer which includes the sign bit, so for our keyspace fanout the tablespaces 
 * comprise the first 3 bits of the key, the rootPage field has initial page with 2 bit keys, 
 * then the childPages array has the 3 pages with 9 bit LSB keys.<p/>
 * The page layout is as follows:<br/>
 * tablespace = hashkeys[0] 3 bits <br/>
 * rootKeys[0-MAXKEYSROOT] ->  rootpage = hashkeys[1] 2 bits <br/>
 * [childPages[0].hashkeys[2]].pageId -> childPages[1] =  hashkeys[2] 9 bits <br/>
 * [childPages[1].hashkeys[3]].pageId -> childPages[2] =  hashkeys[3] 9 bits <br/>
 * [childPages[2].hashkeys[4]].pageId -> keyvaluespage =  hashkeys[4] 9 bits <br/>
 * key/values page.nextPage -> linked list of collision space key pages<br/>
 * @author Jonathan Groff (C) NeoCoreTechs 2021
 *
 */
public class HMapRootKeyPage implements RootKeyPageInterface {
	private boolean DEBUG = true;
	protected long numKeys; // number of keys currently utilized
	public final static int MAXKEYSROOT = 4; // 2 bits (0-3) times 8 bytes long page pointer
	protected BlockAccessIndex rootPage; // initial root page with 2 bit keys
	private long rootKeys[] = new long[MAXKEYSROOT]; // buffers the contents of root page

	protected KeyValueMainInterface hMapMain;
	/**
	 * The root page contains MAXKEYSROOT pointers to childPages, and there is a collection of childPages for each pointer. 
	 * @param hMap
	 * @param lbai
	 * @param read
	 * @throws IOException
	 */
	public HMapRootKeyPage(HMapMain hMap, BlockAccessIndex lbai, boolean read) throws IOException {
		this.hMapMain = hMap;
		this.rootPage = lbai;
		for(int i = 0; i < MAXKEYSROOT; i++)
			rootKeys[i] = -1L;
		if(read) {	
			readFromDBStream(GlobalDBIO.getBlockInputStream(this.rootPage));
		}
	}
	/**
	 * Read the page using the given DataInputStream
	 * @throws IOException
	 */
	public synchronized void readFromDBStream(DataInputStream dis) throws IOException {
		// check for fresh database
		if(dis.available() < 8)
			return;
		numKeys = dis.readLong(); // size
		if(DEBUG)
			System.out.printf("%s.readFromDBStream read numKeys=%d %s%n",this.getClass().getName(),numKeys,rootPage);
		for(int i = 0; i < numKeys; i++) {
			rootKeys[i] = dis.readLong();	
		}
		if(DEBUG)
			System.out.printf("%s.readFromDBStream read %s%n",this.getClass().getName(),this);
	}
	
	@Override
	/**
	 * Called from HmapMain.createRootNode when there is symmetry between the root node and a key page
	 * and we are using a NodeInterface to re-create the root from a new key, such as a in a BTree.
	 * @param bai The BlockAccessIndex to use to create the new root node
	 * @throws IOException
	 */
	public synchronized void setRootNode(BlockAccessIndex bai) throws IOException {
		this.rootPage = bai;
		DataOutputStream bs = GlobalDBIO.getBlockOutputStream(this.rootPage);
		bs.writeLong(0L); // size
		bs.flush();
		bs.close();
		this.rootPage.setUpdated();
	}
	
	/**
	 * Return total HashMap size
	 * @return
	 */
	public synchronized long getSize() {
		return numKeys;
	}
	/**
	 * Set total HashMap size
	 * @param size
	 */
	public synchronized void setSize(long size) {
		this.numKeys = size;
	}
	

	@Override
	public synchronized void putPage() throws IOException {
		DataOutputStream dos = GlobalDBIO.getBlockOutputStream(rootPage);
		dos.writeLong(MAXKEYSROOT);
		for(int i = 0; i < MAXKEYSROOT; i++) {
			dos.writeLong(rootKeys[i]);
		}
		dos.flush();
		dos.close();
		setUpdated(true);
	} 
	
	@Override
	public synchronized long getPageId() {
		return rootPage.getBlockNum();
	}

	@Override
	public synchronized void setPageIdArray(int index, long block, boolean update) throws IOException {
		if(index >= numKeys) {
			numKeys = index+1;
		}
		rootKeys[index] = block;
		setUpdated(update);
	}

	@Override
	public synchronized long getPageId(int index) {
		return rootKeys[index];
	}


	@Override
	public synchronized RootKeyPageInterface getPage(int index) throws IOException {
		return GlobalDBIO.getHMapChildRootPageFromPool(hMapMain.getIO(),rootKeys[index]);
	}

	@Override
	public synchronized boolean isUpdated() {
		return rootPage.isUpdated();
	}

	@Override
	public synchronized void setUpdated(boolean updated) {
		// updated not optional for this mode, but may be in others
		rootPage.setUpdated();	
	}

	@Override
	public synchronized Datablock getDatablock() {
		return rootPage.getBlk();
	}

	@Override
	public synchronized BlockAccessIndex getBlockAccessIndex() {
		return rootPage;
	}

	@Override
	public synchronized int getNumKeys() {
		return (int) numKeys;
	}

	@Override
	public synchronized void setNumKeys(int numKeys) throws IOException {
		this.numKeys = numKeys;
		setUpdated(true);	
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append(this.getClass().getName());
		sb.append(" ");
		sb.append(rootPage);
		sb.append(" ");
		sb.append("numKeys reported=");
		sb.append(numKeys);
		sb.append(" all keys=");
		for(int i = 0; i < MAXKEYSROOT; i++) {
			sb.append(i);
			sb.append("=");
			sb.append(GlobalDBIO.valueOf(rootKeys[i]));
			sb.append(",");
		}
		sb.append("\r\n");
		return sb.toString();
	}

}
