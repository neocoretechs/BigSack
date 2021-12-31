package com.neocoretechs.bigsack.io.pooled;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.ByteChannel;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;

import com.neocoretechs.bigsack.DBPhysicalConstants;

import com.neocoretechs.bigsack.btree.BTreeKeyPage;
import com.neocoretechs.bigsack.btree.BTreeMain;
import com.neocoretechs.bigsack.btree.BTreeRootKeyPage;

import com.neocoretechs.bigsack.hashmap.HMapChildRootKeyPage;
import com.neocoretechs.bigsack.hashmap.HMapKeyPage;
import com.neocoretechs.bigsack.hashmap.HMapMain;
import com.neocoretechs.bigsack.hashmap.HMapRootKeyPage;

import com.neocoretechs.bigsack.io.IoManagerInterface;
import com.neocoretechs.bigsack.io.MultithreadedIOManager;
import com.neocoretechs.bigsack.io.Optr;
import com.neocoretechs.bigsack.io.stream.CObjectInputStream;
import com.neocoretechs.bigsack.io.stream.DBInputStream;
import com.neocoretechs.bigsack.io.stream.DBOutputStream;
import com.neocoretechs.bigsack.io.stream.DirectByteArrayOutputStream;

import com.neocoretechs.bigsack.keyvaluepages.KeyValueMainInterface;
import com.neocoretechs.bigsack.keyvaluepages.NodeInterface;

/*
* Copyright (c) 1997,2003,2014 NeoCoreTechs
* All rights reserved.
* Redistribution and use in source and binary forms, with or without modification, 
* are permitted provided that the following conditions are met:
*
* Redistributions of source code must retain the above copyright notice, this list of
* conditions and the following disclaimer. 
* Redistributions in binary form must reproduce the above copyright notice, 
* this list of conditions and the following disclaimer in the documentation and/or
* other materials provided with the distribution. 
* Neither the name of NeoCoreTechs nor the names of its contributors may be 
* used to endorse or promote products derived from this software without specific prior written permission. 
* THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED 
* WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A 
* PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE LIABLE FOR 
* ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED
* TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
* HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT 
* (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED 
* OF THE POSSIBILITY OF SUCH DAMAGE.
*
*/
/**
* Global IO operations<br>
* Treats collections of tablespaces (files, etc) as cohesive IO units.  We utilize three caches.
* In our nomenclature, Level 1 cache is the deserialized in-memory tables of Objects.  Level
* 2 cache is the buffer pool of pages (blocks).  Level 3 cache is the memory mapped or filesystem file. 
* @author Jonathan Groff Copyright (C) NeoCoreTechs 1997,2020,2021
*/

public class GlobalDBIO {
	private static final boolean DEBUG = false;
	private static final boolean DEBUGDESERIALIZE = true; // called upon every deserialization
	private static final boolean DEBUGLOGINIT = false; // view blocks written to log and store
	private static final boolean NEWNODEPOSITIONDEBUG = false;;
	private int MAXBLOCKS = 1024; // PoolBlocks property may overwrite
	private int L3cache = 0; // Level 3 cache type, mmap, file, etc
	private String[][] nodePorts = null; // remote worker nodes and their ports, if present
	// Are we using custom class loader for serialized versions?
	private boolean isCustomClassLoader;
	private ClassLoader customClassLoader;
	
	private String dbName;
	private long transId;

	protected IoManagerInterface ioManager = null;// = new MultithreadedIOManager();, ClusterIOManager, etc.
	private KeyValueMainInterface keyValueMain = null;
	public IoManagerInterface getIOManager() {
		return ioManager;
	}
	
	/**
	* Translate the virtual block, composed of tablespace and physical block (first 3 bits/last 61 bits),
	* to a physical block by masking out the first 3 tablespace bits.
	* @param tvblock The virtual block of tablespace/physical block
	* @return The physical block in that tablespace
	*/
	public static long getBlock(long tvblock) {
		return tvblock & 0x1FFFFFFFFFFFFFFFL;
	}

	/**
	* Extract the tablespace from virtual block. Mask out last 61 bits and shift 61 right, leaving 0-7 tablespace value.
	* @param tvblock The virtual block
	* @return The tablespace for that block
	*/
	public static int getTablespace(long tvblock) {
		return (int) ((tvblock & 0xE000000000000000L) >>> 61);
	}

	/**
	* Make a vblock from real block and tablespace
	* @param tblsp The tablespace
	* @param tblk The block in that tablespace
	* @return The virtual block
	*/
	public static long makeVblock(int tblsp, long tblk) {
		return (((long) tblsp) << 61) | tblk;
	}

	/**
	 * Return a string representing a more friendly representation of virtual block
	 */
	public static String valueOf(long vblk) {
		String tsp = ("Tablespace_"+getTablespace(vblk)+"_"+String.valueOf(getBlock(vblk)));
		if( tsp.equals("Tablespace_7_2305843009213693951") ) // this is a -1, if tablespace 7 crashes your exabyte array you know you tried to write this
			tsp = "Empty";
		return tsp;
	}
	
	/**
	* Constructor will utilize values from props file to initialize 
	* global IO.  The backing store type indicates filesystem or memory map etc.<p/>
	* The number of buffer pool entries is controlled by PoolBlocks
	* @param dbname Fully qualified path of DB
	* @param keystoreType "BTree" or "HMap"
	* @param backingstoreType "MMap" or "File" etc
	* @param transId Transaction Id of current owner
	* @param poolBlocks Maximum blocks in bufffer pool
	* @exception IOException if open problem
	*/
	public GlobalDBIO(String dbname, String keystoreType, String backingstoreType, long transId, int poolBlocks) throws IOException {
		this.dbName = dbname;
		this.transId = transId;
		switch(keystoreType) {
			case "BTree":
				keyValueMain =  new BTreeMain(this);
				break;
			case "HMap":
				keyValueMain = new HMapMain(this);
				break;
			default:
				keyValueMain =  new BTreeMain(this);
				break;
		}
		// Set up the proper backing store
		switch(backingstoreType) {
			case "MMap":
				L3cache = 0;
				break;
			case "File":
				L3cache = 1;
				break;
			default:
				throw new IOException("Unsupported L3 cache type");
		}
		
		setMAXBLOCKS(poolBlocks);

		if( DEBUG )
			System.out.println("Multithreaded IO Manager coming up...");
		ioManager = new MultithreadedIOManager((GlobalDBIO) this, L3cache);
		
		ioManager.initialize();
	}

	/**
	* Get first tablespace
	* @return the position of the first byte of first tablespace
	*/
	public long firstTableSpace() throws IOException {
		return 0L;
	}
	/**
	* Get next tablespace, set to 0 position
	* @param prevSpace The previous tablespace to iterate from
	* @return The long virtual block if there was a next space else 0
	* @exception IOException if seek to new position fails
	*/
	public static long nextTableSpace(int prevSpace) throws IOException {
		int tempSpace = prevSpace;
		while (++tempSpace < DBPhysicalConstants.DTABLESPACES) {
				return makeVblock(tempSpace, 0L);
		}
		return 0L;
	}

	/**
	* @return DB name as String
	*/
	public String getDBName() {
		return dbName;
	}
	
	/**
	 * Applies to local log directory if remote dir differs
	 * @return
	 */
	public String getDBPath() {
		return (new File(dbName)).toPath().getParent().toString();
	}
	/**
	 * Return the properties file entries for the remote worker nodes in cluster mode, if they were present
	 * @return the nodes otherwise null if no entry in the properties file
	 */
	public synchronized String[][] getWorkerNodes() { return nodePorts; }
	
	/**
	* Static method for object to serialized byte conversion.
	* Uses DirectByteArrayOutputStream, which allows underlying buffer to be retrieved without
	* copying entire backing store
	* @param Ob the user object
	* @return byte buffer containing serialized data
	* @exception IOException cannot convert
	*/
	public static byte[] getObjectAsBytes(Object Ob) throws IOException {
		byte[] retbytes;
		DirectByteArrayOutputStream baos = new DirectByteArrayOutputStream();
		ObjectOutput s = new ObjectOutputStream(baos);
		s.writeObject(Ob);
		s.flush();
		baos.flush();
		retbytes = baos.getBuf();
		s.close();
		baos.close();
		//return retbytes;
		//---------------------
		// no joy below
		//WritableByteChannel wbc = Channels.newChannel(baos);
		//ObjectOutput s = new ObjectOutputStream(Channels.newOutputStream(wbc));
		//s.writeObject(Ob);
		//s.flush();
		//baos.flush();
		//ByteBuffer bb = ByteBuffer.allocate(baos.size());
		// try to get the bytes from the channel
		//wbc.write(bb);
		//retbytes = bb.array();
		//retbytes = baos.toByteArray();
		//s.close();
		//wbc.close();
		//baos.close();
		
		return retbytes;
	}
	/**
	* static method for serialized byte to object conversion
	* @param sdbio The BlockDBIO which may contain a custom class loader to use
	* @param obuf the byte buffer containing serialized data
	* @return Object instance
	* @exception IOException cannot convert
	*/
	public static Object deserializeObject(GlobalDBIO globalIO, byte[] obuf) throws IOException {
		Object Od;
		try {
			ObjectInputStream s;
			ByteArrayInputStream bais = new ByteArrayInputStream(obuf);
			ReadableByteChannel rbc = Channels.newChannel(bais);
			if(globalIO.isCustomClassLoader())
				s = new CObjectInputStream(Channels.newInputStream(rbc), globalIO.getCustomClassLoader());
			else
				s = new ObjectInputStream(Channels.newInputStream(rbc));
			Od = s.readObject();
			s.close();
			bais.close();
			rbc.close();
		} catch (IOException ioe) {
			throw new IOException(
				"deserializeObject: "
					+ ioe.toString()
					+ ": Class Unreadable, may have been modified beyond version compatibility: from buffer of length "
					+ obuf.length);
		} catch (ClassNotFoundException cnf) {
			throw new IOException(
				cnf.toString()
					+ ":Class Not found, may have been modified beyond version compatibility");
		}
		return Od;
	}

	/**
	* static method for serialized byte to object conversion
	* @param obuf the byte buffer containing serialized data
	* @return Object instance
	* @exception IOException cannot convert
	*/
	public static Object deserializeObject(byte[] obuf) throws IOException {
		Object Od;
		try {
			ObjectInputStream s;
			ByteArrayInputStream bais = new ByteArrayInputStream(obuf);
			ReadableByteChannel rbc = Channels.newChannel(bais);
			s = new ObjectInputStream(Channels.newInputStream(rbc));
			Od = s.readObject();
			s.close();
			bais.close();
			rbc.close();
		} catch (IOException ioe) {
			throw new IOException(
				"deserializeObject: "
					+ ioe.toString()
					+ ": Class Unreadable, may have been modified beyond version compatibility: from buffer of length "
					+ obuf.length);
		} catch (ClassNotFoundException cnf) {
			throw new IOException(
				cnf.toString()
					+ ":Class Not found, may have been modified beyond version compatibility");
		}
		return Od;
	}
	
	/**
	 * Deserialize an object from the provided InputStream
	 * @param is
	 * @return The magically reconstituted object
	 * @throws IOException
	 */
	public static Object deserializeObject(InputStream is) throws IOException {
		Object Od;
		try {
			ObjectInputStream s;
			ReadableByteChannel rbc = Channels.newChannel(is);
			s = new ObjectInputStream(Channels.newInputStream(rbc));
			Od = s.readObject();
			s.close();
			rbc.close();
		} catch (IOException ioe) {
			throw new IOException(
				"deserializeObject: "
					+ ioe.toString()
					+ ": Class Unreadable, may have been modified beyond version compatibility: from inputstream "
					+ is);
		} catch (ClassNotFoundException cnf) {
			throw new IOException(
				cnf.toString()
					+ ":Class Not found, may have been modified beyond version compatibility");
		}
		return Od;
	}
	/**
	 * Deserialize an object from the provided ByteChannel
	 * @param rbc
	 * @return The magically reconstituted object
	 * @throws IOException
	 */
	public static Object deserializeObject(ByteChannel rbc) throws IOException {
		Object Od;
		try {
			ObjectInputStream s;
			s = new ObjectInputStream(Channels.newInputStream(rbc));
			Od = s.readObject();
			s.close();
			rbc.close();
		} catch (IOException ioe) {
			throw new IOException(
				"deserializeObject: "
					+ ioe.toString()
					+ ": Class Unreadable, may have been modified beyond version compatibility: from ByteChannel "
					+ rbc);
		} catch (ClassNotFoundException cnf) {
			throw new IOException(
				cnf.toString()
					+ ":Class Not found, may have been modified beyond version compatibility: from ByteChannel");
		}
		return Od;
	}
	/**
	 * Deserialize an object from the provided ByteBuffer
	 * @param bb
	 * @return The magically reconstituted object
	 * @throws IOException
	 */
	public static Object deserializeObject(ByteBuffer bb) throws IOException {
		Object Od;
		try {
			assert( bb.arrayOffset() == 0 ) : "GlobalDBIO.deserializeObject ByteBuffer has bad array offset "+bb.arrayOffset();
			ObjectInputStream s;
			byte[] ba = bb.array();
			s = new ObjectInputStream(new ByteArrayInputStream(ba));
			Od = s.readObject();
			s.close();
		} catch (IOException ioe) {
			/*
	        FileChannel channel = new FileOutputStream(new File("dumpobj.ser"), false).getChannel();
	        channel.write(bb);
	        channel.close();
	        */
			throw new IOException(
				"deserializeObject: "
					+ ioe.toString()
					+ ": Class Unreadable, may have been modified beyond version compatibility: from ByteBuffer "
					+ bb);
		} catch (ClassNotFoundException cnf) {
			throw new IOException(
				cnf.toString()
					+ ":Class Not found, may have been modified beyond version compatibility: from ByteBuffer");
		}
		return Od;
	}

	/**
	 * Decrement the access count on a page pool resident block.
	 * @param bai
	 * @throws IOException
	 */
	protected static void dealloc(BlockAccessIndex bai) throws IOException {
		bai.decrementAccesses();
	}
	/**
	 * Flush the page pool buffer and commit the outstanding blocks
	 * @throws IOException
	 */
	public synchronized void commitBufferFlush() throws IOException {
		ioManager.commitBufferFlush();
	}
	
	/**
	 * Flush the page pool buffer and commit the outstanding blocks
	 * @throws IOException
	 * @throws IllegalAccessException 
	 */
	public synchronized void checkpointBufferFlush() throws IOException, IllegalAccessException {
		ioManager.checkpointBufferFlush();
	}
	/**
	 * Flush the page pool buffers and discard outstanding blocks.
	 */
	public synchronized void rollbackBufferFlush() {
		forceBufferClear();
	}
	/**
	* We'll do this on a 'clear' of collection, reset all caches
	* Take the used block list, reset the blocks, move to to free list, then
	* finally clear the used block list. We do this during rollback to remove any modified
	*/
	public synchronized void forceBufferClear() {
			ioManager.forceBufferClear();
	}
	/**
	 * Find the block at the desired vblock in the page pool or allocate the resources and
	 * bring block into pool from deep store. Eventually the call reached the {@link MappedBlockBuffer} for
	 * a particular tablespace and the block is processed setting its 'accesses' up by 1.
	 * @param pos The virtual block to find or add via call to {@link IoManagerInterfce}
	 * @throws IOException
	 */
	public synchronized BlockAccessIndex findOrAddBlock(long pos) throws IOException {
		return ioManager.findOrAddBlockAccess(pos);	
	}
	/**
	 * Deallocate the outstanding buffer resources, block latches, etc. for 
	 * without regard to transaction status..
	 * @throws IOException
	 */
	public synchronized void deallocOutstanding(DBInputStream blockStream) throws IOException {
		if(blockStream != null)
			ioManager.deallocOutstanding(blockStream.getBlockAccessIndex());	
	}
	/**
	 * Deallocate the outstanding buffer resources, block latches, etc. for 
	 * without regard to transaction status..
	 * @throws IOException
	 */
	public synchronized void deallocOutstanding(DBOutputStream blockStream) throws IOException {
		if(blockStream != null)
			ioManager.deallocOutstanding(blockStream.getBlockAccessIndex());	
	}
	/**
	 * Selects a tablespace to prepare call the ioManager
	 * {@link MultithreadedIOManager}, implementing {@link IoManagerInterface} to acquire the {@link MappedBlockBuffer} part
	 * of the {@link BufferPool}.<p/> 
	 * In order to acquire the {@link BlockAccessIndex} block, find the smallest tablespace free block list,
	 * and remove it for use by placing it into the {@link MappedBlockBuffer} BlockAccessIndex buffer for that tablespace.<p/>
	 * @return The BlockAccessIndex from the random tablespace freelist, with byte index set to 0
	 * @throws IOException
	 */
	public synchronized BlockAccessIndex stealblk() throws IOException {
		BlockAccessIndex blk = ioManager.getNextFreeBlock();
		if(DEBUG)
			System.out.printf("%s.stealblk got block %s%n", this.getClass().getName(),GlobalDBIO.valueOf(blk.getBlockNum()));
		return blk;
	}
	/**
	 * Deallocate the outstanding buffer resources, block latches, etc. for 
	 * the purpose of transaction checkpoint rollback.
	 * @throws IOException
	 */
	public synchronized void deallocOutstandingRollback(DBOutputStream blockStream) throws IOException {
		if(blockStream != null)
			ioManager.deallocOutstandingRollback(blockStream.getBlockAccessIndex());
	}
	/**
	 * Deallocate the outstanding buffer resources, block latches, etc. for 
	 * the purpose of transaction checkpoint commit.
	 * @throws IOException
	 */	
	public synchronized void deallocOutstandingCommit(DBOutputStream blockStream) throws IOException {
		if(blockStream != null)
			ioManager.deallocOutstandingCommit(blockStream.getBlockAccessIndex());
		if(DEBUGLOGINIT)
			System.out.printf("%s.deallocOutstandingCommit%n", this.getClass().getName());
	}
	
	/**
	* Create initial buckets. As blocks are allocated, they are placed on the free block chain.<p/>
	* We first scan the existing allocation and determine the extent of free blocks there as our initial extent
	* may encompass the entirety of the free tablespace.<p/>
	* @param freeBlockList The free block list to recieve the newly created buckets, per tablespace, so physical block is used.
	* @exception IOException if buckets cannot be created
	*/
	public synchronized void createBuckets(int ispace, LinkedHashMap<Long, BlockAccessIndex> freeBlockList, boolean isNew) throws IOException {
		long xsize = 0L;
		if(!isNew) {
			xsize = ioManager.Fsize(ispace);
			if(DEBUG)
				System.out.printf("%s.createBuckets extending tablespace:%d from:%d to %d. free block list size=%d%n",this.getClass().getName(),ispace,xsize,(xsize+(DBPhysicalConstants.DBUCKETS*DBPhysicalConstants.DBLOCKSIZ)),freeBlockList.size());
			ioManager.extend(ispace, xsize+(DBPhysicalConstants.DBUCKETS*DBPhysicalConstants.DBLOCKSIZ));
		}
		// write bucket blocks
		for (int i = 0; i < DBPhysicalConstants.DBUCKETS; i++) {
			long vblock = makeVblock(ispace, xsize);
			Datablock d = new Datablock(DBPhysicalConstants.DATASIZE);
			d.resetBlock();
			ioManager.FseekAndWriteFully(vblock, d);
			BlockAccessIndex bai = new BlockAccessIndex(this, vblock, d);
			freeBlockList.put(vblock, bai);
			xsize += (long) DBPhysicalConstants.DBLOCKSIZ;
		}
	}
	/**
	 * Used for direct access to deep store.
	 * The header data and used bytes are read into the specified (@link Datablock}
	 * @param toffset
	 * @param tblk
	 * @throws IOException
	 */
	public synchronized void FseekAndRead(long toffset, Datablock tblk) throws IOException {
		if( DEBUG )
			System.out.printf("%s.FseekAndRead offset:%s%n",this.getClass().getName(),valueOf(toffset));
		// send write command and queues be damned, writes only happen
		// immediately after log writes
		ioManager.FseekAndRead(toffset, tblk);
		//if( Props.DEBUG ) System.out.print("GlobalDBIO.FseekAndWriteFully:"+valueOf(toffset)+" "+tblk.toVblockBriefString()+"|");
	}	
	/**
	 * Utility write. Immediately after log write, we come here to make sure header of the block
	 * is written back to main store. We do not use the queues as this operation
	 * is always adjunct to a processed queue request. We go through the {@link IoManagerInterface} 
	 * which translates the tablespace and block writes always perform Fforce after write.
	 * @param toffset The virtual block to write
	 * @param tblk the datablock with header payload
	 * @throws IOException
	 */
	public synchronized void FseekAndWrite(long toffset, Datablock tblk) throws IOException {
		if( DEBUG )
			System.out.printf("%s.FseekAndWrite offset:%s Datablock blockdump:%s%n",this.getClass().getName(),valueOf(toffset),tblk.blockdump());
		// send write command and queues be damned, writes only happen
		// immediately after log writes
		ioManager.FseekAndWrite(toffset, tblk);
		//if( Props.DEBUG ) System.out.print("GlobalDBIO.FseekAndWriteFully:"+valueOf(toffset)+" "+tblk.toVblockBriefString()+"|");
	}
	/**
	 * Utility header write for log commit maintenance.
	 * @param toffset
	 * @param tblk
	 * @throws IOException
	 
	public synchronized void FseekAndWriteHeader(long toffset, Datablock tblk) throws IOException {
		if( DEBUG )
			System.out.printf("%s.FseekAndWriteHeader offset:%s Datablock blockdump:%s%n",this.getClass().getName(),valueOf(toffset),tblk.blockdump());
		// send write command and queues be damned, writes only happen
		// immediately after log writes
		ioManager.FseekAndWriteHeader(toffset, tblk);
		//if( Props.DEBUG ) System.out.print("GlobalDBIO.FseekAndWriteFully:"+valueOf(toffset)+" "+tblk.toVblockBriefString()+"|");
	}
	*/
	/**
	 * Used for direct access to deep store for direct operations such as resetting inlog after commit.
	 * @param toffset
	 * @param tblk
	 * @throws IOException
	 
	public void FseekAndReadHeader(long toffset, Datablock tblk) throws IOException {
		if( DEBUG )
			System.out.printf("%s.FseekAndReadHeader offset:%s%n",this.getClass().getName(),valueOf(toffset));
		// send write command and queues be damned, writes only happen
		// immediately after log writes
		ioManager.FseekAndReadHeader(toffset, tblk);	
	}
	*/
	/**
	 * 
	 * @return main HMapMain, BTreeMain or other main interface to keystore implementation
	 */
	public KeyValueMainInterface getKeyValueMain() {
			return keyValueMain;
	}
	/**
	* Get a page from the buffer pool based on the specified location.<p/>
	* No effort is made to guarantee the record being accessed is a viable KeyPageInterface, that is assumed.
	* The KeyPageInterface constructor is called with option true to read the page upon access.
	* @param pos The block containing page
	* @return The KeyPageInterface page instance, which also contains a reference to the BlockAccessIndex and BTreeMain
	* @exception IOException If retrieval fails
	*/
	public BTreeKeyPage getBTreePageFromPool(long pos) throws IOException {
		//assert(pos != -1L) : "Page index invalid in getPage "+getDBName();
		BlockAccessIndex bai;
		if(pos == -1L) {
			bai = stealblk();
		} else {
			bai = findOrAddBlock(pos);
		}
		BTreeKeyPage btk = new BTreeKeyPage(getKeyValueMain(), bai, true);
		if( DEBUG ) 
			System.out.printf("getBtreePageFromPool KeyPageInterface:%s BlockAccessIndex:%s%n",btk,bai);
		//for(int i = 0; i <= MAXKEYS; i++) {
		//	btk.pageArray[i] = btk.getPage(this,i);
		//}
		return btk;
	}
	/**
	 * Get a new page from the pool from round robin tablespace.
	 * Call stealblk, create KeyPageInterface with the page Id of stolen block.
	 * KeyPageInterface constructor is called with option false (no read), set up for new block instead.
	 * This will set the updated flag in the block since the block is new,
	 * @param btnode The new node
	 * @return The KeyPageInterface page instance, which also contains a reference to the BlockAccessIndex and new node
	 * @throws IOException
	 */
	public BTreeKeyPage getBTreePageFromPool(NodeInterface btnode) throws IOException {
		// Get a fresh block if necessary
		BlockAccessIndex lbai = null;
		if(btnode.getPageId() == -1L) {
			lbai = stealblk();
			btnode.setPageId(lbai.getBlockNum());
		} else {
			lbai = findOrAddBlock(btnode.getPageId());
		}
		// initialize transients, set page with this block, false=no read, set up for new block instead
		// this will set updated since the block is new
		BTreeKeyPage btk = new BTreeKeyPage(getKeyValueMain(), lbai, true);
		btk.setNode(btnode);
		if( DEBUG ) 
			System.out.printf("getBTreePageFromPool KeyPageInterface:%s BlockAccessIndex:%s%n",btk,lbai);
		return btk;
	}
	
	public BTreeRootKeyPage getBTreeRootPageFromPool() throws IOException {
		BlockAccessIndex bai = findOrAddBlock(0L);
		BTreeRootKeyPage btk = new BTreeRootKeyPage(getKeyValueMain(), bai, true);
		if( DEBUG ) 
			System.out.printf("getBTreeRootPageFromPool KeyPageInterface:%s BlockAccessIndex:%s%n",btk,bai);
		return btk;
	}
	/**
	* Get a page from the buffer pool based on the specified location.<p/>
	* No effort is made to guarantee the record being accessed is a viable KeyPageInterface, that is assumed.
	* The KeyPageInterface constructor is called with option true to read the page upon access.
	* @param sdbio The BufferPool io instance
	* @param pos The block containing page
	* @return The KeyPageInterface page instance, which also contains a reference to the BlockAccessIndex and BTreeMain
	* @exception IOException If retrieval fails
	*/
	public HMapKeyPage getHMapPageFromPool(long pos) throws IOException {
		//assert(pos != -1L) : "Page index invalid in getPage "+sdbio.getDBName();
		BlockAccessIndex bai;
		if(pos == -1L) {
			bai = stealblk();
		} else {
			bai = findOrAddBlock(pos);
		}
		HMapKeyPage btk = new HMapKeyPage((HMapMain) getKeyValueMain(), bai, true);
		if( DEBUG ) 
			System.out.printf("getHMapPageFromPool KeyPageInterface:%s BlockAccessIndex:%s%n",btk,bai);
		return btk;
	}
	
	/**
	* Determine location of new node.
	* Attempts to cluster entries in used blocks near insertion point relative to other entries.
	* Choose a random tablespace, then find a key that has that tablespace, then cluster there.
	* @param locs The array of page pointers of existing entries to check for block space
	* @param index The index of the target in array, such that we dont check against that entry
	* @param nkeys The total keys in use to check in array
	* @param bytesneeded The number of bytes to write
	* @return The Optr pointing to the new node position
	* @exception IOException If we cannot get block for new node
	*/
	public synchronized Optr getNewInsertPosition(ArrayList<Long> locs, int bytesNeeded) throws IOException {
		long blockNum = -1L;
		BlockAccessIndex ablk = null;
		short bytesUsed = 0; 
		for(int i = 0; i < locs.size(); i++) {
			ablk = findOrAddBlock(locs.get(i));
			short bytesAvailable = (short) (DBPhysicalConstants.DATASIZE - ablk.getBlk().getBytesused());
			if( bytesAvailable >= bytesNeeded || bytesAvailable == DBPhysicalConstants.DATASIZE) {
				// eligible
				blockNum = ablk.getBlockNum();
				bytesUsed = ablk.getBlk().getBytesused();
				break;
			}
			ablk.decrementAccesses();
		}
		boolean stolen = false;
		// come up empty?
		if (blockNum == -1L) {
			ablk = stealblk();
			blockNum = ablk.getBlockNum();
			bytesUsed = ablk.getBlk().getBytesused();
			stolen = true;
		}
		if( NEWNODEPOSITIONDEBUG )
			System.out.println("MappedBlockBuffer.getNewNodePosition "+GlobalDBIO.valueOf(blockNum)+" Used bytes:"+bytesUsed+" stolen:"+stolen+" locs:"+Arrays.toString(locs.toArray()));
		return new Optr(blockNum, bytesUsed);
	}

	/**
	 * Have a node, need to get a page. <p/>
	 * Get a new page from the pool from round robin tablespace.
	 * Call stealblk, create KeyPageInterface with the page Id of stolen block.
	 * KeyPageInterface constructor is called with option false (no read), set up for new block instead.
	 * This will set the updated flag in the block since the block is new,
	 * @param btnode The new node
	 * @return The KeyPageInterface page instance, which also contains a reference to the BlockAccessIndex and new node
	 * @throws IOException
	 */
	public HMapKeyPage getHMapPageFromPool(NodeInterface btnode) throws IOException {
		// Get a fresh block if necessary
		BlockAccessIndex lbai = null;
		if(btnode.getPageId() == -1L) {
			lbai = stealblk();
			btnode.setPageId(lbai.getBlockNum());
		} else {
			lbai = findOrAddBlock(btnode.getPageId());
		}
		// initialize transients, set page with this block, false=no read, set up for new block instead
		// this will set updated since the block is new
		HMapKeyPage btk = new HMapKeyPage((HMapMain) getKeyValueMain(), lbai, true);
		btk.setNode(btnode);
		if( DEBUG ) 
			System.out.printf("getHMapPageFromPool KeyPageInterface:%s BlockAccessIndex:%s%n",btk,lbai);
		return btk;
	}
	/**
	* Get the first Root page from the buffer pool from 0 block of tablespace.<p/>
	* No effort is made to guarantee the record being accessed is a viable KeyPageInterface, that is assumed.
	* The RootKeyPageInterface constructor is called with option true to read the page upon access.
	* @param tablespace The tablespace
	* @return The KeyPageInterface page instance, which also contains a reference to the BlockAccessIndex and BTreeMain
	* @exception IOException If retrieval fails
	*/
	public HMapRootKeyPage getHMapRootPageFromPool(int tablespace) throws IOException {
		long pos = GlobalDBIO.makeVblock(tablespace, 0L);
		BlockAccessIndex bai = findOrAddBlock(pos);
		HMapRootKeyPage btk = new HMapRootKeyPage((HMapMain) getKeyValueMain(), bai, true);
		if( DEBUG ) 
			System.out.printf("getHMapRootPageFromPool KeyPageInterface:%s BlockAccessIndex:%s%n",btk,bai);
		return btk;
	}
	/**
	 * Get a child of the root HMap page from the pool. Upon acquiring a new page we have to prepare it by setting the contents
	 * so the page pointers come back as -1 so that as we fan out our key space the intermediary pointers read as empty.<p/>
	 * This is because our numKeys is essentially a high water mark.
	 * @param pos block number, if -1, acquire block from freechain
	 * @return The HMapChildRootKeyPage as derived from the BlockAccessIndex.
	 * @throws IOException
	 */
	public HMapChildRootKeyPage getHMapChildRootPageFromPool(long pos) throws IOException {
		// Get a fresh block if necessary
		BlockAccessIndex lbai = null;
		if(pos == -1L) {
			lbai = stealblk();
		} else {
			lbai = findOrAddBlock(pos);
		}
		// initialize transients, set page with this block, false=no read, set up for new block instead
		// this will set updated since the block is new
		HMapChildRootKeyPage btk = new HMapChildRootKeyPage((HMapMain)getKeyValueMain(), lbai, true);
		if( DEBUG ) 
			System.out.printf("%s getHMapChildPageFromPool KeyPageInterface:%s BlockAccessIndex:%s%n",BlockAccessIndex.class.getName(), btk,lbai);
		return btk;
	}
	
	public static DataOutputStream getDataOutputStream(BlockAccessIndex bai) throws IOException {
		DBOutputStream bks = new DBOutputStream(bai, bai.getSdbio().getIOManager().getBlockBuffer(GlobalDBIO.getTablespace(bai.getBlockNum())));
		bks.setBlockAccessIndex(bai); // sets byteindex to 0
		return bks.getDBOutput();
	}
	
	public static DataInputStream getDataInputStream(BlockAccessIndex bai) throws IOException {
		DBInputStream bks = new DBInputStream(bai, bai.getSdbio().getIOManager().getBlockBuffer(GlobalDBIO.getTablespace(bai.getBlockNum())));
		bks.setBlockAccessIndex(bai); // sets byteindex to 0
		return bks.getDBInput();
	}
	
	public static DataOutputStream getDataOutputStream(BlockAccessIndex bai, short byteindex) throws IOException {
		DBOutputStream bks = new DBOutputStream(bai, bai.getSdbio().getIOManager().getBlockBuffer(GlobalDBIO.getTablespace(bai.getBlockNum())));
		bks.setBlockAccessIndex(bai, byteindex);
		return bks.getDBOutput();
	}
	
	public static DataInputStream getDataInputStream(BlockAccessIndex bai, short byteindex) throws IOException {
		DBInputStream bks = new DBInputStream(bai, bai.getSdbio().getIOManager().getBlockBuffer(GlobalDBIO.getTablespace(bai.getBlockNum())));
		bks.setBlockAccessIndex(bai, byteindex);
		return bks.getDBInput();
	}
	
	public static DBOutputStream getBlockOutputStream(BlockAccessIndex bai) throws IOException {
		DBOutputStream bks = new DBOutputStream(bai, bai.getSdbio().getIOManager().getBlockBuffer(GlobalDBIO.getTablespace(bai.getBlockNum())));
		bks.setBlockAccessIndex(bai); // sets byteindex to 0
		return bks;
	}
	
	public static DBInputStream getBlockInputStream(BlockAccessIndex bai) throws IOException {
		DBInputStream bks = new DBInputStream(bai, bai.getSdbio().getIOManager().getBlockBuffer(GlobalDBIO.getTablespace(bai.getBlockNum())));
		bks.setBlockAccessIndex(bai); // sets byteindex to 0
		return bks;
	}
	
	public static DBOutputStream getBlockOutputStream(BlockAccessIndex bai, short byteindex) throws IOException {
		DBOutputStream bks = new DBOutputStream(bai, bai.getSdbio().getIOManager().getBlockBuffer(GlobalDBIO.getTablespace(bai.getBlockNum())));
		bks.setBlockAccessIndex(bai, byteindex);
		return bks;
	}
	
	public static DBInputStream getBlockInputStream(BlockAccessIndex bai, short byteindex) throws IOException {
		DBInputStream bks = new DBInputStream(bai, bai.getSdbio().getIOManager().getBlockBuffer(GlobalDBIO.getTablespace(bai.getBlockNum())));
		bks.setBlockAccessIndex(bai, byteindex);
		return bks;
	}
	
	/**
	* delete an object at the given block and offset
	*/
	public static void deleteFromOptr(GlobalDBIO sdbio, DBOutputStream blockStream, Optr pos, Object target) throws IOException {
		if(pos == null || pos == Optr.emptyPointer) 
			throw new IOException("Object index "+pos+" invalid in deleteFromOptr for db:"+sdbio.getDBName()+" for key:"+target);
		byte[] b = GlobalDBIO.getObjectAsBytes(target);
		sdbio.delete_object(blockStream, pos, b.length);
	}
	/**
	* delete_object and potentially reclaim space
	* @param loc Location of object
	* @param osize object size
	* @exception IOException if the block cannot be sought or written
	*/
	public synchronized void delete_object(DBOutputStream blockStream, Optr loc, int osize) throws IOException {
		//System.out.println("GlobalDBIO.delete_object "+loc+" "+osize);
		ioManager.objseek(blockStream, loc);
		blockStream.deleten(blockStream.getBlockAccessIndex(), osize);
	}
		
	/**
	 * Add an object, which in this case is a load of bytes.
	* @param loc Location to add this
	* @param o The byte payload to add to pool
	* @param osize  The size of the payload to add from array
	* @exception IOException If the adding did not happen
	*/
	public synchronized void add_object(DBOutputStream blockStream, Optr loc, byte[] o, int osize) throws IOException {
		int tblsp = ioManager.objseek(blockStream, loc);
		//assert(ioManager.getBlockStream(tblsp).getLbai().getAccesses() > 0 ) : "Writing unlatched block:"+loc+" with payload:"+osize;
		blockStream.writen(blockStream.getBlockAccessIndex(), o, osize);
		//assert(ioManager.getBlockStream(tblsp).getLbai().getAccesses() > 0 && 
		//	   ioManager.getBlockStream(tblsp).getLbai().getBlk().isIncore()) : 
		//	"Block "+loc+" unlatched after write, accesses: "+ioManager.getBlockStream(tblsp).getLbai().getAccesses();
			//ioManager.deallocOutstandingWriteLog(tblsp);
	}

	/**
	* Read Object in pool: deserialize the byte array.
	* @param iloc The location of the object to retrieve from backing store
	* @return The Object extracted from the backing store
	* @exception IOException if the op fails
	*/
	public synchronized Object deserializeObject(long iloc) throws IOException {
		// read Object at ptr to byte array
		if(DEBUG)
			System.out.print(" Deserialize "+GlobalDBIO.valueOf(iloc));
		Object Od = null;
		try {
			ObjectInput s;
			if (isCustomClassLoader())
				s =	new CObjectInputStream(
						GlobalDBIO.getBlockInputStream(ioManager.findOrAddBlockAccess(iloc)),
						getCustomClassLoader());
			else
				s = new ObjectInputStream(GlobalDBIO.getBlockInputStream(ioManager.findOrAddBlockAccess(iloc)));
			Od = s.readObject();
			s.close();
		} catch (IOException | ClassNotFoundException ioe) {
			throw new IOException(
				"deserializeObject from long: "
					+ ioe.toString()
					+ ": Class Unreadable, may have been modified beyond version compatibility "
					+ GlobalDBIO.valueOf(iloc)+" in "+getDBName());
		} 
		if( DEBUG ) System.out.println("From long "+GlobalDBIO.valueOf(iloc)+" Deserialized:\r\n "+Od);
		return Od;
	}
	/**
	* Read Object in pool: deserialize the byte array.
	* @param sdbio the session database IO object from which we get our DBInput stream and perhaps custom class loader
	* @param iloc The location of the object
	* @return the Object from dir. entry ptr.
	* @exception IOException if the op fails
	*/
	public synchronized Object deserializeObject(Optr iloc) throws IOException {
			// read Object at ptr to byte array
		Object Od;
		if(DEBUGDESERIALIZE)
			System.out.print(" Deserialize "+iloc);
		try {
			ObjectInput s;
			//ioManager.objseek(iloc);
			if (isCustomClassLoader())
				s =	new CObjectInputStream(GlobalDBIO.getBlockInputStream(ioManager.findOrAddBlockAccess(iloc.getBlock()), iloc.getOffset()), getCustomClassLoader());
			else
				s = new ObjectInputStream(GlobalDBIO.getBlockInputStream(ioManager.findOrAddBlockAccess(iloc.getBlock()), iloc.getOffset()));
			Od = s.readObject();
			s.close();
		} catch (IOException | ClassNotFoundException ioe) {
			throw new IOException(
				"deserializeObject from pointer: "
					+ ioe.toString()
					+ ": Class Unreadable, may have been modified beyond version compatibility "
					+ iloc+" in "+getDBName());
		}
		if( DEBUG ) System.out.println("From ptr "+iloc+" Deserialized:\r\n "+Od);
		return Od;
	}
		
	public synchronized boolean isCustomClassLoader() {
		return isCustomClassLoader;
	}

	public synchronized void setCustomClassLoader(boolean isCustomClassLoader) {
		this.isCustomClassLoader = isCustomClassLoader;
	}

	public synchronized ClassLoader getCustomClassLoader() {
		return customClassLoader;
	}

	public synchronized void setCustomClassLoader(ClassLoader customClassLoader) {
		this.customClassLoader = customClassLoader;
	}
		

	public long getTransId() {
		return transId;
	}

	public int getMAXBLOCKS() {
		return MAXBLOCKS;
	}

	public void setMAXBLOCKS(int mAXBLOCKS) {
		MAXBLOCKS = mAXBLOCKS;
	}

	public void updateDeepStoreInLog(DBOutputStream blockStream, long tblock, boolean b) throws IOException {
		ioManager.objseek(blockStream, tblock, Datablock.INLOGFLAGPOSITION);
		blockStream.writen(blockStream.getBlockAccessIndex(), new byte[] { (byte) (b ? 1 : 0)}, 1);
	}

	
}
