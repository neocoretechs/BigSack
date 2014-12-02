package com.neocoretechs.bigsack.io.pooled;
import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.*;
import java.util.*;

import com.neocoretechs.bigsack.DBPhysicalConstants;
import com.neocoretechs.bigsack.Props;
import com.neocoretechs.bigsack.btree.BTreeKeyPage;
import com.neocoretechs.bigsack.io.FileIO;
import com.neocoretechs.bigsack.io.IOWorker;
import com.neocoretechs.bigsack.io.IoInterface;
import com.neocoretechs.bigsack.io.MmapIO;
import com.neocoretechs.bigsack.io.MultithreadedIOManager;
import com.neocoretechs.bigsack.io.RecoveryLog;
import com.neocoretechs.bigsack.io.ThreadPoolManager;
import com.neocoretechs.bigsack.io.stream.CObjectInputStream;
import com.neocoretechs.bigsack.io.stream.DirectByteArrayOutputStream;
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
* @author Groff
*/

public class GlobalDBIO {
	private static final boolean DEBUG = true;
	private String Name;
	private long transId;
	protected boolean isNew = false; // if we create and no data yet
	private MultithreadedIOManager ioManager = new MultithreadedIOManager();
	private MappedBlockBuffer  usedBL = new MappedBlockBuffer(); // block number to Datablock
	private Vector<BlockAccessIndex> freeBL = new Vector<BlockAccessIndex>(); // free blocks
	private BlockAccessIndex tmpBai = new BlockAccessIndex(); // general utility
	private BlockAccessIndex tbai = new BlockAccessIndex(); // getUsedBlock reserved
	private RecoveryLog ulog;		
	private long new_node_pos_blk = -1L;
	private int L3cache = 0; // Level 3 cache type, mmap, file, etc
	static int MAXBLOCKS = 1024; // PoolBlocks property may overwrite

	public RecoveryLog getUlog() {
		return ulog;
	}
	
	public MultithreadedIOManager getIOManager() {
		return ioManager;
	}
	
	public void checkBufferFlush() throws IOException {
		usedBL.checkBufferFlush(this, freeBL);
	}

	public void commitBufferFlush() throws IOException {
		usedBL.commitBufferFlush(this, freeBL);
	}

	public void rollbackBufferFlush() {
		forceBufferClear();
	}
	public void forceBufferWrite() throws IOException {
		usedBL.directBufferWrite(this);
	}
	/**
	* Translate the virtual tablspace (first 3 bits) and block to real block
	* @param tvblock The virtual block
	* @return The actual block for that tablespace
	*/
	public static long getBlock(long tvblock) {
		return tvblock & 0x1FFFFFFFFFFFFFFFL;
	}

	/**
	* Extract the tablespace from virtual block
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
	* global IO.  The level 3 cache type (L3Cache) can currently be MMap
	* or File.  The number of buffer pool entries is controlled by the PoolBlocks
	* property. 
	* @param dbname Fully qualified path of DB
	* @param create true to create the database if it does not exist
	* @param transId Transaction Id of current owner
	* @exception IOException if open problem
	*/
	public GlobalDBIO(String dbname, boolean create, long transId) throws IOException {
		this.transId = transId;
		Name = dbname;
		if (Props.toString("L3Cache").equals("MMap"))
			L3cache = 0;
		else if (Props.toString("L3Cache").equals("File"))
				L3cache = 1;
			else
				throw new IOException("Unsupported L3 cache type");
		//
		Fopen(Name, create);

		// MAXBLOCKS may be set by PoolBlocks property
		MAXBLOCKS = Props.toInt("PoolBlocks");
		// populate with blocks, they're all free for now
		for (int i = 0; i < MAXBLOCKS; i++) {
			freeBL.addElement(new BlockAccessIndex(this));
		}

		if (create && isnew()) {
			isNew = true;
			// init the Datablock arrays, create freepool
			createBuckets();
			ioManager.setNextFreeBlocks();
		} 

	}
	
	private boolean isnew() {
		return ioManager.isNew();
	}

	/**
	* Constructor creates DB if not existing, otherwise open
	* @param dbname Fully qualified path or URL of DB
	* @param transId
	* @exception IOException if IO problem
	*/
	public GlobalDBIO(String dbname, long transId) throws IOException {
		this(dbname, true, transId);
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
	public long nextTableSpace(int prevSpace) throws IOException {
		int tempSpace = prevSpace;
		while (++tempSpace < DBPhysicalConstants.DTABLESPACES) {
				return makeVblock(tempSpace, 0L);
		}
		return 0L;
	}

	/**
	* If create is true, create only primary tablespace
	* else try to open all existing
	* We are spinning threads in the default executor group composed of an IoWorker for each
	* tablespace
	* @param fname String file name
	* @param create true to create if not existing
	* @exception IOException if problems opening/creating
	* @return true if successful
	* @see IoInterface
	*/
	boolean Fopen(String fname, boolean create) throws IOException {
		return ioManager.Fopen(fname, L3cache, create);
	}
	
 	void Fopen() throws IOException {
 		ioManager.Fopen();
	}

	/**
	* @return DB name as String
	*/
	public String getDBName() {
		return Name;
	}


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
	public static Object deserializeObject(ObjectDBIO sdbio, byte[] obuf) throws IOException {
		Object Od;
		try {
			ObjectInputStream s;
			ByteArrayInputStream bais = new ByteArrayInputStream(obuf);
			//if (sdbio.isCustomClassLoader())
			//	s = new CObjectInputStream(bais, sdbio.getCustomClassLoader());
			//else
			//	s = new ObjectInputStream(bais);
			//Od = s.readObject();
			//s.close();
			//bais.close();
			//--------------------
			//ByteBuffer bb = ByteBuffer.wrap(obuf);
			ReadableByteChannel rbc = Channels.newChannel(bais);
			if (sdbio.isCustomClassLoader())
				s = new CObjectInputStream(Channels.newInputStream(rbc), sdbio.getCustomClassLoader());
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
	 * Latching block
	 * @param lbn The block number to allocate
	 */
	void alloc(long lbn) throws IOException {
		tmpBai.setTemplateBlockNumber(lbn);
		BlockAccessIndex bai = getUsedBlock(tmpBai);
		alloc(bai);
	}

	/**
	* Latching block, increment access count
	* @param bai The block access and index object
	*/
	public void alloc(BlockAccessIndex bai) throws IOException {
		bai.addAccess();
	}
	/**
	 * deallocation involves decrementing the access number if the block is in use
	 * @param lbn
	 * @throws IOException
	 */
	public void dealloc(long lbn) throws IOException {
		tmpBai.setTemplateBlockNumber(lbn);
		BlockAccessIndex bai = getUsedBlock(tmpBai);
		if (bai == null) {
			throw new IOException(
				"Could not locate " + bai + " for deallocation");
		}
		dealloc(bai);
	}
	public void dealloc(BlockAccessIndex bai) throws IOException {
		bai.decrementAccesses();
	}

	/**
	* We'll do this on a 'clear' of collection, reset all tables
	*/
	public void forceBufferClear() {
		//freeBL.clear();
		// populate with blocks, they're all free for now
		Set<BlockAccessIndex> sbai = usedBL.keySet();
		Iterator<BlockAccessIndex> it = sbai.iterator();
		while(it.hasNext()) {
			BlockAccessIndex bai = (BlockAccessIndex) it.next();
			bai.resetBlock();
			freeBL.addElement(bai);
		}
		usedBL.clear();
	}
	/**
	* Get from free list, puts in used list of block access index.
	* Comes here when we can't find blocknum in table in findOrAddBlock.
	* New instance of BlockAccessIndex causes allocation
	* @param Lbn block number to add
	* @exception IOException if new dblock cannot be created
	*/
	private BlockAccessIndex addBlockAccess(Long Lbn) throws IOException {
		// see if we have open slots
		checkBufferFlush();
		BlockAccessIndex bai = (freeBL.elementAt(0));
		freeBL.removeElementAt(0);
		bai.setBlockNum(Lbn.longValue());
		usedBL.put(bai, null);
		return bai;
	}
	/**
	* Add a block to table of blocknums and block access index.
	* Comes here for acquireBlock. No setting of block in BlockAccessIndex, no initial read
	* @param Lbn block number to add
	* @exception IOException if new dblock cannot be created
	*/
	private BlockAccessIndex addBlockAccessNoRead(Long Lbn) throws IOException {
		// see if we have open slots
		checkBufferFlush();
		BlockAccessIndex bai = (freeBL.elementAt(0));
		freeBL.removeElementAt(0);
		bai.setTemplateBlockNumber(Lbn.longValue());
		usedBL.put(bai, null);
		return bai;
	}

	/**
	* findOrAddBlockAccess - find and return block in pool or bring
	* it in and add it to pool<br> Always ends up calling alloc, here or in addBlock.
	* @param bn The block number to add
	* @return The index into the block array
	* @exception IOException if cannot read
	*/
	public BlockAccessIndex findOrAddBlockAccess(long bn)
		throws IOException {
		Long Lbn = new Long(bn);
		tmpBai.setTemplateBlockNumber(bn);
		BlockAccessIndex bai = getUsedBlock(tmpBai);
		if (bai != null) {
			return bai;
		}
		// didn't find it, we must add
		return addBlockAccess(Lbn);
	}
	/**
	* @param tlbn The block number to retrieve 
	* @return actual Datablock to caller via findoradd
	* @exception IOException if cannot read
	*/
	Datablock getDatablock(Long tlbn) throws IOException {
		return findOrAddBlockAccess(tlbn.longValue()).getBlk();
	}

	/**
	* Get a block access control instance from L2 cache
	* @param bai The template containing the block number, used to locate key
	* @return The key found or whatever set returns otherwise if nothing a null is returned
	*/
	public BlockAccessIndex getUsedBlock(BlockAccessIndex bai) {
		tbai.setTemplateBlockNumber(bai.getBlockNum() + 1L);
		SortedMap<BlockAccessIndex, ?> sm = usedBL.subMap(bai, tbai);
		return (sm.isEmpty() ? null : (sm.firstKey()));
	}

	/**
	* Create initial buckets
	* @exception IOException if buckets cannot be created
	*/
	private void createBuckets() throws IOException {
		Datablock d = new Datablock(DBPhysicalConstants.DATASIZE);
		for (int ispace = 0;ispace < DBPhysicalConstants.DTABLESPACES;ispace++) {
			long xsize = 0L;
			// write bucket blocks
			//	int ispace = 0;
			for (int i = 0; i < DBPhysicalConstants.DBUCKETS; i++) {
				// check for tablespace 0 , pos 0 and add our btree root
				if( ispace == 0 && i == 0) {
					long rootbl = makeVblock(0, 0);
					BTreeKeyPage broot = new BTreeKeyPage(rootbl);
					broot.setUpdated(true);
					//broot.putPage(this);
					byte[] pb = GlobalDBIO.getObjectAsBytes(broot);
					if( DEBUG ) System.out.println("Main btree create root: "+pb.length+" bytes");
					if( pb.length > DBPhysicalConstants.DATASIZE) {
						System.out.println("WARNING: Btree root node keysize of "+pb.length+
								" overflows page boundary of size "+DBPhysicalConstants.DATASIZE+
								" possible database instability. Fix configs to rectify.");
					}
					if( DEBUG ) {
						int inz = 0;
						for(int ii = 0; ii < pb.length; ii++) {
							if( pb[ii] != 0) ++inz;
						}
						assert(inz > 0) : "No non-zero elements in btree root buffer";
						System.out.println(inz+" non zero elements in serialized array");
					}
					Datablock db = new Datablock();
					db.setBytesused((short) pb.length);
					db.setBytesinuse((short) pb.length);
					db.setWriteid(1L);
					db.setPageLSN(-1L);
					System.arraycopy(pb, 0, db.data, 0, pb.length);
					db.setIncore(true);
					ioManager.FseekAndWriteFully(rootbl, db);
					//db.setIncore(false);
					//Fsync(rootbl);
					if( DEBUG ) System.out.println("CreateBuckets Added object @ root, bytes:"+pb.length+" data:"+db);
					broot.setUpdated(false);
				} else {
					ioManager.FseekAndWriteFully(makeVblock(ispace, xsize), d);
				}
				xsize += (long) DBPhysicalConstants.DBLOCKSIZ;
			}
		}

	}
	

	/**
	 * acquireblk - get block from unused chunk or create chunk and get<br>
	 * return acquired block
	 * @param lastGoodBlk The block for us to link to
	 * @return The BlockAccessIndex
	 * @exception IOException if db not open or can't get block
	 */
	public BlockAccessIndex acquireblk(BlockAccessIndex lastGoodBlk)
		throws IOException {
		//synchronized(globalPool) {
		long newblock;
		BlockAccessIndex ablk = lastGoodBlk;
		// this way puts it all in one tablespace
		//int tbsp = getTablespace(lastGoodBlk.getBlockNum());
		int tbsp = new Random().nextInt(DBPhysicalConstants.DTABLESPACES);
		newblock = makeVblock(tbsp, ioManager.getNextFreeBlock(tbsp));
		// update old block
		ablk.getBlk().setNextblk(newblock);
		ablk.getBlk().setIncore(true);
		ablk.getBlk().setInlog(false);
		dealloc(ablk);
		// new block number for BlockAccessIndex set in addBlockAccessNoRead
		BlockAccessIndex dblk = addBlockAccessNoRead(new Long(newblock));
		dblk.getBlk().setPrevblk(lastGoodBlk.getBlockNum());
		dblk.getBlk().setNextblk(-1L);
		dblk.getBlk().setBytesused((short) 0);
		dblk.getBlk().setBytesinuse ((short)0);
		dblk.getBlk().setWriteid(1L);
		dblk.getBlk().setPageLSN(-1L);
		dblk.getBlk().setIncore(true);
		dblk.getBlk().setInlog(false);
		return dblk;
	}
	/**
	 * stealblk - get block from unused chunk or create chunk and get.
	 * We dont try to link it to anything because our little linked lists
	 * of instance blocks are not themselves linked to anything<br>
	 * return acquired block
	 * @param currentBlk Current block so we can deallocate and replace it off chain
	 * @return The BlockAccessIndex of stolen blk
	 * @exception IOException if db not open or can't get block
	 */
	public BlockAccessIndex stealblk(BlockAccessIndex currentBlk) throws IOException {
		long newblock;
		if (currentBlk != null)
			dealloc(currentBlk);
		int tbsp = new Random().nextInt(DBPhysicalConstants.DTABLESPACES);
		newblock = makeVblock(tbsp, ioManager.getNextFreeBlock(tbsp));
		// new block number for BlockAccessIndex set in addBlockAccessNoRead
		BlockAccessIndex dblk = addBlockAccessNoRead(new Long(newblock));
		dblk.getBlk().setPrevblk(-1L);
		dblk.getBlk().setNextblk(-1L);
		dblk.getBlk().setBytesused((short) 0);
		dblk.getBlk().setBytesinuse((short)0);
		dblk.getBlk().setWriteid(1L);
		dblk.getBlk().setPageLSN(-1L);
		dblk.getBlk().setIncore(false);
		dblk.getBlk().setInlog(false);
		return dblk;
	}


	public long getNew_node_pos_blk() {
		return new_node_pos_blk;
	}

	public void setNew_node_pos_blk(long new_node_pos_blk) {
		this.new_node_pos_blk = new_node_pos_blk;
	}

	public long getTransId() {
		return transId;
	}

	public void setUlog(RecoveryLog ulog) {
		this.ulog = ulog;
	}

}
