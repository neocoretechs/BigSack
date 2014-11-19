package com.neocoretechs.bigsack.io.pooled;
import java.io.*;
import java.nio.channels.*;
import java.util.*;

import com.neocoretechs.bigsack.DBPhysicalConstants;
import com.neocoretechs.bigsack.Props;
import com.neocoretechs.bigsack.io.FileIO;
import com.neocoretechs.bigsack.io.IoInterface;
import com.neocoretechs.bigsack.io.MmapIO;
import com.neocoretechs.bigsack.io.RecoveryLog;
import com.neocoretechs.bigsack.io.stream.CObjectInputStream;
/*
* Copyright (c) 1997,2003, NeoCoreTechs
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
	private String Name;
	private long transId;
	protected boolean isNew = false; // if we create and no data yet
	// this plugs in proper io module, file, network, etc.
	// for tablespace array
	//private IoInterface[] ioUnit = new FileIO[DBPhysicalConstants.DTABLESPACES];
	private IoInterface[] ioUnit;
	private MappedBlockBuffer  usedBL = new MappedBlockBuffer(); // block number to Datablock
	private Vector<BlockAccessIndex> freeBL = new Vector<BlockAccessIndex>(); // free blocks
	private BlockAccessIndex tmpBai;
	private RecoveryLog ulog;		
	private long[] nextFreeBlock = new long[DBPhysicalConstants.DTABLESPACES];
	private long new_node_pos_blk = -1L;
	private int L3cache = 0; // Level 3 cache type, mmap, file, etc
	static int MAXBLOCKS = 1024; // PoolBlocks property may overwrite

	public RecoveryLog getUlog() {
		return ulog;
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
		// set up proper io module
		switch (L3cache) {
			case 0 :
				ioUnit = new MmapIO[DBPhysicalConstants.DTABLESPACES];
				break;
			case 1 :
				ioUnit = new FileIO[DBPhysicalConstants.DTABLESPACES];
				break;
			default :
				throw new IOException("Unsupported L3 cache type");
		}

		//
		Fopen(Name, create);
		tmpBai = new BlockAccessIndex();
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
			setNextFreeBlocks();
			create = true;
		} else {
			getNextFreeBlocks();
			create = false;
		}

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
	* Return the first available block that can be acquired for write
	* @param tblsp The tablespace
	* @return The block available
	* @exception IOException if IO problem
	*/
	long getNextFreeBlock(int tblsp) throws IOException {
		long tsize = ioUnit[tblsp].Fsize();
		nextFreeBlock[tblsp] += (long) DBPhysicalConstants.DBLOCKSIZ;
		if (nextFreeBlock[tblsp] >= tsize) {
			// extend tablespace in pool-size increments
			long newLen =
				tsize
					+ (long) (DBPhysicalConstants.DBLOCKSIZ
						* DBPhysicalConstants.DBUCKETS);
			ioUnit[tblsp].Fset_length(newLen);
			Datablock d = new Datablock(DBPhysicalConstants.DATASIZE);
			while (tsize < newLen) {
				ioUnit[tblsp].Fseek(tsize);
				d.write(ioUnit[tblsp]);
				tsize += (long) DBPhysicalConstants.DBLOCKSIZ;
			}
			ioUnit[tblsp].Fforce(); // flush on block creation
		}
		return nextFreeBlock[tblsp];
	}
	
	void setNextFreeBlock(int tblsp, long tnextFree) {
		nextFreeBlock[tblsp] = tnextFree;
	}
	/**
	* Set the initial free blocks after buckets created or bucket initial state
	* Since our directory head gets created in block 0 tablespace 0, the next one is actually the start
	*/
	void setNextFreeBlocks() {
		for (int i = 0; i < DBPhysicalConstants.DTABLESPACES; i++)
			if (i == 0)
				nextFreeBlock[i] = (long) DBPhysicalConstants.DBLOCKSIZ;
			else
				nextFreeBlock[i] = 0L;
	}
	/**
	* Set the next free block position from reverse scan of blocks
	* @exception IOException if seek or size fails
	*/
	void getNextFreeBlocks() throws IOException {
		Datablock d = new Datablock(DBPhysicalConstants.DATASIZE);
		long endBlock =
			(long) (DBPhysicalConstants.DBLOCKSIZ
				* DBPhysicalConstants.DBUCKETS);
		for (int i = 0; i < DBPhysicalConstants.DTABLESPACES; i++) {
			long endBl = ioUnit[i].Fsize();
			while (endBl > endBlock) {
				ioUnit[i].Fseek(endBl - (long) DBPhysicalConstants.DBLOCKSIZ);
				d.read(ioUnit[i]);
				if (d.getPrevblk() == -1L
					&& d.getNextblk() == -1L
					&& d.getBytesused() == 0
					&& d.getBytesinuse() == 0) {
					endBl -= (long) DBPhysicalConstants.DBLOCKSIZ;
					continue;
				} else {
					// this is it
					break;
				}
			}
			nextFreeBlock[i] = endBl;
		}
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
		while (++tempSpace < ioUnit.length) {
			if (ioUnit[tempSpace] != null) {
				long vBlock = makeVblock(tempSpace, 0L);
				Fseek(vBlock);
				return vBlock;
			}
		}
		return 0L;
	}
	/**
	* Find the smallest tablespace for storage balance, we will always favor creating one
	* over extending an old one
	* @return tablespace
	* @exception IOException if seeking new tablespace or creating fails
	*/
	int findSmallestTablespace() throws IOException {
		// always make sure we have primary
		long primarySize = ioUnit[0].Fsize();
		int smallestTablespace = 0; // default main
		long smallestSize = primarySize;
		for (int i = 0; i < ioUnit.length; i++) {
			if (ioUnit[i] == null) {
				switch (L3cache) {
					case 0 :
						ioUnit[i] = new MmapIO();
						break;
					case 1 :
						ioUnit[i] = new FileIO();
				}
				if (!ioUnit[i].Fopen(Name + "." + String.valueOf(i), true))
					throw new IOException("Cannot create tablespace!");
				return i;
			}
			long fsize = ioUnit[i].Fsize();
			if (fsize < smallestSize) {
				smallestSize = fsize;
				smallestTablespace = i;
			}
		}
		return smallestTablespace;
	}
	/**
	* If create is true, create only primary tablespace
	* else try to open all existing
	* @param fname String file name
	* @param create true to create if not existing
	* @exception IOException if problems opening/creating
	* @return true if successful
	* @see IoInterface
	*/
	boolean Fopen(String fname, boolean create) throws IOException {
		for (int i = 0; i < ioUnit.length; i++) {
			if (ioUnit[i] == null)
				switch (L3cache) {
					case 0 :
						ioUnit[i] = new MmapIO();
						break;
					case 1 :
						ioUnit[i] = new FileIO();
				}
			if (!ioUnit[i].Fopen(fname + "." + String.valueOf(i), create))
				return false;
		}
		return true;
	}
	
	
 	void Fopen() throws IOException {
		for (int i = 0; i < ioUnit.length; i++)
			if (ioUnit[i] != null && !ioUnit[i].isopen())
				ioUnit[i].Fopen();
	}
	
	void Fclose() throws IOException {
		for (int i = 0; i < ioUnit.length; i++)
			if (ioUnit[i] != null && ioUnit[i].isopen())
				ioUnit[i].Fclose();
	}
	
	public void Fforce() throws IOException {
		for (int i = 0; i < ioUnit.length; i++)
			if (ioUnit[i] != null && ioUnit[i].isopen())
				ioUnit[i].Fforce();
	}
	
	long Ftell(int ttableSpace) throws IOException {
		return makeVblock(ttableSpace, ioUnit[ttableSpace].Ftell());
	}
	
	void Fseek(long toffset) throws IOException {
		int ttableSpace = getTablespace(toffset);
		long offset = getBlock(toffset);
		ioUnit[ttableSpace].Fseek(offset);
	}
	
	public void FseekAndRead(long toffset, Datablock tblk)
		throws IOException {
		int ttableSpace = getTablespace(toffset);
		long offset = getBlock(toffset);
		if (ioUnit[ttableSpace] == null) {
			throw new RuntimeException(
				"GlobalDBIO.FseekAndRead tablespace null "
					+ toffset
					+ " = "
					+ ttableSpace
					+ ","
					+ offset);
		}
		if (tblk == null) {
			throw new RuntimeException(
				"GlobalDBIO.FseekAndRead Datablock null "
					+ toffset
					+ " = "
					+ ttableSpace
					+ ","
					+ offset);
		}
		if (tblk.isIncore())
			throw new RuntimeException(
				"GlobalDBIO.FseekAndRead block incore preempts read "
					+ toffset
					+ " "
					+ tblk);

		ioUnit[ttableSpace].Fseek(offset);
		tblk.readUsed(ioUnit[ttableSpace]);
	}
	
	public void FseekAndWrite(long toffset, Datablock tblk) throws IOException {
		int ttableSpace = getTablespace(toffset);
		long offset = getBlock(toffset);
		ioUnit[ttableSpace].Fseek(offset);
		tblk.writeUsed(ioUnit[ttableSpace]);
		tblk.setIncore(false);
		if( Props.DEBUG ) System.out.println("GlobalDBIO.FseekAndWrite:"+valueOf(toffset)+" "+tblk.toVblockBriefString());
	}
	
	
	void FseekAndReadFully(long toffset, Datablock tblk)
		throws IOException {
		int ttableSpace = getTablespace(toffset);
		long offset = getBlock(toffset);
		if (tblk.isIncore())
			throw new RuntimeException(
				"GlobalDBIO.FseekAndRead: block incore preempts read "
					+ valueOf(toffset)
					+ " "
					+ tblk);
		ioUnit[ttableSpace].Fseek(offset);
		tblk.read(ioUnit[ttableSpace]);
	}
	
	void FseekAndWriteFully(long toffset, Datablock tblk)
		throws IOException {
		int ttableSpace = getTablespace(toffset);
		long offset = getBlock(toffset);
		ioUnit[ttableSpace].Fseek(offset);
		tblk.write(ioUnit[ttableSpace]);
		tblk.setIncore(false);
		if( Props.DEBUG ) System.out.println("GlobalDBIO.FseekAndWriteFully:"+valueOf(toffset)+" "+tblk.toVblockBriefString());
	}
	
	/** Transfer tablespace 0 to all others (copy op) */
	void Ftransfer() throws IOException {
		FileChannel fFrom = (FileChannel) (ioUnit[0].getChannel());
		fFrom.force(true);
		for (int i = 1; i < ioUnit.length; i++) {
			FileChannel fTo = (FileChannel) (ioUnit[i].getChannel());
			fFrom.transferTo(0, fFrom.size(), fTo);
			fTo.force(true);
		}
	}
	
	void Fset_length(int ttableSpace, long tsize)
		throws IOException {
		ioUnit[ttableSpace].Fset_length(tsize);
	}

	public long Fsize(int ttableSpace) throws IOException {
		return ioUnit[ttableSpace].Fsize();
	}
	
	// writing..
	void Fwrite(int ttableSpace, byte[] obuf) throws IOException {
		ioUnit[ttableSpace].Fwrite(obuf);
	}
	
	void Fwrite(int ttableSpace, byte[] obuf, int osiz)
		throws IOException {
		ioUnit[ttableSpace].Fwrite(obuf, osiz);
	}
	
	void Fwrite_long(int ttableSpace, long obuf) throws IOException {
		ioUnit[ttableSpace].Fwrite_long(obuf);
	}
	
	void Fwrite_short(int ttableSpace, short obuf)
		throws IOException {
		ioUnit[ttableSpace].Fwrite_short(obuf);
	}
	
	void Fwrite_int(int ttableSpace, int obuf) throws IOException {
		ioUnit[ttableSpace].Fwrite_int(obuf);
	}
	// reading..
	int Fread(int ttableSpace, byte[] b, int osiz)
		throws IOException {
		return ioUnit[ttableSpace].Fread(b, osiz);
	}
	
	int Fread(int ttableSpace, byte[] b) throws IOException {
		return ioUnit[ttableSpace].Fread(b);
	}
	
	int Fread_int(int ttableSpace) throws IOException {
		return ioUnit[ttableSpace].Fread_int();
	}
	
	long Fread_long(int ttableSpace) throws IOException {
		return ioUnit[ttableSpace].Fread_long();
	}
	
	short Fread_short(int ttableSpace) throws IOException {
		return ioUnit[ttableSpace].Fread_short();
	}
	
	void Fdelete(int ttableSpace) {
		ioUnit[ttableSpace].Fdelete();
	}
	
	// misc..
	String Fname(int ttableSpace) {
		synchronized (ioUnit[ttableSpace]) {
			return ioUnit[ttableSpace].Fname();
		}
	}
	
	boolean isopen(int ttableSpace) {
		return ioUnit[ttableSpace].isopen();
	}
	
	boolean isnew() {
		return ioUnit[0].isnew();
	}

	boolean iswriteable() {
		return ioUnit[0].iswriteable();
	}

	/**
	* @return DB name as String
	*/
	public String getDBName() {
		return Name;
	}

	/**
	* re-open DB
	*/
	public void Open() throws IOException {
		Fopen();
	}

	/**
	* Close DB
	* @exception IOException if problem closing
	*/
	public void Close() throws IOException {
		Fclose();
	}
	/**
	* static method for object to serialized byte conversion
	* @param Ob the user object
	* @return byte buffer containing serialized data
	* @exception IOException cannot convert
	*/
	public static byte[] getObjectAsBytes(Object Ob) throws IOException {
		byte[] retbytes;
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		ObjectOutput s = new ObjectOutputStream(baos);
		s.writeObject(Ob);
		s.flush();
		baos.flush();
		retbytes = baos.toByteArray();
		s.close();
		baos.close();
		return retbytes;
	}
	/**
	* static method for serialized byte to object conversion
	* @param sdbio The BlockDBIO which may contain a custom class loader to use
	* @param obuf the byte buffer containing serialized data
	* @return Object instance
	* @exception IOException cannot convert
	*/
	public static Object deserializeObject(ObjectDBIO sdbio, byte[] obuf)
		throws IOException {
		Object Od;
		try {
			ObjectInput s;
			ByteArrayInputStream bais = new ByteArrayInputStream(obuf);
			if (sdbio.isCustomClassLoader())
				s = new CObjectInputStream(bais, sdbio.getCustomClassLoader());
			else
				s = new ObjectInputStream(bais);
			Od = s.readObject();
			s.close();
			bais.close();
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
		usedBL.clear();
		freeBL.clear();
		// populate with blocks, they're all free for now
		for (int i = 0; i < MAXBLOCKS; i++) {
			freeBL.addElement(new BlockAccessIndex(this));
		}
	}
	/**
	* Get from free list, puts in used list of block access index.
	* Comes here when we can't find blocknum in table in findOrAddBlock.
	* New instance of BlockAccessIndex causes allocation
	* @param Lbn block number to add
	* @exception IOException if new dblock cannot be created
	*/
	BlockAccessIndex addBlockAccess(Long Lbn) throws IOException {
		// see if we have open slots
		usedBL.checkBufferFlush(freeBL);
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
	BlockAccessIndex addBlockAccessNoRead(Long Lbn)
		throws IOException {
		// see if we have open slots
		usedBL.checkBufferFlush(freeBL);
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
		BlockAccessIndex tbai = new BlockAccessIndex();
		tbai.setTemplateBlockNumber(bai.getBlockNum() + 1L);
		SortedMap<BlockAccessIndex, ?> sm = usedBL.subMap(bai, tbai);
		return (sm.isEmpty() ? null : (sm.firstKey()));
	}

	/**
	* Create initial buckets
	* @exception IOException if buckets cannot be created
	*/
	void createBuckets() throws IOException {
		Datablock d = new Datablock(DBPhysicalConstants.DATASIZE);
		for (int ispace = 0;ispace < DBPhysicalConstants.DTABLESPACES;ispace++) {
			long xsize = 0L;
			// write bucket blocks
			//	int ispace = 0;
			for (int i = 0; i < DBPhysicalConstants.DBUCKETS; i++) {
				FseekAndWriteFully(makeVblock(ispace, xsize), d);
				xsize += (long) DBPhysicalConstants.DBLOCKSIZ;
			}
		}
		//Ftransfer(); // copy this space to others
	}
	/**
	* Reset the tablespaces and write initial buckets
	* @exception IOException if buckets cannot be traversed
	*/
	public void resetBuckets() throws IOException {
		Datablock d = new Datablock(DBPhysicalConstants.DATASIZE);
		for (int ispace = 0;ispace < DBPhysicalConstants.DTABLESPACES;ispace++) {
			long xsize = 0L;
			Fset_length(
				ispace,
				DBPhysicalConstants.DBLOCKSIZ * DBPhysicalConstants.DBUCKETS);
			// rewrite initial bucket setup
			//	if( ispace == 0 )
			for (int i = 0; i < DBPhysicalConstants.DBUCKETS; i++) {
				FseekAndWriteFully(makeVblock(ispace, xsize), d);
				xsize += (long) DBPhysicalConstants.DBLOCKSIZ;
			}
		}
		//Ftransfer();
		setNextFreeBlocks();
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
		newblock = makeVblock(tbsp, getNextFreeBlock(tbsp));
		// update old block
		ablk.getBlk().setNextblk(newblock);
		ablk.getBlk().setIncore(true);
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
		newblock = makeVblock(tbsp, getNextFreeBlock(tbsp));
		// new block number for BlockAccessIndex set in addBlockAccessNoRead
		BlockAccessIndex dblk = addBlockAccessNoRead(new Long(newblock));
		dblk.getBlk().setPrevblk(-1L);
		dblk.getBlk().setNextblk(-1L);
		dblk.getBlk().setBytesused((short) 0);
		dblk.getBlk().setBytesinuse((short)0);
		dblk.getBlk().setWriteid(1L);
		dblk.getBlk().setPageLSN(-1L);
		dblk.getBlk().setIncore(true);
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
