package com.neocoretechs.bigsack.io.pooled;
import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.*;
import java.util.*;

import com.neocoretechs.bigsack.DBPhysicalConstants;
import com.neocoretechs.bigsack.Props;
import com.neocoretechs.bigsack.btree.BTreeKeyPage;

import com.neocoretechs.bigsack.io.IoInterface;
import com.neocoretechs.bigsack.io.IoManagerInterface;
import com.neocoretechs.bigsack.io.MultithreadedIOManager;
import com.neocoretechs.bigsack.io.RecoveryLogManager;
import com.neocoretechs.bigsack.io.cluster.ClusterIOManager;
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
	private static final boolean DEBUG = false;
	private int MAXBLOCKS = 1024; // PoolBlocks property may overwrite
	private String dbName;
	private String remoteDBName;
	private long transId;
	protected boolean isNew = false; // if we create and no data yet
	protected IoManagerInterface ioManager = null;// = new MultithreadedIOManager();

	private RecoveryLogManager ulog;		

	private int L3cache = 0; // Level 3 cache type, mmap, file, etc

	public RecoveryLogManager getUlog() {
		return ulog;
	}
	
	public IoManagerInterface getIOManager() {
		return ioManager;
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
	* or File or Cluster.  The number of buffer pool entries is controlled by the PoolBlocks
	* property. 
	* @param dbname Fully qualified path of DB
	* @param remoteDBName the path to remote tablespace if it differs from log dir
	* @param create true to create the database if it does not exist
	* @param transId Transaction Id of current owner
	* @exception IOException if open problem
	*/
	public GlobalDBIO(String dbname, String remoteDBName, boolean create, long transId) throws IOException {
		this.dbName = dbname;
		this.remoteDBName = remoteDBName;
		this.transId = transId;
		// Set up the proper IO manager based on the execution model of cluster main cluster node or standalone
		if (Props.toString("L3Cache").equals("MMap")) {
			L3cache = 0;
		} else {
			if (Props.toString("L3Cache").equals("File")) {
				L3cache = 1;
			} else {
					throw new IOException("Unsupported L3 cache type");
			}
		}
		
		// MAXBLOCKS may be set by PoolBlocks property
		setMAXBLOCKS(Props.toInt("PoolBlocks"));
		
		if (Props.toString("Model").startsWith("Cluster")) {
			if( DEBUG )
				System.out.println("Cluster Node IO Manager coming up...");
			ioManager = new ClusterIOManager(this);
		} else {
			if( DEBUG )
				System.out.println("Multithreaded IO Manager coming up...");
			ioManager = new MultithreadedIOManager(this);
		}
	    
		Fopen(dbName, remoteDBName, create);
	
		if (create && isNew()) {
			isNew = true;
			// init the Datablock arrays, create freepool
			createBuckets();
			ioManager.setNextFreeBlocks();
		} 

	}
	
	private boolean isNew() {
		return ioManager.isNew();
	}

	/**
	* Constructor creates DB if not existing, otherwise open
	* @param dbname Fully qualified path or URL of DB
	* @param transId
	* @exception IOException if IO problem
	*/
	public GlobalDBIO(String dbname, String remoteDbName, long transId) throws IOException {
		this(dbname, remoteDbName, true, transId);
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
	* If create is true, create primary tablespace else try to open all existing
	* Call the ioManager Fopen instance
	* @param fname String file name
	* @param remote The remote database path
	* @param create true to create if not existing
	* @exception IOException if problems opening/creating
	* @return true if successful
	* @see IoInterface
	*/
	boolean Fopen(String fname, String remote, boolean create) throws IOException {
		return ioManager.Fopen(fname, remote, L3cache, create);
	}
	
	/**
	 * Re-open tablespace
	 * @throws IOException
	 */
 	void Fopen() throws IOException {
 		ioManager.Fopen();
	}

	/**
	* @return DB name as String
	*/
	public String getDBName() {
		return dbName;
	}

	public String getRemoteDBName() {
		return remoteDBName;	
	}
	/**
	 * Applies to local log directory if remote dir differs
	 * @return
	 */
	public String getDBPath() {
		return (new File(dbName)).toPath().getParent().toString();
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
	* static method for serialized byte to object conversion
	* @param sdbio The BlockDBIO which may contain a custom class loader to use
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
	* Latching block, increment access count
	* @param bai The block access and index object
	*/
	/*
	protected void alloc(BlockAccessIndex bai) throws IOException {
		bai.addAccess();
	}
	*/
	protected void dealloc(BlockAccessIndex bai) throws IOException {
		bai.decrementAccesses();
	}

	/**
	 * Part of general block/page buffer maintenance that can come through here
	 * @throws IOException
	 */
	public void checkBufferFlush() throws IOException {
		ioManager.freeupBlock();
	}
	
	public void commitBufferFlush() throws IOException {
		ioManager.commitBufferFlush();
	}
	
	public void rollbackBufferFlush() {
		forceBufferClear();
	}
	public void forceBufferWrite() throws IOException {
		ioManager.directBufferWrite();
	}
	/**
	* We'll do this on a 'clear' of collection, reset all caches
	* Take the used block list, reset the blocks, move to to free list, then
	* finally clear the used block list. We do this during rollback to remove any modified
	*/
	public void forceBufferClear() {
			ioManager.forceBufferClear();
	}

	/**
	* Create initial buckets
	* @exception IOException if buckets cannot be created
	*/
	private void createBuckets() throws IOException {
		Datablock d = new Datablock(DBPhysicalConstants.DATASIZE);
		d.resetBlock();
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
	 * Immediately after log write, we come here to make sure block
	 * is written back to main store. We do not use the queues as this operation
	 * is always adjunct to a processed queue request
	 * @param toffset
	 * @param tblk
	 * @throws IOException
	 */
	public void FseekAndWrite(long toffset, Datablock tblk) throws IOException {
		//if( DEBUG )
		//	System.out.print("GlobalDBIO.FseekAndWrite:"+valueOf(toffset)+" "+tblk.toVblockBriefString()+"|");
		// send write command and queues be damned, writes only happen
		// immediately after log writes
		ioManager.FseekAndWrite(toffset, tblk);
		//if( Props.DEBUG ) System.out.print("GlobalDBIO.FseekAndWriteFully:"+valueOf(toffset)+" "+tblk.toVblockBriefString()+"|");
	}	

	/**
	 * acquireblk - get block from unused chunk or create chunk and get<br>
	 * return acquired block
	 * Add a block to table of blocknums and block access index.
	 * No setting of block in BlockAccessIndex, no initial read reading through ioManager.addBlockAccessnoRead
	 * @param lastGoodBlk The block for us to link to
	 * @return The BlockAccessIndex
	 * @exception IOException if db not open or can't get block
	 */
	protected BlockAccessIndex acquireblk(BlockAccessIndex lastGoodBlk) throws IOException {
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
		BlockAccessIndex dblk =  ioManager.addBlockAccessNoRead(new Long(newblock));
		dblk.getBlk().setPrevblk(lastGoodBlk.getBlockNum());
		dblk.getBlk().setNextblk(-1L);
		dblk.getBlk().setBytesused((short) 0);
		dblk.getBlk().setBytesinuse ((short)0);
		dblk.getBlk().setPageLSN(-1L);
		dblk.getBlk().setIncore(true);
		dblk.getBlk().setInlog(false);
		return dblk;
	}
	/**
	 * stealblk - get block from unused chunk or create chunk and get.
	 * We dont try to link it to anything because our little linked lists
	 * of instance blocks are not themselves linked to anything<br>
	 * return acquired block. Read through the ioManager calling addBlockAccessNoRead with new block
	 * @param currentBlk Current block so we can deallocate and replace it off chain
	 * @return The BlockAccessIndex of stolen blk
	 * @exception IOException if db not open or can't get block
	 */
	protected BlockAccessIndex stealblk(BlockAccessIndex currentBlk) throws IOException {
		long newblock;
		if (currentBlk != null)
			dealloc(currentBlk);
		int tbsp = new Random().nextInt(DBPhysicalConstants.DTABLESPACES);
		newblock = makeVblock(tbsp, ioManager.getNextFreeBlock(tbsp));
		// new block number for BlockAccessIndex set in addBlockAccessNoRead
		BlockAccessIndex dblk = ioManager.addBlockAccessNoRead(new Long(newblock));
		dblk.getBlk().setPrevblk(-1L);
		dblk.getBlk().setNextblk(-1L);
		dblk.getBlk().setBytesused((short) 0);
		dblk.getBlk().setBytesinuse((short)0);
		dblk.getBlk().setPageLSN(-1L);
		dblk.getBlk().setIncore(false);
		dblk.getBlk().setInlog(false);
		return dblk;
	}

	public long getTransId() {
		return transId;
	}

	public void setUlog(RecoveryLogManager ulog) {
		this.ulog = ulog;
	}

	public int getMAXBLOCKS() {
		return MAXBLOCKS;
	}

	public void setMAXBLOCKS(int mAXBLOCKS) {
		MAXBLOCKS = mAXBLOCKS;
	}

}
