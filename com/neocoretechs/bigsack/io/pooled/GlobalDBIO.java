package com.neocoretechs.bigsack.io.pooled;
import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import com.neocoretechs.bigsack.DBPhysicalConstants;
import com.neocoretechs.bigsack.Props;
import com.neocoretechs.bigsack.btree.BTreeKeyPage;
import com.neocoretechs.bigsack.io.IoInterface;
import com.neocoretechs.bigsack.io.IoManagerInterface;
import com.neocoretechs.bigsack.io.MultithreadedIOManager;
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
* @author Jonathan Groff Copyright (C) NeoCoreTechs 1997,2020,2021
*/

public class GlobalDBIO {
	private static final boolean DEBUG = false;
	private int MAXBLOCKS = 1024; // PoolBlocks property may overwrite
	private int MAXKEYS = 5; // Number of keys per page max
	private int L3cache = 0; // Level 3 cache type, mmap, file, etc
	private String[][] nodePorts = null; // remote worker nodes and their ports, if present
	
	private String dbName;
	private String remoteDBName;
	private long transId;
	protected boolean isNew = false; // if we create and no data yet
	protected IoManagerInterface ioManager = null;// = new MultithreadedIOManager();, ClusterIOManager, etc.

	public IoManagerInterface getIOManager() {
		return ioManager;
	}
	
	/**
	* Translate the virtual tablespace (first 3 bits) and block to real block
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
		try {
			setMAXBLOCKS(Props.toInt("PoolBlocks"));
		} catch(IllegalArgumentException iae) {} // use default;
		// MAXKEYS, maximum keys per page may also be set as it coincides with page size
		try {
			setMAXKEYS(Props.toInt("MaxKeysPerPage"));
		} catch(IllegalArgumentException iae) {} // use default;
		
		if (Props.toString("Model").startsWith("Cluster")) {
			if( DEBUG )
				System.out.println("Cluster Node IO Manager coming up...");
			// see if we assign the array of target worker nodes in cluster
			if( Props.toString("Nodes") != null ) {
				String[] nodes = Props.toString("Nodes").split(",");
				nodePorts = new String[nodes.length][];
				for(int i = 0; i < nodes.length; i++) {
					nodePorts[i] = nodes[i].split(":");
				}
				if( DEBUG )
					for(int i = 0; i < nodePorts.length; i++)
						System.out.println("Node "+nodePorts[i][0]+" port "+nodePorts[i][1]);
			}
			ioManager = new ClusterIOManager((ObjectDBIO) this);
		} else {
			if( DEBUG )
				System.out.println("Multithreaded IO Manager coming up...");
			ioManager = new MultithreadedIOManager((ObjectDBIO) this);
		}
		
		Fopen(dbName, remoteDBName, create);
		if(create && isNew()) {
			isNew = true;
		}

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
	
	private boolean isNew() {
		return ioManager.isNew();
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
	* If create is true, create primary tablespace else try to open all existing
	* Call the ioManager Fopen instance
	* @param fname String file name
	* @param remote The remote database path
	* @param create true to create if not existing
	* @exception IOException if problems opening/creating
	* @return true if successful
	* @see IoInterface
	*/
	synchronized boolean Fopen(String fname, String remote, boolean create) throws IOException {
		if( DEBUG )
			System.out.println("GlobalDBIO.Fopen "+fname+" "+remote+" "+create);
		return ioManager.Fopen(fname, remote, L3cache, create);
	}
	
	/**
	 * Re-open tablespace
	 * @throws IOException
	 */
 	synchronized void Fopen() throws IOException {
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
	 */
	public synchronized void checkpointBufferFlush() throws IOException {
		ioManager.checkpointBufferFlush();
	}
	/**
	 * Flush the page pool buffers and discard outstanding blocks.
	 */
	public synchronized void rollbackBufferFlush() {
		forceBufferClear();
	}
	/**
	 * Force a write of the outstanding blocks in the page buffer pool.
	 * @throws IOException
	 */
	public synchronized void forceBufferWrite() throws IOException {
		ioManager.directBufferWrite();
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
	 * bring block into pool from deep store.
	 * @param pos
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
	public synchronized void deallocOutstanding() throws IOException {
		ioManager.deallocOutstanding();	
	}
	/**
	 * Deallocate the resources for the pooled block at the desired position
	 * @param pos The long vblock of the resource
	 * @throws IOException
	 */
	public synchronized void deallocOutstanding(long pos) throws IOException {
		ioManager.deallocOutstanding(pos);	
	}
	/**
	 * Selects a tablespace to prepare call the ioManager.
	 * (ClusterIOManager, MultithreadedIOManager, etc implementing IoManagerInterface), in order to acquire the
	 * blockbuffer for that tablespace to steal a block from the freechain or acquire it into the 
	 * BlockAccessIndex buffer for that tablespace.
	 * At that point the buffers are set for cursor based retrieval from the tablespace buffer.
	 * @return The BlockAccessIndex from the random tablespace, with byte index set to 0
	 * @throws IOException
	 */
	public synchronized BlockAccessIndex stealblk() throws IOException {
		int tbsp = ioManager.getFreeBlockAllocator().nextTablespace();
		// ioManager = ClusterIOManager, MultithreadedIOManager, etc implementing IoManagerInterface
		return ioManager.getBlockBuffer(tbsp).stealblk(ioManager.getBlockStream(tbsp).getBlockAccessIndex());
	}
	/**
	 * Deallocate the outstanding buffer resources, block latches, etc. for 
	 * the purpose of transaction checkpoint rollback.
	 * @throws IOException
	 */
	public synchronized void deallocOutstandingRollback() throws IOException {
		ioManager.deallocOutstandingRollback();
	}
	/**
	 * Deallocate the outstanding buffer resources, block latches, etc. for 
	 * the purpose of transaction checkpoint commit.
	 * @throws IOException
	 */	
	public synchronized void deallocOutstandingCommit() throws IOException {
		ioManager.deallocOutstandingCommit();	
	}
	
	/**
	* Create initial buckets
	* @exception IOException if buckets cannot be created
	*/
	public synchronized void createBuckets() throws IOException {
		Datablock d = new Datablock(DBPhysicalConstants.DATASIZE);
		d.resetBlock();
		for (int ispace = 0;ispace < DBPhysicalConstants.DTABLESPACES;ispace++) {
			long xsize = 0L;
			// write bucket blocks
			for (int i = 0; i < DBPhysicalConstants.DBUCKETS; i++) {
				// check for tablespace 0 , pos 0 and add our btree root
				ioManager.FseekAndWriteFully(makeVblock(ispace, xsize), d);
				xsize += (long) DBPhysicalConstants.DBLOCKSIZ;
			}
		}
		long rootbl = makeVblock(0, 0);
		// This constructor for BtreeKeyPage takes a block from freechain and allocates it
		BTreeKeyPage broot = new BTreeKeyPage((ObjectDBIO) this, rootbl, true);
		// Set all fields to be written
		broot.setUpdated(true);
		broot.setAllUpdated(true);
		// Put the page into the block byte buffer
		broot.putPage();
		if( DEBUG ) {
				System.out.println("Main btree create root: "+broot+" with block "+broot.getBlockAccessIndex());
		}
		if( DEBUG ) System.out.println("GlobalDBIO.CreateBuckets Added root key page @:"+broot);

	}
	/**
	 * Immediately after log write, we come here to make sure block
	 * is written back to main store. We do not use the queues as this operation
	 * is always adjunct to a processed queue request
	 * @param toffset
	 * @param tblk
	 * @throws IOException
	 */
	public synchronized void FseekAndWrite(long toffset, Datablock tblk) throws IOException {
		if( DEBUG )
			System.out.print("GlobalDBIO.FseekAndWrite:"+valueOf(toffset)+" "+tblk.blockdump()+"|");
		// send write command and queues be damned, writes only happen
		// immediately after log writes
		ioManager.FseekAndWrite(toffset, tblk);
		//if( Props.DEBUG ) System.out.print("GlobalDBIO.FseekAndWriteFully:"+valueOf(toffset)+" "+tblk.toVblockBriefString()+"|");
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
	
	public int getMAXKEYS() {
		return MAXKEYS;
	}

	public synchronized void setMAXKEYS(int mAXKEYS) {
		MAXKEYS = mAXKEYS;
		BTreeKeyPage.MAXKEYS = MAXKEYS;
	}
}
