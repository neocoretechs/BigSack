package com.neocoretechs.bigsack.io.pooled;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;

import com.neocoretechs.bigsack.DBPhysicalConstants;
import com.neocoretechs.bigsack.io.Optr;
//import com.neocoretechs.bigsack.io.stream.CObjectInputStream;
import com.neocoretechs.bigsack.io.stream.CObjectInputStream;
import com.neocoretechs.bigsack.session.SessionManager;
/**
* Create the block IO and up through the chain to global IO. After constructing, create an IO manager of the proper
* type based on our cluster or standalone configuration. In the IO manager the block pool objects and associated
* buffers along with the threaded IO workers are spun up, one for each tablespace of each database.
* In addition a recovery log instance for each tablespace is created
* and determine if a roll forward recovery is needed. The flow is create_recovery_log which calls boot()
* undolog instance is then set after construction. Finally, the LogToFile instance is extracted and 'recover' is called
* @param objname The database table
* @param remoteDbName The remote location of tablespaces 
* @param create True to create if not existing
* @param transId The Transaction id
* @exception IOException If problems setting up IO
*/
public final class ObjectDBIO extends GlobalDBIO {
	private static boolean DEBUG = false;
	public ObjectDBIO(String objname, String remoteObjName, boolean create, long transId) throws IOException {
		super(objname, remoteObjName, create, transId);
	}
	/**
	 * Connect without recovery log, to debug or for some read-only purpose
	 * @param dbname
	 * @throws IOException 
	 */
	public ObjectDBIO(String dbname, String remote) throws IOException {
		super(dbname, remote, SessionManager.getGlobalTransId());
	}

	// Are we using custom class loader for serialized versions?
	private boolean isCustomClassLoader;
	private ClassLoader customClassLoader;
	/**
	* delete_object and potentially reclaim space
	* @param loc Location of object
	* @param osize object size
	* @exception IOException if the block cannot be sought or written
	*/
	public synchronized void delete_object(Optr loc, int osize) throws IOException {
		//System.out.println("ObjectDBIO.delete_object "+loc+" "+osize);
		ioManager.objseek(loc);
		ioManager.deleten(loc, osize);
	}
	
	/**
	 * Add an object, which in this case is a load of bytes.
	 * @param loc Location to add this
	 * @param o The byte payload to add to pool
	 * @param osize  The size of the payload to add from array
	 * @exception IOException If the adding did not happen
	 */
	public synchronized void add_object(Optr loc, byte[] o, int osize) throws IOException {
		int tblsp = ioManager.objseek(loc);
		//assert(ioManager.getBlockStream(tblsp).getLbai().getAccesses() > 0 ) : "Writing unlatched block:"+loc+" with payload:"+osize;
		ioManager.writen(tblsp, o, osize);
		//assert(ioManager.getBlockStream(tblsp).getLbai().getAccesses() > 0 && 
		//	   ioManager.getBlockStream(tblsp).getLbai().getBlk().isIncore()) : 
		//	"Block "+loc+" unlatched after write, accesses: "+ioManager.getBlockStream(tblsp).getLbai().getAccesses();
		
		//ioManager.deallocOutstandingWriteLog(tblsp);
	}
	/**
	 * Add an object, which in this case is a load of bytes.
	 * @param loc Location to add this
	 * @param o The byte payload to add to pool
	 * @param osize  The size of the payload to add from array
	 * @exception IOException If the adding did not happen
	 */
	public synchronized void add_object(int tblsp, BlockAccessIndex lbai, byte[] o, int osize) throws IOException {
		ioManager.getBlockStream(tblsp).setBlockAccessIndex(lbai);
		ioManager.writen(tblsp, o, osize);
		
		//ioManager.deallocOutstandingWriteLog(tblsp, lbai);
	}
	/**
	* Read Object in pool: deserialize the byte array.
	* @param sdbio The BlockDBIO where we may have a custom class loader and do have a DBInput stream
	* @param iloc The location of the object to retrieve from backing store
	* @return The Object extracted from the backing store
	* @exception IOException if the op fails
	*/
	public synchronized Object deserializeObject(long iloc) throws IOException {
		// read Object at ptr to byte array
		int tblsp = GlobalDBIO.getTablespace(iloc);
		if(DEBUG)
			System.out.print(" Deserialize "
					+GlobalDBIO.valueOf(iloc)+" current block "+ioManager.getBlockStream(tblsp));
		Object Od = null;
		try {

			ObjectInput s;
			ioManager.objseek(iloc);
			if (isCustomClassLoader())
				s =	new CObjectInputStream(
						ioManager.getBlockStream(tblsp).getDBInput(),
						getCustomClassLoader());
			else
				s = new ObjectInputStream(ioManager.getBlockStream(tblsp).getDBInput());
			Od = s.readObject();
			s.close();		
			/*
			int tblsp = GlobalDBIO.getTablespace(iloc);
			DBSeekableByteChannel dbByteChannel = getDBByteChannel(tblsp);
			dbByteChannel.setBlockNumber(iloc);
			if(DEBUG)
				System.out.print(" Deserialize "+GlobalDBIO.valueOf(iloc)+" current block "+getBlockIndex()+" DUMP:"+getBlockIndex().getBlk().blockdump());
			Od = GlobalDBIO.deserializeObject(dbByteChannel);
			*/
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
		int tblsp = GlobalDBIO.getTablespace(iloc.getBlock());
		if(DEBUG)
			System.out.print(" Deserialize "
					+iloc+" current block "+ioManager.getBlockStream(tblsp));
		try {
			ObjectInput s;
			ioManager.objseek(iloc);
			if (isCustomClassLoader())
				s =	new CObjectInputStream(ioManager.getBlockStream(tblsp).getDBInput(), getCustomClassLoader());
			else
				s = new ObjectInputStream(ioManager.getBlockStream(tblsp).getDBInput());
			Od = s.readObject();
			s.close();	
			/*
			DBSeekableByteChannel dbByteChannel = getDBByteChannel(tblsp);
			dbByteChannel.setBlockNumber(iloc.getBlock());
			Od = GlobalDBIO.deserializeObject(dbByteChannel);
			*/
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
	
}
