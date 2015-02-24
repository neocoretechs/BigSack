package com.neocoretechs.bigsack.io.pooled;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.nio.channels.SeekableByteChannel;

import com.neocoretechs.bigsack.io.Optr;
import com.neocoretechs.bigsack.io.channel.DBSeekableByteChannel;
import com.neocoretechs.bigsack.io.stream.CObjectInputStream;

public final class ObjectDBIO extends OffsetDBIO {
	private static boolean DEBUG = false;
	public ObjectDBIO(String objname, String remoteObjName, boolean create, long transId) throws IOException {
		super(objname, remoteObjName, create, transId);
		setNew_node_pos_blk(-1L);
	}
	/**
	 * Connect without recovery log, to debug or for some read-only purpose
	 * @param dbname
	 * @throws IOException 
	 */
	public ObjectDBIO(String dbname, String remote) throws IOException {
		super(dbname, remote);
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
		objseek(loc);
		deleten(osize);
	}
	/**
	 * Add an object, which in this case is a load of bytes.
	 * @param loc Location to add this
	 * @param o The byte payload to add to pool
	 * @param osize  The size of the payload to add from array
	 * @exception IOException If the adding did not happen
	 */
	public synchronized void add_object(Optr loc, byte[] o, int osize) throws IOException {
		objseek(loc);
		writen(o, osize);
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
		Object Od = null;
		try {
			/*
			ObjectInput s;
			objseek(iloc);
			if (isCustomClassLoader())
				s =	new CObjectInputStream(
						getDBInput(),
						getCustomClassLoader());
			else
				s = new ObjectInputStream(getDBInput());
			Od = s.readObject();
			s.close();
			*/
			DBSeekableByteChannel dbByteChannel = getDBByteChannel();
			dbByteChannel.setBlockNumber(iloc);
			Od = GlobalDBIO.deserializeObject(dbByteChannel);
		} catch (IOException ioe) {
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
		try {
			/*
			ObjectInput s;
			objseek(iloc);
			if (isCustomClassLoader())
				s =	new CObjectInputStream(getDBInput(), getCustomClassLoader());
			else
				s = new ObjectInputStream(getDBInput());
			Od = s.readObject();
			s.close();
			*/
			DBSeekableByteChannel dbByteChannel = getDBByteChannel();
			dbByteChannel.setBlockNumber(iloc.getBlock());
			Od = GlobalDBIO.deserializeObject(dbByteChannel);
		} catch (IOException ioe) {
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
