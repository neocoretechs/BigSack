package com.neocoretechs.bigsack.io.pooled;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;

import com.neocoretechs.bigsack.Props;
import com.neocoretechs.bigsack.btree.BTreeKeyPage;
import com.neocoretechs.bigsack.io.Optr;
import com.neocoretechs.bigsack.io.stream.CObjectInputStream;

public class ObjectDBIO extends OffsetDBIO {
	
	public ObjectDBIO(String objname, boolean create, long transId) throws IOException {
		super(objname, create, transId);
		// create or read the initial directory struct
		createOrLoad(create);
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
	public void delete_object(Optr loc, int osize) throws IOException {
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
	public void add_object(Optr loc, byte[] o, int osize)
		throws IOException {
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
	public Object deserializeObject(long iloc)
		throws IOException {
		// read Object at ptr to byte array
		Object Od;
		try {
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
		} catch (IOException ioe) {
			throw new IOException(
				"deserializeObject: "
					+ ioe.toString()
					+ ": Class Unreadable, may have been modified beyond version compatibility "
					+ GlobalDBIO.valueOf(iloc)+" in "+getDBName());
		} catch (ClassNotFoundException cnf) {
			throw new IOException(
				cnf.toString()
					+ ": Class Not found, may have been modified beyond version compatibility "+GlobalDBIO.valueOf(iloc)+" in "+getDBName());
		}
		if( Props.DEBUG ) System.out.println("From ptr "+GlobalDBIO.valueOf(iloc)+" Deserialized:\r\n "+Od);
		return Od;
	}
	/**
	* Read Object in pool: deserialize the byte array.
	* @param sdbio the session database IO object from which we get our DBInput stream and perhaps custom class loader
	* @param iloc The location of the object
	* @return the Object from dir. entry ptr.
	* @exception IOException if the op fails
	*/
	public Object deserializeObject(Optr iloc) throws IOException {
		// read Object at ptr to byte array
		Object Od;
		try {
			ObjectInput s;
			objseek(iloc);
			if (isCustomClassLoader())
				s =	new CObjectInputStream(getDBInput(), getCustomClassLoader());
			else
				s = new ObjectInputStream(getDBInput());
			Od = s.readObject();
			s.close();
		} catch (IOException ioe) {
			throw new IOException(
				"deserializeObject: "
					+ ioe.toString()
					+ ": Class Unreadable, may have been modified beyond version compatibility "
					+ iloc+" in "+getDBName());
		} catch (ClassNotFoundException cnf) {
			throw new IOException(
				cnf.toString()
					+ ": Class Not found, may have been modified beyond version compatibility "+iloc+" in "+getDBName());
		}
		if( Props.DEBUG ) System.out.println("From ptr "+iloc+" Deserialized:\r\n "+Od);
		return Od;
	}
	
	/**
	* create the directory linked list of blocks,
	* start a system-level session to do it
	* @param create true to create initial root
	* @exception IOException if we cannot create it
	*/
	public void createOrLoad(boolean create) throws IOException {
		if ( create && isNew ) { // determined in globalio ctor
			BTreeKeyPage broot = new BTreeKeyPage(0L);
			broot.setUpdated(true);
			broot.putPage(this);
			deallocOutstandingCommit();
		} //else
			//deallocOutstandingNoCommit();
		setNew_node_pos_blk(-1L);
	}
	
	public boolean isCustomClassLoader() {
		return isCustomClassLoader;
	}

	public void setCustomClassLoader(boolean isCustomClassLoader) {
		this.isCustomClassLoader = isCustomClassLoader;
	}

	public ClassLoader getCustomClassLoader() {
		return customClassLoader;
	}

	public void setCustomClassLoader(ClassLoader customClassLoader) {
		this.customClassLoader = customClassLoader;
	}


}
