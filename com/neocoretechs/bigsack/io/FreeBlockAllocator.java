package com.neocoretechs.bigsack.io;

import java.io.IOException;

import com.neocoretechs.bigsack.DBPhysicalConstants;
import com.neocoretechs.bigsack.io.pooled.GlobalDBIO;

/**
 * Free block allocator for all tablespaces of a particular database.
 * @author jg
 *
 */
public class FreeBlockAllocator {
	long[] nextFree = new long[DBPhysicalConstants.DTABLESPACES];
	int roundRobinTablespace = -1;
	IoManagerInterface ioManager;
	
	public FreeBlockAllocator(IoManagerInterface ioManager) {
		this.ioManager = ioManager;
	}
	
	public synchronized int nextTablespace() throws IOException {
		if( roundRobinTablespace == -1) {
			roundRobinTablespace = findSmallestTablespace(ioManager.Fsize(0));
		} else
			++roundRobinTablespace;
		if( roundRobinTablespace == DBPhysicalConstants.DTABLESPACES ) {
			roundRobinTablespace = 0;
		}
		return roundRobinTablespace;
	}
	/**
	 * Get next free block from given tablespace
	 * @param tblsp
	 * @return
	 * @throws IOException 
	 */
	public synchronized long getNextFree() throws IOException {
		nextTablespace();
		return nextFree[roundRobinTablespace];
	}
	
	public synchronized long getNextFree(int tblsp) throws IOException {
		return nextFree[tblsp];
	}
	/**
	 * Set the position of the next free block in the freechain
	 * @param tblsp Target tablespace
	 * @param longReturn The block virtual number
	 */
	public synchronized void setNextFree(int tblsp, long longReturn) {
		nextFree[tblsp] = longReturn;	
	}
	/**
	 * Set the free blocks for initial bucket creation. Page 0 of tablespace 0 always has root node, so we
	 * allocate first free of that one to be at DBLOCKSIZ
	 */
	public synchronized void setNextFreeBlocks() {
		for (int i = 0; i < DBPhysicalConstants.DTABLESPACES; i++)
			if (i == 0)
				nextFree[i] = ((long) DBPhysicalConstants.DBLOCKSIZ);
			else
				nextFree[i] = 0L;
	}
	
	/**
	 * Find the smallest tablespace. Presumably to do an insert. Attempt to keep
	 * pages somewhat balanced.
	 * @param primarySize The size of primary tablespace 0 to use as a reference point.
	 * @return
	 */
	public synchronized int findSmallestTablespace(long primarySize) {
		int smallestTablespace = 0; // default main
		long smallestSize = primarySize;
		for (int i = 0; i < nextFree.length; i++) {
			if(nextFree[i] != -1 && GlobalDBIO.getBlock(nextFree[i]) < smallestSize) {
				smallestSize = GlobalDBIO.getBlock(nextFree[i]);
				smallestTablespace = i;
			}
		}
		return smallestTablespace;
	}

	public synchronized int getTablespace() {
		return roundRobinTablespace;
	}

	public synchronized void setNextFree(long[] freeArray) {
		nextFree = freeArray;	
	}
	
	
}
