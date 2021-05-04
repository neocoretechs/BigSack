package com.neocoretechs.bigsack.io;

import java.io.IOException;
import java.util.Arrays;

import com.neocoretechs.bigsack.DBPhysicalConstants;
import com.neocoretechs.bigsack.io.pooled.GlobalDBIO;

/**
 * Free block allocator for all tablespaces of a particular database.
 * @author Jonathan Groff (C) NeoCoreTechs 2021
 *
 */
public final class FreeBlockAllocator {
	private static boolean DEBUG = false;
	private long[] nextFree = new long[DBPhysicalConstants.DTABLESPACES];
	private int roundRobinTablespace = -1;
	private IoManagerInterface ioManager;
	
	public FreeBlockAllocator(IoManagerInterface ioManager) {
		if(DEBUG)
			System.out.printf("%s created for ioManager:%s%n", this.getClass().getName(), ioManager);
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
		if(DEBUG)
			System.out.printf("%s nextTablespace %d for ioManager:%s%n", this.getClass().getName(), roundRobinTablespace, ioManager);
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
		if(DEBUG)
			System.out.printf("%s getNextFree roundRobinTablespace %d block %d for ioManager:%s%n", this.getClass().getName(), roundRobinTablespace, nextFree[roundRobinTablespace], ioManager);
		return nextFree[roundRobinTablespace];
	}
	
	public synchronized long getNextFree(int tblsp) throws IOException {
		if(DEBUG)
			System.out.printf("%s getNextFree tablespace %d block %d for ioManager:%s%n", this.getClass().getName(), tblsp, nextFree[tblsp], ioManager);
		return nextFree[tblsp];
	}
	/**
	 * Set the position of the next free block in the freechain
	 * @param tblsp Target tablespace
	 * @param longReturn The block virtual number
	 */
	public synchronized void setNextFree(int tblsp, long longReturn) {
		if(DEBUG)
			System.out.printf("%s setNextFree tablespace %d block %d for ioManager:%s%n", this.getClass().getName(), tblsp, longReturn, ioManager);
		nextFree[tblsp] = longReturn;	
	}
	/**
	 * Set the free blocks for initial bucket creation. Page 0 of tablespace 0 always has root node, so we
	 * allocate first free of that one to be at DBLOCKSIZ
	 */
	public synchronized void setNextFreeBlocks() {
		for (int i = 0; i < DBPhysicalConstants.DTABLESPACES; i++) {
			if (i == 0) {
				nextFree[i] = ((long) DBPhysicalConstants.DBLOCKSIZ);
			} else {
				nextFree[i] = 0L;
			}
			if(DEBUG)
				System.out.printf("%s setNextFreeBlocks tablespace %d block %d for ioManager:%s%n", this.getClass().getName(), i, nextFree[i], ioManager);
		}
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
		if(DEBUG)
			System.out.printf("%s findSmallestTablespace tablespace %d for ioManager:%s%n", this.getClass().getName(), smallestTablespace, ioManager);
		return smallestTablespace;
	}

	public synchronized int getTablespace() {
		return roundRobinTablespace;
	}

	public synchronized void setNextFree(long[] freeArray) {
		nextFree = freeArray;
		if(DEBUG)
			System.out.printf("%s setNextFree array %s for ioManager:%s%n", this.getClass().getName(), Arrays.toString(freeArray), ioManager);
	}
	
	
}
