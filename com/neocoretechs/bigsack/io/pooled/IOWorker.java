package com.neocoretechs.bigsack.io.pooled;

import java.io.File;
import java.io.IOException;
import java.nio.channels.Channel;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.Callable;

import com.neocoretechs.bigsack.DBPhysicalConstants;
import com.neocoretechs.bigsack.io.FileIO;
import com.neocoretechs.bigsack.io.IoInterface;
import com.neocoretechs.bigsack.io.MmapIO;

/**
 * This is the primordial IO worker. It exists in standalone mode as the primary worker accessing
 * a particular tablespace fulfilling the {@link IoInterface} contract.<p/>
 * The ioUnit is an IoInterface that connects to the underlying raw store, outside of the page/block pool/buffer
 * and provides the low level 'fread','fwrite','fseek' etc functions.<p/>
 * NOTE: references to page blocks is tablespace relative, NOT virtual.
 * @author Jonathan Groff Copyright (C) NeoCoreTechs 2021
 *
 */
public class IOWorker implements IoInterface { 
	private static final boolean DEBUG = false;
	private static final boolean DEBUGSEEK = false;
	private static final boolean DEBUGFREE = false;
	private IoInterface ioUnit;
	private int tablespace; // 0-7
	private GlobalDBIO sdbio;
	private LinkedHashMap<Long, BlockAccessIndex> freeBlockList; // set from free block allocation
	
	/**
	 * Create an IOWorker for the local store with the default of logs and tablespaces under
	 * the single directory structure.
	 * @param sdbio The global IO module.
	 * @param tablespace the tablespace we are ofcused on maintaining
	 * @param L3cache The type of primary backing store to utilize for page-level database data, be it filesystem, memory map, etc.
	 * @throws IOException
	 */
	public IOWorker(GlobalDBIO sdbio, int tablespace, int L3cache) throws IOException {
		this.sdbio = sdbio;
		this.tablespace = tablespace;
		switch (L3cache) {
			case 0 :
				ioUnit = new MmapIO();
				break;
			case 1 :
				ioUnit = new FileIO();
				break;
			default:
				throw new IOException("Unknown level 3 cache type, repair configuration file");
		}
		if (!ioUnit.Fopen(translateDb(sdbio.getDBName(),tablespace) + "." + String.valueOf(tablespace), true))
			throw new IOException("IOWorker Cannot create tablespace "+tablespace+" using db:"+translateDb(sdbio.getDBName(),tablespace));

	}
	
	public GlobalDBIO getGlobalDBIO() {
		return sdbio;
	}
	
	public Callable<Object> callGetNextFreeBlock(LinkedHashMap<Long, BlockAccessIndex> freeBlockList) { 
		return () -> {
			getNextFreeBlocks(freeBlockList);
			return true;
		};
	}
	
	/**
	* Set the next free block position from reverse scan of blocks. nextFreeBlock is tablespace relative, NOT virtual.
	* nextFreeBlock is guaranteed to be valid unless disk space is exhausted, which presumably would throw an exception.
	* @exception IOException if seek or size fails
	*/
	private void getNextFreeBlocks(LinkedHashMap<Long, BlockAccessIndex> freeBlockList) throws IOException {
		if(DEBUGFREE)
			System.out.printf("%s.getNextFreeBlocks freelist=%d%n", this.getClass().getName(), freeBlockList.size());
			this.freeBlockList = freeBlockList;
			// tablespace 0 end of rearward scan is block 2 otherwise 0, tablespace 0 has root node
			long endBlock = 0L;
			long endBl = ioUnit.Fsize();
			long nextFreeBlock = -1L;
			while (endBl > endBlock) {
				long startOfNextFreeBlock = endBl - (long) DBPhysicalConstants.DBLOCKSIZ;
				ioUnit.Fseek(startOfNextFreeBlock);
				Datablock d = new Datablock();
				d.read(ioUnit);
				if(d.isEmpty()) {
					endBl -= (long) DBPhysicalConstants.DBLOCKSIZ; // set up next block end
					nextFreeBlock = startOfNextFreeBlock; // global next free block, this one
					d.resetBlock();
					BlockAccessIndex bai = new BlockAccessIndex(sdbio, GlobalDBIO.makeVblock(tablespace, startOfNextFreeBlock), d);
					//ioUnit.FseekAndWrite(startOfNextFreeBlock, d); // ensure its reset with writethrough
					freeBlockList.put(nextFreeBlock, bai);
					if(DEBUGFREE)
						System.out.printf("%s.getNextFreeBlocks nextFreeBlock=%d freelist=%d%n", this.getClass().getName(),nextFreeBlock,freeBlockList.size());
				} else {
					break;
				}
			}
			if(nextFreeBlock == -1L) {
				sdbio.createBuckets(tablespace, freeBlockList, false);
			}
			//ioUnit.Fforce();
			if(DEBUGFREE)
				System.out.printf("%s.getNextFreeBlocks EXIT freelist=%d%n", this.getClass().getName(),freeBlockList.size());
	}
	
	protected String translateDb(String dbname, int tablespace) {
		String db;
        // replace any marker of $ with tablespace number
        if( dbname.indexOf('$') != -1) {
        	db = dbname.replace('$', String.valueOf(tablespace).charAt(0));
        } else
        	db = dbname;
        db = (new File(db)).toPath().getParent().toString() + File.separator +
        		"tablespace"+String.valueOf(tablespace) + File.separator +
        		(new File(dbname).getName());
        return db;
	}
	/**
	 * Get the next free block of minimum block number such that we return the free blocks in ascending order.
	 * @return the minimum block key so that when we do the next backward scan we recover maximum free blocks.
	 * @throws IOException
	 */
	public synchronized long getNextFreeBlock() throws IOException {
		if(freeBlockList.isEmpty())
			getNextFreeBlocks(freeBlockList);
		long min = freeBlockList.entrySet().stream().min(Map.Entry.comparingByKey()).get().getKey();
		return min;
	}

	
	@Override
	public synchronized boolean Fopen(String fname, boolean create) throws IOException {
			if (!ioUnit.Fopen(fname + "." + String.valueOf(tablespace), create))
				return false;
			return true;
	}
	
	@Override
	public synchronized void Fopen() throws IOException {
		Fopen(translateDb(sdbio.getDBName(), tablespace), false);	
	}
	
	public Callable<Object> callFclose = () -> {
		Fclose();
		return true;
	};
	
	@Override
	public synchronized void Fclose() throws IOException {
			ioUnit.Fclose();
	}
	/**
	 * Return the position as a real block number
	 */
	@Override
	public synchronized long Ftell() throws IOException {
				return ioUnit.Ftell();
	}
	
	@Override 
	public synchronized void FseekAndWriteFully(Long block, Datablock dblk) throws IOException {
        Fseek(block);
        dblk.write(this);
        Fforce();
        dblk.setIncore(false);
	}
	
	@Override 
	public synchronized void FseekAndWrite(Long block, Datablock dblk) throws IOException {
        Fseek(block);
        dblk.writeUsed(this);
        Fforce();
        dblk.setIncore(false);
	}
	/*
	Callable<Object> callFseekAndWrite(Long block, Datablock dblk) {
	    return () -> {
	        Fseek(block);
	        dblk.writeUsed(this);
	        Fforce();
	        dblk.setIncore(false);
	        return true;
	    };
	}
	*/
	@Override 
	public synchronized void FseekAndReadFully(Long block, Datablock dblk) throws IOException {
        Fseek(block);
        dblk.read(this);
        dblk.setIncore(false);
	}
	
	@Override 
	public synchronized void FseekAndRead(Long block, Datablock dblk) throws IOException {
        Fseek(block);
        dblk.readUsed(this);
        dblk.setIncore(false);
	}
	/**
	 * Seek the real offset, not virtual
	 */
	@Override
	public synchronized void Fseek(long offset) throws IOException {
		if( DEBUG || DEBUGSEEK)
			System.out.println("IOWorker Fseek "+offset);
		ioUnit.Fseek(offset);
	}
	@Override
	public synchronized long Fsize() throws IOException {
		if( DEBUG )
			System.out.println("IOWorker fsize "+ioUnit.Fsize());
		return ioUnit.Fsize();
	}
	@Override
	public synchronized void Fset_length(long newlen) throws IOException {
		if( DEBUG )
			System.out.println("IOUnit Fset_length "+newlen);
		ioUnit.Fset_length(newlen);	
		Fforce();
	}
	
	public Callable<Object> callFforce = () -> {
		Fforce();
		return true;
	};
	
	@Override
	public synchronized void Fforce() throws IOException {
		if( DEBUG )
			System.out.println("IOWorker force ");
		ioUnit.Fforce();
	}
	@Override
	public synchronized void Fwrite(byte[] obuf) throws IOException {
		if( DEBUG )
			System.out.println("IOWorker fwrite "+obuf.length+" @"+Ftell());
		ioUnit.Fwrite(obuf);
	}
	@Override
	public synchronized void Fwrite(byte[] obuf, int osiz) throws IOException {
		if( DEBUG )
			System.out.println("IOWorker fwrite "+obuf.length+" @"+Ftell());
		ioUnit.Fwrite(obuf, osiz);
	}
	@Override
	public synchronized void Fwrite_int(int obuf) throws IOException {
		ioUnit.Fwrite_int(obuf);
	}
	@Override
	public synchronized void Fwrite_long(long obuf) throws IOException {
			ioUnit.Fwrite_long(obuf);
	}
	@Override
	public synchronized void Fwrite_short(short obuf) throws IOException {
		ioUnit.Fwrite_short(obuf);
	}
	@Override
	public synchronized void Fwrite_byte(byte keypage) throws IOException {
		if( DEBUG )
			System.out.println("IOWorker Fwrite_byte @"+Ftell());
		ioUnit.Fwrite_byte(keypage);
	}
	@Override
	public synchronized int Fread(byte[] b, int osiz) throws IOException {
		if( DEBUG )
			System.out.println("IOWorker fread "+osiz+" @"+Ftell());
		return ioUnit.Fread(b, osiz);
	}
	@Override
	public synchronized int Fread(byte[] b) throws IOException {
		if( DEBUG )
			System.out.println("IOWorker fread "+b.length+" @"+Ftell());
		return ioUnit.Fread(b);
	}
	@Override
	public synchronized long Fread_long() throws IOException {
		if( DEBUG )
			System.out.println("IOWorker fread_long @"+Ftell());
		return ioUnit.Fread_long();
	}
	@Override
	public synchronized int Fread_int() throws IOException {
		if( DEBUG )
			System.out.println("IOWorker fread_int @"+Ftell());
		return ioUnit.Fread_int();
	}
	@Override
	public synchronized short Fread_short() throws IOException {
		if( DEBUG )
			System.out.println("IOWorker fread_short @"+Ftell());
		return ioUnit.Fread_short();
	}
	@Override
	public synchronized byte Fread_byte() throws IOException {
		if( DEBUG )
			System.out.println("IOWorker Fread_byte @"+Ftell());
		return ioUnit.Fread_byte();
	}
	@Override
	public synchronized String FTread() throws IOException {
			return ioUnit.FTread();
	}
	@Override
	public synchronized void FTwrite(String ins) throws IOException {
			ioUnit.FTwrite(ins);
	}
	@Override
	public synchronized void Fdelete() {
		ioUnit.Fdelete();
	}
	@Override
	public synchronized String Fname() {
		return translateDb(sdbio.getDBName(),tablespace);
	}
	@Override
	public synchronized boolean isopen() {
			return ioUnit.isopen();
	}
	@Override
	public synchronized boolean iswriteable() {
			return ioUnit.iswriteable();
	}
	@Override
	public synchronized Channel getChannel() {
		return ioUnit.getChannel();
	}

	public boolean isnew() {
		return ioUnit.isnew();
	}

	@Override
	public void FseekAndWriteHeader(Long block, Datablock dblk) throws IOException {
		ioUnit.FseekAndWriteHeader(block, dblk);	
	}

	@Override
	public void FseekAndReadHeader(Long block, Datablock dblk) throws IOException {
		ioUnit.FseekAndReadHeader(block, dblk);
	}



}
