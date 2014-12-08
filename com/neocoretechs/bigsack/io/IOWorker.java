package com.neocoretechs.bigsack.io;

import java.io.IOException;
import java.nio.channels.Channel;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import com.neocoretechs.bigsack.io.pooled.GlobalDBIO;
import com.neocoretechs.bigsack.io.request.IoRequestInterface;

public class IOWorker implements Runnable, IoInterface {
	private static final boolean DEBUG = false;
	private IoInterface ioUnit;
	private long nextFreeBlock = 0L;
	private BlockingQueue<IoRequestInterface> requestQueue;
	public boolean shouldRun = true;
	private int tablespace; // 0-7
	private String DBName;
	
	public IOWorker(String name, int tablespace, int L3cache) throws IOException {
		this.DBName = name;
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
		if (!ioUnit.Fopen(DBName + "." + String.valueOf(tablespace), true))
			throw new IOException("Cannot create tablespace!");
		requestQueue = new ArrayBlockingQueue<IoRequestInterface>(1024);
	}

	public long getNextFreeBlock() {
		return nextFreeBlock;
	}
	public void setNextFreeBlock(long nextFreeBlock) {
		this.nextFreeBlock = nextFreeBlock;
	}
	
	/**
	 * Queue a request on this worker, the request is assumed to be on this tablespace
	 * once the request is processed, a notify is issued on the request object
	 * @param irf
	 */
	public synchronized void queueRequest(IoRequestInterface irf) {
		irf.setIoInterface(ioUnit);
		irf.setTablespace(tablespace);
		if( DEBUG ) {
			System.out.println("Adding request "+irf+" size:"+requestQueue.size());
		}
		requestQueue.add(irf);
	}
	
	public synchronized int getRequestQueueLength() { return requestQueue.size(); }
	
	@Override
	public void run() {
		while(shouldRun) {
			try {
				IoRequestInterface iori = requestQueue.take();
				iori.process();
			} catch (InterruptedException e) {
				System.out.println("Request queue interrupt ");
				e.printStackTrace();
			} catch (IOException e) {
				System.out.println("Request queue exception "+e);
				e.printStackTrace();
			}
		}

	}
	
	@Override
	public synchronized boolean Fopen(String fname, boolean create) throws IOException {
		if (!ioUnit.Fopen(fname + "." + String.valueOf(tablespace), create))
			return false;
		return true;
	}
	
	@Override
	public synchronized void Fopen() throws IOException {
		Fopen(DBName, false);	
	}
	
	@Override
	public synchronized void Fclose() throws IOException {
			ioUnit.Fclose();
	}
	/**
	 * Return the position as a virtual block number
	 */
	@Override
	public synchronized long Ftell() throws IOException {
		return GlobalDBIO.makeVblock(tablespace, ioUnit.Ftell());
	}
	/**
	 * Seek the real offset, not virtual
	 */
	@Override
	public synchronized void Fseek(long offset) throws IOException {
		ioUnit.Fseek(offset);
	}
	@Override
	public synchronized long Fsize() throws IOException {
		return ioUnit.Fsize();
	}
	@Override
	public synchronized void Fset_length(long newlen) throws IOException {
		ioUnit.Fset_length(newlen);	
	}
	@Override
	public synchronized void Fforce() throws IOException {
		ioUnit.Fforce();
		
	}
	@Override
	public synchronized void Fwrite(byte[] obuf) throws IOException {
		ioUnit.Fwrite(obuf);	
	}
	@Override
	public synchronized void Fwrite(byte[] obuf, int osiz) throws IOException {
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
	public synchronized int Fread(byte[] b, int osiz) throws IOException {
		return ioUnit.Fread(b, osiz);
	}
	@Override
	public synchronized int Fread(byte[] b) throws IOException {
		return ioUnit.Fread(b);
	}
	@Override
	public synchronized long Fread_long() throws IOException {
		return ioUnit.Fread_long();
	}
	@Override
	public synchronized int Fread_int() throws IOException {
		return ioUnit.Fread_int();
	}
	@Override
	public synchronized short Fread_short() throws IOException {
		return ioUnit.Fread_short();
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
		return DBName;
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
	public synchronized boolean isnew() {
		return ioUnit.isnew();
	}
	@Override
	public synchronized Channel getChannel() {
		return ioUnit.getChannel();
	}


}
