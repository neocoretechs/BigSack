package com.neocoretechs.bigsack.io;

import java.io.IOException;
import java.nio.channels.Channel;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import com.neocoretechs.bigsack.io.pooled.GlobalDBIO;
import com.neocoretechs.bigsack.io.request.IoRequestInterface;

public class IOWorker implements Runnable, IoInterface {
	private static final boolean DEBUG = true;
	private IoInterface ioUnit;
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
	/**
	 * Queue a request on this worker, the request is assumed to be on this tablespace
	 * once the request is processed, a notify is issued on the request object
	 * @param irf
	 */
	public void queueRequest(IoRequestInterface irf) {
		irf.setIoInterface(ioUnit);
		irf.setTablespace(tablespace);
		if( DEBUG ) {
			System.out.println("Adding request "+irf+" size:"+requestQueue.size());
		}
		requestQueue.add(irf);
	}
	
	public int getRequestQueueLength() { return requestQueue.size(); }
	
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
	public boolean Fopen(String fname, boolean create) throws IOException {
		if (!ioUnit.Fopen(fname + "." + String.valueOf(tablespace), create))
			return false;
		return true;
	}
	
	@Override
	public void Fopen() throws IOException {
		Fopen(DBName, false);	
	}
	
	@Override
	public void Fclose() throws IOException {
			ioUnit.Fclose();
	}
	/**
	 * Return the position as a virtual block number
	 */
	@Override
	public long Ftell() throws IOException {
		return GlobalDBIO.makeVblock(tablespace, ioUnit.Ftell());
	}
	/**
	 * Seek the real offset, not virtual
	 */
	@Override
	public void Fseek(long offset) throws IOException {
		ioUnit.Fseek(offset);
	}
	@Override
	public long Fsize() throws IOException {
		return ioUnit.Fsize();
	}
	@Override
	public void Fset_length(long newlen) throws IOException {
		ioUnit.Fset_length(newlen);	
	}
	@Override
	public void Fforce() throws IOException {
		ioUnit.Fforce();
		
	}
	@Override
	public void Fwrite(byte[] obuf) throws IOException {
		ioUnit.Fwrite(obuf);	
	}
	@Override
	public void Fwrite(byte[] obuf, int osiz) throws IOException {
		ioUnit.Fwrite(obuf, osiz);
	}
	@Override
	public void Fwrite_int(int obuf) throws IOException {
		ioUnit.Fwrite_int(obuf);	
	}
	@Override
	public void Fwrite_long(long obuf) throws IOException {
		ioUnit.Fwrite_long(obuf);	
	}
	@Override
	public void Fwrite_short(short obuf) throws IOException {
		ioUnit.Fwrite_short(obuf);		
	}
	@Override
	public int Fread(byte[] b, int osiz) throws IOException {
		return ioUnit.Fread(b, osiz);
	}
	@Override
	public int Fread(byte[] b) throws IOException {
		return ioUnit.Fread(b);
	}
	@Override
	public long Fread_long() throws IOException {
		return ioUnit.Fread_long();
	}
	@Override
	public int Fread_int() throws IOException {
		return ioUnit.Fread_int();
	}
	@Override
	public short Fread_short() throws IOException {
		return ioUnit.Fread_short();
	}
	@Override
	public String FTread() throws IOException {
		return ioUnit.FTread();
	}
	@Override
	public void FTwrite(String ins) throws IOException {
		ioUnit.FTwrite(ins);	
	}
	@Override
	public void Fdelete() {
		ioUnit.Fdelete();	
	}
	@Override
	public String Fname() {
		return DBName;
	}
	@Override
	public boolean isopen() {
		return ioUnit.isopen();
	}
	@Override
	public boolean iswriteable() {
		return ioUnit.iswriteable();
	}
	@Override
	public boolean isnew() {
		return ioUnit.isnew();
	}
	@Override
	public Channel getChannel() {
		return ioUnit.getChannel();
	}


}
