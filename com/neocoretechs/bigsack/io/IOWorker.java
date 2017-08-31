package com.neocoretechs.bigsack.io;

import java.io.IOException;
import java.nio.channels.Channel;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import com.neocoretechs.bigsack.io.cluster.IOWorkerInterface;
import com.neocoretechs.bigsack.io.request.IoRequestInterface;
/**
 * This is the primordial IO worker. It exists in standalone mode as the primary threaded worker accessing
 * a particular tablespace from the requests placed in the ArrayBlockingQueue. It also exists in cluster mode
 * as the thread behind the TCPWorker which is derived from it. In cluster mode the WorkerRequestProcessor is an
 * additional thread that actually uses the IOWorker queue to queue requests back to the TCPWorker, an
 * intentional design compromise. 
 * The ioUnit is an IoInterface that connects to the underlying raw store, outside of the page/block pool/buffer
 * and provides the low level 'fread','fwrite','fseek' etc functions.
 * The request queue is initialized with a capacity of 1024 requests with 'put' operations blocking 
 * until a number below that are served.
 * @author jg
 *
 */
public class IOWorker implements Runnable, IoInterface, IOWorkerInterface {
	private static final boolean DEBUG = false;
	private static final int QUEUEMAX = 1024;
	private IoInterface ioUnit;
	private long nextFreeBlock = -1L;
	private BlockingQueue<IoRequestInterface> requestQueue;
	public boolean shouldRun = true;
	private int tablespace; // 0-7
	private String DBName;
	private String remoteDBName = null;
	/**
	 * Default Constructor. The purpose is to allow a cluster master to boot without
	 * reference to the locally mapped IO subsystem. In effect, the local IO operations
	 * are replaced with traffic to remote nodes instead. The requests themselves are similar
	 * but network bound ones add additional garnish
	 */
	public IOWorker() {
		requestQueue = new ArrayBlockingQueue<IoRequestInterface>(QUEUEMAX, true); // true maintains FIFO order
	}
	/**
	 * Create an IOWorker for the local store with the default of logs and tablespaces under
	 * the single directory structure in parameter 1
	 * @param name
	 * @param tablespace
	 * @param L3cache
	 * @throws IOException
	 */
	public IOWorker(String name, int tablespace, int L3cache) throws IOException {
		this.DBName = name;
		this.tablespace = tablespace;
		requestQueue = new ArrayBlockingQueue<IoRequestInterface>(QUEUEMAX, true);
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
			throw new IOException("IOWorker Cannot create tablespace "+tablespace+" using db:"+name);

	}
	
	/**
	 * Create an IOWorker for the local store with the separation of logs and tablespaces under
	 * the 2 directory structures in parameter 1 and 2
	 * @param name
	 * @param tablespace
	 * @param L3cache
	 * @throws IOException
	 */
	public IOWorker(String name, String remote, int tablespace, int L3cache) throws IOException {
		this.DBName = name;
		this.remoteDBName = remote;
		this.tablespace = tablespace;
		requestQueue = new ArrayBlockingQueue<IoRequestInterface>(QUEUEMAX, true);
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
		if (!ioUnit.Fopen(remoteDBName + "." + String.valueOf(tablespace), true))
			throw new IOException("IOWorker Cannot create tablespace "+tablespace+" for "+name+" using remote "+remote);

	}

	public synchronized long getNextFreeBlock() {
		return nextFreeBlock;
	}
	public synchronized void setNextFreeBlock(long nextFreeBlock) {
		this.nextFreeBlock = nextFreeBlock;
	}
	public synchronized BlockingQueue<IoRequestInterface> getRequestQueue() {
		return requestQueue;
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
		try {
			requestQueue.put(irf);
		} catch (InterruptedException e) {
			return; // executor shutdown in effect most likely
		}
	}
	
	public synchronized int getRequestQueueLength() { return requestQueue.size(); }
	
	@Override
	public void run() {
		while(shouldRun) {
			try {
				IoRequestInterface iori = requestQueue.take();
				iori.process();
			} catch (InterruptedException e) {
				 // most likely a shutdown request
			     // quit the processing thread
			     return;
			} catch (IOException e) {
				System.out.println("IOWorker Request queue exception "+e+" in db "+DBName+" tablespace "+tablespace);
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
	public void Fclose() throws IOException {
		synchronized(ioUnit) {
			ioUnit.Fclose();
		}
	}
	/**
	 * Return the position as a real block number
	 */
	@Override
	public long Ftell() throws IOException {
		synchronized(ioUnit) {
				return ioUnit.Ftell();
		}
	}
	/**
	 * Seek the real offset, not virtual
	 */
	@Override
	public void Fseek(long offset) throws IOException {
		if( DEBUG )
			System.out.println("IOWorker Fseek "+offset);
		synchronized(ioUnit) {
			ioUnit.Fseek(offset);
		}
	}
	@Override
	public long Fsize() throws IOException {
		if( DEBUG )
			System.out.println("IOWorker fsize "+ioUnit.Fsize());
		synchronized(ioUnit) {
			return ioUnit.Fsize();
		}
	}
	@Override
	public void Fset_length(long newlen) throws IOException {
		if( DEBUG )
			System.out.println("IOUnit Fset_length "+newlen);
		synchronized(ioUnit) {
			ioUnit.Fset_length(newlen);	
		}
	}
	@Override
	public void Fforce() throws IOException {
		if( DEBUG )
			System.out.println("IOWorker force ");
		synchronized(ioUnit) {
			ioUnit.Fforce();
		}
	}
	@Override
	public void Fwrite(byte[] obuf) throws IOException {
		if( DEBUG )
			System.out.println("IOWorker fwrite "+obuf.length+" @"+Ftell());
		synchronized(ioUnit) {
			ioUnit.Fwrite(obuf);
		}
	}
	@Override
	public void Fwrite(byte[] obuf, int osiz) throws IOException {
		if( DEBUG )
			System.out.println("IOWorker fwrite "+obuf.length+" @"+Ftell());
		synchronized(ioUnit) {
			ioUnit.Fwrite(obuf, osiz);
		}
	}
	@Override
	public void Fwrite_int(int obuf) throws IOException {
		synchronized(ioUnit) {
			ioUnit.Fwrite_int(obuf);
		}
	}
	@Override
	public void Fwrite_long(long obuf) throws IOException {
		synchronized(ioUnit) {
			ioUnit.Fwrite_long(obuf);
		}
	}
	@Override
	public void Fwrite_short(short obuf) throws IOException {
		synchronized(ioUnit) {
			ioUnit.Fwrite_short(obuf);
		}
	}
	@Override
	public void Fwrite_byte(byte keypage) throws IOException {
		if( DEBUG )
			System.out.println("IOWorker Fwrite_byte @"+Ftell());
		synchronized(ioUnit) {
			ioUnit.Fwrite_byte(keypage);
		}
	}
	@Override
	public int Fread(byte[] b, int osiz) throws IOException {
		if( DEBUG )
			System.out.println("IOWorker fread "+osiz+" @"+Ftell());
		synchronized(ioUnit) {
			return ioUnit.Fread(b, osiz);
		}
	}
	@Override
	public int Fread(byte[] b) throws IOException {
		if( DEBUG )
			System.out.println("IOWorker fread "+b.length+" @"+Ftell());
		synchronized(ioUnit) {
			return ioUnit.Fread(b);
		}
	}
	@Override
	public long Fread_long() throws IOException {
		if( DEBUG )
			System.out.println("IOWorker fread_long @"+Ftell());
		synchronized(ioUnit) {
			return ioUnit.Fread_long();
		}
	}
	@Override
	public int Fread_int() throws IOException {
		if( DEBUG )
			System.out.println("IOWorker fread_int @"+Ftell());
		synchronized(ioUnit) {
			return ioUnit.Fread_int();
		}
	}
	@Override
	public short Fread_short() throws IOException {
		if( DEBUG )
			System.out.println("IOWorker fread_short @"+Ftell());
		synchronized(ioUnit) {
			return ioUnit.Fread_short();
		}
	}
	@Override
	public byte Fread_byte() throws IOException {
		if( DEBUG )
			System.out.println("IOWorker Fread_byte @"+Ftell());
		synchronized(ioUnit) {
			return ioUnit.Fread_byte();
		}
	}
	@Override
	public synchronized String FTread() throws IOException {
		synchronized(ioUnit) {
			return ioUnit.FTread();
		}
	}
	@Override
	public void FTwrite(String ins) throws IOException {
		synchronized(ioUnit) {
			ioUnit.FTwrite(ins);
		}
	}
	@Override
	public void Fdelete() {
		synchronized(ioUnit) {
			ioUnit.Fdelete();
		}
	}
	@Override
	public synchronized String Fname() {
		return DBName;
	}
	@Override
	public boolean isopen() {
		synchronized(ioUnit) {
			return ioUnit.isopen();
		}
	}
	@Override
	public boolean iswriteable() {
		synchronized(ioUnit) {
			return ioUnit.iswriteable();
		}
	}
	@Override
	public boolean isnew() {
		synchronized(ioUnit) {
			return ioUnit.isnew();
		}
	}
	@Override
	public synchronized Channel getChannel() {
		synchronized(ioUnit) {
			return ioUnit.getChannel();
		}
	}

}
