package com.neocoretechs.bigsack.io.channel;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;

import com.neocoretechs.bigsack.io.pooled.OffsetDBIOInterface;

public class DBSeekableByteChannel implements SeekableByteChannel {
	private OffsetDBIOInterface sdbio;
	private long blockNum;
	private int position = 0;
	public DBSeekableByteChannel(OffsetDBIOInterface sdbio) {
		this.sdbio = sdbio;
	}
	public void setBlockNumber(long bnum) {
		this.blockNum = bnum;
	}
	@Override
	public void close() throws IOException {
	}

	@Override
	public boolean isOpen() {
		return true;
	}

	@Override
	public long position() throws IOException {
		return position;
	}

	@Override
	public SeekableByteChannel position(long arg0) throws IOException {
		sdbio.objseek(blockNum);
		position = (int) arg0;
		if( position > 0 )
			sdbio.seek_fwd(position);
		return this;
	}

	@Override
	public int read(ByteBuffer arg0) throws IOException {
		int size = sdbio.readn(arg0, arg0.limit());
		position += size;
		return (size == 0 ? -1: size);
	}

	@Override
	public long size() throws IOException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public SeekableByteChannel truncate(long arg0) throws IOException {
		return this;
	}

	@Override
	public int write(ByteBuffer arg0) throws IOException {
		return sdbio.writen(arg0, arg0.limit());
	}

}
