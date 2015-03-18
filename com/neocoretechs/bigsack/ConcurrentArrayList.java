package com.neocoretechs.bigsack;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.neocoretechs.bigsack.io.pooled.BlockAccessIndex;

public class ConcurrentArrayList<T> {

    /** use this to lock for write operations like add/remove */
    private final Lock readLock;
    /** use this to lock for read operations like get/iterator/contains.. */
    private final Lock writeLock;
    /** the underlying list*/
    private final List<T> list;
    public ConcurrentArrayList() {
    	this(10);
    }

    public ConcurrentArrayList(int pOOLBLOCKS) {
    	list  = new ArrayList<T>(pOOLBLOCKS);
        ReentrantReadWriteLock rwLock = new ReentrantReadWriteLock();
        readLock = rwLock.readLock();
        writeLock = rwLock.writeLock();
	}

	public void add(T e){
        writeLock.lock();
        try{
            list.add(e);
        }finally{
            writeLock.unlock();
        }
    }

    public void get(int index){
        readLock.lock();
        try{
            list.get(index);
        }finally{
            readLock.unlock();
        }
    }

    public Iterator<T> iterator(){
        readLock.lock();
        try {
            return new ArrayList<T>( list ).iterator();
                   //^ we iterate over an snapshot of our list 
        } finally{
            readLock.unlock();
        }
    }

	public T remove(int i) {
		writeLock.lock();
	    try {
	            return list.remove(i);
	    } finally {
	            writeLock.unlock();
	    }
	}

	public int size() {
	     readLock.lock();
	      try {
	            return list.size();
	      } finally{
	            readLock.unlock();
	      }
	}
}
