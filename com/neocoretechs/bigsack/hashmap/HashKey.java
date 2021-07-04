package com.neocoretechs.bigsack.hashmap;
/**
 * Compute the hash key for a given integer hash code.<p/>
 * The method is to extract the first 3 bits as tablespace 0-7, then the next 5 bits (0-31) then the next 8 bits (0-256) value 
 * times the next 2 bytes 8bits x 2 (256,256). 4,294,967,296 possible keyspace.
 * Then the successive groups of bytes
 * are used as indexes to sublists. The third element of the array is the first page of each tablespace indexed
 * into 256 keys that point to pages within that tablespace. Each page of that is pointed to in turn by the
 * 256 keys to the next pages of the array. Until the 5 integer points to the actual entries.
 * So its a tree with a root, and internal nodes, and leaf nodes, much like a BTree.
 * @author Jonathan Groff (C) NeoCoreTechs 2021
 *
 */
public class HashKey {
	/**
	 * Compute a 5 element array from the integer hashkey the represents our tablespace/keypage positions.
	 * Essentially, a tree of depth 5.
	 * @param hashKey The hashkey from hashKey()
	 * @return the 5 element hash key space index
	 */
	public static int[] computeKey(int hashKey) {
		int[] result = new int[5];
		result[0] = (int) ((((long)hashKey) & 0xE0000000L) >> 29);// 0-7 3 bits = tablespace
		result[1] = (int) ((((long)hashKey) & 0x18000000L) >> 27);// 0-5 2 bits root
		result[2] = (int) ((((long)hashKey) & 0x07FC0000L) >> 18);// 0-511 9 bits after tablespace = root page 1
		result[3] = (int) (((long)(hashKey) & 0x0003FE00L) >> 9); // 0-511 9 bits root page 2
		result[4] = (int) ((long)(hashKey) & 0x000001FFL); //0-511 9 bits root page 3
		return result;
	}
}
