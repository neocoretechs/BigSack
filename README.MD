<h1>BigSack</h1>
<h2>Why BigSack?</h2>
A 'Sack' is a generic computer science term for a construct to hold a large amount of data that is usually unstructured.
'Big' here refers to the fact that the amount of data the system can maintain far exceeds resident and even virtual memory.
So in addition to BigSack having the means to store a large amount of unstructured data it adds the properties of recoverability,
isolation, durability, atomicity, concurrency, all while maintaining the transparency to underlying system design. So imagine the Java
TreeMap and TreeSet with some additional transaction semantics and storage subsystem needs and you have a good picture of this system.
<h2>Technical Details:</h2>
BigSack is a Java persistence mechanism that provides TreeSet and TreeMap key/value store functionality 
with a small footprint and exabyte object serialization capability. Serializable objects implementing the 
Comparable interface can be stored backed by a BTree and multiple levels of pooled tuneable cache.
Multiple memory mapped tablespace files facilitate high throughput to disk.
Durability is attained through the use of checkpointing and roll forward recovery utilizing the ARIES protocol.
<p/>
The disk structure for a 'database' is specified as follows from the example path "/home/db/TestDB":
/home/db/log - Contains the ARIES log files; control and mirror and log TestDB<tablespace>.log.
/home/db/tablespace0/TestDB.0 - Contains the tablespace files for tablespace 0 for databases
/home/db/tablespace0/TestDB.1 - Contains the tablespace files for tablespace 1 for databases
/home/db/tablespace0/TestDB.2 - Contains the tablespace files for tablespace 2 for databases
/home/db/tablespace0/TestDB.3 - Contains the tablespace files for tablespace 3 for databases
/home/db/tablespace0/TestDB.4 - Contains the tablespace files for tablespace 4 for databases
/home/db/tablespace0/TestDB.5 - Contains the tablespace files for tablespace 5 for databases
/home/db/tablespace0/TestDB.6 - Contains the tablespace files for tablespace 6 for databases
/home/db/tablespace0/TestDB.7 - Contains the tablespace files for tablespace 7 for databases
In a cluster configuration the tablespaces are resident on remote or local nodes connected by TCP sockets.
The tablespace structure is the same on nodes under Cluster configuration.

For further information see java.util.TreeMap and java.util.TreeSet for specific details of the API.
See the documentation for the ARIES protocol standard for further information on that subsystem.

<h2>Runtime Configuration:</h2>

To specify the properties file at runtime the -DBigSack.properties=/[path]/[to]/properties JVM parameter may be passed:
```
#
# Properties for the BigSack Java Deep K/V Store
#
#Model represents the execution model, Cluster UDP, Cluster TCP, Cluster MPI or Standalone
#Model: Standalone
#Model: Cluster UDP
Model: Cluster TCP
# Define the nodes for remote processing. These are the locations, by tablespace of the 'workboot' processes on remote worker nodes.
# If we wish to test in cluster mode we can start 8 processes locally. These are workboot processes activated by the following:
# workboot.sh:
# export LD_LIBRARY_PATH=/usr/share/openmpi/lib
# export PATH=$PATH:/usr/share/openmpi/bin
# /usr/bin/java -Xmx2048m -cp /usr/lib/mpi.jar:/usr/lib/BigSack.jar -DBigSack.properties=/usr/lib/BigSack.properties com.neocoretechs.bigsack.io.cluster.WorkBoot
# end of workboot.sh
# The remote nodes are defined as follows in this BigSack.properties file:
Nodes: localhost:8000,localhost:8001,localhost:8002,localhost:8003,localhost:8004,localhost:8005,localhost:8006,localhost:8007
# In the case of MPI the following definitions may be used but re not finalized:
#Model Cluster MPI
#Nodes:01237+tcp://...? MPI remote nodes?
#
# L3 cache is our final backing store; File or MMap. this can be changed at will
# File is filesystem based, 
#L3Cache: File
# MMap is memory-mapped demand paging, preferred for very large tables
L3Cache: MMap
#
# Number of pool blocks in runtime buffer pool (bytes=PoolBlocks*BlockSize)
# This can also be modified without fear.  A large pool increases performance at the cost of memory
#
PoolBlocks: 8192
#
# The BlockSize constant is dangerous, dont change it after creating a table.
# Try to use a blocksize that is 4 * your largest key size in serialized bytes. 
# Add up the size of your primitives and add about 20% for overhead. BigSack aligns B+Tree key pages on page boundaries
# so a block size too small causes excess paging, while one too large wastes space and causes unnecessary IO. Best to err on the side of 
# 'slightly too large' rather than the alternative.
# Table page size, or block size, in bytes. Some typical values are 1024, 2048, 4096. 8192 is probably as max as you may want for typical Java
# object. If you are using image bitmaps as keys, then you may have to increase it, but typically you make that a map with the key a smaller
# on-page element and the B+ tree storing the value off-keypage.
BlockSize: 2048
#
# Number of buckets in each tablespace.
# buckets in the BigSack constitute the pre-allocated storage blocks
# More buckets can increase performance of large datasets at the cost of overhead. This affects your
# runtime footprint by increasing the size of the per tablespace page buffers. Recall each 'Set'or 'Tree' or 'Table' or 'Database'
# depending on your nomenclature, has 8 tablespaces..
#
Buckets: 1024
# End
```

