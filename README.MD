<h1>BigSack - Now With Java 8 Streams!</h1>
<h3>Why BigSack?</h3>
A bag or sack is a computer science term for a structure to hold a large amount of data that is usually unstructured.
BigSack can store Java objects so that they can be efficiently indexed, preserved, and retrieved in a manner that mirrors the java.util.Hashmap, java.util.TreeMap and java.util.TreeSet classes while providing the benefits of a full blown database.
'Big' refers to the fact that the amount of data the system can maintain far exceeds resident and even virtual memory.
So in addition to BigSack having the means to store a large number of objects, it adds the properties of recoverability,
isolation, durability, atomicity, and concurrency. So imagine the Java HashMap,
TreeMap and TreeSet with transaction semantics and a storage subsystem with data recovery and the ability to scale to terabytes.<br/>

<h3>Technical Details:</h3>
BigSack is a Java persistence mechanism that provides HashMap, TreeSet and TreeMap key/value store functionality 
with a small footprint and terrabyte object storage capability. Just about any Java object, meaning Serializable objects implementing the 
java.lang.Comparable interface, can be stored, backed by a hashed page structure or BTree and multiple levels of pooled tuneable cache. The Comparable interface 
is part of the standard Java Collections Framework and is implemented in the majority of built-in Java classes such as String.<p/>

There are methods in the class com.neocoretechs.bigsack.BigSackAdapter to organize the maps and sets on the basis of type. In this way a rudimentary schema can be maintained. A non-transactional BufferedHashSet, BufferedTreeSet or BufferedTreeMap can be obtained by the following methods:

```

BigSackAdapter.setTableSpaceDir("/home/db/TestDB"); // sets the database name, which will be modified with class for each new class stored.
BufferedTreeSet bts = BigSackAdapter.getBigSackSet(Class.class); // implements Comparable and is a type of 'Class'. Or..
BufferedTreeMap btm = BigSackAdapter.getBigSackMap(Class.class);

```

The 'Buffered' semantics imply that a commit is performed automatically after each operation. If a failure occurs, recovery still happens.<br/>
If a transaction context is desired, in other words one in which multiple operations can be committed or rolled back under the control of the application, the following methods can be used:

```

BigSackAdapter.setTableSpaceDir("/home/db/TestDB"); // sets the database name, which will be modified with class for each new class stored.
TransactionalTreeSet tts = BigSackAdapter.getBigSackSetTransaction(Class1.class); // Or
TransactionalTreeMap ttm = BigSackAdapter.getBigSackMapTransaction(Class2.class);
tts.add(object);
ttm.put(key, value);
ttm.commit(); // Or
ttm.rollback(); // Or
ttm.checkpoint(); // establish intermediate checkpoint that can be committed or rolled back to

```

So if you were to store instances of a class named 'com.you.code' you would see files in each tablespace directory similar to "TestDBcom.you.code".
The typing is not strongly enforced, any key type can be inserted, but a means to manage types is provided that prevents exceptions being thrown in the 'compareTo' method.

In addition to the 'get','put','remove','contains','size','keySet','entrySet','contains','containsKey','first','last','firstKey','lastKey' the full set of
iterators can be obtained to retrieve subsets of the data for sets and maps:<br>
Sets:<br/>
headSet<br/>
tailSet<br/>
subSet<br/>
Maps:<br/>
headMap<br/>
headMapKV (key and value)<br/>
tailMap<br/>
tailMapKV<br/>
subMap<br/>
subMapKV<br/>
```
		TransactionalTreeSet session = BigSackAdapter.getBigSackSetTransaction(Class.forName(argv[1]));
		Object o;
		int i = 0;
		session.headSetStream((Comparable) session.first()).forEach(o ->System.out.println("["+(i++)+"]"+o));
		session.headSetStream((Comparable) session.first()).forEach(o -> {			
			System.out.println("["+(i++)+"]"+o);
			comparableClass.mapEntry = (Map.Entry) o;
			...
		});
		session.tailSetStream((Comparable) session.last()).forEach(o -> {
			System.out.println("["+(i++)+"]"+o);
		});
		session.subSetStream((Comparable) session.first(), (Comparable) session.last()).forEach(o -> {
			System.out.println("["+(i++)+"]"+o);
		});
```
<h3>Features:</h3>
<ul>
<li>Multiple memory mapped tablespace files facilitate high throughput to disk. Standard filesystem can also be specified at startup if desired.</li>
<li>Durability is attained through the use of checkpointing and roll forward recovery utilizing the ARIES protocol. The ARIES protocol is an industry
standard whose acronym means Algorithms for Recovery and Isolation Exploiting Semantics. ARIES is a protocol used in DB2, SQL Server, and other databases.
<li>Extensive use of multithreading makes maximum efficient use of resources. 
</li></ul>
<p/>
The disk structure for a 'database' is specified as follows from the example path "/home/db/TestDB":
<ul>
<li>/home/db/log - Contains the ARIES log files; control and mirror and log TestDB<tablespace>.log.</li>
<li>/home/db/tablespace0/TestDB.0 - Contains the tablespace files for tablespace 0 for databases</li>
<li>/home/db/tablespace0/TestDB.1 - Contains the tablespace files for tablespace 1 for databases</li>
<li>/home/db/tablespace0/TestDB.2 - Contains the tablespace files for tablespace 2 for databases</li>
<li>/home/db/tablespace0/TestDB.3 - Contains the tablespace files for tablespace 3 for databases</li>
<li>/home/db/tablespace0/TestDB.4 - Contains the tablespace files for tablespace 4 for databases</li>
<li>/home/db/tablespace0/TestDB.5 - Contains the tablespace files for tablespace 5 for databases</li>
<li>/home/db/tablespace0/TestDB.6 - Contains the tablespace files for tablespace 6 for databases</li>
<li>/home/db/tablespace0/TestDB.7 - Contains the tablespace files for tablespace 7 for databases</li>
</li></ul>
<p/>
In a cluster configuration the tablespaces are resident on remote or local nodes connected by TCP sockets.
The tablespace structure is the same on nodes under Cluster configuration.
<p/>
For further information see java.util.HashMap, java.util.TreeMap and java.util.TreeSet for specific details of the API.
See the documentation for the ARIES protocol standard for further information on that subsystem.
<p/>

