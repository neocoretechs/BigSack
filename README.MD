BigSack
BigSack is a Java persistence mechanism that provides TreeSet and TreeMap key/value store functionality 
with a small footprint and exabyte object serialization capability. Serializable objects implementing the 
Comparable interface can be stored backed by a BTree and multiple levels of pooled tuneable cache.
Durability is attained through the use of checkpointing and roll forward recovery utilizing the ARIES protocol.
See java.util.TreeMap and java.util.TreeSet for further details.
See the documentation for the ARIES protocol standard for further information on that subsystem.