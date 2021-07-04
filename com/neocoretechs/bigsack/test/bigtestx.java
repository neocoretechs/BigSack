package com.neocoretechs.bigsack.test;

import java.io.Serializable;
	/**
	 * Generic key , byte value payload block with various keys for test harnesses
	 * @author Jonathan Groff Copyright (C) NeoCoreTechs 2021
	 *
	 */
	class bigtestx implements Serializable, Comparable, TestSetInterface {
		private static final long serialVersionUID = 8890285185155972693L;
		public int payloadSize = 32767;
		byte[] b; 
		Comparable key = 0;
		//UUID uuid = null;
		//String randomUUIDString;
		public bigtestx() {
		}
		public void init(Comparable key, int payload) {
			//uuid = UUID.randomUUID();
			//randomUUIDString = uuid.toString();
			this.key = key;
			this.payloadSize = payload;
			b = new byte[payloadSize];
		}
		
		@Override
		public boolean equals(Object arg0) {
			//return randomUUIDString.compareTo(((bigtest)arg0).randomUUIDString);
			return key.compareTo(((bigtestx)arg0).key) == 0;
		}
		@Override
		public int hashCode() {
			return key.hashCode();
		}
		@Override
		public int compareTo(Object arg0) {
			//return randomUUIDString.compareTo(((bigtest)arg0).randomUUIDString);
			return key.compareTo(((bigtestx)arg0).key);
		}
		public String toString() { return String.valueOf(key); }//randomUUIDString; }
	}

