package com.neocoretechs.bigsack.iterator;
import java.io.Serializable;
import java.util.Map;
/*
* Copyright (c) 1997,2003, NeoCoreTechs
* All rights reserved.
* Redistribution and use in source and binary forms, with or without modification, 
* are permitted provided that the following conditions are met:
*
* Redistributions of source code must retain the above copyright notice, this list of
* conditions and the following disclaimer. 
* Redistributions in binary form must reproduce the above copyright notice, 
* this list of conditions and the following disclaimer in the documentation and/or
* other materials provided with the distribution. 
* Neither the name of NeoCoreTechs nor the names of its contributors may be 
* used to endorse or promote products derived from this software without specific prior written permission. 
* THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED 
* WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A 
* PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE LIABLE FOR 
* ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED
* TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
* HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT 
* (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED 
* OF THE POSSIBILITY OF SUCH DAMAGE.
*
*/
/**
* Key-Value pair to be stored in persistent collection.
* Collections are transparent to type, so we provide this wrapper to give a key-value type
* if necessary.
* @author Jonathan Groff Copyright (C) NeoCoreTechs 2021
*/
@SuppressWarnings("rawtypes")
public class KeyValuePair implements Serializable, Comparable, Map.Entry {
        static final long serialVersionUID = -927653922205205452L;
        public Object key, value;
        public KeyValuePair(Object tkey, Object tvalue) {
                key = tkey;
                value = tvalue;
        }
        public Object getKey() { return key; }
        public Object getValue() { return value; }
        public Object setValue(Object o) {
                throw new UnsupportedOperationException("Map.Entry write-through not supported");
        }
        @SuppressWarnings({ "unchecked" })
		public int compareTo(Object tobj) {
                return ((Comparable)key).compareTo(((KeyValuePair)tobj).key);
        }
        public String toString() { return "KeyValuePair:["+key+","+value+"]"; }
}
