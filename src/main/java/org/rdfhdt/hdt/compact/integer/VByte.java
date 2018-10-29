/*
 * File: $HeadURL: https://hdt-java.googlecode.com/svn/trunk/hdt-java/src/org/rdfhdt/hdt/compact/integer/VByte.java $
 * Revision: $Rev: 191 $
 * Last modified: $Date: 2013-03-03 11:41:43 +0000 (dom, 03 mar 2013) $
 * Last modified by: $Author: mario.arias $
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA  02110-1301  USA
 *
 * Contacting the authors:
 *   Mario Arias:               mario.arias@deri.org
 *   Javier D. Fernandez:       jfergar@infor.uva.es
 *   Miguel A. Martinez-Prieto: migumar2@infor.uva.es
 *   Alejandro Andres:          fuzzy.alej@gmail.com
 */

package org.rdfhdt.hdt.compact.integer;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;

import org.rdfhdt.hdt.util.Mutable;

/**
 * Typical implementation of Variable-Byte encoding for integers.
 * http://nlp.stanford.edu/IR-book/html/htmledition/variable-byte-codes-1.html
 * 
 * The first bit of each byte specifies whether there are more bytes available.
 * Numbers from 0 to 126 are encoded using just one byte.
 * Numbers from 127 to 16383 are encoded using two bytes.
 * Numbers from 16384 to 2097151 are encoded using three bytes.
 * 
 * @author mario.arias
 *
 */
public class VByte {
	
	private VByte() {}
	
	public static void encode(OutputStream out, long value) throws IOException {
		if(value<0) {
			throw new IllegalArgumentException("Only can encode VByte of positive values");
		}
		while( value > 127) {
			out.write((int)(value & 127));
			value>>>=7;
		}
		out.write((int)(value|0x80));
	}
	
	public static long decode(InputStream in) throws IOException {
		long out = 0;
		int shift=0;
		long readbyte = in.read(); if(readbyte==-1) throw new EOFException();
		
		while( (readbyte & 0x80)==0) {
			if(shift>=50) { // We read more bytes than required to load the max long
				throw new IllegalArgumentException();
			}
			
			out |= (readbyte & 127) << shift;
			
			readbyte = in.read(); if(readbyte==-1) throw new EOFException();
			
			shift+=7;
		}
		out |= (readbyte & 127) << shift;
		return out;
	}
	
	public static long decode(ByteBuffer in) throws IOException {
		long out = 0;
		int shift=0;
		if(!in.hasRemaining()) throw new EOFException();
		byte readbyte = in.get();
		
		while( (readbyte & 0x80)==0) {
			if(shift>=50) { // We read more bytes than required to load the max long
				throw new IllegalArgumentException();
			}
			
			out |= (readbyte & 127) << shift;
			
			if(!in.hasRemaining()) throw new EOFException();
			readbyte = in.get();
			
			shift+=7;
		}
		out |= (readbyte & 127) << shift;
		return out;
	}
	
	public static int encode(byte[] data, int offset, int value) {
		if(value<0) {
			throw new IllegalArgumentException("Only can encode VByte of positive values");
		}
		int i=0;
		while( value > 127) {
			data[offset+i] = (byte)(value & 127);
			i++;
			value>>>=7;
		}
		data[offset+i] = (byte)(value|0x80);
		i++;
		
		return i;
	}
	
	public static int decode(byte[] data, int offset, Mutable<Long> value) {
		long out = 0;
		int i=0;
		int shift=0;
		while( (0x80 & data[offset+i])==0) {
			out |= (data[offset+i] & 127) << shift;
			i++;
			shift+=7;
		}
		out |= (data[offset+i] & 127) << shift;
		i++;
		value.setValue(out);
		return i;
	}
	
	public static void show(byte[] data, int len) {
		for(int i=0;i<len;i++) {
			System.out.print(Long.toHexString(data[i]&0xFF)+" ");
		}
	}
}
