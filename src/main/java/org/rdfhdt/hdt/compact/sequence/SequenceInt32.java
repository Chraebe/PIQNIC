/*
 * File: $HeadURL: https://hdt-java.googlecode.com/svn/trunk/hdt-java/src/org/rdfhdt/hdt/compact/sequence/SequenceInt32.java $
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

package org.rdfhdt.hdt.compact.sequence;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;
import java.util.Iterator;

import org.rdfhdt.hdt.exceptions.IllegalFormatException;
import org.rdfhdt.hdt.exceptions.NotImplementedException;
import org.rdfhdt.hdt.hdt.HDTVocabulary;
import org.rdfhdt.hdt.listener.ProgressListener;
import org.rdfhdt.hdt.util.io.IOUtil;

/**
 * Stream of Integer objects
 * 
 */
public class SequenceInt32 implements DynamicSequence {

	/** The array that holds the objects **/
	int [] data;
	int numelements;

	/**
	 * Basic constructor
	 * 
	 */
	public SequenceInt32() {
		this(10);
	}
	
	public SequenceInt32(int capacity) {
		data = new int[capacity];
		numelements = 0;
	}
	
	private void resizeArray(int size) {
		int [] newData = new int[size];
		System.arraycopy(data, 0, newData, 0, Math.min(newData.length, data.length));
		data = newData;
	}
	
	@Override
    public void trimToSize() {
		resizeArray(numelements);
	}
	
	@Override
    public void aggressiveTrimToSize() {
		trimToSize();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see hdt.triples.array.Stream#get(int)
	 */
	@Override
	public long get(long position) {
		if(position<0 || position>=numelements) {
			throw new IndexOutOfBoundsException();
		}

		return data[(int)position];
	}
	
	@Override
    public void set(long position, long value) {
		if(position<0 || position>=data.length) {
			throw new IndexOutOfBoundsException();
		}
		
		data[(int)position] = (int)value;
		numelements = (int) Math.max(numelements, position+1);
	}
	
	@Override
    public void append(long value) {
		assert value>=0 && value<=Integer.MAX_VALUE;
		
		if(data.length<numelements+1) {
			resizeArray(data.length*2);
		}
		data[numelements++] = (int) value;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see hdt.triples.array.Stream#getNumberOfElements()
	 */
	@Override
	public long getNumberOfElements() {
		return numelements;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see hdt.triples.array.Stream#save(java.io.OutputStream)
	 */
	@Override
	public void save(OutputStream output, ProgressListener listener) throws IOException {
		output.write(SequenceFactory.TYPE_SEQ32);
		IOUtil.writeInt(output, numelements);
		for (int i=0;i<numelements;i++) {
			IOUtil.writeInt(output, data[i]);
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see hdt.triples.array.Stream#load(java.io.InputStream)
	 */
	@Override
	public void load(InputStream input, ProgressListener listener) throws IOException {		
		int type = input.read();
		if(type!=SequenceFactory.TYPE_SEQ32) {
			throw new IllegalFormatException("Trying to read INT32 but data is not INT32");
		}
		
		int size = IOUtil.readInt(input);
		if(size<0) {
			throw new IOException("Cannot load values bigger than 2Gb with this data structure");
		}
		
		numelements = 0;
		data = new int[size];

		for(long i=0;i<size;i++) {
			append(IOUtil.readInt(input));
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see hdt.triples.array.Stream#add(int)
	 */
	@Override
	public void add(Iterator<Long> iterator) {
		while (iterator.hasNext()) {
			long value = iterator.next();
			append(value);
		}
	}

	public void addIntegers(List<Integer> elements) {
		for (int i=0;i<elements.size();i++){
			long value = elements.get(i).longValue();
			append(value);
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		throw new NotImplementedException();
	}

	/* (non-Javadoc)
	 * @see hdt.triples.array.Stream#size()
	 */
	@Override
	public long size() {
		return 4*numelements;
	}

	/* (non-Javadoc)
	 * @see hdt.compact.array.Stream#getType()
	 */
	@Override
	public String getType() {
		return HDTVocabulary.SEQ_TYPE_INT32;
	}

	@Override
	public void close() throws IOException {
		data=null;
	}
}
