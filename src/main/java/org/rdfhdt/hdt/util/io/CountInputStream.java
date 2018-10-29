/*
 * File: $HeadURL: https://hdt-java.googlecode.com/svn/trunk/hdt-java/src/org/rdfhdt/hdt/util/io/CountInputStream.java $
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

package org.rdfhdt.hdt.util.io;

import java.io.IOException;
import java.io.InputStream;

/**
 * @author mck
 *
 */
public class CountInputStream extends InputStream {
	long total;
	long partial;
	final InputStream in;
	
	long markTotal;
	long markPartial;
	
	/**
	 * @param parent
	 */
	public CountInputStream(InputStream input) {
		this.in = input;
		total = 0;
		partial = 0;
	}
	
	public long getTotalBytes() {
		return total;
	}
	
	public long getPartialBytes() {
		return partial;
	}
	
	public void resetPartial() {
		partial = 0;
	}
	
	@Override
	public int read() throws IOException {
		int value = in.read();
		if(value!=-1) {
			partial++;
			total++;
		}
		return value;
	}
	
	@Override
	public int read(byte[] b) throws IOException {
		int value = in.read(b);
		if(value!=-1) {
			partial+=value;
			total+=value;
		}
		return value;
	}
	
	@Override
	public int read(byte[] b, int off, int len) throws IOException {
		int value = in.read(b, off, len);
		if(value!=-1) {
			partial+=value;
			total+=value;
		}
		return value;
	}
	
	@Override
	public long skip(long n) throws IOException {
		long skipped = in.skip(n);
		partial+=skipped;
		total+=skipped;
		return skipped;
	}
	
	@Override
	public void close() throws IOException {
		in.close();
	}
	
	@Override
	public void mark(int readlimit) {
		markTotal = total;
		markPartial = partial;
		if(in.markSupported())
			in.mark(readlimit);
	}
	
	@Override
	public boolean markSupported() {
		return in.markSupported();
	}
	
	@Override
	public void reset() throws IOException {
		total = markTotal;
		partial = markPartial;
		in.reset();
	}
}
