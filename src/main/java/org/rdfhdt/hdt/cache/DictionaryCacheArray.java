/**
 * File: $HeadURL: https://hdt-java.googlecode.com/svn/trunk/hdt-jena/src/org/rdfhdt/hdtjena/cache/DictionaryCacheArray.java $
 * Revision: $Rev: 190 $
 * Last modified: $Date: 2013-03-03 11:30:03 +0000 (dom, 03 mar 2013) $
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
 */

package org.rdfhdt.hdt.cache;



/**
 * 
 * Cache using an array. Assumes valid id>0
 * 
 * @author mario.arias
 *
 */
public class DictionaryCacheArray<T> implements DictionaryCache<T> {

	private Object array[];
	final int capacity;
	int numentries=0;
	
	public DictionaryCacheArray(int capacity) {
		array = null;
		numentries=0;
		this.capacity=capacity;
	}
	
	/* (non-Javadoc)
	 * @see hdt.jena.DictionaryNodeCache#getNode(int)
	 */
	@SuppressWarnings("unchecked")
	@Override
	public T get(int id) {
		if(array==null) {
			return null;
		}
		if(id>array.length) {
			return null; // Not found
		}
		return (T) array[id-1];
	}
	
	public void put(int id, T node) {
		if(array==null) {
			array = new Object[(int)capacity];
		}
		if(array.length<id) {
//			System.err.println("Warning: Trying to insert a value in cache out of bounds: "+id + " / "+array.length);
			return;
		}
		if(array[id-1]==null) {
			numentries++;
		}
		array[id-1] = node;
	}

	@Override
	public int size() {
		return numentries;
	}

	@Override
	public void clear() {
		array=null;
	}

}
