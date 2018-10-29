/*
 * File: $HeadURL: https://hdt-java.googlecode.com/svn/trunk/hdt-java/src/org/rdfhdt/hdt/compact/bitmap/AdjacencyList.java $
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

package org.rdfhdt.hdt.compact.bitmap;

import org.rdfhdt.hdt.compact.sequence.Sequence;
import org.rdfhdt.hdt.exceptions.NotFoundException;

/**
 * @author mario.arias
 *
 */
public class AdjacencyList {
	private final Sequence array;
	private final Bitmap bitmap;
	
	/**
	 * @param array
	 * @param bitmap
	 */
	public AdjacencyList(Sequence array, Bitmap bitmap) {
		super();
		this.array = array;
		this.bitmap = bitmap;
		if(array.getNumberOfElements()!=bitmap.getNumBits()) {
			throw new IllegalArgumentException("Adjacency list bitmap and array should have the same size");
		}
	}

	/** 
	 * Find pos of the list x
	 * @param x
	 * @return
	 */
	public final long find(long x) {
		return x<=0 ? 0 : bitmap.select1(x)+1;
	}
	
	public final long findNext(long pos) {
		return bitmap.selectNext1(pos);
	}
	
	/**
	 * Find the last pos of the list x
	 * @param x
	 * @return
	 */
	public final long last(long x) {
		return bitmap.select1(x+1);
	}
	
	/**
	 * Find element y, in the list x
	 * @param x
	 * @param y
	 * @return
	 * @throws NotFoundException 
	 */
	public final long find(long x, long y) throws NotFoundException {
		// Find first and last element of the list.
		long begin = find(x);
		long end = last(x);
		// Binary search y within the list
		return binSearch(y, begin, end);
	}

	/**
	 * Find to which list x does the element at globalpos belongs
	 * @param globalpos
	 * @return
	 */
	public final long findListIndex(long globalpos) {
		return bitmap.rank1(globalpos-1);
	}
	
	/**
	 * Count how many lists there are
	 * @return
	 */
	public final long countListsX() {
		return bitmap.countOnes();
	}
	
	/** 
	 * Count the number of items in list x
	 * @param x
	 * @return
	 */
	public long countItemsY(long x) {
		return last(x)-find(x)+1;
	}
	
	public long search(long element, long begin, long end) throws NotFoundException {
		if(end-begin>10) {
			return binSearch(element, begin, end);
		} else {
			return linSearch(element, begin, end);
		}
	}
	
	public long binSearch(long element, long begin, long end) throws NotFoundException {
		while(begin<=end) {
			long mid = (begin+end)/2;
			long read = array.get(mid);
			if(element>read) {
				begin = mid+1;
			} else if(element<read) {
				end = mid-1;
			} else {
				return mid;
			}
		}
		throw new NotFoundException();
	}
	
	public long linSearch(long element, long begin, long end) throws NotFoundException {
		while(begin<=end) {
			long read = array.get(begin);
			if(read==element) {
				return begin;
			}
			begin++;
		}
		throw new NotFoundException();
	}
	
	public final long get(long pos) {
		return array.get(pos);
	}
	
	public final long getNumberOfElements() {
		return array.getNumberOfElements();
	}
	
	/**
	 * Finds the next appearance of the element "element" starting at global pos: "oldpos" inclusive.
	 * @param oldpos Old global position to start searching.
	 * @param element Element to be searched
	 * @return Position of the next appearance, -1 if no more appearances.
	 */

	public long findNextAppearance(long oldpos, long element) {
		// Keep doing binary search within each list until we find it.
//		while(true) {
//			long next = bitmap.selectNext1(oldpos+1);
//			if(next==-1) {
//				return -1;
//			}
////			System.out.println("Current: "+oldpos+ " Next: "+next);
//			try {
//				long pos = binSearch(element, oldpos, next);
//				return pos;
//			} catch (NotFoundException e) {
//				oldpos=next;
//			}
//		}
		
		// Sequential approach (Faster if lists are short due to caching).
		long pos = oldpos;
		long y;
		while(pos<array.getNumberOfElements()) {
			y = array.get(pos);
			if(y==element) {
				return pos;
			}
			pos++;
		}
		
		return -1;
	}
	
	/**
	 * Finds the previous appearance of the element "element" starting at global pos: "oldpos" inclusive.
	 * @param oldpos Old global position to start searching.
	 * @param element Element to be searched
	 * @return Position of the next appearance, -1 if no more appearances.
	 */
	public long findPreviousAppearance(long old, long element) {
		long y;
		do {
			y = array.get(old--);
		} while(old>=0 && y!=element);
		
		// No previous appearances of predicate
		if(old<0) {
			return -1;
		}
		return old+1;
	}
	
	public void dump() {
		for(long i=0;i<getNumberOfElements();i++) {
			System.out.print(" "+get(i));
			if(i!=getNumberOfElements()) {
				System.out.println(",");
			}
		}
		System.out.println("");
		
		for(long i=0;i<countListsX() && i<100; i++) {
			System.out.print("List "+i+" [");
			long base = find(i);
			long items = countItemsY(i);
			for(long j=0;j<countItemsY(i) && j<100; j++) {
				System.out.print(get(base+j));
				if(j!=items-1) {
					System.out.print(',');
				}
			}
			System.out.println("] ");
		}
		System.out.println();
	}
	
}
