/*
 * File: $HeadURL: https://hdt-java.googlecode.com/svn/trunk/hdt-java/iface/org/rdfhdt/hdt/triples/TripleID.java $
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

package org.rdfhdt.hdt.triples;

import java.io.Serializable;



/**
 * TripleID holds a triple as integers
 * 
 */
public final class TripleID implements Comparable<TripleID>, Serializable {
	private static final long serialVersionUID = -4685524566493494912L;
	
	private int subject;
	private int predicate;
	private int object;

	/**
	 * Basic constructor
	 */
	public TripleID() {
		super();
	}

	/**
	 * Constructor
	 * 
	 * @param subject
	 *            The subject
	 * @param predicate
	 *            The predicate
	 * @param object
	 *            The object
	 */
	public TripleID(int subject, int predicate, int object) {
		super();
		this.subject = subject;
		this.predicate = predicate;
		this.object = object;
	}
	
	/**
	 * Build a TripleID as a copy of another one.
	 * @param other
	 */
	public TripleID(TripleID other) {
		super();
		this.subject = other.subject;
		this.predicate = other.predicate;
		this.object = other.object;
	}

	/**
	 * @return the subject
	 */
	public int getSubject() {
		return subject;
	}

	/**
	 * @param subject
	 *            the subject to set
	 */
	public void setSubject(int subject) {
		this.subject = subject;
	}

	/**
	 * @return the object
	 */
	public int getObject() {
		return object;
	}

	/**
	 * @param object
	 *            the object to set
	 */
	public void setObject(int object) {
		this.object = object;
	}

	/**
	 * @return the predicate
	 */
	public int getPredicate() {
		return predicate;
	}

	/**
	 * @param predicate
	 *            the predicate to set
	 */
	public void setPredicate(int predicate) {
		this.predicate = predicate;
	}

	/**
	 * Replace all components of a TripleID at once. Useful to reuse existing objects.
	 * @param subject
	 * @param predicate
	 * @param object
	 */
	public void setAll(int subject, int predicate, int object) {
		this.subject = subject;
		this.predicate = predicate;
		this.object = object;
	}
	
	public void assign(TripleID replacement) {
		subject = replacement.getSubject();
        object = replacement.getObject();
        predicate = replacement.getPredicate();
	}

	/**
	 * Set all components to zero.
	 */
	public void clear() {
		subject = predicate = object = 0;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return Integer.toString(subject) + " " + predicate + " " + object;
	}
	
	public boolean equals(TripleID other) {
		return !( subject!=other.subject || predicate!=other.predicate || object!=other.object );
	}

	/**
	 * Compare TripleID to another one using SPO Order. 
	 * To compare using other orders use {@link TripleStringComparator} 
	 */
	@Override
	public int compareTo(TripleID other) {
		 int result = this.subject - other.subject;

         if(result==0) {
                 result = this.predicate - other.predicate;
                 if(result==0) {
                         return this.object - other.object;
                 } else {
                         return result;
                 }
         } else {
                 return result;
         }
	}
	
	/**
	 * Check whether this triple matches a pattern of TripleID. 0 acts as a wildcard
	 * 
	 * @param pattern
	 *            The pattern to match against
	 * @return boolean
	 */
	public boolean match(TripleID pattern) {

		// get the components of the pattern
		int subjectPattern = pattern.getSubject();
		int predicatePattern = pattern.getPredicate();
		int objectPattern = pattern.getObject();

		/* Remember that 0 acts as a wildcard */
		if (subjectPattern == 0 || this.subject == subjectPattern) {
			if (predicatePattern == 0 || this.predicate == predicatePattern) {
				if (objectPattern == 0 || this.object == objectPattern) {
					return true;
				}
			}
		}
		return false;
	}
	
	/**
	 * Check whether all the components of the triple are empty (zero).
	 * @return
	 */
	public boolean isEmpty() {
		return !(subject != 0 || predicate != 0 || object != 0);
	}

	/**
	 * Check whether none of the components of the triple are empty.
	 * @return
	 */
	public boolean isValid() {
		return subject>0 && predicate>0 && object>0;
	}
	
	/**
	 * Checks whether any of the components of the triple are "no match" (-1).
	 * @return
	 */
	public boolean isNoMatch() {
		return subject == -1 || predicate == -1 || object == -1;
	}

	/**
	 * Get the pattern of the triple as String, such as "SP?".
	 * @return
	 */
	public String getPatternString() {
		return "" +
			(subject==0   ? '?' : 'S') + 
			(predicate==0 ? '?' : 'P') +
			(object==0    ? '?' : 'O');
	}
	
	/** size of one TripleID in memory */
	public static int size(){
		return 24;
	}

}
