/*
 * File: $HeadURL: https://hdt-java.googlecode.com/svn/trunk/hdt-java/src/org/rdfhdt/hdt/dictionary/impl/BaseDictionary.java $
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

package org.rdfhdt.hdt.dictionary.impl;

import org.rdfhdt.hdt.dictionary.DictionaryPrivate;
import org.rdfhdt.hdt.dictionary.DictionarySection;
import org.rdfhdt.hdt.dictionary.DictionarySectionPrivate;
import org.rdfhdt.hdt.enums.DictionarySectionRole;
import org.rdfhdt.hdt.enums.TripleComponentRole;
import org.rdfhdt.hdt.options.HDTOptions;
import org.rdfhdt.hdt.util.string.CompactString;
import org.rdfhdt.hdt.util.string.DelayedString;

/**
 * 
 * This abstract class implements all general methods that are the same
 * for every implementation of Dictionary.
 * 
 * @author mario.arias, Eugen
 *
 */
public abstract class BaseDictionary implements DictionaryPrivate {
	
	protected final HDTOptions spec;
	
	protected DictionarySectionPrivate subjects; 
	protected DictionarySectionPrivate predicates;
	protected DictionarySectionPrivate objects;
	protected DictionarySectionPrivate shared;
	
	public BaseDictionary(HDTOptions spec) {
		this.spec = spec;
	}
	
	protected int getGlobalId(int id, DictionarySectionRole position) {
		switch (position) {
		case SUBJECT:
		case OBJECT:
			return shared.getNumberOfElements()+id;
			
		case PREDICATE:
		case SHARED:	                
			return id;
		default:
			throw new IllegalArgumentException();
		}
	}

	protected int getLocalId(int id, TripleComponentRole position) {
		switch (position) {
		case SUBJECT:
		case OBJECT:
			if(id<=shared.getNumberOfElements()) {
				return id;
			} else {
				return id-shared.getNumberOfElements();
			}
		case PREDICATE:
			return id;
		default:
			throw new IllegalArgumentException();
		}
	}
	
	/* (non-Javadoc)
	 * @see hdt.dictionary.Dictionary#stringToId(java.lang.CharSequence, datatypes.TripleComponentRole)
	 */
	@Override
	public int stringToId(CharSequence str, TripleComponentRole position) {
		str = DelayedString.unwrap(str);

		if(str==null || str.length()==0) {
			return 0;
		}
				
		if(str instanceof String) {
			// CompactString is more efficient for the binary search.
			str = new CompactString(str);
		}

		int ret;
		switch(position) {
		case SUBJECT:
			ret = shared.locate(str);
			if(ret!=0) {
				return getGlobalId(ret, DictionarySectionRole.SHARED);
			}
			ret = subjects.locate(str);
			if(ret!=0) {
				return getGlobalId(ret, DictionarySectionRole.SUBJECT);
			}
			return -1;
		case PREDICATE:
			ret = predicates.locate(str);
			if(ret!=0) {
				return getGlobalId(ret, DictionarySectionRole.PREDICATE);
			}
			return -1;
		case OBJECT:
			if(str.charAt(0)!='"') {
				ret = shared.locate(str);
				if(ret!=0) {
					return getGlobalId(ret, DictionarySectionRole.SHARED);
				}
			}
			ret = objects.locate(str);
			if(ret!=0) {
				return getGlobalId(ret, DictionarySectionRole.OBJECT);
			}
			return -1;
		default:
			throw new IllegalArgumentException();
		}
	}	
	
	@Override
	public long getNumberOfElements() {
		return subjects.getNumberOfElements()+predicates.getNumberOfElements()+objects.getNumberOfElements()+shared.getNumberOfElements();
	}

	@Override
	public long size() {
		return subjects.size()+predicates.size()+objects.size()+shared.size();
	}

	@Override
	public long getNsubjects() {
		return subjects.getNumberOfElements()+shared.getNumberOfElements();
	}

	@Override
	public long getNpredicates() {
		return predicates.getNumberOfElements();
	}

	@Override
	public long getNobjects() {
		return objects.getNumberOfElements()+shared.getNumberOfElements();
	}

	@Override
	public long getNshared() {
		return shared.getNumberOfElements();
	}

	@Override
	public DictionarySection getSubjects() {
		return subjects;
	}
	
	@Override
	public DictionarySection getPredicates() {
		return predicates;
	}
	
	@Override
	public DictionarySection getObjects() {
		return objects;
	}
	
	@Override
	public DictionarySection getShared() {
		return shared;
	}
	
	private DictionarySectionPrivate getSection(int id, TripleComponentRole role) {
		switch (role) {
		case SUBJECT:
			if(id<=shared.getNumberOfElements()) {
				return shared;
			} else {
				return subjects;
			}
		case PREDICATE:
			return predicates;
		case OBJECT:
			if(id<=shared.getNumberOfElements()) {
				return shared;
			} else {
				return objects;
			}
		default:
			throw new IllegalArgumentException();
		}
	}

	/* (non-Javadoc)
	 * @see hdt.dictionary.Dictionary#idToString(int, datatypes.TripleComponentRole)
	 */
	@Override
	public CharSequence idToString(int id, TripleComponentRole role) {
		DictionarySectionPrivate section = getSection(id, role);
		int localId = getLocalId(id, role);
		return section.extract(localId);
	}
	
}