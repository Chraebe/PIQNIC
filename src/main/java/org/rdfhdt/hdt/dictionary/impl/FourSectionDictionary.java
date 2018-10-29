/*
 * File: $HeadURL: https://hdt-java.googlecode.com/svn/trunk/hdt-java/src/org/rdfhdt/hdt/dictionary/impl/FourSectionDictionary.java $
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

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.rdfhdt.hdt.dictionary.DictionarySectionPrivate;
import org.rdfhdt.hdt.dictionary.TempDictionary;
import org.rdfhdt.hdt.dictionary.impl.section.DictionarySectionFactory;
import org.rdfhdt.hdt.dictionary.impl.section.PFCDictionarySection;
import org.rdfhdt.hdt.exceptions.IllegalFormatException;
import org.rdfhdt.hdt.hdt.HDTVocabulary;
import org.rdfhdt.hdt.header.Header;
import org.rdfhdt.hdt.listener.ProgressListener;
import org.rdfhdt.hdt.options.ControlInfo;
import org.rdfhdt.hdt.options.ControlInfo.Type;
import org.rdfhdt.hdt.options.ControlInformation;
import org.rdfhdt.hdt.options.HDTOptions;
import org.rdfhdt.hdt.util.io.CountInputStream;
import org.rdfhdt.hdt.util.listener.IntermediateListener;


/**
 * @author mario.arias
 *
 */
public class FourSectionDictionary extends BaseDictionary {

	public FourSectionDictionary(HDTOptions spec, 
			DictionarySectionPrivate s, DictionarySectionPrivate p, DictionarySectionPrivate o, DictionarySectionPrivate sh) {
		super(spec);
		this.subjects = s;
		this.predicates = p;
		this.objects = o;
		this.shared = sh;
	}
	
	public FourSectionDictionary(HDTOptions spec) {
		super(spec);
		// FIXME: Read type from spec.
		subjects = new PFCDictionarySection(spec);
		predicates = new PFCDictionarySection(spec);
		objects = new PFCDictionarySection(spec);
		shared = new PFCDictionarySection(spec);
	}

	/* (non-Javadoc)
	 * @see hdt.dictionary.Dictionary#load(hdt.dictionary.Dictionary)
	 */
	@Override
	public void load(TempDictionary other, ProgressListener listener) {
		IntermediateListener iListener = new IntermediateListener(listener);
		subjects.load(other.getSubjects(), iListener);
		predicates.load(other.getPredicates(), iListener);
		objects.load(other.getObjects(), iListener);
		shared.load(other.getShared(), iListener);
	}

	/* (non-Javadoc)
	 * @see hdt.dictionary.Dictionary#save(java.io.OutputStream, hdt.ControlInformation, hdt.ProgressListener)
	 */
	@Override
	public void save(OutputStream output, ControlInfo ci, ProgressListener listener) throws IOException {
		ci.setType(Type.DICTIONARY);
		ci.setFormat(getType());
		ci.setInt("elements", this.getNumberOfElements());
		ci.save(output);

		IntermediateListener iListener = new IntermediateListener(listener);
		shared.save(output, iListener);
		subjects.save(output, iListener);
		predicates.save(output, iListener);
		objects.save(output, iListener);

	}

	/* (non-Javadoc)
	 * @see hdt.dictionary.Dictionary#load(java.io.InputStream)
	 */
	@Override
	public void load(InputStream input, ControlInfo ci, ProgressListener listener) throws IOException {
		if(ci.getType()!=ControlInfo.Type.DICTIONARY) {
			throw new IllegalFormatException("Trying to read a dictionary section, but was not dictionary.");
		}
		
		IntermediateListener iListener = new IntermediateListener(listener);

		shared = DictionarySectionFactory.loadFrom(input, iListener);
		subjects = DictionarySectionFactory.loadFrom(input, iListener);
		predicates = DictionarySectionFactory.loadFrom(input, iListener);
		objects = DictionarySectionFactory.loadFrom(input, iListener);
	}
	
	@Override
	public void mapFromFile(CountInputStream in, File f, ProgressListener listener) throws IOException {
		ControlInformation ci = new ControlInformation();
		ci.load(in);
		if(ci.getType()!=ControlInfo.Type.DICTIONARY) {
			throw new IllegalFormatException("Trying to read a dictionary section, but was not dictionary.");
		}
		
		IntermediateListener iListener = new IntermediateListener(listener);
		shared = DictionarySectionFactory.loadFrom(in, f, iListener);
		subjects = DictionarySectionFactory.loadFrom(in, f, iListener);
		predicates = DictionarySectionFactory.loadFrom(in, f, iListener);
		objects = DictionarySectionFactory.loadFrom(in, f, iListener);
		
		// Use cache only for predicates. Preload only up to 100K predicates.
		// FIXME: DISABLED
//		predicates = new DictionarySectionCacheAll(predicates, predicates.getNumberOfElements()<100000);
	}

	/* (non-Javadoc)
	 * @see hdt.dictionary.Dictionary#populateHeader(hdt.header.Header, java.lang.String)
	 */
	@Override
	public void populateHeader(Header header, String rootNode) {
		header.insert(rootNode, HDTVocabulary.DICTIONARY_TYPE, getType());
//		header.insert(rootNode, HDTVocabulary.DICTIONARY_NUMSUBJECTS, getNsubjects());
//		header.insert(rootNode, HDTVocabulary.DICTIONARY_NUMPREDICATES, getNpredicates());
//		header.insert(rootNode, HDTVocabulary.DICTIONARY_NUMOBJECTS, getNobjects());
		header.insert(rootNode, HDTVocabulary.DICTIONARY_NUMSHARED, getNshared());
//		header.insert(rootNode, HDTVocabulary.DICTIONARY_MAXSUBJECTID, getMaxSubjectID());
//		header.insert(rootNode, HDTVocabulary.DICTIONARY_MAXPREDICATEID, getMaxPredicateID());
//		header.insert(rootNode, HDTVocabulary.DICTIONARY_MAXOBJECTTID, getMaxObjectID());
		header.insert(rootNode, HDTVocabulary.DICTIONARY_SIZE_STRINGS, size());
	}

	/* (non-Javadoc)
	 * @see hdt.dictionary.Dictionary#getType()
	 */
	@Override
	public String getType() {
		return HDTVocabulary.DICTIONARY_TYPE_FOUR_SECTION;
	}

	@Override
	public void close() throws IOException {
		shared.close();
		subjects.close();
		predicates.close();
		objects.close();
	}
}
