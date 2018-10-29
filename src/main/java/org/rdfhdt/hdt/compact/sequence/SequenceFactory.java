/*
 * File: $HeadURL: https://hdt-java.googlecode.com/svn/trunk/hdt-java/src/org/rdfhdt/hdt/compact/sequence/SequenceFactory.java $
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

import java.io.File;
import java.io.IOException;
import java.io.InputStream;

import org.rdfhdt.hdt.exceptions.IllegalFormatException;
import org.rdfhdt.hdt.exceptions.NotImplementedException;
import org.rdfhdt.hdt.hdt.HDTVocabulary;
import org.rdfhdt.hdt.util.io.CountInputStream;

/**
 * @author mario.arias
 *
 */
public class SequenceFactory {
	public static final byte TYPE_SEQLOG = 1;
	public static final byte TYPE_SEQ32 = 2;
	public static final byte TYPE_SEQ64 = 3;
	
	private SequenceFactory() {}
	
	public static Sequence createStream(String name) {
		if(name==null) {
			return new SequenceLog64();
		} else if(name.equals(HDTVocabulary.SEQ_TYPE_INT32)) {
			return new SequenceInt32();
		} else if(name.equals(HDTVocabulary.SEQ_TYPE_INT64)) {
			return new SequenceInt64();
		} else if(name.equals(HDTVocabulary.SEQ_TYPE_LOG)) {
			return new SequenceLog64();
		}
		return new SequenceLog64();
	}
	
	public static Sequence createStream(InputStream input) throws IOException {
		input.mark(1);
		int type = input.read();
		input.reset();
		switch (type) {
		case TYPE_SEQLOG:
			return new SequenceLog64();
		case TYPE_SEQ32:
			return new SequenceInt32();
		case TYPE_SEQ64:
			return new SequenceLog64();
		default :
			throw new IllegalFormatException("Implementation not found for Sequence with code "+type);
		}		
	}
	
	public static Sequence createStream(CountInputStream input, File f) throws IOException {
		input.mark(1);
		int type = input.read();
		input.reset();
		switch (type) {
		case TYPE_SEQLOG:
			return new SequenceLog64Map(input, f);
		case TYPE_SEQ32:
//			return new SequenceInt32();
		case TYPE_SEQ64:
//			return new SequenceLog64();
			throw new NotImplementedException();
		default:
			throw new IllegalFormatException("Implementation not found for Sequence with code "+type);
		}
	}
	
}
