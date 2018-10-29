/*
 * File: $HeadURL: https://hdt-java.googlecode.com/svn/trunk/hdt-java/src/org/rdfhdt/hdt/util/RDFInfo.java $
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

package org.rdfhdt.hdt.util;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;

import org.rdfhdt.hdt.enums.RDFNotation;
import org.rdfhdt.hdt.exceptions.ParserException;
import org.rdfhdt.hdt.options.HDTOptions;
import org.rdfhdt.hdt.rdf.RDFParserCallback;

/**
 * A class for getting basic information about a file
 * 
 * @author Eugen
 *
 */
public class RDFInfo {

	public static final String size_prop = "rdf.sizeInBytes";
	public static final String triples_prop = "rdf.triples";
	public static final String compression_prop = "rdf.expectedCompression";
	
	private RDFInfo() {}

	/**
	 * Fills the given HDTSpecification object with the given information about the (RDF)
	 * file.
	 * 
	 * It sets these options: rdf.sizeInBytes, rdf.triples
	 * 
	 * WILL overwrite if options already set in specs.
	 *  
	 * @param specs HDTSpecification object to be filled
	 */
	public static void fillHDTSpecifications(long sizeInBytes, long numLines, HDTOptions specs){

		if (sizeInBytes<0)
			System.err.println("WARNING! Setting rdf.sizeInBytes to a negative value!");
		specs.setInt(size_prop, sizeInBytes);
		if (numLines<0)
			System.err.println("WARNING! Setting rdf.triples to a negative value!");
		specs.setInt(triples_prop, numLines);
	}

	/**
	 * Sets the "rdf.sizeInBytes" property, overwrites if existing
	 */
	public static void setSizeInBytes(long sizeInBytes, HDTOptions specs){
		specs.setInt(size_prop, sizeInBytes);
	}

	/**
	 * Sets the "rdf.triples" property, overwrites if existing
	 */
	public static void setTriples(long numLines, HDTOptions specs){
		specs.setInt(triples_prop, numLines);
	}
	
	/**
	 * Sets the "rdf.expectedCompression" property, overwrites if existing
	 */
	public static void setCompression(float compression, HDTOptions specs){
		specs.set(compression_prop, String.format("%.3f", compression));
	}

	/**
	 * Gets the "rdf.sizeInBytes" property and returns it if set, otherwise returns null.
	 */
	public static Long getSizeInBytes(HDTOptions specs){
		long x = specs.getInt(size_prop);
		if (x>0)
			return x;
		else
			return null;
	}

	/**
	 * Gets the "rdf.triples" property and returns it if set, otherwise returns null.
	 */
	public static long getTriples(HDTOptions specs){
		return specs.getInt(triples_prop);
	}
	
	/**
	 * Checks if "rdf.triples" property was set by the user
	 */
	public static boolean triplesSet(HDTOptions specs){
		if (specs.get(triples_prop)!=null)
			return true;
		else 
			return false;
	}
	
	/**
	 * Gets the "rdf.expectedCompression" property is it is set. If not sets it to 0.15 and returns that value.
	 */
	public static float getCompression(HDTOptions specs){
		float compression;
		try {
			compression = Float.parseFloat(specs.get(compression_prop));
		} catch (NumberFormatException e){
			System.err.println(compression_prop+" improperly set, using default 0.15");
			compression = 0.15f; //FIXME hard-coded default parameter here, not best of practices
			setCompression(compression, specs);
		} catch (NullPointerException e){
			System.err.println(compression_prop+" missing, using default 0.15");
			compression = 0.15f; //FIXME hard-coded default parameter here, not best of practices
			setCompression(compression, specs);
		}
		return compression;
	}

	/**
	 * Finds the length of file in bytes and counts the number of lines and fills
	 * the given specs with them using "fillHDTSpecification(long, int, HDTOptions)" method.
	 * 
	 */
	public static long countLines(String filename, RDFParserCallback parser, RDFNotation notation)
			throws FileNotFoundException, IOException, ParserException {
		InputStream is = new BufferedInputStream(new FileInputStream(filename));
	    try {
	        byte[] c = new byte[1024];
	        int count = 0;
	        int readChars;
	        boolean empty = true;
	        while ((readChars = is.read(c)) != -1) {
	            empty = false;
	            for (int i = 0; i < readChars; ++i) {
	                if (c[i] == '\n')
	                    ++count;
	            }
	        }
	        return (count == 0 && !empty) ? 1 : count;
	    } finally {
	        is.close();
	    }
	}
}
