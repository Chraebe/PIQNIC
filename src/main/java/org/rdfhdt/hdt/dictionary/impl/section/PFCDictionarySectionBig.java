/*
 * File: $HeadURL: https://hdt-java.googlecode.com/svn/trunk/hdt-java/src/org/rdfhdt/hdt/dictionary/impl/section/PFCDictionarySectionBig.java $
 * Revision: $Rev: 194 $
 * Last modified: $Date: 2013-03-04 21:30:01 +0000 (lun, 04 mar 2013) $
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

package org.rdfhdt.hdt.dictionary.impl.section;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Iterator;

import org.rdfhdt.hdt.compact.integer.VByte;
import org.rdfhdt.hdt.compact.sequence.SequenceLog64Big;
import org.rdfhdt.hdt.dictionary.DictionarySectionPrivate;
import org.rdfhdt.hdt.dictionary.TempDictionarySection;
import org.rdfhdt.hdt.exceptions.CRCException;
import org.rdfhdt.hdt.exceptions.IllegalFormatException;
import org.rdfhdt.hdt.listener.ProgressListener;
import org.rdfhdt.hdt.options.HDTOptions;
import org.rdfhdt.hdt.util.BitUtil;
import org.rdfhdt.hdt.util.Mutable;
import org.rdfhdt.hdt.util.crc.CRC32;
import org.rdfhdt.hdt.util.crc.CRC8;
import org.rdfhdt.hdt.util.crc.CRCInputStream;
import org.rdfhdt.hdt.util.crc.CRCOutputStream;
import org.rdfhdt.hdt.util.io.IOUtil;
import org.rdfhdt.hdt.util.string.ByteStringUtil;
import org.rdfhdt.hdt.util.string.CompactString;
import org.rdfhdt.hdt.util.string.ReplazableString;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementation of Plain Front Coding that divides the data in different arrays, therefore
 * overcoming Java's limitation of 2Gb for a single array.
 * 
 *  It allows loading much bigger files, but waste some memory in pointers to the blocks and 
 *  some CPU to locate the array at search time.
 *  
 * @author mario.arias, Lyudmila Balakireva
 *
 */
public class PFCDictionarySectionBig implements DictionarySectionPrivate {
	private static final Logger log = LoggerFactory.getLogger(PFCDictionarySectionBig.class);

	public static final int TYPE_INDEX = 2;
	public static final int DEFAULT_BLOCK_SIZE = 16;
	public static final int BLOCK_PER_BUFFER = 1000000;
	
	byte [][] data;
	long [] posFirst;
	protected SequenceLog64Big blocks;
	protected int blocksize;
	protected int numstrings;
	protected long size;
	static int filecounter = 0;
	
	public PFCDictionarySectionBig(HDTOptions spec) {
		this.blocksize = (int) spec.getInt("pfc.blocksize");
		if(blocksize==0) {
			blocksize = DEFAULT_BLOCK_SIZE;
		}
	}
	
	/* (non-Javadoc)
	 * @see hdt.dictionary.DictionarySection#load(hdt.dictionary.DictionarySection)
	 */
	@Override
	public void load(TempDictionarySection other, ProgressListener listener) {
		this.blocks = new SequenceLog64Big(BitUtil.log2(other.size()), other.getNumberOfElements()/blocksize);
		log.info("numbits:{}", BitUtil.log2(other.size()));
		Iterator<? extends CharSequence> it = other.getSortedEntries();		
		this.load((Iterator<? extends CharSequence>)it, other.getNumberOfElements(), listener);
		
	}
	
	public void load(Iterator<? extends CharSequence> it, long numentries, ProgressListener listener)  {		
		
		this.blocks = new SequenceLog64Big(64, numentries/blocksize);
		this.numstrings = 0;
		
		filecounter++;
		File file;
		FileOutputStream out;
		
		try {
			file = File.createTempFile("test", ".tmp");
			out = new FileOutputStream(file);
		} catch (IOException e) {
			throw new RuntimeException("Error creating temporary file.", e);
		}
		
		long byteoutsize = 0;
		ByteArrayOutputStream byteOut = new ByteArrayOutputStream(16*1024);		
		CharSequence previousStr=null;
		
		try {
			while(it.hasNext()) {
				CharSequence str = it.next();

				if(numstrings%blocksize==0) {
					// Add new block pointer
					byteOut.flush();
					byteoutsize = byteoutsize+byteOut.size();
					
					blocks.append(byteoutsize);
					byteOut.writeTo(out);
					byteOut.reset();
					// Copy full string
					ByteStringUtil.append(byteOut, str, 0);
				
				} else {
					// Find common part.
					int delta = ByteStringUtil.longestCommonPrefix(previousStr, str);
					// Write Delta in VByte
					VByte.encode(byteOut, delta);
					// Write remaining
					ByteStringUtil.append(byteOut, str, delta);
					
				}
				byteOut.write(0); // End of string
			
				numstrings++;
								
				previousStr = str;
				
			}
			
			// Ending block pointer.
			byteOut.flush();
			byteoutsize = byteoutsize + byteOut.size();
			
			//blocks.append(byteOut.size());
			blocks.append(byteoutsize);
			// Trim text/blocks
			blocks.aggressiveTrimToSize();
			
			byteOut.flush();
			byteOut.writeTo(out);
			out.close();
			
			InputStream in = new FileInputStream(file);
			// Read block by block
			// Read packed data
			
			int block = 0;
			int buffer = 0;
			long bytePos = 0;
			long numBlocks = blocks.getNumberOfElements();
			//System.out.println("numblocks:"+numBlocks);
			
			long numBuffers = 1+numBlocks/BLOCK_PER_BUFFER;
			data = new byte[(int)numBuffers][];
			posFirst = new long[(int)numBuffers];
			
			while(block<numBlocks-1) {
				int nextBlock = (int) Math.min(numBlocks-1, block+BLOCK_PER_BUFFER);
				long nextBytePos = blocks.get(nextBlock);
				
				//System.out.println("Loading block: "+i+" from "+previous+" to "+ current+" of size "+ (current-previous));
				data[buffer]=IOUtil.readBuffer(in, (int)(nextBytePos-bytePos), null);
				
				posFirst[buffer] = bytePos;
				
				bytePos = nextBytePos;
				block+=BLOCK_PER_BUFFER;
				buffer++;
			}
			
			
			
			
		} catch (IOException e) {
			log.error("Unexpected exception.", e);
		}
		finally {
			try {
				out.close();
				file.delete();
			} catch (IOException e) {
				log.error("Unexpected exception.", e);
			}
		}
	}
	
	
	
	
	
	/**
	 * Locate the block of a string doing binary search.
	 */
	protected int locateBlock(CharSequence str) {	
		int low = 0;
		int high = (int)blocks.getNumberOfElements() - 1;
		int max = high;
		
		while (low <= high) {
			int mid = (low + high) >>> 1;
			
			int cmp;
			if(mid==max) {
				cmp = -1;
			} else {
				cmp = ByteStringUtil.strcmp(str, data[mid/BLOCK_PER_BUFFER], (int)(blocks.get(mid)-posFirst[mid/BLOCK_PER_BUFFER]));
				//System.out.println("Comparing against block: "+ mid + " which is "+ ByteStringUtil.asString(data[mid], 0)+ " Result: "+cmp);
			}

			if (cmp<0) {
				high = mid - 1;
			} else if (cmp > 0) {
				low = mid + 1;
			} else {
				return mid; // key found
			}
		}
		return -(low + 1);  // key not found.
	}
	
	/* (non-Javadoc)
	 * @see hdt.dictionary.DictionarySection#locate(java.lang.CharSequence)
	 */
	@Override
	public int locate(CharSequence str) {

		int blocknum = locateBlock(str);
		if(blocknum>=0) {
			// Located exactly
			return (blocknum*blocksize)+1;
		} else {
			// Not located exactly.
			blocknum = -blocknum-2;
			
			if(blocknum>=0) {
				int idblock = locateInBlock(blocknum, str);

				if(idblock != 0) {
					return (blocknum*blocksize)+idblock+1;
				}
			}
		}
		
		// Not found
		return 0;
	}
		
	protected int locateInBlock(int blockid, CharSequence str) {
	
		ReplazableString tempString = new ReplazableString();
		
		Mutable<Long> delta = new Mutable<>(0L);
		int idInBlock = 0;
		int cshared=0;
		
		byte [] block = data[blockid/BLOCK_PER_BUFFER];
		int pos = (int) (blocks.get(blockid)-posFirst[blockid/BLOCK_PER_BUFFER]);
		
		// Read the first string in the block
		int slen = ByteStringUtil.strlen(block, pos);
		tempString.append(block, pos, slen);
		pos+=slen+1;
		idInBlock++;
		
		while( (idInBlock<blocksize) && (pos<block.length)) 
		{
			// Decode prefix
			pos += VByte.decode(block, pos, delta);
			
			// Copy suffix
			slen = ByteStringUtil.strlen(block, pos);
			tempString.replace(delta.getValue().intValue(), block, pos, slen);
			
			if(delta.getValue()>=cshared)
			{
				// Current delta value means that this string
				// has a larger long common prefix than the previous one
				cshared += ByteStringUtil.longestCommonPrefix(tempString, str, cshared);
				
				if((cshared==str.length()) && (tempString.length()==str.length())) {
					return idInBlock;
				}
			} else {
				// We have less common characters than before, 
				// this string is bigger that what we are looking for.
				// i.e. Not found.
				idInBlock = 0;
				break;
			}
			pos+=slen+1;
			idInBlock++;
			
		}

		// Not found
		if(pos==block.length || idInBlock== blocksize) {
			idInBlock=0;
		}
		
		return idInBlock;
	}

	/* (non-Javadoc)
	 * @see hdt.dictionary.DictionarySection#extract(int)
	 */
	@Override
	public CharSequence extract(int id) {
		
		if(id<1 || id>numstrings) {
			return null;
		}
		
		// Locate block
		int blockid = (id-1)/blocksize;
		int nstring = (id-1)%blocksize;
		
		byte [] block = data[blockid/BLOCK_PER_BUFFER];
		int pos = (int) (blocks.get(blockid)-posFirst[blockid/BLOCK_PER_BUFFER]);
		
		// Copy first string
 		int len = ByteStringUtil.strlen(block, pos);
		
		Mutable<Long> delta = new Mutable<>(0L);
		ReplazableString tempString = new ReplazableString();
		tempString.append(block, pos, len);
		
		// Copy strings until we find our's.
		for(int i=0;i<nstring;i++) {
			pos+=len+1;
			pos += VByte.decode(block, pos, delta);
			len = ByteStringUtil.strlen(block, pos);
			tempString.replace(delta.getValue().intValue(), block, pos, len);
		}
		return new CompactString(tempString).getDelayed();
	}

	/* (non-Javadoc)
	 * @see hdt.dictionary.DictionarySection#size()
	 */
	@Override
	public long size() {
		return size;
	}

	/* (non-Javadoc)
	 * @see hdt.dictionary.DictionarySection#getNumberOfElements()
	 */
	@Override
	public int getNumberOfElements() {
		return numstrings;
	}

	/* (non-Javadoc)
	 * @see hdt.dictionary.DictionarySection#getEntries()
	 */
	@Override
	public Iterator<CharSequence> getSortedEntries() {
		return new Iterator<CharSequence>() {
			int pos;

			@Override
			public boolean hasNext() {
				return pos<getNumberOfElements();
			}

			@Override
			public CharSequence next() {
				// FIXME: It is more efficient to go through each block, each entry.
				pos++;
				return extract(pos);
			}

			@Override
			public void remove() {
				throw new UnsupportedOperationException();
			}
		};
	}

	/* (non-Javadoc)
	 * @see hdt.dictionary.DictionarySection#save(java.io.OutputStream, hdt.ProgressListener)
	 */
	@Override
	public void save(OutputStream output, ProgressListener listener) throws IOException {
		
        CRCOutputStream out = new CRCOutputStream(output, new CRC8());		
		out.write(TYPE_INDEX);
		VByte.encode(out, numstrings);		
		
		long datasize=0;
	
		for (int i =0; i<data.length;i++) {
			datasize = data[i].length+ datasize;
			
		}
		log.info("datasize:{}", datasize);
		VByte.encode(out, datasize);
		VByte.encode(out, blocksize);				
		out.writeCRC();
		blocks.save(output, listener);	// Write blocks directly to output, they have their own CRC check.		
		out.setCRC(new CRC32());
		for (int i =0; i<data.length;i++) {			
		IOUtil.writeBuffer(out, data[i], 0, data[i].length, listener);		
		}
		out.writeCRC();
		//throw new NotImplementedException();
	}

	/* (non-Javadoc)
	 * @see hdt.dictionary.DictionarySection#load(java.io.InputStream, hdt.ProgressListener)
	 */
	@SuppressWarnings("resource")
	@Override
	public void load(InputStream input, ProgressListener listener) throws IOException {
		
		CRCInputStream in = new CRCInputStream(input, new CRC8());
		
		// Read type
		int type = in.read();
		if(type!=TYPE_INDEX) {
			throw new IllegalFormatException("Trying to read a DictionarySectionPFC from data that is not of the suitable type");
		}
		numstrings = (int) VByte.decode(in);
		this.size = VByte.decode(in);
		blocksize = (int)VByte.decode(in);
		
		if(!in.readCRCAndCheck()) {
			throw new CRCException("CRC Error while reading Dictionary Section Plain Front Coding Header.");
		}
		
		// Load block pointers
		blocks = new SequenceLog64Big();
		blocks.load(input, listener);
		
		// Initialize global block array
		
		// Read block by block
		// Read packed data
		in.setCRC(new CRC32());

		int block = 0;
		int buffer = 0;
		long bytePos = 0;
		long numBlocks = blocks.getNumberOfElements();
		long numBuffers = 1+numBlocks/BLOCK_PER_BUFFER;
		data = new byte[(int)numBuffers][];
		posFirst = new long[(int)numBuffers];
		
		while(block<numBlocks-1) {
			int nextBlock = (int) Math.min(numBlocks-1, block+BLOCK_PER_BUFFER);
			long nextBytePos = blocks.get(nextBlock);
			
			//System.out.println("Loading block: "+i+" from "+previous+" to "+ current+" of size "+ (current-previous));
			data[buffer]=IOUtil.readBuffer(in, (int)(nextBytePos-bytePos), null);
			
			posFirst[buffer] = bytePos;
			
			bytePos = nextBytePos;
			block+=BLOCK_PER_BUFFER;
			buffer++;
		}
		
		if(!in.readCRCAndCheck()) {
			throw new CRCException("CRC Error while reading Dictionary Section Plain Front Coding Data.");
		}
	}

	@Override
	public void close() throws IOException {
		data=null;
		posFirst=null;
		blocks.close();
		blocks=null;
	}
}
