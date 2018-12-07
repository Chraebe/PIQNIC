
package org.rdfhdt.hdt.hdt;


public class HDTVersion {
	// Version of the actual hdt file that is generated or read.
	// Software must be backwards compatible with all hdt files with the same number.
	public static final String HDT_VERSION = "1";
	
	// Version of the accompagning .index file that is generated or read
	// Software must be backwards compatible with all index files with the same index and hdt version number.
	public static final String INDEX_VERSION = "1";

	// Subreleases that are backwards compatible with both hdt and index file
	public static final String RELEASE_VERSION ="2";
	
	 public static String get_version_string(String delimiter) {
	      return "v" + HDT_VERSION + delimiter + INDEX_VERSION + delimiter + RELEASE_VERSION;
	    };

	  public static String get_index_suffix(String delimiter) {
	      return ".index.v" + HDT_VERSION + delimiter+INDEX_VERSION;
	    };
}
