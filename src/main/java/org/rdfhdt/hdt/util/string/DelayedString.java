package org.rdfhdt.hdt.util.string;

public final class DelayedString implements CharSequence {
	private CharSequence str;

	public DelayedString(CharSequence str) {
		this.str = str;
	}
	
	private void ensure() {
		if(!(str instanceof String)) {
			str = str.toString();
		}
	}
	
	@Override
	public int length() {
		ensure();
		return str.length();
	}

	@Override
	public char charAt(int index) {
		ensure();
		return str.charAt(index);
	}

	@Override
	public CharSequence subSequence(int start, int end) {
		ensure();
		return str.subSequence(start, end);
	}
	
	@Override
	public String toString() {
		ensure();
		return str.toString();
	}
	
	public static CharSequence unwrap(CharSequence str) {
		if (str instanceof DelayedString) {
			return ((DelayedString) str).str;
		}
		return str;
	}
	
	public CharSequence getInternal(){
		return str;
	}

}
