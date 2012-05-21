package fr.inria.jessy.utils;

public class Compress {

	/**
	 * The main reason for this class and method is exactly to make it
	 * homogeneous. So far, every keys stored or retrieved to/from the store
	 * uses the key as the concatenation of
	 * {@code Compress#compressClassName(String)} and the actual key.
	 * <p>
	 * Note: If this method returns nothing, then two different entities from
	 * different entity classes with the same entity key will be mixed by the
	 * Store, thus leads to conversion exception.
	 * 
	 */
	public static String compressClassName(String className) {
		return className.substring(className.lastIndexOf("."));
	}

}