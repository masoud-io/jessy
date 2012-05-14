package fr.inria.jessy.utils;

public class Compress {

	public static String compressClassName(String className) {
		return className.substring(className.lastIndexOf((".")));
	}

}