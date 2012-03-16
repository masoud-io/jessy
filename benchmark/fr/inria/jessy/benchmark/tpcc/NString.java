package fr.inria.jessy.benchmark.tpcc;


import java.util.Random;

/**
 * @author Wang Haiyun & ZHAO Guang
 * 
 */

/*according to tpcc  section 4.3.2.2, this class is used for generate random string, length: [x..y], 
 * the character set must has at least 26 lower case and 26 upper case, and the digits from 1...9
 */
public final class NString {
	private NString(){
		
	}
	
	private static String set = "abcdefghjklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890";
	private static Random rand = new Random(System.currentTimeMillis());
	
	public static String generate(int x, int y){
		int i,length;
		String result="";
		length = rand.nextInt(y - x) + x;
		for(i=0; i<length; i++){
			result = result + set.charAt(rand.nextInt(61));
		}
		
		
		return result;
	}
	
	public static String generateFix(int x){
		//generate a String with a given fixed length x
		int i;
		String result="";
		for(i=0; i<x; i++){
			result = result + set.charAt(rand.nextInt(61));
		}
		return result;
	}
	
	public static String original(int x, int y){
		/*this method is written for populate Item table( add substring "ORIGINAL" in a random position of a 
		 * string, tpcc section 4.3.3.1
		 */
		int i, length, pos;
		String result = "";
		y = y-8;
		x = x-8;
		length = rand.nextInt(y-x)+x;
		pos = rand.nextInt(length); //where we put "original"
		for(i=0; i<x; i++){
			if(i == pos){
				result = result + "ORIGINAL";
			}
			result = result + set.charAt(rand.nextInt(61));
		}
		return result;
	}
}
