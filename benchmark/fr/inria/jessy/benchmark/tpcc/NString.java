package fr.inria.jessy.benchmark.tpcc;


import java.util.Random;


/*according to tpcc  section 4.3.2.2, this class is used for generate random string, lenth: [x..y], 
 * the character set must has at least 26 lower case and 26 upper case, and the digits from 1...9
 */
public final class NString {
	private NString(){
		
	}
	
	private static String set = "abcdefghjklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
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
		int i;
		String result="";
		for(i=0; i<x; i++){
			result = result + set.charAt(rand.nextInt(61));
		}
		return result;
	}
}
