package fr.inria.jessy.benchmark.tpcc;

import java.util.Date;
import java.util.Random;

public class CalculateFloat {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		float H_AMOUNT;
		Random rand = new Random(System.currentTimeMillis());
		H_AMOUNT = (float) (((float)rand.nextInt(500000-1)+1)/100.00);
		System.out.println(new Date());
		
		System.out.println(H_AMOUNT);

	}

}
