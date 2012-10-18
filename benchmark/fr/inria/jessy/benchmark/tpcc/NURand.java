package fr.inria.jessy.benchmark.tpcc;

import java.util.Calendar;
import java.util.Random;

/**
 * @author Wang Haiyun & ZHAO Guang
 * 
 */
public class NURand {
	public NURand(){
		
	}
	
	public NURand(int A, int x, int y){
		this.A = A;
		this.x = x;
		this.y = y;
	}


	private int A;
	private int x;
	private int y;
	private int c;

	private Random rand_c = new Random(Calendar.getInstance().get(Calendar.HOUR_OF_DAY));
	private Random rand = new Random(System.currentTimeMillis());

	/* tpcc section 2.1.6 NURand(A,x,y) =  (((random(0,A)|random(x,y))+C)%(y-x+1))+x */
	public int calculate(){
		int exp1, exp2, or ;
		/*random(0,A)*/
		exp1 = rand.nextInt(A + 1);
		
		/*random(x,y)*/
		exp2 = rand.nextInt(y - x + 1) + x;
		
		/* or*/
		or = exp1 | exp2;
		c = rand_c.nextInt(A + 1); //[0..A] 
		return ((or+c)%(y-x+1)) + x; 
	}
	public int getA() {
		return A;
	}

	public void setA(int a) {
		A = a;
	}

	public int getX() {
		return x;
	}

	public void setX(int x) {
		this.x = x;
	}

	public int getY() {
		return y;
	}

	public void setY(int y) {
		this.y = y;
	}

	
}
