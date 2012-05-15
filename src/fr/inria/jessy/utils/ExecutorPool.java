package fr.inria.jessy.utils;

import java.io.FileInputStream;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * 
 * A facility that provides a set of executors for Callable objects.
 * 
 * @author Pierre Sutra
 * @author Masoud Saeida Ardekani
 * 
 */

public class ExecutorPool {

	private ExecutorService es;
	private static ExecutorPool instance;
	static{
		instance=new ExecutorPool();
	}

	private ExecutorPool() {
		int poolsize = readConfig();
//		es = Executors.newFixedThreadPool(poolsize);
		es=Executors.newCachedThreadPool();
	}

	public <T> Future<T> submit(Callable<T> t) {
		return es.submit(t);
	}

	public static ExecutorPool getInstance() {
		return instance;
	}
	
	private static int readConfig() {
		String poolSize = "";
		try {
			Properties myProps = new Properties();
			FileInputStream MyInputStream = new FileInputStream(
					"config.property");
			myProps.load(MyInputStream);
			poolSize= myProps.getProperty("pool_size");
		} catch (Exception ex) {
			ex.printStackTrace();
			System.out.println("OPS");
		}

		return Integer.valueOf(poolSize);

	}

}
