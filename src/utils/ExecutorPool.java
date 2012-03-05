package utils;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * 
 * A facility that provides a set of executors for Callable objects.
 * 
 * @author Pierre Sutra
 *
 */

public class ExecutorPool {

	private ExecutorService es;
	
	public ExecutorPool(int poolsize){
		es=Executors.newFixedThreadPool(poolsize);
	}
	
	public <T> Future<T> submit(Callable<T> t){
		return es.submit(t);
	}
}
