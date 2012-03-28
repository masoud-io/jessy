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
		// FIXME parameterize in a config file.
		int poolsize = 100;
		es = Executors.newFixedThreadPool(poolsize);
	}

	public <T> Future<T> submit(Callable<T> t) {
		return es.submit(t);
	}

	public static ExecutorPool getInstance() {
		return instance;
	}
}
