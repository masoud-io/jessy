package fr.inria.jessy;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingDeque;

import net.sourceforge.fractal.membership.Group;
import net.sourceforge.fractal.utils.ExecutorPool;
import net.sourceforge.fractal.utils.PerformanceProbe.TimeRecorder;
import net.sourceforge.fractal.utils.PerformanceProbe.ValueRecorder;

import org.apache.log4j.Logger;
import org.cliffc.high_scale_lib.NonBlockingHashtable;


import fr.inria.jessy.store.JessyEntity;
import fr.inria.jessy.store.ReadReply;
import fr.inria.jessy.store.ReadRequest;

/**
 * * A remote reader for distributed Jessy. This class takes as input a remote
 * read request via function {@link RemoteReader#remoteRead(ReadRequest)} and
 * returns a Future encapsulating a JessyEntity.
 * 
 * * TODO: put the ExecutorPool inside Jessy (?)
 * 
 * TODO: suppress or garbage-collect cancelled requests.
 * 
 * @author Masoud Saeida Ardekani
 * 
 */

public abstract class RemoteReader {

	protected static Logger logger = Logger.getLogger(RemoteReader.class);

	protected static ValueRecorder batching, clientAskingTime;

	static {
		clientAskingTime = new TimeRecorder("RemoteReader#clientAskingTime(ms)");
		clientAskingTime.setFormat("%a");

		batching = new ValueRecorder("RemoteReader#batching)");
	}

	protected DistributedJessy jessy;

	protected ExecutorPool pool = ExecutorPool.getInstance();

	protected NonBlockingHashtable<Integer, RemoteReadFuture<JessyEntity>> pendingRemoteReads;

	protected BlockingQueue<RemoteReadFuture<JessyEntity>> remoteReadQ;
	
//	protected ConcurrentHashMap<Group, Integer> partitionsContact;

	public RemoteReader(DistributedJessy j) {
		jessy = j;

		remoteReadQ = new LinkedBlockingDeque<RemoteReadFuture<JessyEntity>>();
		pendingRemoteReads = new NonBlockingHashtable<Integer, RemoteReadFuture<JessyEntity>>();
		
//		partitionsContact = new ConcurrentHashMap<Group, Integer>();
	}

	public abstract <E extends JessyEntity> Future<ReadReply<E>> remoteRead(
			ReadRequest<E> readRequest) throws InterruptedException;

	public void closeProxyConnections(){
		return;
	}
	
	public void closeReplicaConnections(){
		return;
	}
}