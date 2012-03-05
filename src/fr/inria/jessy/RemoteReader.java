package fr.inria.jessy;

import java.io.Serializable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;

import net.sourceforge.fractal.FractalManager;
import net.sourceforge.fractal.Learner;
import net.sourceforge.fractal.Stream;
import net.sourceforge.fractal.UMessage;
import net.sourceforge.fractal.membership.Membership;
import net.sourceforge.fractal.rmcast.RMCastStream;
import net.sourceforge.fractal.rmcast.WanMessage;
import utils.ExecutorPool;
import fr.inria.jessy.store.JessyEntity;
import fr.inria.jessy.transaction.TransactionHandler;
import fr.inria.jessy.vector.Vector;

/**
 * 
 * A remote reader for distributed Jessy.
 * This class takes as input a remote read request via function <p>remoteRead</p>,
 * and returns a Future encapsulating a JessyEntity.
 * It maekes use of the Fractal group ALLNODES to exchange replies , and create a ReliableMulticastStream named RemoteReaderStream.
 * 
 * FIXME: need a way to serializable JessyEntities.
 * 
 * TODO: put the ExecutorPool inside Jessy (?)
 * TODO: suppress or garbage-collect cancelled requests.
 * 
 * @author Pierre Sutra
 *
 */

public class RemoteReader implements Learner{

	private static RemoteReader instance;
	
	private ExecutorPool pool;
	private RMCastStream stream;
	private Map<TransactionHandler,Future<JessyEntity>> replies;
	
	
	public static RemoteReader getInstance(){
		if(instance==null)
			instance = new RemoteReader();
		return instance;
	}
	
	private RemoteReader(){
		
		Membership.getInstance().getOrCreateTCPGroup("ALLNODES");
		stream = FractalManager.getInstance().getOrCreateRMCastStream("RemoteReaderStream",Membership.getInstance().myGroup().name());
		stream.registerLearner("RemoteReadRequestMessage", this);
		stream.registerLearner("RemoteReadReplyMessage", this);

		pool = new ExecutorPool(100);
		replies = new HashMap<TransactionHandler,Future<JessyEntity>>();
		
	}
	
	public Future<JessyEntity> remoteRead(TransactionHandler h, Vector<String> v, String k){
		assert !Partitioner.getInstance().isLocal(k);
		Future<JessyEntity> reply = pool.submit(new RemoteReadRequestTask(new RemoteReadRequest(h,v,k)));
		replies.put(h, reply);
		return reply;
	}
	

	@SuppressWarnings("unchecked")
	@Deprecated
	public void learn(Stream s, Serializable v) {
		if(v instanceof RemoteReadRequestMessage){
			pool.submit(new RemoteReadReplyTask((RemoteReadRequestMessage)v));
		}else{ // RemoteReadReplyMessage
			RemoteReadReply reply = ((RemoteReadReplyMessage)v).reply;
			replies.get(reply.handler).notify();
		}
			
	}
	
	//
	// INNER CLASSES
	//
	
	class RemoteReadRequest implements Serializable{
		
		private static final long serialVersionUID = ConstantPool.JESSY_MID;
		TransactionHandler handler;
		Vector<String> vector;
		String key;
		
		RemoteReadRequest(TransactionHandler h, Vector<String> v, String k) {
			handler = h;
			vector = v;
			key = k;
		}

	}
	
	class RemoteReadReply implements Serializable {

		private static final long serialVersionUID = ConstantPool.JESSY_MID;
		TransactionHandler handler;
		JessyEntity entity;
		
		RemoteReadReply(JessyEntity e,TransactionHandler h){
			entity=e;
			handler=h;
		}
		
	}
	
	public class RemoteReadReplyMessage extends UMessage {

		static final long serialVersionUID = ConstantPool.JESSY_MID;
		RemoteReadReply reply;
		
		// For Fractal
		public RemoteReadReplyMessage() {}
		
		RemoteReadReplyMessage(RemoteReadReply r) {
			super(r,Membership.getInstance().myId());
		}

	}
	
	public class RemoteReadRequestMessage extends WanMessage {

		private static final long serialVersionUID = ConstantPool.JESSY_MID;
		RemoteReadRequest request;

		// For Fractal
		public RemoteReadRequestMessage() {}
		
		RemoteReadRequestMessage(RemoteReadRequest r, Set<String> dest) {
			super(r, dest, Membership.getInstance().myGroup().name(),Membership.getInstance().myId());
		}
	
	}

	
	class RemoteReadRequestTask implements Callable<JessyEntity>{
		
		private RemoteReadRequest request;
		
		private RemoteReadRequestTask(RemoteReadRequest r){
			request=r;
		}
		
		public JessyEntity call() throws Exception {
			Set<String> dest = new HashSet<String>(1);
			dest.add(Partitioner.getInstance().resolve(request.key).name());
			stream.reliableMulticast(new RemoteReadRequestMessage(request,dest));
			replies.get(request.handler).wait();
			return replies.get(request.handler).get();
		}
		
	}
	
	class RemoteReadReplyTask implements Callable{

		private RemoteReadRequestMessage message;
		
		public RemoteReadReplyTask(RemoteReadRequestMessage m) {
			message = m;
		}

		public Object call() throws Exception {
			// FIXME the JessyEntity returned should not be null !
			RemoteReadReply r = new RemoteReadReply(null,message.request.handler);
			Membership.getInstance().getOrCreateTCPGroup("ALLNODES").unicastSW(message.source,new RemoteReadReplyMessage(r));
			return null;
		}
		
	}

	
	
	
}
