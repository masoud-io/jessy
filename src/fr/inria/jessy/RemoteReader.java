package fr.inria.jessy;

import java.io.Serializable;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;

import net.sourceforge.fractal.Message;
import net.sourceforge.fractal.MutedStream;
import net.sourceforge.fractal.Stream;
import net.sourceforge.fractal.membership.Group;
import net.sourceforge.fractal.membership.Membership;

import fr.inria.jessy.store.JessyEntity;
import fr.inria.jessy.store.ReadReply;
import fr.inria.jessy.store.ReadRequest;
import fr.inria.jessy.utils.ExecutorPool;

/**
 * 
 * A remote reader for distributed Jessy. This class takes as input a remote
 * read request via function
 * <p>
 * remoteRead
 * </p>
 * , and returns a Future encapsulating a JessyEntity. It maekes use of the
 * Fractal group ALLNODES to exchange replies , and create a
 * ReliableMulticastStream named RemoteReaderStream.
 * 
 * TODO: put the ExecutorPool inside Jessy (?) TODO: suppress or garbage-collect
 * cancelled requests.
 * 
 * @author Pierre Sutra
 * @author Masoud Saeida Ardekani
 */

// FIXME fix parametrized types.
public class RemoteReader extends MutedStream implements Runnable{

	private DistributedJessy jessy;
	
	private ExecutorPool pool = ExecutorPool.getInstance();

	private Membership membership;
	private Group group;
	
	private Map<UUID, ReadReply<? extends JessyEntity>> replies;
	private Map<UUID, ReadRequest<? extends JessyEntity>> requests;
	
	private Thread thread;
	private boolean isTerminated;
	private BlockingQueue<Message> queue;	

	public RemoteReader(DistributedJessy j) {
		jessy = j;
		isTerminated = false;
		
		membership  = jessy.membership;
		group = membership.getOrCreateTCPDynamicGroup(ConstantPool.JESSY_ALL_GROUP,	ConstantPool.JESSY_ALL_PORT);
		queue = new LinkedBlockingQueue<Message>();
		group.registerQueue("RemoteReadReplyMessage",queue);
		group.registerQueue("RemoteReadRequestMessage",queue);
		
		thread = new Thread(this,"RemoteReaderThread");
		
		replies = new ConcurrentHashMap<UUID, ReadReply<? extends JessyEntity>>();
		requests = new ConcurrentHashMap<UUID, ReadRequest<? extends JessyEntity>>();
	}

	@SuppressWarnings("unchecked")
	public <E extends JessyEntity> Future<ReadReply<E>> remoteRead(
			ReadRequest<E> readRequest) {
		requests.put(readRequest.getReadRequestId(),readRequest);
		Future<ReadReply<E>> reply = pool.submit(new RemoteReadRequestTask(readRequest));
		return reply;
	}

	

	@Override
	@SuppressWarnings("unchecked")
	public void run() {
		try{
			while(!isTerminated){
				Message m = queue.take();
				if(m instanceof RemoteReadRequestMessage){
					pool.submit(new RemoteReadReplyTask((RemoteReadRequestMessage) m));
				}else{
					ReadReply reply = ((RemoteReadReplyMessage) m).getReadReply();
					replies.put(reply.getReadRequestId(), reply);
					synchronized(requests.get(reply.getReadRequestId())){
						requests.get(reply.getReadRequestId()).notify();
					}
				}
			}
		}catch(InterruptedException e){
			if(!isTerminated)
				e.printStackTrace();
		}
		
	}

	@Override
	public void start() {
		isTerminated=false;
		if(thread.getState().equals(Thread.State.NEW))
			thread.start();
	}

	@Override
	public void stop() {
		isTerminated=true;
		thread.interrupt();
	}
	
	
	@SuppressWarnings("unchecked")
	public void learn(Stream s, Serializable v) {
		if (v instanceof RemoteReadRequestMessage) {
			System.out.println("GOT "+((RemoteReadRequestMessage) v).getReadRequest().getReadRequestId());
			pool.submit(new RemoteReadReplyTask((RemoteReadRequestMessage) v));
		} else { // RemoteReadReplyMessage
			ReadReply reply = ((RemoteReadReplyMessage) v).getReadReply();
			replies.put(reply.getReadRequestId(), reply);
			synchronized(requests.get(reply.getReadRequestId())){
				requests.get(reply.getReadRequestId()).notify();
			}
		}

	}

	class RemoteReadRequestTask<E extends JessyEntity> implements
			Callable<ReadReply<E>> {

		private ReadRequest<E> request;

		private RemoteReadRequestTask(ReadRequest<E> readRequest) {
			this.request = readRequest;
		}

		@SuppressWarnings("unchecked")
		public ReadReply<E> call() throws Exception {
			// TODO fault tolerance, asynchronism ?
			Group destGroup = jessy.partitioner.resolve(request.getPartitioningKey());
			synchronized(requests.get(request.getReadRequestId())){
				int peer = destGroup.members().iterator().next();
				destGroup.unicast(peer,new RemoteReadRequestMessage<E>(request));
				System.out.println("ASKING "+peer+" FOR "+request.getReadRequestId());
				requests.get(request.getReadRequestId()).wait();
			}
			ReadReply<E> reply = (ReadReply<E>) replies.get(request.getReadRequestId());
			return reply;
		}

	}

	class RemoteReadReplyTask<E extends JessyEntity> implements Callable<ReadReply<E>>  {

		private RemoteReadRequestMessage<E> message;

		public RemoteReadReplyTask(RemoteReadRequestMessage<E> m) {
			message = m;
		}

		public ReadReply<E> call() throws Exception {
			ReadRequest<E> readRequest = message.getReadRequest();
			ReadReply<E> readReply = jessy.getDataStore().get(readRequest);
			Group destGroup = membership.groupOf(message.source).iterator().next();
			destGroup.unicast(message.source, new RemoteReadReplyMessage<E>(readReply));
			return null;
		}

	}


}
