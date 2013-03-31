package fr.inria.jessy.communication;

import java.io.Serializable;
import java.util.Collection;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

import net.sourceforge.fractal.Learner;
import net.sourceforge.fractal.Stream;
import net.sourceforge.fractal.membership.Group;
import net.sourceforge.fractal.multicast.MulticastStream;
import net.sourceforge.fractal.utils.ExecutorPool;
import net.sourceforge.fractal.wanamcast.WanAMCastStream;
import fr.inria.jessy.ConstantPool;
import fr.inria.jessy.DistributedJessy;
import fr.inria.jessy.communication.message.TerminateTransactionRequestMessage;
import fr.inria.jessy.communication.message.TransactionHandlerMessage;
import fr.inria.jessy.transaction.ExecutionHistory;
import fr.inria.jessy.transaction.TransactionHandler;

/**
 * This class provides a light termination communication primitives.
 * 
 * Since {@link TerminateTransactionRequestMessage} size is huge, and fractal should perform two consensus with this message, the cost of
 * genuine atomic multicast is huge.
 * This class instead performs two communications in parallel.
 * It rm-cast(TerminateTransactionRequestMessage) and am-cast(TransactionId).
 * Thus {@link TransactionHandler#getId()} is light, the consensus is performed way faster than when we use {@link GenuineTerminationCommunication}.
 * 
 * Our study shows that with this class, SER performance increases by 70%.
 * 
 * @author Masoud Saeida Ardekani
 * 
 */
public class LightGenuineTerminationCommunicationWithFractal extends TerminationCommunication implements Learner, Runnable{

	/**
	 * Stream used for multicast messages
	 */
	protected WanAMCastStream aMCastStream;
	
	private Learner realLearner;

	private Map<UUID,TerminateTransactionRequestMessage> rm_DeliveredTerminateTransactionRequestMessages=new ConcurrentHashMap<UUID, TerminateTransactionRequestMessage>();
	private LinkedBlockingQueue<UUID> am_DeliveredTransactionHandlerMessage=new LinkedBlockingQueue<UUID>();
	
	protected MulticastStream mCastStream;
	
	public LightGenuineTerminationCommunicationWithFractal(
			Group group, 
			Learner fractalLearner, UnicastLearner nettyLearner,
			DistributedJessy j) {
		super(j, fractalLearner,nettyLearner);
		mCastStream = j.manager.fractal.getOrCreateMulticastStream(
				ConstantPool.JESSY_VOTE_STREAM, manager.getMyGroup().name());
		
		mCastStream.registerLearner("TerminateTransactionRequestMessage", this);
		
		aMCastStream = j.manager.fractal.getOrCreateWanAMCastStream(group.name(), group.name());
		aMCastStream.registerLearner("TransactionHandlerMessage", this);
		aMCastStream.start();
		
		realLearner = fractalLearner;
		
		ExecutorPool.getInstance().submit(this);
	}

	
	@Override
	public void terminateTransaction(
			ExecutionHistory eh, Collection<String> gDest, String gSource, int swidSource) {
		try{
			
			mCastStream.multicast(new TerminateTransactionRequestMessage(eh,gDest,gSource,swidSource));
			aMCastStream.atomicMulticast(new TransactionHandlerMessage(eh,gDest,gSource,swidSource));
			
		}
		catch(Exception exception){
			exception.printStackTrace();
		}
	}


	@Override
	public void learn(Stream arg0, Serializable s) {
		if (s instanceof TransactionHandlerMessage){
			TransactionHandlerMessage msg=(TransactionHandlerMessage)s;
			am_DeliveredTransactionHandlerMessage.offer(msg.getId());
		}else
		{
			TerminateTransactionRequestMessage msg=(TerminateTransactionRequestMessage)s;
			synchronized (rm_DeliveredTerminateTransactionRequestMessages) {
				rm_DeliveredTerminateTransactionRequestMessages.put(msg.getExecutionHistory().getTransactionHandler().getId(), msg);
				rm_DeliveredTerminateTransactionRequestMessages.notify();
			}
		}
	}


	@Override
	public void run() {
		UUID id;
		boolean delivered=false;
		while(true){
			try {
				id=am_DeliveredTransactionHandlerMessage.take();
				delivered=false;
				
				while (!delivered){
					synchronized (rm_DeliveredTerminateTransactionRequestMessages) {

						if (rm_DeliveredTerminateTransactionRequestMessages.containsKey(id)){
							delivered=true;
						}
						else{
							rm_DeliveredTerminateTransactionRequestMessages.wait();
						}
					}
					
					if(delivered)
						realLearner.learn(null, rm_DeliveredTerminateTransactionRequestMessages.remove(id));
				}
				
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			
		}
	}
}
