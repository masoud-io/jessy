package fr.inria.jessy.communication;

import java.io.Serializable;
import java.util.Collection;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

import net.sourceforge.fractal.FractalManager;
import net.sourceforge.fractal.Learner;
import net.sourceforge.fractal.Stream;
import net.sourceforge.fractal.membership.Group;
import net.sourceforge.fractal.utils.ExecutorPool;
import net.sourceforge.fractal.wanamcast.WanAMCastStream;
import fr.inria.jessy.communication.message.TerminateTransactionRequestMessage;
import fr.inria.jessy.communication.message.TransactionHandlerMessage;
import fr.inria.jessy.transaction.ExecutionHistory;

/**
 * TODO
 * 
 * @author Masoud Saeida Ardekani
 * 
 */
public class LightGenuineTerminationCommunication extends TerminationCommunication implements Learner, Runnable{

	/**
	 * Stream used for multicast messages
	 */
	protected WanAMCastStream aMCastStream;
	
	private Learner realLearner;

	private Map<UUID,TerminateTransactionRequestMessage> rm_DeliveredTerminateTransactionRequestMessages=new ConcurrentHashMap<UUID, TerminateTransactionRequestMessage>();
	private LinkedBlockingQueue<UUID> am_DeliveredTransactionHandlerMessage=new LinkedBlockingQueue<UUID>();
	
	public LightGenuineTerminationCommunication(Group group, Learner learner) {
		super(learner);
		mCastStream.registerLearner("TerminateTransactionRequestMessage", this);
		
		aMCastStream = FractalManager.getInstance()
				.getOrCreateWanAMCastStream(group.name(), group.name());
		aMCastStream.registerLearner("TransactionHandlerMessage", this);
		aMCastStream.start();
		
		realLearner = learner;
		
		ExecutorPool.getInstance().submit(this);
	}

	
	@Override
	public void terminateTransaction(
			ExecutionHistory eh, Collection<String> gDest, String gSource, int swidSource) {
		try{
			
			aMCastStream.atomicMulticast(new TransactionHandlerMessage(eh.getTransactionHandler().getId(),gDest,gSource,swidSource));
			mCastStream.multicast(new TerminateTransactionRequestMessage(eh,gDest,gSource,swidSource));
			
		}
		catch(Exception exception){
			exception.printStackTrace();
		}
	}


	@Override
	public void learn(Stream arg0, Serializable s) {
		if (s instanceof TransactionHandlerMessage){
			TransactionHandlerMessage msg=(TransactionHandlerMessage)s;
			System.out.println("delivered TransactionHandlerMessage" + msg.getId());
			am_DeliveredTransactionHandlerMessage.offer(msg.getId());
			synchronized (am_DeliveredTransactionHandlerMessage) {
				am_DeliveredTransactionHandlerMessage.notify();
			}
		}else
		{
			TerminateTransactionRequestMessage msg=(TerminateTransactionRequestMessage)s;
			System.out.println("msg id is " + msg.getExecutionHistory().getTransactionHandler());
			rm_DeliveredTerminateTransactionRequestMessages.put(msg.getExecutionHistory().getTransactionHandler().getId(), msg);
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

					if (rm_DeliveredTerminateTransactionRequestMessages.containsKey(id)){
						System.out.println("BINGO, DELIVERED");
						realLearner.learn(null, rm_DeliveredTerminateTransactionRequestMessages.remove(id));
						delivered=true;
					}
					else{
						synchronized (am_DeliveredTransactionHandlerMessage) {
							am_DeliveredTransactionHandlerMessage.wait();
						}
					}
				}
				
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			
		}
	}
}
