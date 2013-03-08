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

import org.jboss.netty.channel.Channel;

import fr.inria.jessy.ConstantPool;
import fr.inria.jessy.communication.message.TerminateTransactionRequestMessage;
import fr.inria.jessy.communication.message.TransactionHandlerMessage;
import fr.inria.jessy.transaction.ExecutionHistory;
import fr.inria.jessy.transaction.TransactionHandler;

/**
 * Provides the primitives needed for termination communication using both
 * fractal and netty. {@link TransactionHandler} is am-cast using Fractal, and
 * {@link ExecutionHistory} is is rm-cast using netty.
 * <p>
 * TODO Does not work with SI
 * 
 * @author Masoud Saeida Ardekani
 * 
 */
public class LightGenuineTerminationCommunicationWithNetty extends
		TerminationCommunication implements Learner, UnicastLearner, Runnable {

	/**
	 * Stream used for multicast messages
	 */
	protected WanAMCastStream aMCastStream;

	private Learner realLearner;

	UnicastClientManager cManager;
	UnicastServerManager sManager;

	private Map<UUID, TerminateTransactionRequestMessage> rm_DeliveredTerminateTransactionRequestMessages = new ConcurrentHashMap<UUID, TerminateTransactionRequestMessage>();
	private LinkedBlockingQueue<UUID> am_DeliveredTransactionHandlerMessage = new LinkedBlockingQueue<UUID>();

	public LightGenuineTerminationCommunicationWithNetty(Group group,
			Learner fractalLearner, UnicastLearner nettyLearner) {
		super(fractalLearner, nettyLearner);

		aMCastStream = FractalManager.getInstance().getOrCreateWanAMCastStream(
				group.name(), group.name());
		aMCastStream.registerLearner("TransactionHandlerMessage", this);
		aMCastStream.start();

		if (!manager.isProxy()) {
			sManager = new UnicastServerManager(this,
					ConstantPool.JESSY_NETTY_TERMINATIONMESSAGE_PORT);
		} else {
			cManager = new UnicastClientManager(null,
					ConstantPool.JESSY_NETTY_TERMINATIONMESSAGE_PORT, manager
							.getAllReplicaGroup().members());
		}

		realLearner = fractalLearner;

		ExecutorPool.getInstance().submit(this);
	}

	@Override
	public void terminateTransaction(ExecutionHistory eh,
			Collection<String> gDest, String gSource, int swidSource) {
		try {

			multiCast(new TerminateTransactionRequestMessage(eh, gDest,
					gSource, swidSource), gDest);
			aMCastStream.atomicMulticast(new TransactionHandlerMessage(eh, gDest, gSource,
					swidSource));

		} catch (Exception exception) {
			exception.printStackTrace();
		}
	}

	@Override
	public void learn(Stream arg0, Serializable s) {
		if (s instanceof TransactionHandlerMessage) {
			TransactionHandlerMessage msg = (TransactionHandlerMessage) s;
			am_DeliveredTransactionHandlerMessage.offer(msg.getId());
		}
	}

	@Override
	public void run() {
		UUID id;
		boolean delivered = false;
		while (true) {
			try {
				id = am_DeliveredTransactionHandlerMessage.take();
				delivered = false;

				while (!delivered) {
					synchronized (rm_DeliveredTerminateTransactionRequestMessages) {

						if (rm_DeliveredTerminateTransactionRequestMessages
								.containsKey(id)) {
							realLearner.learn(null,
									rm_DeliveredTerminateTransactionRequestMessages
									.remove(id));
							delivered = true;
						} else {
							rm_DeliveredTerminateTransactionRequestMessages
							.wait();
						}
					}
				}

			} catch (InterruptedException e) {
				e.printStackTrace();
			}

		}
	}

	@Override
	public void receiveMessage(Object message, Channel channel) {
		if (message instanceof TerminateTransactionRequestMessage) {
			TerminateTransactionRequestMessage msg = (TerminateTransactionRequestMessage) message;
			synchronized (rm_DeliveredTerminateTransactionRequestMessages) {
				rm_DeliveredTerminateTransactionRequestMessages
				.put(msg.getExecutionHistory().getTransactionHandler()
						.getId(), msg);
				rm_DeliveredTerminateTransactionRequestMessages.notify();
			}
		} else {
			System.err.println("Received unwanted message");
		}
	}

	private void multiCast(Object obj, Collection<String> dest) {
		for (String g : dest) {
			for (Integer swid : manager.getMembership().group(g).members()) {
				cManager.unicast(obj, swid);
			}
		}
	}

}
