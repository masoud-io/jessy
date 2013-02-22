package fr.inria.jessy.communication;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.Serializable;
import java.util.Collection;

import net.sourceforge.fractal.FractalManager;
import net.sourceforge.fractal.Learner;
import net.sourceforge.fractal.Stream;
import net.sourceforge.fractal.consensus.gpaxos.GPaxosStream;
import net.sourceforge.fractal.consensus.gpaxos.GPaxosStream.RECOVERY;
import net.sourceforge.fractal.membership.Group;
import net.sourceforge.fractal.replication.Command;
import fr.inria.jessy.communication.message.TerminateTransactionRequestMessage;
import fr.inria.jessy.transaction.ExecutionHistory;

public class NonGenuineTerminationCommunication extends
		TerminationCommunication implements Learner {

	/**
	 * Stream used for atomic broadcast messages
	 */
	protected GPaxosStream gpaxosStream;
	private Learner realLearner;

	public NonGenuineTerminationCommunication(Group group, Learner learner) {
		super(learner);
		gpaxosStream = FractalManager.getInstance().getOrCreateGPaxosStream(
				"gpaxosStream",
				manager.getEverybodyGroup().name(), // the proposers are all the nodes in the system. 
				manager.getReplicaGroups().iterator().next().name(), // pick a random group as the acceptors for GPaxos. 
				manager.getAllReplicaGroup().name(), // the learners are all the replicas.
				"net.sourceforge.fractal.consensus.gpaxos.cstruct.CSched", 
				false, RECOVERY.DEFAULT, 1000, 1000);
		gpaxosStream.registerLearner("*", this);
		gpaxosStream.start();
		realLearner = learner;
	}

	@Override
	public void terminateTransaction(
			ExecutionHistory ex, Collection<String> gDest, String gSource, int swidSource) {
		gpaxosStream.propose(new CommandBox(new TerminateTransactionRequestMessage(ex,gDest,gSource,swidSource)));

	}
	
	@Override
	public void learn(Stream s, Serializable b) {
		realLearner.learn(s, ((CommandBox)b).msg);
	}
	
	private static class CommandBox extends Command{
		
		public TerminateTransactionRequestMessage msg;
		
		@Deprecated
		public CommandBox(){
		}
		
		public CommandBox(TerminateTransactionRequestMessage m){
			super(FractalManager.getInstance().membership.myId());
			msg = m;
		}
		
		@Override
		public String toString(){
			return msg.getExecutionHistory().getTransactionHandler().toString();
		}
		
		@Override
		public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
			super.readExternal(in);
			msg = (TerminateTransactionRequestMessage) in.readObject();
		}

		public void writeExternal(ObjectOutput out) throws IOException {
			super.writeExternal(out);
			out.writeObject(msg);
		}
		
	}


}
