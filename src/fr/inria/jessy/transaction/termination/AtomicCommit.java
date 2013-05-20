package fr.inria.jessy.transaction.termination;

import java.util.LinkedList;
import java.util.Set;

import net.sourceforge.fractal.membership.Group;

import fr.inria.jessy.ConstantPool;
import fr.inria.jessy.DistributedJessy;
import fr.inria.jessy.ConstantPool.CAST_MODE;
import fr.inria.jessy.communication.TerminationCommunication;
import fr.inria.jessy.communication.TerminationCommunicationFactory;
import fr.inria.jessy.communication.VoteMulticast;
import fr.inria.jessy.communication.VoteMulticastWithFractal;
import fr.inria.jessy.communication.VoteMulticastWithNetty;
import fr.inria.jessy.communication.message.TerminateTransactionRequestMessage;
import fr.inria.jessy.communication.message.VoteMessage;
import fr.inria.jessy.transaction.ExecutionHistory;
import fr.inria.jessy.transaction.TransactionState;
import fr.inria.jessy.transaction.termination.vote.GroupVotingQuorum;

public abstract class AtomicCommit {

	LinkedList<TerminateTransactionRequestMessage> atomicDeliveredMessages;
	DistributedTermination termination;
	DistributedJessy jessy;

	TerminationCommunication terminationCommunication;
	VoteMulticast voteMulticast;
	
	protected Group group;
	
	public AtomicCommit(DistributedTermination termination){
		this.termination=termination;
		this.atomicDeliveredMessages=termination.getAtomicDeliveredMessages();
		this.jessy=termination.jessy;
		
		terminationCommunication=TerminationCommunicationFactory.initAndGetConsistency(jessy.manager.getMyGroup(), termination,termination, jessy);
		
		if (ConstantPool.JESSY_VOTING_PHASE_MULTICAST_MODE==CAST_MODE.FRACTAL){
			voteMulticast=new VoteMulticastWithFractal(termination,jessy);
		}
		else{
			voteMulticast=new VoteMulticastWithNetty(jessy, termination);
		}
		
		group=jessy.manager.getMyGroup();
	}

	/**
	 * A thread starts certifying the transaction in the TerminateTransactionRequestMessage.
	 * 
	 * @param msg TerminateTransactionRequestMessage containing the transaction need to be certified
	 * @return true if this transaction can be certified, false if it need to preemptively abort.
	 */
	public abstract boolean proceedToCertifyAndVote(TerminateTransactionRequestMessage msg);
	

	/**
	 * Computes a set of destinations for the votes, and sends out
	 * the votes to all replicas <i>that replicate objects modified
	 * inside the transaction</i>. The group this node belongs to is
	 * omitted.
	 * 
	 * <p>
	 * 
	 * The votes will be sent to all concerned keys. Note that the
	 * optimization to only send the votes to the nodes replicating
	 * objects in the write set is not included. Thus, for example,
	 * under serializability, a node may wait to receive the votes
	 * from all nodes replicating the concerned keys, and then
	 * returns without performing anything.

	 * @param msg contains the terminating transaction.
	 * @param voteReceivers 
	 * @param voteSenders
	 */
	public abstract void setVoters(TerminateTransactionRequestMessage msg ,Set<String> voteReceivers, Set<String> voteSenders);
	
	
	public void closeAtomicCommit(){
		voteMulticast.close();
	}
	
	
//	public HashSet<String> getDestinationGroups(Set<String> concernedKeys){
//		return jessy.partitioner.resolveNames(concernedKeys);
//		HashSet<String> destGroups = new HashSet<String>();
//		
//		destGroups
//				.addAll(jessy.partitioner.resolveNames(concernedKeys));
//		
//		return destGroups;
//	}
	
	
	/**
	 * The proxy of a transaction (usually the client) calls this function to terminate a transaction. 
	 * @param executionHistory
	 * @param concernedKeys
	 * @return
	 */
	public GroupVotingQuorum broadcastTransaction(ExecutionHistory executionHistory, Set<String> destGroups){
		
		if (destGroups.contains(jessy.manager.getMyGroup().name())) {
			executionHistory.setCertifyAtCoordinator(true);
		} else {
			int coordinatorSwid=jessy.manager.getSourceId();
			executionHistory.setCertifyAtCoordinator(false);
			executionHistory.setCoordinatorSwid(coordinatorSwid);
			executionHistory.setCoordinatorHost(jessy.manager.getMembership()
					.adressOf(coordinatorSwid));
		}

		termination.votingQuorums.put(
				executionHistory.getTransactionHandler(),
				new GroupVotingQuorum(executionHistory
						.getTransactionHandler()));

		/*
		 * gets the pointer for the transaction's VotingQuorum because
		 * the votingQuorums might be garbage collected by another
		 * thread after multicasting this transaction.
		 */
		GroupVotingQuorum vq = termination.votingQuorums.get(executionHistory
				.getTransactionHandler());

		/*
		 * Atomic multicast the transaction.
		 */
		executionHistory.clearReadValues();
		
		terminationCommunication
		.terminateTransaction(executionHistory, destGroups, termination.manager.getMyGroup().name(), termination.manager.getSourceId());
		
		return vq;
	}
	
	public abstract void sendVote(VoteMessage voteMessage, TerminateTransactionRequestMessage msg);
	
	public void quorumReached(TerminateTransactionRequestMessage msg,TransactionState state){
		
	}
	
}
