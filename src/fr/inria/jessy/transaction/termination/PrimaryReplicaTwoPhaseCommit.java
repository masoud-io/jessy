package fr.inria.jessy.transaction.termination;

import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import net.sourceforge.fractal.membership.Group;

import org.apache.log4j.Logger;

import fr.inria.jessy.DebuggingFlag;
import fr.inria.jessy.communication.message.TerminateTransactionRequestMessage;
import fr.inria.jessy.communication.message.VoteMessage;
import fr.inria.jessy.consistency.Consistency.ConcernedKeysTarget;
import fr.inria.jessy.partitioner.Partitioner;
import fr.inria.jessy.transaction.ExecutionHistory;
import fr.inria.jessy.transaction.TransactionState;
import fr.inria.jessy.transaction.termination.vote.Vote;

/**
 * 
 * This is the update everywhere implementation of 2PC.
 * Hence all replica groups participate.
 * 
 * we first choose the first write as the deterministic key.
 * The leader of a group replicating a deterministic key returned by {@link this#getDetermisticKey(ExecutionHistory)} 
 * will be the 2PC coordinator.
 * 
 * @author Masoud Saeida Ardekani
 *
 */
public class PrimaryReplicaTwoPhaseCommit extends AtomicCommit {

	private static Logger logger = Logger
			.getLogger(PrimaryReplicaTwoPhaseCommit.class);

	String swid=""+jessy.manager.getSourceId();
	
	//TODO FIX ME THIS IS CRAP
	ConcurrentHashMap<UUID, Integer> causedTermination=new ConcurrentHashMap<UUID, Integer>(); 
	
	public PrimaryReplicaTwoPhaseCommit(DistributedTermination termination) {
		super(termination);
	}

	@Override
	public boolean proceedToCertifyAndVote(
			TerminateTransactionRequestMessage msg) {
		synchronized(atomicDeliveredMessages){
			for (TerminateTransactionRequestMessage n : atomicDeliveredMessages) {
				if (n.equals(msg)) {
					continue;
				}
				if (!jessy.getConsistency().certificationCommute(
						n.getExecutionHistory(), msg.getExecutionHistory())) {
					
					UUID caused=n.getExecutionHistory().getTransactionHandler().getId();
					
					if (causedTermination.containsKey(caused)){
						int i=causedTermination.get(caused);
						if (i>5)
						{
							causedTermination.remove(caused);
							atomicDeliveredMessages.remove(n);
							return true;
						}
						causedTermination.put(caused, i+1);
					}else{
						causedTermination.put(caused, 1);
					}
					
//					System.out.println("Pre-emptive abort for " + msg.getExecutionHistory().toString() + " because " + n.getExecutionHistory().toString());
					return false;
				}
			}
		}
		return true;
	}
	
	@Override
	public void setVoters(TerminateTransactionRequestMessage msg ,Set<String> voteReceivers, AtomicBoolean voteReceiver, Set<String> voteSenders, AtomicBoolean voteSender){

		/*
		 * In 2pc, those that receive the transaction, always participate in voting.
		 * Hence, vote sender is always true.
		 */
		voteSender.set(true);
		
		if (isCoordinator(msg)){
			
			/*
			 * If this is coordinator, wait for the votes from every body.
			 */
			Set<String> keys_for_SendVotes =jessy
					.getConsistency().getConcerningKeys(
							msg.getExecutionHistory(),
							ConcernedKeysTarget.SEND_VOTES);
						
			for (String str:keys_for_SendVotes){
				Group g=jessy.partitioner.resolve(str);
				for (int tmpswid:g.allNodes()){
					voteSenders.add(""+tmpswid);
				}
			}
			
			/*
			 * If this is the coordinator, send the vote to only coordinator
			 */
			voteReceivers.add(swid);
			voteReceiver.set(true);
		}
		else{
			/*
			 *If this is not the coordinator, it needs to send vote to the coordinator, and receive vote from coordinator.  
			 */
			Set<String> keys_for_SendVotes =jessy
					.getConsistency().getConcerningKeys(
							msg.getExecutionHistory(),
							ConcernedKeysTarget.SEND_VOTES);
						
			for (String str:keys_for_SendVotes){
				Group g=jessy.partitioner.resolve(str);
				if (g.name().equals(group.name())){
					voteSenders.add( getCoordinatorId(msg.getExecutionHistory(), jessy.partitioner));
				}
			}
			
			if (jessy.partitioner.resolveNames(jessy
					.getConsistency().getConcerningKeys(
							msg.getExecutionHistory(),
							ConcernedKeysTarget.RECEIVE_VOTES)).contains(group.name()))
			{
				voteReceivers.add( getCoordinatorId(msg.getExecutionHistory(), jessy.partitioner));
				voteReceiver.set(true);
			}

		}
		
	}

	@Override
	public void sendVote(VoteMessage voteMessage,
			TerminateTransactionRequestMessage msg) {
		voteMessage.getVote().setVoterEntityName(swid);
		voteMulticast.sendVote(voteMessage, Integer.parseInt(getCoordinatorId(msg.getExecutionHistory(), jessy.manager.getPartitioner())), "");
	}
	
	@Override
	public void quorumReached(TerminateTransactionRequestMessage msg,TransactionState state, Vote selfVote){
		if (isCoordinator(msg)){
			
			Set<String> voteReceivers=	jessy.partitioner.resolveNames(jessy
					.getConsistency().getConcerningKeys(
							msg.getExecutionHistory(),
							ConcernedKeysTarget.RECEIVE_VOTES));
			voteReceivers.remove(swid);
			boolean committed=(state==TransactionState.COMMITTED) ? true : false;
			Vote vote=new Vote(msg.getExecutionHistory().getTransactionHandler(), committed , swid, (selfVote.getVotePiggyBack()==null? null : selfVote.getVotePiggyBack()));
			VoteMessage voteMsg = new VoteMessage(vote, voteReceivers,
					group.name(), jessy.manager.getSourceId());
			
			/*
			 * Send the final vote to every replica, and also to the client.
			 * Note that here, the proxy is the client.  
			 */
			if (DebuggingFlag.TWO_PHASE_COMMIT)
				logger.error("quorumReached for " +
						msg.getExecutionHistory().getTransactionHandler().getId().toString() + 
						" isCertifyAtCoordinator: " + msg.getExecutionHistory().isCertifyAtCoordinator()
						+ " coordinatorHost: " + msg.getExecutionHistory().getCoordinatorHost());
				
			
			voteMulticast.sendVote(voteMsg, msg.getExecutionHistory().isCertifyAtCoordinator(), msg.getExecutionHistory().getCoordinatorSwid(), msg.getExecutionHistory().getCoordinatorHost());
		}
		
	}
	
	private boolean isCoordinator(TerminateTransactionRequestMessage msg){		
		String deterministicKey=getDetermisticKey(msg.getExecutionHistory());
		
		if (jessy.partitioner.resolve(deterministicKey).leader() == jessy.manager.getSourceId()){
			return true;
		}
		else{
			return false;
		}
	}
	
	public static String getDetermisticKey(ExecutionHistory history){
		String deterministicKey;
		
		//we first choose the first write as the determistic key.
		//hence the leader of a group replicating this key will be the 2PC coordinator.
		if (history.getWriteSet() !=null && history.getWriteSet().size()>0){
			deterministicKey=history.getWriteSet().getKeys().iterator().next();
		}
		else if (history.getCreateSet() !=null &&  history.getCreateSet().size()>0){
			deterministicKey=history.getCreateSet().getKeys().iterator().next();
		}
		else {
			deterministicKey=history.getReadSet().getKeys().iterator().next();
		}
		return deterministicKey;
	}

	public static String getCoordinatorId(ExecutionHistory executionHistory, Partitioner partitioner){
		String firstWriteKey=getDetermisticKey(executionHistory);
		return "" + partitioner.resolve(firstWriteKey).leader();
	}

}
