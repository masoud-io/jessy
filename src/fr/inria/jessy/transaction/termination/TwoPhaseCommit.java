package fr.inria.jessy.transaction.termination;

import java.util.HashSet;
import java.util.Set;

import net.sourceforge.fractal.membership.Group;

import fr.inria.jessy.communication.message.TerminateTransactionRequestMessage;
import fr.inria.jessy.communication.message.VoteMessage;
import fr.inria.jessy.consistency.Consistency.ConcernedKeysTarget;
import fr.inria.jessy.transaction.TransactionState;
import fr.inria.jessy.transaction.termination.vote.Vote;

public class TwoPhaseCommit extends AtomicCommit {

	String swid=""+jessy.manager.getSourceId();
	
	public TwoPhaseCommit(DistributedTermination termination) {
		super(termination);
	}

	@Override
	public boolean proceedToCertifyAndVote(
			TerminateTransactionRequestMessage msg) {
		for (TerminateTransactionRequestMessage n : atomicDeliveredMessages) {
			if (!jessy.getConsistency().certificationCommute(
					n.getExecutionHistory(), msg.getExecutionHistory())) {
				return false;
			}
		}
		return true;
	}
	
	@Override
	public void setVoters(TerminateTransactionRequestMessage msg ,Set<String> voteReceivers, Set<String> voteSenders){
		if (isCoordinator(msg)){
			Set<String> tmp =jessy
					.getConsistency().getConcerningKeys(
							msg.getExecutionHistory(),
							ConcernedKeysTarget.SEND_VOTES);

			Group g;
			for (String str:tmp){
				g=jessy.partitioner.resolve(str);
				for (int tmpswid:g.allNodes()){
					tmp.add(""+tmpswid);
				}
			}
			voteReceivers.addAll(tmp);
			voteSenders.add(swid);
		}
		else{
			String firstWriteKey=msg.getExecutionHistory().getWriteSet().getKeys().iterator().next();
			voteSenders.add(""+jessy.partitioner.resolve(firstWriteKey).leader());

			if (jessy.partitioner.resolveNames(jessy
					.getConsistency().getConcerningKeys(
							msg.getExecutionHistory(),
							ConcernedKeysTarget.RECEIVE_VOTES)).contains(group))
			{

				voteReceivers =new HashSet<String>();
				voteReceivers.add(""+jessy.partitioner.resolve(firstWriteKey).leader());
			}

		}
		
	}

	@Override
	public void sendVote(VoteMessage voteMessage,
			TerminateTransactionRequestMessage msg) {
		String firstWriteKey=msg.getExecutionHistory().getWriteSet().getKeys().iterator().next();
		voteMessage.getVote().setVoterEntityName(swid);
		voteMulticast.sendVote(voteMessage, jessy.partitioner.resolve(firstWriteKey));
	}
	
	@Override
	public void quorumReached(TerminateTransactionRequestMessage msg,TransactionState state){
		if (isCoordinator(msg)){
			
			Set<String> voteReceivers=	jessy.partitioner.resolveNames(jessy
					.getConsistency().getConcerningKeys(
							msg.getExecutionHistory(),
							ConcernedKeysTarget.RECEIVE_VOTES));
			
			boolean committed=(state==TransactionState.COMMITTED) ? true : false;
			Vote vote=new Vote(msg.getExecutionHistory().getTransactionHandler(), committed , swid, null);
			VoteMessage voteMsg = new VoteMessage(vote, voteReceivers,
					group.name(), jessy.manager.getSourceId());
		}
		
	}
	
	private boolean isCoordinator(TerminateTransactionRequestMessage msg){
		String firstWriteKey=msg.getExecutionHistory().getWriteSet().getKeys().iterator().next();
		if (jessy.partitioner.resolve(firstWriteKey).leader() == jessy.manager.getSourceId()){
			return true;
		}
		else{
			return false;
		}
	}

}
