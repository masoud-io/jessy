package fr.inria.jessy.transaction.termination.vote;

import java.util.Collection;
import java.util.HashSet;

import org.apache.log4j.Logger;

import fr.inria.jessy.transaction.TransactionHandler;
import fr.inria.jessy.transaction.TransactionState;
import fr.inria.jessy.transaction.termination.GroupCommunicationCommit;
import fr.inria.jessy.transaction.termination.TwoPhaseCommit;

/**
 * This class receives {@link Vote} from different participations in the
 * transaction termination phase. It returns {@link TransactionState.COMMITTED}
 * if it receives <code>true</code> votes from all replica/groups concerned
 * by the transaction. If it receives a <code>false</code> vote from a member of
 * a group or a process, it will returns {@link TransactionState.ABORTED_BY_VOTING}.
 * Otherwise, it will simply waits until receiving enough votes.
 * <p>
 * 
 * @author Masoud Saeida Ardekani
 * @author Pierre Sutra
 * 
 */
public abstract class VotingQuorum {
	protected static Logger logger = Logger.getLogger(GroupVotingQuorum.class);
	
	protected TransactionState result = TransactionState.COMMITTING;
	protected TransactionHandler transactionHandler;
	
	protected Collection<String> voters;
	protected boolean notified=false;

	public VotingQuorum(TransactionHandler th){
		transactionHandler = th;
		voters = new HashSet<String>();

	}
	
	/**
	 * 
	 * It simply waits until it receives enough votes from members of all groups/replicas
	 * concerned by the transaction.
	 * 
	 * @param vote
	 */
	public abstract void addVote(Vote vote);
	
	/**
	 * 
	 * @param concernedEntities either number of groups or number of replicas that the voting quorum should wait for.
	 * <p>
	 * In {@link TwoPhaseCommit}, replicas are used, and in {@link GroupCommunicationCommit} groups are used.
	 * 
	 * @return
	 */
	public abstract TransactionState waitVoteResult(Collection<String> concernedEntities) ;
	
	public Collection<String> getVoters() {
		return voters;
	}
}
