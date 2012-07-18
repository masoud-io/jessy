package fr.inria.jessy.transaction;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collection;

import net.sourceforge.fractal.Messageable;
import net.sourceforge.fractal.utils.PerformanceProbe.TimeRecorder;
import fr.inria.jessy.ConstantPool;
import fr.inria.jessy.store.EntitySet;
import fr.inria.jessy.store.JessyEntity;

//TODO COMMENT ME
public class ExecutionHistory implements Messageable {

	private static final long serialVersionUID = ConstantPool.JESSY_MID;

	public static enum TransactionType {
		/**
		 * the execution history is for a read only transaction
		 */
		READONLY_TRANSACTION,
		/**
		 * the execution history is for a blind write transaction
		 */
		BLIND_WRITE,
		/**
		 * the execution history is for an update transaction
		 */
		UPDATE_TRANSACTION,
		/**
		 * the execution history is for an initialization transaction
		 */
		INIT_TRANSACTION
	};

	private TransactionHandler transactionHandler;

	/**
	 * if true, the coordinator will perform a certification, thus does not need
	 * to receive the result from other processes. Otherwise, the proxy will
	 * only atomic multicast the transaction for certification, thus it needs to
	 * receive back the {@link TerminationResult}.
	 */
	private boolean certifyAtCoordinator;

	private TransactionState transactionState = TransactionState.NOT_STARTED;

	/**
	 * readSet and writeSet to store read and written entities
	 * 
	 */
	private EntitySet createSet;
	private EntitySet writeSet;
	private EntitySet readSet;
	private int coordinator;

	/**
	 * Start time (in nano) of the transaction upon A-Delivering to a server.
	 */
	private long startCertification;

	// for fractal
	@Deprecated
	public ExecutionHistory() {
	}

	public ExecutionHistory(TransactionHandler th) {
		readSet = new EntitySet();
		writeSet = new EntitySet();
		createSet = new EntitySet();
		transactionHandler = th;
		certifyAtCoordinator = false;
	}

	public EntitySet getReadSet() {
		return readSet;
	}

	public EntitySet getWriteSet() {
		return writeSet;
	}

	public EntitySet getCreateSet() {
		return createSet;
	}

	public <E extends JessyEntity> E getReadEntity(String keyValue) {
		return readSet.getEntity(keyValue);
	}

	public <E extends JessyEntity> E getWriteEntity(String keyValue) {
		return writeSet.getEntity(keyValue);
	}

	public <E extends JessyEntity> E getCreateEntity(String keyValue) {
		return createSet.getEntity(keyValue);
	}

	public <E extends JessyEntity> void addReadEntity(E entity) {
		readSet.addEntity(entity);
	}

	public <E extends JessyEntity> void addReadEntity(Collection<E> entities) {
		readSet.addEntity(entities);
	}

	public <E extends JessyEntity> void addWriteEntity(E entity) {
		writeSet.addEntity(entity);
	}

	public <E extends JessyEntity> void addCreateEntity(E entity) {
		createSet.addEntity(entity);
	}

	public TransactionType getTransactionType() {
		if (createSet.size() > 0 && writeSet.size() == 0 && readSet.size() == 0)
			return TransactionType.INIT_TRANSACTION;
		else if (readSet.size() == 0)
			return TransactionType.BLIND_WRITE;
		else if (writeSet.size() == 0)
			return TransactionType.READONLY_TRANSACTION;
		else
			return TransactionType.UPDATE_TRANSACTION;
	}

	public TransactionState getTransactionState() {
		return transactionState;
	}

	public void changeState(TransactionState transactionNewState) {
		transactionState = transactionNewState;
	}

	public String toString() {
		String result;
		result = transactionState.toString() + "\n";
		result = result + readSet.toString() + "\n";
		result = result + writeSet.toString() + "\n";
		return result;
	}

	public TransactionHandler getTransactionHandler() {
		return transactionHandler;
	}

	public boolean isCertifyAtCoordinator() {
		return certifyAtCoordinator;
	}

	public void setCertifyAtCoordinator(boolean certifyAtCoordinator) {
		this.certifyAtCoordinator = certifyAtCoordinator;
	}

	public void setCoordinator(int coordinator) {
		this.coordinator = coordinator;
	}

	public int getCoordinator() {
		return coordinator;
	}

	public void setStartCertification(long startCertification) {
		this.startCertification = startCertification;
	}

	public long getStartCertification() {
		return startCertification;
	}

	@SuppressWarnings("unchecked")
	public void readExternal(ObjectInput in) throws IOException,
			ClassNotFoundException {

		transactionHandler = (TransactionHandler) in.readObject();
		certifyAtCoordinator = in.readBoolean();
		transactionState = (TransactionState) in.readObject();

		createSet = (EntitySet) in.readObject();
		if(createSet == null ) createSet = new EntitySet();
		
		writeSet = (EntitySet) in.readObject();
		if(writeSet == null ) writeSet = new EntitySet();
		
		readSet = (EntitySet) in.readObject();
		if(readSet == null ) readSet = new EntitySet();

		coordinator = in.readInt();
	}

	public void writeExternal(ObjectOutput out) throws IOException {

		out.writeObject(transactionHandler);
		out.writeBoolean(certifyAtCoordinator);
		out.writeObject(transactionState);

		if(createSet.size()==0)
			out.writeObject(null);
		else
			out.writeObject(createSet);
			
		
		if(writeSet.size()==0)
			out.writeObject(null);
		else
			out.writeObject(writeSet);
		
		if(readSet.size()==0)
			out.writeObject(null);
		else
			out.writeObject(readSet);

		out.writeInt(coordinator);

	}

}
