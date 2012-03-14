package fr.inria.jessy;

import fr.inria.jessy.store.JessyEntity;
import fr.inria.jessy.transaction.TransactionHandler;
import fr.inria.jessy.vector.CompactVector;

public class DistributedJessy extends Jessy{

	private static DistributedJessy distributedJessy = null;

	private DistributedJessy() throws Exception {
		super();
	}

	public static synchronized DistributedJessy getInstance() throws Exception {
		if (distributedJessy == null) {
			distributedJessy = new DistributedJessy();
		}
		return distributedJessy;
	}

	@Override
	protected <E extends JessyEntity, SK> E performRead(Class<E> entityClass,
			String keyName, SK keyValue, CompactVector<String> readSet) {
		if (Partitioner.getInstance().isLocal(entityClass.toString()+keyValue.toString())){
			return getDataStore().get(entityClass, keyName, keyValue, readSet);
		}
		else{
			RemoteReader.getInstance().remoteRead(h, v, k)ead(h, v, k)
		}
		
		
		return null;
	}

	@Override
	public boolean terminateTransaction(TransactionHandler transactionHandler) {
		// TODO Auto-generated method stub
		return false;
	}

}
