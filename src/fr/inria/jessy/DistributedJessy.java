package fr.inria.jessy;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import fr.inria.jessy.store.JessyEntity;
import fr.inria.jessy.store.ReadRequest;
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
			String keyName, SK keyValue, CompactVector<String> readSet) throws InterruptedException, ExecutionException {
		ReadRequest<E, SK> readRequest=new ReadRequest<E, SK>(entityClass,keyName,keyValue,readSet);
		
		if (Partitioner.getInstance().isLocal(readRequest.getPartitioningKey())){
			return getDataStore().get(readRequest).getEntity();
		}
		else{
			Future<E> future=RemoteReader.getInstance().remoteRead(readRequest);
			return future.get();
		}		
		
	}

	@Override
	public boolean terminateTransaction(TransactionHandler transactionHandler) {
		// TODO Auto-generated method stub
		return false;
	}

}
