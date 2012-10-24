package fr.inria.jessy;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutionException;

import com.sleepycat.je.DatabaseException;
import com.yahoo.ycsb.measurements.Measurements;

import fr.inria.jessy.ConstantPool.MeasuredOperations;
import fr.inria.jessy.ConstantPool.TransactionPhase;
import fr.inria.jessy.consistency.Consistency;
import fr.inria.jessy.store.DataStore;
import fr.inria.jessy.store.JessyEntity;
import fr.inria.jessy.store.ReadRequestKey;
import fr.inria.jessy.transaction.ExecutionHistory;
import fr.inria.jessy.transaction.TransactionHandler;
import fr.inria.jessy.vector.CompactVector;

/**
 * Wrapper around Jessy that measures latencies and counts return codes.
 */
public class JessyWrapper extends Jessy{

	Jessy _jessy;
	Measurements _measurements;

	//	private static JessyWrapper _jessyWrapper = null;
	//	
	//	public static synchronized JessyWrapper getInstance(Jessy jessy) throws Exception {
	//		if (_jessyWrapper == null) {
	//			_jessyWrapper = new JessyWrapper(jessy);
	//		}
	//		return _jessyWrapper;
	//	}

	public JessyWrapper(Jessy jessy) throws Exception{
		//		super();
		_jessy=jessy;
		_measurements=Measurements.getMeasurements();
	}

	//	TODO distinguish between songle/multiple keys reads?
	/**
	 * Performs a local or remote read operation depending on the specific implementation of Jessy.
	 * Each field/value pair from the result will be stored in a HashMap.
	 * @throws ExecutionException 
	 * @throws InterruptedException 
	 */
	@Override
	protected <E extends JessyEntity, SK> E performRead(Class<E> entityClass,
			String keyName, SK keyValue, CompactVector<String> readSet) throws InterruptedException, ExecutionException
			{

		if(!Measurements._operationWideMeasurement){
			return _jessy.performRead(entityClass, keyName, keyValue, readSet);
		}
		else{
			int ReturnCode=0;
			long st = System.currentTimeMillis();
			E res = _jessy.performRead(entityClass, keyName, keyValue, readSet);
			long en = System.currentTimeMillis();

			_measurements.measure(TransactionPhase.OW, MeasuredOperations.READ, (int) (en - st));
			if(res==null){
				ReturnCode=-1;
			}
			_measurements.reportReturnCode(TransactionPhase.OW, MeasuredOperations.READ, ReturnCode);

			return res;
		}
			}

	/**
	 * Performs a local or remote read operation depending on the specific implementation of Jessy on all provided keys. 
	 * Each field/value pair from the result will be stored in a HashMap.
	 */
	@Override
	protected <E extends JessyEntity> Collection<E> performRead(
			Class<E> entityClass, List<ReadRequestKey<?>> keys,
			CompactVector<String> readSet) throws InterruptedException,
			ExecutionException {

		if(!Measurements._operationWideMeasurement){
			return  _jessy.performRead(entityClass, keys, readSet);
		}
		else{
			int ReturnCode=0;
			long st = System.currentTimeMillis();
			Collection<E> res = _jessy.performRead(entityClass, keys, readSet);
			long en = System.currentTimeMillis();

			_measurements.measure(TransactionPhase.OW, MeasuredOperations.READ, (int) (en - st));
			if(res==null){
				ReturnCode=-1;
			}
			_measurements.reportReturnCode(TransactionPhase.OW, MeasuredOperations.READ, ReturnCode);

			return res;
		}
	}

	@Override
	public ExecutionHistory commitTransaction(
			TransactionHandler transactionHandler) {

		if(!Measurements._transactionWideMeasurement){
			return  _jessy.commitTransaction(transactionHandler);
		}
		else{
			int returnCode=0;

			long st = System.currentTimeMillis();
			ExecutionHistory res = _jessy.commitTransaction(transactionHandler);
			long en = System.currentTimeMillis();

			if(res==null){
				returnCode=-1;
			}
			else{
				boolean committed=false;
				switch (res.getTransactionState()) {
				case  COMMITTED:
					_measurements.measure(TransactionPhase.TER, MeasuredOperations.COMMITTED, (int) (en - st));
					committed=true;
					break;

				case  ABORTED_BY_CERTIFICATION:
					_measurements.measure(TransactionPhase.TER, MeasuredOperations.ABORTED_BY_CERTIFICATION, (int) (en - st));
					break;

				case  ABORTED_BY_VOTING:
					_measurements.measure(TransactionPhase.TER, MeasuredOperations.ABORTED_BY_VOTING, (int) (en - st));
					break;

				case  ABORTED_BY_CLIENT:
					_measurements.measure(TransactionPhase.TER, MeasuredOperations.ABORTED_BY_CLIENT, (int) (en - st));
					break;

				case  ABORTED_BY_TIMEOUT:
					_measurements.measure(TransactionPhase.TER, MeasuredOperations.ABORTED_BY_TIMEOUT, (int) (en - st));
					break;

				default:
					break;
				}

				_measurements.measure(TransactionPhase.TER, MeasuredOperations.TERMINATED, (int) (en - st));
				if(!committed){
					_measurements.measure(TransactionPhase.TER, MeasuredOperations.ABORTED, (int) (en - st));
				}

			}
			_measurements.reportReturnCode(TransactionPhase.TER, MeasuredOperations.TERMINATED, returnCode);

			return res;
		}
	}

	//	TODO distinguish between Transactional/NON Transactional writes?
	/**
	 * Performs a non transactional write.Each field/value pair from the result will be stored in a HashMap.
	 * Writes never fail.
	 */
	@Override
	protected <E extends JessyEntity> void performNonTransactionalWrite(E entity)
			throws InterruptedException, ExecutionException {

		if(!Measurements._operationWideMeasurement){
			_jessy.performNonTransactionalWrite(entity);
		}
		else{
			long st = System.currentTimeMillis();
			_jessy.performNonTransactionalWrite(entity);
			long en = System.currentTimeMillis();

			_measurements.measure(TransactionPhase.OW, MeasuredOperations.WRITE, (int) (en - st));
		}
	}

	//	no measurements for this operation
	@Override
	public void applyModifiedEntities(ExecutionHistory executionHistory) {
		_jessy.applyModifiedEntities(executionHistory);
	}





	//	Override all methods implemented in Jessy abstract class to be executed from local/remote jessy instead of wrapper 

	@Override
	public ExecutionHistory abortTransaction(
			TransactionHandler transactionHandler) {

		if(!Measurements._transactionWideMeasurement){
			return _jessy.abortTransaction(transactionHandler);
		}
		else{

			int ReturnCode=0;
			long st = System.currentTimeMillis();
			ExecutionHistory res = _jessy.abortTransaction(transactionHandler);
			long en = System.currentTimeMillis();

			_measurements.measure(TransactionPhase.EXE, MeasuredOperations.ABORTED_BY_CLIENT, (int) (en - st));
			//		TODO is res null if the abort fails? abort can fail?
			if(res==null){
				ReturnCode=-1;
			}
			_measurements.reportReturnCode(TransactionPhase.EXE, MeasuredOperations.ABORTED_BY_CLIENT, ReturnCode);

			return res;
		}
	}

	@Override
	public <E extends JessyEntity> void addEntity(Class<E> entityClass)
			throws Exception{
		_jessy.addEntity(entityClass);
	}

	@Override
	public <E extends JessyEntity, SK> void addSecondaryIndex(
			Class<E> entityClass, Class<SK> keyClass, String keyName)
					throws Exception {
		_jessy.addSecondaryIndex(entityClass, keyClass, keyName);
	}

	@Override
	public void close(Object object) throws DatabaseException {
		_jessy.close(object);
	}

	@Override
	public <E extends JessyEntity> void create(
			TransactionHandler transactionHandler, E entity)
					throws NullPointerException {
		_jessy.create(transactionHandler, entity);
	}

	@Override
	public void garbageCollectTransaction(TransactionHandler transactionHandler) {
		_jessy.garbageCollectTransaction(transactionHandler);
	}

	@Override
	public Consistency getConsistency() {
		return _jessy.getConsistency();
	}

	@Override
	public DataStore getDataStore() {
		return _jessy.getDataStore();
	}

	@Override
	public ExecutionHistory getExecutionHistory(
			TransactionHandler transactionHandler) {
		return _jessy.getExecutionHistory(transactionHandler);
	}

	@Override
	public void open() {
		_jessy.open();
	}

	@Override
	public <E extends JessyEntity> E read(Class<E> entityClass, String keyValue)
			throws Exception {

		if(!Measurements._operationWideMeasurement){
			return _jessy.read(entityClass, keyValue);
		}
		else{
			int ReturnCode=0;
			long st = System.currentTimeMillis();
			E res = _jessy.read(entityClass, keyValue);
			long en = System.currentTimeMillis();

			_measurements.measure(TransactionPhase.OW, MeasuredOperations.READ, (int) (en - st));
			if(res==null){
				ReturnCode=-1;
			}
			_measurements.reportReturnCode(TransactionPhase.OW, MeasuredOperations.READ, ReturnCode);

			return res;
		}
	}

	@Override
	public <E extends JessyEntity, SK> Collection<E> read(
			TransactionHandler transactionHandler, Class<E> entityClass,
			List<ReadRequestKey<?>> keys) throws Exception {

		if(!Measurements._operationWideMeasurement){
			return _jessy.read(transactionHandler, entityClass, keys);
		}
		else{
			int ReturnCode=0;
			long st = System.currentTimeMillis();
			Collection<E> res = _jessy.read(transactionHandler, entityClass, keys);
			long en = System.currentTimeMillis();

			_measurements.measure(TransactionPhase.OW, MeasuredOperations.READ, (int) (en - st));
			if(res==null){
				ReturnCode=-1;
			}
			_measurements.reportReturnCode(TransactionPhase.OW, MeasuredOperations.READ, ReturnCode);

			return res;
		}
	}

	@Override
	public <E extends JessyEntity> E read(
			TransactionHandler transactionHandler, Class<E> entityClass,
			String keyValue) throws Exception {

		if(!Measurements._operationWideMeasurement){
			return _jessy.read(transactionHandler, entityClass, keyValue);
		}
		else{
			int ReturnCode=0;
			long st = System.currentTimeMillis();
			E res = _jessy.read(transactionHandler, entityClass, keyValue);
			long en = System.currentTimeMillis();

			_measurements.measure(TransactionPhase.OW, MeasuredOperations.READ, (int) (en - st));
			if(res==null){
				ReturnCode=-1;
			}
			else{
				_measurements.measure(TransactionPhase.OW, MeasuredOperations.VECTOR_SIZE, res.getLocalVector().size());

			}
			_measurements.reportReturnCode(TransactionPhase.OW, MeasuredOperations.READ, ReturnCode);

			return res;
		}
	}

	@Override
	public synchronized void registerClient(Object object) {
		_jessy.registerClient(object);
	}

	@Override
	public <E extends JessyEntity> void remove(
			TransactionHandler transactionHandler, E entity)
					throws NullPointerException {
		_jessy.remove(transactionHandler, entity);
	}

	@Override
	public void setExecutionHistory(ExecutionHistory executionHistory) {
		_jessy.setExecutionHistory(executionHistory);
	}

	@Override
	public TransactionHandler startTransaction() throws Exception {
		return _jessy.startTransaction();
	}

	@Override
	public <E extends JessyEntity> void write(E entity) throws Exception {

		if(!Measurements._operationWideMeasurement){
			_jessy.write(entity);
		}
		else{
			long st = System.currentTimeMillis();
			_jessy.write(entity);
			long en = System.currentTimeMillis();
			_measurements.measure(TransactionPhase.OW, MeasuredOperations.WRITE, (int) (en - st));
		}
	}

	@Override
	public <E extends JessyEntity> void write(
			TransactionHandler transactionHandler, E entity)
					throws NullPointerException {

		if(!Measurements._operationWideMeasurement){
			_jessy.write(transactionHandler, entity);
		}
		else{
			long st = System.currentTimeMillis();
			_jessy.write(transactionHandler, entity);
			long en = System.currentTimeMillis();
			_measurements.measure(TransactionPhase.OW, MeasuredOperations.WRITE, (int) (en - st));
		}
	}

}
