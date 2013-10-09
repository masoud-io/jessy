package com.yahoo.ycsb;

import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.Vector;

import org.apache.log4j.Logger;

import com.sleepycat.je.DatabaseException;
import com.yahoo.ycsb.workloads.YCSBTransactionalCreateRequest;
import com.yahoo.ycsb.workloads.YCSBTransactionalReadRequest;
import com.yahoo.ycsb.workloads.YCSBTransactionalUpdateRequest;

import fr.inria.jessy.DebuggingFlag;
import fr.inria.jessy.DistributedJessy;
import fr.inria.jessy.Jessy;
import fr.inria.jessy.LocalJessy;
import fr.inria.jessy.transaction.ExecutionHistory;
import fr.inria.jessy.transaction.Transaction;
import fr.inria.jessy.transaction.TransactionState;

public class JessyDBClient extends DB {

	private static Logger logger = Logger.getLogger(JessyDBClient.class);

	private static boolean USE_DIST_JESSY = true;

	private static Jessy jessy;

	static {
		try {
			if (USE_DIST_JESSY) {
				jessy = DistributedJessy.getInstance();
			} else {
				jessy = LocalJessy.getInstance();
			}
			jessy.addEntity(YCSBEntity.class);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Override
	public void init() {
		jessy.registerClient(this);
	}

	@Override
	public void cleanup() {
		try {
			jessy.close(this);
		} catch (DatabaseException ex) {
			ex.printStackTrace();
		}
	}

	@Override
	public int read(String table, String key, Set<String> fields,
			HashMap<String, String> result) {
		try {
			YCSBEntity en = jessy.read(YCSBEntity.class, key);
			if (en == null) {
				logger.error("unable to read " + key);
				return -1;
			}
		} catch (Exception e) {
			e.printStackTrace();
			return -1;
		}
		logger.debug("successfull read " + key);
		return 0;
	}

	@Override
	public int scan(String table, String startkey, int recordcount,
			Set<String> fields, Vector<HashMap<String, String>> result) {
		throw new RuntimeException("NYI");
	}

	@Override
	public int update(String table, String key, HashMap<String, String> values) {

		try {
			YCSBEntity en = jessy.read(YCSBEntity.class, key);
			if (en == null) {
				logger.error("unable to update " + key);
				return -1;
			}
			en.setFields(values);
			jessy.write(en);
		} catch (Exception e) {
			e.printStackTrace();
			return -1;
		}
		logger.debug("successfull update " + key);
		return 0;
	}

	@Override
	public int insert(String table, String key, HashMap<String, String> values) {
		try {
			YCSBEntity en = new YCSBEntity(key, values);
			jessy.write(en);
		} catch (Exception e) {
			e.printStackTrace();
			return -1;
		}

		logger.debug("successfull insert " + key);
		return 0;
	}

	@Override
	public int delete(String table, String key) {
		throw new RuntimeException("NYI");
	}

	@Override
	public int readTransaction(final List<YCSBTransactionalReadRequest> readList) {
		try {

			Transaction trans = new Transaction(jessy, readList.size(), 0,0) {
				@Override
				public ExecutionHistory execute() {
					for (YCSBTransactionalReadRequest request : readList) {
						try {
							YCSBEntity en = read(YCSBEntity.class, request.key);
							if (en == null) {
								logger.error("Read Operation (r-o txs) for: "
										+ request.key + " failed.");
								return null;
							}
						} catch (Exception e) {
							e.printStackTrace();
						}
					}
					return commitTransaction();
				}
			};

			ExecutionHistory history = trans.execute();
			if (history == null)
				return -1;
			if (history.getTransactionState() == TransactionState.COMMITTED) {
				return 0;
			} else
				return -1;

		} catch (Exception e) {
			e.printStackTrace();
			return -1;
		}
	}

	@Override
	public int updateTransaction(
			final List<YCSBTransactionalReadRequest> readList,
			final List<YCSBTransactionalUpdateRequest> updateList) {
		try {
			Transaction trans = new Transaction(jessy, readList.size() + updateList.size(),updateList.size(),0 ) {
				@Override
				public ExecutionHistory execute() {
					
					try {
						
						/*
						 * First, we execute the read operations,
						 */
						for (YCSBTransactionalReadRequest request : readList) {
							try {
								
								if (DebuggingFlag.JESSY_DB_CLIENT)
									logger.debug("Embedded transaction starts execution of read " + this.getTransactionHandler().toString() + " for key " + request.key);
								
								YCSBEntity en = read(YCSBEntity.class, request.key);
								
								if (DebuggingFlag.JESSY_DB_CLIENT)
									logger.debug("Embedded transaction finishes execution of read " + this.getTransactionHandler().toString() + " for key " + request.key);

								if (en == null){
									if (DebuggingFlag.JESSY_DB_CLIENT)
										logger.error("Read Operation (update txs) for: "
												+ request.key + " failed.");								
									return null;
								}
							} catch (Exception e) {
								e.printStackTrace();
							}
						}

						/*
						 * Second, and since we have read before writes, we read what we would like to write.
						 * Then, we perform write operations.
						 */
						for (YCSBTransactionalUpdateRequest request : updateList) {
							try {

								if (DebuggingFlag.JESSY_DB_CLIENT)
									logger.debug("Embedded transaction starts execution of read " + this.getTransactionHandler().toString() + " for key " + request.key);

								YCSBEntity en = read(YCSBEntity.class, request.key);
								
								if (DebuggingFlag.JESSY_DB_CLIENT)
									logger.debug("Embedded transaction finishes execution of read " + this.getTransactionHandler().toString() + " for key " + request.key);

								
								if (en == null){
									if (DebuggingFlag.JESSY_DB_CLIENT)
										logger.error("Read Operation (update txs-seond part) for: "
												+ request.key + " failed.");								
									return null;
								}

								en.setFields(request.values);
								write(en);

							} catch (Exception e) {
								e.printStackTrace();
							}
						}

						if (DebuggingFlag.JESSY_DB_CLIENT)
							logger.debug("Embedded transaction starts committing " + this.getTransactionHandler().toString());
						
						return commitTransaction();
					} catch (Exception e) {
						e.printStackTrace();
					}
					
					return null;
				}
			};

			if (DebuggingFlag.JESSY_DB_CLIENT)
				logger.info("Client starts " + trans.getTransactionHandler().toString());
			
			ExecutionHistory history = trans.execute();
			
			if (DebuggingFlag.JESSY_DB_CLIENT)
				logger.info("Client finishes " + trans.getTransactionHandler().toString());
			
			if (history == null){
				if (DebuggingFlag.JESSY_DB_CLIENT)
					logger.error("Returned history from exeuction is Null with id "+ history.getTransactionHandler().getId());
				return -1;
			}
			if (history.getTransactionState() == TransactionState.COMMITTED) {
				return 0;
			} else{
				if (DebuggingFlag.JESSY_DB_CLIENT)
					logger.error("Returned history from exeuction is not committed with id "+ history.getTransactionHandler().getId());
				return -1;
			}

		} catch (Exception e) {
			e.printStackTrace();
			return -1;
		}
	}

	@Override
	public int createTransaction(
			final YCSBTransactionalCreateRequest createRequest) {
		try {

			Transaction trans = new Transaction(jessy,0,0,1) {
				@Override
				public ExecutionHistory execute() {

					YCSBEntity en = new YCSBEntity(createRequest.key,
							createRequest.values);

					create(en);
					return commitTransaction();
				}
			};

			ExecutionHistory history = trans.execute();
			if (history == null)
				return -1;
			if (history.getTransactionState() == TransactionState.COMMITTED) {
				return 0;
			} else
				return -1;

		} catch (Exception e) {
			e.printStackTrace();
			return -1;
		}
	}

}
