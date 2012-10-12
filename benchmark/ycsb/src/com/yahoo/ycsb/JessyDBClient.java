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

	// FIXME merge this into init
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

	public JessyDBClient() {
		super();
//		 try {
//		 if (USE_DIST_JESSY) {
////		 jessy = DistributedJessy.getInstance();
//			 jessy=new DistributedJessy();
//		 } else {
//		 jessy = LocalJessy.getInstance();
//		 }
//		 jessy.addEntity(YCSBEntity.class);
//		 } catch (Exception e) {
//		 e.printStackTrace();
//		 }
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

			Transaction trans = new Transaction(jessy) {
				@Override
				public ExecutionHistory execute() {
					for (YCSBTransactionalReadRequest request : readList) {
						try {
							YCSBEntity en = read(YCSBEntity.class, request.key);
							if (en == null) {
								logger.error("Read Operation for: "
										+ request.key + " failed.");
								return null;
							}
						} catch (Exception e) {
							// TODO Auto-generated catch block
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
			// TODO Auto-generated catch block
			e.printStackTrace();
			return -1;
		}
	}

	@Override
	public int updateTransaction(
			final List<YCSBTransactionalReadRequest> readList,
			final List<YCSBTransactionalUpdateRequest> updateList) {
		try {

			Transaction trans = new Transaction(jessy) {
				@Override
				public ExecutionHistory execute() {
					for (YCSBTransactionalReadRequest request : readList) {
						try {
							YCSBEntity en = read(YCSBEntity.class, request.key);
							if (en == null)
								return null;
						} catch (Exception e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					}

					for (YCSBTransactionalUpdateRequest request : updateList) {
						try {
							YCSBEntity en = read(YCSBEntity.class, request.key);
							if (en == null)
								return null;

							en.setFields(request.values);
							write(en);

						} catch (Exception e) {
							// TODO Auto-generated catch block
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
			// TODO Auto-generated catch block
			e.printStackTrace();
			return -1;
		}
	}

	@Override
	public int createTransaction(
			final YCSBTransactionalCreateRequest createRequest) {
		try {

			Transaction trans = new Transaction(jessy) {
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
			// TODO Auto-generated catch block
			e.printStackTrace();
			return -1;
		}
	}

}
