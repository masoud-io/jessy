package com.yahoo.ycsb;

import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;
import com.yahoo.ycsb.workloads.YCSBTransactionalCreateRequest;
import com.yahoo.ycsb.workloads.YCSBTransactionalReadRequest;
import com.yahoo.ycsb.workloads.YCSBTransactionalUpdateRequest;

import org.infinispan.Cache;
import org.infinispan.atomic.AtomicMap;
import org.infinispan.atomic.AtomicMapLookup;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.util.FastCopyHashMap.EntrySet;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.Vector;

/**
 * This is a client implementation for Infinispan 5.x.
 *
 * Some settings:
 *
 * @author Manik Surtani (manik AT jboss DOT org)
 */
public class InfinispanClient extends DB {

	private static final int OK = 0;
	private static final int ERROR = -1;
	private static final int NOT_FOUND = -2;

	// An optimisation for clustered mode
	private final boolean clustered;

	private EmbeddedCacheManager infinispanManager;

	//private static final Log logger = LogFactory.getLog(InfinispanClient.class);

	public InfinispanClient() {
		
		clustered = Boolean.getBoolean("infin" +
				"ispan.clustered");
		
	}

	public void init() throws DBException {
			
			
				
					infinispanManager = new DefaultCacheManager();
				
	
	}

	public void cleanup() {
		infinispanManager.stop();
		infinispanManager = null;
	}

	public int read(String table, String key, Set<String> fields, HashMap<String, String> result) {
		try {
			Map<String, String> row;
			if (clustered) {
				row = AtomicMapLookup.getAtomicMap(infinispanManager.getCache(table), key, false);
			} else {
				Cache<String, Map<String, String>> cache = infinispanManager.getCache(table);
				row = cache.get(key);
			}
			if (row != null) {
				result.clear();
				if (fields == null || fields.isEmpty()) {
					
					for(String k : row.keySet())
						result.put(k, row.get(k));	
				} else {
					for (String field : fields) result.put(field,row.get(field));
				}
			}
			return OK;
		} catch (Exception e) {
			return ERROR;
		}
	}

	public int scan(String table, String startkey, int recordcount, Set<String> fields, Vector<HashMap<String, String>> result) {
		//logger.warn("Infinispan does not support scan semantics");
		return OK;
	}

	public int update(String table, String key, HashMap<String, String> values) {
		try {
			if (clustered) {
				AtomicMap<String, String> row = AtomicMapLookup.getAtomicMap(infinispanManager.getCache(table), key);
				for(String k : values.keySet())
					row.put(k, values.get(k));
			} else {
				Cache<String, Map<String, String>> cache = infinispanManager.getCache(table);
				Map<String, String> row = cache.get(key);
				if (row == null) {
					row = values;
					cache.put(key, row);
				} else {
					for(String k : values.keySet())
						row.put(k, values.get(k));
				}
			}

			return OK;
		} catch (Exception e) {
			return ERROR;
		}
	}

	public int insert(String table, String key, HashMap<String, String> values) {
		try {
			if (clustered) {
				AtomicMap<String, String> row = AtomicMapLookup.getAtomicMap(infinispanManager.getCache(table), key);
				row.clear();
				for(String k : values.keySet())
					row.put(k, values.get(k));

			} else {
				infinispanManager.getCache(table).put(key, values);
			}

			return OK;
		} catch (Exception e) {
			return ERROR;
		}
	}

	public int delete(String table, String key) {
		try {
			if (clustered)
				AtomicMapLookup.removeAtomicMap(infinispanManager.getCache(table), key);
			else
				infinispanManager.getCache(table).remove(key);
			return OK;
		} catch (Exception e) {
			return ERROR;
		}
	}

	@Override
	public int createTransaction(YCSBTransactionalCreateRequest createRequest) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int readTransaction(List<YCSBTransactionalReadRequest> readList) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int updateTransaction(List<YCSBTransactionalReadRequest> readList,
			List<YCSBTransactionalUpdateRequest> updateList) {
		// TODO Auto-generated method stub
		return 0;
	}
}
