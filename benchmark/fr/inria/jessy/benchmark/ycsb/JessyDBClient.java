package fr.inria.jessy.benchmark.ycsb;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.Vector;
import fr.inria.jessy.*;
import fr.inria.jessy.store.DataStore;
import fr.inria.jessy.store.JessyEntity;
import fr.inria.jessy.store.SampleEntityClass;

import com.sun.org.apache.xml.internal.security.Init;
import com.yahoo.ycsb.*;

public class JessyDBClient extends DB {
	/*For the Jessy Client*/
	private static Jessy _CLIENT;
	/*to store the tables*/
	private static  HashMap<String,Class<? extends JessyEntity>> _TABLES;
	

	/*JessyDbClientCOnstructor*/
	public JessyDBClient() throws Exception {
		initDB();
	}
	private static void initDB() throws Exception {
		_CLIENT = LocalJessy.getInstance();
		_TABLES = new HashMap<String,Class<? extends JessyEntity>>();
		//TODO : Finish this work
	}
	/*Create a table in dataStore*/
	public void AddTable (Class<? extends JessyEntity> entityClass) throws Exception {
		_CLIENT.addEntity(entityClass);
		_TABLES.put(entityClass.getClass().getName(),entityClass);
		
	}
	
	/*Read an occurence identified with a key */
	@Override
	public int read(String table, String key, Set<String> fields,
			HashMap<String, String> result) {
		
		JessyEntity en;
		
		Class<? extends JessyEntity> tableName = _TABLES.get(table);
		
		try {
		
			en  = _CLIENT.read(tableName,key);
			
		} catch (Exception e) {
			
			e.printStackTrace();
		
			return -1;
		}
		if (en == null)
			return -1;
		
		// I haven't found the usage of fields and result
				//==> let's do it later
		
		return 0;
	}


	@Override
	public int scan(String table, String startkey, int recordcount,
			Set<String> fields, Vector<HashMap<String, String>> result) {
		
		// same thing for fields and results
		/*Not implemented yet*/
		throw new UnsupportedOperationException("Unsuopported operation yet");
		
	}

	@Override
	public int update(String table, String key, HashMap<String, String> values) {
		
		/*Not implemented yet*/
		//TODO : read and delete , modify then update
		throw new UnsupportedOperationException("Unsuopported operation yet");
	}

	@Override
	public int insert(String table, String key, HashMap<String, String> values) {
		Class<? extends JessyEntity> c = _TABLES.get(table);
		try {
			//
			JessyEntity entity  = new SampleEntityClass();
			Set<String> keys = values.keySet();
			HashMap<String, String> towrite;
			for (String string : keys) {
				
			}
			_CLIENT.write(entity);
		} catch (Exception e) {
			
			e.printStackTrace();
			return -1;
		}
		
		
	
		return 0;
	}

	@Override
	public int delete(String table, String key) {
		JessyEntity e;
		try {
			e = _CLIENT.read(this._TABLES.get(table),key);
			/*not finished*/
		} catch (Exception e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
			return -1;
		}
		return 0;
	}

}
