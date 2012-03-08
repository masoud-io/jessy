package fr.inria.jessy.benchmark.ycsb;
import java.util.HashMap;
import java.util.Set;
import java.util.Vector;

import com.yahoo.ycsb.DB;

import fr.inria.jessy.Jessy;
import fr.inria.jessy.LocalJessy;
import fr.inria.jessy.store.JessyEntity;

public class JessyDBClient extends DB {
	/*Jessy Client*/
	private static Jessy jessy;
	

	/*JessyDbClientCOnstructor*/
	public JessyDBClient() throws Exception {
		initDB();
	}
	private static void initDB() throws Exception {
		jessy = LocalJessy.getInstance();
		jessy.addEntity(YCSBEntity.class);
		//TODO : Finish this work
	}
	
	/*Read an occurence identified with a key */
	@Override
	public int read(String table, String key, Set<String> fields,
			HashMap<String, String> result) {
		
		YCSBEntity en;
		try {
		 en=jessy.read(YCSBEntity.class, table+key);
		
			
		} catch (Exception e) {
			
			e.printStackTrace();
		
			return -1;
		}
		if (en == null)
			return -1;
		
		return 0;
	}


	@Override
	public int scan(String table, String startkey, int recordcount,
			Set<String> fields, Vector<HashMap<String, String>> result) {
		
		// same thing for fields and results
		
		throw new UnsupportedOperationException("Unsuopported operation yet");
		
	}

	@Override
	public int update(String table, String key, HashMap<String, String> values) {
		try {
			
			YCSBEntity en =  jessy.read(YCSBEntity.class,table+key);
			if(en==null){
				return -1;
			}
			
			en.setFields(values);
			jessy.write(en);
		} catch (Exception e) {
			
			e.printStackTrace();
			return -1;
		}
		return 0;  
	}

	@Override
	public int insert(String table, String key, HashMap<String, String> values) {
		
		try {
			//
			YCSBEntity entity  = new YCSBEntity(YCSBEntity.class.toString(),table+key,values);
			jessy.write(entity);
			
		} catch (Exception e) {
			
			e.printStackTrace();
			return -1;
		}
	
		return 0;
	}

	
	@Override
	public int delete(String table, String key) {
		YCSBEntity e;
		try {
			e = jessy.read(YCSBEntity.class,table+key);
			
			e.removeAll();
			
			jessy.write(e);
		} catch (Exception e1) {
			
			e1.printStackTrace();
			return -1;
		}
		return 0;
	}

	

}
