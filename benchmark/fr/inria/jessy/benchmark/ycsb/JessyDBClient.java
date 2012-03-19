package fr.inria.jessy.benchmark.ycsb;
import java.io.BufferedWriter;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Set;
import java.util.Vector;

import com.sun.org.apache.bcel.internal.generic.InstructionHandle;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;

import fr.inria.jessy.Jessy;
import fr.inria.jessy.LocalJessy;



public class JessyDBClient extends DB {
	/*Jessy Client*/
	private static Jessy jessy;
	private static  DataOutputStream instructionHistory;
	private static  DataOutputStream errorLogger;
	private static int inst;
	/*JessyDbClientCOnstructor*/
	public JessyDBClient() throws Exception {
		super();
		initDB();
		
	}
	private static void initDB() throws Exception {
		
		jessy = LocalJessy.getInstance();
		jessy.addEntity(YCSBEntity.class);
		inst = 0;
		File f = new File("error.txt");
		f.delete();
		f = new File("all_the_instructions.txt");
		f.delete();
		FileOutputStream fos = new FileOutputStream("error.txt",false);
		errorLogger = new DataOutputStream(fos);
		fos = new FileOutputStream("all_the_instructions.txt",false);
		instructionHistory = new DataOutputStream(fos);
		
		
		//TODO : Finish this work
	}
	@Override
	public void init() throws DBException {
				super.init();
				try {
					initDB();
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
	}
	
	/*Read an occurence identified with a key */
	@Override
	public int read(String table, String key, Set<String> fields,
			HashMap<String, String> result) {
		inst++;
		Operation op = new Operation(inst, table+key, OPState.UNKNOWN,OPType.READ);
		
		YCSBEntity en;
		try {
		 en=jessy.read(YCSBEntity.class, table+key);
		
			
		} catch (Exception e) {
			op.setState(OPState.EXCEPTIONFAILED);
			
			e.printStackTrace();
		
			return -1;
		}
		if (en == null){
			op.setState(OPState.NULLFAILED);
			return -1;
		
		}

		
		
		op.setState(OPState.SUCCESS);
		try {
			
			instructionHistory.write(op.toString().getBytes());
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
		
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
		Operation op = new Operation(inst,table+key, OPState.UNKNOWN,OPType.UPDATE);
		inst++;
		try {
			
			
			
			YCSBEntity en =  jessy.read(YCSBEntity.class,table+key);
			if(en==null){
				op.setState(OPState.NULLFAILED);
				errorLogger.write(op.toString().getBytes());
				instructionHistory.write(op.toString().getBytes());
				return -1;
			}
			
			en.setFields(values);
			jessy.write(en);
		} catch (Exception e) {
			op.setState(OPState.EXCEPTIONFAILED);
			try {
				errorLogger.write(op.toString().getBytes());
				instructionHistory.write(op.toString().getBytes());
			} catch (IOException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
			e.printStackTrace();
			return -1;
		}

		
		
		op.setState(OPState.SUCCESS);
		try {
			
			instructionHistory.write(op.toString().getBytes());
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
		
		return 0;  
	}

	@Override
	public int insert(String table, String key, HashMap<String, String> values) {
		Operation op = new Operation(inst,table+key, OPState.UNKNOWN,OPType.WRITE);
		inst++;
		try {
			//
			
			
			YCSBEntity entity  = new YCSBEntity(YCSBEntity.class.toString(),table+key,values);
			jessy.write(entity);
		
		} catch (Exception e) {
			op.setState(OPState.EXCEPTIONFAILED);
			e.printStackTrace();
			return -1;
		}

		
		
		op.setState(OPState.SUCCESS);
		try {
			
			instructionHistory.write(op.toString().getBytes());
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
		
		return 0;
	}

	
	@Override
	public int delete(String table, String key) {
		YCSBEntity e;
		
		Operation op = new Operation(inst, table+key, OPState.UNKNOWN,OPType.DELETE);
		
		inst++;
		try {
			e = jessy.read(YCSBEntity.class,table+key);
			if (e == null) {
				op.setState(OPState.NULLFAILED);
				try {
					errorLogger.write(op.toString().getBytes());
					instructionHistory.write(op.toString().getBytes());
				} catch (IOException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
				return -1;
			}
			e.removeAll();
			
			jessy.write(e);
		} catch (Exception e1) {
			// the operation generated an exception
			op.setState(OPState.EXCEPTIONFAILED);
			
			
			try {
				errorLogger.write(op.toString().getBytes());
				instructionHistory.write(op.toString().getBytes());
			} catch (IOException e2) {
				// TODO Auto-generated catch block
				e2.printStackTrace();
			}
			e1.printStackTrace();
			return -1;
		}
		
		
		
		op.setState(OPState.SUCCESS);
		try {
			
			instructionHistory.write(op.toString().getBytes());
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
		
		return 0;
	}

	

}
