package fr.inria.jessy.protocol;

import fr.inria.jessy.ConstantPool;
import fr.inria.jessy.communication.JessyGroupManager;
import fr.inria.jessy.consistency.Consistency;
import fr.inria.jessy.consistency.RC;
import fr.inria.jessy.store.DataStore;
import fr.inria.jessy.utils.Configuration;

public class ProtocolFactory {

	private static Consistency _instance;

	private static String protocolName;
	
	static{
		protocolName = Configuration
				.readConfig(ConstantPool.CONSISTENCY_TYPE);	
	}

	public static Consistency initProtocol(JessyGroupManager m, DataStore dataStore) {
		
		if (_instance != null)
			return _instance;

		if (protocolName.equals("serrano_sv_gc")) {
			_instance = new Serrano_SV_GC(m, dataStore);
		} else if (protocolName.equals("pstore_lsv_gc")) {
			_instance = new PStore_LSV_GC(m, dataStore);
		} else if (protocolName.equals("pstore_lsv_2pc")) {
			_instance = new PStore_LSV_2PC(m, dataStore);			
		} else if (protocolName.equals("sdur_vv_gc")) {
			_instance = new SDUR_VV_GC(m, dataStore);
		} else if (protocolName.equals("rc")) {
			_instance = new RC(m, dataStore);
		} else if (protocolName.equals("psi_vv_gc")) {
			_instance = new PSI_VV_GC(m, dataStore);
		} else if (protocolName.equals("us_dv_gc")) {
			_instance = new US_DV_GC(m, dataStore);
		} else if (protocolName.equals("us_gmv_gc")) {
			_instance = new US_GMV_GC(m, dataStore);
		} else if (protocolName.equals("gmu_gmv_2pc")) {
			_instance = new GMU_GMV_2PC(m, dataStore);
		}else if (protocolName.equals("nmsi_dv_gc")) {
			_instance = new NMSI_DV_GC(m, dataStore);
		} else if (protocolName.equals("nmsi_gmv_gc")) {
			_instance = new NMSI_GMV_GC(m,dataStore);
		} else if (protocolName.equals("nmsi_gmv2_gc")) {
			_instance = new NMSI_GMV2_GC(m, dataStore);
		} else if (protocolName.equals("nmsi_pdv_gc")) {
			_instance = new NMSI_PDV_GC(m,dataStore);			
		} else if (protocolName.equals("walter_vv_2pc")) {
			_instance = new Walter_VV_2PC(m, dataStore);
		} else if (protocolName.equals("ser_pdv_gc")) {
			_instance = new SER_PDV_GC(m, dataStore);
		}
		
		System.out.println("Protocol " + protocolName + " is initalized with persistence directory " + protocolName);
		if (_instance!=null)
			System.out.println("Protocol " + _instance.getClass().toString() + " is initialized.");
		return _instance;
	}

	public static Consistency getProtocolInstance() {
		return _instance;
	}

	public static String getProtocolName() {
		return protocolName;
	}
	
}
