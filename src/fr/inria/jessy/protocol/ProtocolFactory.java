package fr.inria.jessy.protocol;

import org.apache.log4j.Logger;

import fr.inria.jessy.ConstantPool;
import fr.inria.jessy.communication.JessyGroupManager;
import fr.inria.jessy.consistency.Consistency;
import fr.inria.jessy.consistency.RC;
import fr.inria.jessy.store.DataStore;
import fr.inria.jessy.utils.Configuration;
import fr.inria.jessy.vector.DependenceVector;
import fr.inria.jessy.vector.GMUVector;
import fr.inria.jessy.vector.GMUVector2;
import fr.inria.jessy.vector.LightScalarVector;
import fr.inria.jessy.vector.NullVector;
import fr.inria.jessy.vector.PartitionDependenceVector;
import fr.inria.jessy.vector.ScalarVector;
import fr.inria.jessy.vector.Vector;
import fr.inria.jessy.vector.VersionVector;

public class ProtocolFactory {

	private static Logger logger = Logger.getLogger(ProtocolFactory.class);

	private static Consistency _instance;

	private static String protocolName;
	
	static{
		protocolName = Configuration
				.readConfig(ConstantPool.CONSISTENCY_TYPE);	
	}

	public static Consistency initProtocol(JessyGroupManager m, DataStore dataStore) {
		
		if (_instance != null)
			return _instance;

		if (protocolName.equals("serrano_sv")) {
			_instance = new Serrano_SV(m, dataStore);
		} else if (protocolName.equals("pstore_lsv")) {
			_instance = new PStore_LSV(m, dataStore);
		} else if (protocolName.equals("sdur_vv")) {
			_instance = new SDUR_VV(m, dataStore);
		} else if (protocolName.equals("rc")) {
			_instance = new RC(m, dataStore);
		} else if (protocolName.equals("psi_vv_gc")) {
			_instance = new PSI_VV_GC(m, dataStore);
		} else if (protocolName.equals("us_dv_gc")) {
			_instance = new US_DV_GC(m, dataStore);
		} else if (protocolName.equals("us_gmv_gc")) {
			_instance = new US_GMV_GC(m, dataStore);
		} else if (protocolName.equals("gmu_gmv")) {
			_instance = new GMU_GMV(m, dataStore);
		}else if (protocolName.equals("nmsi_dv_gc")) {
			_instance = new NMSI_DV_GC(m, dataStore);
		} else if (protocolName.equals("nmsi_gmv_gc")) {
			_instance = new NMSI_GMV_GC(m,dataStore);
		} else if (protocolName.equals("nmsi_gmv2_gc")) {
			_instance = new NMSI_GMV2_GC(m, dataStore);
		} else if (protocolName.equals("nmsi_pdv_gc")) {
			_instance = new NMSI_PDV_GC(m,dataStore);			
		} else if (protocolName.equals("walter_vv")) {
			_instance = new Walter_VV(m, dataStore);
		}
		
		System.out.println("Protocol " + protocolName + " is initalized with persistence directory " + protocolName);
		return _instance;
	}

	public static Consistency getProtocolInstance() {
		return _instance;
	}

	public static String getProtocolName() {
		return protocolName;
	}
	
}
