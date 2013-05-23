package fr.inria.jessy.consistency;

import org.apache.log4j.Logger;

import fr.inria.jessy.ConstantPool;
import fr.inria.jessy.communication.JessyGroupManager;
import fr.inria.jessy.persistence.FilePersistence;
import fr.inria.jessy.protocol.GMU;
import fr.inria.jessy.protocol.NMSI_DV_GC;
import fr.inria.jessy.protocol.NMSI_GMUVector_GC;
import fr.inria.jessy.protocol.PSI_VV_GC;
import fr.inria.jessy.protocol.PStore;
import fr.inria.jessy.protocol.SDUR;
import fr.inria.jessy.protocol.Serrano;
import fr.inria.jessy.protocol.US_DV_GC;
import fr.inria.jessy.protocol.US_GMUVector_GC;
import fr.inria.jessy.store.DataStore;
import fr.inria.jessy.utils.Configuration;

public class ProtocolFactory {

	private static Logger logger = Logger.getLogger(ProtocolFactory.class);

	private static Consistency _instance;

	private static String protocolName;
	
	/**
	 * Specifies the directory of files for loading the data store.
	 * See {@link FilePersistence} for details.
	 */
	private static String persistenceName;
	

	static {
		protocolName = Configuration
				.readConfig(ConstantPool.CONSISTENCY_TYPE);
		
		if (protocolName.equals("serrano")) {
			persistenceName="si2";
		} else if (protocolName.equals("pstore")) {
			persistenceName="ser";
		} else if (protocolName.equals("sdur")) {
			persistenceName="psi";  //SDUR uses Version Vector that is already filled for PSI.
		} else if (protocolName.equals("rc")) {
			persistenceName="rc";
		} else if (protocolName.equals("psi_vv_gc")) {
			persistenceName="psi";
		} else if (protocolName.equals("us_dv_gc")) {
			persistenceName="us";
		} else if (protocolName.equals("us_gmv_gc")) {
			persistenceName="us3";
		} else if (protocolName.equals("gmu")) {
			persistenceName="us2";
		} else if (protocolName.equals("nmsi_gmv_gc")) {
			persistenceName="nmsi3";
		}else if (protocolName.equals("nmsi_dv_gc")) {
			persistenceName="nmsi";
		}
	}

	public static Consistency initProtocol(JessyGroupManager m, DataStore dataStore) {
		if (_instance != null)
			return _instance;

		if (protocolName.equals("serrano")) {
			_instance = new Serrano(m, dataStore);
		} else if (protocolName.equals("pstore")) {
			_instance = new PStore(m, dataStore);
		} else if (protocolName.equals("sdur")) {
			_instance = new SDUR(m, dataStore);
		} else if (protocolName.equals("rc")) {
			_instance = new RC(m, dataStore);
		} else if (protocolName.equals("psi_vv_gc")) {
			_instance = new PSI_VV_GC(m, dataStore);
		} else if (protocolName.equals("us_dv_gc")) {
			_instance = new US_DV_GC(m, dataStore);
		} else if (protocolName.equals("us_gmv_gc")) {
			_instance = new US_GMUVector_GC(m, dataStore);
		} else if (protocolName.equals("gmu")) {
			_instance = new GMU(m, dataStore);
		} else if (protocolName.equals("nmsi_gmv_gc")) {
			_instance = new NMSI_GMUVector_GC(m,dataStore);
		}else if (protocolName.equals("nmsi_dv_gc")) {
			_instance = new NMSI_DV_GC(m, dataStore);
		}
		
		System.out.println("Protocol " + protocolName + " is initalized with persistence directory " + persistenceName);
		return _instance;
	}

	public static Consistency getProtocolInstance() {
		return _instance;
	}

	public static String getProtocolName() {
		return protocolName;
	}
	
	public static String getPersistenceName() {
		return persistenceName;
	}
	
}
