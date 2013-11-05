package fr.inria.jessy.vector;

import fr.inria.jessy.communication.JessyGroupManager;
import fr.inria.jessy.protocol.ProtocolFactory;
import fr.inria.jessy.store.JessyEntity;
import fr.inria.jessy.store.ReadRequest;

public class VectorFactory {

	private static String protocolName = ProtocolFactory.getProtocolName();
	
	private static Vector<String> tmpVector=GetVector("");
	
	public static <K> Vector<K> GetVector(K selfKey) {

//		if (protocolName.contains("Walter")) {
//			return new VersionVector(manager.getMyGroup().name(), 0);
//		}
		if (protocolName.contains("_dv_")) {
			return new DependenceVector<K>(selfKey);
		}
		if (protocolName.contains("_pdv_")) {
			return new PartitionDependenceVector<K>(selfKey,0);
		}
		if (protocolName.contains("rc")) {
			return new NullVector<K>(selfKey);
		}
		if (protocolName.contains("_vv_")) {
			return new VersionVector<K>(selfKey, 0);
		}
		if (protocolName.contains("_sv_")) {
			return new ScalarVector<K>();
		}
		if (protocolName.contains("_gmv_") ) {
			return new GMUVector<K>();
		}
		if (protocolName.contains("_gmv2_") ) {
			return new GMUVector2<K>();
		}		
		if (protocolName.contains("_lsv_") ) {
			return new LightScalarVector<K>(selfKey);
		}
		return null;
	}
	
	public static void init(JessyGroupManager m) {
		tmpVector.init(m);
	}	
	
	public static void makePersistent() {
		tmpVector.makePersistent();
	}	
	
	
	public static boolean prepareRead(ReadRequest rr){
		return tmpVector.prepareRead(rr);
	}
	
	public static void postRead(ReadRequest rr, JessyEntity entity){
		tmpVector.postRead(rr, entity);
	}

	/**
	 * This method is not NECESSARY.
	 * It is just a dirty way to improve performance.
	 *  
	 * @return true if {@link Vector#updateExtraObjectInCompactVector(Vector, Object)} is implemented in the 
	 * corresponding vector. 
	 * 
	 */
	public static boolean needExtraObject() {

		while (protocolName==null || protocolName.equals("")){
			protocolName = ProtocolFactory.getProtocolName();
			System.out.println("Unable to read Protocol Name.");
		}
		
		if (protocolName.equals("nmsi_gmv_gc") || 
				protocolName.equals("us_gmv_gc") ||
				protocolName.equals("gmu") || 
				protocolName.equals("nmsi_gmv2_gc") ||
				protocolName.equals("us_gmv2_gc") || 
				protocolName.equals("nmsi_pdv_gc")) {
			return true;
		}

		return false;
	}

}
