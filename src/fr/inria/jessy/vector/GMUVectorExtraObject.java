package fr.inria.jessy.vector;

import java.util.HashSet;
import java.util.Set;

import net.sourceforge.fractal.membership.Group;
import fr.inria.jessy.communication.JessyGroupManager;

public class GMUVectorExtraObject {
	
	public static final String READ_CONSTANT="READ";
	public static final String XACT_CONSTANT="XACT";
	
	/*
	 * Specifies the visited processes/groups 
	 */
	public Set<String> hasReads;
	
	public GMUVector<String> xact;
	
	private GMUVectorExtraObject(){
		hasReads=new HashSet<String>();
		xact=new GMUVector<String>();
	}
	
	public static GMUVectorExtraObject getGMUVectorExtraObject(Vector<String> vector){
		
		GMUVectorExtraObject result=new GMUVectorExtraObject();
		
		try {
			Vector<String> tmp=vector.clone();
			for (String str:tmp.getMap().keySet()){
				if (str.contains(READ_CONSTANT)){
					vector.getMap().remove(str);
					result.hasReads.add(str.replaceFirst(READ_CONSTANT, ""));
				}
				
				if (str.contains(XACT_CONSTANT)){
					vector.getMap().remove(str);
					result.xact.getMap().put(str.replaceFirst(XACT_CONSTANT, ""), tmp.getValue(str));
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		return result;
		
	}

	public static GMUVector<String> getXactVector(GMUVector<String> vector){
		GMUVector<String> result=new GMUVector<String>();
		
		try {
			for (String str:vector.getMap().keySet()){
				result.getMap().put(XACT_CONSTANT + str, vector.getValue(str));
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		return result;
	}
	
	public static GMUVector<String> getHasRead(JessyGroupManager j,  String key){
		GMUVector<String> result=new GMUVector<String>();

		try {
			Group g=j.getPartitioner().resolve(key);
			for (Integer swid:g.allNodes()){
				result.getMap().put(READ_CONSTANT+ swid.toString(), 0);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		return result;
	}
}
