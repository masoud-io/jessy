package fr.inria.jessy;

public class JessyFactory {

	public static Jessy getLocalJessy() throws Exception{
		
		JessyWrapper jw = new JessyWrapper(LocalJessy.getInstance());
//		return jw;
		
//		uncomment this line to skyp new measurements
		return LocalJessy.getInstance();
	}
	public static Jessy getDistributedJessy() throws Exception{
		JessyWrapper jw = new JessyWrapper(DistributedJessy.getInstance());
//		return jw;
		
//		uncomment this line to skyp new measurements
		return DistributedJessy.getInstance();
	}

}
