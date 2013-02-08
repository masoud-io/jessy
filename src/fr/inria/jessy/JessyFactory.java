package fr.inria.jessy;

public class JessyFactory {

	public static Jessy getLocalJessy() throws Exception{
		return LocalJessy.getInstance();
	}
	public static Jessy getDistributedJessy() throws Exception{
		return DistributedJessy.getInstance();
	}

}
