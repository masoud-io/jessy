package fr.inria.jessy;

/**
 * 
 * This class contains multiple constant that might be used in Jessy.
 * 
 * @author Pierre Sutra
 *
 */

public class ConstantPool {

	public static enum EXECUTION_MODE {
		SERVER,
		PROXY
	}
	
	public final static long JESSY_MID = 0x1L;
	public static final String JESSY_ALL_GROUP = "JALL";
	public static final int JESSY_ALL_PORT = 4479;
	public static final String JESSY_SERVER_GROUP = "JSERV";
	public static final int JESSY_SERVER_PORT = 4480;
	static final int GROUP_SIZE = 1;

	public static final String CONFIG_PROPERTY="config.property";
	public static final String VECTOR_TYPE="vector_type";
	public static final String CONSISTENCY_TYPE="consistency_type";
	public static final String RETRY_COMMIT="retry_commit";
	public static final String FRACTAL_FILE="fractal_file";
	
	
}
