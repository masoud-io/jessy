package fr.inria.jessy;

import java.io.FileInputStream;
import java.util.Properties;

/**
 * 
 * This class contains multiple constant that might be used in Jessy.
 * 
 * @author Pierre Sutra
 * 
 */

public class ConstantPool {

	public static enum EXECUTION_MODE {
		SERVER, PROXY
	}

	public final static long JESSY_MID = 0x1L;

	public static final String JESSY_EVERYBODY_GROUP = "JEVERYBODY";
	public static final int JESSY_EVERYBODY_PORT = 4479;

	public static final String JESSY_SERVER_GROUP = "JSERV";
	public static final int JESSY_SERVER_PORT = 4480;

	public static final String JESSY_ALL_REPLICA_GROUP = "JALLR";
	public static final int JESSY_ALL_REPLICA_PORT = 4481;

	public static final int GROUP_SIZE = 1;

	/**
	 * Atomic Broadcast Configs
	 */
	public static final int MAX_INTERGROUP_MESSAGE_DELAY = 3000;
	public static final int CONSENSUS_LATENCY = 2000;

	/**
	 * Config.property file constants
	 */
	public static final String CONFIG_PROPERTY = "config.property";
	public static final String VECTOR_TYPE = "vector_type";
	public static final String CONSISTENCY_TYPE = "consistency_type";
	public static final String PARTITIONER_TYPE = "partitioner_type";
	public static final String RETRY_COMMIT = "retry_commit";
	public static final String FRACTAL_FILE = "fractal_file";

}
