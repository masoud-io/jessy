package fr.inria.jessy;

import java.util.concurrent.TimeUnit;

import net.sourceforge.fractal.Messageable;

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

	public final static long JESSY_MID = Messageable.FRACTAL_MID; // for marshalling/unmarshalling facilities

	public static final String JESSY_EVERYBODY_GROUP = "JEVERYBODY";
	public static final int JESSY_EVERYBODY_PORT = 4479;

	public static final String JESSY_SERVER_GROUP = "JSERV";
	public static final int JESSY_SERVER_PORT = 4480;

	public static final String JESSY_ALL_SERVERS_GROUP = "JALLSERVERS";
	public static final int JESSY_ALL_SERVERS_PORT = 4481;

	public static final int GROUP_SIZE = 1;

	/**
	 * Communication layer related constants
	 */
	public static final String JESSY_TERMINATION_STREAM = "JSTERM";
	public static final String JESSY_VOTE_STREAM = "JVOTE";
	public static final String JESSY_READER_STREAM = "JREADER";

	/**
	 * GPaxos Configs
	 */
	public static final int MAX_INTERGROUP_MESSAGE_DELAY = 3000;
	public static final int CONSENSUS_LATENCY = 2000;

	/**
	 * Specifies the timeout and its type for each remote read request. Since
	 * the read request might be lost, upon the timeout, a new read request
	 * should be sent out to a jessy instance replicating the entity.
	 */
	public static final long JESSY_REMOTE_READER_TIMEOUT = 5000;
	public static final TimeUnit JESSY_REMOTE_READER_TIMEOUT_TYPE = TimeUnit.MILLISECONDS;

	/**
	 * Specifies the timeout and its type for each transaction termination.
	 * Since a vote request might be lost, upon the timeout, a new transaction
	 * termination should be initialized.
	 */
	public static final long JESSY_TRANSACTION_TERMINATION_TIMEOUT = 10000;
	public static final TimeUnit JESSY_TRANSACTION_TERMINATION_TIMEOUT_TYPE = TimeUnit.MILLISECONDS;

	/**
	 * Specifies the threshold for ignoring the condition checks before applying
	 * the propagation. NOTE: if the propagation is reliable broadcast, this
	 * constant should be useless.
	 */
	public static final int JESSY_PSI_PROPAGATION_THRESHOLD = 20;

	/**
	 * Config.property file constants
	 */
	public static final String CONFIG_PROPERTY = "config.property";
	public static final String VECTOR_TYPE = "vector_type";
	public static final String CONSISTENCY_TYPE = "consistency_type";
	public static final String PARTITIONER_TYPE = "partitioner_type";
	public static final String RETRY_COMMIT = "retry_commit";
	public static final String FRACTAL_FILE = "fractal_file";
	public static final String CHECK_COMMUTAVITY = "check_commutativity";

}
