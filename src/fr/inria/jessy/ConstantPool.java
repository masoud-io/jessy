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

	public final static long JESSY_MID = Messageable.FRACTAL_MID; // for
																	// marshalling/unmarshalling
																	// facilities

	public static final String JESSY_EVERYBODY_GROUP = "JEVERYBODY";
	public static final int JESSY_EVERYBODY_PORT = 4479;

	public static final String JESSY_SERVER_GROUP = "JSERV";
	public static final int JESSY_SERVER_PORT = 4480;

	public static final String JESSY_ALL_SERVERS_GROUP = "JALLSERVERS";
	public static final int JESSY_ALL_SERVERS_PORT = 4481;

	public static final int JESSY_NETTY_REMOTE_READER_PORT=4250;
	
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
	public static final long JESSY_REMOTE_READER_TIMEOUT = 50000;
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
	 * The timeout (in millisecond) and the number of times to retry a read upon
	 * returning a null value from the server.
	 */
	public static final long JESSY_READ_RETRY_TIMEOUT = 1;
	public static final short JESSY_READ_RETRY_TIMES = 3;

	/**
	 * Number of read operations in Read-only transaction in YCSB 
	 */
	public static final short READ_ONLY_TRANSACTION_READ_OPERATION_COUNT=4;
	
	/**
	 * Number of read/update operations in update transaction in YCSB 
	 */
	public static final short UPDATE_TRANSACTION_READ_OPERATION_COUNT=3;
	public static final short UPDATE_TRANSACTION_WRITE_OPERATION_COUNT=1;
	
	/**
	 * These two variables are used in {@link Jessy} to prevent checking for objects that have been
	 *  read/written previously by the same transaction. Of course in real system, they should be true.
	 *  But, for the sake of performance we set them to false.  
	 */
	public static final boolean CHECK_IF_HAS_BEEN_READ=false;
	public static final boolean CHECK_IF_HAS_BEEN_WRITTEN=false;
	
	/**
	 * Config.property file constants
	 */
	public static final String CONFIG_PROPERTY = "config.property";
	public static final String DATA_STORE_TYPE = "datastore_type";
	public static final String VECTOR_TYPE = "vector_type";
	public static final String CONSISTENCY_TYPE = "consistency_type";
	public static final String PARTITIONER_TYPE = "partitioner_type";
	public static final String RETRY_COMMIT = "retry_commit";
	public static final String FRACTAL_FILE = "fractal_file";
	public static final String CHECK_COMMUTAVITY = "check_commutativity";
	public static final String REPLICATION_FACTOR = "replication_factor";
	public static final String WAREHOUSES_NUMBER = "warehouses_number";
	public static final String OPERATION_WIDE_MEASUREMENTS = "operation_wide_measurements";
	public static final String TRANSACTION_WIDE_MEASUREMENTS = "transaction_wide_measurements";
	
	/**
	 * Measurements
	 */
	

	/**
	 * measurement phases
	 */
	public static enum TransactionPhase{
		/**
		 * Transaction Wide, Termination Phase
		 */
		TER,
		/**
		 * Transaction Wide, Execution Phase
		 */
		EXE,
		/**
		 * Transaction Wide, Execution AND Termination Phase
		 */
		OVERALL,
		/**
		 * Operation Wide Parameter 
		 */
		OW
	};
	
	/**
	 * measurement operations
	 */
	public static enum MeasuredOperations{
		
		/**
		 * Operation Wide Parameters to be measure.
		 */
		READ,
		WRITE,
		VECTOR_SIZE,
		
		
		/**
		 * Termination: committed
		 */
		COMMITTED,
		/**
		 * Termination: aborted by certification
		 */
		ABORTED_BY_CERTIFICATION,
		/**
		 * Termination: aborted by voting
		 */
		ABORTED_BY_VOTING,
		/**
		 * Termination: aborted by client
		 */
		ABORTED_BY_CLIENT,
		/**
		 * Termination: aborted by timeout
		 */
		ABORTED_BY_TIMEOUT,
		/**
		 * Termination: either aborted(for any reason) and committed
		 */
		TERMINATED,
		/**
		 * Termination: aborted for any reason
		 */
		ABORTED,	
		/**
		 * YCSB SPECIFIC, TODO: delete me once ycsb wrapper is not more used
		 */
		/**
		 * scan: ycsb specific
		 */
		MIRROR_YCSB_SCAN,
		/**
		 * Update: ycsb specific
		 */
		MIRROR_YCSB_UPDATE,
		/**
		 * Insert: ycsb specific
		 */
		MIRROR_YCSB_INSERT,
		/**
		 * Read: ycsb specific
		 */
		MIRROR_YCSB_READ,
		/**
		 * Delete: ycsb specific
		 */
		MIRROR_YCSB_DELETE,
		/**
		 * Read-Modify-write: ycsb specific
		 */
		YCSB_READ_MODIFY_WRITE,
	};

	/**
	 * define all transactions of TPC-C and ycsb. 
	 * @author pcincilla
	 *
	 */
	public static enum WorkloadTransactions {
		
		/**
		 * TPC-C
		 */
		/**
		 * New Order
		 */
		NO,
		/**
		 * Payment
		 */
		P,
		/**
		 * Order Status
		 */
		OS,
		/**
		 * Delivery
		 */
		D,
		/**
		 * Stock Level
		 */
		SL,
		/**
		 * YCSB
		 */
		/**
		 * Readonly
		 */
		READONLY,
		/**
		 * Update
		 */
		UPDATE
	};
}
