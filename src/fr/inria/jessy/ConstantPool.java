package fr.inria.jessy;

import java.util.concurrent.TimeUnit;

import fr.inria.jessy.communication.message.TerminateTransactionRequestMessage;
import fr.inria.jessy.transaction.termination.DistributedTermination;

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
	
	public static enum UNICAST_MODE{
		FRACTAL, NETTY
	}
	
	public static enum MULTICAST_MODE{
		FRACTAL, NETTY
	}
	
	public static enum GENUINE_TERMINATION_MODE{
		GENUINE, LIGHT_GENUINE
	}

	public static final GENUINE_TERMINATION_MODE TERMINATION_COMMUNICATION_TYPE = GENUINE_TERMINATION_MODE.LIGHT_GENUINE;
	
	/**
	 * Specify the framework to be used for multicasting the vote
	 * Only Fractal is available
	 */
	
	public static final MULTICAST_MODE JESSY_VOTING_PHASE_MULTICAST_MODE=MULTICAST_MODE.FRACTAL;
	/**
	 * Specify the framework to be used for multicasting {@link TerminateTransactionRequestMessage}
	 * Only Fractal is available
	 */
	public static final MULTICAST_MODE JESSY_LIGHT_GENUINE_MULTICAST_MODE=MULTICAST_MODE.FRACTAL;

	public static final UNICAST_MODE JESSY_REMOTE_READ_UNICST_MODE=UNICAST_MODE.FRACTAL;
	
	public final static long JESSY_MID = Messageable.FRACTAL_MID; // for marshalling unmarshalling facilities
	
	public static final boolean logging=false;

	public static final String JESSY_EVERYBODY_GROUP = "JEVERYBODY";
	public static final int JESSY_EVERYBODY_PORT = 4479;

	public static final String JESSY_SERVER_GROUP = "JSERV";
	public static final int JESSY_SERVER_PORT = 4480;

	public static final String JESSY_ALL_SERVERS_GROUP = "JALLSERVERS";
	public static final int JESSY_ALL_SERVERS_PORT = 4481;

	public static final int JESSY_NETTY_REMOTE_READER_PORT=4250;
	public static final int JESSY_NETTY_VOTING_PHASE_PORT=4251;
	
	public static final int JESSY_NETTY_TERMINATIONMESSAGE_PORT=4252;
	
	/**
	 * Communication layer related constants
	 */
	public static final String JESSY_TERMINATION_STREAM = "JSTERM";
	public static final String JESSY_VOTE_STREAM = "JVOTE";
	public static final String JESSY_READER_STREAM = "JREADER";


	/**
	 * Specifies the timeout and its type for each remote read request. Since
	 * the read request might be lost, upon the timeout, a new read request
	 * should be sent out to a jessy instance replicating the entity.
	 */
	public static final long JESSY_REMOTE_READER_TIMEOUT = 10000;
	public static final TimeUnit JESSY_REMOTE_READER_TIMEOUT_TYPE = TimeUnit.MILLISECONDS;

	/**
	 * Specifies the timeout and its type for each transaction termination.
	 * Since a vote request might be lost, upon the timeout, a new transaction
	 * termination should be initialized.
	 */
	public static final long JESSY_TRANSACTION_TERMINATION_TIMEOUT = 50000;
	public static final TimeUnit JESSY_TRANSACTION_TERMINATION_TIMEOUT_TYPE = TimeUnit.MILLISECONDS;

	/**
	 * Specifies the size of <code>terminated</code> hashmap in {@link DistributedTermination}.
	 * This hashmap is only used for checking newly received votes whether to add them to the voting quorum or not.
	 * Of course, if the transaction is already terminated (some other members of a group already voted, 
	 * thus the transaction is terminated), the vote shouldn't be added. 
	 * However, choosing an infinite hashmap has its own memory problem. Thus, we should specify the size of the 
	 * <code>terminated</code> hashmap.   
	 */
	public static final long JESSY_TERMINATED_TRANSACTIONS_LOG_SIZE=1000;
		
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
	 * Specifies the timeout for a voting quorum. 
	 * If everything goes well, there is no need for this timeout, but because of a bug or some other failure, it can be the cast that a voting message does not deliver, thus, to be able to continue the execution, we define a voting timeout.
	 * <p>
	 * Note that this might violate the safety of the correctness criteria if some nodes receives the voting message, and some others does not receive it. 
	 */
	public static long JESSY_VOTING_QUORUM_TIMEOUT=30000;

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
	public static final String GROUP_SIZE = "group_size";
	public static final String DATA_STORE_TYPE = "datastore_type";
	public static final String CONSISTENCY_TYPE = "consistency_type";
	public static final String PARTITIONER_TYPE = "partitioner_type";
	public static final String RETRY_COMMIT = "retry_commit";
	public static final String FRACTAL_FILE = "fractal_file";
	public static final String REPLICATION_FACTOR = "replication_factor";
	public static final String WAREHOUSES_NUMBER = "warehouses_number";
	public static final String OPERATION_WIDE_MEASUREMENTS = "operation_wide_measurements";
	public static final String TRANSACTION_WIDE_MEASUREMENTS = "transaction_wide_measurements";

	
	/**
	 * SequentialPartitioner
	 * TODO generalize me
	 */
	public static final String NUMBER_OF_OBJECTS_PROPERTY_FILE=System.getProperty("user.dir")+ "/workload";
	public static final String NUMBER_OF_OBJECTS_PROPERTY_NAME="recordcount";
	
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
