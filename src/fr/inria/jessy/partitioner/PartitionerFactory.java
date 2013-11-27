package fr.inria.jessy.partitioner;

import fr.inria.jessy.communication.JessyGroupManager;
import fr.inria.jessy.store.Keyspace;
import fr.inria.jessy.utils.Configuration;

public class PartitionerFactory {

	private static String PartitionerType = Configuration
	.readConfig(fr.inria.jessy.ConstantPool.PARTITIONER_TYPE);

	public static Partitioner getPartitioner(JessyGroupManager m, Keyspace keyspace) {

		if (PartitionerType.equals("keyspace")) {
			return new KeySpacePartitioner(m, keyspace);
		} else if (PartitionerType.equals("modulo")) {
			return new ModuloPartitioner(m);
		} else if (PartitionerType.equals("replicatedModulo")) {
			return new ReplicatedModuloPartitioner(m);
		}else if (PartitionerType.equals("sequential")) {
			return new SequentialPartitioner(m);
		}else if (PartitionerType.equals("replicatedsequential")) {
			return new ReplicatedSequentialPartitioner(m);
		}

		return null;
	}

}
