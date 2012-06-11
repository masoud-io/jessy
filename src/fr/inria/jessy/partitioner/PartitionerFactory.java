package fr.inria.jessy.partitioner;

import fr.inria.jessy.store.Keyspace;
import fr.inria.jessy.utils.Configuration;

public class PartitionerFactory {

	private static String PartitionerType = Configuration
			.readConfig(fr.inria.jessy.ConstantPool.PARTITIONER_TYPE);

	public static Partitioner getPartitioner(Keyspace keyspace) {

		if (PartitionerType.equals("keyspace")) {
			return new KeySpacePartitioner( keyspace);
		} else if (PartitionerType.equals("modulo")) {
			return new ModuloPartitioner();
		}
		return null;
	}

}
