package fr.inria.jessy.partitioner;

import net.sourceforge.fractal.membership.Membership;
import fr.inria.jessy.store.Keyspace;
import fr.inria.jessy.utils.Configuration;

public class PartitionerFactory {

	private static String PartitionerType = Configuration
			.readConfig(fr.inria.jessy.ConstantPool.PARTITIONER_TYPE);

	public static Partitioner getPartitioner(Membership m, Keyspace keyspace) {

		if (PartitionerType.equals("keyspace")) {
			return new KeySpacePartitioner(m, keyspace);
		} else if (PartitionerType.equals("modulo")) {
			return new ModuloPartitioner(m);
		}
		return null;
	}

}
