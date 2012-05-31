package fr.inria.jessy;

import static fr.inria.jessy.store.Keyspace.Distribution.UNIFORM;
import net.sourceforge.fractal.membership.Membership;

import org.junit.BeforeClass;
import org.junit.Test;

import fr.inria.jessy.partitioner.KeySpacePartitioner;
import fr.inria.jessy.store.Keyspace;

public class PartitionerTest {

	private static KeySpacePartitioner partitioner;
	
	@BeforeClass
	public static void initGroups(){
		Membership membership = new Membership();
		membership.addNode(1,"1");
		membership.addNode(2,"2");
		membership.addNode(3,"3");
		membership.addNode(4,"4");
		membership.dispatchPeers(
				ConstantPool.JESSY_SERVER_GROUP,
				ConstantPool.JESSY_SERVER_PORT,
				ConstantPool.GROUP_SIZE);
		partitioner = new KeySpacePartitioner(membership,new Keyspace("test#for###the######fun",UNIFORM));
	}
	
	@Test
	public void resolution(){
		partitioner.assign(new Keyspace("abc##",UNIFORM));
//		assert partitioner.resolve("abc90").name().equals(ConstantPool.JESSY_SERVER_GROUP+"3");
		partitioner.assign(new Keyspace("#",UNIFORM));
//		assert partitioner.resolve("0").name().equals(ConstantPool.JESSY_SERVER_GROUP+"0");
	}
	
	
}
