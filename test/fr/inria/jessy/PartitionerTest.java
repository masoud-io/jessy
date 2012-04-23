package fr.inria.jessy;

import org.junit.BeforeClass;
import org.junit.Test;

import fr.inria.jessy.store.Keyspace;

import net.sourceforge.fractal.membership.Membership;

import static fr.inria.jessy.store.Keyspace.Distribution.UNIFORM;

public class PartitionerTest {

	private static Partitioner partitioner;
	
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
		partitioner = new Partitioner(membership);
	}
	
	@Test
	public void assignement(){
		partitioner.assign(new Keyspace("test#for###the######fun",UNIFORM));
	}
	
	@Test
	public void resolution(){
		partitioner.assign(new Keyspace("abc##",UNIFORM));
		assert partitioner.resolve("abc90").name().equals(ConstantPool.JESSY_SERVER_GROUP+"3");
		partitioner.assign(new Keyspace("#",UNIFORM));
		assert partitioner.resolve("0").name().equals(ConstantPool.JESSY_SERVER_GROUP+"0");
	}
	
	
}
