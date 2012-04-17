package fr.inria.jessy;

import org.junit.BeforeClass;
import org.junit.Test;

import net.sourceforge.fractal.membership.Membership;

import static fr.inria.jessy.Partitioner.Distribution.UNIFORM;

public class PartitionerTest {

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
				ConstantPool.REPLICATION_FACTOR);
	}
	
	@Test
	public void assignement(){
		Partitioner partitioner = Partitioner.getInstance();
		partitioner.assign("test#for###the######fun",UNIFORM);
	}
	
	@Test
	public void resolution(){
		Partitioner partitioner = Partitioner.getInstance();
		partitioner.assign("abc##",UNIFORM);
		assert partitioner.resolve("abc90").name().equals("3");
		partitioner.assign("#",UNIFORM);
		assert partitioner.resolve("0").name().equals("0");
	}
	
	
}
