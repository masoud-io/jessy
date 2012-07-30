package fr.inria.jessy.benchmark.tpcc;

import junit.framework.TestCase;

import org.apache.log4j.PropertyConfigurator;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import fr.inria.jessy.DistributedJessy;
import fr.inria.jessy.Jessy;
import fr.inria.jessy.benchmark.tpcc.entities.District;

public class TpccTestDistrictEntity extends TestCase{

	Jessy jessy;

	/**
	 * @throws java.lang.Exception
	 */
	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
	}

	@Before
	public void setUp() {
		PropertyConfigurator.configure("log4j.properties");

		try {
			jessy = DistributedJessy.getInstance();

			jessy.addEntity(District.class);

		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Test
	public void test() {
		try{
			
			District d;
			
//			read and write the district entity with warehouse id=1 and district id=1
			System.out.println();
			System.out.println("first read and write");
			d=readAndWriteDistrict(1, 1);
			System.out.println();
			
			if(d.getD_ID()==null){
				System.err.println("Error: the District id is null");
				assertFalse(true);
			}

			Thread.sleep(1000);
			
//			read and write the district entity with warehouse id=1 and district id=1
			System.out.println();
			System.out.println("second read and write");
			d=readAndWriteDistrict(1, 1);
			System.out.println();

			if(d.getD_ID()==null){
				System.err.println("Error: the District id is null");
				assertFalse(true);
			}
			
		} catch (Exception e) {
			e.printStackTrace();
			assert false;
		}
	}

	private District readAndWriteDistrict(int warehouseId, int districtId) throws Exception {

		ReandAndWriteOnDistrict rw = new ReandAndWriteOnDistrict(jessy, warehouseId, districtId);
		return rw.readAndWrite();
	}

}
