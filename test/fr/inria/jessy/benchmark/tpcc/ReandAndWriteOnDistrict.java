package fr.inria.jessy.benchmark.tpcc;

import fr.inria.jessy.Jessy;
import fr.inria.jessy.benchmark.tpcc.entities.District;
import fr.inria.jessy.transaction.ExecutionHistory;
import fr.inria.jessy.transaction.Transaction;

public class ReandAndWriteOnDistrict extends Transaction {

	private String W_ID;
	private String D_ID;
	
	private District district;

	public ReandAndWriteOnDistrict(Jessy jessy, int warhouseId, int districtId) throws Exception {
		super(jessy);
		
		this.W_ID = "" + warhouseId;
		this.D_ID= "" + districtId;
		
	}
	
	public District readAndWrite(){
		execute();
		return district;
	}

	@Override
	public ExecutionHistory execute() {

		try {

			

			System.out.println("reading District D_W_" + W_ID + "_" + "D_" + D_ID);
			district = read(District.class, "D_W_" + W_ID + "_" + "D_" + D_ID);
			System.out.println("readed, district.getD_ID()= "+district.getD_ID());
			
			System.out.println("updating District");
			write(district);
			System.out.println("District updated");

			return commitTransaction();

		} catch (Exception e) {
			e.printStackTrace();
			
			return abortTransaction();
		}
	}			


}
