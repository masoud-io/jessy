package fr.inria.jessy.benchmark.tpcc.workflow;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Properties;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import com.yahoo.ycsb.measurements.Measurements;
import com.yahoo.ycsb.measurements.exporter.MeasurementsExporter;
import com.yahoo.ycsb.measurements.exporter.TextMeasurementsExporter;

import fr.inria.jessy.ConstantPool;
import fr.inria.jessy.ConstantPool.WorkloadTransactions;
import fr.inria.jessy.Jessy;
import fr.inria.jessy.JessyFactory;
import fr.inria.jessy.ConstantPool.MeasuredOperations;
import fr.inria.jessy.ConstantPool.TransactionPhase;
import fr.inria.jessy.benchmark.tpcc.Delivery;
import fr.inria.jessy.benchmark.tpcc.NewOrder;
import fr.inria.jessy.benchmark.tpcc.OrderStatus;
import fr.inria.jessy.benchmark.tpcc.Payment;
import fr.inria.jessy.benchmark.tpcc.StockLevel;
import fr.inria.jessy.benchmark.tpcc.entities.Customer;
import fr.inria.jessy.benchmark.tpcc.entities.District;
import fr.inria.jessy.benchmark.tpcc.entities.History;
import fr.inria.jessy.benchmark.tpcc.entities.Item;
import fr.inria.jessy.benchmark.tpcc.entities.New_order;
import fr.inria.jessy.benchmark.tpcc.entities.Order;
import fr.inria.jessy.benchmark.tpcc.entities.Order_line;
import fr.inria.jessy.benchmark.tpcc.entities.Stock;
import fr.inria.jessy.benchmark.tpcc.entities.Warehouse;
import fr.inria.jessy.transaction.ExecutionHistory;
import fr.inria.jessy.utils.Configuration;

public class TpccClient {

	Jessy jessy;
	Measurements _measurements;

	static int _numberTransaction; /* number of Transactions to execute */
	static int _warehouseNumber;
	int _districtNumber;

	int quotient = 0; /* for the number of NewOrder and Payment Transactions */
	int rest = 0;
	int i, j;

	private static Logger logger = Logger.getLogger(TpccClient.class);

	static {
		final String warehouses = Configuration
				.readConfig(ConstantPool.WAREHOUSES_NUMBER);
		logger.warn("Warehouse number is " + warehouses);

		_warehouseNumber= Integer.parseInt(warehouses);
	}



	public TpccClient(int numberTransaction,/* int warehouseNumber,*/ int districtNumber) {
		PropertyConfigurator.configure("log4j.properties");

		_numberTransaction = numberTransaction;
		_districtNumber = districtNumber;

		_measurements=Measurements.getMeasurements();
		try {
			//			jessy = JessyFactory.getDistributedJessy();
			jessy = JessyFactory.getLocalJessy();

			jessy.addEntity(Warehouse.class);
			jessy.addEntity(District.class);
			jessy.addEntity(Customer.class);
			jessy.addEntity(Item.class);
			jessy.addEntity(Stock.class);
			jessy.addEntity(History.class);
			jessy.addEntity(Order.class);
			jessy.addEntity(New_order.class);
			jessy.addEntity(Order_line.class);

			jessy.addSecondaryIndex(Customer.class, String.class, "C_W_ID");
			jessy.addSecondaryIndex(Customer.class, String.class, "C_D_ID");
			jessy.addSecondaryIndex(Customer.class, String.class, "C_LAST");

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public void execute() {

		/*
		 * System.out.print("Input the number of transactions : "); Scanner
		 * reader = new Scanner(System.in); String s = reader.nextLine(); int
		 * number = (int) Integer.valueOf(s);
		 */

		quotient = 10 * (_numberTransaction / 23);
		rest = _numberTransaction % 23;

		try {
			if (_numberTransaction < 23) {

				/* for the rest */
				for (i = 1; i <= rest / 2; i++) {

					neworder();
					logger.debug("1 NewOrder transaction committed in round: " + i );

					payment();
					logger.debug("2 Payment transaction committed in round: " + i );
				}

				if (rest % 2 == 1) {

					neworder();
					logger.debug("3 NewOrder transaction committed in round: " + i );
				}

			} else {

				for (i = 1; i <= quotient; i++) {

					neworder();
					logger.debug("4 NewOrder transaction committed in round: " + i );

					payment();
					logger.debug("5 Payment transaction committed in round: " + i );

					/* for decks */
					if (i != 0 && i % 10 == 0) {

						orderstatus();
						logger.debug("6 OrderStatus transaction committed in round: " + i );

						delivery();
						logger.debug("7 Delivery transaction committed in round: " + i );

						stocklevel();
						logger.debug("8 StockLevel transaction committed in round: " + i );
					}

				}

				/* for the rest */
				for (j = 0; j < rest / 2; j++) {

					neworder();
					logger.debug("9 NewOrder transaction committed in round: " + i );

					payment();
					logger.debug("10 Payment transaction committed in round: " + i );
				}

				if (rest % 2 == 1) {

					neworder();
					logger.debug("11 NewOrder transaction committed in round: " + i );
				}
			}

		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	public void neworder() throws Exception {

		logger.debug("executing NewOrder...");
		NewOrder neworder = new NewOrder(jessy, _warehouseNumber);

		if(!Measurements._transactionWideMeasurement){
			neworder.execute();
		}
		else{
			long st=System.currentTimeMillis();
			ExecutionHistory eh = neworder.execute();
			long en=System.currentTimeMillis();

			fillStatistics(eh, st, en, TransactionPhase.OVERALL, WorkloadTransactions.NO);
		}
	}

	public void payment() throws Exception {

		logger.debug("executing payment...");
		Payment payment = new Payment(jessy, _warehouseNumber);

		if(!Measurements._transactionWideMeasurement){
			payment.execute();
		}
		else{
			long st=System.currentTimeMillis();
			ExecutionHistory eh = payment.execute();
			long en=System.currentTimeMillis();

			fillStatistics(eh, st, en, TransactionPhase.OVERALL, WorkloadTransactions.P);
		}
	}

	public void orderstatus() throws Exception {

		logger.debug("executing orderstatus...");
		OrderStatus orderstatus = new OrderStatus(jessy, _warehouseNumber);

		if(!Measurements._transactionWideMeasurement){
			orderstatus.execute();
		}
		else{
			long st=System.currentTimeMillis();
			ExecutionHistory eh = orderstatus.execute();
			long en=System.currentTimeMillis();

			fillStatistics(eh, st, en, TransactionPhase.OVERALL, WorkloadTransactions.OS);
		}
	}

	public void delivery() throws Exception {

		logger.debug("executing delivery...");
		Delivery delivery = new Delivery(jessy, _warehouseNumber);

		if(!Measurements._transactionWideMeasurement){
			delivery.execute();
		}
		else{
			long st=System.currentTimeMillis();
			ExecutionHistory eh = delivery.execute();
			long en=System.currentTimeMillis();

			fillStatistics(eh, st, en, TransactionPhase.OVERALL, WorkloadTransactions.D);
		}

	}

	public void stocklevel() throws Exception {

		logger.debug("executing stocklevel...");
		StockLevel stocklevel = new StockLevel(jessy, _warehouseNumber, this._districtNumber);

		if(!Measurements._transactionWideMeasurement){
			stocklevel.execute();
		}
		else{
			long st=System.currentTimeMillis();
			ExecutionHistory eh = stocklevel.execute();
			long en=System.currentTimeMillis();

			fillStatistics(eh, st, en, TransactionPhase.OVERALL, WorkloadTransactions.SL);
		}
	}

	public static void main(String[] args) throws Exception {

		Properties props = new Properties();
		//			TODO ADD consistency, move to wrapper
		Measurements.setProperties(props);

		TpccClient client = new TpccClient(100, /*10,*/ 10);
		long st=System.currentTimeMillis();
		client.execute();
		long en=System.currentTimeMillis();

		exportMeasurements(props, _numberTransaction, en - st);

		//		TODO cleanly close fractal
		System.exit(0);
	}

	/**
	 * Exports the measurements to either sysout or a file using the exporter
	 * loaded from conf.
	 * @throws IOException Either failed to write to output stream or failed to close it.
	 */
	private static void exportMeasurements(Properties props, int opcount, long runtime)
			throws IOException
			{
		MeasurementsExporter exporter = null;
		try
		{
			// if no destination file is provided the results will be written to stdout
			OutputStream out;
			String exportFile = props.getProperty("exportfile");

			if (exportFile == null)
			{
				out = System.err;
			} else
			{
				out = new FileOutputStream(exportFile);
			}

			// if no exporter is provided the default text one will be used
			String exporterStr = props.getProperty("exporter", "com.yahoo.ycsb.measurements.exporter.TextMeasurementsExporter");
			try
			{
				exporter = (MeasurementsExporter) Class.forName(exporterStr).getConstructor(OutputStream.class).newInstance(out);
			} catch (Exception e)
			{
				System.err.println("Could not find exporter " + exporterStr
						+ ", will use default text reporter.");
				e.printStackTrace();
				exporter = new TextMeasurementsExporter(out);
			}
			//DataOutputStream dos = new DataOutputStream(new FileOutputStream("results"));
			exporter.write("OVERALL", "RunTime(ms)", runtime);

			double throughput = 1000.0 * ((double) opcount) / ((double) runtime);
			exporter.write("OVERALL", "Throughput(ops/sec)", throughput);

			Measurements.getMeasurements().exportMeasurements(exporter);
		} finally
		{
			if (exporter != null)
			{
				exporter.close();	
			}
		}
			}

	/**
	 * Used to take separate measurements on all TPC-C transactions types
	 * 
	 * @param eh Transaction ExecutionHistory
	 * @param st Transaction starting time
	 * @param en Transaction ending time
	 * @param tt Transaction type
	 */
	private void fillStatistics(ExecutionHistory eh, long st, long en, TransactionPhase phase, WorkloadTransactions tt ) {

		int returnCode=0;
		boolean committed=false;
		if(eh==null){
			returnCode=-1;
		}
		else{
			switch (eh.getTransactionState()) {
			case  COMMITTED:
				_measurements.measure(phase, MeasuredOperations.COMMITTED, tt, (int) (en - st));
				committed=true;
				break;

			case  ABORTED_BY_CERTIFICATION:
				_measurements.measure(phase, MeasuredOperations.ABORTED_BY_CERTIFICATION,tt, (int) (en - st));
				returnCode=-1;
				break;

			case  ABORTED_BY_VOTING:
				_measurements.measure(phase, MeasuredOperations.ABORTED_BY_VOTING,tt, (int) (en - st));
				returnCode=-1;
				break;

			case  ABORTED_BY_CLIENT:
				_measurements.measure(phase, MeasuredOperations.ABORTED_BY_CLIENT,tt, (int) (en - st));
				returnCode=-1;
				break;

			case  ABORTED_BY_TIMEOUT:
				_measurements.measure(phase, MeasuredOperations.ABORTED_BY_TIMEOUT,tt, (int) (en - st));
				returnCode=-1;
				break;

			default:
				break;
			}

			_measurements.measure(TransactionPhase.OVERALL, MeasuredOperations.TERMINATED, (int) (en - st));
			if(!committed){
				_measurements.measure(TransactionPhase.OVERALL, MeasuredOperations.ABORTED, (int) (en - st));
			}
		}
		_measurements.reportReturnCode(phase, MeasuredOperations.TERMINATED, tt, returnCode);

	}
}
