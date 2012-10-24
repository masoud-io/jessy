/**                                                                                                                                                                                
 * Copyright (c) 2010 Yahoo! Inc. All rights reserved.                                                                                                                             
 *                                                                                                                                                                                 
 * Licensed under the Apache License, Version 2.0 (the "License"); you                                                                                                             
 * may not use this file except in compliance with the License. You                                                                                                                
 * may obtain a copy of the License at                                                                                                                                             
 *                                                                                                                                                                                 
 * http://www.apache.org/licenses/LICENSE-2.0                                                                                                                                      
 *                                                                                                                                                                                 
 * Unless required by applicable law or agreed to in writing, software                                                                                                             
 * distributed under the License is distributed on an "AS IS" BASIS,                                                                                                               
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or                                                                                                                 
 * implied. See the License for the specific language governing                                                                                                                    
 * permissions and limitations under the License. See accompanying                                                                                                                 
 * LICENSE file.                                                                                                                                                                   
 */

package com.yahoo.ycsb.measurements;

import java.io.IOException;
import java.util.HashMap;
import java.util.Properties;

import com.yahoo.ycsb.measurements.exporter.MeasurementsExporter;

import fr.inria.jessy.ConstantPool;
import fr.inria.jessy.ConstantPool.MeasuredOperations;
import fr.inria.jessy.ConstantPool.TransactionPhase;
import fr.inria.jessy.ConstantPool.WorkloadTransactions;
import fr.inria.jessy.transaction.ExecutionHistory;

/**
 * Collects latency measurements, and reports them when requested.
 * 
 * @author cooperb
 *
 */
public class Measurements
{
	private static final String MEASUREMENT_TYPE = "measurementtype";

	private static final String MEASUREMENT_TYPE_DEFAULT = "histogram";

	static Measurements singleton=null;
	
	static Properties measurementproperties=null;
	
	public static final boolean _operationWideMeasurement = (fr.inria.jessy.utils.Configuration
			.readConfig(ConstantPool.OPERATION_WIDE_MEASUREMENTS).equals("false")) ? false
			: true;
	
	public static final boolean _transactionWideMeasurement = (fr.inria.jessy.utils.Configuration
			.readConfig(ConstantPool.TRANSACTION_WIDE_MEASUREMENTS).equals("false")) ? false
			: true;
	
	public static void setProperties(Properties props)
	{
		measurementproperties=props;
	}

      /**
       * Return the singleton Measurements object.
       */
	public synchronized static Measurements getMeasurements()
	{
		if (singleton==null)
		{
			singleton=new Measurements(measurementproperties);
		}
		return singleton;
	}

	HashMap<String,OneMeasurement> data;
	boolean histogram=true;

	private Properties _props;
	
      /**
       * Create a new object with the specified properties.
       */
	private Measurements(Properties props)
	{
		data=new HashMap<String,OneMeasurement>();
		
		_props=props;
		
		if (_props.getProperty(MEASUREMENT_TYPE, MEASUREMENT_TYPE_DEFAULT).compareTo("histogram")==0)
		{
			histogram=true;
		}
		else
		{
			histogram=false;
		}
	}
	
	private OneMeasurement constructOneMeasurement(String name)
	{
		if (histogram)
		{
			return new OneMeasurementHistogram(name,_props);
		}
		else
		{
			return new OneMeasurementTimeSeries(name,_props);
		}
	}
	
	/**
	 * Call the measure function with an operation part of the static set of operations defined in the CostantPool
	 * @param operation
	 * @param latency 
	 */
	public synchronized void measure(TransactionPhase phase, MeasuredOperations operation, int latency){
		measure(phase.toString()+"_"+operation.toString(), latency);
	}
	
	/**
	 * Call the measure function with an operation part of the static set of operations defined in the CostantPool and a 
	 * transaction type
	 * @param operation
	 * @param latency
	 */
	public synchronized void measure(TransactionPhase phase, MeasuredOperations operation, WorkloadTransactions t, int latency){
		measure(phase.toString()+"_"+operation.toString()+"_"+t, latency);
	}
	

      /**
       * Report a single value of a single metric. E.g. for read latency, operation="READ" and latency is the measured value.
       */
	private synchronized void measure(String operation, int latency)
	{
		if (!data.containsKey(operation))
		{
			synchronized(this)
			{
				if (!data.containsKey(operation))
				{
					data.put(operation,constructOneMeasurement(operation));
				}
			}
		}
		try
		{
			data.get(operation).measure(latency);
		}
		catch (java.lang.ArrayIndexOutOfBoundsException e)
		{
			System.out.println("ERROR: java.lang.ArrayIndexOutOfBoundsException - ignoring and continuing");
			e.printStackTrace();
			e.printStackTrace(System.out);
		}
	}
	
	/**
	 * Call the reportReturnCode function with an operation part of the static set of operations defined in the CostantPool and a 
	 * transaction type
	 * @param operation
	 * @param code
	 */
	public void reportReturnCode(TransactionPhase phase, MeasuredOperations operation, int code){
		reportReturnCode(phase.toString()+"_"+operation.toString(), code);
	}
	
	/**
	 * Call the reportReturnCode function with an operation part of the static set of operations defined in the CostantPool
	 * 
	 * @param operation
	 * @param t
	 * @param code
	 */
	public void reportReturnCode(TransactionPhase phase,MeasuredOperations operation, WorkloadTransactions t, int code){
		reportReturnCode(phase.toString()+"_"+operation.toString()+"_"+t, code);
	}

      /**
       * Report a return code for a single DB operaiton.
       */
	private void reportReturnCode(String operation, int code)
	{
		if (!data.containsKey(operation))
		{
			synchronized(this)
			{
				if (!data.containsKey(operation))
				{
					data.put(operation,constructOneMeasurement(operation));
				}
			}
		}
		data.get(operation).reportReturnCode(code);
	}
	
  /**
   * Export the current measurements to a suitable format.
   * 
   * @param exporter Exporter representing the type of format to write to.
   * @throws IOException Thrown if the export failed.
   */
  public void exportMeasurements(MeasurementsExporter exporter) throws IOException
  {
    for (OneMeasurement measurement : data.values())
    {
      measurement.exportMeasurements(exporter);
    }
  }
	
      /**
       * Return a one line summary of the measurements.
       */
	public String getSummary()
	{
		String ret="";
		for (OneMeasurement m : data.values())
		{
			ret+=m.getSummary()+" ";
		}
		
		return ret;
	}
	
	/**
	 * Used to take separate measurements on all transactions types accordingly with the result of a transaction
	 * 
	 * @param eh Transaction ExecutionHistory
	 * @param st Transaction starting time
	 * @param en Transaction ending time
	 * @param tt Transaction type
	 */
	public void fillStatistics(ExecutionHistory eh, long st, long en, TransactionPhase phase, WorkloadTransactions tt ) {

		int returnCode=0;
		boolean committed=false;
		if(eh==null){
			returnCode=-1;
		}
		else{
			switch (eh.getTransactionState()) {
			case  COMMITTED:
				measure(phase, MeasuredOperations.COMMITTED, tt, (int) (en - st));
				committed=true;
				break;

			case  ABORTED_BY_CERTIFICATION:
				measure(phase, MeasuredOperations.ABORTED_BY_CERTIFICATION,tt, (int) (en - st));
				returnCode=-1;
				break;

			case  ABORTED_BY_VOTING:
				measure(phase, MeasuredOperations.ABORTED_BY_VOTING,tt, (int) (en - st));
				returnCode=-1;
				break;

			case  ABORTED_BY_CLIENT:
				measure(phase, MeasuredOperations.ABORTED_BY_CLIENT,tt, (int) (en - st));
				returnCode=-1;
				break;

			case  ABORTED_BY_TIMEOUT:
				measure(phase, MeasuredOperations.ABORTED_BY_TIMEOUT,tt, (int) (en - st));
				returnCode=-1;
				break;

			default:
				break;
			}

			measure(TransactionPhase.OVERALL, MeasuredOperations.TERMINATED, (int) (en - st));
			if(!committed){
				measure(TransactionPhase.OVERALL, MeasuredOperations.ABORTED, (int) (en - st));
			}
		}
		reportReturnCode(phase, MeasuredOperations.TERMINATED, tt, returnCode);

	}
}
