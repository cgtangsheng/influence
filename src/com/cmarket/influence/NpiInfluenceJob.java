package com.cmarket.influence;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;



public class NpiInfluenceJob {
	private static int iteration = 0;
    private static Configuration conf= HBaseConfiguration.create();
	public static void main(String[] args) throws Exception {
		iteration=Integer.parseInt(args[1]);
		if (args.length != 2) {
			System.err.println("Wrong number of arguments: " + args.length);
			System.err.println("Usage: " + "  <tablename>    <iteration>");
			System.exit(-1);

		}
		else {			
			for(int i=0;i<iteration;i++){
				Job job = configureJob(args[0],i);
				if(!job.waitForCompletion(true))
					break;
			}
		}
	}
	private static Job configureJob(String tableName,int i) throws IOException {
		// TODO Auto-generated method stub
		Job job=new Job(conf,tableName);
		conf.set("Iteration", Integer.toString(i));
		job.setJarByClass(NpiInfluenceJob.class);
		Scan scan=new Scan();
		TableMapReduceUtil.initTableMapperJob(
		  tableName,        // input HBase table name
		  scan,             // Scan instance to control CF and attribute selection
		  InputMapper.class,   // mapper
		  Text.class,             // mapper output key 
		  DoubleWritable.class,             // mapper output value
		  job, false);
		TableMapReduceUtil.initTableReducerJob(
				tableName,      // output table
				OutputReducer.class,             // reducer class
				job);
		return job;
		
	}
}
