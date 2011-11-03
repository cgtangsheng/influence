package com.cmarket.influence;

import java.io.IOException;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;

public class InputMapper extends TableMapper<Text, DoubleWritable> {
	private int iteration;
	private double eigen_vector = 0;
	private double total_out_amount = 0;

	@Override
	public void map(ImmutableBytesWritable row, Result value, Context context)
			throws IOException, InterruptedException {
		iteration = Integer.parseInt(context.getConfiguration()
				.get("Iteration"));
		if (iteration == 0) {
			eigen_vector = 100;
			for (KeyValue kv : value.raw()) {
				if ("out".equals(kv.getFamily().toString())) {
					total_out_amount += Integer.parseInt(kv.getValue()
							.toString());
				}
			}
		} else {
			eigen_vector = Double.parseDouble(new String(value.getValue(
					"property".getBytes(), "eigen-vector".getBytes())));
			for (KeyValue kv : value.raw()) {
				if ("out".equals(kv.getFamily().toString())) {
					context.write(new Text(kv.getQualifier().toString()),new DoubleWritable(
							eigen_vector
									* Double.parseDouble(kv.getValue().toString())
									/ total_out_amount));
				}
			}
		}
	}
	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
	}
}
