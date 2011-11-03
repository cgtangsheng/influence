package com.cmarket.influence;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;

public class OutputReducer extends TableReducer<Text, DoubleWritable, ImmutableBytesWritable> {
	@Override
	public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
			throws IOException, InterruptedException {
		double sum=0;
		for (DoubleWritable val : values) {
			sum += val.get();
		}
		Put put = new Put(Bytes.toBytes(key.toString()));
		put.add(Bytes.toBytes("property"), Bytes.toBytes("eigen-vector"), Bytes.toBytes(sum));
		context.write(null, put);
	}
}
