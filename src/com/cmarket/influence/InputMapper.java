package com.cmarket.influence;

import java.io.IOException;
import java.util.NavigableMap;
import java.util.Map.Entry;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.DoubleWritable;


public class InputMapper extends TableMapper<ImmutableBytesWritable, DoubleWritable> {
	private int iteration=0;
	private double eigen_vector = 100;
	private double total_out_amount = 0;

	@Override
	public void map(ImmutableBytesWritable row, Result value, Context context)
			throws IOException, InterruptedException {
		NavigableMap<byte[],byte[]> familyCol=value.getFamilyMap(Bytes.toBytes("out"));
		if (iteration == 0) {
			iteration=1;
			eigen_vector = 100;
    		for (Entry<byte[],byte[]> key:familyCol.entrySet()){
    			total_out_amount +=Bytes.toInt(key.getValue());    			
			} 
		}else {
			eigen_vector =Bytes.toDouble(value.getValue(
			Bytes.toBytes("property"), Bytes.toBytes("eigen-vector")));
		}
		for (Entry<byte[],byte[]> kv : familyCol.entrySet()) {
			context.write(new ImmutableBytesWritable(kv.getKey()),new DoubleWritable(
						eigen_vector
								* Bytes.toInt(kv.getValue())
								/ total_out_amount));
		}
	}
	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
	}
}
