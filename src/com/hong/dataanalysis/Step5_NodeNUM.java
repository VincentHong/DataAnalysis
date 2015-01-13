package com.hong.dataanalysis;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.hong.hdfs.HdfsDAO;



public class Step5_NodeNUM {

	public static String ListToString(List<String> stringList) {
		StringBuffer buffer = new StringBuffer();
		boolean flag = false;
		for (String string : stringList) {
			if (flag) {
				buffer.append("	");
			} else {
				flag = true;
			}
			buffer.append(string);
		}
		return buffer.toString();
	}

	public static class NUMMap extends
			Mapper<Object, Text, FloatWritable, Text> {

		Text mapValue = new Text();
		FloatWritable mapKey = new FloatWritable();

		@Override
		protected void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {

			List<String> tokens = new ArrayList<String>(Arrays.asList(value
					.toString().split("	")));
			List<String> eNode = tokens.subList(8, 11);
			mapKey.set(Float.valueOf(tokens.get(0)));
			mapValue.set(ListToString(eNode));
			context.write(mapKey, mapValue);

		}

	}

	public static class NUMReduce extends
			Reducer<FloatWritable, Text, Text, FloatWritable> {

		FloatWritable num = new FloatWritable(1);

		@Override
		protected void reduce(FloatWritable key, Iterable<Text> values,
				Context context) throws IOException, InterruptedException {
			while (values.iterator().hasNext()) {
				num.set(num.get() + 1);
				context.write(values.iterator().next(), num);
			}
		}
	}

	public static class Partition extends Partitioner<FloatWritable, Text> {

		@Override
		public int getPartition(FloatWritable key, Text values, int numPartition) {
			// TODO Auto-generated method stub
			int maxNumber = 5500000;

			int bound = maxNumber / numPartition + 1;
			float keyvalue = key.get();
			for (int i = 0; i < numPartition; i++) {
				if (keyvalue < bound * i && keyvalue >= bound * (i - 1)) {
					return i - 1;
				}
			}
			return 0;
		}

	}

	public static void run(String in, String out) throws Exception {
		String input = "hdfs://namenode:9000/user/flp/" + in;
		String output = "hdfs://namenode:9000/user/flp/" + out;

		Configuration conf = new Configuration();
		Job job =  Job.getInstance(conf, "Step5_NodeNUM");
		job.setJarByClass(Step5_NodeNUM.class);

		HdfsDAO hdfs = new HdfsDAO("hdfs://192.168.1.206:9000", conf);
		hdfs.rmr(output);

		job.setMapperClass(NUMMap.class);
		job.setReducerClass(NUMReduce.class);
		job.setPartitionerClass(Partition.class);
		job.setMapOutputKeyClass(FloatWritable.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(FloatWritable.class);

		FileInputFormat.setInputPaths(job, new Path(input));
		FileOutputFormat.setOutputPath(job, new Path(output));

		job.waitForCompletion(true);
	}

}
