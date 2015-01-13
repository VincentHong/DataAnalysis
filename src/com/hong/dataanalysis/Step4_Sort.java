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



public class Step4_Sort {

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

	public static class SortMap extends
			Mapper<Object, Text, FloatWritable, Text> {

		@Override
		protected void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			List<String> tokens = new ArrayList<String>(Arrays.asList(value
					.toString().split("	")));
			FloatWritable segment = new FloatWritable(Float.valueOf(tokens
					.get(0)));
			tokens.remove(0);
			context.write(segment, new Text(ListToString(tokens)));

		}

	}

	public static class SortReduce extends
			Reducer<FloatWritable, Text, FloatWritable, Text> {

		protected void reduce(FloatWritable key, Iterable<Text> values,
				Context context) throws IOException, InterruptedException {
			for (Text value : values) {
				context.write(key, value);
			}
		}

	}

	public static class Partition extends Partitioner<FloatWritable, Text> {

		@Override
		public int getPartition(FloatWritable key, Text values, int numPartition) {
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

		Job job = Job.getInstance(conf, "Step4_Sort");
		job.setJarByClass(Step4_Sort.class);

		HdfsDAO hdfs = new HdfsDAO("hdfs://192.168.1.206:9000", conf);
		hdfs.rmr(output);

		job.setMapperClass(SortMap.class);
		job.setReducerClass(SortReduce.class);
		job.setPartitionerClass(Partition.class);
		job.setMapOutputKeyClass(FloatWritable.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(FloatWritable.class);
		job.setOutputValueClass(Text.class);

		FileInputFormat.setInputPaths(job, new Path(input));
		FileOutputFormat.setOutputPath(job, new Path(output));

		job.waitForCompletion(true);
	}

}
