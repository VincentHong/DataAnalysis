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
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import com.hong.hdfs.HdfsDAO;


public class Step1_delete {

	public static class DeleteMap extends
			Mapper<Object, Text, FloatWritable, Text> {

		@Override
		protected void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			List<String> tokens = new ArrayList<String>(Arrays.asList(value
					.toString().split("	")));
			context.write(new FloatWritable(Float.valueOf(tokens.get(0))),
					new Text("legal"));
			for (int i = 11; i < 14; i++) {
				if (!(Float.valueOf(tokens.get(i)) == 0)) {
					context.write(
							new FloatWritable(Float.valueOf(tokens.get(i))),
							new Text(tokens.get(0)));
				}
			}
		}
	}

	public static class DeleteReduce extends
			Reducer<FloatWritable, Text, FloatWritable, Text> {

		protected void reduce(FloatWritable key, Iterable<Text> values,
				Context context) throws IOException, InterruptedException {
			boolean isExists = false;
			List<String> outputs = new ArrayList<String>();
			while (values.iterator().hasNext()) {
				String value = values.iterator().next().toString();
				if (value.equals("legal")) {
					isExists = true;
				} else {
					outputs.add(value);
				}
			}
			if (isExists) {
				context.write(new FloatWritable(Float.valueOf(key.toString())),
						new Text("0.0"));
				for (String output : outputs) {
					context.write(new FloatWritable(Float.valueOf(output)),
							new Text(key.toString()));
				}
			}
		}

	}

	public static void run(String in, String out) throws Exception {
		String input = "hdfs://namenode:9000/user/flp/" + in;
		String output = "hdfs://namenode:9000/user/flp/" + out;

		Configuration conf = new Configuration();

		Job job =  Job.getInstance(conf, "Step1_delete");
		job.setJarByClass(Step1_delete.class);

		HdfsDAO hdfs = new HdfsDAO("hdfs://192.168.1.206:9000", conf);
		hdfs.rmr(output);

		job.setMapperClass(DeleteMap.class);
		job.setReducerClass(DeleteReduce.class);

		job.setMapOutputKeyClass(FloatWritable.class);
		job.setMapOutputValueClass(Text.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.setInputPaths(job, new Path(input));
		FileOutputFormat.setOutputPath(job, new Path(output));
		
		job.waitForCompletion(true);
	}
}
