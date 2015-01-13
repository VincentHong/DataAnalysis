package com.hong.dataanalysis;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import com.hong.hdfs.HdfsDAO;


public class Step3_Add {

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

	public static class AddMap extends Mapper<Object, Text, Text, Text> {

		@Override
		protected void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			List<String> tokens = new ArrayList<String>(Arrays.asList(value
					.toString().split("	")));
			context.write(
					new Text(tokens.get(0)),
					new Text(ListToString(tokens.subList(1, tokens.size()))));
		}

	}

	public static class AddReduce extends Reducer<Text, Text, Text, Text> {

		protected void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			Text output = new Text();
			String end = "";
			String add = "";
			List<String> result = new ArrayList<String>();
			while (values.iterator().hasNext()) {
				String value = values.iterator().next().toString();
				List<String> valueList = new ArrayList<String>(
						Arrays.asList(value.split("	")));
				if (valueList.size() > 4) {
					end = valueList.get(14);
					result = valueList.subList(0, 10);
				} else {
					add = value;
					
				}
			}
			output.set(ListToString(result) + "	" + add + "	" + end);
			context.write(key, output);

		}

	}

	public static void run(String in1, String in2, String out) throws Exception {

		String input1 = "hdfs://namenode:9000/user/flp/" + in1;
		String input2 = "hdfs://namenode:9000/user/flp/" + in2;
		String output = "hdfs://namenode:9000/user/flp/" + out;

		Configuration conf = new Configuration();

		Job job =Job.getInstance(conf, "Step3_Add");
		job.setJarByClass(Step3_Add.class);

		HdfsDAO hdfs = new HdfsDAO("hdfs://192.168.1.206:9000", conf);
		hdfs.rmr(output);

		job.setMapperClass(AddMap.class);
		job.setReducerClass(AddReduce.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.setInputPaths(job, new Path(input1), new Path(input2));
		FileOutputFormat.setOutputPath(job, new Path(output));

		job.waitForCompletion(true);
	}

}
