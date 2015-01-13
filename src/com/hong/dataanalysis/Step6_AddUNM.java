package com.hong.dataanalysis;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.hong.hdfs.HdfsDAO;


public class Step6_AddUNM {
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

	public static class AddNUMMap extends Mapper<Object, Text, Text, Text> {
		private String flag;
		Text mapKey = new Text();
		Text mapValue = new Text();

		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
			FileSplit split = (FileSplit) context.getInputSplit();
			flag = split.getPath().getParent().getName();
		}

		@Override
		protected void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {

			if (flag.equals("step4_sort")) {
				List<String> tokens = new ArrayList<String>(Arrays.asList(value
						.toString().split("	")));
				List<String> sNode = tokens.subList(5, 8);
				mapKey.set(ListToString(sNode));
				context.write(mapKey, value);
			} else {
				List<String> tokens = new ArrayList<String>(Arrays.asList(value
						.toString().split("	")));
				List<String> eNode = tokens.subList(0, 3);
				mapKey.set(ListToString(eNode));
				mapValue.set(tokens.get(3));
				context.write(mapKey, mapValue);
			}
		}

	}

	public static class AddNUMReduce extends
			Reducer<Text, Text, NullWritable, Text> {

		
		
		NullWritable myNull = NullWritable.get();
		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			float num = 1;
			Text value = new Text();
			List<String> valueList = new ArrayList<String>();
			while (values.iterator().hasNext()) {

				value = values.iterator().next();

				if (value.toString().indexOf("	") == -1) {
					num = Float.valueOf(value.toString());
				}else{
					valueList.add(value.toString());
				
				}
			}
			for (String valueOut : valueList) {
				valueOut = valueOut + "	" + num;
				value.set(valueOut);
				context.write(myNull, value);
			}
		}
	}

	public static void run(String in1, String in2, String out) throws Exception {
		String input1 = "hdfs://namenode:9000/user/flp/" + in1;
		String input2 = "hdfs://namenode:9000/user/flp/" + in2;
		String output = "hdfs://namenode:9000/user/flp/" + out;

		Configuration conf = new Configuration();
		Job job =  Job.getInstance(conf, "Step6_AddUNM");
		job.setJarByClass(Step6_AddUNM.class);

		HdfsDAO hdfs = new HdfsDAO("hdfs://192.168.1.206:9000", conf);
		hdfs.rmr(output);

		job.setMapperClass(AddNUMMap.class);
		job.setReducerClass(AddNUMReduce.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);

		FileInputFormat.setInputPaths(job, new Path(input1), new Path(input2));
		FileOutputFormat.setOutputPath(job, new Path(output));
		
		

		job.waitForCompletion(true);
	}

}
