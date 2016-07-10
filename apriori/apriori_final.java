
/**
 * 
 *  Apriori Algorithm: http://www.codeproject.com/KB/recipes/AprioriAlgorithm.aspx
 * 
 * 输入数据：Item1, Item2, Item3...
 * 输出数据：{Frequent Items}, support value (in file out2)
 * 
 */

import java.io.IOException;
import java.util.*;
import java.lang.Integer.*;
import java.lang.String;

import org.apache.log4j.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.*; // For KeyValue
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class apriori {

	public static class FrequentItemsMap extends Mapper<LongWritable, Text, Text, Text> {
		private Text word = new Text();
		private Text count = new Text();

		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException {
			String line = value.toString();
			StringTokenizer tokenizer = new StringTokenizer(line);

			while (tokenizer.hasMoreTokens()) {
				word.set(tokenizer.nextToken());
				count.set(Integer.toString(1));
				try {
					context.write(word, count);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}

		}
	}

	public static class CandidateGenMap extends Mapper<LongWritable, Text, Text, Text> {
		private Text word = new Text();
		private Text count = new Text();

		public void CandidatesGenRecursion(Vector<String> in, Vector<String> out, int length, int level, int start,
				Context context) throws IOException {

			int i, size;

			for (i = start; i < length; i++) {
				if (level == 0) {
					out.add(in.get(i));
				} else {
					out.add(in.get(i));

					int init = 1;
					StringBuffer current = new StringBuffer();
					for (String s : out) {
						if (init == 1) {
							current.append(s);
							init = 0;
						} else {
							current.append(" ");
							current.append(s);
						}
					}

					word.set(current.toString());
					count.set(Integer.toString(1));
					try {
						context.write(word, count);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}
				if (i < length - 1) {
					CandidatesGenRecursion(in, out, length, level + 1, i + 1, context);
				}
				size = out.size();
				if (size > 0) {
					out.remove(size - 1);
				}
			}

		}

		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException {
			Configuration conf = context.getConfiguration();
			String databaseName = conf.get("databaseName");
			int Support = Integer.parseInt(conf.get("Support"));
			
			String line = value.toString();
			StringTokenizer tokenizer = new StringTokenizer(line);

			Configuration config = HBaseConfiguration.create();
			HTable tableC = new HTable(config, databaseName);
			Vector<String> lst = new Vector<String>();
			int loop = 0;
			while (tokenizer.hasMoreTokens()) {

				String str = tokenizer.nextToken();
				Get g = new Get(Bytes.toBytes(str));
				Result r = tableC.get(g);
				byte[] contents = r.getValue(Bytes.toBytes("count"), Bytes.toBytes(str));

				String countStr = Bytes.toString(contents);

				int cnt = Integer.parseInt(countStr);
				if (cnt >= Support) {
					lst.add(str);
					loop++;
				}

			}

			Vector<String> combinations = new Vector<String>();

			if (!lst.isEmpty()) {
				CandidatesGenRecursion(lst, combinations, loop, 0, 0, context);
			}
			for (String i : lst) {
				word.set(i.toString());
				count.set(Integer.toString(1));
				try {
					context.write(word, count);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
			tableC.close();
		}
	}

	public static class FrequentItemsReduce extends Reducer<Text, Text, Text, Text> {
		@Override
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException {
			Iterator<Text> iterator = values.iterator();
			int sum = 0;
			Configuration conf = context.getConfiguration();
			String databaseName = conf.get("databaseName");
			
			Configuration config = HBaseConfiguration.create();
			HBaseAdmin hBaseAdmin = new HBaseAdmin(config);
			HTableDescriptor tableDescriptor = new HTableDescriptor(databaseName);
			tableDescriptor.addFamily(new HColumnDescriptor("count"));
			if (!hBaseAdmin.tableExists(databaseName)) {
				hBaseAdmin.createTable(tableDescriptor);
			}
			HTable table = new HTable(config, databaseName);
			Put p = new Put(Bytes.toBytes(key.toString()));

			while (iterator.hasNext()) {
				String prevVal = iterator.next().toString();
				sum += Integer.parseInt(prevVal);
			}

			try {
				context.write(key, new Text(Integer.toString(sum)));
				p.add(Bytes.toBytes("count"), Bytes.toBytes(key.toString()), Bytes.toBytes(Integer.toString(sum)));
				context.progress();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			table.put(p);
			table.close();
			hBaseAdmin.close();
		}
	}

	public static class CandidateGenReduce extends Reducer<Text, Text, Text, Text> {

		@Override
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException {
			Configuration conf = context.getConfiguration();
			String databaseName = conf.get("databaseName");
			int Support = Integer.parseInt(conf.get("Support"));
			
			Iterator<Text> iterator = values.iterator();
			int sum = 0;

			Configuration config = HBaseConfiguration.create();
			HTable table = new HTable(config, databaseName);;
			Put p = new Put(Bytes.toBytes(key.toString()));

			while (iterator.hasNext()) {
				String prevVal = iterator.next().toString();
				sum += Integer.parseInt(prevVal);
			}
			if (sum >= Support) {
				try {
					context.write(key, new Text(Integer.toString(sum)));
					p.add(Bytes.toBytes("count"), Bytes.toBytes(key.toString()), Bytes.toBytes(Integer.toString(sum)));
					context.progress();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				table.put(p);
				table.close();
			}
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		conf.set("Support", "5");
		
		Date dt;
		long start, end;
		dt = new Date();
		start = dt.getTime();
		conf.set("databaseName", "FreqItems"+Long.toString(start));

		args[1] = "hdfs://localhost:9000/output_apriori/" + start + "/out1";
		args[2] = "hdfs://localhost:9000/output_apriori/" + start + "/out2";

//		Configuration config = HBaseConfiguration.create();
//		HBaseAdmin hBaseAdmin = new HBaseAdmin(config);
//		if (hBaseAdmin.tableExists("FreqItems")) {
//			hBaseAdmin.disableTable("FreqItems");
//			hBaseAdmin.deleteTable("FreqItems");
//			System.out.println("In main: FreqItems is exist,detele....");
//		}
		
		Job job = new Job(conf, "apriori freq items");
		job.setJarByClass(apriori.class);
		job.setMapperClass(FrequentItemsMap.class);
		job.setCombinerClass(FrequentItemsReduce.class); //
		job.setReducerClass(FrequentItemsReduce.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		if (job.waitForCompletion(true)) {
			System.out.println("FrequentItem complete.");
		}

		Configuration conf1 = new Configuration();
		conf1.set("Support", "5");
		conf1.set("databaseName", "FreqItems"+Long.toString(start));
		Job job2 = new Job(conf1, "apriori candidate gen");
		job2.setJarByClass(apriori.class);

		job2.setMapperClass(CandidateGenMap.class);
		job2.setCombinerClass(CandidateGenReduce.class); 
		job2.setReducerClass(CandidateGenReduce.class);
		job2.setMapOutputKeyClass(Text.class);
		job2.setMapOutputValueClass(Text.class);
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(Text.class);

		job2.setInputFormatClass(TextInputFormat.class);
		job2.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job2, new Path(args[0]));
		FileOutputFormat.setOutputPath(job2, new Path(args[2]));

		if (job2.waitForCompletion(true)) {
			System.out.println("CandidateGen complete.");
		}

		dt = new Date();
		end = dt.getTime();

	}

}
