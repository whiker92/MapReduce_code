
/*
 * 
 *  Apriori Algorithm: http://www.codeproject.com/KB/recipes/AprioriAlgorithm.aspx
 * 
 *  支持度5 频繁集于out2中
 * 
 *  Note:
 *  ======
 *  Running the final code with log4j causes performance hit. Disable log4j in the final run
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

			Logger logger = Logger.getLogger(apriori.class.getName());
			PropertyConfigurator.configure("/home/whiker/soft/hadoop-1.2.1/conf/log4j.properties");
			logger.info("Map: Line is => " + line);

			// Word Count
			while (tokenizer.hasMoreTokens()) {
				word.set(tokenizer.nextToken());
				count.set(Integer.toString(1));
				try {
					context.write(word, count);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}

		}
	}

	public static class CandidateGenMap extends Mapper<LongWritable, Text, Text, Text> {
		private Text word = new Text();
		private Text count = new Text();
		private int Support = 5;

		public void CandidatesGenRecursion(Vector<String> in, Vector<String> out, int length, int level, int start,
				Context context) throws IOException {

			// StringBuffer current = new StringBuffer();
			Logger logger = Logger.getLogger(apriori.class.getName());
			PropertyConfigurator.configure("/home/whiker/soft/hadoop-1.2.1/conf/log4j.properties");
			int i, size;

			logger.info("CandidatesGenRecursion: [len, lev, start] => [" + length + "," + level + "," + start + "]");

			for (i = start; i < length; i++) {
				if (level == 0) {
					out.add(in.get(i));
					logger.info("CandidatesGenRecursion: Candidate Level0 is => " + out.toString());
				} else {
					logger.info("CandidatesGenRecursion: Level0+ Before " + out.toString());
					logger.info("CandidatesGenRecursion: Level0++ Before " + out.toString() + "-" + in.get(i));
					out.add(in.get(i));
					// Emit this
					logger.info("CandidatesGenRecursion: Candidate Level" + level + " is => " + out.toString());

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

					logger.info("CandidatesGenRecursion: EMIT => " + current.toString());

					word.set(current.toString());
					count.set(Integer.toString(1));
					try {
						context.write(word, count);
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
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
			String line = value.toString();
			StringTokenizer tokenizer = new StringTokenizer(line);

			Logger logger = Logger.getLogger(apriori.class.getName());
			PropertyConfigurator.configure("/home/whiker/soft/hadoop-1.2.1/conf/log4j.properties");
			logger.info("CandidateGenMap: Line is => " + line);
			logger.info("CandidateGenMap: KEY is => " + key.toString());

			Configuration config = HBaseConfiguration.create();
			HTable tableC = new HTable(config, "FreqItems");
			Vector<String> lst = new Vector<String>();
			int loop = 0;
			while (tokenizer.hasMoreTokens()) {

				String str = tokenizer.nextToken();
				logger.info("CandidateGenMap: BEFORE LOOP Str is " + str);

				// Now, to retrieve the data we just wrote. The values that come
				// back are
				// Result instances. Generally, a Result is an object that will
				// package up
				// the hbase return into the form you find most palatable.
				Get g = new Get(Bytes.toBytes(str));
				Result r = tableC.get(g);
				byte[] contents = r.getValue(Bytes.toBytes("count"), Bytes.toBytes(str));

				// If we convert the value bytes, we should get back 'Some
				// Value', the
				// value we inserted at this location.
				String countStr = Bytes.toString(contents);
				logger.info("CandidateGenMap: BEFORE LOOP Count-Str is " + countStr);

				// Prune
				int cnt = Integer.parseInt(countStr);
				if (cnt >= Support) {
					lst.add(str);
					loop++;
				}

			}

			logger.info("CandidateGenMap: Line is => " + line);
			logger.info("APRIORI_Map: BEFORE EMIT =>" + lst.toString());

			Vector<String> combinations = new Vector<String>();

			if (!lst.isEmpty()) {
				CandidatesGenRecursion(lst, combinations, loop, 0, 0, context);
			}
			for(String i:lst){
				word.set(i.toString());
				count.set(Integer.toString(1));
				try {
					context.write(word, count);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
	}

	public static class FrequentItemsReduce extends Reducer<Text, Text, Text, Text> {
		@Override
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException {
			Iterator<Text> iterator = values.iterator();
			int sum = 0;

			Logger logger = Logger.getLogger(apriori.class.getName());
			PropertyConfigurator.configure("/home/whiker/soft/hadoop-1.2.1/conf/log4j.properties");
			logger.info("FrequentItemsReduce: ");

			Configuration config = HBaseConfiguration.create();
			HBaseAdmin hBaseAdmin = new HBaseAdmin(config);
			HTableDescriptor tableDescriptor = new HTableDescriptor("FreqItems");
			tableDescriptor.addFamily(new HColumnDescriptor("count"));
			if (!hBaseAdmin.tableExists("FreqItems")) {
				hBaseAdmin.createTable(tableDescriptor);
			}
			HTable table = new HTable(config, Bytes.toBytes("FreqItems"));
			Put p = new Put(Bytes.toBytes(key.toString()));

			while (iterator.hasNext()) {
				String prevVal = iterator.next().toString();
				sum += Integer.parseInt(prevVal);
			}

			try {
				logger.info("FrequentItemsReduce: Adding to DB FreqItems (Key,Qualifier,Value) => (" + key.toString()
						+ "," + key.toString() + "," + Integer.toString(sum) + ")");
				context.write(key, new Text(Integer.toString(sum)));
				p.add(Bytes.toBytes("count"), Bytes.toBytes(key.toString()), Bytes.toBytes(Integer.toString(sum)));
				context.progress();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				logger.error("FrequentItemsReduce: Error while setting sum = " + sum);
				e.printStackTrace();
			}
			table.put(p);
		}
	}

	public static class CandidateGenReduce extends Reducer<Text, Text, Text, Text> {
		private int Support = 5;

		@Override
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException {
			Iterator<Text> iterator = values.iterator();
			int sum = 0;

			Logger logger = Logger.getLogger(apriori.class.getName());
			PropertyConfigurator.configure("/home/whiker/soft/hadoop-1.2.1/conf/log4j.properties");
			// logger.info("CandidateGenReduce: ");

			Configuration config = HBaseConfiguration.create();
			HTable table = new HTable(config, "FreqItems");
			Put p = new Put(Bytes.toBytes(key.toString()));

			while (iterator.hasNext()) {
				String prevVal = iterator.next().toString();
				sum += Integer.parseInt(prevVal);
			}
			if (sum >= Support) {
				try {
					logger.info("CandidateGenReduce: Adding to DB Candidates (Key,Qualifier,Value) => ("
							+ key.toString() + "," + key.toString() + "," + Integer.toString(sum) + ")");
					context.write(key, new Text(Integer.toString(sum)));
					p.add(Bytes.toBytes("count"), Bytes.toBytes(key.toString()), Bytes.toBytes(Integer.toString(sum)));
					context.progress();
				} catch (InterruptedException e) {
					logger.error("CandidateGenReduce: Error while setting sum = " + sum);
					e.printStackTrace();
				}
				table.put(p);
			}
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		System.out.println("Started Job");
		Date dt;
		long start, end; // Start and end time

		// Start Timer
		dt = new Date();
		start = dt.getTime();

		args[1] = "hdfs://localhost:9000/" + start + "/output_apriori/out1";
		args[2] = "hdfs://localhost:9000/" + start + "/output_apriori/out2";

		Configuration config = HBaseConfiguration.create();
		HBaseAdmin hBaseAdmin = new HBaseAdmin(config);
		if (hBaseAdmin.tableExists("FreqItems")) {// 如果存在要创建的表，那么先删除，再创建
			hBaseAdmin.disableTable("FreqItems");
			hBaseAdmin.deleteTable("FreqItems");
			System.out.println("In main: FreqItems is exist,detele....");
		}

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

		Logger logger = Logger.getLogger(apriori.class.getName());
		PropertyConfigurator.configure("/home/whiker/soft/hadoop-1.2.1/conf/log4j.properties");
		logger.info("Entering application Apriori.");

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		// System.exit(job.waitForCompletion(true) ? 0 :1);
		if (job.waitForCompletion(true)) {
			logger.info("Completed Word Count Generation SUCCESSFULLY for Apriori.");
		} else {
			logger.info(" ERROR - Completed Candidate Generation for Apriori.");
		}

		Configuration conf1 = new Configuration();
		System.out.println("Starting Job2");
		Job job2 = new Job(conf1, "apriori candidate gen");
		job2.setJarByClass(apriori.class);

		job2.setMapperClass(CandidateGenMap.class);
		job2.setCombinerClass(CandidateGenReduce.class); //
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
			logger.info("Completed Candidate Generation SUCCESSFULLY for Apriori 2.");

		} else {
			logger.info(" ERROR - Completed Candidate Generation for Apriori 2.");
		}

		// End Timer
		dt = new Date();
		end = dt.getTime();

		logger.info("Completed Association Rule for Apriori.");
		logger.info("Association Rule Start time=>" + start);
		logger.info("Association Rule End time=>" + end);

		logger.info("Total time => " + (end - start));

	}

}
