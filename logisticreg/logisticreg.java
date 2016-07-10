
/**
 *  输入格式：y x1 x2...
 *  输出格式：theta0, theta1, theta2...
 *  主要思想：
		在Mapper中进行SGD：theta(j) = theta(j)+alpha*(y-y')*x(j)
		在Reducer中进行加和，取平均值
		对每次计算结果与y值进行作差，查看准确度，若达到要求，停止迭代continue=true stop=false
 *  缺点：
		SGD刚开始时的theta值偏差较大，因此取平均值得到的theta，对于样本数比较小的情况下偏差大
 */

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;
import java.lang.String;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class logisticreg {

	public static class LogisticRegMap extends Mapper<LongWritable, Text, Text, Text> {
		private Text k = new Text();
		private Text val = new Text();
		private static Vector<Double> thetavec = new Vector<Double>();

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			Configuration conf = context.getConfiguration();
			String alpha = conf.get("alpha");
			Double accuracy = Double.parseDouble(conf.get("accuracy"));
			String thetaPath = conf.get("thetaPath");

			String line = value.toString();
			StringTokenizer tokenizer = new StringTokenizer(line);

			int first = 0; // number of feature
			Double y = 0.0; // value of y
			Vector<Double> x = new Vector<Double>(); // value list of xi
			Double h = 0.0; // value of y'
			String values = ""; // value of theta to reducer
			Double tol = 0.0; // value of y-y'

			while (tokenizer.hasMoreTokens()) {
				if (first == 0) {
					y = Double.parseDouble(tokenizer.nextToken());
					x.addElement(1.0D);
				} else {
					x.addElement(Double.parseDouble(tokenizer.nextToken()));
				}
				first++;
			}
			if (thetavec.isEmpty()) {
				FileSystem hdfs = FileSystem.get(conf);
				FSDataInputStream in = hdfs.open(new Path(thetaPath));
				BufferedReader d = new BufferedReader(new InputStreamReader(in));
				String[] thetaString = d.readLine().split(" ");
				for (int i = 0; i < first; i++) {
					thetavec.addElement(Double.parseDouble(thetaString[i]));
				}
			}
			for (int i = 0; i < first; i++) {
				h += x.get(i) * thetavec.get(i);
			}
			h = 1.0/(1.0+Math.exp(-h));
			// the condition of stop iterator: continue=true stop=false
			tol = Math.abs(h - y);
			if (tol > accuracy) {
				values += String.valueOf(true);
				for (int i = 0; i < first; i++) {
					Double theta = thetavec.get(i) + Double.parseDouble(alpha) * ((y - h) * x.get(i));
					thetavec.set(i, theta);
					values += " " + theta.toString();
				}
			} else {
				values += String.valueOf(false);
				for (int i = 0; i < first; i++) {
					values += " " + thetavec.get(i).toString();
				}
			}
			k.set("1");
			val.set(values);
			context.write(k, val);
		}
	}

	public static class LogisticRegReduce extends Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

			Iterator<Text> iterator = values.iterator();
			Vector<Double> thetavec = new Vector<Double>();
			Double num = 0D;

			boolean stopOrNot = true;
			while (iterator.hasNext()) {
				boolean first = true;
				String bstr = iterator.next().toString();
				String[] vals = bstr.split(" ");
				if (thetavec.isEmpty()) {
					for (String i : vals) {
						if (first) {
							first = false;
							stopOrNot = stopOrNot && Boolean.parseBoolean(i);
						} else
							thetavec.addElement(Double.parseDouble(i));
					}
				} else {
					for (int i = 0; i < vals.length; i++) {
						if (first) {
							first = false;
							stopOrNot = stopOrNot && Boolean.parseBoolean(vals[i]);
						} else {
							Double theta = thetavec.get(i - 1) + Double.parseDouble(vals[i]);
							thetavec.set(i - 1, theta);
						}
					}
				}
				num++;
			}

			String ans = String.valueOf(stopOrNot);
			for (int i = 0; i < thetavec.size(); i++) {
				Double theta = thetavec.get(i) / num;
				ans += " " + theta.toString();
			}
			context.write(new Text(ans), new Text(""));
		}
	}

	public static void main(String[] args) throws Exception {
		// 需要给定的值：
		// 特征个数，迭代次数，准确度，学习率，hdfs地址
		int numIter = 50, numFeature = 1 + 1;
		long start = new Date().getTime();
		Configuration conf = new Configuration();
		conf.set("alpha", "0.03"); // 学习率
		conf.set("accuracy", "0.01"); // 误差范围
		conf.set("numIter", "2"); // 最大迭代次数
		conf.set("numFeature", "1");// 特征个数
		conf.set("thetaPath", "hdfs://localhost:9000/logisticregtheta/" + String.valueOf(start) + "/theta.txt");

		FileSystem hdfs = FileSystem.get(conf);

		for (int iter = 0; iter < numIter; iter++) {

			// theta list file
			if (iter == 0) {
				String theta = "0";
				for (int i = 1; i < numFeature; i++)
					theta += " 0";
				hdfs.mkdirs(new Path("hdfs://localhost:9000/logisticregtheta/"));
				hdfs.mkdirs(new Path("hdfs://localhost:9000/logisticregtheta/" + String.valueOf(start)));
				FSDataOutputStream outputStream = hdfs.create(
						new Path("hdfs://localhost:9000/logisticregtheta/" + String.valueOf(start) + "/theta.txt"));
				outputStream.write(theta.getBytes(), 0, theta.getBytes().length);
				outputStream.close();
			}

			args[1] = "hdfs://localhost:9000/output_logisticreg/" + String.valueOf(start) + "/out";
			Job job = new Job(conf, "logistic_regression");
			job.setJarByClass(logisticreg.class);
			job.setMapperClass(LogisticRegMap.class);
			job.setReducerClass(LogisticRegReduce.class);
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(Text.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			job.setInputFormatClass(TextInputFormat.class);
			job.setOutputFormatClass(TextOutputFormat.class);

			FileInputFormat.addInputPath(job, new Path(args[0]));
			FileOutputFormat.setOutputPath(job, new Path(args[1]));

			if (job.waitForCompletion(true)) {
				System.out.println("Completed iterator " + String.valueOf(iter) + " logistic_regression.");
			}

			// find condition value of iterator
			FSDataInputStream in = hdfs.open(new Path(args[1] + "/part-r-00000"));
			BufferedReader d = new BufferedReader(new InputStreamReader(in));
			String[] values = d.readLine().split(" ");
			int numTheta = values.length;
			String ans = values[1];
			for (int iTheta = 2; iTheta < numTheta; iTheta++) {
				ans += " " + values[iTheta];
			}
			d.close();
			hdfs.delete(new Path(args[1]), true);
			if (values[0].equals("false") || iter == (numIter - 1)) { // stop
																		// iterator
				hdfs.mkdirs(new Path(args[1]));
				FSDataOutputStream outputStream = hdfs.create(new Path(args[1] + "/theta"));
				outputStream.write(ans.getBytes(), 0, ans.getBytes().length);
				outputStream.close();
				break;
			} else { // continue iterator
				hdfs.delete(new Path("hdfs://localhost:9000/logisticregtheta/" + String.valueOf(start)), true);
				hdfs.mkdirs(new Path("hdfs://localhost:9000/logisticregtheta/" + String.valueOf(start)));
				FSDataOutputStream outputStream = hdfs.create(
						new Path("hdfs://localhost:9000/logisticregtheta/" + String.valueOf(start) + "/theta.txt"));
				outputStream.write(ans.getBytes(), 0, ans.getBytes().length);
				outputStream.close();
			}
		}
		hdfs.close();
	}
}
