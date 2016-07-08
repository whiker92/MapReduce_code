
/**
 *  输入格式：y x1 x2...
 *  输出格式：theta0, theta1, theta2...
 *  主要思想：
		在Mapper中进行SGD：theta(j) = theta(j)+alpha*(y-y')*x(j)
		在Reducer中进行加和，取平均值。
    缺点：
		SGD刚开始时的theta值偏差较大，因此取平均值得到的theta，对于样本数比较小的情况下偏差大。
 */

import java.io.IOException;
import java.util.*;
import java.lang.String;

import org.apache.hadoop.conf.Configuration;
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

public class multilinereg {

	public static class MultiLineRegMap extends Mapper<LongWritable, Text, Text, Text> {
		private Text k = new Text();
		private Text val = new Text();
		private static Vector<Double> thetavec = new Vector<Double>();

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			Configuration conf = context.getConfiguration();
			String alpha = conf.get("alpha");
			String line = value.toString();
			StringTokenizer tokenizer = new StringTokenizer(line);

			int first = 0;
			int y = 0;
			Vector<Double> x = new Vector<Double>();
			Double h = 0d;
			String values = "";

			while (tokenizer.hasMoreTokens()) {
				if (first == 0) {
					y = Integer.parseInt(tokenizer.nextToken());
					x.addElement(1.0D);
				} else {
					x.addElement(Double.parseDouble(tokenizer.nextToken()));
				}
				first++;
			}
			if (thetavec.isEmpty()) {
				for (int i = 0; i < first; i++) {
					thetavec.addElement(0.0D);
				}
			}
			for (int i = 0; i < first; i++) {
				h += x.get(i) * thetavec.get(i);
			}
			for (int i = 0; i < first; i++) {
				Double theta = thetavec.get(i) + Double.parseDouble(alpha) * (y - h) * x.get(i);
				thetavec.set(i, theta);
				if (i == 0) {
					values += theta.toString();
				} else {
					values += " " + theta.toString();
				}
			}
			k.set("1");
			val.set(values);
			context.write(k, val);
		}
	}

	public static class MultiLineRegReduce extends Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

			Iterator<Text> iterator = values.iterator();
			Vector<Double> thetavec = new Vector<Double>();
			Double num = 0D;

			while (iterator.hasNext()) {
				String bstr = iterator.next().toString();
				if (!bstr.isEmpty()) {
					String[] vals = bstr.split(" ");
					if (thetavec.isEmpty()) {
						for (String i : vals) {
							thetavec.addElement(Double.parseDouble(i));
						}
					} else {
						for (int i = 0; i < vals.length; i++) {
							Double theta = thetavec.get(i) + Double.parseDouble(vals[i]);
							thetavec.set(i, theta);
						}
					}
					num++;
				}
			}

			String ans = "";
			for (int i = 0; i < thetavec.size(); i++) {
				if (i == 0) {
					Double theta = thetavec.get(i) / num;
					ans += theta.toString();
				} else {
					Double theta = thetavec.get(i) / num;
					ans += " " + theta.toString();
				}
			}
			context.write(new Text(ans), new Text(""));
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		conf.set("alpha", "0.03");

		args[1] = "hdfs://localhost:9000/output_multilinereg/" + new Date().getTime() + "/out";
		Job job = new Job(conf, "multi_linear_reg");
		job.setJarByClass(multilinereg.class);
		job.setMapperClass(MultiLineRegMap.class);
		job.setReducerClass(MultiLineRegReduce.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		if (job.waitForCompletion(true)) {
			System.out.println("Completed multi-linear-regression.");
		}
	}
}
