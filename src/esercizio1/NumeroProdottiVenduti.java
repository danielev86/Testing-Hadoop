package esercizio1;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.BooleanWritable.Comparator;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapreduce.Partitioner;
public class NumeroProdottiVenduti extends Configured implements Tool {
	public int run(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		
		

		Job conf = new Job(getConf(), "NumeroProdottiVenduti");
		conf.setJarByClass(NumeroProdottiVenduti.class);

		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));

		conf.setMapperClass(ProdottiAcquistatiMapper.class);
		conf.setReducerClass(ProdottiAcquistatiReducer.class);
		
		conf.setMapOutputKeyClass(Text.class);
		conf.setMapOutputValueClass(IntWritable.class);
		
		
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(IntWritable.class);
		
		


		return conf.waitForCompletion(true) ? 0 : 1;
	}
		
	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new NumeroProdottiVenduti(), args);
		System.exit(exitCode);
	}

}
