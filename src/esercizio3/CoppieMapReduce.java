package esercizio3;

import java.io.IOException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class CoppieMapReduce extends Configured implements Tool  {

	public int run(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		
		

		Job conf = new Job(getConf(), "CoppieMapReduce");
		conf.setJarByClass(CoppieMapReduce.class);

		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));

		conf.setMapperClass(ScontrinoMapper.class);
		conf.setCombinerClass(ScontrinoReduce.class);
		conf.setReducerClass(ScontrinoReduce.class);
	    conf.setMapOutputKeyClass(Text.class);
	    conf.setMapOutputValueClass(IntWritable.class);
	    conf.setMapOutputKeyClass(Text.class);
	    conf.setMapOutputValueClass(IntWritable.class);
		
		


		return conf.waitForCompletion(true) ? 0 : 1;
	}
		
	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new CoppieMapReduce(), args);
		System.exit(exitCode);
	}

}
