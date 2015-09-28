package esercizio1;
import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;


public class ProdottiAcquistatiMapper extends Mapper<Object,Text,Text,IntWritable>{
	
	private static IntWritable primo = new IntWritable(1);
	private Text word = new Text();
	
	
	public void map(Object key,Text value,Context context)throws IOException, InterruptedException{
		String inputValue = value.toString();
		String[] valueParts = inputValue.split(",");
		int i;
		for(i=1;i<valueParts.length;i++){
			word.set(valueParts[i]);
			context.write(word, primo);
			
		}
	}

}
