package esercizio2;
import java.io.IOException;
import java.text.ParseException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class ProdottiTrimestreMapper extends Mapper<Object,Text,Text,IntWritable>{
	
	private static IntWritable primo = new IntWritable(1);
	private Scontrino scontrino=new Scontrino();
	
	public void map(Object key,Text value,Context context)throws IOException, InterruptedException{
		String inputValue = value.toString();
		String[] valueParts = inputValue.split(",");
		int i;
		for(i=1;i<valueParts.length;i++){
			String[] data = valueParts[0].split("-");
			if((data[1].equals("1")) || (data[1].equals("2")) || (data[1].equals("3")))
					context.write(new Text(valueParts[i]+","+data[1]+"/"+data[0]), primo);
		}
	}
	

}
