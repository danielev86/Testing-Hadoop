package esercizio3;
import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class ScontrinoMapper extends Mapper<Object,Text,Text,IntWritable>{
	private static final IntWritable ONE = new IntWritable(1);
	private static final BigramWritable BIGRAM = new BigramWritable();

	@Override
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		String line = value.toString();
		String[] split = line.split(",");
		String tmp="";
		int i;
		for(i=1;i<split.length-1;i++)
			tmp+=split[i]+" ";
		tmp+=split[i];
		String prev = null;
		StringTokenizer itr = new StringTokenizer(tmp);
		while (itr.hasMoreTokens()) {
			String cur = itr.nextToken();
			if (prev != null) {
				BIGRAM.set(new Text(prev),new Text(cur));
				context.write(new Text(BIGRAM.toString()), ONE);
			}
			prev = cur;
		}
		
		
	}   

}
