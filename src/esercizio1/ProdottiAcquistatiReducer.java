package esercizio1;
import java.io.IOException;
import java.util.Comparator;
import java.util.*;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ProdottiAcquistatiReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
	private class Pair {
	      public String str;
	      public Integer count;
	      
	      public Pair(String str, Integer count) {
	        this.str = str;
	        this.count = count;
	      }
	      
	      public int hashcode(){
	    	  return str.hashCode();
	      }
	 };
	 
	 private class Compara implements Comparator<Pair>{
		 
		 public int compare(Pair p1, Pair p2) {
	          return p1.str.compareTo(p2.str);
	     }
		 
	 };
	 private List<Pair> queue;
	    
	    @Override
	 protected void setup(Context ctx) {
	      queue = new ArrayList<Pair>();
	    }
	
	public void reduce(Text key, Iterable<IntWritable> values, Context context)throws IOException, InterruptedException{
		int sum=0;
		for(IntWritable value:values)
			sum+=value.get();
		queue.add(new Pair(key.toString(), sum));
		
	}
	protected void cleanup(Context ctx) throws IOException, InterruptedException{
		Collections.sort(queue, new Compara());
	    for (int i = queue.size() - 1; i >= 0; i--) {
	    	Pair topKPair = queue.get(i);
	    	ctx.write(new Text(topKPair.str),new IntWritable(topKPair.count));
	    	}
	    }

}
