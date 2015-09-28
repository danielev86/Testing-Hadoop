package esercizio3;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;


import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;



public class ScontrinoReduce  extends Reducer<Text,IntWritable,Text,IntWritable> {
	private PriorityQueue<Pair> queue;
	private static final int TOP_K = 10;

	@Override
	protected void setup(Context ctx) {
		queue = new PriorityQueue<Pair>(TOP_K, new Comparator<Pair>() {
			public int compare(Pair p1, Pair p2) {
				return p1.count.compareTo(p2.count);
			}
		});
	}

	@Override
	protected void reduce(Text key, Iterable<IntWritable> values, 
			Context ctx) throws IOException, InterruptedException {
		int count = 0;
		for (IntWritable value : values) {
			count = count + value.get();
		}
		queue.add(new Pair(key.toString(), count));
		if (queue.size() > TOP_K) {
			queue.remove();
		}
	}

	@Override
	protected void cleanup(Context ctx) 
			throws IOException, InterruptedException {
		List<Pair> topKPairs = new ArrayList<Pair>();
		while (! queue.isEmpty()) {
			topKPairs.add(queue.remove());
		}
		for (int i = topKPairs.size() - 1; i >= 0; i--) {
			Pair topKPair = topKPairs.get(i);
			ctx.write(new Text(topKPair.str), 
					new IntWritable(topKPair.count));
		}
	}

}
