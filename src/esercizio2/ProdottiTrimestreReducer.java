package esercizio2;


import java.io.IOException;
import java.util.*;
import java.util.Collections;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;







public class ProdottiTrimestreReducer extends Reducer<Text,IntWritable,Text,Text> {
	
	
	private Map<String,List<Scontrino>> mappa;
	protected void setup(Context ctx) {
		mappa = new HashMap<String,List<Scontrino>>();
	}
	public void reduce(Text key, Iterable<IntWritable> values, Context context)throws IOException, InterruptedException{
		int sum=0;
		for(IntWritable value:values)
			sum+=value.get();
		
		String[] splitS = key.toString().split(",");
		if(mappa.containsKey(splitS[0]))
			mappa.get(splitS[0]).add(new Scontrino(splitS[0],splitS[1],sum));
		else{
			Scontrino  temp = new Scontrino(splitS[0],splitS[1],sum);
			List<Scontrino> listaTmp = new ArrayList<Scontrino>();
			listaTmp.add(temp);
			mappa.put(splitS[0],listaTmp);
		}
		
		
	}
	
	protected void cleanup(Context ctx) throws IOException, InterruptedException{
		List<String> set = new ArrayList<String>();
		set.addAll(mappa.keySet());
		Collections.sort(set, new Comparator<String>() {
		    public int compare(String o1, String o2) {
		       
		        return (-1)*o1.compareTo(o2);
		    }
		});
		
	    for (String iterator:set) {
	    	List<Scontrino> lista = mappa.get(iterator);
	    	String info="";
	    	for(Scontrino scontrino:lista){

		    	info += scontrino.getData()+":"+scontrino.getValore()+" ";
	    	}
	    	
	    	ctx.write(new Text(iterator), new Text(info));
	    	}
	    }

}

