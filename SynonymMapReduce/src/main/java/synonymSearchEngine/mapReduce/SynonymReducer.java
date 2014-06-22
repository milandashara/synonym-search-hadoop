package synonymSearchEngine.mapReduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hive.hcatalog.data.DefaultHCatRecord;
import org.apache.hive.hcatalog.data.HCatRecord;


public class SynonymReducer extends Reducer<Text, Text, WritableComparable, HCatRecord> {

	private Text synonymsText = new Text();
	private int id=0;
	//private MultipleOutputs<Text, Text> mos;

	@Override
	public void setup(final Context context) {
		//mos = new MultipleOutputs<Text, Text>(context);
	}

	@Override
	public void cleanup(final Context context) throws IOException,
			InterruptedException {
		//mos.close();
	}

	@Override
	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		int wordCount = 0;
		Iterator<Text> it = values.iterator();
		String synonyms=it.next().toString();
		List<String> s=new ArrayList<String>();
		while (it.hasNext()) {
			
			//synonyms=synonyms+","+it.next().toString();
			s.add(it.next().toString());
		}
		synonymsText.set(synonyms);
		//mos.write("synonyms",key, synonymsText);
		 HCatRecord record = new DefaultHCatRecord(1);
         record.set(0, key.toString());
         record.set(1, s);
         context.write(new LongWritable(++id), record);
		
		//context.write(key, totalWordCount);
		//context.progress();
	}
}
