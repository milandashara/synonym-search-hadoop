package synonymSearchEngine.mapReduce;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class SynonymReducer extends Reducer<Text, Text, Text, Text> {

	private Text synonymsText = new Text();
	private MultipleOutputs<Text, Text> mos;

	@Override
	public void setup(final Context context) {
		mos = new MultipleOutputs<Text, Text>(context);
	}

	@Override
	public void cleanup(final Context context) throws IOException,
			InterruptedException {
		mos.close();
	}

	@Override
	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		int wordCount = 0;
		Iterator<Text> it = values.iterator();
		String synonyms=it.next().toString();
		while (it.hasNext()) {
			
			synonyms=synonyms+","+it.next().toString();
		}
		synonymsText.set(synonyms);
		mos.write("synonyms",key, synonymsText);
		
		
		//context.write(key, totalWordCount);
		context.progress();
	}
}
