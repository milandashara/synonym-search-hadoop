package synonymSearchEngine.mapReduce;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class SumReducer extends Reducer<Text, Text, Text, Text> {

	private Text totalWordCount = new Text();

	@Override
	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		int wordCount = 0;
		Iterator<Text> it = values.iterator();
		
		while (it.hasNext()) {
			System.out.println(it.next().toString());
			wordCount ++;
		}
		totalWordCount.set(""+wordCount);
		context.write(key, totalWordCount);
	}
}
