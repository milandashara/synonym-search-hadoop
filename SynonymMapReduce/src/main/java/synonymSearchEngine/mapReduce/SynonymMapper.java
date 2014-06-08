
package synonymSearchEngine.mapReduce;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class SynonymMapper extends Mapper<Object, Text, Text, Text> {


	@Override
	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
		// Break line into words for processing
		String keyword = context.getConfiguration().get("keyword");
		FileSplit fileSplit = (FileSplit) context.getInputSplit();
		String filename = fileSplit.getPath().getName();
		

		if (filename.equalsIgnoreCase("dictionary1")) {
			
			if (value.toString().startsWith(keyword) && value.toString().contains("Synonyms")) {
				//System.out.println(value.toString());
				String temp[]=value.toString().split("\\.");
				String commaSeperatedSynonyms = "";
				if(temp !=null && temp.length>=2)
				{
					commaSeperatedSynonyms=temp[1];
				}
				commaSeperatedSynonyms=commaSeperatedSynonyms.substring(10);
				//System.out.println(commaSeperatedSynonyms);
				
				
				

				String synonym[] = commaSeperatedSynonyms.split(",");
				for (String synony : synonym) {
					System.out.println(synony.trim());
					context.write(new Text(keyword), new Text(synony.trim()));
				}

			}

		}

	}
}
