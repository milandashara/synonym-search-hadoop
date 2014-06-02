/**
 * Copyright 2012 Impetus Infotech.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package synonymSearchEngine.mapReduce;

/**
 * <Prove description of functionality provided by this Type> 
 * @author amresh.singh
 */
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
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
