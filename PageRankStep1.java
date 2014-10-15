import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;



public class PageRankStep1 {
	private static Configuration configuration = new Configuration();
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		// TODO Auto-generated method stub
		// Check parameters
		for(int i = 0; i < args.length; i++){
			if(args[i].equals("-input")){
				configuration.set("input_path", args[i + 1]);
			}
			
			if (args[i].equals("-output")) {
				configuration.set("output_path", args[i + 1]);
			}
		}
		// Initialize Job
		Job job = new Job(configuration);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setMapperClass(Parser.class);
		job.setReducerClass(Encapsulator.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setJarByClass(PageRankStep1.class);
		FileInputFormat.addInputPath(job, new Path(configuration.get("input_path")));
		FileOutputFormat.setOutputPath(job, new Path(configuration.get("output_path")));
		// Start Job
		job.waitForCompletion(true);
	}

	
	public static class Parser extends Mapper<Object, Text, Text, Text>{
		// For each line, split the line by "\t" and make the first split as key and second split as value
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
			// Modify: Add replacing for white spaces at page1
			String[] splits = value.toString().replace(' ', '_').split("\t");
			//System.out.println(value + "" + splits.length);
			if(splits.length >= 2){
				context.write(new Text(splits[0]), new Text(splits[1]));
			}
			// splits[0] = "page1" splits[1] = "page2"
			
		}
	}
	
	public static class Encapsulator extends Reducer<Text, Text, Text, Text>{
		// Now the data is shown as {page i: [page a, page b, ...]}
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
			// We still need to append pagerank value to each key. So set the initial pagerank value as 0.5
			String linkList = "";
			Iterator<Text> iterator = values.iterator();
			if (iterator.hasNext()) {
				linkList = iterator.next().toString();
			}
			if(iterator.hasNext()){
				do {
					linkList += "\t" + iterator.next().toString();
				} while (iterator.hasNext());
			}
			// Prepare key
			String keyString = key.toString() + "," + "0.5";
			context.write(new Text(keyString), new Text(linkList));
		}
		// Now the data is what we need. The list is separate by \t
	}
}
