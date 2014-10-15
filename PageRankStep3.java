import java.io.IOException;

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


public class PageRankStep3 {
	private static enum Counters{
		ITEM_COUNTER
	};
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
		job.setMapperClass(RebuildStructure.class);
		job.setReducerClass(WriteOut.class);
		// Write to one file
		job.setNumReduceTasks(1);
		job.setMapOutputKeyClass(OutputKey.class);
		job.setMapOutputValueClass(Text.class);
		job.setJarByClass(PageRankStep3.class);
		FileInputFormat.addInputPath(job, new Path(configuration.get("input_path")));
		FileOutputFormat.setOutputPath(job, new Path(configuration.get("output_path")));
		// Start Job
		job.waitForCompletion(true);

	}
	
	public static class RebuildStructure extends Mapper<Object, Text, OutputKey, Text>{
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
			// Get the "page,rank" part of data
			String[] splits = value.toString().split("\t", 2);
			// Get last comma index
			int lastComma = splits[0].lastIndexOf(',');
			String page = splits[0].substring(0, lastComma);
			double rank = Double.parseDouble(splits[0].substring(lastComma + 1));
			// page_rank[0] = page, [1] = rank. Encapsulate them
			OutputKey outputKey = new OutputKey(page, rank);
			// Write them out
			context.write(outputKey, new Text());
		}
	}
	
	public static class WriteOut extends Reducer<OutputKey, Text, Text, Text>{
		public void reduce(OutputKey key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
			// Due to Outputkey is already sorted, just write them out
			if(context.getCounter(Counters.ITEM_COUNTER).getValue() < 100){
				context.write(new Text(key.toString()), new Text());
				context.getCounter(Counters.ITEM_COUNTER).increment(1);
			}
			
		}
		public void cleanup(Context context){
			System.out.println(context.getCounter(Counters.ITEM_COUNTER).getValue());
		}
	}

}
