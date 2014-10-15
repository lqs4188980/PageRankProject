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


public class PageRankStep2 {
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
		job.setMapperClass(PageRankSplitter.class);
		job.setReducerClass(PageRankGenerator.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setJarByClass(PageRankStep2.class);
		FileInputFormat.addInputPath(job, new Path(configuration.get("input_path")));
		FileOutputFormat.setOutputPath(job, new Path(configuration.get("output_path")));
		// Start Job
		job.waitForCompletion(true);
	}
	
	public static class PageRankSplitter extends Mapper<Object, Text, Text, Text>{
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
			String[] splits = value.toString().split("\t");
			if(splits.length >= 2){
				// Get the first split: page, rank
				// Get last comma index
				int lastComma = splits[0].lastIndexOf(',');
				String page = splits[0].substring(0, lastComma);
				double rank = Double.parseDouble(splits[0].substring(lastComma + 1));
				// Get counts of link list
				int counts = splits.length - 1;
				// Get separated rank
				double rankDevided = rank/counts;
				// Send out rankDevide to each outgoing link
				for(int i = 1; i < splits.length; i++){
					context.write(new Text(splits[i]), new Text("" + rankDevided));
					//System.out.println("Key:" + page + ",value: " + "" + rankDevided);
				}
				// Write out the link list
				String[] listSplit = value.toString().split("\t", 2);
				context.write(new Text(page), new Text(listSplit[1]));
				//System.out.println("Key:" + page + ",Value: " + listSplit[1]);
				
			}
		}
	}
	
	public static class PageRankGenerator extends Reducer<Text, Text, Text, Text>{
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
			// The format come to reducer will be {key:page;value:[rank1, rank2, ..., rankn, [list of links]]}
			// Initialize sum
			double sum = 0.0;
			String linkList = "";
			double d = 0.85;
			// Get sum of each rank and link list
			//System.out.println("----------->Key: " + key.toString());
			for(Text text: values){
				//System.out.println("-------->" + text);
				try {
					//System.out.println("------------> Link list before assignment: " + linkList);
					//System.out.println("----------->Parse Double");
					sum += Double.parseDouble(text.toString());
					
				} catch (Exception e) {
					// TODO: handle exception
					//System.out.println("------------> Link List: " + text.toString());
					linkList = text.toString();
					//System.out.println("-----------> Link list after assignment: " + linkList);
				}
			}
			// re-calculate pagerank for this page
			double pagerank = (1-d) + d*sum;
			// Write the result to output
			String page_rank = key.toString() + "," + pagerank;
			//System.out.println("New Line: " + page_rank + "\t" + linkList);
			context.write(new Text(page_rank), new Text(linkList));
		}
	}

}

