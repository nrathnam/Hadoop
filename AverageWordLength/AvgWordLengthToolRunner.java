package solution1;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;



public class AvgWordLengthToolRunner extends Configured implements Tool {

  public static void main(String[] args)  throws Exception {
	  Configuration conf = new Configuration();
  
	//conf.setBoolean("caseSensitive", false);
		int exitCode = ToolRunner.run(conf, new AvgWordLengthToolRunner(),args);
		System.exit(exitCode);
  
    }

  @Override
	public int run(String[] args) throws Exception {
		
		if (args.length != 2) {
			System.out.printf("Usage: AvgWordLengthToolRunner <input dir> <output dir>\n");
			System.exit(-1);
		}
  
  
    Job job = new Job(getConf());
    
  
    job.setJarByClass(AvgWordLengthToolRunner.class);
    

    job.setJobName("Average Word Length using Tool Runner");


    FileInputFormat.setInputPaths(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

  
    job.setMapperClass(LetterMapper1.class);
    job.setReducerClass(AverageReducer1.class);

  
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(IntWritable.class);

 
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(DoubleWritable.class);

   
    boolean success = job.waitForCompletion(true);
    return(success ? 0 : 1);
    
  
}
}


} 
