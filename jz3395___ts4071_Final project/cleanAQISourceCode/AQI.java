import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.NullWritable; 
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat; 
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
public class AQI {
    public static void main(String[] args) throws Exception { 
        if (args.length != 2) {
            System.err.println("Usage: MaxTemperature <input path> <output path>");
            System.exit(-1);
        }
        Job job = Job.getInstance(); 
        job.setJarByClass(AQI.class); 
        job.setJobName("AQI");
        job.setNumReduceTasks(1);
        FileInputFormat.addInputPath(job, new Path(args[0])); 
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setMapperClass(AQIMapper.class);
        //Combiner's implementation is the same as Reducer's in our case.
        //Cannot use reducer class as combiner because it outputs NullWritable as key.
        //job.setCombinerClass(AQIReducer.class);
        job.setReducerClass(AQIReducer.class);
        //We didn't set input because we are using default
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        System.exit(job.waitForCompletion(true) ? 0 : 1); 
    }
}