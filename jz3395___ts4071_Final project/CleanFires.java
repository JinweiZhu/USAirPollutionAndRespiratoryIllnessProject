import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class CleanFires {
  public static void main(String[] args) throws Exception {
    if (args.length != 2) {
      System.err.println("Usage: CleanRespiratory <input path> <output path>");
      System.exit(-1);
    }
    Job job = Job.getInstance();
    job.setJarByClass(CleanFires.class);
    job.setJobName("Clean fires data");
    job.setNumReduceTasks(5);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    job.setMapperClass(CleanFiresMapper.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
