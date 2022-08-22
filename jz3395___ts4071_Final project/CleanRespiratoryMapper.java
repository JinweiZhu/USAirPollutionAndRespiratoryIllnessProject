import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Arrays;
import java.util.stream.Collectors;

public class CleanRespiratoryMapper extends Mapper<LongWritable, Text, Text, Text> {

  @Override
  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
    String line = value.toString();
    final String[] split = line.split(",");

    if (!"measure_id".equals(split[0]) && split[4].length() >= 4) {
      if (split[4].length() == 4) {
        split[4] = "0" + split[4];
        System.out.println(Arrays.stream(split).collect(Collectors.joining(", ")));
      }
      context.write(new Text(), new Text(Arrays.stream(split).collect(Collectors.joining(", "))));
    } else {
      System.out.println("skip: " + line);
    }
  }
}
