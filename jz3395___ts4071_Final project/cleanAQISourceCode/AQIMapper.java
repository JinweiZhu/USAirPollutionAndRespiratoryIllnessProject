import java.io.IOException;
import org.apache.hadoop.io.IntWritable; 
import org.apache.hadoop.io.LongWritable; 
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.util.*;
import java.lang.Integer;
import java.lang.String;
public class AQIMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] array=line.split(",");
        String outputKey="";
        int outputValue=0;
        if (!(array[0].equals("\"State Name\""))){
            outputKey = array[0].substring(1,array[0].length()-1)+","+array[1].substring(1,array[1].length()-1)+","+array[4].substring(1,5);
            outputValue=Integer.parseInt(array[5]);
        }
        context.write(new Text(outputKey), new IntWritable(outputValue));
    }

}
    