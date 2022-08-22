import java.io.IOException;
import org.apache.hadoop.io.IntWritable; 
import org.apache.hadoop.io.NullWritable; 
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.util.*;
import java.lang.Integer;
import java.lang.StringBuilder;
import java.lang.String;
public class AQIReducer extends Reducer<Text, IntWritable, NullWritable, Text> {
    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int[] AQIvalue=new int[11];
        for (IntWritable value : values) {
            if (value.get()<=20){
                AQIvalue[0]++;
            }
            else if (value.get()<=40){
                AQIvalue[1]++;
            }
            else if (value.get()<=60){
                AQIvalue[2]++;
            }
            else if (value.get()<=80){
                AQIvalue[3]++;
            }
            else if (value.get()<=100){
                AQIvalue[4]++;
            }
            else if (value.get()<=120){
                AQIvalue[5]++;
            }
            else if (value.get()<=140){
                AQIvalue[6]++;
            }
            else if (value.get()<=160){
                AQIvalue[7]++;
            }
            else if (value.get()<=180){
                AQIvalue[8]++;
            }
            else if (value.get()<=200){
                AQIvalue[9]++;
            }
            else if (value.get()>200){
                AQIvalue[10]++;
            }
        }
        String outputValue = "";
        //Convert the array into CSV form using stringBuilder;
        StringBuilder sb = new StringBuilder();
        for (int s : AQIvalue) { 
            sb.append(Integer.toString(s)).append(","); 
        }
        outputValue=key.toString()+","+sb.deleteCharAt(sb.length()-1).toString();
        context.write(NullWritable.get(), new Text(outputValue)); 
    }
}