import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class CleanFiresMapper extends Mapper<LongWritable, Text, Text, Text> {

  @Override
  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
    String line = value.toString();

    if (line == null) return;

    final String[] split = line.split("\\|");

    if (split.length == 37 && !"FOD_ID".equalsIgnoreCase(split[0])) {
      String nwcg_reporting_unit_name = split[6];
      String source_reporting_unit_name = split[8];
      String fire_name = split[12];
      String fire_year = split[18];
      String discovery_date = split[19];
      String nwcg_cause_classification = split[22];
      String nwcg_general_cause = split[23];
      String fire_size = split[28];
      String fire_size_class = split[29];
      String state = split[33];
      String fips_code = split[35];

      if (nwcg_reporting_unit_name != null
          && source_reporting_unit_name != null
          && fire_name != null
          && fire_year != null
          && discovery_date != null
          && nwcg_cause_classification != null
          && nwcg_general_cause != null
          && fire_size != null
          && fire_size_class != null
          && state != null
          && fips_code != null) {

        final List<String> cleanDataList =
            Arrays.asList(
                nwcg_reporting_unit_name,
                source_reporting_unit_name,
                fire_name,
                fire_year,
                discovery_date,
                nwcg_cause_classification,
                nwcg_general_cause,
                fire_size,
                fire_size_class,
                state,
                fips_code);
        //        System.out.println(cleanDataList);
        context.write(new Text(), new Text(String.join(", ", cleanDataList)));
      }

    } else {
      System.out.println("skip: " + split.length + "  " + line);
    }
  }
}