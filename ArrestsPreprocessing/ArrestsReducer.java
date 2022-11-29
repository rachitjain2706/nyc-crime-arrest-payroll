import java.io.IOException;
import java.util.*;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ArrestsReducer extends Reducer<Text, Text, Text, Text> {

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        String boroughString = key.toString();
        Map<String, Integer> boroughYearArrestsCount = new HashMap<String, Integer>();
        for (Text value : values) {
            String row = value.toString();
            String[] args = row.split(",");
            String boroughYearKey = boroughString + "_" + args[0];
            boroughYearArrestsCount.putIfAbsent(boroughYearKey, 0);
            boroughYearArrestsCount.put(boroughYearKey, boroughYearArrestsCount.get(boroughYearKey) + 1);
            context.write(new Text(boroughString), value);
        }
        for (Map.Entry<String, Integer> entry : boroughYearArrestsCount.entrySet()) {
            String boroughYearKey = entry.getKey();
            int count = entry.getValue();
            context.write(new Text(boroughYearKey), new Text(String.valueOf(count)));
        }
    }
}