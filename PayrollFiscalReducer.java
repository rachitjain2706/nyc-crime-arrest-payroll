import java.io.IOException;
import java.util.*;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class PayrollFiscalReducer extends Reducer<Text, Text, Text, Text> {

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        String boroughString = key.toString();
        List<String> yearList = new ArrayList<>();
        Map<String, Integer> boroughYearDroppedMap = new HashMap<String, Integer>();
        for (Text value : values) {
            String row = value.toString();
            String[] args = row.split(",");
            double totalPay = Double.parseDouble(args[4]);
            String boroughYearKey = boroughString + "_" + args[0];
            if (totalPay < 20000) {
                if (boroughYearDroppedMap.containsKey(boroughYearKey)) {
                    int count = boroughYearDroppedMap.get(boroughYearKey);
                    boroughYearDroppedMap.put(boroughYearKey, count + 1);
                } else {
                    boroughYearDroppedMap.put(boroughYearKey, 1);
                }
            } else {
                context.write(new Text(boroughString), value);
            }
        }
        for (Map.Entry<String, Integer> entry : boroughYearDroppedMap.entrySet()) {
            String boroughYearKey = entry.getKey();
            int count = entry.getValue();
            context.write(new Text(boroughYearKey), new Text(String.valueOf(count)));
        }
    }
}