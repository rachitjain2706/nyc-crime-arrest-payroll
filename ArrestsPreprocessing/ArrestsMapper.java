import java.io.IOException;
import java.util.*;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ArrestsMapper extends Mapper<LongWritable, Text, Text, Text> {

    HashMap<String, String> boroughMap;

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        boroughMap = new HashMap<>();
        boroughMap.put("B", "BRONX");
        boroughMap.put("K", "BROOKLYN");
        boroughMap.put("M", "MANHATTAN");
        boroughMap.put("Q", "QUEENS");
        boroughMap.put("S", "STATEN ISLAND");
        StringBuilder builder = new StringBuilder(value.toString());
        boolean inQuotes = false;
        for (int currentIndex = 0; currentIndex < builder.length(); currentIndex++) {
            char currentChar = builder.charAt(currentIndex);
            if (currentChar == '\"') {
                inQuotes = !inQuotes;
            }
            if (currentChar == ',' && inQuotes) {
                builder.setCharAt(currentIndex, ';');
            }
        }
        String[] args = builder.toString().split(",");

        StringBuilder outputString = new StringBuilder();
        int numColumns = args.length;
        String boroughString = args[8];
        if (!boroughMap.containsKey(boroughString)) {
            return;
        }

        String[] arrestDate = args[1].split("/");
        if(arrestDate == null || arrestDate.length != 3) return;
        boroughString = boroughMap.get(boroughString);
        if(args[5] == null || args[5].length() == 0){
            args[5] = "UNKNOWN";
        }
        if(args[11] == null || args[11].length() == 0){
            args[11] = "UNKNOWN";
        }

        outputString.append(arrestDate[2] + "," + args[5] + "," + args[11]);
        context.write(new Text(boroughString), new Text(outputString.toString()));
    }
}