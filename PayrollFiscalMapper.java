import java.io.IOException;
import java.util.*;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class PayrollFiscalMapper extends Mapper<LongWritable, Text, Text, Text> {

    Set<String> boroughSet = new HashSet<>(Arrays.asList("MANHATTAN", "BRONX", "BROOKLYN", "QUEENS", "STATEN ISLAND"));

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        StringBuilder builder = new StringBuilder(value.toString());
        boolean inQuotes = false;
        for (int currentIndex = 0; currentIndex < builder.length(); currentIndex++) {
            char currentChar = builder.charAt(currentIndex);
            if (currentChar == '\"') {
                inQuotes = !inQuotes; // toggle state
            }
            if (currentChar == ',' && inQuotes) {
                builder.setCharAt(currentIndex, ';');
            }
        }
        String[] args = builder.toString().split(",");

        String outputString = "";
        StringBuilder listOfOutputString = new StringBuilder();
        int numColumns = args.length;
        double totalPay = 0;
        String boroughString = args[7];
        if (!boroughSet.contains(boroughString)) {
            return;
        }
        String payBasis = args[11];
        if (payBasis.equals("per Annum") || payBasis.equals("Prorated Annual")) {
            totalPay = Double.parseDouble(args[10]) + Double.parseDouble(args[15]) + Double.parseDouble(args[16]);
        } else if (payBasis.equals("per Hour")) {
            totalPay = (Double.parseDouble(args[10]) * 7 * 5 * 49) + Double.parseDouble(args[15]) + Double.parseDouble(args[16]);
        } else {
            totalPay = (Double.parseDouble(args[10]) * 5 * 49) + Double.parseDouble(args[15]) + Double.parseDouble(args[16]);
        }
        listOfOutputString.append(args[0] + "," + args[2] + "," + args[14] + "," + args[15] + "," + String.valueOf(totalPay));
        context.write(new Text(boroughString), new Text(listOfOutputString.toString()));
    }
}
