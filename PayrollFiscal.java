import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class PayrollFiscal {
    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: PayrollFiscal <input path> <output path>");
            System.exit(-1);
        }
        Configuration configuration = new Configuration();
        configuration.set("mapred.textoutputformat.separator", ",");
        Job job = Job.getInstance(configuration);
        job.setJarByClass(PayrollFiscal.class);
        job.setJobName("Payroll Fiscal");
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setMapperClass(PayrollFiscalMapper.class);
        job.setReducerClass(PayrollFiscalReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.waitForCompletion(true);
    }
}
