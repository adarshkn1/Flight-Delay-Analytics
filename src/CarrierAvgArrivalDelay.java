import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class CarrierAvgArrivalDelay {

    public static class DelayMapper
            extends Mapper<LongWritable, Text, Text, DoubleWritable> {

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            String line = value.toString();

            // Skip CSV header
            if (line.startsWith("FL_DATE,")) {
                return;
            }

            String[] fields = line.split(",", -1);

            // Required columns: carrier = 1, arrival delay = 5
            if (fields.length <= 5) {
                return;
            }

            String carrier = fields[1].trim();
            String delayText = fields[5].trim();

            if (carrier.isEmpty() || delayText.isEmpty()) {
                return;
            }

            try {
                double arrivalDelay = Double.parseDouble(delayText);
                context.write(new Text(carrier),
                              new DoubleWritable(arrivalDelay));
            } catch (NumberFormatException ignored) {
                // Ignore malformed delay values
            }
        }
    }

    public static class AverageReducer
            extends Reducer<Text, DoubleWritable, Text, Text> {

        @Override
        protected void reduce(Text carrier, Iterable<DoubleWritable> delays,
                              Context context)
                throws IOException, InterruptedException {

            double sum = 0.0;
            long count = 0;

            for (DoubleWritable delay : delays) {
                sum += delay.get();
                count++;
            }

            if (count > 0) {
                double average = sum / count;
                context.write(carrier,
                        new Text(String.format("%.2f minutes (%d flights)",
                                             average, count)));
            }
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println(
                    "Usage: CarrierAvgArrivalDelay <input HDFS path> <output HDFS path>");
            System.exit(2);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Average Arrival Delay by Carrier");

        job.setJarByClass(CarrierAvgArrivalDelay.class);
        job.setMapperClass(DelayMapper.class);
        job.setReducerClass(AverageReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DoubleWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
