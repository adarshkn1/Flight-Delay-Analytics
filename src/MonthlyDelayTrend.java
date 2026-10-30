
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MonthlyDelayTrend {

    public static class MonthlyMapper
            extends Mapper<LongWritable, Text, Text, Text> {

        private final SimpleDateFormat inputFormat =
                new SimpleDateFormat("dd-MM-yyyy HH:mm", Locale.ENGLISH);

        private final SimpleDateFormat monthFormat =
                new SimpleDateFormat("yyyy-MM", Locale.ENGLISH);

        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            String line = value.toString();

            if (line.startsWith("FL_DATE")) {
                return;
            }

            String[] fields = line.split(",", -1);

            if (fields.length < 13) {
                return;
            }

            try {
                Date date = inputFormat.parse(fields[0].trim());
                String month = monthFormat.format(date);

                String arrival = fields[5].trim();

                if (arrival.isEmpty()) {
                    return;
                }

                double delay = Double.parseDouble(arrival);

                int delayed = delay > 15 ? 1 : 0;

                // sum of delay, valid flight count, delayed flight count
                String output = delay + ",1," + delayed;

                context.write(new Text(month), new Text(output));

            } catch (Exception e) {
                // Skip invalid records
            }
        }
    }

    public static class MonthlyReducer
            extends Reducer<Text, Text, Text, Text> {

        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            double totalDelay = 0;
            long totalFlights = 0;
            long delayedFlights = 0;

            for (Text value : values) {

                String[] parts = value.toString().split(",");

                totalDelay += Double.parseDouble(parts[0]);
                totalFlights += Long.parseLong(parts[1]);
                delayedFlights += Long.parseLong(parts[2]);
            }

            double averageDelay = totalDelay / totalFlights;

            double delayRate =
                    (delayedFlights * 100.0) / totalFlights;

            String result = String.format(
                    Locale.US,
                    "%.2f minutes | %d flights | %d delayed | %.2f%%",
                    averageDelay,
                    totalFlights,
                    delayedFlights,
                    delayRate
            );

            context.write(key, new Text(result));
        }
    }

    public static void main(String[] args) throws Exception {

        if (args.length != 2) {
            System.err.println(
                    "Usage: MonthlyDelayTrend <input> <output>"
            );
            System.exit(2);
        }

        Configuration conf = new Configuration();

        Job job = Job.getInstance(
                conf,
                "Monthly Flight Delay Trend Analysis"
        );

        job.setJarByClass(MonthlyDelayTrend.class);

        job.setMapperClass(MonthlyMapper.class);
        job.setReducerClass(MonthlyReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
