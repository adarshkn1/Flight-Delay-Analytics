
import java.io.IOException;
import java.util.Locale;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class CancellationDiversionAnalysis {

    public static class DisruptionMapper
            extends Mapper<LongWritable, Text, Text, Text> {

        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            String line = value.toString();

            if (line.startsWith("FL_DATE")) return;

            String[] fields = line.split(",", -1);

            if (fields.length < 13) return;

            try {
                String carrier = fields[1].trim();

                if (carrier.isEmpty()) return;

                int cancelled = Integer.parseInt(fields[6].trim());
                int diverted = Integer.parseInt(fields[7].trim());

                if ((cancelled != 0 && cancelled != 1)
                        || (diverted != 0 && diverted != 1)) return;

                String result = "1," + cancelled + "," + diverted;

                context.write(new Text(carrier), new Text(result));

            } catch (Exception e) {
                // Skip invalid records
            }
        }
    }

    public static class DisruptionReducer
            extends Reducer<Text, Text, Text, Text> {

        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            long totalFlights = 0;
            long cancelledFlights = 0;
            long divertedFlights = 0;

            for (Text value : values) {

                String[] parts = value.toString().split(",");

                totalFlights += Long.parseLong(parts[0]);
                cancelledFlights += Long.parseLong(parts[1]);
                divertedFlights += Long.parseLong(parts[2]);
            }

            if (totalFlights == 0) return;

            double cancellationRate =
                    cancelledFlights * 100.0 / totalFlights;

            double diversionRate =
                    divertedFlights * 100.0 / totalFlights;

            String result = String.format(
                    Locale.US,
                    "%d,%d,%.2f,%d,%.2f",
                    totalFlights,
                    cancelledFlights,
                    cancellationRate,
                    divertedFlights,
                    diversionRate
            );

            context.write(key, new Text(result));
        }
    }

    public static void main(String[] args) throws Exception {

        if (args.length != 2) {
            System.err.println(
                    "Usage: CancellationDiversionAnalysis <input> <output>"
            );
            System.exit(2);
        }

        Configuration conf = new Configuration();

        Job job = Job.getInstance(
                conf,
                "Cancellation and Diversion Analysis"
        );

        job.setJarByClass(CancellationDiversionAnalysis.class);
        job.setMapperClass(DisruptionMapper.class);
        job.setReducerClass(DisruptionReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
