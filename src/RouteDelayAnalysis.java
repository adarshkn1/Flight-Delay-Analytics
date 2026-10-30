
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

public class RouteDelayAnalysis {

    public static class RouteMapper
            extends Mapper<LongWritable, Text, Text, Text> {

        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            String line = value.toString();

            if (line.startsWith("FL_DATE")) return;

            String[] fields = line.split(",", -1);

            if (fields.length < 13) return;

            try {
                String origin = fields[2].trim();
                String destination = fields[3].trim();
                String arrival = fields[5].trim();

                if (origin.isEmpty() || destination.isEmpty()
                        || arrival.isEmpty()) return;

                double delay = Double.parseDouble(arrival);

                int delayed = delay > 15 ? 1 : 0;

                String route = origin + "-" + destination;
                String result = delay + ",1," + delayed;

                context.write(new Text(route), new Text(result));

            } catch (Exception e) {
                // Skip invalid records
            }
        }
    }

    public static class RouteReducer
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

            if (totalFlights == 0) return;

            double averageDelay = totalDelay / totalFlights;
            double delayRate = delayedFlights * 100.0 / totalFlights;

            String result = String.format(
                    Locale.US,
                    "%.2f,%d,%d,%.2f",
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
                    "Usage: RouteDelayAnalysis <input> <output>"
            );
            System.exit(2);
        }

        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "Route-wise Delay Analysis");

        job.setJarByClass(RouteDelayAnalysis.class);
        job.setMapperClass(RouteMapper.class);
        job.setReducerClass(RouteReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
