import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class AirportDelayRate {

    public static class AirportMapper
            extends Mapper<LongWritable, Text, Text, Text> {

        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            String line = value.toString();

            if (line.startsWith("FL_DATE")) return;

            String[] fields = line.split(",", -1);

            if (fields.length <= 5) return;

            String airport = fields[2].trim(); // ORIGIN airport

            try {
                double arrivalDelay = Double.parseDouble(fields[5].trim());

                // Emit: airport -> totalFlights,delayedFlights
                int delayed = arrivalDelay > 15 ? 1 : 0;
                context.write(new Text(airport),
                        new Text("1," + delayed));

            } catch (NumberFormatException e) {
                // Skip missing or invalid arrival delay
            }
        }
    }

    public static class AirportReducer
            extends Reducer<Text, Text, Text, Text> {

        public void reduce(Text airport, Iterable<Text> values,
                           Context context)
                throws IOException, InterruptedException {

            long totalFlights = 0;
            long delayedFlights = 0;

            for (Text value : values) {
                String[] parts = value.toString().split(",");
                totalFlights += Long.parseLong(parts[0]);
                delayedFlights += Long.parseLong(parts[1]);
            }

            double rate = 100.0 * delayedFlights / totalFlights;

            context.write(airport, new Text(
                    String.format("%.2f%% delayed (%d/%d flights)",
                            rate, delayedFlights, totalFlights)));
        }
    }

    public static void main(String[] args) throws Exception {
        Job job = Job.getInstance(new Configuration(),
                "Airport Delay Rate");

        job.setJarByClass(AirportDelayRate.class);
        job.setMapperClass(AirportMapper.class);
        job.setReducerClass(AirportReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
