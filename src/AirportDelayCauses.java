import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class AirportDelayCauses {

    public static class AirportMapper
            extends Mapper<LongWritable, Text, Text, Text> {

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

            String airport = fields[2].trim();

            String[] causes = {
                "CARRIER_DELAY",
                "WEATHER_DELAY",
                "NAS_DELAY",
                "SECURITY_DELAY",
                "LATE_AIRCRAFT_DELAY"
            };

            for (int i = 0; i < causes.length; i++) {

                try {
                    double delay = Double.parseDouble(
                            fields[i + 8].trim());

                    if (delay > 0) {
                        context.write(
                            new Text(airport + "_" + causes[i]),
                            new Text(String.valueOf(delay))
                        );
                    }

                } catch (NumberFormatException e) {
                    // Ignore missing or invalid delay values
                }
            }
        }
    }

    public static class AirportReducer
            extends Reducer<Text, Text, Text, DoubleWritable> {

        public void reduce(Text key, Iterable<Text> values,
                           Context context)
                throws IOException, InterruptedException {

            double totalDelay = 0;

            for (Text value : values) {
                totalDelay += Double.parseDouble(value.toString());
            }

            context.write(key, new DoubleWritable(totalDelay));
        }
    }

    public static void main(String[] args) throws Exception {

        Job job = Job.getInstance(
                new Configuration(),
                "Airport Delay Causes");

        job.setJarByClass(AirportDelayCauses.class);

        job.setMapperClass(AirportMapper.class);
        job.setReducerClass(AirportReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        FileInputFormat.addInputPath(
                job, new Path(args[0]));

        FileOutputFormat.setOutputPath(
                job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
