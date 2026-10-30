import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class AirportAvgArrivalDelay {

    public static class AirportMapper
            extends Mapper<Object, Text, Text, DoubleWritable> {

        private final Text airport = new Text();
        private final DoubleWritable delay = new DoubleWritable();

        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {

            String line = value.toString();

            if (line.startsWith("FL_DATE")) return;

            String[] fields = line.split(",", -1);
            if (fields.length < 6) return;

            String origin = fields[2].trim();
            String arrival = fields[5].trim();

            if (origin.isEmpty() || arrival.isEmpty()) return;

            try {
                double minutes = Double.parseDouble(arrival);
                airport.set(origin);
                delay.set(minutes);
                context.write(airport, delay);
            } catch (NumberFormatException ignored) {
                // Skip invalid delay values
            }
        }
    }

    public static class AirportReducer
            extends Reducer<Text, DoubleWritable, Text, Text> {

        private final Text result = new Text();

        public void reduce(Text airport, Iterable<DoubleWritable> values,
                           Context context)
                throws IOException, InterruptedException {

            double sum = 0;
            long count = 0;

            for (DoubleWritable value : values) {
                sum += value.get();
                count++;
            }

            result.set(String.format("%.2f minutes (%d flights)",
                                    sum / count, count));
            context.write(airport, result);
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: AirportAvgArrivalDelay <input> <output>");
            System.exit(2);
        }

        Job job = Job.getInstance(new Configuration(),
                                  "Average Arrival Delay by Airport");
        job.setJarByClass(AirportAvgArrivalDelay.class);

        job.setMapperClass(AirportMapper.class);
        job.setReducerClass(AirportReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DoubleWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
