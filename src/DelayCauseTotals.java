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

public class DelayCauseTotals {

    public static class CauseMapper
            extends Mapper<Object, Text, Text, DoubleWritable> {

        private final Text cause = new Text();
        private final DoubleWritable minutes = new DoubleWritable();

        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {

            String line = value.toString();

            if (line.startsWith("FL_DATE")) {
                return; // Skip CSV header
            }

            String[] fields = line.split(",", -1);

            if (fields.length < 13) {
                return;
            }

            String[] causeNames = {
                "CARRIER_DELAY",
                "WEATHER_DELAY",
                "NAS_DELAY",
                "SECURITY_DELAY",
                "LATE_AIRCRAFT_DELAY"
            };

            int[] causeIndexes = {8, 9, 10, 11, 12};

            for (int i = 0; i < causeIndexes.length; i++) {
                String field = fields[causeIndexes[i]].trim();

                if (!field.isEmpty()) {
                    try {
                        double delay = Double.parseDouble(field);

                        if (delay > 0) {
                            cause.set(causeNames[i]);
                            minutes.set(delay);
                            context.write(cause, minutes);
                        }
                    } catch (NumberFormatException ignored) {
                        // Skip invalid numeric values
                    }
                }
            }
        }
    }

    public static class CauseReducer
            extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

        private final DoubleWritable total = new DoubleWritable();

        public void reduce(Text key, Iterable<DoubleWritable> values,
                           Context context)
                throws IOException, InterruptedException {

            double sum = 0;

            for (DoubleWritable value : values) {
                sum += value.get();
            }

            total.set(sum);
            context.write(key, total);
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: DelayCauseTotals <input> <output>");
            System.exit(2);
        }

        Job job = Job.getInstance(new Configuration(), "Delay Cause Totals");
        job.setJarByClass(DelayCauseTotals.class);

        job.setMapperClass(CauseMapper.class);
        job.setReducerClass(CauseReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DoubleWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
