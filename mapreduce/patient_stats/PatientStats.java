import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class PatientStats {

    public static class PatientMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        private final static SimpleDateFormat sdf1 = new SimpleDateFormat("dd/MM/yyyy HH:mm");
        private final static SimpleDateFormat sdf2 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        private static final String SENTINEL_DATE = "9999-12-31 00:00:00";
        private Text outputKey = new Text();
        private IntWritable outputValue = new IntWritable(1);  // For counting gender
        private Text ageKey = new Text("AGE_SUM");
        private Text ageCountKey = new Text("AGE_COUNT");

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            if (key.get() == 0 && value.toString().contains("row_id")) {
                return;  // skip header
            }

            String[] fields = value.toString().split(",");
            if (fields.length < 8) return;

            try {
                String gender = fields[2].trim().toUpperCase();
                String dobStr = fields[3].trim();
                String dodStr = fields[4].trim();

                // Emit gender counts (e.g., GENDER_M or GENDER_F)
                if (gender.equals("M") || gender.equals("F")) {
                    outputKey.set("GENDER_" + gender);
                    context.write(outputKey, outputValue);
                }

                // Parse dates
                Date dob = null;
                Date refDate = null;

                if (dobStr.contains("/")) {
                    dob = sdf1.parse(dobStr);
                } else if (dobStr.contains("-")) {
                    dob = sdf2.parse(dobStr);
                }

                if (dodStr.equals(SENTINEL_DATE) || dodStr.isEmpty()) {
                    refDate = new Date(); // current date if no dod
                } else {
                    if (dodStr.contains("/")) {
                        refDate = sdf1.parse(dodStr);
                    } else if (dodStr.contains("-")) {
                        refDate = sdf2.parse(dodStr);
                    }
                }

                if (dob != null && refDate != null && !dob.after(refDate)) {
                    long diffInMillis = refDate.getTime() - dob.getTime();
                    int age = (int) TimeUnit.MILLISECONDS.toDays(diffInMillis) / 365;
                    if (age >= 0 && age <= 90) {
                        // Emit age sum and count for average calculation
                        context.write(ageKey, new IntWritable(age));
                        context.write(ageCountKey, new IntWritable(1));
                    }
                }

            } catch (Exception e) {
                // Ignore bad parsing
            }
        }
    }

    public static class PatientReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private int ageSum = 0;
        private int ageCount = 0;

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            String k = key.toString();
            if (k.equals("AGE_SUM")) {
                for (IntWritable val : values) {
                    ageSum += val.get();
                }
            } else if (k.equals("AGE_COUNT")) {
                for (IntWritable val : values) {
                    ageCount += val.get();
                }
            } else if (k.equals("GENDER_M") || k.equals("GENDER_F")) {
                int sum = 0;
                for (IntWritable val : values) {
                    sum += val.get();
                }
                context.write(new Text(k), new IntWritable(sum));
            }
        }

        protected void cleanup(Context context) throws IOException, InterruptedException {
            if (ageCount != 0) {
                int avgAge = ageSum / ageCount;
                context.write(new Text("AVERAGE_AGE"), new IntWritable(avgAge));
            }
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: PatientStats <input path> <output path>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Patient Statistics");

        job.setJarByClass(PatientStats.class);
        job.setMapperClass(PatientMapper.class);
        job.setReducerClass(PatientReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
