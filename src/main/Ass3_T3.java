/*
 * Inverted Index example
 *
 * parameters:
 * args[0] -> input directory
 * args[1] -> output directory
 */

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Ass3_T3 extends Configured implements Tool {

    public static class Map extends Mapper<Object, Text, Text, IntWritable> {

        private final Text word = new Text();

        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {

            String line = value.toString();

            String[] parts = line.split(":", 2);

            if (parts.length < 2) {
                return;
            }

            int docid = Integer.parseInt(parts[0].trim());

            String text = parts[1]
                    .replace('\"', ' ')
                    .replace('\'', ' ')
                    .replace('!', ' ')
                    .replace('?', ' ')
                    .replace('(', ' ')
                    .replace(')', ' ')
                    .replace('-', ' ')
                    .replace('.', ' ')
                    .replace(':', ' ')
                    .replace(';', ' ')
                    .replace('=', ' ')
                    .replace(',', ' ')
                    .replace('[', ' ')
                    .replace(']', ' ')
                    .toLowerCase();

            StringTokenizer itr = new StringTokenizer(text);

            while (itr.hasMoreTokens()) {
                word.set(itr.nextToken());
                context.write(word, new IntWritable(docid));
            }
        }
    }

    public static class Reduce
            extends Reducer<Text, IntWritable, Text, Text> {

        private final Text result = new Text();

        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context)
                throws IOException, InterruptedException {

            StringBuilder postingList = new StringBuilder();

            int count = 0;

            for (IntWritable val : values) {

                if (count > 0) {
                    postingList.append(", ");
                }

                postingList.append(val.get());

                count++;
            }

            if (count >= 5) {
                result.set(postingList.toString());
                context.write(key, result);
            }
        }
    }

    public int run(String[] args) throws Exception {

        Configuration conf = getConf();

        Job job = Job.getInstance(conf, "inverted index");

        job.setJarByClass(Ass3_T3.class);

        // set Mapper and Reducer classes
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        // output data types for Mapper
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        // output data types for Reducer
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // delete output directory if it already exists
        Path output = new Path(args[1]);
        output.getFileSystem(conf).delete(output, true);

        // set input and output directories
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, output);

        // run the job
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {

        ToolRunner.run(
                new Configuration(),
                new Ass3_T3(),
                args
        );
    }
}