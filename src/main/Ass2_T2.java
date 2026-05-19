/*
 * Grep example
 *
 * parameters:
 * args[0] -> input directory
 * args[1] -> output directory
 */

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

public class Ass2_T2 extends Configured implements Tool {

    public static class Map extends Mapper<Object, Text, Text, Text> {

        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {

            String line = value.toString();

            String[] parts = line.split(":", 2);

            if (parts.length < 2) {
                return;
            }

            String docid = parts[0];
            String text = parts[1].toLowerCase();

            if (text.contains("malware")) {
                context.write(new Text(docid), new Text(""));
            }
        }
    }

    public int run(String[] args) throws Exception {
        Configuration conf = getConf();

        Job job = Job.getInstance(conf, "grep");
        job.setJarByClass(Ass2_T2.class);

        // set Mapper class
        job.setMapperClass(Map.class);

        // map-only job
        job.setNumReduceTasks(0);

        // output data types
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

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
        ToolRunner.run(new Configuration(), new Ass2_T2(), args);
    }
}