/**
 * Created by chetanchandak on 8/6/15.
 */

import java.io.IOException;
import java.util.ArrayList;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class MyJob extends Configured implements Tool {

    public static class MapClass extends Mapper<LongWritable, Text, Text, IntWritable> {
        IntWritable one = new IntWritable(1);

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            StringTokenizer tokenizer = new StringTokenizer(line);
            ArrayList<String> tokens = new ArrayList<>();
            while (tokenizer.hasMoreTokens())
            {
                tokens.add(tokenizer.nextToken());
            }

            if(tokens.size()>2) {
                String genre = tokens.get(2);
                Text em = new Text();

                if (genre.contains("Comedy")) {
                    em.set("Comedy");
                } else if (genre.contains("Adventure")) {
                    em.set("Adventure");
                } else if (genre.contains("Animation")) {
                    em.set("Animation");
                } else if (genre.contains("Children")) {
                    em.set("Children");
                } else if (genre.contains("Fantasy")) {
                    em.set("Fantasy");
                } else if (genre.contains("Romance")) {
                    em.set("Romance");
                } else if (genre.contains("Drama")) {
                    em.set("Drama");
                } else if (genre.contains("Thriller")) {
                    em.set("Thriller");
                } else if (genre.contains("Action")) {
                    em.set("Action");
                } else if (genre.contains("Horror")) {
                    em.set("Horror");
                } else if (genre.contains("Crime")) {
                    em.set("Crime");
                } else if (genre.contains("Sci-Fi")) {
                    em.set("Sci-Fi");
                } else if (genre.contains("Mystery")) {
                    em.set("Mystery");
                } else if (genre.contains("Documentary")) {
                    em.set("Documentary");
                } else if (genre.contains("War")) {
                    em.set("War");
                } else if (genre.contains("Musical")) {
                    em.set("Musical");
                }
                else {
                    em.set("Other Categories");
                }

                context.write(em, one);
            }

        }
    }

    public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result= new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int totalCount = 0;
            for(IntWritable count: values) {
                totalCount = totalCount+count.get();
            }
            result.set(totalCount);

            context.write(key, result);

        }
    }

    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        Job job = new Job(conf, "MyJob");
        job.setJarByClass(MyJob.class);
        Path in = new Path(args[0]);
        Path out = new Path(args[1]);
        FileInputFormat.setInputPaths(job, in);
        FileOutputFormat.setOutputPath(job, out);
        job.setMapperClass(MapClass.class);
        job.setReducerClass(Reduce.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        System.exit(job.waitForCompletion(true)?0:1);
        return 0;
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new MyJob(), args);
        System.exit(res);
    }
}

