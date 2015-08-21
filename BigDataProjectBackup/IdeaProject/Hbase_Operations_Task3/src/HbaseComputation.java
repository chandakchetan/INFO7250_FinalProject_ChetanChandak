/**
 * Created by chetanchandak on 8/10/15.
*/
 import java.io.IOException;
 import org.apache.hadoop.conf.Configuration;
 import org.apache.hadoop.fs.Path;
 import org.apache.hadoop.hbase.HBaseConfiguration;
 import org.apache.hadoop.hbase.client.HTable;
 import org.apache.hadoop.hbase.client.Result;
 import org.apache.hadoop.hbase.client.Scan;
 import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
 import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
 import org.apache.hadoop.hbase.mapreduce.TableMapper;
 import org.apache.hadoop.hbase.util.Bytes;
 import org.apache.hadoop.io.FloatWritable;
 import org.apache.hadoop.io.IntWritable;
 import org.apache.hadoop.io.Text;
 import org.apache.hadoop.mapreduce.Job;
 import org.apache.hadoop.mapreduce.Reducer;
 import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
 import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class HbaseComputation {

        public static class HMapper extends TableMapper<Text, FloatWritable> {
            private Text Key = new Text();
            private FloatWritable Val = new FloatWritable();

            @Override
            public void map(ImmutableBytesWritable movieIdKey, Result result, Context context) throws IOException, InterruptedException {

                // Reading values from Result class object
                byte [] value = result.getValue(Bytes.toBytes("userId"),Bytes.toBytes("uID"));
                byte[] value1 = result.getValue(Bytes.toBytes("movieId"),Bytes.toBytes("mID"));
                byte[] value2 = result.getValue(Bytes.toBytes("rating"),Bytes.toBytes("mRating"));

                byte[] rowKey = Bytes.add(value,value1);

                // Printing the values
                //String userID = Bytes.toString(value);
                String movieID = Bytes.toString(value1);
                String rating = Bytes.toString(value2);

                Key.set(movieID);
                Val.set(Float.parseFloat(rating));
                context.write(Key,Val);

            }

        }

        public static class HReducer extends Reducer<Text, FloatWritable, Text, FloatWritable> {
            private FloatWritable result= new FloatWritable();

            @Override
            public void reduce(Text key, Iterable<FloatWritable> values, Context context) throws IOException, InterruptedException {

                float totalRating = 0;
                int occurences = 0;
                for(FloatWritable count: values) {
                    totalRating = totalRating+count.get();
                    occurences++;
                }
                float averageRating = totalRating/occurences;
                result.set(averageRating);

                context.write(key, result);
            }
        }

        public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

            Configuration configuration = HBaseConfiguration.create();
            Job job = new Job(configuration, "HbaseComputation");
            job.setJarByClass(HbaseComputation.class);     // class that contains mapper and reducer

            Scan scan = new Scan();
            scan.setCaching(500);        // 1 is the default in Scan, which will be bad for MapReduce jobs
            scan.setCacheBlocks(false);  // don't set to true for MR jobs

            TableMapReduceUtil.initTableMapperJob(
                    "MovieRatings",        // input table
                    scan,               // Scan instance to control CF and attribute selection
                    HMapper.class,     // mapper class
                    Text.class,         // mapper output key
                    FloatWritable.class,  // mapper output value
                    job);

            job.setReducerClass(HReducer.class);    // reducer class
            job.setNumReduceTasks(10);    // at least one, adjust as required

            Path out = new Path(args[1]);
            FileOutputFormat.setOutputPath(job, out);  // adjust directories as required

            boolean b = job.waitForCompletion(true);
            if (!b) {
                throw new IOException("error with job!");
            }


            //Path in = new Path(args[0]);
            //FileInputFormat.addInputPath(job,in);
        }

    }
