package edu.ucr.cs.cs226.group2;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Attribute;
import org.jsoup.nodes.Attributes;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

public class CountSpecificKey
{
    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, IntWritable>{

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            String line = value.toString().trim();
            if (line.startsWith("<tag")) {
                Document d = Jsoup.parse(line);
                Elements inputs = d.select("tag");
                for (Element el : inputs) {
                    Attributes attrs = el.attributes();
                    String k = "";
                    for (Attribute attr : attrs) {
                        if (attr.getKey().equals("k")) {
                            k = attr.getValue();
                            continue;
                        }
                        if (k.equals("amenity")) {
                            word.set(attr.getValue());
                            context.write(word, one);
                        }
                    }
                }
            }
        }
    }

    public static class IntSumReducer
            extends Reducer<Text,IntWritable,Text,IntWritable> {
        //private IntWritable result = new IntWritable();

        private HashMap<Text, Integer> countMap = new HashMap<>();
        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            countMap.put(new Text(key), sum);
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException{
            List<Map.Entry<Text, Integer>> list = new LinkedList<>(countMap.entrySet());
            Collections.sort(list, new Comparator<Map.Entry<Text, Integer>>() {
                @Override
                public int compare(Map.Entry<Text, Integer> o1, Map.Entry<Text, Integer> o2) {
                    return o1.getValue() < o2.getValue() ? 1 : -1;
                }
            });

            for (Map.Entry<Text, Integer> l : list) {
                context.write(l.getKey(), new IntWritable(l.getValue()));
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "CountTag");
        job.setJarByClass(CountSpecificKey.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setInputFormatClass(TextInputFormat.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path("outputCount_" + args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
