package edu.ucr.cs.cs226.group2;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Properties;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Attribute;
import org.jsoup.nodes.Attributes;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

public class XMLParsing {
    public static Properties properties;
    public static Set<Object> propConf;
    //public static Set<String> propSet = new HashSet<String>();

    public static class XmlInputFormat1 extends TextInputFormat {
        public static final String START_TAG_KEY = "xmlinput.start";
        public static final String END_TAG_KEY = "xmlinput.end";

        public RecordReader<LongWritable, Text> createRecordReader(
                InputSplit split, TaskAttemptContext context) {
            return new XmlRecordReader();
        }

        public static class XmlRecordReader extends
                RecordReader<LongWritable, Text> {
            private byte[] startTag;
            private byte[] endTag;
            private long start;
            private long end;
            //private String[] prop;
            private FSDataInputStream fsin;
            private DataOutputBuffer buffer = new DataOutputBuffer();
            private LongWritable key = new LongWritable();
            private Text value = new Text();

            public void initialize(InputSplit split, TaskAttemptContext context)
                    throws IOException, InterruptedException {
                Configuration conf = context.getConfiguration();
                this.startTag = conf.get("xmlinput.start").getBytes("utf-8");
                this.endTag = conf.get("xmlinput.end").getBytes("utf-8");
                FileSplit fileSplit = (FileSplit) split;
                this.start = fileSplit.getStart();
                this.end = (this.start + fileSplit.getLength());
                Path file = fileSplit.getPath();
                FileSystem fs = file.getFileSystem(conf);
                this.fsin = fs.open(fileSplit.getPath());
                this.fsin.seek(this.start);
				/*this.prop = conf.getStrings("propetries");
				propConf = new ArrayList<String>(Arrays.asList(prop));*/

            }

            public boolean nextKeyValue() throws IOException,
                    InterruptedException {
                if ((this.fsin.getPos() < this.end)
                        && (readUntilMatch(this.startTag, false))) {
                    try {
                        this.buffer.write(this.startTag);
                        if (readUntilMatch(this.endTag, true)) {
                            this.key.set(this.fsin.getPos());
                            this.value.set(this.buffer.getData(), 0,
                                    this.buffer.getLength());
                            return true;
                        }
                    } finally {
                        this.buffer.reset();
                    }
                    this.buffer.reset();

                    this.buffer.reset();
                }
                return false;
            }

            public LongWritable getCurrentKey() throws IOException,
                    InterruptedException {
                return this.key;
            }

            public Text getCurrentValue() throws IOException,
                    InterruptedException {
                return this.value;
            }

            public void close() throws IOException {
                this.fsin.close();
            }

            public float getProgress() throws IOException {
                return (float) (this.fsin.getPos() - this.start)
                        / (float) (this.end - this.start);
            }

            private boolean readUntilMatch(byte[] match, boolean withinBlock)
                    throws IOException {
                int i = 0;
                do {
                    int b = this.fsin.read();
                    if (b == -1) {
                        return false;
                    }
                    if (withinBlock) {
                        this.buffer.write(b);
                    }
                    if (b == match[i]) {
                        i++;
                        if (i >= match.length) {
                            return true;
                        }
                    } else {
                        i = 0;
                    }
                } while ((withinBlock) || (i != 0)
                        || (this.fsin.getPos() < this.end));
                return false;
            }
        }
    }

    //Mapper - Must initialize variable in setup
    //Override setup and cleanup..
    //Setup and cleanup runs everytime new map is created

    public static class Map extends
            Mapper<LongWritable, Text, Text, Text> {
        //private static final IntWritable one = new IntWritable(1);
        private Text keyword = new Text();
        private Text valueword = new Text();
        //MultipleOutputs<Text, IntWritable> mos;
        static Set<Object> alist = new HashSet<Object>();

        @Override
        protected void setup(
                Mapper<LongWritable, Text, Text, Text>.Context context)
                throws IOException, InterruptedException {
            // TODO Auto-generated method stub
            properties = new Properties();
            super.setup(context);
            //File file1 = new File(path[1]);
			/*BufferedReader br = new BufferedReader(new FileReader(file1));
			String linecont;
			while((linecont = br.readLine())!=null){
				alist.add(linecont);
			}
			br.close();
			//System.out.println(alist.toString());*/
            alist = properties.keySet();
            //mos = new MultipleOutputs<Text, IntWritable>(context);
        }

        @SuppressWarnings({ "unchecked", "rawtypes" })
        protected void map(LongWritable key, Text value, Mapper.Context context)
                throws IOException, InterruptedException {
            String document = value.toString();
            Configuration conf = context.getConfiguration();

            String[] tokens = document.split("\\n");
            String temp = "";
            for (int i = 0; i < tokens.length; i++) {
                if (tokens[i].trim().startsWith("<" + conf.get("xmltag")) && tokens[i].trim().endsWith("/>")) {
                    continue;
                }
                temp += tokens[i].trim() + " ";
            }

            Document d = Jsoup.parse(temp);
            Elements inputs = d.select(conf.get("xmltag"));
            for (Element el : inputs) {
                Attributes attrs = el.attributes();
                //System.out.print("ELEMENT: " + el.tagName());
                for (Attribute attr : attrs) {
                    //System.out.print(" " + attr.getKey() + "=" + attr.getValue());
                    if (attr.getKey().equals("id")) {
                        keyword.set(attr.getValue());
                    } else {
                        valueword.set(attr.getKey() + "=\"" + attr.getValue() + "\"");
                        context.write(keyword, valueword);
                    }
                }
            }
            inputs = d.select("tag");
            for (Element el : inputs) {
                Attributes attrs = el.attributes();
                //System.out.print("ELEMENT: " + el.tagName());
                String k = "";
                String v = "";
                for (Attribute attr : attrs) {
                    //System.out.print(" " + attr.getKey() + "=" + attr.getValue());
                    if (attr.getKey().equals("k")) {
                        k = attr.getValue();
                    } else {
                        v = attr.getValue();
                        valueword.set(k + "=\"" + v + "\"");
                        context.write(keyword, valueword);
                    }
                }
            }
        }

        @Override
        protected void cleanup(
                Mapper<LongWritable, Text, Text, Text>.Context context)
                throws IOException, InterruptedException {
            //mos.close();
        }

    }
    //Reducer begins //
    public static class Reduce extends
            Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values,
                           Reducer<Text, Text, Text, Text>.Context context)
                throws IOException, InterruptedException {
            //Same as word count
            String sum = "";
            for (Text val : values) {
                sum += val.toString() + " ";
            }
            Text result = new Text();
            result.set(sum);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        //Define a new configuration file
        Configuration conf = new Configuration();
        conf.set("xmlinput.start", "<" + args[1]);
        conf.set("xmlinput.end", "</" + args[1] + ">");
        conf.set("xmltag", args[1]);
        Job job = Job.getInstance(conf);

        job.setJarByClass(XMLParsing.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);
        job.setInputFormatClass(XmlInputFormat1.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path("output_" + args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
