package edu.ucr.cs.cs226.group2;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataOutputBuffer;
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

public class CheckInvalidEnclosure {
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
                int tagIndex = readUntilMatch(this.startTag, false);
                if ((this.fsin.getPos() < this.end)
                        && (tagIndex != 0)) {
                    try {
                        this.buffer.write(tagIndex == 1 ? Arrays.copyOfRange(this.startTag, 0, 9) : Arrays.copyOfRange(this.startTag, 9, 13));
                        if (readUntilMatch(this.endTag, true) != 0) {
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

            private int readUntilMatch(byte[] match, boolean withinBlock) throws IOException {
                //<relation<way false
                //</relation></way> true
                boolean end = withinBlock;
                int i = 0;
                int j = end ? 11 : 9;
                do {
                    int b = this.fsin.read();
                    if (b == -1) {
                        return 0;
                    }
                    if (withinBlock) {
                        this.buffer.write(b);
                    }
                    if (b == match[i]) {
                        i++;
                        if ((i >= 11 && end) || (i >= 9 && !end)) {
                            return 1;
                        }
                    } else {
                        i = 0;
                    }
                    if (b == match[j]) {
                        j++;
                        if ((j >= 17 && end) || (j >= 13 && !end)) {
                            return 2;
                        }
                    } else {
                        j = end ? 11 : 9;
                    }
                } while ((withinBlock) || (i != 0) || (j != 9 && !end) || (j != 11 && end) || (this.fsin.getPos() < this.end));
                return 0;
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
            //System.out.println(document);
            Configuration conf = context.getConfiguration();

            String[] tokens = document.split("\\n");
            String temp = "";
            for (int i = 0; i < tokens.length; i++) {
                if ((tokens[i].trim().startsWith("<" + conf.get("xmltag")) || tokens[i].trim().startsWith("<" + conf.get("xmltag2"))) && tokens[i].trim().endsWith("/>")) {
                    continue;
                }
                temp += tokens[i].trim() + " ";
            }

            Document d = Jsoup.parse(temp);
            Elements inputs = d.select(conf.get("xmltag"));
            for (Element el : inputs) {
                keyword.set(conf.get("v") + el.id());
                Elements children = el.children();
                boolean needsWrite = false;
                String member = "";
                for (Element child : children) {
                    Attributes attr = child.attributes();
                    //<tag k="natural" v="water"/>
                    if (child.tagName().equals("tag") && attr.get("k").equals("natural") && attr.get("v").equals(conf.get("v"))) {
                        needsWrite = true;
                    }
                    if (child.tagName().equals("member")) {
                        if (attr.hasKey("type") && attr.hasKey("ref") && attr.hasKey("role")) {
                            if (attr.get("type").equals("way")) {
                                if (attr.get("role").equals("outer")) {
                                    member += "," + attr.get("ref");
                                }
                            }
                        }
                    }
                }
                if (needsWrite && !member.isEmpty()) {
                    valueword.set(member.substring(1));
                    context.write(keyword, valueword);
                }
            }
            inputs = d.select(conf.get("xmltag2"));
            for (Element el : inputs) {
                keyword.set(el.id());
                String nds = "";
                Elements children = el.children();
                for (Element child : children) {
                    if (child.tagName().equals("nd")) {
                        Attributes attr = child.attributes();
                        nds += "," + attr.get("ref");
                    }
                }
                valueword.set(nds.substring(1));
                context.write(keyword, valueword);
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

        private HashMap<Text, Text> enclosureMap = new HashMap<>();
        private HashMap<Text, Text> wayMap = new HashMap<>();

        public void reduce(Text key, Iterable<Text> values,
                           Reducer<Text, Text, Text, Text>.Context context)
                throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            String sum = "";
            for (Text val : values) {
                sum += val.toString() + ",";
            }
            Text result = new Text();
            result.set(sum);
            if (key.toString().startsWith(conf.get("v"))) {
                enclosureMap.put(new Text(key), result);
            } else {
                wayMap.put(new Text(key), result);
            }
        }

        protected void cleanup(Reducer.Context context) throws IOException, InterruptedException{
            for (Text t : enclosureMap.keySet()) {
                boolean broken = false;
                String[] tokens = enclosureMap.get(t).toString().split(",");
                for (int i = 0; i < tokens.length; i++) {
                    Text k = new Text(tokens[i]);
                    if (wayMap.containsKey(k)) {
                        tokens[i] = wayMap.get(k).toString();
                    } else {
                        context.write(t, new Text("Way not found - " + tokens[i]));
                        broken = true;
                        break;
                    }
                }
                if (!broken) {
                    ArrayList<String[]> points = new ArrayList<>();
                    for (int i = 0; i < tokens.length; i++) {
                        String[] token = tokens[i].split(",");
                        points.add(new String[2]);
                        points.get(i)[0] = token[0];
                        points.get(i)[1] = token[token.length-1];
                    }
                    int tryNumber = 0;
                    String first = points.get(0)[0];
                    String last = points.get(0)[1];
                    points.remove(0);
                    int tryTotal = points.size();
                    while ((tryNumber < tryTotal) && points.size() > 0) {
                        tryNumber++;
                        for (int i = 0; i < points.size(); i++) {
			    			if (points.get(i)[0].equals(points.get(i)[1])) {
                                points.remove(i);
                                break;
                            }
                            if (points.get(i)[0].equals(last)) {
                                last = points.get(i)[1];
                                points.remove(i);
                                break;
                            }
                            if (points.get(i)[1].equals(first)) {
                                first = points.get(i)[0];
                                points.remove(i);
                                break;
                            }
                        }
                    }
                    if (!points.isEmpty()) {
						for (String[] p : points) {
							if (!p[0].equals(p[1])) {
								String bad = p[0] + "," + p[1];
                        		context.write(t, new Text("Extra way - " + bad));
							}
						}
                    } else if (!first.equals(last)) {
                        context.write(t, new Text("Not enclosed"));
                    }

                }
            }
        }
    }


    public static void main(String[] args) throws Exception {
        //Define a new configuration file
        Configuration conf = new Configuration();
        //<relation<
        conf.set("xmlinput.start", "<relation" + "<way");
        conf.set("xmlinput.end", "</relation" + "></way>");
        conf.set("xmltag", "relation");
        conf.set("xmltag2", "way");
        conf.set("v", args[1]);
        Job job = Job.getInstance(conf);

        job.setJarByClass(CheckInvalidEnclosure.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);
        job.setInputFormatClass(XmlInputFormat1.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path("output2"));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
