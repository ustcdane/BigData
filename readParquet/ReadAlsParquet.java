package readParquet;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import org.apache.parquet.Log;
import org.apache.parquet.example.data.Group;

import org.apache.parquet.hadoop.ParquetInputFormat;
import org.apache.parquet.hadoop.api.DelegatingReadSupport;
import org.apache.parquet.hadoop.api.InitContext;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

class Hadoop2Util {
	
	private static Configuration conf=null;
	
	private static final String YARN_RESOURCE="10.129.250.52:8032";
	private static final String DEFAULT_FS="hdfs://10.129.250.52:9000";
	
	public static Configuration getConf(){
		if(conf==null){
			conf = new YarnConfiguration();
			//conf = new Configuration();
			/*conf.set("fs.defaultFS", DEFAULT_FS);
			conf.set("mapreduce.framework.name", "yarn");
			conf.set("yarn.resourcemanager.address", YARN_RESOURCE);
			*/
		}
		return conf;
	}
}

public class ReadAlsParquet extends Configured implements Tool {
	private static final Logger LOG = LoggerFactory.getLogger(ReadAlsParquet.class);//Log.getLog(ReadAlsParquet.class);

	/*
	 * Read a Parquet record, write a CSV record
	 */
	public static class ReadRequestMap extends Mapper<Void, Group, Text, Text> {

		@Override
		public void map(Void _key, Group value, Context context) throws IOException, InterruptedException {
			
			List<String> list = new ArrayList<>();
			String id = Integer.toString(value.getInteger(0, 0));
			String recRes = value.getString(1,0);
			recRes = recRes.replace("[", "");
			recRes = recRes.replace("]", "");
			String[] segs = recRes.split(",");
			for( String item:segs) {
				String[] wc = item.split("#");
				if(wc.length == 2) {
					String rec = new String(wc[0] + "#ALS#" + wc[1]);
					list.add(rec);
				}
			}
			context.write(new Text(id), new Text(String.join("\t",list)));
		}
	}
	
	public static class NormalReducer extends Reducer<Text, Text, Text, Text> {

        public void reduce(Text _key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
           
            List<String> list = new ArrayList<>();
        	for (Text text : values) {
                //context.write(_key, text);
        		list.add(text.toString());
            }
            context.write(_key, new Text(String.join("\t",list)));
        }
    }

	public static final class MyReadSupport extends DelegatingReadSupport<Group> {
        public MyReadSupport() {
            super(new GroupReadSupport());
        }
 
        @Override
        public org.apache.parquet.hadoop.api.ReadSupport.ReadContext init(InitContext context) {
            return super.init(context);
        }
    }
	
	public int run(String[] args) throws Exception {

		Configuration conf = config();//new Configuration();
		
		/*
		String readSchema = "message example {\n" +
                "required int32 userId;\n" +
                "required binary rec;\n" +
                "}";
        conf.set(ReadSupport.PARQUET_READ_SCHEMA, readSchema);
		 */
		@SuppressWarnings("deprecation")
		Job job = new Job(conf);
		job.setJarByClass(getClass());
		job.setJobName(getClass().getName()  + "_parquet_wangdan");

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setMapperClass(ReadRequestMap.class);
		job.setNumReduceTasks(300);
		job.setReducerClass(NormalReducer.class);
		 ParquetInputFormat.setReadSupportClass(job, MyReadSupport.class);
		 
		//FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
        job.setInputFormatClass(ParquetInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        
        ParquetInputFormat.addInputPath(job, new Path(args[0]));
		

		job.waitForCompletion(true);

		return 0;
	}  
	
	public static Configuration  config() {
		Configuration conf= Hadoop2Util.getConf();
	      return conf;
	  }
	
	

	public static void main(String[] args) throws Exception {
		try {
				Configuration conf = config();
				LOG.info("args is:\n" + StringUtils.join(args, " "));
				LOG.info("config is:\n");
				/*for (Entry<String, String> entry : conf) {
					LOG.info("%s=%s\n", entry.getKey(), entry.getValue());
				}*/
			int res = ToolRunner.run(conf, new ReadAlsParquet(), args);
			System.exit(res);
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(255);
		}
	}
}
