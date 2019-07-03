package readParquet;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import org.apache.parquet.Log;
import org.apache.parquet.example.data.Group;

import org.apache.parquet.hadoop.ParquetInputFormat;
import org.apache.parquet.hadoop.api.DelegatingReadSupport;
import org.apache.parquet.hadoop.api.InitContext;
import org.apache.parquet.hadoop.example.GroupReadSupport;


public class ReadAlsParquet extends Configured implements Tool {
	private static final Log LOG = Log.getLog(ReadAlsParquet.class);

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

		Configuration conf = new Configuration();
		
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
		job.setNumReduceTasks(0);

		 ParquetInputFormat.setReadSupportClass(job, MyReadSupport.class);
		 
		//FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
        job.setInputFormatClass(ParquetInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        
        ParquetInputFormat.addInputPath(job, new Path(args[0]));
		

		job.waitForCompletion(true);

		return 0;
	}

	public static void main(String[] args) throws Exception {
		try {
			int res = ToolRunner.run(new Configuration(), new ReadAlsParquet(), args);
			System.exit(res);
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(255);
		}
	}
}
