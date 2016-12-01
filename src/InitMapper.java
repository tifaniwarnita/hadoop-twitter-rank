import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

/**
 * Created by Tifani on 11/30/2016.
 */
public class InitMapper extends Mapper<Object, Text, Text, Text> {
    private Text id = new Text();
    private Text folower = new Text();

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        StringTokenizer itr = new StringTokenizer(value.toString());
        while (itr.hasMoreTokens()) {
            id.set(String.valueOf(itr.nextToken()));
            if (itr.hasMoreTokens()) {
                folower.set(String.valueOf(itr.nextToken()));
                context.write(folower, id);
            }
        }
    }
}
