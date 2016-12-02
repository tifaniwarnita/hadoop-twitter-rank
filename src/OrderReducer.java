import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.TreeMap;

/**
 * Created by Tifani on 12/1/2016.
 */
public class OrderReducer extends Reducer<NullWritable, Text, NullWritable, Text> {
    private TreeMap<String, Text> userRank = new TreeMap<String, Text>();

    @Override
    public void reduce(NullWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        int i = 0;
        for (Text t : values) {
            context.write(NullWritable.get(), t);
//            if (i >= 5) break;
//                else i++;
        }
    }
}
