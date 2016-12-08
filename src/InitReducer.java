import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by Tifani on 11/30/2016.
 */
public class InitReducer extends Reducer<Text, Text, Text, Text> {
    private final static String initPageRank = "1.0";
    private StringBuilder sb = new StringBuilder();

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        StringBuilder sb = new StringBuilder();
        sb.append(initPageRank);
        sb.append("\t");

        boolean first = true;
        for (Text val : values) {
            if (!val.toString().equals("*")) {
                if (!first) {
                    sb.append(",");
                }
                first = false;
                sb.append(val.toString());
            }
        }
        if (first) {
            sb = new StringBuilder();
            sb.append(initPageRank);
        }

        context.write(key, new Text(sb.toString()));
    }
}
