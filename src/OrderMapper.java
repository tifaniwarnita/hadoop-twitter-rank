import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.nio.charset.CharacterCodingException;
import java.util.Map;
import java.util.TreeMap;

/**
 * Created by Tifani on 12/1/2016.
 */
public class OrderMapper extends Mapper<LongWritable, Text, NullWritable, Text> {
    private TreeMap<String, String> userRank = new TreeMap<String, String>();

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] userAndRank = getUserAndRank(value);

        double parseRank = Double.parseDouble(userAndRank[1]);
        String result = userAndRank[1] + "\t" + userAndRank[0];
        userRank.put(userAndRank[0], result);

        if (userRank.size() > TwitterRank.RANK) {
            userRank.remove(userRank.firstKey());
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        for (String result : userRank.values()) {
            context.write(NullWritable.get(), new Text(result));
        }
    }

    private String[] getUserAndRank(Text value) throws CharacterCodingException {
        String[] userAndRank = new String[2];
        int tabPageIndex = value.find("\t");
        int tabRankIndex = value.find("\t", tabPageIndex + 1);

        // no tab after rank (when there are no links)
        int end;
        if (tabRankIndex == -1) {
            end = value.getLength() - (tabPageIndex + 1);
        } else {
            end = tabRankIndex - (tabPageIndex + 1);
        }

        userAndRank[0] = Text.decode(value.getBytes(), 0, tabPageIndex);
        userAndRank[1] = Text.decode(value.getBytes(), tabPageIndex + 1, end);

        return userAndRank;
    }

}
