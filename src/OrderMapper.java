import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.nio.charset.CharacterCodingException;

/**
 * Created by Tifani on 12/1/2016.
 */
public class OrderMapper extends Mapper<LongWritable, Text, NullWritable, Text> {

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] userAndRank = getUserAndRank(value);
        context.write(NullWritable.get(), new Text(userAndRank[0] + "\t" + userAndRank[1]));
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
