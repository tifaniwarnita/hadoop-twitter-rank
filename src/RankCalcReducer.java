import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by Tifani on 12/1/2016.
 */
public class RankCalcReducer extends Reducer<Text, Text, Text, Text> {
    private static final float damping = 0.85F;

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        boolean isExistingUser = false;
        String[] split;
        float sumShareOtherUserRanks = 0;
        String links = "";
        String userWithRank;

        // For each otherUser:
        // - check control characters
        // - calculate pageRank share <rank> / count(<links>)
        // - add the share to sumShareOtherUserRanks
        for(Text val : values) {
            userWithRank = val.toString();
            if(userWithRank.equals("!")) {
                isExistingUser = true;
                continue;
            }

            if(userWithRank.startsWith("|")){
                links = "\t"+userWithRank.substring(1);
                continue;
            }

            split = userWithRank.split("\\t");

            float pageRank = Float.valueOf(split[0]);
            int countOutLinks = Integer.valueOf(split[1]);

            sumShareOtherUserRanks += (pageRank/countOutLinks);
        }

        if(!isExistingUser) return;
        float newRank = damping * sumShareOtherUserRanks + (1-damping);

        context.write(key, new Text(newRank + links));
    }
}
