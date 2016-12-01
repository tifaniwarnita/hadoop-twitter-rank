import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by Tifani on 12/1/2016.
 */
public class RankCalcMapper extends Mapper<LongWritable, Text, Text, Text> {

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        int userTabIndex = value.find("\t");
        int rankTabIndex = value.find("\t", userTabIndex+1);

        String page = Text.decode(value.getBytes(), 0, userTabIndex);
        String userWithRank = Text.decode(value.getBytes(), 0, rankTabIndex+1);

        // Mark page as an existing user
        context.write(new Text(page), new Text("!"));

        // Skip user with no followers
        if(rankTabIndex == -1) return;

        String users = Text.decode(value.getBytes(), rankTabIndex+1, value.getLength()-(rankTabIndex+1));
        String[] allOtherUsers = users.split(",");
        int totalLinks = allOtherUsers.length;

        for (String otherUser : allOtherUsers){
            Text userRankTotalLinks = new Text(userWithRank + totalLinks);
            context.write(new Text(otherUser), userRankTotalLinks);
        }

        // Put the original links of the page for the reduce output
        context.write(new Text(page), new Text("|"+users));
    }
}
