import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.nio.charset.CharacterCodingException;
import java.util.*;

/**
 * Created by Tifani on 12/1/2016.
 */
public class OrderReducer extends Reducer<NullWritable, Text, Text, Text> {
    private static ArrayList<UserRank> userRank = new ArrayList<>();

    @Override
    public void reduce(NullWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        for(Text val : values) {
            userRank.add(new UserRank(val));
            Collections.sort(userRank);
            if (userRank.size() > TwitterRank.RANK) {
                userRank.remove(userRank.size() - 1);
            }
        }

        for (int i=0; i<userRank.size(); i++) {
            context.write(new Text("Rank #" + (i+1)), new Text("\n"
                    + "   User ID    : " + userRank.get(i).userId + "\n"
                    + "   User Rank  : " + userRank.get(i).rank));
        }

    }

    class UserRank implements Comparable<UserRank> {
        double rank;
        String userId;

        public UserRank(Text text) throws CharacterCodingException {
            int tabPageIndex = text.find("\t");
            this.userId = Text.decode(text.getBytes(), 0, tabPageIndex);
            this.rank = Double.parseDouble(Text.decode(text.getBytes(), tabPageIndex + 1, text.getLength() - (tabPageIndex + 1)));
        }

        @Override
        public int compareTo(UserRank o) {
            if (this.rank > o.rank) {
                return -1;
            } else if (this.rank < o.rank) {
                return 1;
            } else {
                return 0;
            }
        }
    }

}
