import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by Tifani on 11/30/2016.
 */
public class UserWritable implements WritableComparable<UserWritable> {
    private long id;
    private double pageRank = 1;
    private long following;

    @Override
    public int compareTo(UserWritable o) {
        long thisValue = this.id;
        long thatValue = o.getId();
        return (thisValue < thatValue ? -1 : (thisValue==thatValue ? 0 : 1));
    }

    public UserWritable(long id, double pageRank, long following) {
        this.id = id;
        this.pageRank = pageRank;
        this.following = following;
    }

    public long getId() {
        return id;
    }

    public double getPageRank() {
        return pageRank;
    }

    public long getFollowing() {
        return following;
    }

    public void setId(long id) {
        this.id = id;
    }

    public void setPageRank(double pageRank) {
        this.pageRank = pageRank;
    }

    public void setFollowing(long following) {
        this.following = following;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeLong(id);
        dataOutput.writeDouble(pageRank);
        dataOutput.writeLong(following);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        id = dataInput.readLong();
        pageRank = dataInput.readDouble();
        following = dataInput.readLong();
    }

    @Override
    public String toString() {
        String result = "id: " + id
                + " | rank: " + pageRank
                + " | following: " + following;
        return super.toString();
    }
}
