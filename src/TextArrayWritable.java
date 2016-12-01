import org.apache.hadoop.io.*;

import java.util.Iterator;

/**
 * Created by Tifani on 11/30/2016.
 */
public class TextArrayWritable extends ArrayWritable {
    public TextArrayWritable() {
        super(Text.class);
    }

    public TextArrayWritable(String[] strings) {
        super(Text.class);
        Text[] texts = new Text[strings.length];
        for (int i = 0; i < strings.length; i++) {
            texts[i] = new Text(strings[i]);
        }
        set(texts);
    }

    @Override
    public String toString() {
        Writable[] values = get();
        String result = "";
        if (values.length > 0) {
            result = values[0].toString();
        }
        for(int i=1; i<values.length; i++) {
            result += ", " + values[i];
        }
        return result;
    }
}