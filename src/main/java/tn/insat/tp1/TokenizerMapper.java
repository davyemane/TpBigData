package tn.insat.tp1;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;
import java.util.ArrayList;
import java.util.StringTokenizer;

public class TokenizerMapper
        extends Mapper<Object, Text, Text, DoubleWritable>{

    public DoubleWritable val = new DoubleWritable();
    public ArrayList<String> data = new ArrayList<String>();
    private Text word = new Text();
    private final static IntWritable one = new IntWritable(1);

    public void map(Object key, Text value, Mapper.Context context
    ) throws IOException, InterruptedException {

        StringTokenizer itr = new StringTokenizer(value.toString());

        while(itr.hasMoreTokens()){
            data.add(itr.nextToken());
        }

        word.set(data.get(2));
        if (isDouble(data.get(3)) == false){
            word.set(word +" "+ data.get(3));
        }

        if (isDouble(data.get(4)) == false){
            word.set(word +" "+ data.get(4));
        }

        val.set(Double.parseDouble(data.get(data.size()-2)));

        context.write(word,val);

        data.clear();

    }
    public static boolean isDouble(String str) {
        if (str == null) {
            return false;
        }

        str = str.trim();

        if (str.isEmpty()) {
            return false;
        }

        for (char c : str.toCharArray()) {
            if (!Character.isDigit(c) && c != '.' && c != ',') {
                return false;
            }
        }

        return true;
    }}

