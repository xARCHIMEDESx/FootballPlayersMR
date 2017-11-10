package nationalityMR;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class NationalityCountMapper extends Mapper<Object, Text, Text, IntWritable> {

    private final static IntWritable one = new IntWritable(1);

    @Override
    public void map(Object key, Text value, Context output) throws IOException, InterruptedException {
    	if(value.toString().equals("name,club,age,position,position_cat,market_value,page_views,fpl_value,fpl_sel,fpl_points,"
    			+ "region,nationality,new_foreign,age_cat,club_id,big_club,new_signing")){return;}
       
    	String[] words = value.toString().split(",");
        output.write(new Text(words[11]), one);
    }
}
