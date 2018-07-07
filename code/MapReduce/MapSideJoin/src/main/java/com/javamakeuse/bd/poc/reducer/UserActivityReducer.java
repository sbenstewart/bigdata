package com.javamakeuse.bd.poc.reducer;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import com.javamakeuse.bd.poc.vo.UserActivityVO;

public class UserActivityReducer extends Reducer<IntWritable, UserActivityVO, UserActivityVO, NullWritable> {

	NullWritable value = NullWritable.get();

	@Override
	protected void reduce(IntWritable key, Iterable<UserActivityVO> values,
			Reducer<IntWritable, UserActivityVO, UserActivityVO, NullWritable>.Context context)
					throws IOException, InterruptedException {
		for (UserActivityVO userActivityVO : values) {
			context.write(userActivityVO, value);
		}
	}
}
