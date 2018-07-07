package com.javamakeuse.bd.poc.mapper;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.javamakeuse.bd.poc.vo.UserActivityVO;

public class UserActivityMapper extends Mapper<LongWritable, Text, IntWritable, UserActivityVO> {

	// user map to keep the userId-userName
	private Map<Integer, String> userMap = new HashMap<>();

	@Override
	protected void map(LongWritable key, Text value,
			Mapper<LongWritable, Text, IntWritable, UserActivityVO>.Context context)
					throws IOException, InterruptedException {

		String[] columns = value.toString().split("\t");
		if (columns != null && columns.length > 2) {
			UserActivityVO userActivityVO = new UserActivityVO();
			userActivityVO.setUserId(Integer.parseInt(columns[1]));
			userActivityVO.setComments(columns[2]);
			userActivityVO.setPostShared(columns[3]);
			userActivityVO.setUserName(userMap.get(userActivityVO.getUserId()));
			// writing into context
			context.write(new IntWritable(userActivityVO.getUserId()), userActivityVO);
		}
	}

	@Override
	protected void setup(Mapper<LongWritable, Text, IntWritable, UserActivityVO>.Context context)
			throws IOException, InterruptedException {
		// loading user map in context
		loadUserInMemory(context);
	}

	private void loadUserInMemory(Mapper<LongWritable, Text, IntWritable, UserActivityVO>.Context context) {
		// user.log is in distributed cache
		try (BufferedReader br = new BufferedReader(new FileReader("user.log"))) {
			String line;
			while ((line = br.readLine()) != null) {
				String columns[] = line.split("\t");
				userMap.put(Integer.parseInt(columns[0]), columns[1]);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}

	}
}
