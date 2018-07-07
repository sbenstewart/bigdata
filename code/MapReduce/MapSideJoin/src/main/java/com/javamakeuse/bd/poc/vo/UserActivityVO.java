package com.javamakeuse.bd.poc.vo;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class UserActivityVO implements Writable {

	private int userId;
	private String userName;
	private String comments;
	private String postShared;

	public int getUserId() {
		return userId;
	}

	public void setUserId(int userId) {
		this.userId = userId;
	}

	public String getUserName() {
		return userName;
	}

	public void setUserName(String userName) {
		this.userName = userName;
	}

	public String getComments() {
		return comments;
	}

	public void setComments(String comments) {
		this.comments = comments;
	}

	public String getPostShared() {
		return postShared;
	}

	public void setPostShared(String postShared) {
		this.postShared = postShared;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(userId);
		out.writeUTF(userName);
		out.writeUTF(comments);
		out.writeUTF(postShared);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		userId = in.readInt();
		userName = in.readUTF();
		comments = in.readUTF();
		postShared = in.readUTF();
	}

	@Override
	public String toString() {
		return "UserActivityVO [userId=" + userId + ", userName=" + userName + ", comments=" + comments
				+ ", postShared=" + postShared + "]";
	}

}