package com.entity;

import java.util.Date;

/**
 * 该类代表持久化到数据库的实体类
 * @author Dx
 *
 */
public class ClusterResultPersistentEntity {
	
	ClusterResultEntity clusterResultEntity;
	/*
	 * GPS时间
	 */
	public Date GPSTime;

	/*
	 * 创建该条记录时间
	 */
	public Date CreateTime = new Date();
	
	public ClusterResultPersistentEntity(ClusterResultEntity entity,Date time) {
		clusterResultEntity = entity;
		GPSTime = time;
	}
	
	public ClusterResultEntity getClusterResultEntity() {
		return clusterResultEntity;
	}

	public Date getGPSTime() {
		return GPSTime;
	}

	public void setGPSTime(Date GPSTime) {
		this.GPSTime = GPSTime;
	}

	public Date getCreateTime() {
		return CreateTime;
	}
}
