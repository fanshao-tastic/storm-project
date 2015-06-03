package com.entity;

import java.util.Date;
import java.util.List;

/**
 * 该类为聚类后的结果的实体类
 * @author Dx
 *
 */
public class ClusterResultEntity {

	/**
	 * 聚类结果
	 */
	public List<PointCountEntity> ClusterResult;
	
	public ClusterResultEntity(List<PointCountEntity> result) {
		ClusterResult = result;
	}
	
	public List<PointCountEntity> getClusterResult() {
		return ClusterResult;
	}
	
}
