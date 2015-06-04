package com.entity;

import java.io.Serializable;

import com.cluster.dbscan.Point;
/**
 * 该类用于定义聚类结果中,每一个“簇”的数据结构
 * 类型名不允许改动!(最终会转化为Json字符串,传递给前端并调用百度地图进行显示)
 * @author Dx
 *
 */
public class PointCountEntity implements Serializable{

	/**
	 * 该簇中心点的经度
	 */
	public double lng;
	/**
	 * 该簇中心点的纬度
	 */
	public double lat;
	/**
	 * 该簇中所有点的数量
	 */
	public int count;

	public PointCountEntity(Point point,int count) {
		lng = point.getX();
		lat = point.getY();
		this.count = count;
	}
	
	
}
