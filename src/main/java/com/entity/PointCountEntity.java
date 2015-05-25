package com.entity;

import com.cluster.dbscan.Point;

public class PointCountEntity {

	public double lng;
	
	public double lat;
	
	public int count;

	public PointCountEntity(Point point,int count) {
		lng = point.getX();
		lat = point.getY();
		this.count = count;
	}
	
	
}
