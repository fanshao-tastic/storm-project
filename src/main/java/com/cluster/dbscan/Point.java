package com.cluster.dbscan;

public class Point {
	/**
	 * 经度
	 */
	public double x;
	/**
	 * 纬度
	 */
	public double y;
	/**
	 * 该元素是否被访问
	 */
	boolean isVisited;
	
	public Point(double x,double y) {
		this.x = x;
		this.y = y;
		isVisited = false;
	}
	
	public double getX() {
		return x;
	}
	
	public double getY() {
		return y;
	}
	
}
