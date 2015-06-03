package com.cluster.dbscan;

import java.util.Iterator;
import java.util.List;
import java.util.Vector;

import com.entity.MessageEntity;

/**
 * DBSCAN聚类算法入口
 * 该类用于对车辆GPS点集进行聚类分析
 * TODO:考虑构建R-tree和并行DBSCAN优化聚类速度
 * @author Dx
 *
 */
public class DBSCAN {
	/**
	 * 簇扫描半径
	 */
	public double eps;
	/**
	 * 最小包含点数阈值
	 */
	public int minPts;
	/**
	 * 所有坐标点
	 */
	public Vector<Point> pointList;
	/**
	 * 聚类结果
	 */
	public Vector<Vector<Point>> resultList = new Vector<Vector<Point>>();
	/**
	 * E领域集合
	 */
	public Vector<Point> neighbours;
	/**
	 * 噪声数据
	 */
	public Vector<Point> noisePoint = new Vector<Point>();
	
	public DBSCAN(Vector<MessageEntity> messageEntities,double eps,int minPts) {
		this.eps = eps;
		this.minPts = minPts;
		pointList = ClusterUtils.getListFromMsgEntityVector(messageEntities);
	}
	
	/**
	 * 入口函数
	 * @return 聚类结果
	 */
	public Vector<Vector<Point>> applyDBSCAN() {
		for(int i=0;i<pointList.size();i++) {
			Point p = pointList.get(i);
			if(!p.isVisited) {
				p.isVisited = true;
				
				neighbours = getNeighbours(p);
				if(neighbours.size() >= minPts) {//满足最小点数
					
					//expandCluster
					for(int j=0;j<neighbours.size();j++) {
						Point q = neighbours.get(j);
						if(!q.isVisited) {
							q.isVisited = true;
							
							Vector<Point> neighbours2 = getNeighbours(q);
							
							if(neighbours2.size() >= minPts) {
								neighbours = Merge(neighbours, neighbours2);
							}
						}
					}
					resultList.add(neighbours);
					
				} else { //否则标记为噪声
					noisePoint.add(p);
				}
			}
		}
		return resultList;
	}
	
	/**
	 * 获取p的E领域(到p的距离<=e)
	 * @param p
	 * @return
	 */
	private Vector<Point> getNeighbours(Point p) {
		Vector<Point> neighbours = new Vector<Point>();
		Iterator<Point> pointsIterator = pointList.iterator();
		while (pointsIterator.hasNext()) {
			Point q = pointsIterator.next();
			if(ClusterUtils.getEarthDistance(p, q) <= eps) {
				neighbours.add(q);
			}
		}
		
		return neighbours;
	}
	
	/**
	 * 合并两个簇(a,b的并集)
	 * @param a
	 * @param b
	 * @return
	 */
	private static Vector<Point> Merge(Vector<Point> a,Vector<Point> b) {
		Iterator<Point> iterator = b.iterator();
		while (iterator.hasNext()) {
			Point point = iterator.next();
			if(!a.contains(point)) {
				a.add(point);
			}
		}
		return a;
	}
}
