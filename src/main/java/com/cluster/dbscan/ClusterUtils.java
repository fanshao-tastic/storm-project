package com.cluster.dbscan;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Vector;

import com.entity.ClusterResultEntity;
import com.entity.MessageEntity;
import com.entity.PointCountEntity;

public class ClusterUtils {

	//地球半径
	private static double EARTH_RADIUS = 6378137;//单位:米
	/**
	 * 获取两点间地球距
	 * @param 坐标1
	 * @param 坐标2
	 * @return 两点间距
	 */
	public static double getEarthDistance(Point p,Point q) {
		double radLat1 = rad(p.x);
		double radLat2 = rad(q.x);
		double a = radLat1 - radLat2;
		double b = rad(p.y) - rad(q.y);
		
		double s = 2 * Math.asin(Math.sqrt(Math.pow(Math.sin(a/2),2) + Math.cos(radLat1)*Math.cos(radLat2)*Math.pow(Math.sin(b/2), 2)));
		s = s * EARTH_RADIUS;
//		s = Math.round(s * 10000) / 10000;
		return s;
	}
	
	private static double rad(double d) {
		return d * Math.PI / 180.0;
	}
	public static double getDistance(Point p,Point q) {
		double dx = p.x - q.x;
		double dy = p.y - q.y;
		return Math.sqrt (dx * dx + dy * dy);
	}
	
	/**
	 * 从消息实体中取出经纬度信息,并构造point实体,添加至Vector
	 * @param entityVector
	 * @return
	 */
	public static Vector<Point> getListFromMsgEntityVector(Vector<MessageEntity> entityVector) {
		Vector<Point> pointList = new Vector<Point>();
		Iterator<MessageEntity> iterator = entityVector.iterator();
		while (iterator.hasNext()) {
			MessageEntity messageEntity = (MessageEntity) iterator.next();
			pointList.add(new Point(messageEntity.getGPSLongitude(), messageEntity.getGPSLatitude()));
		}
		return pointList;
	}
	
	/**
	 * 从文件中读取坐标,返回坐标的列表
	 * @param filepath
	 * @return
	 */
	public static Vector<Point> getListFromFile(String filepath) {
		Vector<Point> pointList = new Vector<Point>();
		File file = new File(filepath);
		try {
			BufferedReader buffReader = new BufferedReader(new FileReader(file));
			String strline;
			String[] tmpLoc = new String[2];
			double tmpX;
			double tmpY;
			while ((strline=buffReader.readLine()) != null) {
				tmpLoc = strline.split(",");
				try {
					tmpX = Double.parseDouble(tmpLoc[0]);
					tmpY = Double.parseDouble(tmpLoc[1]);
					pointList.add(new Point(tmpX, tmpY));
				} catch (Exception e) {
				}		
			}
			buffReader.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return pointList;
	}
	/**
	 * 打印辅助函数
	 * @param vector
	 */
	public static void printList(Vector<Point> vector) {
		for(Point point : vector) {
			System.out.println(point.x+","+point.y);
		}
	}
	
	public static void dumpsToFile(String filePath,Vector<List> resultList) {
		try {
			BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(filePath));
			int round=1;
			for(List<Point> list : resultList) {
				bufferedWriter.write("第"+(round++)+"簇为:");
				bufferedWriter.newLine();
				for(Point p : list) {
					bufferedWriter.write(p.x+","+p.y);
					bufferedWriter.newLine();
				}
				bufferedWriter.flush();
			}
			bufferedWriter.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * 从聚类结果构造结果实体对象
	 * @param vector
	 * @return
	 */
	public static ClusterResultEntity buildClusterEntity(Vector<Vector<Point>> vector) {
		List<PointCountEntity> list = new ArrayList<PointCountEntity>();
		for(Vector<Point> v : vector) {
			Point p = v.get(0);
			PointCountEntity pointCountEntity = new PointCountEntity(p,v.size());
			list.add(pointCountEntity);
		}

		return new ClusterResultEntity(list);
	}
}
