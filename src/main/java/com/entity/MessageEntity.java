package com.entity;

import java.util.Date;

/**
 * 该类用于描述消息实体的数据结构
 * @author Dx
 *
 */
public class MessageEntity {

	/**
	 * 该条信息ID
	 */
	public int ID;
	/**
	 * 车辆所属公司
	 */
	public String CompanyID;
	/**
	 * 车辆编号(唯一)
	 */
	public String VehicleSimID;
	/**
	 * GPS接收时间
	 */
	public Date GPSTime;
	/**
	 * GPS经度
	 */
	public double GPSLongitude;
	/**
	 * GPS纬度
	 */
	public double GPSLatitude;
	/**
	 * GPS速度
	 */
	public int GPSSpeed;
	/**
	 * GPS方向角度
	 */
	public int GPSDirection;
	/**
	 * 乘客状态
	 */
	public int PassengerState;
	
	public int ReadFlag;
	/**
	 * GPS信息创建时间
	 */
	public Date CreateDate;

	public MessageEntity(int ID, String CompanyID, String VehicleSimID, Date GPSTime,
			double GPSLongitude, double GPSLatitude, int GPSSpeed,
			int GPSDirection, int PassengerState, int ReadFlag, Date CreateDate) {
		this.ID = ID;
		this.CompanyID = CompanyID;
		this.VehicleSimID = VehicleSimID;
		this.GPSTime = GPSTime;
		this.GPSLongitude = GPSLongitude;
		this.GPSLatitude = GPSLatitude;
		this.GPSSpeed = GPSSpeed;
		this.GPSDirection = GPSDirection;
		this.PassengerState = PassengerState;
		this.ReadFlag = ReadFlag;
		this.CreateDate = CreateDate;
	}

	public double getGPSLongitude() {
		return GPSLongitude;
	}

	public double getGPSLatitude() {
		return GPSLatitude;
	}

	public Date getGPSTime() {
		return GPSTime;
	}

	@Override
	public String toString() {
		return ID+","+CompanyID+","+VehicleSimID+","+GPSTime+","+GPSLongitude+","+GPSLatitude
				+","+GPSSpeed+","+GPSDirection+","+PassengerState+","+ReadFlag+","+CreateDate;
	}
}
