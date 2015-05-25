create table if not exists ClusterResult ( 
ID int(12) not null auto_increment, 
GPSTime timestamp(14), 
CreateTime timestamp(14), 
ClusterResult varchar(4000), 
primary key(ID));

create table if not exists CarGPSLog (
ID int(11),
CompanyID varchar(50),
VehicleSimID varchar(50),
GPSTime timestamp(14),
GPSLongitude double,
GPSLatitude double,
GPSSpeed double,
GPSDirection double,
PassengerState int(11),
ReadFlag int(11),
CreateDate timestamp(14)
);