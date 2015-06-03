#!/usr/bin/env python


import os
import subprocess
import sys
from optparse import OptionParser
import ConfigParser
import PropertiesHelper
import commands



def main():
    usage = "Usage: %prog [options] --start or --stop"
    parser = OptionParser(usage)
    parser.add_option('-d','--debug',action='store_true',dest='debug',default=False,help='whether to open stdout console message')
    parser.add_option('--start',action="store_true",dest="status",help="start the project")
    parser.add_option('--stop',action="store_false",dest="status",help="stop the project")
    (options,args) = parser.parse_args(sys.argv)
    if len(sys.argv) < 2:
        parser.error('incorrect number of arguments')
        sys.exit()
    if options.status: #start
        start(options)
    else: #stop
        stop()
        
def start(options):
    if os.path.exists(os.path.join(sys.path[0],'pid.lock')):
        print "Task already started! Please stop it first!"
    else:
        print "=======Starting the project======="
        print "Checking whether the kafka topic is created..."
        # read config/conf.properties
        print "read the config/conf.properties file..."
        properties = PropertiesHelper.Properties()
        properties.load(open('config/conf.properties'))
        kafkaTopic = properties['topic']
        kafkaBroker = properties['kafkaBrokeList'].split(':')[0]
        zookeeper = properties['zkHosts']
        sourceTableName = properties['sourceTableName']
        cmd = 'ssh '+kafkaBroker+' "/usr/hdp/current/kafka-broker/bin/kafka-topics.sh --list --zookeeper '+zookeeper+'" | grep '+kafkaTopic
        printCmd(cmd)
        (status,output) = commands.getstatusoutput(cmd)
        if (status == 0) and (output == kafkaTopic):
            print "kafka topic:"+kafkaTopic+" is already created! Going to launch the project!"
        else:
            print "kafka topic isn't created , going to create the topic:"+kafkaTopic+"..."
            cmd = 'ssh '+kafkaBroker+' "/usr/hdp/current/kafka-broker/bin/kafka-topics.sh --create --zookeeper '+zookeeper+' --replication-factor 1 --partitions 3 --topic '+kafkaTopic+'"'
            (status,output) = commands.getstatusoutput(cmd)
            printCmd(cmd)
            print output
            if (status == 0):
                print "kafka topic create successful!"
            else:
                print "kafka topic create failed!"
                sys.exit()
        # define the shell cmd!!
        kafkaCmd = "java -cp StormProject-1.0-SNAPSHOT-jar-with-dependencies.jar com.kafka.ProducerMain "+sourceTableName
        stormCmd = "storm jar StormProject-1.0-SNAPSHOT-jar-with-dependencies.jar com.storm.Main"
        if (options.debug):
            kafkaProducerProcess = subprocess.Popen(kafkaCmd,shell=True)
            stormMainProcess = subprocess.Popen(stormCmd,shell=True)
        else:
            kafkaProducerProcess = subprocess.Popen(kafkaCmd,stdout=open('/dev/null','w'),shell=True)
            stormMainProcess = subprocess.Popen(stormCmd,stdout=open('/dev/null','w'),shell=True)
        printCmd(kafkaCmd)
        printCmd(stormCmd)
        pidLockFile = ConfigParser.ConfigParser()
        pidLockFile.add_section('Exist Task Pid')
        pidLockFile.set('Exist Task Pid','kafkaProducerProcess',kafkaProducerProcess.pid)
        pidLockFile.set('Exist Task Pid','stormMainProcess',stormMainProcess.pid)
        with open('pid.lock','wb') as lockfile:
            pidLockFile.write(lockfile)
        print "the child process is " + str(kafkaProducerProcess.pid)
        print "the child process is " + str(stormMainProcess.pid)
        print "Projece start successful!"
    
def stop():
    lockFilePath = os.path.join(sys.path[0],'pid.lock')
    if os.path.exists(lockFilePath):
        pidLockFile = ConfigParser.ConfigParser()
        pidLockFile.read('pid.lock')
        kafkaProducerProcessPid = pidLockFile.get('Exist Task Pid','kafkaProducerProcess')
        stormMainProcessPid = pidLockFile.get('Exist Task Pid','stormMainProcess')
        kafkaCmd = "kill -9 "+str(kafkaProducerProcessPid)
        stormCmd = "kill -9 "+str(stormMainProcessPid)
        subprocess.call(kafkaCmd,shell=True)
        subprocess.call(stormCmd,shell=True)
        os.remove(lockFilePath)
        print "Task stop successful!"
        
def printCmd(cmd):
    print "execute command : ["+cmd+"]"
    

if __name__ == '__main__':
    main()
