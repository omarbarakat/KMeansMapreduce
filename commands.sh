/usr/local/hadoop/sbin/stop-all.sh
sudo nano /usr/local/hadoop/etc/hadoop/hdfs-site.xml
/usr/local/hadoop/sbin/start-all.sh
/usr/local/hadoop/bin/hadoop dfsadmin -safemode leave
/usr/local/hadoop/bin/hadoop fs -rm /usr/hduser/input_p1/ratings.dat
/usr/local/hadoop/bin/hadoop fs -copyFromLocal /home/omar/ratings.dat  /usr/hduser/input_p1
time java -jar /home/omar/Desktop/Main.jar 100 100 100 100
