sudo cp /home/omar/Desktop/data /home/hduser/data
/usr/local/hadoop/bin/hadoop fs -rm /usr/hduser/input_p1/data
/usr/local/hadoop/bin/hadoop fs -copyFromLocal /home/hduser/data /usr/hduser/input_p1/data
sudo rm /home/hduser/centers
/usr/local/hadoop/bin/hadoop fs -rmr /usr/hduser/output_p1
/usr/local/hadoop/bin/hadoop jar /home/omar/Desktop/KMeansPass.jar KMeansPass /usr/hduser/input_p1/data /usr/hduser/output_p1
