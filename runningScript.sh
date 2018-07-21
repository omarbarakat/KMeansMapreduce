for var1 in 320 640 1280 2560
do
	for var2 in 1
	do
		echo $var1 $var2 >> /home/hduser/output.txt
		{ time java -jar /home/omar/Desktop/Main.jar $var1 $var2 $var1 $var2 ; } 2>> /home/hduser/output.txt
	done
done
