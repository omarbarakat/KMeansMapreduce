import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

public class Main {

	public static void main(String[] args) throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		String hadoopPath = "/usr/local/hadoop/bin/hadoop";
		String averageJar = "/home/omar/Desktop/average.jar";
		String binningJar = "/home/omar/Desktop/Binning.jar";
		String ratingsFile = "/usr/hduser/input_p1/ratings.dat";
		String averageRatingsFile = "/usr/hduser/output_p1";
		String binsFile = "/usr/hduser/output_p2";
		int numMapsP1 = Integer.parseInt(args[0]);
		int numRedsP1 = Integer.parseInt(args[1]);
		int numMapsP2 = Integer.parseInt(args[2]);
		int numRedsP2 = Integer.parseInt(args[3]);

		Process p1 = Runtime.getRuntime().exec(hadoopPath+" fs -rm -f -r "+binsFile);
		p1.waitFor();
		Process p2 = Runtime.getRuntime().exec(hadoopPath+" fs -rm -f -r "+averageRatingsFile);
		p2.waitFor();

		String cmdP1 = hadoopPath+" jar "+averageJar+" Average "+
				"-Dmapred.map.tasks = "+numMapsP1+"-Dmapred.reduce.tasks = "+numRedsP1;
		System.out.println("Excuting: " + cmdP1);
		Process p3 = Runtime.getRuntime().exec(cmdP1);
		p3.waitFor();
		
		String cmdP2 = hadoopPath+" jar "+binningJar+" Binning "+
				"-Dmapred.map.tasks = "+numMapsP2+"-Dmapred.reduce.tasks = "+numRedsP2;
		System.out.println("Excuting: " + cmdP2);
		Process p4 = Runtime.getRuntime().exec(cmdP2);
		p4.waitFor();
		
		Process p5 = Runtime.getRuntime().exec(hadoopPath+" fs -cat "+binsFile+"/part-r-00000");
		p5.waitFor();
		
		printFromStream(p5.getInputStream());
	}

	
	static void printFromStream(InputStream is) throws IOException{
		String line;
		BufferedReader input = new BufferedReader(new InputStreamReader(is));
		while ((line = input.readLine()) != null) 
		{
			System.out.println(line);
		}
		input.close();
	}
}