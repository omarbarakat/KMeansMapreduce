import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Time;

public class KMeansPass {

	private static BufferedReader reader;
	private static final String LOCAL_DATA_PATH = "data";
	private static final String LOCAL_CENTERS_PATH = "centers";
	private static final Set<Character> DIGITS = new HashSet<Character>(Arrays.asList('0', '1', '2', '3', '4', '5', '6', '7', '8', '9', '-'));;
	
	private static ArrayList<String> readFile(String path) {
		ArrayList<String> result = new ArrayList<String>();
		BufferedReader br = null;
		try {
			String sCurrentLine;
			br = new BufferedReader(new FileReader(path));
			while ((sCurrentLine = br.readLine()) != null) {
				result.add(sCurrentLine);
			}

		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				if (br != null)
					br.close();
			} catch (IOException ex) {
				ex.printStackTrace();
			}
		}
		
		return result;
	}

	private static ArrayList<String> toStringArray(String input) {
		String[] arr = input.split(",");
		ArrayList<String> result = new ArrayList<String>();
		for (int i = 0; i < arr.length; i++)
			result.add(arr[i]);
		return result;
	}
	
	private static ArrayList<String> chooseCenters(int n, int linesNumber) {
		ArrayList<String> result = new ArrayList<String>(n);
		Set<Integer> centersIndexes = new HashSet<Integer>();
		Random r = new Random(Time.now());
		while (centersIndexes.size() < n) {
			int num = ((r.nextInt() % linesNumber) + linesNumber) % linesNumber;
			centersIndexes.add(num);
		}
		try {
			File f = new File(LOCAL_DATA_PATH);
			reader = new BufferedReader(new InputStreamReader(new FileInputStream(f)));
			int lineNumber = 0;
			String line;

			line = reader.readLine();
			while (line != null) {
				if (centersIndexes.contains(lineNumber)) {
					result.add(line);
				}
				lineNumber++;
				line = reader.readLine();
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		return result;
	}
	
	private static boolean isNominal(String s){
		return !DIGITS.contains( s.charAt(0) );
	}

	public static class Map extends Mapper<LongWritable, Text, Text, Text> {

		private ArrayList<String> centers = null;
		
		private double getDistance(List<String> data_1, List<String> data_2) {
			double euclideanDistance = 0;
			for (int i = 0; i < data_1.size(); i++){
				if(isNominal(data_1.get(i))){
					euclideanDistance = (data_1.get(i).equals(data_2.get(i)))? 
							euclideanDistance:euclideanDistance+1;
				}else{
					Double x1 = Double.parseDouble(data_1.get(i));
					Double x2 = Double.parseDouble(data_2.get(i));
					euclideanDistance += Math.pow( x1-x2 , 2);
				}
			}

			return euclideanDistance;
		}

		@Override
		public void setup(Context context) {
			centers = KMeansPass.readFile(LOCAL_CENTERS_PATH);
		}

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			ArrayList<String> data = toStringArray(value.toString());
			double minDist = Double.MAX_VALUE;
			int minIndex = -1;
			for (int j = 0; j < centers.size(); j++) {
				double dist = getDistance(data, toStringArray(centers.get(j)));
				if (dist < minDist) {
					minDist = dist;
					minIndex = j;
				}
			}

			context.write(new Text(centers.get(minIndex)), value);
		}
	}

	public static class Reduce extends Reducer<Text, Text, Text, Text> {

		private ArrayList<String> getAverage(ArrayList<ArrayList<String>> data) {
			ArrayList<String> averages = new ArrayList<String>();
			for (int i = 0; i < data.get(0).size(); i++) {
				if(isNominal(data.get(0).get(i))){
					TreeMap<String, Integer> nominalMap = new TreeMap<String, Integer>();
					for (int j = 0; j < data.size(); j++){
						int count = nominalMap.containsKey(data.get(j).get(i)) ? nominalMap.get(data.get(j).get(i)) : 0;
						nominalMap.put(data.get(j).get(i), count + 1);	// increment count
					}
					String average = "";
					int max = 0;
					for( Entry<String, Integer> entry : nominalMap.entrySet())
						if(entry.getValue()>max){
							average = entry.getKey();
							max = entry.getValue();
						}
					averages.add(average);
				}else{
					double average = 0;
					for (int j = 0; j < data.size(); j++)
						average += Double.parseDouble(data.get(j).get(i));
					average /= (double) data.size();
					averages.add(Double.toString(average));
				}
			}
			return averages;
		}
		
		private String arrayToString(ArrayList<String> input) {
			StringBuffer result = new StringBuffer(input.get(0));
			for (int i = 1; i < input.size(); i++)
				result.append( "," ).append( input.get(i) );
			return result.toString();
		}

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			ArrayList<ArrayList<String>> data = new ArrayList<ArrayList<String>>();
			
			for (Text val : values)
				data.add(toStringArray(val.toString()));
			ArrayList<String> averages = getAverage(data);
			Text center = new Text(arrayToString(averages));
			context.write(center, new Text(key));
		}
	}

	static void writeToFile(ArrayList<String> centers) {
		try {
			File file = new File(LOCAL_CENTERS_PATH);
			if (!file.exists())
				file.createNewFile();

			FileWriter fw = new FileWriter(file.getAbsoluteFile());
			BufferedWriter bw = new BufferedWriter(fw);
			for (int i = 0; i < centers.size(); i++) {
				bw.write(centers.get(i));
				bw.newLine();
			}
			bw.close();

		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public static int getNumberOfRows(String path){
		int number = 0;
		BufferedReader br = null;

		try {

			br = new BufferedReader(new FileReader(path));
			while (br.readLine() != null)
				number ++;

		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				if (br != null)br.close();
			} catch (IOException ex) {
				ex.printStackTrace();
			}
		}
		return number;
	}

	public static void main(String[] args) throws Exception {
		boolean isDone = true;
		int numIterations = 0;
		int maxIterations = 100;
		Configuration conf = new Configuration();
		int numberOfRows = getNumberOfRows(LOCAL_DATA_PATH), numberOfClusters = 3;
		writeToFile(chooseCenters(numberOfClusters, numberOfRows));
		do {
			isDone = true;
			@SuppressWarnings("deprecation")
			Job job = new Job(conf, "KMeansClustering");
			job.setJarByClass(KMeansPass.class);

			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);

			job.setMapperClass(Map.class);
			job.setReducerClass(Reduce.class);

			job.setInputFormatClass(TextInputFormat.class);
			job.setOutputFormatClass(TextOutputFormat.class);

			FileInputFormat.addInputPath(job, new Path(args[0]));
			FileOutputFormat.setOutputPath(job, new Path(args[1]));

			job.waitForCompletion(true);

			FileSystem fs = FileSystem.get(new Configuration());
			FileStatus[] status = fs.listStatus(new Path(args[1]));
			ArrayList<String> newCenters = new ArrayList<String>();
			for (int i = 0; i < status.length; i++) {
				FSDataInputStream fstream = fs.open(status[i].getPath());
				String line;
				while ((line = fstream.readLine()) != null) {
					if (!equalCenters(line.split("\t")[0], line.split("\t")[1])) {
						isDone = false;
					}
					newCenters.add(line.split("\t")[0]);
				}
			}
			if(isDone)
				System.out.println("Converged");
			numIterations++;
			if (numIterations == maxIterations)
				isDone = true;
			if (!isDone) {
				String command = "hadoop fs -rm -r -f " + args[1];
				Process delete = Runtime.getRuntime().exec(command);

				PrintWriter writer = new PrintWriter(LOCAL_CENTERS_PATH, "UTF-8");
				for (int i = 0; i < newCenters.size(); i++) {
					writer.println(newCenters.get(i));
				}
				writer.close();
				delete.waitFor();
			}

		} while (!isDone);

	}

	private static boolean equalCenters(String string, String string2) {
		double epsilon = 0.0001;
		String[] val1 = string.split(",");
		String[] val2 = string2.split(",");
		for (int i = 0; i < val1.length; i++) {
			if(!isNominal(val1[i]) && Math.abs(Double.parseDouble(val1[i]) - Double.parseDouble(val2[i])) < epsilon)
				continue;
			if(!val1[i].equals(val2[i]))
				return false;
		}
		return true;
	}
}