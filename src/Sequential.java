import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintStream;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class Sequential {

	static final String INTERMEDIATE_FILE = "/home/omar/Desktop/intermediate.txt";
	static String filePath = "/home/omar/Desktop/data";
	static int clustersNumber, featuresNo, rowsNo, maxNoOfIterations;
	static String[] currentCenters, prevCenters;
	static long[] currentCentersClusters;
	private static String delimiter = ",";
	static double epsilon = 0.001;
	private static final Set<Character> DIGITS = new HashSet<Character>(Arrays.asList('0', '1', '2', '3', '4', '5', '6', '7', '8', '9', '-'));
	
	static void chooseCentersInStart() {
		BufferedReader br = null;
		try {
			String sCurrentLine;
			br = new BufferedReader(new FileReader(filePath));
			int counter = 0;
			while ((sCurrentLine = br.readLine()) != null
					&& counter < clustersNumber) {
				currentCenters[counter] = sCurrentLine;
				prevCenters[counter] = sCurrentLine;
				counter++;
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
	}

	private static int getFeaturesNo() {
		int num = 0;
		BufferedReader br = null;
		try {
			String sCurrentLine;
			br = new BufferedReader(new FileReader(filePath));
			while ((sCurrentLine = br.readLine()) != null) {
				num = sCurrentLine.split(delimiter).length + 1;
				break;
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
		return num;
	}

	static int getNumberOfRows() {
		int number = 0;
		BufferedReader br = null;
		try {
			br = new BufferedReader(new FileReader(filePath));
			while (br.readLine() != null)
				number++;

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
		return number;
	}

	private static boolean isConverged() {
		for (int i = 0; i < currentCenters.length; i++)
			if (getDistance(currentCenters[i], prevCenters[i]) > epsilon)
				return false;

		return true;
	}
	
	private static boolean isNominal(String s){
		return !DIGITS.contains( s.charAt(0) );
	}

	private static double getDistance(String val1, String val2) {
		double dist = 0;
		String[] d_1 = val1.split(delimiter);
		String[] d_2 = val2.split(delimiter);

		for (int i = 0; i < d_1.length; i++) {
			if(isNominal(d_1[i])){
				dist = (d_1[i].equals(d_2[i]))?	dist : dist+1;		// increment if different
			}else{
				Double x1 = Double.parseDouble(d_1[i]);
				Double x2 = Double.parseDouble(d_2[i]);
				dist += Math.pow( x1-x2 , 2);
			}
		}
		return dist;
	}
	
	
	private static void setLabels() {
		File file = new File(INTERMEDIATE_FILE);
		file.delete();
		BufferedReader br = null;
		PrintStream fw = null;
		try {
			String sCurrentLine;
			br = new BufferedReader(new FileReader(filePath));
			if (!file.exists())
				file.createNewFile();
			
			fw=new PrintStream(file);  
			
			while ((sCurrentLine = br.readLine()) != null) {
				double minDistance = Double.MAX_VALUE;
				int minIndex = -1;
				for (int i = 0; i < currentCenters.length; i++) {
					double dist = getDistance(currentCenters[i], sCurrentLine);
					if (dist < minDistance) {
						minDistance = dist;
						minIndex = i;
					}
				}
				fw.println(new String(
						currentCenters[minIndex] + "\t" + sCurrentLine));
			}
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				if (br != null)
					br.close();
				if (fw != null)
					fw.close();
			} catch (IOException ex) {
				ex.printStackTrace();
			}
		}
	}

	private static void refreshCentroids() {
		BufferedReader br = null;
		try {
			String sCurrentLine;
			br = new BufferedReader(new FileReader(INTERMEDIATE_FILE));
			while ((sCurrentLine = br.readLine()) != null) {
				String[] arr = sCurrentLine.split("\t");
				int centerIndex = getCenterIndex(arr[0]);
				addPoint(centerIndex, arr[1]);
				currentCentersClusters[centerIndex] ++;
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
	}

	private static void addPoint(int centerIndex, String value) {
		String result = "";
		String[] centerArr = currentCenters[centerIndex].split(delimiter) ;
		String[] valueArr = value.split(delimiter) ;
		result = Double.toString(Double.parseDouble(centerArr[0]) + Double.parseDouble(valueArr[0]));
		for (int i = 1; i < centerArr.length; i++) 
			result += delimiter + Double.toString(Double.parseDouble(centerArr[i]) + Double.parseDouble(valueArr[i]));  
		
		currentCenters[centerIndex] = result;
	}

	private static int getCenterIndex(String center) {
		for (int i = 0; i < prevCenters.length; i++) 
			if(center.equals(prevCenters[i]))
				return i;
		
		return -1;
	}

	private static void normalizeCentroids() {
		for (int i = 0; i < currentCenters.length; i++) {
			String arr[] = currentCenters[i].split(delimiter);
			String result = Double.toString(Double.parseDouble(arr[0]) / currentCentersClusters[i]);
			for (int j = 1; j < arr.length; j++) 
				result += "," + Double.toString(Double.parseDouble(arr[j]) / currentCentersClusters[i]);
			currentCenters[i] = result;
		}
	}

	public static void main(String[] args) {
		clustersNumber = 7;
		maxNoOfIterations = 10;
		currentCenters = new String[clustersNumber];
		currentCentersClusters = new long[clustersNumber];
		prevCenters = new String[clustersNumber];

		chooseCentersInStart();
		featuresNo = getFeaturesNo();
		rowsNo = getNumberOfRows();
		int iterationsCounter = 0;
		do {
			
			System.out.println(iterationsCounter);
			for (int i = 0; i < prevCenters.length; i++)
				prevCenters[i] = currentCenters[i];
			for (int i = 0; i < currentCentersClusters.length; i++)
				currentCentersClusters[i] = 0;
			iterationsCounter++;

			System.out.println("done copying prevCenters");
			setLabels();
			System.out.println("done classifying points");
			refreshCentroids();
			System.out.println("done updating centroids");
			normalizeCentroids();
			
		}while(!isConverged() && iterationsCounter < maxNoOfIterations);
		
		for (int i = 0; i < currentCentersClusters.length; i++)
			System.out.println(currentCenters[i]);
	}
}
