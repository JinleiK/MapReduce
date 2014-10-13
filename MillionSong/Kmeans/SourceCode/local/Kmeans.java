package kmeans;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class Kmeans {
	
	private static final int DIMENSION = 5;
	private static int CLUSTER_NUM = 2;
	private static final String SPLIT = ",";
	private static double[][] curCentroids;
	private static double[][] updatedCentroids;
	private static final double STOP_EPOCHS = 0.0001;
	private static List<double[]> points = new ArrayList<double[]>();
	
	//read all the points from the file
	@SuppressWarnings("resource")
	public static void initPoints(String filename){
		try {
			BufferedReader br = new BufferedReader(new FileReader(filename));
			String line = null;
			while((line = br.readLine()) != null){
				points.add(stringToDoubles(line));
			}
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public static double[] stringToDoubles(String s) {
		String[] doubleString = s.split(SPLIT);
		// int l = doubleString.length;
		double[] doubles = new double[DIMENSION + 1];
		for (int i = 0; i < DIMENSION; i++) {
			doubles[i] = Double.parseDouble(doubleString[i]);
		}
		return doubles;
	}
	
	public static void initCentroids(){
		curCentroids = new double[CLUSTER_NUM][DIMENSION];
		updatedCentroids = new double[CLUSTER_NUM][DIMENSION];
		for(int i = 0; i < CLUSTER_NUM; i ++){
			updatedCentroids[i] = points.get(i);
			Arrays.fill(curCentroids[i], 0.0);
		}

	}
	
	public static boolean isConverge(){
		double dist;
		for(int i = 0; i < CLUSTER_NUM; i ++){
			dist = distance(curCentroids[i], updatedCentroids[i]);
			if(dist > STOP_EPOCHS){
				return false;
			}
		}
		return true;
	}
	
	public static double distance(double[] p1, double[] p2){
		double sum = 0.0;
		for(int i = 0; i < DIMENSION; i ++){
			sum += Math.pow(Math.abs(p1[i] - p2[i]), DIMENSION);
		}
		return Math.pow(sum, 1d / DIMENSION);
	}
	
	public static void updateCentroids(){
		for(int i = 0; i < CLUSTER_NUM; i ++){
			for(int j = 0; j < DIMENSION; j ++){
				curCentroids[i][j] = updatedCentroids[i][j];
			}
			
		}
	}
	
	//find the nearest centroids for all the points
	public static void nearestCentroids(){
		double dist;
		for(double[] p : points){
			double min = Double.MAX_VALUE;
			for(int i = 0; i < CLUSTER_NUM; i ++){
				dist = distance(p, curCentroids[i]);
				if(dist < min){
					min = dist;
					p[DIMENSION] = (double) i;
				}
			}
		}
	}
	
	//calculate the new centroids
	public static void distribution(){
		int i = 0;
		int j = 0;
		double[][] sum = new double[CLUSTER_NUM][DIMENSION];
		double[] count = new double[CLUSTER_NUM];
		for(i = 0; i < CLUSTER_NUM; i ++){
			count[i] = 0.0;
			for(j = 0; j < DIMENSION; j ++){
				sum[i][j] = 0.0;
			}
		}
		
		int index;
		for(double[] p : points){
			index = (int) p[DIMENSION];
			for(i = 0; i < DIMENSION; i ++){
				sum[index][i] += p[i];
			}
			count[index] ++;
		}
		
		for(i = 0; i < CLUSTER_NUM; i ++){
			for(j = 0; j < DIMENSION; j ++){
				updatedCentroids[i][j] = sum[i][j] / count[i];
			}
		}
	}
	
	public static double squaredError(){
		double squaredError = 0.0;
		int index;
		for(double[] p : points){
			index = (int) p[DIMENSION];
			squaredError += Math.pow(distance(p, updatedCentroids[index]), 2.0);
		}
		return squaredError;
	}
 
	public static void main(String[] args) {
		if(args.length != 2){
			System.err.println("Usage: Kmeans <in> <k>");
			System.exit(2);
		}
		long startTime = System.currentTimeMillis();
		CLUSTER_NUM = Integer.parseInt(args[1]);
		initPoints(args[0]);
		initCentroids();
		while(!isConverge()){
			updateCentroids();
			nearestCentroids();
			distribution();
		}
		for(int i = 0; i < CLUSTER_NUM; i ++){
			for(int j = 0; j < DIMENSION; j ++){
				System.out.print(curCentroids[i][j] + ",");
			}
			System.out.println();
		}
		
		long endTime = System.currentTimeMillis();
		System.out.println(endTime - startTime);
		
		System.out.println("SquaredError:" + squaredError());
		
	}

}
