package kmeans;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

public class Utils {

	static final int DIMENSION = 5;
	static final double THRESHOLD = 0.0001;
	static final String SPLIT = ",";

	public static double distance(String point1, String point2) {
		double[] p1 = stringToDoubles(point1);
		double[] p2 = stringToDoubles(point2);
		double sum = 0.0;
		for (int i = 0; i < DIMENSION; i++) {
			sum += Math.pow(Math.abs(p1[i] - p2[i]), DIMENSION);
		}
		return Math.pow(sum, 1d / DIMENSION);
	}

	public static double[] stringToDoubles(String s) {
		String[] doubleString = s.split(SPLIT);
		// int l = doubleString.length;
		double[] doubles = new double[DIMENSION];
		for (int i = 0; i < DIMENSION; i++) {
			doubles[i] = Double.parseDouble(doubleString[i]);
		}
		return doubles;
	}

	public static String doublesToString(double[] ds) {
		StringBuffer sb = new StringBuffer();
		for (double d : ds) {
			sb.append(d + ",");
		}
		sb.deleteCharAt(sb.length() - 1);
		return sb.toString();
	}
	
	public static String partialFilename(int k){
		String index = String.valueOf(k);
		int l = index.length();
		String filename = "/part-r-";
		for(int i = 0; i < 5 - l; i ++){
			filename += "0";
		}
		return filename + index;
	}
	
	public static void initialClusters(Configuration conf) throws IOException{
		String uri;
		String pointPath = conf.get("pointsPath");
		String currClusters = conf.get("clustersPath");
		String updatedClusters = conf.get("updatedClustersPath");
		int num = Integer.parseInt(conf.get("clustersNum"));
		double[] origin = new double[DIMENSION];
		Arrays.fill(origin, 0.0);
		String originString = doublesToString(origin);
		
		FileSystem fsCur = FileSystem.get(URI.create(currClusters), conf);
		Path pathCur = new Path(currClusters);
		if(fsCur.exists(pathCur)){
			fsCur.delete(pathCur, true);
		}
		for(int i = 0; i < num; i ++){
			uri = currClusters + partialFilename(i);
			FSDataOutputStream out = fsCur.create(new Path(uri));
			out.write(originString.getBytes());
			out.close();
		}
		
		FileSystem fsPoint = FileSystem.get(URI.create(pointPath), conf);
		FileSystem fsUpdate = FileSystem.get(URI.create(updatedClusters), conf);
		Path pathUpdate = new Path(updatedClusters);
		if(fsUpdate.exists(pathUpdate)){
			fsUpdate.delete(pathUpdate, true);
		}
		FSDataInputStream in = fsPoint.open(new Path(pointPath));
		BufferedReader reader = new BufferedReader(new InputStreamReader(in));
		String line;
		int k = 0;
		while((line = reader.readLine()) != null && k < num){
			uri = updatedClusters + partialFilename(k);
			FSDataOutputStream out = fsUpdate.create(new Path(uri));
			out.write(line.getBytes());
			out.close();
			k ++;
		}
	}

	public static boolean isConverge(Configuration conf) throws IOException {
		String currClusters = conf.get("clustersPath");
		String updatedClusters = conf.get("updatedClustersPath");
		int num = Integer.parseInt(conf.get("clustersNum"));
		String[] currCenters = readCenters(currClusters, num, conf);
		String[] updatedCenters = readCenters(updatedClusters, num, conf);
		
		for (int i = 0; i < num; i++) {
			double dist = distance(currCenters[i], updatedCenters[i]);
			if (dist > THRESHOLD) {
				return false;
			}
		}
		return true;
	}
	
	public static String[] readCenters(String folderPath, int num, Configuration conf) throws IOException{
		String[] centers = new String[num];
		int count = 0;
		int index = 0;
		String line;
		while(count < num && index < num){
			String uri = folderPath + partialFilename(index);
			FileSystem fs = FileSystem.get(URI.create(uri), conf);
			FSDataInputStream in = null;
//			if(!fs.exists(new Path(uri))){
//				break;
//			}
			try{
				in = fs.open(new Path(uri));
				BufferedReader reader = new BufferedReader(new InputStreamReader(in));
				while((line = reader.readLine()) != null){
					centers[count] = line;
					count ++;
				}
				index ++;
			} finally{
				IOUtils.closeStream(in);
			}
		}
		return centers;
	}

	public static void updateCenters(Configuration conf) throws IOException {
		String currClusters = conf.get("clustersPath");
		String updatedClusters = conf.get("updatedClustersPath");
		int num = Integer.parseInt(conf.get("clustersNum"));
		FSDataInputStream in;
		FSDataOutputStream out;
		String uriIn, uriOut;
		FileSystem fsOut = FileSystem.get(URI.create(currClusters),conf);
		FileSystem fsIn = FileSystem.get(URI.create(updatedClusters),conf);
		Path current = new Path(currClusters);
		if (fsOut.exists(current)) {
			fsOut.delete(current, true);
		}

		for(int i = 0; i < num; i ++){
			uriIn = updatedClusters + partialFilename(i);
			uriOut = currClusters + partialFilename(i);
			if(fsIn.exists(new Path(uriIn))){
				in = fsIn.open(new Path(uriIn));
				out = fsOut.create(new Path(uriOut));
				IOUtils.copyBytes(in, out, conf);
				in.close();
				out.close();
			}
		}
	}
}
