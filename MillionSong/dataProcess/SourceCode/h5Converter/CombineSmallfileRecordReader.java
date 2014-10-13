package h5Converter;
import java.io.IOException;

import ncsa.hdf.object.h5.H5File;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;

public class CombineSmallfileRecordReader extends
		RecordReader<LongWritable, Text> {
	
	private static int YEAR_START = 1926;
	private static int YEAR_END = 2010;

	private CombineFileSplit combineFileSplit;
	private H5File h5;
	//private JobContext jobContext;
	private Path path;
	private int totalLength;
	private int currentIndex;
	private float currentProgress = 0;
	private LongWritable currentKey;
	private Text currentValue = new Text();
	private boolean processed = false;

	public CombineSmallfileRecordReader(CombineFileSplit combineFileSplit,
			TaskAttemptContext context, Integer index) throws IOException {
		super();
		this.combineFileSplit = combineFileSplit;
		this.currentIndex = index;

	}

	@Override
	public void initialize(InputSplit split, TaskAttemptContext context)
			throws IOException, InterruptedException {
		this.combineFileSplit = (CombineFileSplit) split;
		//this.jobContext = context;
		
		path = combineFileSplit.getPath(currentIndex);
		String folder = path.getParent().getName();
		String filename = path.getName();
		context.getConfiguration().set("map.input.file",
				folder + "/" + filename);
//		context.getConfiguration().set("map.input.file",
//				path.toString());
		h5 = H5_Getter.hdf5_open_readonly(context.getConfiguration().get(
				"map.input.file"));
	}

	@Override
	public LongWritable getCurrentKey() throws IOException,
			InterruptedException {
		return currentKey;
	}

	@Override
	public Text getCurrentValue() throws IOException, InterruptedException {
		return currentValue;
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		if (!processed) {
			StringBuffer sbuffer = new StringBuffer();
			try {
				double song_hot = H5_Getter.get_song_hotttnesss(h5);
				if(Double.isNaN(song_hot)){
					song_hot = 0.0;
				}
				sbuffer.append(song_hot + ",");

				sbuffer.append(H5_Getter.get_key_confidence(h5) + ",");
				sbuffer.append(H5_Getter.get_mode_confidence(h5) + ",");
				sbuffer.append(doublesAverage(H5_Getter.get_segments_pitches(h5)) + ",");		

				sbuffer.append(H5_Getter.get_artist_hotttnesss(h5));
//				double artist_hotness = H5_Getter.get_artist_hotttnesss(h5);
//				if(artist_hotness > 0.75){
//					sbuffer.append("veryHot");
//				} else if(artist_hotness > 0.5){
//					sbuffer.append("Hot");
//				} else if(artist_hotness > 0.25){
//					sbuffer.append("notHot");
//				} else{
//					sbuffer.append("bad");
//				} 
				

			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

			currentValue.set(sbuffer.toString());
			processed = true;
			return true;
		}
		return false;
	}

	@Override
	public float getProgress() throws IOException {
		if (currentIndex >= 0 && currentIndex < totalLength) {
			currentProgress = (float) currentIndex / totalLength;
			return currentProgress;
		}
		return currentProgress;
	}

	@Override
	public void close() throws IOException {
		H5_Getter.hdf5_close(h5);
	}

	public String doublesAverage(double[] doubleArray) {
		double sum = 0.0;
		int count = 1;
		for (double d : doubleArray) {
			sum += d;
			count ++;
		}
		return Double.toString(sum / count);
	}
	
	public String convertYear(int year){
		if(year == 0){
			return "0.0";
		}
		return Double.toString((double) (year - YEAR_START) / (YEAR_END - YEAR_START));
	}

}
