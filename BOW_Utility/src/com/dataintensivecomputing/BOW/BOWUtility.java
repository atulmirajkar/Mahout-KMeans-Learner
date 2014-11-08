package com.dataintensivecomputing.BOW;

import ij.ImagePlus;
import ij.process.ImageProcessor;

import java.awt.Image;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.mahout.math.DenseVector;
import org.apache.mahout.math.NamedVector;
import org.apache.mahout.math.VectorWritable;

import com.sun.jimi.core.Jimi;

import mpicbg.ij.SIFT;
import mpicbg.imagefeatures.Feature;
import mpicbg.imagefeatures.FloatArray2DSIFT;

public class BOWUtility {
	
	private Configuration conf;
	public BOWUtility() {
		conf = new Configuration();
		conf.addResource(new Path("/usr/local/hadoop/conf/core-site.xml"));
		conf.addResource(new Path("/usr/local/hadoop/conf/mapred-site.xml"));
		conf.addResource(new Path("/usr/local/hadoop/conf/hdfs-site.xml"));

	}

	public void BOWHandler(String prefixPath, String clusterParentPath)
			throws IOException {
		// read file with png filename and label
		String trainLabelFile = prefixPath + "/" + "trainsetlabels" + "/"
				+ "trainLabels.txt";
		BufferedReader reader = null;
		try {
			reader = new BufferedReader(new FileReader(trainLabelFile));
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		//write features to a file
		String featureFile = prefixPath + "/" + "classifierfeatures" + "/" + "featurefile.txt";
		BufferedWriter writer = new BufferedWriter(new FileWriter(featureFile));

		// get centroid NamedVectors
		ArrayList<NamedVector> namedVectorList = getCentroidVectors(clusterParentPath);
		
		//fix the sequence of centroids
		ArrayList<String> centroidList = new ArrayList<String>();
		for(NamedVector vec: namedVectorList)
		{
			centroidList.add(vec.getName());
		}
		
		String line = null;
		Map<String, Integer> centroidCountsMap = null;
		while ((line = reader.readLine()) != null) {
			String[] pathAndLabel = line.split("\\t");

			// open image get sift descriptor
			String imagePath = prefixPath + "/" + pathAndLabel[0];
			List<Feature> sift_list = extractImageSift(imagePath);

			centroidCountsMap = getFeatureCounts(sift_list, namedVectorList);
			
			//use centroildList for sequence
			writer.write(pathAndLabel[1]);
			for(String centroidName: centroidList)
			{
				if(!centroidCountsMap.containsKey(centroidName))
				{
					writer.write(",0");
				}
				else
				{
					writer.write("," + String.valueOf(centroidCountsMap.get(centroidName)));
				}
			}
			writer.write("\n");
		}//for ach line of labels file
		
		reader.close();
					writer.close();
	}

	public Map<String, Integer> getFeatureCounts(List<Feature> sift_list,
			ArrayList<NamedVector> namedVectorList) {
		double[] siftDoubleArr = new double[132];
		Map<String, Integer> centroidCounts = new HashMap<String,Integer>();
		for (Feature siftFeature : sift_list) {
			if (siftFeature.descriptor.length != 128) {
				continue;
			}
			siftDoubleArr[0] = siftFeature.location[0];
			siftDoubleArr[1] = siftFeature.location[1];
			siftDoubleArr[2] = siftFeature.scale;
			siftDoubleArr[3] = siftFeature.orientation;

			for (int i = 0; i < 128; i++) {
				siftDoubleArr[i + 4] = siftFeature.descriptor[i];

			}
			NamedVector siftVector = new NamedVector(new DenseVector(
					siftDoubleArr), "");

			Map<String, Double> centroidDistrance = new HashMap<String, Double>();

			// get distance and name of centroid to map
			for (NamedVector centroidVec : namedVectorList) {
				centroidDistrance.put(centroidVec.getName(),
						centroidVec.getDistanceSquared(siftVector));

			}

			// get closest centroid
			String centroidName = "";
			double minCentroidDist = Double.MAX_VALUE;
			Iterator it = centroidDistrance.entrySet().iterator();
			while (it.hasNext()) {
				Map.Entry pair = (Map.Entry) it.next();
				if ((Double) pair.getValue() < minCentroidDist) {
					minCentroidDist = (Double) pair.getValue();
					centroidName = (String) pair.getKey();
				}
			}
			
			if(!centroidCounts.containsKey(centroidName))
			{
				centroidCounts.put(centroidName, 1);
			}
			else
			{
				centroidCounts.put(centroidName, centroidCounts.get(centroidName) + 1);
			}
			
		}//end for all sift features for image
		return centroidCounts;
	}

	public ArrayList<NamedVector> getCentroidVectors(String clusterParentPath)
			throws IOException {
	
		Path dirPath = new Path(clusterParentPath);
		FileStatus[] fileStatusArr = null;
		FileSystem fs = null;
		ArrayList<Path> clusterList = new ArrayList<Path>();
		try {
			// get the file system
			fs = FileSystem.get(conf);

			// get metadata of desired dir
			fileStatusArr = fs.listStatus(dirPath);

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		for (FileStatus status : fileStatusArr) {
			if (status != null && !status.isDir()) {
				clusterList.add(status.getPath());
				System.out.println(status.getPath().getName());
			}

		}
		ArrayList<NamedVector> namedVectorList = new ArrayList<NamedVector>();
		// add namedvectors to arraylist
		for (Path centroidPath : clusterList) {
			SequenceFile.Reader reader = null;
			try {
				reader = new SequenceFile.Reader(fs, centroidPath, conf);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			Text key = new Text();
			VectorWritable value = new VectorWritable();

			while (reader.next(key, value)) {
				namedVectorList.add((NamedVector) value.get());
			}
			reader.close();
		}

		return namedVectorList;

	}

	public List<Feature> extractImageSift(String imagePath) {
		final FloatArray2DSIFT.Param p = new FloatArray2DSIFT.Param();

		final List<Feature> fs = new ArrayList<Feature>();

		// SET PARAMEMTER
		// p.initialSigma = ( float )gd.getNextNumber(); //1.6
		p.steps = 5;
		// p.minOctaveSize = ( int )gd.getNextNumber(); //64
		// p.maxOctaveSize = ( int )gd.getNextNumber(); //1024
		// p.fdSize = ( int )gd.getNextNumber();
		// p.fdBins = ( int )gd.getNextNumber();

		final long start_time = System.currentTimeMillis();
		System.out.print("processing SIFT ...");

		File child = new File(imagePath);
		Image image = Jimi.getImage(child.getAbsolutePath());
		ImagePlus imp = new ImagePlus("Image", image);
		final ImageProcessor ip1 = imp.getProcessor().resize(64, 64, true)
				.convertToFloat();

		final SIFT ijSift = new SIFT(new FloatArray2DSIFT(p));
		fs.clear();

		ijSift.extractFeatures(ip1, fs);

		System.out.println(fs.size() + " features identified and processed");

		return fs;

	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {

		BOWUtility bowUtility = new BOWUtility();
		try {
			bowUtility.BOWHandler(args[0], args[1]);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

}
