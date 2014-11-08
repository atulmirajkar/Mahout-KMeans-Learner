package com.dataintensivecomputing.centroidutility;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.mahout.math.DenseVector;
import org.apache.mahout.math.NamedVector;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;
import org.easymock.internal.matchers.InstanceOf;
import org.hamcrest.core.IsInstanceOf;

public class ClustersCentroidUtility {

	private Configuration conf;

	ClustersCentroidUtility() {
		conf = new Configuration();
		conf.addResource(new Path("/usr/local/hadoop/conf/core-site.xml"));
		conf.addResource(new Path("/usr/local/hadoop/conf/mapred-site.xml"));
		conf.addResource(new Path("/usr/local/hadoop/conf/hdfs-site.xml"));

	}

	public ArrayList<Path> getClusterFiles(String prefixPath) {
		Path dirPath = new Path(prefixPath + "/" + "cluster-means/");
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
		//log
		System.out.println(fileStatusArr[0].getPath().getName());
		//log
		FileStatus[] partStatusArr = null;
		for (FileStatus status : fileStatusArr) {
			if (status.isDir()) {
				//log
				System.out.println(status.getPath());
				//log
				try {
					partStatusArr = fs.listStatus(status.getPath());

					// the first entry should be a file
					if (partStatusArr != null && !partStatusArr[0].isDir()) {
						clusterList.add(partStatusArr[0].getPath());
						System.out
								.println(partStatusArr[0].getPath().getName());
					}

				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}

			}

		}

		return clusterList;
	}

	public void calculateClusterMeans(String prefixPath,
			ArrayList<Path> clusterSeqFiles) {
		for (int j = 0; j < clusterSeqFiles.size(); j++) {

			SequenceFile.Reader reader = null;
			try {
				Path input = clusterSeqFiles.get(j);

				FileSystem fs = FileSystem.get(conf);
				reader = new SequenceFile.Reader(fs, input, conf);

				IntWritable key = new IntWritable();
				VectorWritable value = new VectorWritable();
				int totalCount = 0;
				double[] meanVector = new double[132];
				double[] parsedVector = null;

				while (reader.next(key, value)) {
					totalCount++;
					// System.out.println(key + "\t" + value);
					parsedVector = parseClusterInstance(value);
					updateMeanVector(meanVector, parsedVector);
				}

				// calculate mean of the vector
				for (int i = 0; i < 132; i++) {
					meanVector[i] /= totalCount;

				}

				for (int i = 0; i < 132; i++) {
					System.out.println(meanVector[i]);

				}

				System.out.println(totalCount);

				writeClusterToSequenceFile(input.getName(), meanVector,
						prefixPath);

			} catch (IOException e) {
				e.printStackTrace();
			} finally {

				try {
					reader.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}// for each cluster
	}

	public void writeClusterToSequenceFile(String name, double[] meanVector,
			String prefixPath) {
		Path outPath = null;
		FileSystem fs = null;
		SequenceFile.Writer writer = null;
		try {

			fs = FileSystem.get(conf);
			outPath = new Path(prefixPath + "/cluster-output" + "/" + name);
			fs.delete(outPath, false);
			fs.createNewFile(outPath);

			// open a sequence file writer
			writer = SequenceFile.createWriter(fs, conf, outPath, Text.class,
					VectorWritable.class);

			// create a key
			Text key = new Text();
			key.set(name);

			// the namedvector has a delegate and name
			NamedVector vec = new NamedVector(new DenseVector(meanVector),
					key.toString());

			VectorWritable value = new VectorWritable();
			value.set(vec);

			// append the key and vector
			writer.append(key, value);

		} catch (IOException e) {
			e.printStackTrace();

		} finally {

			try {
				writer.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

	public void updateMeanVector(double[] meanvector, double[] parsedVector) {

		if (meanvector.length != parsedVector.length) {
			System.out.println("Mismatch");
			return;

		}
		for (int i = 0; i < 132; i++) {
			meanvector[i] += parsedVector[i];

		}
	}

	double[] parseClusterInstance(VectorWritable value) {
		Vector vector = value.get();
		double[] outputVector = null;
		if (vector instanceof NamedVector) {

			vector = ((NamedVector) vector).getDelegate();
			outputVector = new double[vector.size()];
			for (int i = 0; i < vector.size(); i++) {
				outputVector[i] = vector.get(i);

			}
		}
		return outputVector;

	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		//args[0] - prefix path
		ClustersCentroidUtility ccu = new ClustersCentroidUtility();
		ArrayList<Path> clusterSeqFiles = ccu.getClusterFiles(args[0]);

		ccu.calculateClusterMeans(args[0], clusterSeqFiles);
	}

}
