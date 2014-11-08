package com.dataintensivecomputing.LogisticRegression;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.mahout.classifier.sgd.AdaptiveLogisticRegression;
import org.apache.mahout.classifier.sgd.L1;
import org.apache.mahout.classifier.sgd.ModelSerializer;
import org.apache.mahout.math.DenseVector;
import org.apache.mahout.math.NamedVector;
import org.apache.mahout.math.VectorWritable;

public class MahoutLR {

	Configuration conf;
	int numFeatures;
	int numClasses;
	public MahoutLR(int classesCount,int vectorWidth)
	{
		conf = new Configuration();
		conf.addResource(new Path("/usr/local/hadoop/conf/core-site.xml"));
		conf.addResource(new Path("/usr/local/hadoop/conf/mapred-site.xml"));
		conf.addResource(new Path("/usr/local/hadoop/conf/hdfs-site.xml"));
		this.numFeatures = vectorWidth;
		this.numClasses = classesCount;
	}
	
	public void LRHandler(String plainFeatureFile,String hadoopPrefixPath) throws IOException
	{
		//create a sequencefile
		createSequenceFile(plainFeatureFile,hadoopPrefixPath);
		
		//run LR
		AdaptiveLogisticRegression lrTrainModel = runLR(hadoopPrefixPath + "featureFile");
		
		//save model 
		saveLRModel(lrTrainModel,hadoopPrefixPath);
	}
	
	public void saveLRModel(AdaptiveLogisticRegression lrTrainModel, String prefixPath) throws IOException
	{
		FileSystem fs = FileSystem.get(conf);
		Path path = new Path(prefixPath + "/" + "LR_Model");
		fs.delete(path,false);
		fs.create(path);
		ModelSerializer.writeBinary(prefixPath +"/"+ "LR_Model", lrTrainModel);
		
	}
	
	
	AdaptiveLogisticRegression  runLR(String featureFilePath) throws IOException
	{
		FileSystem fs = FileSystem.get(conf);
		Path path = new Path(featureFilePath);
		SequenceFile.Reader reader = new SequenceFile.Reader(fs, path, conf);
		
		IntWritable key = new IntWritable();
		VectorWritable value = new VectorWritable();
		
		AdaptiveLogisticRegression lrTrainModel = new AdaptiveLogisticRegression(numClasses, numFeatures,new L1());
		
		while(reader.next(key, value))
		{
			lrTrainModel.train(key.get(), value.get());
		}
		
		lrTrainModel.close();
		reader.close();
		System.out.println(lrTrainModel.auc());
		return lrTrainModel;
	}
	
	void createSequenceFile(String plainFeatureFile,String hadoopPrefixPath) throws IOException
	{
		//open plain text file to read
		BufferedReader br = new BufferedReader(new FileReader(plainFeatureFile));
		
		//open file to write
		FileSystem fs = FileSystem.get(conf);
		Path featureFilePath = new Path(hadoopPrefixPath + "featureFile");
		fs.delete(featureFilePath,false);
		
		SequenceFile.Writer sfWriter = new SequenceFile.Writer(fs, conf, featureFilePath, IntWritable.class, VectorWritable.class);
		IntWritable key = new IntWritable();
		VectorWritable val = new VectorWritable();
		
		String line=null;
		String[] tokens = null;
		double[] features = null;
		while((line=br.readLine())!=null)
		{
			tokens = line.split(",");
			NamedVector vec = null;
			
			features = new double[tokens.length-1];
			for(int i=1;i<tokens.length;i++)
			{
				features[i-1] = Double.parseDouble(tokens[i]);
			}
			
			//first value is the label
			vec = new NamedVector(new DenseVector(features),"");
			key.set(Integer.parseInt(tokens[0]));
			val.set(vec);
			sfWriter.append(key, val);
		}
		br.close();
		sfWriter.close();
	}
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		MahoutLR mlr = new MahoutLR(Integer.parseInt(args[2]),Integer.parseInt(args[3]));
		try {
			mlr.LRHandler(args[0],args[1]);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
