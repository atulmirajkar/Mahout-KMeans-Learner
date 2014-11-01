package com.dataintensivecomputing.handler;

import java.io.FileNotFoundException;
import java.io.IOException;

import com.dataintensivecomputing.image.SiftFeatureExtractor;

public class ImageHandler {
	private String imageInputPath;
	/**
	 * @param args
	 */
	public void setimageInputPath(String imagePath)
	{
		this.imageInputPath = imagePath;
		
	}
	
	/*
	 * 1.create sift features
	 * 2.cluster sift features
	 * 3.get feature vectors for each image
	 * 4.classify images
	 * */
	public void mainHandler() throws IOException
	{
		SiftFeatureExtractor siftExtractor = new SiftFeatureExtractor(this.imageInputPath);
		siftExtractor.allImagesFeatureExtractor();
		//siftExtractor.run("/home/atul/UMBC/fall2014/yesha/code/testset/0.png");
		//siftExtractor.run("/home/atul/Pictures/datastructures/IMG-20140926-WA0009.jpg");
		
	}
	public static void main(String[] args) {
		ConfigHandler config = ConfigHandler.getInstance() ;
		//read config file
		config.setProperties();	
		
		ImageHandler imageHandler = new ImageHandler();
		imageHandler.setimageInputPath(ConfigHandler.prop.getProperty("imageinputpath"));
		
		//call actual handler
		try
		{
			imageHandler.mainHandler();
		}
		catch(IOException E)
		{	
			E.printStackTrace();
		}
		
	}

}
