package com.dataintensivecomputing.image;
import ij.ImagePlus;
import ij.process.ImageProcessor;

import java.awt.Image;
import java.util.ArrayList;
import java.util.List;

import com.dataintensivecomputing.handler.ConfigHandler;
import com.sun.jimi.core.*;
import mpicbg.ij.SIFT;
import mpicbg.imagefeatures.Feature;
import mpicbg.imagefeatures.FloatArray2DSIFT;

//public class SiftFeatureExtractor {
//	
//	public void extractFeatures()
//	{
//		Image image = Jimi.getImage("/home/atul/UMBC/fall2014/yesha/code/testset/0.png");
//		ImagePlus imagePlus = new ImagePlus("testImage", image);
//	
//		ImageProcessor ip1 = imagePlus.getProcessor().convertToFloat();
//	
//		FloatArray2DSIFT.Param p = new FloatArray2DSIFT.Param(); 
//
//		SIFT ijSift = new SIFT( new FloatArray2DSIFT(p));
//		List< Feature > fs = new ArrayList< Feature >();
//		
//		ijSift.extractFeatures( ip1, fs );
//		for (Feature f : fs )
//			System.out.println(fs.get(0));
//	}
//	
//}

import mpicbg.ij.SIFT;
import mpicbg.imagefeatures.*;

import ij.plugin.*;
import ij.gui.*;
import ij.*;
import ij.process.*;

import java.util.ArrayList;
import java.util.List;
import java.awt.Color;
import java.awt.Polygon;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;

/**
 * Extract and display Scale Invariant Features after the method of David Lowe
 * \cite{Lowe04} in an image.
 * 
 * BibTeX:
 * <pre>
 * &#64;article{Lowe04,
 *   author    = {David G. Lowe},
 *   title     = {Distinctive Image Features from Scale-Invariant Keypoints},
 *   journal   = {International Journal of Computer Vision},
 *   year      = {2004},
 *   volume    = {60},
 *   number    = {2},
 *   pages     = {91--110},
 * }
 * </pre>
 * 
 * @author Stephan Saalfeld <saalfeld@mpi-cbg.de>
 * @version 0.2b
 */
public class SiftFeatureExtractor implements PlugIn
{
	final static private FloatArray2DSIFT.Param p = new FloatArray2DSIFT.Param(); 

	final static private List< Feature > fs = new ArrayList< Feature >();

	private String inputPath;
	private String outputFilePathName;
	
	public SiftFeatureExtractor(String inputPath) {
		// TODO Auto-generated constructor stub
		this.inputPath = inputPath;
		outputFilePathName = ConfigHandler.prop.getProperty("featuresFile");
	}
	
	public void allImagesFeatureExtractor() throws IOException
	{
		File dir = new File(inputPath);
		File[] dirListing = dir.listFiles();
		if(dirListing==null)
		{
			throw new FileNotFoundException();
			
		}
		File outputFile = new File(outputFilePathName);
		BufferedWriter bw = new BufferedWriter(new FileWriter(outputFile));
		//SET PARAMEMTER
		//p.initialSigma = ( float )gd.getNextNumber(); 	//1.6
		p.steps = 5;
//		p.minOctaveSize = ( int )gd.getNextNumber();   //64
//		p.maxOctaveSize = ( int )gd.getNextNumber();	//1024
//		p.fdSize = ( int )gd.getNextNumber();		
//		p.fdBins = ( int )gd.getNextNumber();
		
		final long start_time = System.currentTimeMillis();
		System.out.print( "processing SIFT ..." );
		for(File child :dirListing)
		{
			Image image = Jimi.getImage(child.getAbsolutePath());
			ImagePlus imp = new ImagePlus("Image", image);
			final ImageProcessor ip1  = imp.getProcessor().resize(64, 64,true).convertToFloat();
			
		
			final SIFT ijSift = new SIFT( new FloatArray2DSIFT( p ) );
			fs.clear();
			
		
			ijSift.extractFeatures( ip1, fs );
			
			
			System.out.println( fs.size() + " features identified and processed" );
		
			
			for ( final Feature f : fs )
			{
				
				if(f.descriptor.length != 128)
				{
					continue;
				}
				
				bw.write(f.location[0] + " " + f.location[1] + " " + f.scale + " " + f.orientation + " ");
				
				for(Float i: f.descriptor)
				{
					bw.write(i+" ");
				
					
				}
				bw.write("\n");
		
			}
		}
		bw.close();
		System.out.println( " took " + ( System.currentTimeMillis() - start_time ) + "ms" );
	}
	/**
	 * Draw a rotated square around a center point having size and orientation
	 * 
	 * @param o center point
	 * @param scale size
	 * @param orient orientation
	 */
	static void drawSquare( ImageProcessor ip, double[] o, double scale, double orient )
	{
		/*scale /= 2;
	    double sin = Math.sin( orient );
	    double cos = Math.cos( orient );
	    
	    int[] x = new int[ 6 ];
	    int[] y = new int[ 6 ];
	    

	    x[ 0 ] = ( int )( o[ 0 ] + ( sin - cos ) * scale );
	    y[ 0 ] = ( int )( o[ 1 ] - ( sin + cos ) * scale );
	    
	    x[ 1 ] = ( int )o[ 0 ];
	    y[ 1 ] = ( int )o[ 1 ];
	    
	    x[ 2 ] = ( int )( o[ 0 ] + ( sin + cos ) * scale );
	    y[ 2 ] = ( int )( o[ 1 ] + ( sin - cos ) * scale );
	    x[ 3 ] = ( int )( o[ 0 ] - ( sin - cos ) * scale );
	    y[ 3 ] = ( int )( o[ 1 ] + ( sin + cos ) * scale );
	    x[ 4 ] = ( int )( o[ 0 ] - ( sin + cos ) * scale );
	    y[ 4 ] = ( int )( o[ 1 ] - ( sin - cos ) * scale );
	    x[ 5 ] = x[ 0 ];
	    y[ 5 ] = y[ 0 ];
	    
	    ip.drawPolygon( new Polygon( x, y, x.length ) );
*/	    
		ip.drawDot((int)o[0], (int)o[1]);
	}

	//@Override //Java 6 fixes this
	public void run( String args )
	{
		if ( IJ.versionLessThan( "1.37i" ) ) return;

		//final ImagePlus imp = WindowManager.getCurrentImage();
		//if ( imp == null )  { System.err.println( "There are no images open" ); return; }

		//Image image = Jimi.getImage("/home/atul/UMBC/fall2014/yesha/code/testset/456.png");
		//Image image = Jimi.getImage("/home/atul/UMBC/fall2014/yesha/dataset/anatolia_001820.jpg");
		Image image = Jimi.getImage("/home/atul/Pictures/datastructures/karl-fritsch-silver-salt-shakers-1997.jpg");
		ImagePlus imp = new ImagePlus("testImage", image);
//		final GenericDialog gd = new GenericDialog( "Test SIFT" );
		
//		SIFT.addFields( gd, p );
//		gd.showDialog();
//		if ( gd.wasCanceled() ) return;
//		SIFT.readFields( gd, p );
		
//		p.initialSigma = ( float )gd.getNextNumber(); 	//1.6
		p.steps = 5;
//		p.minOctaveSize = ( int )gd.getNextNumber();   //64
//		p.maxOctaveSize = ( int )gd.getNextNumber();	//1024
//		p.fdSize = ( int )gd.getNextNumber();		
//		p.fdBins = ( int )gd.getNextNumber();
//		
		
		//final ImageProcessor ip1  = imp.getProcessor().resize(64, 64,true).convertToFloat();
		final ImageProcessor ip1  = imp.getProcessor().convertToFloat();
		
		//final ImageProcessor ip1 = imp.getProcessor().convertToFloat();
		//final ImageProcessor ip2 = imp.getProcessor().resize(64, 64,true).duplicate().convertToRGB();
		final ImageProcessor ip2 = imp.getProcessor().duplicate().convertToRGB();
		final SIFT ijSift = new SIFT( new FloatArray2DSIFT( p ) );
		fs.clear();
		
		final long start_time = System.currentTimeMillis();
		System.out.print( "processing SIFT ..." );
		ijSift.extractFeatures( ip1, fs );
		System.out.println( " took " + ( System.currentTimeMillis() - start_time ) + "ms" );
		
		System.out.println( fs.size() + " features identified and processed" );
		
		ip2.setLineWidth( 3 );
		ip2.setColor( Color.red );
		int count =0;
		for ( final Feature f : fs )
		{
			drawSquare( ip2, new double[]{ f.location[ 0 ], f.location[ 1 ] }, p.fdSize * 4.0 * ( double )f.scale, ( double )f.orientation );
			
			System.out.println(f.location[0] + " " + f.location[ 1 ] + " " + f.scale + f.orientation + " ");
			for(Float i: f.descriptor)
			{
				System.out.print(i + " ");
				count++;
				
			}
			System.out.println(count + "\n");
		}
		new ImagePlus( imp.getTitle() + " Features ", ip2 ).show();
	}
}
