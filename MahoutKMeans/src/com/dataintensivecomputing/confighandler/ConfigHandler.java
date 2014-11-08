package com.dataintensivecomputing.confighandler;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class ConfigHandler {
	public static final Properties prop = new Properties();
	private ConfigHandler()
	{}
	private static ConfigHandler singleton = new ConfigHandler();
	
	public static ConfigHandler getInstance()
	{
		return singleton;
		
	}
	public void setProperties(String configPath)
	{
		
		InputStream inStream= null;
		
		try {
			inStream = new FileInputStream(configPath);
			prop.load(inStream);
			
			
			
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
	}

}

