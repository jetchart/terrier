package configuration;

import java.util.Collection;

public interface INodeConfiguration {

	public static String defaultConfigurationFilePath = "./configuration.properties";
//	public static String defaultConfigurationFilePath = "/home/jetchart/terrier-4.0/configuration.properties";
	
//	public static String defaultConfigurationFilePath = "./src/main/resources/configuration.properties";
	
	public static String indexName = "jmeIndex";
	
	String getIdMasterNode();
	
	Collection<String> getSlavesNodes();
	
	String getTerrierHome();
	
	String getFolderPath();
	
	String getDestinationFolderPath();
	
	String getNodeType();
	
	String getIdNode();
	
	Integer getPort();
}
