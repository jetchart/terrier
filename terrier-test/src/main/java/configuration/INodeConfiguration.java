package configuration;

import java.util.Collection;

public interface INodeConfiguration {

	public static String defaultConfigurationMasterNodeFilePath = "/home/javier/terrier-4.0/etc/configurationMasterNode.properties";
	
	public static String defaultConfigurationSlaveNodeFilePath = "/home/javier/terrier-4.0/etc/configurationSlaveNode.properties";
	
	public static String defaultConfigurationFilePath = "/home/javier/terrier-4.0/configuration.properties";
	
//	public static String defaultConfigurationFilePath = "./src/main/resources/configuration.properties";
	
	public static String indexName = "jmeIndex";
	
	String getIdMasterNode();
	
	Collection<String> getSlavesNodes();
	
	String getTerrierHome();
	
	String getFolderPath();
	
	String getDestinationFolderPath();
	
	String getNodeType();
}
