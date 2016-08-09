package configuration;

import java.util.Collection;

public interface INodeConfiguration {

	public static String configurationMasterNodeFilePath = "./configurationMasterNode.properties";
	public static String configurationSlaveNodeFilePath = "./configurationSlaveNode.properties";
	
//	public static String configurationMasterNodeFilePath = "./src/main/resources/configurationMasterNode.properties";
//	public static String configurationSlaveNodeFilePath = "./src/main/resources/configurationSlaveNode.properties";
	
	public static String prefixIndex = "jmeIndex_";
	
	String getIdMasterNode();
	
	Collection<String> getSlavesNodes();
	
	String getTerrierHome();
	
	String getFolderPath();
	
	void setFolderPath(String folderPath);
	
	String getDestinationFolderPath();
	
	String getNodeType();
	
	String getIdNode();
	
	Integer getPort();
	
	/* SFTP */
	String getMasterSFTPHost();
	Integer getMasterSFTPPort();
	String getUserSFTP();
	String getPasswordSFTP();
}
