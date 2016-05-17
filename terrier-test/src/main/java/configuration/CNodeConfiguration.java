package configuration;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Properties;

public class CNodeConfiguration implements INodeConfiguration {

	Integer port;
	String idNode;
	String nodeType;
	String terrierHome;
	String folderPath;
	String destinationFolderPath;
	String idMasterNode;
	Collection<String> slavesNodes = new ArrayList<String>();
	Integer nodesAmount;
	private String passwordSFTP;
	private String userSFTP;
	private Integer masterSFTPPort;
	private String masterSFTPHost;

	public CNodeConfiguration(String configurationFilePath) {
		this.readFileConfiguration(configurationFilePath);
	}
	
	/**
	 * Se encarga de leer y setear los atributos del archivo configurationFilePath
	 * @param configurationFilePath		Ruta del archivo de configuracion
	 */
	public void readFileConfiguration(String configurationFilePath) {
		try {
			/* Creamos un Objeto de tipo Properties */
			Properties propiedades = new Properties();
			/* Cargamos el archivo desde la ruta especificada */
			propiedades.load(new FileInputStream(configurationFilePath));
			/* Obtenemos los parametros definidos en el archivo */
			this.terrierHome = propiedades.getProperty("terrierHome");
			this.folderPath = propiedades.getProperty("folderPath");
			this.nodeType = propiedades.getProperty("nodeType");
			this.idNode = propiedades.getProperty("idNode");
			this.port = Integer.valueOf(propiedades.getProperty("port"));
			this.destinationFolderPath = propiedades.getProperty("destinationFolderPath");
			/* TODO Sirve esto?? */
			this.idMasterNode = propiedades.getProperty("idMasterNode");
			/* Cantidad de nodos disponibles */
			this.nodesAmount = Integer.valueOf(propiedades.getProperty("nodesAmount"));
			/* Nodos */
			for (int i=1;i<nodesAmount; i++){
				slavesNodes.add(propiedades.getProperty("idSlaveNode_"+i));
			}
			/* SFTP */
			this.passwordSFTP = propiedades.getProperty("passwordSFTP");
			this.userSFTP = propiedades.getProperty("userSFTP");
			this.masterSFTPPort = Integer.valueOf(propiedades.getProperty("masterSFTPPort"));
			this.masterSFTPHost =  propiedades.getProperty("masterSFTPHost");
			
		} catch (FileNotFoundException e) {
			System.out.println("Error, no existe el archivo: " + configurationFilePath);
		} catch (IOException e) {
			System.out.println("Error, no se puede leer el archivo" + configurationFilePath);
		} catch (Exception e) {
			System.out.println("Error");
		}
	}

	public String getTerrierHome() {
		return this.terrierHome;
	}

	public String getFolderPath() {
		return this.folderPath;
	}

	public String getDestinationFolderPath() {
		return this.destinationFolderPath;
	}

	public String getIdMasterNode() {
		return this.idMasterNode;
	}
	
	public Collection<String> getSlavesNodes() {
		return this.slavesNodes;
	}

	public String getNodeType() {
		return nodeType;
	}

	public void setNodeType(String nodeType) {
		this.nodeType = nodeType;
	}

	@Override
	public String getIdNode() {
		return idNode;
	}

	@Override
	public Integer getPort() {
		return port;
	}

	@Override
	public String getMasterSFTPHost() {
		return masterSFTPHost;
	}

	@Override
	public Integer getMasterSFTPPort() {
		return masterSFTPPort;
	}

	@Override
	public String getUserSFTP() {
		return userSFTP;
	}

	@Override
	public String getPasswordSFTP() {
		return passwordSFTP;
	}
}
