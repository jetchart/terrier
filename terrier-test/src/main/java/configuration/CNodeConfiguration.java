package configuration;

import java.io.File;
import java.io.FileInputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Properties;

import node.CNode;

import org.apache.log4j.Logger;

public class CNodeConfiguration implements INodeConfiguration {

	static final Logger logger = Logger.getLogger(CNodeConfiguration.class);
	
	Integer port;
	String idNode;
	String nodeType;
	String terrierHome;
	String folderPath;
	String destinationFolderPath;
	String indexPath;
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
			this.destinationFolderPath = propiedades.getProperty("destinationFolderPath");
			this.idNode = propiedades.getProperty("idNode");
			this.port = Integer.valueOf(propiedades.getProperty("port"));
			this.nodeType = propiedades.getProperty("nodeType");
			this.indexPath = terrierHome +"var/index/";
			if (CNode.ID_MASTER.equals(this.nodeType.toUpperCase())){
				readMasterConfiguration(propiedades);
			}else if (CNode.ID_SLAVE.equals(this.nodeType.toUpperCase())){
				readSlaveConfiguration(propiedades);
			}else{
				logger.error("Propiedad \"nodeType\" errónea");
			}
			/* Validar path's leidos en la configuracion */
			validarRutaExistente("terrierHome", this.terrierHome);
			validarRutaExistente("destinationFolderPath", this.destinationFolderPath);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * Carga las configuraciones propias del Master 
	 * @param propiedades
	 * @throws Exception 
	 */
	private void readMasterConfiguration(Properties propiedades) throws Exception{
		this.idMasterNode = propiedades.getProperty("idMasterNode");
		this.folderPath = propiedades.getProperty("folderPath");
		/* Cantidad de nodos disponibles */
		this.nodesAmount = Integer.valueOf(propiedades.getProperty("nodesAmount"));
		/* Nodos */
		for (int i=1;i<=nodesAmount; i++){
			slavesNodes.add(propiedades.getProperty("idSlaveNode_"+i));
		}
		/* Validar path's leidos en la configuracion */
		validarRutaExistente("folderPath", this.folderPath);
	}
	
	/**
	 * Carga las configuraciones propias del Slave 
	 * @param propiedades
	 */
	private void readSlaveConfiguration(Properties propiedades){
		/* SFTP */
		this.passwordSFTP = propiedades.getProperty("passwordSFTP");
		this.userSFTP = propiedades.getProperty("userSFTP");
		this.masterSFTPPort = Integer.valueOf(propiedades.getProperty("masterSFTPPort"));
		this.masterSFTPHost =  propiedades.getProperty("masterSFTPHost");
	}

	/**
	 * El metodo validarRutasExistentes debe validar que el path recibido 
	 * sea un directorio existente. En caso contrario lanza una excepcion.
	 * 
	 * @throws Exception Lista de path's incorrectos
	 */
	private void validarRutaExistente(String propertieName, String path) throws Exception{
		File f = new File(path);
		if (!f.exists())
			throw new Exception("No existe la siguiente carpeta para la propiedad \"" + propertieName + "\": " + path + "\n");
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
	@Override
	public void setFolderPath(String folderPath){
		this.folderPath = folderPath;
	}

	public String getIndexPath() {
		return indexPath;
	}

	public void setIndexPath(String indexPath) {
		this.indexPath = indexPath;
	}
}
