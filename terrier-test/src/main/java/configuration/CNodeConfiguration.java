package configuration;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Properties;

public class CNodeConfiguration implements INodeConfiguration {

	String terrierHome;
	String folderPath;
	String destinationFolderPath;
	String idMasterNode;
	Collection<String> slavesNodes = new ArrayList<String>();
	Integer nodesAmount;

	public CNodeConfiguration() {
		this.readFileConfiguration(INodeConfiguration.defaultConfigurationFilePath);
	}
	
	public CNodeConfiguration(String configurationFilePath) {
		this.readFileConfiguration(configurationFilePath);
	}
	
	
	public void readFileConfiguration(String configurationFilePath) {
		try {
			/** Creamos un Objeto de tipo Properties */
			Properties propiedades = new Properties();
			/** Cargamos el archivo desde la ruta especificada */
			propiedades.load(new FileInputStream(configurationFilePath));
			/** Obtenemos los parametros definidos en el archivo */
			this.terrierHome = propiedades.getProperty("terrierHome");
			this.folderPath = propiedades.getProperty("folderPath");
			this.destinationFolderPath = propiedades.getProperty("destinationFolderPath");
			this.idMasterNode = propiedades.getProperty("idMasterNode");
			this.nodesAmount = Integer.valueOf(propiedades.getProperty("nodesAmount"));
			for (int i=1;i<nodesAmount; i++){
				slavesNodes.add(propiedades.getProperty("idSlaveNode_"+i));
			}
		} catch (FileNotFoundException e) {
			System.out.println("Error, El archivo no existe");
		} catch (IOException e) {
			System.out.println("Error, No se puede leer el archivo");
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

}
