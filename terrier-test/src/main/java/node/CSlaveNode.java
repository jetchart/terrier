package node;

import java.util.ArrayList;
import java.util.Collection;
import java.util.regex.Pattern;

import util.CUtil;
import configuration.CParameters;
import configuration.INodeConfiguration;
import connections.CServer;


public class CSlaveNode extends CNode implements ISlaveNode {

	CServer server;

	public CSlaveNode(String nodeType) {
		super(nodeType);
	}

	public INodeConfiguration getNodeConfiguration() {
		return configuration;
	}

	public void executeMessage(String msg) {
		String[] array = msg.split(Pattern.quote(CUtil.separator));
		switch (array[0]) {
			case task_INITIALIZE + "_SSH":
				setColCorpus(array, "SSH");
				/* Si la comunicación es por SSH, se deberán transferir los índices al momento de mergear, en
				 * cambio si es PATH, directamente se crearán los índices de los nodos en la carpeta del Master */
				configuration.setIndexPath(configuration.getTerrierHome() +"var/index/");
				break;
			case task_INITIALIZE + "_PATH":
				setColCorpus(array, "PATH");
				break;
			case task_CREATE_INDEX:
				this.createIndex(INodeConfiguration.prefixIndex, this.configuration.getIdNode());
				this.copiarIndexProperties(Boolean.FALSE);
		        break;
			case task_CLEAN_INDEXES:
				CUtil.deleteIndexFiles(configuration.getIndexPath(), INodeConfiguration.prefixIndex + this.configuration.getIdNode());
		        break;
			case task_DELETE_CORPUS:
				this.eliminarCorpus(colCorpus);
		        break;
			case task_COPY_INDEX + "_PATH":
				this.copyIndexToMaster(array[1], "PATH");
		        break;
			case task_COPY_INDEX + "_SSH":
				this.copyIndexToMaster(array[1], "SSH");
		        break;
			case task_RETRIEVAL:
				try {
					this.retrieval(array[1]);
				} catch (Exception e) {
					e.printStackTrace();
				}
		        break;
		}
	}

	private void copyIndexToMaster(String indexMasterPath, String metodoComunicacion) {
		for (String indexFile : CUtil.indexFiles){
			String pathOnSlave = configuration.getIndexPath() + INodeConfiguration.prefixIndex + this.getId() + indexFile;
			String pathOnMaster = indexMasterPath + INodeConfiguration.prefixIndex + this.getId() + indexFile;
			logger.info("pathOnSlave: " + pathOnSlave);
			logger.info("pathOnMaster: " + pathOnMaster);
			if (CParameters.metodoComunicacion_SSH.equals(metodoComunicacion)){
				CUtil.copyFileSFTP(pathOnSlave, pathOnMaster, configuration.getUserSFTP(), configuration.getPasswordSFTP(), configuration.getMasterSFTPHost(), configuration.getMasterSFTPPort());
			}else if (CParameters.metodoComunicacion_PATH.equals(metodoComunicacion)){
				CUtil.copyFile(pathOnSlave, pathOnMaster);
			}
		}
	}

	public CServer getServer() {
		return server;
	}

	public void setServer(CServer server) {
		this.server = server;
	}
	
	private void setColCorpus(String[] array, String metodoComunicacion){
		/* Agrego el path de la porcion de corpus que se le asignó */
		Collection<String> col = new ArrayList<String>();
		String corpusPathOnMaster = array[1];
		String corpusPathOnSlave = null;
		/* Si el método de comunicacion via SSH copia el archivo del master en el slave utilizando SFTP */
		if (CParameters.metodoComunicacion_SSH.equals(metodoComunicacion)){
			/* TODO --> Pensar como hacer para que funcione en WINDOWS (por el tema de la barra invertida) */
			String nombreCorpusOnMaster = corpusPathOnMaster.split("/")[corpusPathOnMaster.split("/").length-1];
			corpusPathOnSlave = configuration.getDestinationFolderPath()+nombreCorpusOnMaster;
			logger.info("corpusPathOnMaster: " + corpusPathOnMaster);
			logger.info("corpusPathOnSlave: " + corpusPathOnSlave);
			this.copiarCorpusDesdeMaster(corpusPathOnMaster, corpusPathOnSlave, configuration.getUserSFTP(), configuration.getPasswordSFTP(), configuration.getMasterSFTPHost(), configuration.getMasterSFTPPort());
		}
		/* Si el método de comunicacion via PATH utiliza el archivo desde el mismo path que el master */
		else if (CParameters.metodoComunicacion_PATH.equals(metodoComunicacion)){
			logger.info("Se eligió metodo de comunicación vía path, por lo tanto se utilizará el path del master: " + corpusPathOnMaster);
			corpusPathOnSlave = corpusPathOnMaster;
		}
		col.add(corpusPathOnSlave);
		this.setColCorpus(col);
	}

	@Override
	public void copiarCorpusDesdeMaster(String corpusPathOnMaster,String corpusPathOnSlave, String userSFTP, String passwordSFTP,String masterSFTPHost, Integer masterSFTPPort) {
		logger.info("------------------------------------");
		logger.info("INICIO COPIA DE CORPUS DESDE MASTER A SLAVE");
		logger.info("------------------------------------");
		Long inicioCopiaCorpus = System.currentTimeMillis();
		logger.info("Se eligió metodo de comunicación vía SSH");
		CUtil.copyFileSFTP(corpusPathOnMaster, corpusPathOnSlave, configuration.getUserSFTP(), configuration.getPasswordSFTP(), configuration.getMasterSFTPHost(), configuration.getMasterSFTPPort());
		Long finCopiaCorpus = System.currentTimeMillis() - inicioCopiaCorpus;
		logger.info("Copia del corpus tardó " + finCopiaCorpus + " milisegundos");
		logger.info("------------------------------------");
		logger.info("FIN COPIA DE CORPUS DESDE MASTER A SLAVE");
		logger.info("------------------------------------");
	}
}
