package node;

import java.util.ArrayList;
import java.util.Collection;
import java.util.regex.Pattern;

import util.CUtil;
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
			case "setId":
				/* Defino Id del nodo */
				this.setId(Integer.parseInt(array[1]));
				break;
			case "setColCorpus_1":
				setColCorpus(array, "1");
				break;
			case "setColCorpus_2":
				setColCorpus(array, "2");
				break;
			case "createIndex":
				this.createIndex(Boolean.TRUE, "slaveNode_"+this.configuration.getIdNode());
		        break;
			case "retrieval":
				try {
					this.retrieval(array[1]);
				} catch (Exception e) {
					e.printStackTrace();
				}
		        break;
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
		/* Si el método de comunicacion es 1 (via SSH) se copia el archivo del master en el slave utilizando SFTP */
		if ("1".equals(metodoComunicacion)){
			/* TODO --> Pensar como hacer para que funcione en WINDOWS (por el tema de la barra invertida) */
			String nombreCorpusOnMaster = corpusPathOnMaster.split("/")[corpusPathOnMaster.split("/").length-1];
			corpusPathOnSlave = configuration.getDestinationFolderPath()+nombreCorpusOnMaster;
			logger.info("corpusPathOnMaster: " + corpusPathOnMaster);
			logger.info("corpusPathOnSlave: " + corpusPathOnSlave);
			CUtil.copyFileSFTP(corpusPathOnMaster, corpusPathOnSlave, configuration.getUserSFTP(), configuration.getPasswordSFTP(), configuration.getMasterSFTPHost(), configuration.getMasterSFTPPort());
		}
		/* Si el método de comunicacion es 2 (via PATH) se utiliza el archivo desde el mismo path que el master */
		else if ("2".equals(metodoComunicacion)){
			logger.info("Se eligió metodo de comunicación vía path, por lo tanto se utilizará el path del master: " + corpusPathOnMaster);
			corpusPathOnSlave = corpusPathOnMaster;
		}
		col.add(corpusPathOnSlave);
		this.setColCorpus(col);
	}
}
