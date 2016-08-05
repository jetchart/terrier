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
			case "setColCorpus":
				/* Agrego el path de la porcion de corpus que se le asignó */
				Collection<String> col = new ArrayList<String>();
				String corpusPathOnMaster = array[1];
				/* TODO --> Pensar como hacer para que funcione en WINDOWS (por el tema de la barra invertida) */
				String nombreCorpusOnMaster = corpusPathOnMaster.split("/")[corpusPathOnMaster.split("/").length-1];
				String corpusPathOnSlave= configuration.getDestinationFolderPath()+nombreCorpusOnMaster;
				logger.info("corpusPathOnMaster: " + corpusPathOnMaster);
				logger.info("corpusPathOnSlave: " + corpusPathOnSlave);
				CUtil.copyFileSFTP(corpusPathOnMaster, corpusPathOnSlave, configuration.getUserSFTP(), configuration.getPasswordSFTP(), configuration.getMasterSFTPHost(), configuration.getMasterSFTPPort());
				col.add(corpusPathOnSlave);
				this.setColCorpus(col);
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
}
