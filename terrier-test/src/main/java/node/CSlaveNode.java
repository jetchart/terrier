package node;

import java.util.ArrayList;
import java.util.Collection;
import java.util.regex.Pattern;

import util.CUtil;
import configuration.INodeConfiguration;
import connections.Server;


public class CSlaveNode extends CNode implements ISlaveNode {

	Server server;
	public CSlaveNode(Server server) {
		super();
		this.server = server;
	}

	public INodeConfiguration getNodeConfiguration() {
		// TODO Auto-generated method stub
		return null;
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
				col.add(array[1]);
				this.setColCorpus(col);
				/* Indexo */
				this.createIndex("S", "nombreIndice");
				break;
			case "createIndex":
				this.createIndex("S", "nombreIndice");
		        break;
		}
	}

}
