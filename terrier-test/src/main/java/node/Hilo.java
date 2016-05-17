package node;

import connections.CClient;
import util.CUtil;

public class Hilo extends Thread{

	CClient cliente; 
	
	public Hilo (CClient cliente){
		this.cliente = cliente;
	}
	@Override
	public void run() {
		if ("Inicializar".equals(cliente.getTarea())){
			cliente.enviar("setId" + CUtil.separator + cliente.getPort());
			cliente.enviar("setColCorpus" + CUtil.separator + cliente.getNodoColCorpus());
			cliente.recibir();
		}else if ("Indexar".equals(cliente.getTarea())){
			cliente.enviar("createIndex");
			cliente.recibir();
		}else if ("Recuperar".equals(cliente.getTarea())){
			cliente.enviar("retrieval" + CUtil.separator + cliente.getQuery());
			cliente.recibir();
		}
	}
}
