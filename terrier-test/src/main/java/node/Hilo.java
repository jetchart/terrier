package node;

import connections.Client;
import util.CUtil;

public class Hilo extends Thread{

	Client cliente; 
	
	public Hilo (Client cliente){
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
		}
	}
}
