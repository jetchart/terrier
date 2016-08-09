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
		/* El parámetro Inicializar viene acompañado del metodoComunicacion, separado por _
		 * Por lo tanto se hace un split("_")*/
		if ("Inicializar".equals(cliente.getTarea().split("_")[0])){
//			cliente.enviar("setId" + CUtil.separator + cliente.getPort());
			String metodoComunicacion = cliente.getTarea().split("_")[1];
			cliente.enviar("setColCorpus_" + metodoComunicacion + CUtil.separator + cliente.getNodoColCorpus());
			cliente.recibir();
		}else if ("Indexar".equals(cliente.getTarea())){
			cliente.enviar("createIndex");
			cliente.recibir();		
		}else if ("LimpiarIndices".equals(cliente.getTarea())){
				cliente.enviar("cleanIndexes");
				cliente.recibir();
		}else if ("EliminarCorpus".equals(cliente.getTarea())){
			cliente.enviar("deleteCorpus");
			cliente.recibir();
		}else if ("Salir".equals(cliente.getTarea())){
			cliente.enviar("salir");
			cliente.recibir();
		}else if ("Recuperar".equals(cliente.getTarea())){
			cliente.enviar("retrieval" + CUtil.separator + cliente.getQuery());
			try {
				cliente.setResultSetNodo(cliente.recibirObjeto());
			} catch (ClassNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
}
