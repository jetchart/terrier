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
		if (CNode.task_INITIALIZE.equals(cliente.getTarea().split("_")[0])){
			cliente.enviar(cliente.getTarea() + CUtil.separator + cliente.getNodoColCorpus());
			cliente.recibir();
		}else if (CNode.task_CREATE_INDEX.equals(cliente.getTarea())){
			cliente.enviar(CNode.task_CREATE_INDEX);
			cliente.recibir();		
		}else if (CNode.task_CLEAN_INDEXES.equals(cliente.getTarea())){
				cliente.enviar(CNode.task_CLEAN_INDEXES);
				cliente.recibir();
		}else if (CNode.task_DELETE_CORPUS.equals(cliente.getTarea())){
			cliente.enviar(CNode.task_DELETE_CORPUS);
			cliente.recibir();
		}else if (CNode.task_CLOSE.equals(cliente.getTarea())){
			cliente.enviar(CNode.task_CLOSE);
			cliente.recibir();
		}else if (CNode.task_COPY_INDEX.equals(cliente.getTarea().split("_")[0])){
			cliente.enviar(cliente.getTarea());
			cliente.recibir();
		}else if (CNode.task_RETRIEVAL.equals(cliente.getTarea())){
			cliente.enviar(CNode.task_RETRIEVAL + CUtil.separator + cliente.getQuery());
			try {
				cliente.setResultSetNodo(cliente.recibirObjeto());
			} catch (ClassNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
}
