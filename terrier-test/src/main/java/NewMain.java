import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import node.CMasterNode;
import node.CSlaveNode;
import node.IMasterNode;
import node.INode;
import node.ISlaveNode;

import org.apache.log4j.Logger;

import util.CUtil;
import configuration.CParameters;
import connections.CClient;
import connections.CServer;

public class NewMain {

	static final Logger logger = Logger.getLogger(NewMain.class);
	
	public static void main(String[] args) throws IOException {
		
		String opcion = args.length>0?args[0]:null;
		if (opcion!=null && INode.ID_MASTER.equals(opcion.toUpperCase())){
			if (args.length == 10){
				CParameters parameters = new CParameters(INode.ID_MASTER,args[1],args[2],args[3],args[4],args[5], args[6], args[7], args[8], args[9]);
				createMaster(parameters);
			}else{
				mostrarMensajeParametros();
			}
		}else if (opcion!=null && INode.ID_SLAVE.equals(opcion.toUpperCase())){
			if (args.length == 1){
				/* TODO creo que habría que sacar esto */
				/* Si se especificó un puerto lo utilizo, sino lo extraigo de configuration.properties */
				Integer port = null;
				createSlave(port);
			}else{
				mostrarMensajeParametros();
			}
		}else{
			mostrarMensajeParametros();
		}
	}
	
	private static void createMaster(CParameters parameters){
		try {
		/* Muestro los parametros recibidos */
		logger.info("******************************************************************************************************************************************************");
		logger.info("*********************************************************************** INICIO ***********************************************************************");
		logger.info("******************************************************************************************************************************************************");
		logger.info("");
		logger.info("---------------------------------------");
		logger.info("---------- INICIO PARAMETROS ----------");
		logger.info("---------------------------------------");
		logger.info("Tipo nodo: " + parameters.getTipoNodo());
		logger.info("Acción: " + parameters.getAction());
		logger.info("Master indexa: " + parameters.getMasterIndexa().toString());
		logger.info("Recrear corpus: " + parameters.getRecrearCorpus().toString());
		logger.info("Metodo particionamiento: " + parameters.getMetodoParticionamiento().getClass().getName());
		logger.info("Carpeta colección: " + (parameters.getCarpetaColeccion().trim().isEmpty()?"[Se utiliza el path indicado en la configuración]":parameters.getCarpetaColeccion()));
		logger.info("Cantidad nodos: " + parameters.getCantidadNodos());
		logger.info("Metodo de comunicación: " + parameters.getMetodoComunicacion());
		logger.info("Query: " + parameters.getQuery());
		logger.info("Eliminar corpus: " + parameters.getEliminarCorpus());
		logger.info("---------------------------------------");
		logger.info("----------   FIN PARAMETROS  ----------");
		logger.info("---------------------------------------");
		logger.info("");
		
		/* Creo Nodo Master */
		IMasterNode nodo = new CMasterNode(parameters);
		/* Si en los parámetros se especificó una nueva carpeta se sobreescribe la de la configuración */
		if (!parameters.getCarpetaColeccion().trim().isEmpty()){
			nodo.getNodeConfiguration().setFolderPath(parameters.getCarpetaColeccion());
		}
		nodo.createSlaveNodes(nodo.getParameters().getCantidadNodos());
		if (parameters.getAction().toUpperCase().equals(CParameters.action_INDEX) || parameters.getAction().toUpperCase().equals(CParameters.action_ALL)){
			actionIndex(nodo);
		}
		if (parameters.getAction().toUpperCase().equals(CParameters.action_RETRIEVAL) || parameters.getAction().toUpperCase().equals(CParameters.action_ALL)){
			actionRetrieval(nodo);
		}
		logger.info("");
		logger.info("******************************************************************************************************************************************************");
		logger.info("************************************************************************* FIN ************************************************************************");
		logger.info("******************************************************************************************************************************************************");
		logger.info("");
		
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private static void createSlave(Integer port){
		/* Creo Nodo Esclavo */
		ISlaveNode slaveNode = new CSlaveNode(INode.ID_SLAVE);
		/* Creo Servidor */
		if (port == null){
			port = slaveNode.getNodeConfiguration().getPort();
		}
		CServer server = new CServer(port,slaveNode);
		server.listen();
	}
	
	private static void mostrarMensajeParametros(){
		logger.error("¡Parámetros incorrectos!");
		logger.error("Parámetros necesarios:");
		logger.error("1) Tipo de nodo --> master / slave");
		logger.error("2) Acción --> index / retrieval / all");
		logger.error("3) Master indexa --> S / N");
		logger.error("4) Recrear corpus --> S / N");
		logger.error("5) Metodo particionamiento --> 1,2,3,4");
		logger.error("\t1- Particionamiento por Documentos: RoundRobin");
		logger.error("\t2- Particionamiento por Documentos: Por tamaño (archivos)");
		logger.error("\t3- Particionamiento por Documentos: Por tamaño (cantidad de tokens unicos)");
		logger.error("\t4- Particionamiento por Terminos: RoundRobin");
		logger.error("\t5- Particionamiento por Terminos: Por tamaño");
		logger.error("6) Cantidad nodos --> 1 <= N");
		logger.error("7) Ruta documentos[Opcional] --> Ruta donde se encuentran los documentos que formarán el corpus (sobreescribe a la property \"folderPath\" de la configuración)");
		logger.error("8) Metodo de comunicación: SSH / PATH");
		logger.error("\t-SSH --> Requiere autenticación SSH (no despierta a los esclavos)");
		logger.error("\t-PATH --> Requiere que los nodos compartan la memoria");
		logger.error("9) Query[Opcional] --> \"texto entre comillas\"");
		logger.error("10) Eliminar corpus --> S / N");
		logger.error("");
		logger.error("EJEMPLOS:");
		logger.error("\tjava -jar programa.jar master index S S 1 \"/home/jetchart/TERRIER_JAVA/master/coleccion/\" 2 ssh \"\" N");
		logger.error("\tjava -jar programa.jar master all S S 1 \"\" 2 ssh \"hola como andas\" S");
	}
   
	private static void actionIndex(IMasterNode nodo){
		/* Creo los Corpus (la cantidad será nodo.getParameters().getCantidadNodos()) */
	   	/* Agrego todos los corpus al archivo /etc/collection.spec/ para que se tengan en cuenta en la Indexacion */
		if (nodo.getParameters().getRecrearCorpus()){
			/* Creo Corpus */
			nodo.createCorpus();
			/* Muestro los tamaños de los corpus */
			nodo.showCorpusSize();
		}else{
			/* Si se eligió no recrear los corpus, se levantan los de la corrida anterior (los que se encuentra en el archivo /etc/collection.spec/) */
			nodo.setColCorpusTotal(CUtil.recuperarCollectionSpec());
		}
		/* Envío los corpus a los nodos */
		nodo.sendCorpusToNodes();
		/* Creo Index */
		nodo.sendOrderToIndex(nodo.getParameters().getRecrearCorpus(), nodo.getParameters().getMetodoParticionamiento().getClass().getName());
		/* Eliminar corpus al terminar */
		if (nodo.getParameters().getEliminarCorpus()){
			nodo.eliminarCorpus(nodo.getColCorpusTotal());
		}
	}
	
	private static void actionRetrieval(IMasterNode nodo) throws Exception{
		/* TODO por el momento queda desactivada la opción de RETRIEVAL solamente, ya que habría que:
		 * 		1- Levantar el corpus anterior del Master --> OK!
		 * 		2- Levantar el indice anterior en el Master
		 * 		3- Indicar a cada esclavo que cada uno levante su indice anterior
		 * */
		if (nodo.getParameters().getAction().toUpperCase().equals(CParameters.action_RETRIEVAL)){
			String lastMasterCorpusPath = CUtil.recuperarCollectionSpec().iterator().next();
			List<String> col = new ArrayList<String>();
			col.add(lastMasterCorpusPath);
			nodo.setColCorpusTotal(col);
		}
		/* Se parsea la query */
		nodo.getParameters().setQuery(CUtil.parseString(nodo.getParameters().getQuery()));
		/* Recupero */
		nodo.sendOrderToRetrieval(nodo.getParameters().getQuery());
		/* Mostrar resultados del master */
		if (nodo.getParameters().getMasterIndexa()){
			logger.info("");
			logger.info("Resultados del Master:");
			CUtil.mostrarResultados(nodo.getResultSet(), nodo.getIndex(), nodo.getParameters().getQuery());
		}
		/* Mostrar resultados de los esclavos */
		for (CClient client : nodo.getNodes()){
			logger.info("");
			logger.info("Resultados del Esclavo:" + client.getHost() + ":" + client.getPort());
			CUtil.mostrarResultados(client.getResultSetNodo(), null, nodo.getParameters().getQuery());
		}
	}
}
