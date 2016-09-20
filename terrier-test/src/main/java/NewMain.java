import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;

import node.CMasterNode;
import node.CSlaveNode;
import node.IMasterNode;
import node.INode;
import node.ISlaveNode;

import org.apache.log4j.Logger;
import org.terrier.structures.Index;

import util.CUtil;
import configuration.CParameters;
import connections.CClient;
import connections.CServer;

public class NewMain {

	static final Logger logger = Logger.getLogger(NewMain.class);
	
	public static void main(String[] args) throws IOException {
		String opcion = args.length>0?args[0]:null;
		if (opcion!=null && INode.ID_MASTER.equals(opcion.toUpperCase())){
			if (args.length == 14 || args.length == 5){
				CParameters parameters = null;
				if (args.length == 14){
				parameters = new CParameters(INode.ID_MASTER,args[1],args[2],args[3],args[4],args[5], args[6], args[7], args[8], args[9], args[10], args[11], args[12], args[13]);
				}else if (args.length == 5){
					parameters = new CParameters(INode.ID_MASTER,args[1],args[2],args[3], args[4]);
				}
				createMaster(parameters);
			}else{
				mostrarMensajeParametros();
			}
		}else if (opcion!=null && INode.ID_SLAVE.equals(opcion.toUpperCase())){
			if (args.length == 2){
				createSlave(((String)args[1]).toLowerCase().equals("cycle"));
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
		if (CParameters.action_INDEX.equals(parameters.getAction().toUpperCase()) || CParameters.action_ALL.equals(parameters.getAction().toUpperCase())){
			logger.info("Tipo nodo: " + parameters.getTipoNodo());
			logger.info("Acción: " + parameters.getAction());
			logger.info("Master indexa: " + parameters.getMasterIndexa().toString());
			logger.info("Recrear corpus: " + parameters.getRecrearCorpus().toString());
			logger.info("Metodo particionamiento: " + parameters.getMetodoParticionamiento().getClass().getName());
			logger.info("Carpeta colección: " + (parameters.getCarpetaColeccion().trim().isEmpty()?"[Se utiliza el path indicado en la configuración]":parameters.getCarpetaColeccion()));
			logger.info("Cantidad nodos: " + parameters.getCantidadNodos());
			logger.info("Metodo de comunicación: " + parameters.getMetodoComunicacion());
			logger.info("Despertar Esclavos: " + parameters.getWakeUpSlaves());
			logger.info("Query: " + parameters.getQuery());
			logger.info("Eliminar corpus: " + parameters.getEliminarCorpus());
			logger.info("Mergear índices: " + parameters.getMergearIndices());
			logger.info("Nombre índice previo: " + parameters.getPreviousIndexName());
			logger.info("Nombre corrida: " + parameters.getRunName());
		}else if (CParameters.action_RETRIEVAL.equals(parameters.getAction().toUpperCase())){
			logger.info("Tipo nodo: " + parameters.getTipoNodo());
			logger.info("Acción: " + parameters.getAction());
			logger.info("Nombre Indice: " + parameters.getIndexName());
			logger.info("Query: " + parameters.getQuery());
		}
		logger.info("---------------------------------------");
		logger.info("----------   FIN PARAMETROS  ----------");
		logger.info("---------------------------------------");
		logger.info("");
		
		/* Creo Nodo Master */
		IMasterNode nodo = new CMasterNode(parameters);
		nodo.createSlaveNodes(nodo.getParameters().getCantidadNodos());
		if (parameters.getAction().toUpperCase().equals(CParameters.action_INDEX) || parameters.getAction().toUpperCase().equals(CParameters.action_ALL)){
			actionIndex(nodo);
		}
		if (parameters.getAction().toUpperCase().equals(CParameters.action_RETRIEVAL) || parameters.getAction().toUpperCase().equals(CParameters.action_ALL)){
			actionRetrieval(nodo);
		}
		/* Envio orden para cerrar esclavos */
		nodo.sendOrderToCloseSlaves();
		
		logger.info("");
		logger.info("******************************************************************************************************************************************************");
		logger.info("************************************************************************* FIN ************************************************************************");
		logger.info("******************************************************************************************************************************************************");
		logger.info("");
		
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private static void createSlave(Boolean cycle){
		/* Creo Nodo Esclavo */
		ISlaveNode slaveNode = new CSlaveNode(INode.ID_SLAVE);
		/* Creo Servidor */
		Integer port = slaveNode.getNodeConfiguration().getPort();
		CServer server = new CServer(port,slaveNode);
		server.listen(cycle);
	}
	
	private static void mostrarMensajeParametros(){
		logger.error("¡Parámetros incorrectos!");
		logger.error("Parámetros necesarios:");
		logger.error("1) Tipo de nodo --> master / slave");
		logger.error("2) Acción --> index / retrieval / all");
		logger.error("3) Master indexa --> S / N");
		logger.error("4) Recrear corpus --> S / N");
		logger.error("5) Metodo particionamiento --> 1,2,3,4 o 5");
		logger.error("\t1- Particionamiento por Documentos: RoundRobin");
		logger.error("\t2- Particionamiento por Documentos: Por tamaño (archivos)");
		logger.error("\t3- Particionamiento por Documentos: Por tamaño (cantidad de tokens unicos)");
		logger.error("\t4- Particionamiento por Terminos: RoundRobin");
		logger.error("\t5- Particionamiento por Terminos: Por tamaño");
		logger.error("6) Ruta documentos[Opcional] --> Ruta donde se encuentran los documentos que formarán el corpus (sobreescribe a la property \"folderPath\" de la configuración)");
		logger.error("7) Cantidad nodos --> 1 <= N");
		logger.error("8) Metodo de comunicación: SSH / PATH");
		logger.error("\t-SSH --> Requiere autenticación SSH");
		logger.error("\t-PATH --> Requiere que los nodos compartan la memoria");
		logger.error("9) Despertar esclavos --> S / N");
		logger.error("10) Query[Opcional] --> \"texto entre comillas\"");
		logger.error("11) Eliminar corpus --> S / N");
		logger.error("12) Mergear indices --> S / N");
		logger.error("14) Nombre índice previo --> 'noProcess_partitioning.CRoundRobinByDocuments' ");
		logger.error("13) Nombre corrida --> corrida1 ");
		logger.error("");
		logger.error("EJEMPLOS:");
		logger.error("\tjava -jar programa.jar master index S S 1 \"/home/jetchart/TERRIER_JAVA/master/coleccion/\" 2 ssh S \"\" N S corrida1");
		logger.error("\tjava -jar programa.jar master all S S 1 \"\" 2 ssh N \"hola como andas\" S S corrida2");
		logger.error("\tjava -jar programa.jar slave");
		logger.error("\tjava -java -jar programa.jar master retrieval \"jmeIndex\" \"hola como andas\"");
	}
   
	private static void actionIndex(IMasterNode nodo){
		/* Si en los parámetros se especificó una nueva carpeta se sobreescribe la de la configuración */
		if (!nodo.getParameters().getCarpetaColeccion().trim().isEmpty()){
			nodo.getNodeConfiguration().setFolderPath(nodo.getParameters().getCarpetaColeccion());
		}
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
		/* Envio orden para limpiar indices anteriores */
		nodo.sendOrderToCleanIndexes(Boolean.TRUE);
		logger.info("----------------------------------------------------------------------->");
		logger.info("                   INICIO PROCESO INDEXACION COMPLETO                  >");
		logger.info("----------------------------------------------------------------------->");
		Long inicio = System.currentTimeMillis();
		/* Creo Index */
		nodo.sendOrderToIndex(nodo.getParameters().getRecrearCorpus(), nodo.getParameters().getMetodoParticionamiento().getClass().getName());
		/* Copio los indices de los nodos y realizo un merge */
		if (nodo.getParameters().getMergearIndices()){
			/* Si se deben mergear los indices, pero el master no indexa va a tener la coleccion de corpus vacia. Por lo tanto
			 * le agrego el primer elemento, para poder crear el archivo .properties del merge */
			if (!nodo.getParameters().getMasterIndexa()){
				Collection<String> col = new ArrayList<String>();
				col.add(nodo.getColCorpusTotal().iterator().next());
				nodo.setColCorpus(col);
			}
//			nodo.copyIndexesFromSlaves();
			nodo.sendOrderToGetIndexSlaves();
			Integer minIndex = nodo.getParameters().getMasterIndexa()?0:1;
			Integer maxIndex = minIndex==0?nodo.getParameters().getCantidadNodos()-1:nodo.getParameters().getCantidadNodos();
			nodo.mergeIndexes(nodo.getNodeConfiguration().getTerrierHome() + "var/index/", "jmeIndex", minIndex, maxIndex);
		}
		Long fin = System.currentTimeMillis() - inicio;
		logger.info("Proceso de indexación completo tardó " + fin + " milisegundos");
		logger.info("<-----------------------------------------------------------------------");
		logger.info("<                    FIN PROCESO INDEXACION COMPLETO                    ");
		logger.info("<-----------------------------------------------------------------------");
		/* Eliminar corpus al terminar */
		if (nodo.getParameters().getEliminarCorpus()){
			nodo.sendOrderToDeleteCorpus();
		}
		/* Copio en el master los archivos .properties de los indices de los esclavos (para analizar los indices) */
		nodo.copyPropertiesFileFromSlaves();
		/* Si se mergearon los índices envio orden para eliminarlos en los esclavos */
		if (nodo.getParameters().getMergearIndices()){
			nodo.copiarIndexProperties(Boolean.TRUE);
			/* Indico FALSE ya que el índice del master se elimina en el proceso de MERGE */
			nodo.sendOrderToCleanIndexes(Boolean.FALSE);
		}
	}
	
	private static void actionRetrieval(IMasterNode nodo) throws Exception{
		/* TODO por el momento queda desactivada la opción de RETRIEVAL solamente, ya que habría que:
		 * 		1- Levantar el corpus anterior del Master --> OK!
		 * 		2- Levantar el indice anterior en el Master
		 * 		3- Indicar a cada esclavo que cada uno levante su indice anterior
		 * */
		if (nodo.getParameters().getAction().toUpperCase().equals(CParameters.action_RETRIEVAL)){
//			String lastMasterCorpusPath = CUtil.recuperarCollectionSpec().iterator().next();
//			List<String> col = new ArrayList<String>();
//			col.add(lastMasterCorpusPath);
//			nodo.setColCorpusTotal(col);
			nodo.setIndex(Index.createIndex(nodo.getNodeConfiguration().getTerrierHome() +"var/index/", nodo.getParameters().getIndexName()));
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