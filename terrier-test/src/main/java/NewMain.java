import java.io.IOException;

import node.CMasterNode;
import node.CSlaveNode;
import node.IMasterNode;
import node.INode;
import node.ISlaveNode;

import org.apache.log4j.Logger;

import util.CUtil;
import Factory.CFactoryPartitionMethod;
import configuration.CParameters;
import connections.CClient;
import connections.CServer;

public class NewMain {

	static final Logger logger = Logger.getLogger(NewMain.class);
	
	public static void main(String[] args) throws IOException {
		
		String opcion = args.length>0?args[0]:null;
		if (opcion!=null && INode.ID_MASTER.equals(opcion.toUpperCase())){
			if (args.length == 7){
				CParameters parameters = new CParameters(INode.ID_MASTER,args[1],args[2],args[3],args[4],args[5], args[6]);
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
		logger.info("Master indexa: " + parameters.getMasterIndexa().toString());
		logger.info("Recrear corpus: " + parameters.getRecrearCorpus().toString());
		logger.info("Metodo particionamiento: " + parameters.getMetodoParticionamiento().getClass().getName());
		logger.info("Cantidad nodos: " + parameters.getCantidadNodos());
		logger.info("Metodo de comunicación: " + parameters.getMetodoComunicacion());
		logger.info("Query: " + parameters.getQuery());
		logger.info("---------------------------------------");
		logger.info("----------   FIN PARAMETROS  ----------");
		logger.info("---------------------------------------");
		logger.info("");
		/* Creo Nodo Master */
		IMasterNode nodo = new CMasterNode(parameters);
		nodo.createSlaveNodes(nodo.getParameters().getCantidadNodos());
		/* Creo Corpus */
	   	/* Agrego todos los corpus al archivo /etc/collection.spec/ para que se tengan en cuenta en la Indexacion */
		/* Cuando se tengan Slave nodes se dividirán los Corpus */
		if (parameters.getRecrearCorpus()){
			/* Creo Corpus */
			nodo.createCorpus();
		}else{
			/* Si se eligió no recrear los corpus, se levantan los de la corrida anterior (los que se encuentra en el archivo /etc/collection.spec/) */
			nodo.setColCorpusTotal(CUtil.recuperarCollectionSpec());
		}
		/* Se parsea la query */
		parameters.setQuery(CUtil.parseString(parameters.getQuery()));
		/* Envío los corpus a los nodos */
		nodo.sendCorpusToNodes();
		/* Creo Index */
		nodo.sendOrderToIndex(parameters.getRecrearCorpus(), nodo.getParameters().getMetodoParticionamiento().getClass().getName());
		/* Recupero */
		nodo.sendOrderToRetrieval(parameters.getQuery());
		/* Mostrar resultados del master */
		if (nodo.getParameters().getMasterIndexa()){
			logger.info("");
			logger.info("Resultados del Master:");
			CUtil.mostrarResultados(nodo.getResultSet(), nodo.getIndex(), parameters.getQuery());
		}
		/* Mostrar resultados de los esclavos */
		for (CClient client : nodo.getNodes()){
			logger.info("");
			logger.info("Resultados del Esclavo:" + client.getHost() + ":" + client.getPort());
			CUtil.mostrarResultados(client.getResultSetNodo(), null, parameters.getQuery());
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
		logger.error("2) Master indexa --> S / N");
		logger.error("3) Recrear corpus --> S / N");
		logger.error("4) Metodo particionamiento --> 1,2,3,4");
		logger.error("\t1- Particionamiento por Documentos: RoundRobin");
		logger.error("\t2- Particionamiento por Documentos: Por tamaño (archivos)");
		logger.error("\t3- Particionamiento por Documentos: Por tamaño (cantidad de tokens unicos)");
		logger.error("\t4- Particionamiento por Terminos: RoundRobin");
		logger.error("\t5- Particionamiento por Terminos: Por tamaño");
		logger.error("5) Cantidad nodos --> 1 <= N");
		logger.error("6) Metodo de comunicación: 1 / 2");
		logger.error("\t1- Vía SSH");
		logger.error("\t2- Documentos compartidos");
		logger.error("7) Query --> \"texto entre comillas\"");
	}
   
}
