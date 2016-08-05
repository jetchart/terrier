import java.io.IOException;

import node.CMasterNode;
import node.CSlaveNode;
import node.IMasterNode;
import node.INode;
import node.ISlaveNode;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import util.CUtil;
import Factory.CFactoryPartitionMethod;
import connections.CClient;
import connections.CServer;

public class NewMain {

	static final Logger logger = Logger.getLogger(NewMain.class);
	
	public static void main(String[] args) throws IOException {
		String opcion = args.length>0?args[0]:null;
		if (opcion!=null && INode.ID_MASTER.equals(opcion.toUpperCase())){
			if (args.length == 7){
				createMaster(args[1],args[2],args[3],args[4],args[5], args[6]);
			}else{
				mostrarMensajeParametros();
			}
		}else if (opcion!=null && INode.ID_SLAVE.equals(opcion.toUpperCase())){
			if (args.length != 1){
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
	
	private static void createMaster(String masterIndexa, String recrearCorpus, String metodoParticionamiento, String cantidadNodos, String metodoComunicacion, String query){
		try {
		/* Muestro los parametros recibidos */
		logger.info("Tipo nodo: " + INode.ID_MASTER);
		logger.info("Master indexa: " + masterIndexa);
		logger.info("Recrear corpus: " + recrearCorpus);
		logger.info("Metodo particionamiento: " + CFactoryPartitionMethod.getInstance(Integer.valueOf(metodoParticionamiento)).getClass().getName());
		logger.info("Cantidad nodos: " + cantidadNodos);
		logger.info("Metodo de comunicación: " + metodoComunicacion);
		logger.info("Query: " + query);
		/* Creo Nodo Master */
		IMasterNode nodo = new CMasterNode(INode.ID_MASTER);
		nodo.setIndexa("S".equals(masterIndexa)?Boolean.TRUE:Boolean.FALSE);
		if (recrearCorpus.equals("S")){
			Integer metodoId = Integer.parseInt(metodoParticionamiento);
			/* Instancio el metodo de particionamiento */
			nodo.setPartitionMethod(CFactoryPartitionMethod.getInstance(metodoId));
			/* Cantidad de corpus a crear */
			nodo.setCantidadCorpus(Integer.valueOf(cantidadNodos));
			nodo.createSlaveNodes(nodo.getCantidadCorpus());
		}
		/* Se parsea la query */
		query = CUtil.parseString(query);
		if (recrearCorpus.equals("S") || recrearCorpus.equals("s")){
			/* Creo Corpus */
			nodo.createCorpus();
		   	/* Agrego todos los corpus al archivo /etc/collection.spec/ para que se tengan en cuenta en la Indexacion */
			/* Cuando se tengan Slave nodes se dividirán los Corpus */
			nodo.sendCorpusToNodes();

		}
		/* Creo Index */
		nodo.sendOrderToIndex(recrearCorpus, nodo.getPartitionMethod().getClass().getName());
		/* Recupero */
		nodo.sendOrderToRetrieval(query);
		/* Mostrar resultados del master */
		if (nodo.getIndexa()){
			logger.info("\nResultados del Master:");
			CUtil.mostrarResultados(nodo.getResultSet(), nodo.getIndex(), query);
		}
		/* Mostrar resultados de los esclavos */
		for (CClient client : nodo.getNodes()){
			logger.info("\nResultados del Esclavo:" + client.getHost() + ":" + client.getPort());
			CUtil.mostrarResultados(client.getResultSetNodo(), null, query);
		}
		
		
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
		logger.info("¡Parámetro incorrectos!");
		logger.info("Parámetros necesarios:");
		logger.info("1) Tipo de nodo --> master / slave");
		logger.info("2) Master indexa --> 1 / 0");
		logger.info("3) Recrear corpus --> 1 / 0");
		logger.info("4) Metodo particionamiento --> 1,2,3,4");
		logger.info("\t1- Particionamiento por Documentos: RoundRobin");
		logger.info("\t2- Particionamiento por Documentos: Por tamaño (archivos)");
		logger.info("\t3- Particionamiento por Documentos: Por tamaño (cantidad de tokens unicos)");
		logger.info("\t4- Particionamiento por Terminos: RoundRobin");
		logger.info("\t5- Particionamiento por Terminos: Por tamaño");
		logger.info("5) Cantidad nodos --> 1 <= N");
		logger.info("6) Metodo de comunicación: 1 / 2");
		logger.info("\t1- Vía SSH");
		logger.info("\t2- Documentos compartidos");
		logger.info("7) Query --> \"texto entre comillas\"");
	}
   
}
