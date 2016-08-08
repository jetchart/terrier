package node;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;

import org.apache.log4j.Logger;
import org.terrier.matching.ResultSet;

import partitioning.CRoundRobinByDocuments;
import partitioning.IPartitionByTerms;
import util.CUtil;
import configuration.CParameters;
import configuration.INodeConfiguration;
import connections.CClient;


public class CMasterNode extends CNode implements IMasterNode {

	static final Logger logger = Logger.getLogger(CMasterNode.class);
	
	/* Coleccion de todos los Corpus */
	private Collection<String> colCorpusTotal;
	/* Nodos que posee, incluyéndose a él mismo */
	private Collection<CClient> nodes;
	/* Parametros recibidos */
	CParameters parameters;
	
	public CMasterNode(CParameters parameters) {
		super(parameters.getTipoNodo());
		this.parameters = parameters;
	}
	
	public Collection<String> getColCorpusTotal() {
		return this.colCorpusTotal;
	}

	public INodeConfiguration getNodeConfiguration() {
		return this.configuration;
	}

	public Collection<CClient> getNodes() {
		return this.nodes;
	}
	
	/**
	 * Crea los nodos esclavos en base a la cantidad indicada, seteandole a cada uno su host y port 
	 * correspondiente (esta informacion la saca de {@link INodeConfiguration} configuration.
	 * En caso que el Master deba indexar, se creará cantidad-1 de nodos esclavos.
	 * @param cantidad	Cantidad de nodos esclavos a crear
	 */
	/* TODO Acá estaría bueno que se conecte por SSH a cada nodo y ejecute el programa .jar
	 */
	public void createSlaveNodes(Integer cantidad){
		nodes = new ArrayList<CClient>();
		/* Si el Master indexa, entonces reduzco en 1 la cantidad de nodos a crear */
		if (this.getParameters().getMasterIndexa()){
			cantidad--;
		}
		int i = 0;
		for (String nodo : configuration.getSlavesNodes()){
			i++;
			if (i > cantidad)
				break;
			String host = nodo.split(":")[0];
			Integer port = Integer.valueOf(nodo.split(":")[1]);
			String user = nodo.split(":")[2];
			String pass = nodo.split(":")[3];
			String jarPath = nodo.split(":")[4];
			String jarName = nodo.split(":")[5];
			CUtil.executeCommandSSH(host, 22, user, pass, "cd " + jarPath + "; java -jar " + jarName + " slave");
			CClient cliente = new CClient(host, port, user, pass, jarPath);
			nodes.add(cliente);
		}
	}
	
	public void createCorpus() {
		logger.info("------------------------------------");
		logger.info("COMIENZA CREACION DE CORPUS");
		logger.info("------------------------------------");
		Long inicioCreacionCorpus = System.currentTimeMillis();
		/* Si el metodo de particion es por terminos, necesitamos primero
		 * tener el indice, para obtener los terminos y dividirlos */
		if (this.getParameters().getMetodoParticionamiento() instanceof IPartitionByTerms){
			/* Utilizamos el metodo RoundRobin por documentos para crear el corpus */
			/* TODO se puede mejorar esto */
			colCorpusTotal = new CRoundRobinByDocuments().createCorpus(configuration.getFolderPath(), configuration.getDestinationFolderPath(), this.getParameters().getCantidadNodos(), null, parameters);
			this.setColCorpus(colCorpusTotal);
			this.createIndex(Boolean.TRUE, CRoundRobinByDocuments.class.getName());
			colCorpusTotal.clear();
		}else{
			this.index = null;
		}
		colCorpusTotal = this.getParameters().getMetodoParticionamiento().createCorpus(configuration.getFolderPath(), configuration.getDestinationFolderPath(), this.getParameters().getCantidadNodos(), this.index, parameters);
		/* Guardo backup del collection.spec */
		CUtil.guardarCollectionAnterior(colCorpusTotal);
		Long finCreacionCorpus = System.currentTimeMillis() - inicioCreacionCorpus;
		logger.info("Creación Corpus tardó " + finCreacionCorpus + " milisegundos");
		logger.info("------------------------------------");
		logger.info("FIN CREACION DE CORPUS");
		logger.info("------------------------------------");
	}
	

	public void sendCorpusToNodes() {
		logger.info("------------------------------------");
		logger.info("COMIENZA ENVIO DE CORPUS A NODOS");
		logger.info("------------------------------------");
		if ("1".equals(parameters.getMetodoComunicacion())){
			logger.info("Los nodos esclavos acceden a los corpus a través de una copia vía SFTP");
		}else if ("2".equals(parameters.getMetodoComunicacion())){
			logger.info("Los nodos esclavos acceden a los corpus utilizando el mismo path que el master");
		}else{
			logger.error("Método de comunicación INVÁLIDO: " + parameters.getMetodoComunicacion());
		}
		Long inicioSetCorpusToNodes = System.currentTimeMillis();
		/* Agrego corpus al master node solo si indexa */
		Iterator<String> iterator = this.getColCorpusTotal().iterator();
		if (this.getParameters().getMasterIndexa()){
			String corpusMasterNode = (String) iterator.next();
			Collection<String> colCorpusMasterNode = new ArrayList<String>();
			colCorpusMasterNode.add(corpusMasterNode);
			this.setColCorpus(colCorpusMasterNode);
		}
		/* Agrego corpus a cada slave node */
		Collection<Hilo> hilos = new ArrayList<Hilo>();
		for (CClient cliente : nodes){
			/* Le envío la orden para inicializar, junto con el método de comunicación (via Path o por SCP) */
			cliente.setTarea("Inicializar_" + parameters.getMetodoComunicacion());
			cliente.setNodoColCorpus((String) iterator.next());
			Hilo hilo = new Hilo(cliente);
			hilo.start();
			hilos.add(hilo);
		}
		/* Espero a que todos los nodos terminen */
		esperar(hilos);
		Long finSetCorpusToNodes = System.currentTimeMillis() - inicioSetCorpusToNodes;
		logger.info("Enviar corpus a Nodos tardó " + finSetCorpusToNodes + " milisegundos");
		logger.info("------------------------------------");
		logger.info("FIN ENVIO DE CORPUS A NODOS");
		logger.info("------------------------------------");
	}
	
	public void sendOrderToIndex(Boolean recrearCorpus, String methodPartitionName) {
		logger.info("------------------------------------");
		logger.info("COMIENZA INDEXACION");
		logger.info("------------------------------------");
		Long inicioIndexacion = System.currentTimeMillis();
		/* Envio indicacion para indexar a cada nodo */
		Collection<Hilo> hilos = new ArrayList<Hilo>();
		for (CClient cliente : nodes){
			cliente.setTarea("Indexar");
			Hilo hilo = new Hilo(cliente);
			hilo.start();
			hilos.add(hilo);
		}
		/* Indexo yo mismo si así se indica */
		if (this.getParameters().getMasterIndexa()){
			createIndex(recrearCorpus, "masterNode");
		}
		/* Espero a que todos los nodos terminen */
		esperar(hilos);
		Long finIndexacion = System.currentTimeMillis() - inicioIndexacion;
		logger.info("Indexación TOTAL tardó " + finIndexacion + " milisegundos");
		logger.info("------------------------------------");
		logger.info("FIN INDEXACION");
		logger.info("------------------------------------");
	}

	public Collection<ResultSet> sendOrderToRetrieval(String query) throws Exception {
		logger.info("------------------------------------");
		logger.info("COMIENZA RECUPERACION");
		logger.info("------------------------------------");
		Long inicioRecuperacion = System.currentTimeMillis();
		/* Envio indicacion para indexar a cada nodo */
		Collection<Hilo> hilos = new ArrayList<Hilo>();
		for (CClient cliente : nodes){
			cliente.setTarea("Recuperar");
			cliente.setQuery(query);
			Hilo hilo = new Hilo(cliente);
			hilo.start();
			hilos.add(hilo);
		}
		/* Recupero yo mismo si tuve que indexar */
		if (this.getParameters().getMasterIndexa()){
			retrieval(query);
		}
		/* Espero a que todos los nodos terminen */
		esperar(hilos);
		Long finRecuperacion = System.currentTimeMillis() - inicioRecuperacion;
		logger.info("Recuperacion TOTAL tardó " + finRecuperacion + " milisegundos");
		logger.info("------------------------------------");
		logger.info("FIN RECUPERACION");
		logger.info("------------------------------------");
		return null;

	}
	
	/**
	 * El metodo esperar debe aguardar a que todos los hilos de la coleccion que recibe, 
	 * terminen su ejecución.
	 * @param hilos
	 */
	public void esperar(Collection<Hilo> hilos){
		Boolean continuar = true;
		while (continuar){
			continuar = false;
			for (Hilo hilo: hilos){
				if (hilo.isAlive())
					continuar = true;
			}
		}
	}

	@Override
	public CParameters getParameters() {
		return parameters;
	}

	@Override
	public void setParameters(CParameters parameters) {
		this.parameters = parameters;
	}

	@Override
	public void setColCorpusTotal(Collection<String> colCorpusTotal) {
		this.colCorpusTotal = colCorpusTotal;
	}

}
