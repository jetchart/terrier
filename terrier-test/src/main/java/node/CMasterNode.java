package node;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;

import org.apache.log4j.Logger;
import org.terrier.matching.ResultSet;

import partitioning.CRoundRobinByDocuments;
import partitioning.IPartitionByTerms;
import partitioning.IPartitionMethod;
import configuration.CParameters;
import configuration.INodeConfiguration;
import connections.CClient;


public class CMasterNode extends CNode implements IMasterNode {

	static final Logger logger = Logger.getLogger(CMasterNode.class);
	
	/* Coleccion de todos los Corpus */
	private Collection<String> colCorpusTotal;
	/* Nodos que posee, incluyéndose a él mismo */
	private Collection<CClient> nodes;
	/* Metodo de particionamiento del corpus */
	private IPartitionMethod partitionMethod;
	/* Cantidad de corpus (para repartir entre nodos) */
	private Integer cantidadCorpus;
	/* Indica si el Master indexa o no */
	private Boolean indexa;
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

	public IPartitionMethod getPartitionMethod() {
		return partitionMethod;
	}

	public void setPartitionMethod(IPartitionMethod partitionMethod) {
		this.partitionMethod = partitionMethod;
	}

	public int getCantidadCorpus() {
		return cantidadCorpus;
	}

	public void setCantidadCorpus(Integer cantidadCorpus) {
		this.cantidadCorpus = cantidadCorpus;
	}
	
	public Boolean getIndexa() {
		return indexa;
	}

	public void setIndexa(Boolean indexa) {
		this.indexa = indexa;
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
		if (indexa){
			cantidad--;
		}
		int i = 0;
		for (String nodo : configuration.getSlavesNodes()){
			i++;
			if (i > cantidad)
				break;
			String host = nodo.split(":")[0];
			Integer port = Integer.valueOf(nodo.split(":")[1]);
			CClient cliente = new CClient(host, port);
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
		if (partitionMethod instanceof IPartitionByTerms){
			/* Utilizamos el metodo RoundRobin por documentos para crear el corpus */
			/* TODO se puede mejorar esto */
			colCorpusTotal = new CRoundRobinByDocuments().createCorpus(configuration.getFolderPath(), configuration.getDestinationFolderPath(), cantidadCorpus, null);
			this.setColCorpus(colCorpusTotal);
			this.createIndex("S", CRoundRobinByDocuments.class.getName());
			colCorpusTotal.clear();
		}else{
			this.index = null;
		}
		colCorpusTotal = partitionMethod.createCorpus(configuration.getFolderPath(), configuration.getDestinationFolderPath(), cantidadCorpus, this.index);
		Long finCreacionCorpus = System.currentTimeMillis() - inicioCreacionCorpus;
		logger.info("Creación Corpus tardó " + finCreacionCorpus + " milisegundos");
	}
	

	public void sendCorpusToNodes() {
		logger.info("------------------------------------");
		logger.info("COMIENZA ENVIO DE CORPUS A NODOS");
		logger.info("------------------------------------");
		Long inicioSetCorpusToNodes = System.currentTimeMillis();
		/* Agrego corpus al master node solo si indexa */
		Iterator<String> iterator = this.getColCorpusTotal().iterator();
		if (indexa){
			String corpusMasterNode = (String) iterator.next();
			Collection<String> colCorpusMasterNode = new ArrayList<String>();
			colCorpusMasterNode.add(corpusMasterNode);
			this.setColCorpus(colCorpusMasterNode);
		}
		/* Agrego corpus a cada slave node */
		Collection<Hilo> hilos = new ArrayList<Hilo>();
		for (CClient cliente : nodes){
			cliente.setTarea("Inicializar");
			cliente.setNodoColCorpus((String) iterator.next());
			Hilo hilo = new Hilo(cliente);
			hilo.start();
			hilos.add(hilo);
		}
		/* Espero a que todos los nodos terminen */
		esperar(hilos);
		Long finSetCorpusToNodes = System.currentTimeMillis() - inicioSetCorpusToNodes;
		logger.info("Enviar corpus a Nodos tardó " + finSetCorpusToNodes + " milisegundos");
	}
	
	public void sendOrderToIndex(String recrearCorpus, String methodPartitionName) {
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
		if (indexa){
			createIndex(recrearCorpus, "masterNode");
		}
		/* Espero a que todos los nodos terminen */
		esperar(hilos);
		Long finIndexacion = System.currentTimeMillis() - inicioIndexacion;
		logger.info("Indexación TOTAL tardó " + finIndexacion + " milisegundos");
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
		if (indexa){
			retrieval(query);
		}
		/* Espero a que todos los nodos terminen */
		esperar(hilos);
		Long finRecuperacion = System.currentTimeMillis() - inicioRecuperacion;
		logger.info("Recuperacion TOTAL tardó " + finRecuperacion + " milisegundos");

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

}
