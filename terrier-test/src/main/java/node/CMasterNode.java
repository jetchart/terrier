package node;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;

import org.terrier.matching.ResultSet;

import partitioning.CRoundRobinByDocuments;
import partitioning.IPartitionByTerms;
import partitioning.IPartitionMethod;
import configuration.INodeConfiguration;
import connections.Client;


public class CMasterNode extends CNode implements IMasterNode {

	/* Coleccion de todos los Corpus */
	private Collection<String> colCorpusTotal;
	/* Nodos que posee, incluyéndose a él mismo */
	private Collection<Client> nodes;
	/* Metodo de particionamiento del corpus */
	private IPartitionMethod partitionMethod;
	/* Cantidad de corpus (para repartir entre nodos) */
	private int cantidadCorpus;
	/* TODO esto es temporal!!!!!!!!*/
	String nodesHost = "localhost";
	int fromPort = 1234;
	
	public CMasterNode() {
		super();
	}
	
	public void createCorpus() {
		System.out.println("------------------------------------");
		System.out.println("COMIENZA CREACION DE CORPUS");
		System.out.println("------------------------------------");
		Long inicioCreacionCorpus = System.currentTimeMillis();
		/* Si el metodo de particion es por terminos, necesitamos primero
		 * tener el indice, para obtener los terminos y dividirlos */
		if (partitionMethod instanceof IPartitionByTerms){
			/* Utilizamos el metodo RoundRobin por documentos para crear el corpus */
			/* TODO se puede mejorar esto */
			colCorpusTotal = new CRoundRobinByDocuments().createCorpus(configuration.getFolderPath(), configuration.getDestinationFolderPath(), cantidadCorpus, null);
			this.createIndex("S", CRoundRobinByDocuments.class.getName());
			colCorpusTotal.clear();
		}else{
			this.index = null;
		}
		colCorpusTotal = partitionMethod.createCorpus(configuration.getFolderPath(), configuration.getDestinationFolderPath(), cantidadCorpus, this.index);
		Long finCreacionCorpus = System.currentTimeMillis() - inicioCreacionCorpus;
		System.out.println("Creación Corpus tardó " + finCreacionCorpus + " milisegundos");
	}
	
	public Collection<Client> getNodes() {
		return this.nodes;
	}

	public void sendOrderToIndex(String recrearCorpus, String methodPartitionName) {
		System.out.println("------------------------------------");
		System.out.println("COMIENZA INDEXACION");
		System.out.println("------------------------------------");
		Long inicioIndexacion = System.currentTimeMillis();
		/* Envio indicacion para indexar a cada nodo */
		Collection<Hilo> hilos = new ArrayList<Hilo>();
		for (Client cliente : nodes){
			cliente.setTarea("Indexar");
			Hilo hilo = new Hilo(cliente);
			hilo.start();
			hilos.add(hilo);
		}
		/* Indexo yo mismo */
		createIndex(recrearCorpus, methodPartitionName);
		/* Espero a que todos los nodos terminen */
		esperar(hilos);
		Long finIndexacion = System.currentTimeMillis() - inicioIndexacion;
		System.out.println("Indexación TOTAL tardó " + finIndexacion + " milisegundos");
	}

	public Collection<ResultSet> sendOrderToRetrieval(Collection<INode> nodes, String query) {
		Collection<ResultSet> colResultSet = new ArrayList<ResultSet>();
		for (INode node : nodes){
			colResultSet.add(node.retrieval(query));
		}
		return colResultSet;
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

	public void setCantidadCorpus(int cantidadCorpus) {
		this.cantidadCorpus = cantidadCorpus;
	}
	
	public void createSlaveNodes(int cantidad){
		nodes = new ArrayList<Client>();
		int i;
		int port = 1234;
		for (i=0;i<cantidad-1;i++){
			Client cliente = new Client("localhost", port+i);
			nodes.add(cliente);
		}
	}

	public void sendCorpusToNodes() {
		System.out.println("------------------------------------");
		System.out.println("COMIENZA ENVIO DE CORPUS A NODOS");
		System.out.println("------------------------------------");
		Long inicioSetCorpusToNodes = System.currentTimeMillis();
		/* Agrego corpus al master node */
		Iterator<String> iterator = this.getColCorpusTotal().iterator();
		String corpusMasterNode = (String) iterator.next();
		Collection<String> colCorpusMasterNode = new ArrayList<String>();
		colCorpusMasterNode.add(corpusMasterNode);
		this.setColCorpus(colCorpusMasterNode);
		/* Agrego corpus a cada slave node */
		Collection<Hilo> hilos = new ArrayList<Hilo>();
		for (Client cliente : nodes){
			cliente.setTarea("Inicializar");
			cliente.setNodoColCorpus((String) iterator.next());
			Hilo hilo = new Hilo(cliente);
			hilo.start();
			hilos.add(hilo);
		}
		/* Espero a que todos los nodos terminen */
		esperar(hilos);
		Long finSetCorpusToNodes = System.currentTimeMillis() - inicioSetCorpusToNodes;
		System.out.println("Enviar corpus a Nodos tardó " + finSetCorpusToNodes + " milisegundos");
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
}
