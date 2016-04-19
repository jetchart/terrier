package node;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.terrier.matching.ResultSet;

import partitioning.CRoundRobinByDocuments;
import partitioning.IPartitionByTerms;
import partitioning.IPartitionMethod;
import util.CUtil;
import configuration.INodeConfiguration;
import connections.Client;


public class CMasterNode extends CNode implements IMasterNode {

	/* Coleccion de todos los Corpus */
	private Collection<String> colCorpusTotal;
	/* Nodos que posee, incluyéndose a él mismo */
	private Map<Integer,String> nodes;
	/* Metodo de particionamiento del corpus */
	private IPartitionMethod partitionMethod;
	/* Cantidad de corpus (para repartir entre nodos) */
	private int cantidadCorpus;
	/* Guardo el Cliente para conectar con los Servidores */
	Client client;
	/* TODO esto es temporal!!!!!!!!*/
	String nodesHost = "localhost";
	int fromPort = 1234;
	
	public CMasterNode(Client client) {
		super();
		this.client = client;
//		/* Se agrega como nodo */
//		nodes = new ArrayList<INode>();
//		nodes.add(this);
		
	}
	
	public void createCorpus() {
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
	
	public Map<Integer, String> getNodes() {
		return this.nodes;
	}

	public void sendCorpus(Collection<INode> nodes) {
		for (INode node : nodes){
			node.setColCorpus(null);
		}

	}

	public void sendOrderToIndex(Collection<INode> nodes) {
		for (INode node : nodes){
			//node.createIndex();
		}
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
		nodes = new HashMap<Integer, String>();
		int i;
		int port = 1234;
		// Arranaco desde 1, porque el 0 será este NODO
		for (i=1;i<cantidad;i++){
			nodes.put(i, "localhost:" + (port+i));
		}
	}

	public void setCorpusToNodes() {
		/* Agrego corpus al master node */
		Iterator<String> iterator = this.getColCorpusTotal().iterator();
		String corpusMasterNode = (String) iterator.next();
		Collection<String> colCorpusMasterNode = new ArrayList<String>();
		colCorpusMasterNode.add(corpusMasterNode);
		this.setColCorpus(colCorpusMasterNode);
		/* Agrego corpus a cada slave node */
		for (Entry<Integer, String> entry : this.getNodes().entrySet()){
			client.enviar("setId" + CUtil.separator + entry.getKey());
			client.enviar("setColCorpus" + CUtil.separator + (String) iterator.next());
		}
	}

}
