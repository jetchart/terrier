package node;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;

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
		Long inicioIndexacion = System.currentTimeMillis();
		/* Envio indicacion para indexar a cada nodo */
		for (Client cliente : nodes){
			cliente.enviar("createIndex");
			cliente.recibir();
		}
		/* Indexo yo mismo */
		createIndex(recrearCorpus, methodPartitionName);
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
		Long inicioSetCorpusToNodes = System.currentTimeMillis();
		/* Agrego corpus al master node */
		Iterator<String> iterator = this.getColCorpusTotal().iterator();
		String corpusMasterNode = (String) iterator.next();
		Collection<String> colCorpusMasterNode = new ArrayList<String>();
		colCorpusMasterNode.add(corpusMasterNode);
		this.setColCorpus(colCorpusMasterNode);
		/* Agrego corpus a cada slave node */
		for (Client cliente : nodes){
			cliente.enviar("setId" + CUtil.separator + cliente.getPort());
			cliente.enviar("setColCorpus" + CUtil.separator + (String) iterator.next());
			cliente.recibir();
		}
		Long finSetCorpusToNodes = System.currentTimeMillis() - inicioSetCorpusToNodes;
		System.out.println("Enviar corpus a Nodos tardó " + finSetCorpusToNodes + " milisegundos");
	}

}
