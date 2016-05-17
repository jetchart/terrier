package node;

import java.rmi.Remote;
import java.util.Collection;

import org.terrier.matching.ResultSet;
import org.terrier.structures.Index;

import configuration.INodeConfiguration;


public interface INode extends Remote {
	
	static String ID_MASTER = "master";
	static String ID_SLAVE = "slave";
	
	INodeConfiguration getNodeConfiguration();
	
	Index getIndex();
	
	void createIndex(String recrearCorpus, String methodPartitionName);
	
	Collection<String> getColCorpusNode();
	
	/* Guarda la coleccion de Corpus que se le fue asignada y la vuelca en el archivo collection.spec */
	void setColCorpus(Collection<String> colCorpus);
	
	ResultSet retrieval(String query) throws Exception;
	
	int getId();
	
	void setId(int id);
	
}
