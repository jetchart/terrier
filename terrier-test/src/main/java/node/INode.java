package node;

import java.rmi.Remote;
import java.util.Collection;

import org.terrier.matching.ResultSet;
import org.terrier.structures.Index;

import configuration.INodeConfiguration;


public interface INode extends Remote {
	
	static String ID_MASTER = "MASTER";
	static String ID_SLAVE = "SLAVE";
	
	INodeConfiguration getNodeConfiguration();
	
	Index getIndex();
	
	void createIndex(String prefix, String methodPartitionName);
	
	Collection<String> getColCorpusNode();
	
	/* Guarda la coleccion de Corpus que se le fue asignada y la vuelca en el archivo collection.spec */
	void setColCorpus(Collection<String> colCorpus);
	
	ResultSet retrieval(String query) throws Exception;
	
	int getId();
	
	void setId(int id);
	
	ResultSet getResultSet();
	
	void setResultSet(ResultSet resultSet);
	
	void eliminarCorpus(Collection<String> colPaths);

	void copiarIndexProperties(Boolean isMergeIndex);
}
