package node;

import java.util.Collection;

import org.terrier.matching.ResultSet;
import org.terrier.structures.IndexOnDisk;

import configuration.CParameters;
import connections.CClient;


public interface IMasterNode extends INode, IClient {
	
	Collection<CClient> getNodes();

	void createCorpus();
	
	Collection<String> getColCorpusTotal();
	
	void setColCorpusTotal(Collection<String> colCorpusTotal);
	
	void sendCorpusToNodes();
	
	void sendOrderToIndex(Boolean recrearCorpus, String methodPartitionName);
	
	Collection<ResultSet> sendOrderToRetrieval(String query) throws Exception;
	
	void createSlaveNodes(Integer cantidad);
	
	CParameters getParameters();
	
	void setParameters(CParameters parameters);

	void showCorpusSize();

	void sendOrderToCleanIndexes(String methodPartitionName);

	void sendOrderToCloseSlaves();
	
	void sendOrderToDeleteCorpus();

	void copyIndexesFromSlaves();

	void mergeIndexes();

	void setIndex(IndexOnDisk index);

}
