package node;

import java.util.Collection;

import org.terrier.matching.ResultSet;

import configuration.CParameters;
import connections.CClient;


public interface IMasterNode extends INode, IClient {
	
	Collection<CClient> getNodes();

	void createCorpus();
	
	Collection<String> getColCorpusTotal();
	
	void sendCorpusToNodes();
	
	void sendOrderToIndex(Boolean recrearCorpus, String methodPartitionName);
	
	Collection<ResultSet> sendOrderToRetrieval(String query) throws Exception;
	
	void createSlaveNodes(Integer cantidad);
	
	CParameters getParameters();
	
	void setParameters(CParameters parameters);

}
