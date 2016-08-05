package node;

import java.util.Collection;

import org.terrier.matching.ResultSet;

import partitioning.IPartitionMethod;
import configuration.CParameters;
import connections.CClient;


public interface IMasterNode extends INode, IClient {
	
	Collection<CClient> getNodes();

	void createCorpus();
	
	Collection<String> getColCorpusTotal();
	
	void sendCorpusToNodes();
	
	void sendOrderToIndex(String recrearCorpus, String methodPartitionName);
	
	Collection<ResultSet> sendOrderToRetrieval(String query) throws Exception;
	
	IPartitionMethod getPartitionMethod();
	
	void setPartitionMethod(IPartitionMethod partitionMethod);
	
	int getCantidadCorpus();
	
	void setCantidadCorpus(Integer cantidadCorpus);
	
	void createSlaveNodes(Integer cantidad);
	
	void setIndexa(Boolean indexa);
	
	Boolean getIndexa();
	
	CParameters getParameters();
	
	void setParameters(CParameters parameters);

}
