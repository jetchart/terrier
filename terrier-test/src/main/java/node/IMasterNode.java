package node;

import java.util.Collection;

import org.terrier.matching.ResultSet;

import partitioning.IPartitionMethod;
import connections.Client;


public interface IMasterNode extends INode, IClient {
	
	Collection<Client> getNodes();

	void createCorpus();
	
	Collection<String> getColCorpusTotal();
	
	void sendCorpusToNodes();
	
	void sendOrderToIndex(String recrearCorpus, String methodPartitionName);
	
	Collection<ResultSet> sendOrderToRetrieval(Collection<INode> nodes, String query);
	
	IPartitionMethod getPartitionMethod();
	
	void setPartitionMethod(IPartitionMethod partitionMethod);
	
	int getCantidadCorpus();
	
	void setCantidadCorpus(int cantidadCorpus);
	
	void createSlaveNodes(int cantidad);

}
