package node;

import java.util.Collection;
import java.util.Map;

import org.terrier.matching.ResultSet;

import partitioning.IPartitionMethod;


public interface IMasterNode extends INode, IClient {
	
	Map<Integer, String> getNodes();
	
	/* */
	void createCorpus();
	
	Collection<String> getColCorpusTotal();
	
	void sendCorpus(Collection<INode> nodes);
	
	void sendOrderToIndex(Collection<INode> nodes);
	
	Collection<ResultSet> sendOrderToRetrieval(Collection<INode> nodes, String query);
	
	IPartitionMethod getPartitionMethod();
	
	void setPartitionMethod(IPartitionMethod partitionMethod);
	
	int getCantidadCorpus();
	
	void setCantidadCorpus(int cantidadCorpus);
	
	void createSlaveNodes(int cantidad);
	
	void setCorpusToNodes();
}
