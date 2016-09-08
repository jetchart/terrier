package partitioning;

import java.util.Collection;
import java.util.List;
import java.util.Map;

public interface IPartitionByTerms extends IPartitionMethod {
	
	public void writeDoc(Map<Long, Map<Long, Collection<String>>> mapNodeDocTerm, Map<Long,String> mapDocDocPath, Integer cantidadCorpus, String destinationFolderPath, List<String> colCorpusTotal);
	
}