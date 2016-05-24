package partitioningBkp;

import java.util.Collection;
import java.util.Map;

public interface IPartitionByTerms extends IPartitionMethod {
	
	void writeDoc(Map<Integer, Map<Integer, Collection<String>>> mapNodeDocTerm, Map<Integer,String> mapDocDocPath, int cantidadCorpus, String destinationFolderPath);
}
