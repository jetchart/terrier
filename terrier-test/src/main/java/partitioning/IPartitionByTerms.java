package partitioning;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;

public interface IPartitionByTerms extends IPartitionMethod {
	
	public void writeDoc(Map<Integer, Map<Integer, Collection<String>>> mapNodeDocTerm, Map<Integer,String> mapDocDocPath, int cantidadCorpus, String destinationFolderPath);
	
}
