package partitioning;

import java.util.Collection;
import java.util.List;
import java.util.Map;

public interface IPartitionByTerms extends IPartitionMethod {
	
	public static final Long cantidadMaximaTokensAntesCierre = 100000000L; 
	
	public void writeDoc(Map<Long, Map<Long, Collection<String>>> mapNodeDocTerm, Integer cantidadCorpus, String destinationFolderPath, List<String> colCorpusTotal);
	
}