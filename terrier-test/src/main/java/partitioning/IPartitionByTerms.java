package partitioning;

import java.util.List;
import java.util.Map;

public interface IPartitionByTerms extends IPartitionMethod {
	
	public static final Long cantidadMaximaTokensAntesCierre = 50000000L; 
	
	public void writeDoc(Map<Long, Map<Long, Map<String, Long>>> mapNodeDocTerm, Integer cantidadCorpus, String destinationFolderPath, List<String> colCorpusTotal);
	
}