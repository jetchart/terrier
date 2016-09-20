package partitioning;

import java.util.List;
import java.util.Map;

public interface IPartitionByTerms extends IPartitionMethod {
	
	public static final Long cantidadMaximaTokensAntesCierre = 200000000L;
	public static final Long cantidadMaximaDocumentosAProcesar = 500000L;
	
	
	public void writeDoc(Map<Long, Map<Long, Map<String, Long>>> mapNodeDocTerm, Integer cantidadCorpus, String destinationFolderPath, List<String> colCorpusTotal);
	
}