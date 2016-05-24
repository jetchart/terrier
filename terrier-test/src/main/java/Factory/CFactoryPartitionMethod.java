package Factory;
import partitioning.CRoundRobinByDocuments;
import partitioning.CRoundRobinByTerms;
import partitioning.CSizeByDocuments;
import partitioning.CSizeByTerms;
import partitioning.CSizeTokensByDocuments;
import partitioning.IPartitionMethod;


public class CFactoryPartitionMethod {

	public static IPartitionMethod getInstance(Integer metodo){
		if (metodo == 1){
			return new CRoundRobinByDocuments();
		}
		if (metodo == 2){
			return new CSizeByDocuments();
		}
		if (metodo == 3){
			return new CSizeTokensByDocuments();
		}
		if (metodo == 4){
			return new CRoundRobinByTerms();
		}
		if (metodo == 5){
			return new CSizeByTerms();
		}
		return null;
		
	}
}
