package partitioningBkp;

import java.util.Collection;

import org.terrier.structures.Index;

public interface IPartitionMethod {

	/**
	 * El metodo createCorpus recibe un folderPath donde se encuentran todos los archivos y arma tantos 
	 * documentos como se indica en la variable cantidadCorpus en base al metodo que se implementa, y 
	 * los coloca en la carpeta destinationFolderPath. Si se envia un indice, significa que es un metodo 
	 * particionado por terminos.
	 * @param folderPath
	 * @param destinationFolderPath
	 * @param cantidadCorpus
	 * @param index
	 * @return
	 */
	Collection<String> createCorpus(String folderPath, String destinationFolderPath, Integer cantidadCorpus, Index index);
}
