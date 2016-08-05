package partitioning;

import java.util.Collection;

import org.terrier.structures.Index;

import configuration.CParameters;

public interface IPartitionMethod {

	/**
	 * El metodo createCorpus recibe un folderPath donde se encuentran todos los archivos y arma tantos 
	 * documentos como se indica en la variable cantidadCorpus en base al metodo que se implementa, y 
	 * los coloca en la carpeta destinationFolderPath. Si se envia un indice, significa que es un metodo 
	 * particionado por terminos.
	 * Además, se requiere el argumento "parameters" para armar los nombres de los corpus 
	 * en base a los parámetros de la corrida.
	 * @param folderPath
	 * @param destinationFolderPath
	 * @param cantidadCorpus
	 * @param index
	 * @return
	 */
	Collection<String> createCorpus(String folderPath, String destinationFolderPath, Integer cantidadCorpus, Index index, CParameters parameters);
}
