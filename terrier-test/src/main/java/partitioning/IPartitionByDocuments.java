package partitioning;

public interface IPartitionByDocuments extends IPartitionMethod {
	/**
	 * Indica la longitud máxima del StringBuffer.
	 * En caso que se iguale o supere, se impacta
	 * en el archivo.
	 * 1073741824 sería 2^30
	 */
	public static final Long tamanioMaximoAntesCierre = 1073741824L; 
}
