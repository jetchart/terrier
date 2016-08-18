package node;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;

import org.apache.log4j.Logger;
import org.terrier.matching.ResultSet;
import org.terrier.structures.IndexOnDisk;
import org.terrier.structures.indexing.Indexer;

import partitioning.CRoundRobinByDocuments;
import partitioning.IPartitionByTerms;
import util.CUtil;
import configuration.CParameters;
import configuration.INodeConfiguration;
import connections.CClient;


public class CMasterNode extends CNode implements IMasterNode {

	static final Logger logger = Logger.getLogger(CMasterNode.class);
	
	/* Coleccion de todos los Corpus */
	private Collection<String> colCorpusTotal;
	/* Coleccion de corpus temporales para los particionamientos ByTerms */
	private Collection<String> colCorpusTotalByTerms;
	/* Nodos que posee, incluyéndose a él mismo */
	private Collection<CClient> nodes;
	/* Parametros recibidos */
	CParameters parameters;
	
	public CMasterNode(CParameters parameters) {
		super(parameters.getTipoNodo());
		this.parameters = parameters;
	}
	
	public Collection<String> getColCorpusTotal() {
		return this.colCorpusTotal;
	}

	public INodeConfiguration getNodeConfiguration() {
		return this.configuration;
	}

	public Collection<CClient> getNodes() {
		return this.nodes;
	}
	
	/**
	 * Crea los nodos esclavos en base a la cantidad indicada, seteandole a cada uno su host y port 
	 * correspondiente (esCreo Corpusta informacion la saca de {@link INodeConfiguration} configuration.
	 * En caso que el Master deba indexar, se creará cantidad-1 de nodos esclavos.
	 * @param cantidad	Cantidad de nodos esclavos a crear
	 */
	public void createSlaveNodes(Integer cantidad){
		Long inicio = System.currentTimeMillis();
		logger.info("------------------------------------");
		logger.info("INICIO CREACION ESCLAVOS");
		logger.info("------------------------------------");
		nodes = new ArrayList<CClient>();
		/* Si el Master indexa, entonces reduzco en 1 la cantidad de nodos a crear */
		if (this.getParameters().getMasterIndexa()){
			cantidad--;
		}
		int i = 0;
		for (String nodo : configuration.getSlavesNodes()){
			i++;
			if (i > cantidad)
				break;
			String host = nodo.split(":")[0];
			Integer port = Integer.valueOf(nodo.split(":")[1]);
			String user = null;
			String pass = null;
			String jarPath = null;
			/* Despierto los esclavos */
			if (parameters.getWakeUpSlaves()){
				user = nodo.split(":")[2];
				pass = nodo.split(":")[3];
				jarPath = nodo.split(":")[4];
				String jarName = nodo.split(":")[5];
				CUtil.executeCommandSSH(host, 22, user, pass, "cd " + jarPath + "; java -jar " + jarName + " slave");
			}
			CClient cliente = new CClient(host, port, user, pass, jarPath);
			nodes.add(cliente);
		}
		Long fin = System.currentTimeMillis() - inicio;
		logger.info("Creación esclavos tardó " + fin + " milisegundos");
		logger.info("------------------------------------");
		logger.info("FIN CREACION ESCLAVOS");
		logger.info("------------------------------------");
	}
	
	public void createCorpus() {
		logger.info("------------------------------------");
		logger.info("COMIENZA CREACION DE CORPUS");
		logger.info("------------------------------------");
		Long inicioCreacionCorpus = System.currentTimeMillis();
		/* Si el metodo de particion es por terminos, necesitamos primero
		 * tener el indice, para obtener los terminos y dividirlos */
		if (this.getParameters().getMetodoParticionamiento() instanceof IPartitionByTerms){
			/* Utilizamos el metodo RoundRobin por documentos para crear el corpus */
			/* TODO se puede mejorar esto */
			colCorpusTotal = new CRoundRobinByDocuments().createCorpus(configuration.getFolderPath(), configuration.getDestinationFolderPath(), this.getParameters().getCantidadNodos(), null, parameters);
			this.setColCorpus(colCorpusTotal);
			this.createIndex(INodeConfiguration.prefixIndexNoProcess, CRoundRobinByDocuments.class.getName());
			/* TODO Eliminar siempre o solo cuando se indica? */
			if (getParameters().getEliminarCorpus()){
				colCorpusTotalByTerms = new ArrayList<String>();
				colCorpusTotalByTerms.addAll(colCorpusTotal);
				eliminarCorpus(colCorpusTotalByTerms);
				colCorpusTotalByTerms.clear();
			}
			colCorpusTotal.clear();
		}else{
			this.index = null;
		}
		colCorpusTotal = this.getParameters().getMetodoParticionamiento().createCorpus(configuration.getFolderPath(), configuration.getDestinationFolderPath(), this.getParameters().getCantidadNodos(), this.index, parameters);
		/* Guardo backup del collection.spec */
		CUtil.guardarCollectionAnterior(colCorpusTotal);
		/* Si el metodo es particionado por términos, elimino el indice parcial porque ya fue utilizado */
		if (this.getParameters().getMetodoParticionamiento() instanceof IPartitionByTerms){
			CUtil.deleteIndexFiles(configuration.getTerrierHome() +"var/index/", INodeConfiguration.prefixIndexNoProcess + CRoundRobinByDocuments.class.getName());
		}
		Long finCreacionCorpus = System.currentTimeMillis() - inicioCreacionCorpus;
		logger.info("Creación Corpus tardó " + finCreacionCorpus + " milisegundos");
		logger.info("------------------------------------");
		logger.info("FIN CREACION DE CORPUS");
		logger.info("------------------------------------");
	}
	

	public void sendCorpusToNodes() {
		logger.info("------------------------------------");
		logger.info("COMIENZA ENVIO DE CORPUS A NODOS");
		logger.info("------------------------------------");
		if (CParameters.metodoComunicacion_SSH.equals(parameters.getMetodoComunicacion())){
			logger.info("Los nodos esclavos acceden a los corpus a través de una copia vía SFTP");
		}else if (CParameters.metodoComunicacion_PATH.equals(parameters.getMetodoComunicacion())){
			logger.info("Los nodos esclavos acceden a los corpus utilizando el mismo path que el master");
		}else{
			logger.error("Método de comunicación INVÁLIDO: " + parameters.getMetodoComunicacion());
		}
		Long inicioSetCorpusToNodes = System.currentTimeMillis();
		/* Agrego corpus al master node solo si indexa */
		Iterator<String> iterator = this.getColCorpusTotal().iterator();
		if (this.getParameters().getMasterIndexa()){
			String corpusMasterNode = (String) iterator.next();
			Collection<String> colCorpusMasterNode = new ArrayList<String>();
			colCorpusMasterNode.add(corpusMasterNode);
			this.setColCorpus(colCorpusMasterNode);
		}
		/* Agrego corpus a cada slave node */
		Collection<Hilo> hilos = new ArrayList<Hilo>();
		for (CClient cliente : nodes){
			/* Le envío la orden para inicializar, junto con el método de comunicación (via Path o por SCP) */
			cliente.setTarea(task_INITIALIZE + "_" + parameters.getMetodoComunicacion());
			cliente.setNodoColCorpus((String) iterator.next());
			Hilo hilo = new Hilo(cliente);
			hilo.start();
			hilos.add(hilo);
		}
		/* Espero a que todos los nodos terminen */
		esperar(hilos);
		Long finSetCorpusToNodes = System.currentTimeMillis() - inicioSetCorpusToNodes;
		logger.info("Enviar corpus a Nodos tardó " + finSetCorpusToNodes + " milisegundos");
		logger.info("------------------------------------");
		logger.info("FIN ENVIO DE CORPUS A NODOS");
		logger.info("------------------------------------");
	}
	
	@Override
	public void sendOrderToCleanIndexes(Boolean cleanIndexMaster) {
		logger.info("------------------------------------");
		logger.info("COMIENZA LIMPIEZA INDICES");
		logger.info("------------------------------------");
		Long inicio = System.currentTimeMillis();
		/* Envio indicacion para indexar a cada nodo */
		Collection<Hilo> hilos = new ArrayList<Hilo>();
		for (CClient cliente : nodes){
			cliente.setTarea(task_CLEAN_INDEXES);
			Hilo hilo = new Hilo(cliente);
			hilo.start();
			hilos.add(hilo);
		}
		/* Limpio el Master solo si se indica y además tiene que indexar */
		if (cleanIndexMaster){
			if (this.getParameters().getMasterIndexa()){
				CUtil.deleteIndexFiles(configuration.getTerrierHome() +"var/index/", INodeConfiguration.prefixIndex + "masterNode");
			}
		}
		/* Espero a que todos los nodos terminen */
		esperar(hilos);
		Long fin = System.currentTimeMillis() - inicio;
		logger.info("Limpieza de indices TOTAL tardó " + fin + " milisegundos");
		logger.info("------------------------------------");
		logger.info("FIN LIMPIEZA INDICES");
		logger.info("------------------------------------");
	}
	
	@Override
	public void sendOrderToCloseSlaves() {
		logger.info("------------------------------------");
		logger.info("COMIENZA CIERRE DE ESCLAVOS");
		logger.info("------------------------------------");
		Long inicio = System.currentTimeMillis();
		/* Envio indicacion para indexar a cada nodo */
		Collection<Hilo> hilos = new ArrayList<Hilo>();
		for (CClient cliente : nodes){
			cliente.setTarea(task_CLOSE);
			Hilo hilo = new Hilo(cliente);
			hilo.start();
			hilos.add(hilo);
		}
		/* Espero a que todos los nodos terminen */
		esperar(hilos);
		Long fin = System.currentTimeMillis() - inicio;
		logger.info("Cierre de esclavos TOTAL tardó " + fin + " milisegundos");
		logger.info("------------------------------------");
		logger.info("FIN LIMPIEZA CIERRE DE ESCLAVOS");
		logger.info("------------------------------------");
	}
	
	@Override
	public void sendOrderToDeleteCorpus() {
		logger.info("------------------------------------");
		logger.info("COMIENZA ELIMINACIÓN CORPUS");
		logger.info("------------------------------------");
		Long inicio = System.currentTimeMillis();
		/* Envio indicacion para indexar a cada nodo */
		Collection<Hilo> hilos = new ArrayList<Hilo>();
		for (CClient cliente : nodes){
			cliente.setTarea(task_DELETE_CORPUS);
			Hilo hilo = new Hilo(cliente);
			hilo.start();
			hilos.add(hilo);
		}
		/* El Master también elimina sus corpus */
		this.eliminarCorpus(colCorpusTotal);
		/* Espero a que todos los nodos terminen */
		esperar(hilos);
		Long fin = System.currentTimeMillis() - inicio;
		logger.info("Eliminacion Corpus TOTAL tardó " + fin + " milisegundos");
		logger.info("------------------------------------");
		logger.info("FIN ELIMINACIÓN CORPUS");
		logger.info("------------------------------------");
	}
	
	public void sendOrderToIndex(Boolean recrearCorpus, String methodPartitionName) {
		logger.info("------------------------------------");
		logger.info("COMIENZA INDEXACION DE TODOS LOS NODOS");
		logger.info("------------------------------------");
		Long inicioIndexacion = System.currentTimeMillis();
		/* Envio indicacion para indexar a cada nodo */
		Collection<Hilo> hilos = new ArrayList<Hilo>();
		for (CClient cliente : nodes){
			cliente.setTarea(task_CREATE_INDEX);
			Hilo hilo = new Hilo(cliente);
			hilo.start();
			hilos.add(hilo);
		}
		/* Indexo yo mismo si así se indica */
		if (this.getParameters().getMasterIndexa()){
			createIndex(INodeConfiguration.prefixIndex, configuration.getIdNode());
			copiarIndexProperties(Boolean.FALSE);
		}
		/* Espero a que todos los nodos terminen */
		esperar(hilos);
		Long finIndexacion = System.currentTimeMillis() - inicioIndexacion;
		logger.info("Indexación de todos los nodos tardó " + finIndexacion + " milisegundos");
		logger.info("------------------------------------");
		logger.info("FIN INDEXACION DE TODOS LOS NODOS");
		logger.info("------------------------------------");
	}

	public Collection<ResultSet> sendOrderToRetrieval(String query) throws Exception {
		logger.info("------------------------------------");
		logger.info("COMIENZA RECUPERACION");
		logger.info("------------------------------------");
		Long inicioRecuperacion = System.currentTimeMillis();
		/* Envio indicacion para indexar a cada nodo */
		Collection<Hilo> hilos = new ArrayList<Hilo>();
		for (CClient cliente : nodes){
			cliente.setTarea(task_RETRIEVAL);
			cliente.setQuery(query);
			Hilo hilo = new Hilo(cliente);
			hilo.start();
			hilos.add(hilo);
		}
		/* Recupero yo mismo si tuve que indexar */
		if (this.getParameters().getMasterIndexa()){
			retrieval(query);
		}
		/* Espero a que todos los nodos terminen */
		esperar(hilos);
		Long finRecuperacion = System.currentTimeMillis() - inicioRecuperacion;
		logger.info("Recuperacion TOTAL tardó " + finRecuperacion + " milisegundos");
		logger.info("------------------------------------");
		logger.info("FIN RECUPERACION");
		logger.info("------------------------------------");
		return null;

	}
	
	/**
	 * El metodo esperar debe aguardar a que todos los hilos de la coleccion que recibe, 
	 * terminen su ejecución.
	 * @param hilos
	 */
	public void esperar(Collection<Hilo> hilos){
		Boolean continuar = true;
		while (continuar){
			continuar = false;
			for (Hilo hilo: hilos){
				if (hilo.isAlive())
					continuar = true;
			}
		}
	}

	@Override
	public CParameters getParameters() {
		return parameters;
	}

	@Override
	public void setParameters(CParameters parameters) {
		this.parameters = parameters;
	}

	@Override
	public void setColCorpusTotal(Collection<String> colCorpusTotal) {
		this.colCorpusTotal = colCorpusTotal;
	}

	@Override
	public void showCorpusSize() {
		logger.info("------------------------------------");
		logger.info("COMIENZA MOSTRAR TAMAÑOS DE CORPUS");
		logger.info("------------------------------------");
		Long inicio = System.currentTimeMillis();
		Long total = 0L;
		for (String filePath : colCorpusTotal){
			Long size = CUtil.getFileSizeInBytes(filePath);
			total += size;
			logger.info("Corpus " + filePath + " ocupa " + size + " bytes");
		}
		logger.info("Todos los corpus ocupan " + total + " bytes");
		Long fin = System.currentTimeMillis() - inicio;
		logger.info("Mostrar tamaños de corpus tardó " + fin + " milisegundos");
		logger.info("------------------------------------");
		logger.info("FIN MOSTRAR TAMAÑOS DE CORPUS");
		logger.info("------------------------------------");
	}
	
	@Override
	public void copyIndexesFromSlaves() {
		logger.info("------------------------------------");
		logger.info("INICIO COPIA DE INDICES DE ESCLAVOS");
		logger.info("------------------------------------");
		Long inicio = System.currentTimeMillis();
		for (CClient cliente : nodes){
			for (String indexFile : CUtil.indexFiles){
				String pathOnMaster = configuration.getTerrierHome() + "var/index/" + cliente.getIndexPath().split("/")[cliente.getIndexPath().split("/").length-1] + indexFile;
				String pathOnSlave = cliente.getIndexPath() + indexFile;
				if (CParameters.metodoComunicacion_SSH.equals(parameters.getMetodoComunicacion())){
					CUtil.copyFileSFTP(pathOnSlave, pathOnMaster, cliente.getUser(), cliente.getPass(), cliente.getHost(), 22);
				}else if (CParameters.metodoComunicacion_PATH.equals(parameters.getMetodoComunicacion())){
					CUtil.copyFile(pathOnSlave, pathOnMaster);
				}
			}
		}
		Long fin = System.currentTimeMillis() - inicio;
		logger.info("Copia indices de esclavos tardó " + fin + " milisegundos");
		logger.info("------------------------------------");
		logger.info("FIN COPIA DE INDICES DE ESCLAVOS");
		logger.info("------------------------------------");
	}
	
	@Override
	public void copyPropertiesFileFromSlaves() {
		logger.info("------------------------------------");
		logger.info("INICIO COPIA DE ARCHIVO PROPERTIES DE ESCLAVOS");
		logger.info("------------------------------------");
		Long inicio = System.currentTimeMillis();
		for (CClient cliente : nodes){
			/* Copio el .properties de la corrida de los esclavos */
			String pathOnMaster = INodeConfiguration.logIndexPath + cliente.getIndexPath().split("/")[cliente.getIndexPath().split("/").length-1] + "_" + cliente.getNodoColCorpus().split("/")[ cliente.getNodoColCorpus().split("/").length-1] + ".properties";
			/* El String comentado va a buscar el .properties con el nombre de la corrida */
			String pathOnSlave = cliente.getIndexPath() + "_" + cliente.getNodoColCorpus().split("/")[cliente.getNodoColCorpus().split("/").length-1] + ".properties";
//			String pathOnSlave = cliente.getIndexPath() + "_" + ".properties";
			if (CParameters.metodoComunicacion_SSH.equals(parameters.getMetodoComunicacion())){
				CUtil.copyFileSFTP(pathOnSlave, pathOnMaster, cliente.getUser(), cliente.getPass(), cliente.getHost(), 22);
			}else if (CParameters.metodoComunicacion_PATH.equals(parameters.getMetodoComunicacion())){
				CUtil.copyFile(pathOnSlave, pathOnMaster);
			}
		}
		/* Copio el .properties del master tambien (ya que está en la carpeta de terrier */
		String pathOnMasterTarget = INodeConfiguration.logIndexPath + INodeConfiguration.prefixIndex + getNodeConfiguration().getIdNode() + "_" + colCorpus.iterator().next().split("/")[colCorpus.iterator().next().split("/").length-1] + ".properties";
		/* El String comentado va a buscar el .properties con el nombre de la corrida */
		String pathOnMasterSource = configuration.getTerrierHome() + "var/index/" + INodeConfiguration.prefixIndex + getNodeConfiguration().getIdNode() + "_" + colCorpus.iterator().next().split("/")[colCorpus.iterator().next().split("/").length-1] + ".properties";
//		String pathOnMasterSource = configuration.getTerrierHome() + "var/index/" + INodeConfiguration.prefixIndex + getNodeConfiguration().getIdNode() + ".properties";
		logger.info("pathOnMasterTarget: " + pathOnMasterTarget);
		logger.info("pathOnMasterSource: " + pathOnMasterSource);
		CUtil.copyFile(pathOnMasterSource, pathOnMasterTarget);
		Long fin = System.currentTimeMillis() - inicio;
		logger.info("Copia archivo properties de esclavos tardó " + fin + " milisegundos");
		logger.info("------------------------------------");
		logger.info("FIN COPIA DE ARCHIVO PROPERTIES DE ESCLAVOS");
		logger.info("------------------------------------");
	}
	/**
	 * Realiza un merge de los indices que se encuentren en la carpeta indexPath
	 * y que concuerden con el prefijo y rango de numeros indicados.
	 * 
	 * @param indexPath
	 * @param indexPrefix
	 * @param lowest
	 * @param highest
	 */
	@Override
	public void mergeIndexes(String indexPath, String indexPrefix, Integer lowest, Integer highest){
		logger.info("------------------------------------");
		logger.info("INICIO MERGE DE INDICES");
		logger.info("------------------------------------");
		Long inicio = System.currentTimeMillis();
		Indexer.merge(indexPath, indexPrefix, lowest, highest);
		Long fin = System.currentTimeMillis() - inicio;
		logger.info("Se creó índice mergeado con el prefijo \"" + indexPrefix + "\" en la carpeta " + indexPath);
		logger.info("Merge de indices tardó " + fin + " milisegundos");
		logger.info("------------------------------------");
		logger.info("FIN MERGE DE INDICES");
		logger.info("------------------------------------");
	}

	@Override
	public void setIndex(IndexOnDisk index) {
		this.index = index;
	}

}
