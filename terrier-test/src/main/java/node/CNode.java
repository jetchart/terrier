package node;

import java.util.Collection;
import java.util.Map.Entry;

import org.apache.log4j.Logger;
import org.terrier.indexing.TRECCollection;
import org.terrier.matching.ResultSet;
import org.terrier.querying.Manager;
import org.terrier.querying.SearchRequest;
import org.terrier.structures.Index;
import org.terrier.structures.Lexicon;
import org.terrier.structures.LexiconEntry;
import org.terrier.structures.indexing.Indexer;
import org.terrier.structures.indexing.classical.BasicIndexer;

import configuration.CNodeConfiguration;
import configuration.INodeConfiguration;
import util.CUtil;


public abstract class CNode implements INode {
	
	Logger logger = Logger.getLogger(CNode.class);
	
	/* Index */
	Index index;
	/* Corpus */
	Collection<String> colCorpus;
	/* Configuracion */
	INodeConfiguration configuration;
	/* Identificador del NODO */
	Integer id;
	/* ResultSet */
	ResultSet resultSet;
	
	public static final String task_INITIALIZE = "initialize";
	public static final String task_CREATE_INDEX = "createIndex";
	public static final String task_CLEAN_INDEXES = "cleanIndexes";
	public static final String task_DELETE_CORPUS = "deleteCorpus";
	public static final String task_CLOSE = "close";
	public static final String task_RETRIEVAL = "retrieval";
	public static final String task_COPY_INDEX = "copyIndex";


	public CNode(String nodeType){
		String pathConfiguration = null;
		if (INode.ID_MASTER.equals(nodeType)){
			pathConfiguration = INodeConfiguration.configurationMasterNodeFilePath;
		}
		if (INode.ID_SLAVE.equals(nodeType)){
			pathConfiguration = INodeConfiguration.configurationSlaveNodeFilePath;
		}
		/* Crea la configuracion configuracion del Nodo */
		configuration = new CNodeConfiguration(pathConfiguration);
		/* Le asigno el ID */
		setId(Integer.valueOf(configuration.getIdNode()));
		/* Defino properties necesarias para Terrier */
		System.setProperty("terrier.home",configuration.getTerrierHome());
		System.setProperty("terrier.etc",configuration.getTerrierHome() + "etc/");
		System.setProperty("terrier.setup",configuration.getTerrierHome() + "etc/terrier.properties");
	}
	
	public Index getIndex() {
		return this.index;
	}

	public Collection<String> getColCorpusNode() {
		return this.colCorpus;
	}

	public void setColCorpus(Collection<String> colCorpus) {
		/* Actualizo el archivo collection.spec */
		CUtil.agregarCorpus(colCorpus);
		/* Guardo la coleccion de Corpus */
		this.colCorpus = colCorpus;
	}

	public ResultSet retrieval(String query) throws Exception {
//		logger.info("------------------------------------");
//		logger.info("COMIENZA RECUPERACION");
//		logger.info("------------------------------------");
		logger.info("");
		logger.info("Inicia Recuperación");
		Long inicioRecuperacion = System.currentTimeMillis();
		/* Instancio Manager con el indice creado */
		Manager m = new Manager(this.index);
		/* Creo el SearchRequest con la query */
		SearchRequest srq = m.newSearchRequest("Q1", query);
		/* Indico modelo de Matching */
		srq.addMatchingModel("Matching", "TF_IDF");
		/* Corro query */
		m.runPreProcessing(srq);
		m.runMatching(srq);
		m.runPostProcessing(srq);
		m.runPostFilters(srq);
		 /* Devuelvo ResultSet */
		Long finRecuperacion = System.currentTimeMillis() - inicioRecuperacion;
		logger.info("Recuperación tardó " + finRecuperacion + " milisegundos");	 
		/* Muestro los resultados */
		this.resultSet = srq.getResultSet();
//		CUtil.mostrarResultados(resultSet, index, query);
//		ResultSet rs = srq.getResultSet();
//		CUtil.mostrarResultados(rs, index, query);
		/* Guardo el ResultSet */
		this.resultSet = srq.getResultSet();
		/* Devuelvo el resultSet con los resultados */
		return this.resultSet;
	}

	public void createIndex(String prefix, String sufijoNombreIndice) {
		logger.info("");
		logger.info("Inicia Indexación del nodo " + configuration.getNodeType() + "_" + configuration.getIdNode());
		String indexName = prefix + sufijoNombreIndice;
		CUtil.deleteIndexFiles(configuration.getTerrierHome() +"var/index/", indexName);
		Long inicioIndexacion = System.currentTimeMillis();
    	TRECCollection coleccion = null;
    	/* Creo una nueva coleccion */
		coleccion = new TRECCollection();
		/* Instancio Indexador */
		Indexer indexador = new BasicIndexer(configuration.getTerrierHome() + "var/index/", indexName);
		/* Indico las colecciones a indexar */
		org.terrier.indexing.Collection[] col = new org.terrier.indexing.Collection[1];
		col[0] = coleccion;
		/* Indexo las colecciones */
		indexador.createDirectIndex(col);
		/* Creo el Indice Invertido */
		indexador.createInvertedIndex();
		/* Cierro Coleccion */
		coleccion.close();
    	/* Levanto el índice creado */
		this.index = Index.createIndex(configuration.getTerrierHome() +"var/index/", indexName);		
		Long finIndexacion = System.currentTimeMillis() - inicioIndexacion;
		logger.info("Indexación del nodo " + configuration.getNodeType() + "_" + configuration.getIdNode() + " tardó " + finIndexacion + " milisegundos");
		mostrarTerminosIndice(this.index);
	}

	private void mostrarTerminosIndice(Index index) {
		Lexicon<String> mapLexicon = index.getLexicon();
		Long terminos = 0L;
		for (Entry<String, LexiconEntry> lexicon : mapLexicon){
			terminos++;
//			logger.info(lexicon.getKey().toLowerCase());
		}
		logger.info("CANTIDAD TERMINOS: " + terminos);
	}
	
	public int getId() {
		return this.id;
	}
	
	public void setId(int id){
		this.id = id;
	}
	
	public ResultSet getResultSet() {
		return resultSet;
	}

	public void setResultSet(ResultSet resultSet) {
		this.resultSet = resultSet;
	}


	@Override
	public void eliminarCorpus(Collection<String> colPaths) {
		logger.info("------------------------------------");
		logger.info("COMIENZA ELIMINACIÓN CORPUS");
		logger.info("------------------------------------");
		Long inicio = System.currentTimeMillis();
		for (String corpusPath : colPaths){
			CUtil.deleteFile(corpusPath);
			logger.info("Se elimina el corpus: " + corpusPath);
		}
		Long fin = System.currentTimeMillis() - inicio;
		logger.info("Eliminación de corpus tardó " + fin + " milisegundos");
		logger.info("------------------------------------");
		logger.info("FIN ELIMINACIÓN CORPUS");
		logger.info("------------------------------------");
	}
	
	@Override
	public void copiarIndexProperties(Boolean isMergeIndex){
		logger.info("------------------------------------");
		logger.info("INICIO COPIAR ARCHIVO PROPERTIES DEL INDICE");
		logger.info("------------------------------------");
		Long inicio = System.currentTimeMillis();
		String merge = isMergeIndex?"merge_":"";
		String fileName = (isMergeIndex?INodeConfiguration.prefixIndex.replace("_", ""): INodeConfiguration.prefixIndex + configuration.getIdNode());
		String source = configuration.getTerrierHome() + "var/index/" + fileName + ".properties";
		/* El String comentado va a buscar el .properties con el nombre de la corrida */
		String target = (isMergeIndex? INodeConfiguration.logIndexPath : configuration.getTerrierHome() + "var/index/") + fileName + "_" + merge + colCorpus.iterator().next().split("/")[colCorpus.iterator().next().split("/").length-1] + ".properties";
//		String target = (isMergeIndex? INodeConfiguration.logIndexPath : configuration.getTerrierHome() + "var/index/") + fileName + "_" + merge + ".properties";
		logger.info(source);
		logger.info(target);
		CUtil.copyFile(source, target);
		Long fin = System.currentTimeMillis() - inicio;
		logger.info("Copiar archivo properties del indice tardó " + fin + " milisegundos");
		logger.info("------------------------------------");
		logger.info("FIN COPIAR ARCHIVO PROPERTIES DEL INDICE");
		logger.info("------------------------------------");
	}
}