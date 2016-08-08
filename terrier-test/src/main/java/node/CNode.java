package node;

import java.util.Collection;

import org.apache.log4j.Logger;
import org.terrier.indexing.TRECCollection;
import org.terrier.matching.ResultSet;
import org.terrier.querying.Manager;
import org.terrier.querying.SearchRequest;
import org.terrier.structures.Index;
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
		logger.info("Recuperación tardó " + finRecuperacion + " milisegundos\n");	 
		/* Muestro los resultados */
		this.resultSet = srq.getResultSet();
//		CUtil.mostrarResultados(resultSet, index, query);
		ResultSet rs = srq.getResultSet();
		CUtil.mostrarResultados(rs, index, query);
		/* Guardo el ResultSet */
		this.resultSet = srq.getResultSet();
		/* Devuelvo el resultSet con los resultados */
		return this.resultSet;
	}

	public void createIndex(Boolean recrearCorpus, String sufijoNombreIndice) {
		logger.info("");
		logger.info("Inicia Indexación");
		if (recrearCorpus){
			/* Elimino el indice anterior */
			CUtil.deleteIndexFiles(configuration.getTerrierHome() +"var/index/", sufijoNombreIndice);
		}
		Long inicioIndexacion = System.currentTimeMillis();
    	TRECCollection coleccion = null;
    	/* Creo una nueva coleccion */
		coleccion = new TRECCollection();
		/* Instancio Indexador */
		Indexer indexador = new BasicIndexer(configuration.getTerrierHome() +"var/index/", sufijoNombreIndice);
		/* Indico las colecciones a indexar */
		org.terrier.indexing.Collection[] col = new org.terrier.indexing.Collection[1];
		col[0] = coleccion;
		/* Indexo las colecciones */
		indexador.createDirectIndex(col);
		/* Creo el Indice Invertido */
		indexador.createInvertedIndex();
		/* Cierro Coleccion */
		coleccion.close();
    	/* Guardo el indice creado */
		this.index = Index.createIndex(configuration.getTerrierHome() +"var/index/", sufijoNombreIndice);		
		Long finIndexacion = System.currentTimeMillis() - inicioIndexacion;
		logger.info("Indexación del nodo tardó " + finIndexacion + " milisegundos\n");	
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

}
