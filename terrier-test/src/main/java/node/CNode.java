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
	int id;
	
	public CNode(String nodeType){
		String pathConfiguration = null;
		if (INode.ID_cliente.equals(nodeType)){
			pathConfiguration = INodeConfiguration.configurationMasterNodeFilePath;
		}
		if (INode.ID_servidor.equals(nodeType)){
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
//		System.out.println("------------------------------------");
//		System.out.println("COMIENZA RECUPERACION");
//		System.out.println("------------------------------------");
		System.out.println("\nInicia Recuperación");
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
		System.out.println("Recuperación tardó " + finRecuperacion + " milisegundos\n");	 
		/* Muestro los resultados */
		ResultSet rs = srq.getResultSet();
		CUtil.mostrarResultados(rs, index, query);
		/* Devuelvo el resultSet con los resultados */
		return srq.getResultSet();
	}

	public void createIndex(String recrearCorpus, String sufijoNombreIndice) {
		System.out.println("\nInicia Indexación");
		if (recrearCorpus.equals("S")){
			/* Elimino el indice anterior */
			CUtil.deleteIndexFiles(configuration.getTerrierHome() +"var/index/", sufijoNombreIndice);
		}
		Long inicioIndexacion = System.currentTimeMillis();
    	TRECCollection coleccion = null;
    	/* Creo una nueva coleccion */
		coleccion = new TRECCollection();
		/* Instancio Indexador */
//		Indexer indexador = new BasicIndexer(configuration.getTerrierHome() +"var/index/", INodeConfiguration.indexName + "_" + sufijoNombreIndice);
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
    	/* Devuelvo el indice creado */
//		this.index = Index.createIndex(configuration.getTerrierHome() +"var/index/", INodeConfiguration.indexName + "_" + sufijoNombreIndice);
		this.index = Index.createIndex(configuration.getTerrierHome() +"var/index/", sufijoNombreIndice);		
		Long finIndexacion = System.currentTimeMillis() - inicioIndexacion;
		System.out.println("Indexación tardó " + finIndexacion + " milisegundos\n");	
	}

	public int getId() {
		return this.id;
	}
	
	public void setId(int id){
		this.id = id;
	}
}
