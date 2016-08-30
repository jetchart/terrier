package partitioning;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.Reader;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.log4j.Logger;
import org.terrier.structures.Index;

import util.CUtil;
import configuration.CParameters;

public class CSizeTokensByDocuments implements IPartitionByDocuments {

	static final Logger logger = Logger.getLogger(CSizeTokensByDocuments.class);
	
	public Collection<String> createCorpus(String folderPath, String destinationFolderPath, Integer cantidadCorpus, Index index, CParameters parameters) {
		List<String> colCorpusTotal = new ArrayList<String>();
    	Map<Integer,Integer> tokensByCorpus = null;
    	Map<String, StringBuffer> mapaCorpusContenido = new HashMap<String, StringBuffer>();
        try
        {
        	logger.info("Metodo de particion: " + CSizeTokensByDocuments.class.getName());
        	/* Se obtienen todos los ficheros del folder "folderPath" */
        	java.util.Collection<String> filesPath = CUtil.getFilesFromFolder(new ArrayList<String>(),folderPath, Boolean.TRUE);
        	/* Se obtiene cantidad de ficheros */
        	int cantidadTotalArchivos = filesPath.size();
        	logger.info("Cantidad de corpus a crear: " + cantidadCorpus);
        	logger.info("Cantidad de documentos: " + cantidadTotalArchivos);
        	/* Inicializar mapa tokens */
        	tokensByCorpus = this.inicializarMapa(cantidadCorpus);
        	/* Se crean los corpus vacios, y se agregan a la coleccion de corpus total */
        	colCorpusTotal.addAll(CUtil.crearCorpusVacios(destinationFolderPath, CSizeTokensByDocuments.class.getName(), cantidadCorpus, parameters));
        	/* Inicializo el mapa con la ruta de los corpus vacios */
        	for (String pathCorpus : colCorpusTotal){
        		mapaCorpusContenido.put(pathCorpus, new StringBuffer());
        	}
            Long docno = Long.valueOf(0);
            Long tamanioBuffer = 0L;
    	    FileReader f;
    	    BufferedReader b;
    	    String cadena;
    	    StringBuffer retorno = new StringBuffer();
            /* Se recorren los archivos del folder */
        	for (String filePath : filesPath){
				f = new FileReader(filePath);
			    b = new BufferedReader(f);
			    while((cadena = b.readLine())!=null) {
			    	if (!cadena.startsWith("<DOCNO>") && !cadena.startsWith("<DOC>")){
				    	if (cadena.equals("</DOC>") && !retorno.toString().trim().isEmpty()){
				    		/* >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> */
				    		/* Se obtiene el id del corpus con menos tokens unicos */
			        		Integer corpusId = getIdCorpusSmallestTokens(tokensByCorpus);
			       			/* Abro el corpus correspondiente */
			        		String corpusPath = colCorpusTotal.get(corpusId);
			        		StringBuffer contenido = mapaCorpusContenido.get(corpusPath);   
			        		/* Escribo contenido del archivo en el corpus con formato TREC */
			                contenido.append("<DOC>").append("\n");
			                contenido.append("<DOCNO>"+ docno++ +"</DOCNO>").append("\n");
			                /* TODO ¡REVISAR SI LA CANTIDAD MAXIMA DE CARACTERES PARA EL DOCPATH ALCANZA BIEN! */
//			                contenido.append("<DOCPATH>" + filePath + "</DOCPATH>").append("\n");
//			                contenido.append("<TEXT>").append("\n");
			                /* Obtengo Reader del contenido del archivo */
//			                Reader reader = CUtil.getReaderArchivo(filePath);
			                /* Obtengo la cantidad de tokens unicos que tiene el archivo y se lo sumo al corpus */
			                Integer cantidadTokensUnicos = CUtil.getAmountUniqueTokensInStringBuffer(retorno);
			                tokensByCorpus.put(corpusId, tokensByCorpus.get(corpusId) + cantidadTokensUnicos);
//			                logger.info("Se eligió al corpus " + corpusId + " porque tiene " + tokensByCorpus.get(corpusId)+ " tokens");
			                /* Obtengo el contenido del Reader sin tags */
			                /* TODO SE ESTÁ LEYENDO 2 VECES EL ARCHIVO, INTENTAR EVITAR ESTO */
//			                reader = CUtil.getReaderArchivo(filePath);
			                contenido.append(retorno).append("\n");
//			                reader.close();
//			                contenido.append("</TEXT>").append("\n");
			                contenido.append("</DOC>").append("\n");
			                mapaCorpusContenido.put(corpusPath, contenido);
			                tamanioBuffer += contenido.length();
			                /* Si ya se procesaron mas de la cantidad de archivos permitidas, se impactan */
			                if (tamanioBuffer > IPartitionByDocuments.tamanioMaximoAntesCierre){
			                	tamanioBuffer = 0L;
			                	/* Guardo el contenido de todos los corpus en los archivos sobreescribiendo si ya existe */
			                	CUtil.crearCorpusConDocumentos(mapaCorpusContenido, Boolean.TRUE);
			                	/* Inicializo el mapa con la ruta de los corpus vacios */
			                	for (String pathCorpus : colCorpusTotal){
			                		mapaCorpusContenido.put(pathCorpus, new StringBuffer());
			                	}
			                }
				    		/* <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<< */
				    		retorno = new StringBuffer();
				    	}else{
				    		retorno.append(cadena).append(" ");
				    	}
			    	}
			    }
        	}
        	if (tamanioBuffer > 0){
	        	/* Guardo el contenido de todos los corpus en los archivos sobreescribiendo si ya existe */
	        	CUtil.crearCorpusConDocumentos(mapaCorpusContenido, Boolean.TRUE);
        	}
        } catch (Exception e) {
            e.printStackTrace();
        }
        /* Muestro info sobre los corpus */
        this.showCorpusInfo(tokensByCorpus);
		return colCorpusTotal;
	}

	/**
	 * Inicializa el mapa agregando N claves (donde N = cantidadCorpus) con valor 0.
	 * Cada una de estas claves corresponde con el id del corpus, y cada valor 
	 * almacenará la cantidad de tokens unicos existentes para ese corpus.
	 * @param cantidadCorpus	Cantidad de claves a crear
	 * @return
	 */
	private Map<Integer, Integer> inicializarMapa(Integer cantidadCorpus) {
		Map<Integer,Integer> mapa = new HashMap<Integer,Integer>();
		for (int i = 0;i<cantidadCorpus;i++){
			mapa.put(i,0);
		}
		return mapa;
	}

	/**
	 * Recorre el mapa tokensByCorpus y devuelve aquella clave que contenga menos tokens unicos (valor).
	 * En caso de haber mas de una clave con el mismo valor minimo, se devuelve la primera.
	 * @param tokensByCorpus
	 * @return
	 */
	private Integer getIdCorpusSmallestTokens(Map<Integer, Integer> tokensByCorpus){
		Integer idCorpus = 0;
		Integer min = -1;
		for (Entry<Integer, Integer> entry : tokensByCorpus.entrySet()){
			Integer valor = entry.getValue();
			if (min == -1 || valor < min){
				min = valor;
				idCorpus = entry.getKey();
			}
		}
		return idCorpus;
	}

	private void showCorpusInfo(Map<Integer, Integer> tokensByCorpus){
		logger.info("------------------------------------");
		logger.info("INICIO MOSTRAR INFO CORPUS");
		logger.info("------------------------------------");
		logger.info("Criterio de elección de corpus: Cantidad tokens únicos");
		for (Entry<Integer, Integer> entry : tokensByCorpus.entrySet()){
			Integer valor = entry.getValue();
			Integer idCorpus = entry.getKey();
			logger.info("Corpus " + idCorpus + " tiene " + valor + " tokens únicos");
		}
		logger.info("------------------------------------");
		logger.info("FIN MOSTRAR INFO CORPUS");
		logger.info("------------------------------------");
	}
}
