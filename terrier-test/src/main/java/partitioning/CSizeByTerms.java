package partitioning;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.log4j.Logger;
import org.terrier.structures.Index;
import org.terrier.structures.Lexicon;
import org.terrier.structures.LexiconEntry;
import org.terrier.structures.PostingIndex;
import org.terrier.structures.postings.IterablePosting;

import util.CUtil;
import configuration.CParameters;

public class CSizeByTerms implements IPartitionByTerms {

	static final Logger logger = Logger.getLogger(CSizeByTerms.class);
	        
    	public Collection<String> createCorpus(String folderPath, String destinationFolderPath, Integer cantidadCorpus, Index index, CParameters parameters) {
    		List<String> colCorpusTotal = new ArrayList<String>();
            /* Terminos en documentos para cada Nodo */
            try{
            	logger.info("Metodo de particion: " + CRoundRobinByTerms.class.getName());
            	/* Se crean los corpus vacios, y se agregan a la coleccion de corpus total */
            	colCorpusTotal.addAll(CUtil.crearCorpusVacios(destinationFolderPath, CRoundRobinByTerms.class.getName(), cantidadCorpus, parameters));
            	Long terminosProcesados = 0L;
            	Long cantidadTotalTerminos = (long) index.getLexicon().numberOfEntries();
            	/* Almacena el balance de carga de cada nodo */
            	Map<Long, Long> nodeBalance = new HashMap<Long, Long>();
                for (Long i=0L;i<cantidadCorpus;i++){
                	nodeBalance.put(i, 0L);
                }
            	while (terminosProcesados < cantidadTotalTerminos){
            		terminosProcesados = generateCorpus(index, cantidadCorpus, terminosProcesados, destinationFolderPath, colCorpusTotal, nodeBalance);
            	}
    			/* Mostrar info de corpus */
//    			showCorpusInfo(mapNodeDocTerm);
    	    } catch (IOException e) {
    	        e.printStackTrace();
    	    }
    		return colCorpusTotal;
    	}

	private Long generateCorpus(Index index, Integer cantidadCorpus, Long terminoDesde, String destinationFolderPath, List<String> colCorpusTotal, Map<Long, Long> nodeBalance) throws IOException {
		/* Map<NodeId, Map<DocId, Collection<Terminos>>> */
        Map<Long, Map<Long,Map<String, Long>>> mapNodeDocTerm = new HashMap<Long, Map<Long, Map<String, Long>>>();
		/* Obtengo mapa de lexicon */
		Lexicon<String> mapLexicon = index.getLexicon();
		Long contador = terminoDesde;
		Long cantidadProcesada = 0L;
		for (Entry<String, LexiconEntry> lexicon : mapLexicon){
			if (contador >= terminoDesde){
				Long nodeId = getNodeMin(nodeBalance);
				contador++;
	//		    logger.info("Término " + lexicon.getKey() + " Frecuencia (cant de docs): " + lexicon.getValue().getDocumentFrequency());
			    PostingIndex<?> postingIndex = index.getInvertedIndex();
		        IterablePosting iterablePosting = postingIndex.getPostings(lexicon.getValue());
			        while (!iterablePosting.endOfPostings()){
			        	/* Leo siguiente postingList */
			            iterablePosting.next();
			            /* Si no existe relacion para el Nodo en cuestion la creo */
			            if (mapNodeDocTerm.get(nodeId) == null){
			            	mapNodeDocTerm.put(nodeId, new HashMap<Long,Map<String, Long>>());
			            }
			            /* Si para un Doc no existe lista de terminos la creo, sino devuelvo la existente */
			            Long postingListId = Long.valueOf(iterablePosting.getId());
			            Map<String, Long> termList = mapNodeDocTerm.get(nodeId).get(postingListId) == null? new HashMap<String, Long>() : mapNodeDocTerm.get(nodeId).get(postingListId);
			            if (termList.get(lexicon.getKey()) != null){
			            	termList.put(lexicon.getKey(), termList.get(lexicon.getKey()) + iterablePosting.getFrequency());
			            }else{
			            	termList.put(lexicon.getKey(), Long.valueOf(iterablePosting.getFrequency()));
			            }
			            /* Balance */
			            nodeBalance.put(nodeId, Long.valueOf(iterablePosting.getFrequency()));
			            cantidadProcesada += iterablePosting.getFrequency();
			            /* Guardo la relacion Doc y sus terminos */
			            mapNodeDocTerm.get(nodeId).put(postingListId,termList);
			            if (cantidadProcesada > IPartitionByTerms.cantidadMaximaTokensAntesCierre){
			    			/* Escribo los corpus */
			    			writeDoc(mapNodeDocTerm, cantidadCorpus, destinationFolderPath, colCorpusTotal);
			    			return contador;
			            }
			        }
				}
		}
		if (cantidadProcesada > 0){
			/* Escribo los corpus */
			writeDoc(mapNodeDocTerm, cantidadCorpus, destinationFolderPath, colCorpusTotal);
		}
		return contador;
	}
	
	public void writeDoc(Map<Long, Map<Long, Map<String, Long>>> mapNodeDocTerm, Integer cantidadCorpus, String destinationFolderPath, List<String> colCorpusTotal) {
		Map<String, StringBuffer> mapaCorpusContenido = new HashMap<String, StringBuffer>();
        try{
        	/* Inicializo el mapa con la ruta de los corpus vacios */
        	for (String pathCorpus : colCorpusTotal){
        		mapaCorpusContenido.put(pathCorpus, new StringBuffer());
        	}
        	Long tamanioBuffer = 0L;
        	for (Long nodeId : mapNodeDocTerm.keySet()){
        		StringBuffer contenido = new StringBuffer();
				for (Long docId : mapNodeDocTerm.get(nodeId).keySet()){
		    		/* Escribo contenido del archivo en el corpus con formato TREC */
		            contenido.append("<DOC>\n");
		            contenido.append("<DOCNO>"+ docId +"</DOCNO>\n");
		            /* Terminos */
		            for (String term : mapNodeDocTerm.get(nodeId).get(docId).keySet()){
		            	Long freq =  mapNodeDocTerm.get(nodeId).get(docId).get(term);
		            	for (Long i=0L;i<freq;i++){
		            		contenido.append(term).append(" ");
		            	}
		            }
		            contenido.append("\n</DOC>\n");
		            contenido.append("\n");
		            /* Obtengo el path del corpus actual */
		            String corpusPath = colCorpusTotal.get(Integer.valueOf(nodeId.toString()));
		            /* Escribo contenido en el buffer del corpus actual */
	                mapaCorpusContenido.put(corpusPath, contenido);
	                /* Guardo el tamaño del contenido */
	                tamanioBuffer += contenido.length();
	                /* Si ya se procesaron mas de la cantidad de archivos permitidas, se impactan */
	                if (tamanioBuffer > IPartitionByDocuments.tamanioMaximoAntesCierre){
	                	tamanioBuffer = 0L;
	                	/* Guardo el contenido de todos los corpus en los archivos haciendo un append si ya existe */
	                	CUtil.crearCorpusConDocumentos(mapaCorpusContenido, Boolean.TRUE);
	                	/* Inicializo el mapa con la ruta de los corpus vacios */
	                	for (String pathCorpus : colCorpusTotal){
	                		mapaCorpusContenido.put(pathCorpus, new StringBuffer());
	                	}
	                	contenido = new StringBuffer();
	                }
				}
        	}
        	if (tamanioBuffer > 0){
	        	/* Guardo el contenido de todos los corpus en los archivos haciendo un append si ya existe */
	        	CUtil.crearCorpusConDocumentos(mapaCorpusContenido, Boolean.TRUE);
        	}
        }catch(Exception e){
        	e.printStackTrace();
        }
	}

	/**
	 * Recorre el mapa nodeBalance y devuelve aquella clave que contenga menos el valor mas pequeño.
	 * En caso de haber mas de una clave con el mismo valor minimo, se devuelve la primera.
	 * @param nodeBalance<nodeId,cantidadTokens>
	 * @return
	 */
	private Long getNodeMin(Map<Long, Long> nodeBalance) {
		Long nodeId=0L;
		Long min= -1L;
		/* Busco el nodo menos cargado */
		for (Long id : nodeBalance.keySet()){
			Long minAux=nodeBalance.get(id);
			if (minAux < min || min == -1L){
				min = minAux;
				nodeId = id;
			}
		}
		return nodeId;
	}

	private void showCorpusInfo(Map<Long, Map<Long, Collection<String>>> mapNodeDocTerm){
		logger.info("------------------------------------");
		logger.info("INICIO MOSTRAR INFO CORPUS");
		logger.info("------------------------------------");
		logger.info("Criterio de elección de corpus: Cantidad de posting lists");
		for (Long id : mapNodeDocTerm.keySet()){
			Long tamanioPostingLists=0L;
			for (Long docId : mapNodeDocTerm.get(id).keySet()){
				tamanioPostingLists+= mapNodeDocTerm.get(id).get(docId).size();
			}
    		logger.info("Corpus " + id + " tiene " + mapNodeDocTerm.get(id).size() + " documentos y un total de " + tamanioPostingLists + " tokens");
		}
		logger.info("------------------------------------");
		logger.info("FIN MOSTRAR INFO CORPUS");
		logger.info("------------------------------------");
	}
}