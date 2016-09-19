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

public class CRoundRobinByTerms implements IPartitionByTerms {

	static final Logger logger = Logger.getLogger(CRoundRobinByTerms.class);
	
	public Collection<String> createCorpus(String folderPath, String destinationFolderPath, Integer cantidadCorpus, Index index, CParameters parameters) {
		List<String> colCorpusTotal = new ArrayList<String>();
        /* Se eliminan archivos temporales de corridas previas */
		logger.info("Eliminando archivos temporales..");
		CUtil.deleteFolderFiles(destinationFolderPath+"tmp/");
        try{
        	/* Se crean los corpus vacios, y se agregan a la coleccion de corpus total */
        	colCorpusTotal.addAll(CUtil.crearCorpusVacios(destinationFolderPath, CRoundRobinByTerms.class.getName(), cantidadCorpus, parameters));
        	Long terminosProcesados = 0L;
        	Long cantidadTotalTerminos = (long) index.getLexicon().numberOfEntries();
    		logger.info("Comenzando a procesar los términos");
        	while (terminosProcesados < cantidadTotalTerminos){
        		terminosProcesados = generateCorpus(index, cantidadCorpus, terminosProcesados, destinationFolderPath, colCorpusTotal);
        	}
        	logger.info("Comenzando a escribir los corpus finales");
        	writeFinalCorpus(cantidadCorpus,destinationFolderPath,colCorpusTotal);
			/* Mostrar info de corpus */
//			showCorpusInfo(mapNodeDocTerm);
            /* Se eliminan archivos temporales de corridas previas */
    		logger.info("Eliminando archivos temporales..");
    		CUtil.deleteFolderFiles(destinationFolderPath+"tmp/");
	    } catch (IOException e) {
	        e.printStackTrace();
	    }
		return colCorpusTotal;
	}

	private void writeFinalCorpus(Integer cantidadCorpus, String destinationFolderPath, List<String> colCorpusTotal) throws IOException {
		Map<String, StringBuffer> mapaCorpusContenido = new HashMap<String, StringBuffer>();
    	/* Inicializo el mapa con la ruta de los corpus vacios */
    	for (String pathCorpus : colCorpusTotal){
    		mapaCorpusContenido.put(pathCorpus, new StringBuffer());
    	}
    	Long tamanioBuffer = 0L;
		for (Integer i=0;i<cantidadCorpus;i++){
			Collection<String> docs = CUtil.getFilesFromFolderByPrefix("node"+i+"_",new ArrayList<String>(),destinationFolderPath+"tmp/",Boolean.FALSE);
			String corpusPath = colCorpusTotal.get(i);
			StringBuffer contenido = mapaCorpusContenido.get(corpusPath);
			for (String docPath : docs){
				String docId = docPath.split("/")[docPath.split("/").length-1].replace("node"+i+"_", "").replace(".trec", "");
	    		/* Escribo contenido del archivo en el corpus con formato TREC */
	            contenido.append("<DOC>\n");
	            contenido.append("<DOCNO>"+ docId +"</DOCNO>\n");
	            contenido.append(CUtil.leerArchivo(docPath));
	            contenido.append("\n</DOC>\n");
	            /* Escribo contenido en el buffer del corpus actual */
				mapaCorpusContenido.put(corpusPath, contenido);
                /* Guardo el tamaño del contenido */
                tamanioBuffer += contenido.length();
                /* Si ya se procesaron mas de la cantidad de archivos permitidas, se impactan */
                if (tamanioBuffer > IPartitionByDocuments.tamanioMaximoAntesCierre){
                	tamanioBuffer = 0L;
                	/* Guardo el contenido de todos los corpus en los archivos haciendo un append si ya existe */
                	CUtil.crearCorpusConDocumentos(mapaCorpusContenido, Boolean.TRUE);
                	mapaCorpusContenido = new HashMap<String, StringBuffer>();
                }
			}
		}
        /* Si ya se procesaron mas de la cantidad de archivos permitidas, se impactan */
        if (tamanioBuffer > 0){
        	/* Guardo el contenido de todos los corpus en los archivos haciendo un append si ya existe */
        	CUtil.crearCorpusConDocumentos(mapaCorpusContenido, Boolean.TRUE);
        }
	}

	private Long generateCorpus(Index index, Integer cantidadCorpus, Long terminoDesde, String destinationFolderPath, List<String> colCorpusTotal) throws IOException {
		/* Map<NodeId, Map<DocId, Collection<Terminos>>> */
        Map<Long, Map<Long,Map<String, Long>>> mapNodeDocTerm = new HashMap<Long, Map<Long, Map<String, Long>>>();
		/* Obtengo mapa de lexicon */
		Lexicon<String> mapLexicon = index.getLexicon();
		Long contador = 0L;
		Long cantidadProcesada = 0L;
		PostingIndex<?> postingIndex = index.getInvertedIndex();
		for (Entry<String, LexiconEntry> lexicon : mapLexicon){
			if (contador >= terminoDesde){
				Long nodeId = contador % cantidadCorpus;
	//		    logger.info("Término " + lexicon.getKey() + " Frecuencia (cant de docs): " + lexicon.getValue().getDocumentFrequency());
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
			            /* Guardo la relacion Doc y sus terminos */
			            mapNodeDocTerm.get(nodeId).put(postingListId,termList);
			            cantidadProcesada += iterablePosting.getFrequency();
			            if (cantidadProcesada > IPartitionByTerms.cantidadMaximaTokensAntesCierre){
			    			/* Escribo los corpus */
			    			writeDoc(mapNodeDocTerm, cantidadCorpus, destinationFolderPath, colCorpusTotal);
			    			return contador+1;
			            }
			        }
				}
			contador++;
		}
		if (cantidadProcesada > 0){
			/* Escribo los corpus */
			writeDoc(mapNodeDocTerm, cantidadCorpus, destinationFolderPath, colCorpusTotal);
		}
		return contador;
	}

	public void writeDoc(Map<Long, Map<Long, Map<String, Long>>> mapNodeDocTerm, Integer cantidadCorpus, String destinationFolderPath, List<String> colCorpusTotal) {
		Map<String, StringBuffer> mapaDocContenido = new HashMap<String, StringBuffer>();
        try{
        	Long tamanioBuffer = 0L;
        	for (Long nodeId : mapNodeDocTerm.keySet()){
        		for (Long docId : mapNodeDocTerm.get(nodeId).keySet()){
        			StringBuffer contenido = new StringBuffer();
		    		/* Escribo contenido del archivo en el corpus con formato TREC */
		            /* Terminos */
		            for (String term : mapNodeDocTerm.get(nodeId).get(docId).keySet()){
		            	Long freq =  mapNodeDocTerm.get(nodeId).get(docId).get(term);
		            	for (Long i=0L;i<freq;i++){
		            		contenido.append(term).append(" ");
		            	}
		            }
		            /* Escribo contenido en el buffer del corpus actual */
		            mapaDocContenido.put(destinationFolderPath+"tmp/node"+nodeId+"_"+docId+".trec", contenido);
	                /* Guardo el tamaño del contenido */
	                tamanioBuffer += contenido.length();
	                /* Si ya se procesaron mas de la cantidad de archivos permitidas, se impactan */
	                if (tamanioBuffer > IPartitionByDocuments.tamanioMaximoAntesCierre){
	                	tamanioBuffer = 0L;
	                	/* Guardo el contenido de todos los corpus en los archivos haciendo un append si ya existe */
	                	CUtil.crearCorpusConDocumentos(mapaDocContenido, Boolean.TRUE);
	                	mapaDocContenido = new HashMap<String, StringBuffer>();
	                }
				}
        	}
        	if (tamanioBuffer > 0){
	        	/* Guardo el contenido de todos los corpus en los archivos haciendo un append si ya existe */
	        	CUtil.crearCorpusConDocumentos(mapaDocContenido, Boolean.TRUE);
        	}
        }catch(Exception e){
        	e.printStackTrace();
        }
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