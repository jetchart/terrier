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
        /* Terminos en documentos para cada Nodo */
        try{
        	logger.info("Metodo de particion: " + CRoundRobinByTerms.class.getName());
        	/* Se crean los corpus vacios, y se agregan a la coleccion de corpus total */
        	colCorpusTotal.addAll(CUtil.crearCorpusVacios(destinationFolderPath, CRoundRobinByTerms.class.getName(), cantidadCorpus, parameters));
        	Long contador = 0L;
        	Long documentosProcesados = 0L;
        	Long cantidadTotalDocumentos = (long) index.getDocumentIndex().getNumberOfDocuments();
        	logger.info("Total documentos: " + cantidadTotalDocumentos);
        	while (documentosProcesados < cantidadTotalDocumentos){
        		documentosProcesados = generateCorpus(contador, index, cantidadCorpus, documentosProcesados, destinationFolderPath, colCorpusTotal);
        		logger.info("Documentos procesados: " + documentosProcesados);
        	}
			/* Mostrar info de corpus */
//			showCorpusInfo(mapNodeDocTerm);
	    } catch (IOException e) {
	        e.printStackTrace();
	    }
		return colCorpusTotal;
	}

	private Long generateCorpus(Long contador, Index index, Integer cantidadCorpus, Long documentoDesde, String destinationFolderPath, List<String> colCorpusTotal) throws IOException {
		/* Map<NodeId, Map<DocId, Collection<Terminos>>> */
        Map<Long, Map<Long,Map<String, Long>>> mapNodeDocTerm = new HashMap<Long, Map<Long, Map<String, Long>>>();
		/* Obtengo mapa de lexicon */
		Lexicon<String> mapLexicon = index.getLexicon();
		Long cantidadProcesada = 0L;
		PostingIndex<?> postingIndex = index.getInvertedIndex();
		for (Entry<String, LexiconEntry> lexicon : mapLexicon){
			Long nodeId = contador % cantidadCorpus;
			if (nodeId.equals(0L)){
				contador = 0L;
			}
			contador++;
	//		logger.info("Término " + lexicon.getKey() + " Frecuencia (cant de docs): " + lexicon.getValue().getDocumentFrequency());
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
			    if (documentoDesde <= postingListId && postingListId < documentoDesde+IPartitionByTerms.cantidadMaximaDocumentosAProcesar){
			    	Map<String, Long> termList = mapNodeDocTerm.get(nodeId).get(postingListId) == null? new HashMap<String, Long>() : mapNodeDocTerm.get(nodeId).get(postingListId);
				    if (termList.get(lexicon.getKey()) != null){
				      	termList.put(lexicon.getKey(), termList.get(lexicon.getKey()) + iterablePosting.getFrequency());
				    }else{
				       	termList.put(lexicon.getKey(), Long.valueOf(iterablePosting.getFrequency()));
				    }
				    /* Guardo la relacion Doc y sus terminos */
				    mapNodeDocTerm.get(nodeId).put(postingListId,termList);
				    cantidadProcesada += iterablePosting.getFrequency();
			    }
			}
		}
		/* Escribo los corpus */
		writeDoc(mapNodeDocTerm, cantidadCorpus, destinationFolderPath, colCorpusTotal);
		return documentoDesde+IPartitionByTerms.cantidadMaximaDocumentosAProcesar;
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