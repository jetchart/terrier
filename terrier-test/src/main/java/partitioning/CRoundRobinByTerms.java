package partitioning;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
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
import org.terrier.structures.MetaIndex;
import org.terrier.structures.PostingIndex;
import org.terrier.structures.postings.IterablePosting;

import configuration.CParameters;
import util.CUtil;

public class CRoundRobinByTerms implements IPartitionByTerms {

	static final Logger logger = Logger.getLogger(CRoundRobinByTerms.class);
	
	public Collection<String> createCorpus(String folderPath, String destinationFolderPath, Integer cantidadCorpus, Index index, CParameters parameters) {
		List<String> colCorpusTotal = new ArrayList<String>();
        /* */
        Map<Long, Map<Long, Map<String,Integer>>> mapNodeDocTerm = new HashMap<Long, Map<Long, Map<String,Integer>>>();
        /* Doc y su docPath */
        Map<Long,String> mapDocDocPath = new HashMap<Long,String>();
        try{
        	logger.info("Metodo de particion: " + CRoundRobinByTerms.class.getName());
        	/* Se crean los corpus vacios, y se agregan a la coleccion de corpus total */
        	colCorpusTotal.addAll(CUtil.crearCorpusVacios(destinationFolderPath, CRoundRobinByTerms.class.getName(), cantidadCorpus, parameters));
			/* Obtengo metaIndex (para leer el docPath) */
			MetaIndex meta = index.getMetaIndex();
			/* Obtengo mapa? */
			Lexicon<String> mapLexicon = index.getLexicon();
			Long contador = Long.valueOf(cantidadCorpus);
			for (Entry<String, LexiconEntry> lexicon : mapLexicon){
				Long nodeId = contador++ % cantidadCorpus;
//			    logger.info("Término " + lexicon.getKey() + " Frecuencia (cant de docs): " + lexicon.getValue().getDocumentFrequency());
			    PostingIndex<?> postingIndex = index.getInvertedIndex();
		        IterablePosting iterablePosting = postingIndex.getPostings(lexicon.getValue());
			        while (!iterablePosting.endOfPostings()){
			        	/* Leo siguiente postingList */
			            iterablePosting.next();
			            /* Si no existe relacion para el Nodo en cuestion la creo */
			            if (mapNodeDocTerm.get(nodeId) == null){
			            	mapNodeDocTerm.put(nodeId, new HashMap<Long,Map<String,Integer>>());
			            }
			            /* Si para un Doc no existe lista de terminos la creo, sino devuelvo la existente */
			            Long postingListId = Long.valueOf(iterablePosting.getId());
			            Map<String,Integer> termList = mapNodeDocTerm.get(nodeId).get(postingListId) == null? new HashMap<String,Integer>() : mapNodeDocTerm.get(nodeId).get(postingListId);
			            /* termList --> termino, frecuencia */
			            /* Si termList no existe, le indico la frecuencia actual*/
			            if (termList.get(lexicon.getKey()) == null){
			            	termList.put(lexicon.getKey(),iterablePosting.getFrequency());
			            }else{
			            	/* Si termList SI existe, le sumo la frecuencia actual a las ya existentes */
			            	termList.put(lexicon.getKey(),termList.get(lexicon.getKey())+iterablePosting.getFrequency());
			            }
			            /* Guardo la relacion Doc y sus terminos */
			            mapNodeDocTerm.get(nodeId).put(postingListId,termList);
//			            /* Guardo la relacion Doc y su docPath */
//			            if (mapDocDocPath.get(postingListId) == null){
//			            	mapDocDocPath.put(postingListId, meta.getItem("DOCPATH", iterablePosting.getId()));
//			            }
			        }
			}
			/* Escribo los corpus */
			writeDoc(mapNodeDocTerm, mapDocDocPath, cantidadCorpus, destinationFolderPath, colCorpusTotal);
			/* Mostrar info de corpus */
//			showCorpusInfo(mapNodeDocTerm);
	    } catch (IOException e) {
	        e.printStackTrace();
	    }
		return colCorpusTotal;
	}

	public void writeDoc(Map<Long, Map<Long, Map<String,Integer>>> mapNodeDocTerm, Map<Long,String> mapDocDocPath, Integer cantidadCorpus, String destinationFolderPath, List<String> colCorpusTotal) {
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
		            /* TODO ¡REVISAR SI LA CANTIDAD MAXIMA DE CARACTERES PARA EL DOCPATH ALCANZA BIEN! */
//		            pw.append("<DOCPATH>" + mapDocDocPath.get(docId) + "</DOCPATH>\n");
//		            pw.append("<TEXT>\n");
		            /* Terminos */
		            for (String term : mapNodeDocTerm.get(nodeId).get(docId).keySet()){
		            	Integer freq = mapNodeDocTerm.get(nodeId).get(docId).get(term);
		            	for (int i=0;i<freq;i++){
		            		contenido.append(term).append(" ");
		            	}
		            }
//		            pw.append("\n</TEXT>\n");
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
	                	/* Guardo el contenido de todos los corpus en los archivos sobreescribiendo si ya existe */
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
	        	/* Guardo el contenido de todos los corpus en los archivos sobreescribiendo si ya existe */
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