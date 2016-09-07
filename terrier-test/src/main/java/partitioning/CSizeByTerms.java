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

public class CSizeByTerms implements IPartitionByTerms {

	static final Logger logger = Logger.getLogger(CSizeByTerms.class);
	
	public Collection<String> createCorpus(String folderPath, String destinationFolderPath, Integer cantidadCorpus, Index index, CParameters parameters) {
		List<String> colCorpusTotal = new ArrayList<String>();
        /* */
        Map<Integer, Map<Integer, Collection<String>>> mapNodeDocTerm = new HashMap<Integer, Map<Integer, Collection<String>>>();
        /* Doc y su docPath */
        Map<Integer,String> mapDocDocPath = new HashMap<Integer,String>();
        try{
        	logger.info("Metodo de particion: " + CSizeByTerms.class.getName());
        	/* Se crean los corpus vacios, y se agregan a la coleccion de corpus total */
        	colCorpusTotal.addAll(CUtil.crearCorpusVacios(destinationFolderPath, CSizeByTerms.class.getName(), cantidadCorpus, parameters));
			/* Obtengo metaIndex (para leer el docPath) */
			MetaIndex meta = index.getMetaIndex();
			/* Obtengo mapa? */
			Lexicon<String> mapLexicon = index.getLexicon();
			for (Entry<String, LexiconEntry> lexicon : mapLexicon){
	//		    logger.info("Término " + lexicon.getKey() + " Frecuencia (cant de docs): " + lexicon.getValue().getDocumentFrequency());
			    PostingIndex<?> postingIndex = index.getInvertedIndex();
			    IterablePosting iterablePosting = postingIndex.getPostings(lexicon.getValue());
	        	/* Obtengo nodeId analizando qué nodo tiene menos carga */
	        	int nodeId = this.getNodeId(mapNodeDocTerm, cantidadCorpus);
			        while (!iterablePosting.endOfPostings()){
			        	/* Leo siguiente postingList */
			            iterablePosting.next();
			            /* Si no existe relacion para el Nodo en cuestion la creo */
			            if (mapNodeDocTerm.get(nodeId) == null){
			            	mapNodeDocTerm.put(nodeId, new HashMap<Integer,Collection<String>>());
			            }
			            /* Si para un Doc no existe lista de terminos la creo, sino devuelvo la existente */
			            Collection<String> termList = mapNodeDocTerm.get(nodeId).get(iterablePosting.getId()) == null? new ArrayList<String>() : mapNodeDocTerm.get(nodeId).get(iterablePosting.getId());
			            /* Agrego el termino a la lista de terminos */
			            int i;
			            for (i=0;i<iterablePosting.getFrequency();i++){
			            	termList.add(lexicon.getKey());
			            }
			            /* Guardo la relacion Doc y sus terminos */
			            mapNodeDocTerm.get(nodeId).put(iterablePosting.getId(),termList);
			            /* Guardo la relacion Doc y su docPath */
			            if (mapDocDocPath.get(iterablePosting.getId()) == null){
			            	mapDocDocPath.put(iterablePosting.getId(), meta.getItem("DOCPATH", iterablePosting.getId()));
			            }
			        }
			}
			/* Escribo los corpus */
			writeDoc(mapNodeDocTerm, mapDocDocPath, cantidadCorpus, destinationFolderPath, colCorpusTotal);
	    } catch (IOException e) {
	        // TODO Auto-generated catch block
	        e.printStackTrace();
	    }
        /* Muestro info sobre los corpus */
        this.showCorpusInfo(mapNodeDocTerm, cantidadCorpus);
		return colCorpusTotal;
	}

	public void writeDoc(Map<Integer, Map<Integer, Collection<String>>> mapNodeDocTerm, Map<Integer,String> mapDocDocPath, Integer cantidadCorpus, String destinationFolderPath, List<String> colCorpusTotal) {
        Map<String, StringBuffer> mapaCorpusContenido = new HashMap<String, StringBuffer>();
        try{
        	/* Inicializo el mapa con la ruta de los corpus vacios */
        	for (String pathCorpus : colCorpusTotal){
        		mapaCorpusContenido.put(pathCorpus, new StringBuffer());
        	}
        	Long tamanioBuffer = 0L;
        	for (Integer nodeId : mapNodeDocTerm.keySet()){
        		StringBuffer contenido = new StringBuffer();
				for (int docId : mapNodeDocTerm.get(nodeId).keySet()){
		    		/* Escribo contenido del archivo en el corpus con formato TREC */
		            contenido.append("<DOC>\n");
		            contenido.append("<DOCNO>"+ docId +"</DOCNO>\n");
		            /* TODO ¡REVISAR SI LA CANTIDAD MAXIMA DE CARACTERES PARA EL DOCPATH ALCANZA BIEN! */
//		            pw.append("<DOCPATH>" + mapDocDocPath.get(docId) + "</DOCPATH>\n");
//		            pw.append("<TEXT>\n");
		            /* Terminos */
		            for (String term : mapNodeDocTerm.get(nodeId).get(docId)){
		            	contenido.append(term).append(" ");
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
	
	/**
	 * Recorre el mapa mapNodeDocTerm y devuelve aquella clave que contenga menos carga de Documentos y Terminos.
	 * En caso de haber mas de una clave con el mismo valor minimo, se devuelve la primera.
	 * @param tokensByCorpus
	 * @return
	 */
	private Integer getNodeId(Map<Integer, Map<Integer, Collection<String>>> mapNodeDocTerm, Integer cantidadCorpus){
		int nodeId=0;
		Long min= -1L;
		/* Si aún no se utilizó algún nodo, se lo asigno a él */
		if (mapNodeDocTerm.keySet().size() < cantidadCorpus){
			return mapNodeDocTerm.keySet().size();
		}
		/* Busco el nodo menos cargado */
		for (int id : mapNodeDocTerm.keySet()){
			Long minAux=0L;
			for (int docId : mapNodeDocTerm.get(id).keySet()){
				minAux+= mapNodeDocTerm.get(id).get(docId).size();
			}
			if (minAux < min || min == -1L){
				min = minAux;
				nodeId = id;
			}
		}
		return nodeId;
	}

	private void showCorpusInfo(Map<Integer, Map<Integer, Collection<String>>> mapNodeDocTerm, Integer cantidadCorpus){
		logger.info("------------------------------------");
		logger.info("INICIO MOSTRAR INFO CORPUS");
		logger.info("------------------------------------");
		logger.info("Criterio de elección de corpus: Tamaño de posting lists");
		for (int id : mapNodeDocTerm.keySet()){
			Long tamanioPostingLists=0L;
			for (int docId : mapNodeDocTerm.get(id).keySet()){
				tamanioPostingLists+= mapNodeDocTerm.get(id).get(docId).size();
			}
    		logger.info("Corpus " + id + " tiene " + mapNodeDocTerm.get(id).size() + " documentos y un total de " + tamanioPostingLists + " tokens");
		}
		logger.info("------------------------------------");
		logger.info("FIN MOSTRAR INFO CORPUS");
		logger.info("------------------------------------");
	}
}
