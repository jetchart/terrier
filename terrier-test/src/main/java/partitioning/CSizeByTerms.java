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
import org.terrier.structures.MetaIndex;
import org.terrier.structures.PostingIndex;
import org.terrier.structures.postings.IterablePosting;

import util.CUtil;
import configuration.CParameters;

public class CSizeByTerms implements IPartitionByTerms {

	static final Logger logger = Logger.getLogger(CSizeByTerms.class);
	
	public Collection<String> createCorpus(String folderPath, String destinationFolderPath, Integer cantidadCorpus, Index index, CParameters parameters) {
		List<String> colCorpusTotal = new ArrayList<String>();
        /* */
        Map<Long, Map<String,Long>> mapNodeDocTerm = new HashMap<Long, Map<String,Long>>();
        Map<Long, Long> mapNodeLoaded =  new HashMap<Long, Long>();
        /* Doc y su docPath */
        Map<Long,String> mapDocDocPath = new HashMap<Long,String>();
        try{
        	logger.info("Metodo de particion: " + CRoundRobinByTerms.class.getName());
        	/* Se crean los corpus vacios, y se agregan a la coleccion de corpus total */
        	colCorpusTotal.addAll(CUtil.crearCorpusVacios(destinationFolderPath, CRoundRobinByTerms.class.getName(), cantidadCorpus, parameters));
			/* Obtengo mapa? */
			Lexicon<String> mapLexicon = index.getLexicon();
			logger.info("numberOfEntries: " + mapLexicon.numberOfEntries());
			
			Map<Long, Collection<String>> nodeTerms = calculateBalance(index, cantidadCorpus);
			for (Long nodeId=0L;nodeId<cantidadCorpus;nodeId++){
				for (Entry<String, LexiconEntry> lexicon : mapLexicon){
					Long nodeIdCalculado = 0L;
					for (Long i=0L;i<cantidadCorpus;i++){
						if (nodeTerms.get(i).contains(lexicon.getKey())){
							nodeIdCalculado = i;
							break;
						}
					}
	//			    logger.info("Término " + lexicon.getKey() + " Frecuencia (cant de docs): " + lexicon.getValue().getDocumentFrequency());
				    PostingIndex<?> postingIndex = index.getInvertedIndex();
			        IterablePosting iterablePosting = postingIndex.getPostings(lexicon.getValue());
			        if (nodeId == nodeIdCalculado){
				        while (!iterablePosting.endOfPostings()){
				        	/* Leo siguiente postingList */
				            iterablePosting.next();
				            /* Si para un Doc no existe lista de terminos la creo, sino devuelvo la existente */
				            Long postingListId = Long.valueOf(iterablePosting.getId());
				            Map<String,Long> termList = mapNodeDocTerm.get(postingListId) == null? new HashMap<String,Long>() : mapNodeDocTerm.get(postingListId);
				            /* termList --> termino, frecuencia */
				            /* Si termList no existe, le indico la frecuencia actual*/
				            if (termList.get(lexicon.getKey()) == null){
				            	termList.put(lexicon.getKey(),Long.valueOf(iterablePosting.getFrequency()));
				            }else{
				            	/* Si termList SI existe, le sumo la frecuencia actual a las ya existentes */
				            	termList.put(lexicon.getKey(),termList.get(lexicon.getKey())+iterablePosting.getFrequency());
				            }
				            /* Guardo la relacion Doc y sus terminos */
				            mapNodeDocTerm.put(postingListId,termList);
				        }
			        }
				}
				/* Escribo los corpus */
				writeDoc1(nodeId, mapNodeDocTerm, mapDocDocPath, cantidadCorpus, destinationFolderPath, colCorpusTotal);
				mapNodeDocTerm = new HashMap<Long, Map<String,Long>>();
			}
			/* Mostrar info de corpus */
//			showCorpusInfo(mapNodeDocTerm);
	    } catch (IOException e) {
	        e.printStackTrace();
	    }
		return colCorpusTotal;
	}

	public Map<Long, Collection<String>> calculateBalance(Index index, Integer cantidadCorpus) throws IOException{
		Map<Long, Collection<String>> nodeTerms = new HashMap<Long, Collection<String>>();
		Map<Long, Long> nodeBalance = new HashMap<Long, Long>();
		for (Long i=0L;i<cantidadCorpus;i++){
			nodeTerms.put(i, new ArrayList<String>());
			nodeBalance.put(i, 0L);
		}
		Lexicon<String> mapLexicon = index.getLexicon();
		for (Entry<String, LexiconEntry> lexicon : mapLexicon){
		    PostingIndex<?> postingIndex = index.getInvertedIndex();
	        IterablePosting iterablePosting = postingIndex.getPostings(lexicon.getValue());
	        Long carga = 0L;
	        while (!iterablePosting.endOfPostings()){
	        	iterablePosting.next();
	        	carga+= iterablePosting.getFrequency();
	        }
	        Long nodeMin = getNodeMin(nodeBalance);
	        Collection<String> termsList = nodeTerms.get(nodeMin);
	        termsList.add(lexicon.getKey());
	        nodeTerms.put(nodeMin, termsList);
	        nodeBalance.put(nodeMin, nodeBalance.get(nodeMin)+carga);
		}
		return nodeTerms;
	}
	
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

	public void writeDoc1(Long nodeId, Map<Long, Map<String,Long>> mapNodeDocTerm, Map<Long,String> mapDocDocPath, Integer cantidadCorpus, String destinationFolderPath, List<String> colCorpusTotal) {
		Map<String, StringBuffer> mapaCorpusContenido = new HashMap<String, StringBuffer>();
        try{
//       		mapaCorpusContenido.put(colCorpusTotal.get(nodeId.intValue()), new StringBuffer());
        	Long tamanioBuffer = 0L;
        		StringBuffer contenido = new StringBuffer();
				for (Long docId : mapNodeDocTerm.keySet()){
		    		/* Escribo contenido del archivo en el corpus con formato TREC */
		            contenido.append("<DOC>\n");
		            contenido.append("<DOCNO>"+ docId +"</DOCNO>\n");
		            /* TODO ¡REVISAR SI LA CANTIDAD MAXIMA DE CARACTERES PARA EL DOCPATH ALCANZA BIEN! */
//		            pw.append("<DOCPATH>" + mapDocDocPath.get(docId) + "</DOCPATH>\n");
//		            pw.append("<TEXT>\n");
		            /* Terminos */
		            for (String term : mapNodeDocTerm.get(docId).keySet()){
		            	Long freq = mapNodeDocTerm.get(docId).get(term);
		            	for (Long i=0L;i<freq;i++){
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
	                	mapaCorpusContenido = new HashMap<String, StringBuffer>();
	                	mapaCorpusContenido.put(colCorpusTotal.get(nodeId.intValue()), new StringBuffer());
	                	contenido = new StringBuffer();
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
	
	/**
	 * Recorre el mapa mapNodeDocTerm y devuelve aquella clave que contenga menos carga de Documentos y Terminos.
	 * En caso de haber mas de una clave con el mismo valor minimo, se devuelve la primera.
	 * @param tokensByCorpus
	 * @return
	 */
	private Long getNodeId1(Map<Long, Map<Long, Map<String, Integer>>> mapNodeDocTerm, Integer cantidadCorpus){
		Long nodeId=0L;
		Long min= -1L;
		/* Si aún no se utilizó algún nodo, se lo asigno a él */
		if (mapNodeDocTerm.keySet().size() < cantidadCorpus){
			return Long.valueOf(mapNodeDocTerm.keySet().size());
		}
		/* Busco el nodo menos cargado */
		for (Long id : mapNodeDocTerm.keySet()){
			Long minAux=0L;
			for (Long docId : mapNodeDocTerm.get(id).keySet()){
				for (String term : mapNodeDocTerm.get(id).get(docId).keySet()){
					minAux+= mapNodeDocTerm.get(id).get(docId).get(term);
				}
			}
			if (minAux < min || min == -1L){
				min = minAux;
				nodeId = id;
			}
		}
		return nodeId;
	}
	
	/**
	 * Recorre el mapa mapNodeDocTerm y devuelve aquella clave que contenga menos carga de Documentos y Terminos.
	 * En caso de haber mas de una clave con el mismo valor minimo, se devuelve la primera.
	 * @param tokensByCorpus
	 * @return
	 */
	private Long getNodeId(Map<Long, Map<Long, Map<String, Integer>>> mapNodeDocTerm, Integer cantidadCorpus){
		Long nodeId=0L;
		Long min= -1L;
		/* Si aún no se utilizó algún nodo, se lo asigno a él */
		if (mapNodeDocTerm.keySet().size() < cantidadCorpus){
			return Long.valueOf(mapNodeDocTerm.keySet().size());
		}
		/* Busco el nodo menos cargado */
		for (Long id : mapNodeDocTerm.keySet()){
			Long minAux=0L;
			for (Long docId : mapNodeDocTerm.get(id).keySet()){
				for (String term : mapNodeDocTerm.get(id).get(docId).keySet()){
					minAux+= mapNodeDocTerm.get(id).get(docId).get(term);
				}
			}
			if (minAux < min || min == -1L){
				min = minAux;
				nodeId = id;
			}
		}
		return nodeId;
	}

	private void showCorpusInfo(Map<Long, Map<Long, Collection<String>>> mapNodeDocTerm, Integer cantidadCorpus){
		logger.info("------------------------------------");
		logger.info("INICIO MOSTRAR INFO CORPUS");
		logger.info("------------------------------------");
		logger.info("Criterio de elección de corpus: Tamaño de posting lists");
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