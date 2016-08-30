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
        Map<Integer, Map<Integer, Collection<String>>> mapNodeDocTerm = new HashMap<Integer, Map<Integer, Collection<String>>>();
        /* Doc y su docPath */
        Map<Integer,String> mapDocDocPath = new HashMap<Integer,String>();
        try{
        	logger.info("Metodo de particion: " + CRoundRobinByTerms.class.getName());
        	/* Se crean los corpus vacios, y se agregan a la coleccion de corpus total */
        	colCorpusTotal.addAll(CUtil.crearCorpusVacios(destinationFolderPath, CRoundRobinByTerms.class.getName(), cantidadCorpus, parameters));
			/* Obtengo metaIndex (para leer el docPath) */
			MetaIndex meta = index.getMetaIndex();
			/* Obtengo mapa? */
			Lexicon<String> mapLexicon = index.getLexicon();
			int contador = cantidadCorpus;
			for (Entry<String, LexiconEntry> lexicon : mapLexicon){
				int nodeId = contador++ % cantidadCorpus;
//			    logger.info("Término " + lexicon.getKey() + " Frecuencia (cant de docs): " + lexicon.getValue().getDocumentFrequency());
			    PostingIndex<?> postingIndex = index.getInvertedIndex();
		        IterablePosting iterablePosting = postingIndex.getPostings(lexicon.getValue());
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
			/* Mostrar info de corpus */
			showCorpusInfo(mapNodeDocTerm);
	    } catch (IOException e) {
	        e.printStackTrace();
	    }
		return colCorpusTotal;
	}

	public void writeDoc(Map<Integer, Map<Integer, Collection<String>>> mapNodeDocTerm, Map<Integer,String> mapDocDocPath, Integer cantidadCorpus, String destinationFolderPath, List<String> colCorpusTotal) {
        PrintWriter pw = null;
        try {
        	for (Integer nodeId : mapNodeDocTerm.keySet()){
				for (int docId : mapNodeDocTerm.get(nodeId).keySet()){
		   			/* Abro el corpus correspondiente */
					String corpusPath = colCorpusTotal.get(Integer.valueOf(nodeId.toString()));
		    	    FileOutputStream fileOutputStream;
					fileOutputStream = new FileOutputStream(new File(corpusPath), Boolean.TRUE);
		            pw = new PrintWriter(fileOutputStream);     
		    		/* Escribo contenido del archivo en el corpus con formato TREC */
		            pw.append("<DOC>\n");
		            pw.append("<DOCNO>"+ docId +"</DOCNO>\n");
		            /* TODO ¡REVISAR SI LA CANTIDAD MAXIMA DE CARACTERES PARA EL DOCPATH ALCANZA BIEN! */
//		            pw.append("<DOCPATH>" + mapDocDocPath.get(docId) + "</DOCPATH>\n");
//		            pw.append("<TEXT>\n");
		            /* Terminos */
		            for (String term : mapNodeDocTerm.get(nodeId).get(docId)){
		            	pw.append(term).append(" ");
		            }
//		            pw.append("\n</TEXT>\n");
		            pw.append("\n</DOC>\n");
		            pw.append("\n");  
		            pw.close();
				}
        	}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
	}
	
	private void showCorpusInfo(Map<Integer, Map<Integer, Collection<String>>> mapNodeDocTerm){
		logger.info("------------------------------------");
		logger.info("INICIO MOSTRAR INFO CORPUS");
		logger.info("------------------------------------");
		logger.info("Criterio de elección de corpus: Cantidad de posting lists");
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
