package partitioning;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.terrier.structures.Index;
import org.terrier.structures.Lexicon;
import org.terrier.structures.LexiconEntry;
import org.terrier.structures.MetaIndex;
import org.terrier.structures.PostingIndex;
import org.terrier.structures.postings.IterablePosting;

public class CSizeByTerms implements IPartitionByTerms {

	public Collection<String> createCorpus(String folderPath, String destinationFolderPath, int cantidadCorpus, Index index) {
		Collection<String> colCorpusTotal = new ArrayList<String>();
		FileWriter fichero = null;
        PrintWriter pw = null;
        /* */
        Map<Integer, Map<Integer, Collection<String>>> mapNodeDocTerm = new HashMap<Integer, Map<Integer, Collection<String>>>();
        /* Doc y su docPath */
        Map<Integer,String> mapDocDocPath = new HashMap<Integer,String>();
        try{
        	System.out.println("Metodo de particion: " + CSizeByTerms.class.getName());
	        /* Creo los corpus */
	        int i;
	    	for (i=0;i<cantidadCorpus;i++){
	    			/* Creo nuevo corpus */
	    			String corpusPath = destinationFolderPath + "corpus"+ i +".txt";
	                fichero = new FileWriter(corpusPath);
	                pw = new PrintWriter(fichero, Boolean.TRUE);      
	                pw.close();
	        		/* Se agrega path del corpus creado a la coleccion de corpus */
	        		colCorpusTotal.add(corpusPath);
	    	}
			/* Obtengo metaIndex (para leer el docPath) */
			MetaIndex meta = index.getMetaIndex();
			/* Obtengo mapa? */
			Lexicon<String> mapLexicon = index.getLexicon();
			for (Entry<String, LexiconEntry> lexicon : mapLexicon){
	//		    System.out.println("Término " + lexicon.getKey() + " Frecuencia (cant de docs): " + lexicon.getValue().getDocumentFrequency());
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
			writeDoc(mapNodeDocTerm, mapDocDocPath, cantidadCorpus, destinationFolderPath);
	    } catch (IOException e) {
	        // TODO Auto-generated catch block
	        e.printStackTrace();
	    }
		return colCorpusTotal;
	}

	public void writeDoc(Map<Integer, Map<Integer, Collection<String>>> mapNodeDocTerm, Map<Integer,String> mapDocDocPath, int cantidadCorpus, String destinationFolderPath) {
        PrintWriter pw = null;
        try {
        	for (int nodeId : mapNodeDocTerm.keySet()){
				for (int docId : mapNodeDocTerm.get(nodeId).keySet()){
		   			/* Creo nuevo corpus */
		    		String corpusPath = destinationFolderPath + "corpus"+ nodeId +".txt";
		    	    FileOutputStream fileOutputStream;
					fileOutputStream = new FileOutputStream(new File(corpusPath), Boolean.TRUE);
		            pw = new PrintWriter(fileOutputStream);     
		    		/* Escribo contenido del archivo en el corpus con formato TREC */
		            pw.append("<DOC>\n");
		            pw.append("<DOCNO>"+ docId +"</DOCNO>\n");
		            /* TODO ¡REVISAR SI LA CANTIDAD MAXIMA DE CARACTERES PARA EL DOCPATH ALCANZA BIEN! */
		            pw.append("<DOCPATH>" + mapDocDocPath.get(docId) + "</DOCPATH>\n");
		            pw.append("<TEXT>\n");
		            /* Terminos */
		            for (String term : mapNodeDocTerm.get(nodeId).get(docId)){
		            	pw.append(term).append(" ");
		            }
		            pw.append("\n</TEXT>\n");
		            pw.append("</DOC>\n");
		            pw.append("\n");  
		            pw.close();
				}
        	}
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	/* Devuelvo el ID del nodo que tiene menos carga de Documentos y Terminos */
	int getNodeId(Map<Integer, Map<Integer, Collection<String>>> mapNodeDocTerm, int cantidadCorpus){
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

}
