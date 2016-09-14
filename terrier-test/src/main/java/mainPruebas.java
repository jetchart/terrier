import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import javax.xml.parsers.ParserConfigurationException;

import org.terrier.realtime.memory.MemoryDocumentIndexMap;
import org.terrier.structures.Index;
import org.terrier.structures.Lexicon;
import org.terrier.structures.LexiconEntry;
import org.terrier.structures.PostingIndex;
import org.terrier.structures.postings.IterablePosting;
import org.xml.sax.SAXException;


public class mainPruebas {

	public static void main(String[] args) throws IOException, ParserConfigurationException, SAXException {
		System.out.println("Levantando índice..");
		Index index = Index.createIndex("/storage/juan/terrier-4.0/var/index/previousIndex/", "noProcess_partitioning.CRoundRobinByDocuments");
		System.out.println("Índice levantado");
		Integer cantidadDocumentos = index.getCollectionStatistics().getNumberOfDocuments();
		String fullPath = "/storage/juan/TERRIER_JAVA/master/corpus.trec";
	    PostingIndex<?> postingIndex = index.getInvertedIndex();
		Lexicon<String> mapLexicon = index.getLexicon();
		StringBuffer contenido = new StringBuffer();
		System.out.println("Total de documentos: " + cantidadDocumentos);
		for (Integer documentId=0;documentId<cantidadDocumentos;documentId++){
			System.out.println("Procesando documento: " + documentId);
			Map<String, Integer> map = new HashMap<String, Integer>();
			for (Entry<String, LexiconEntry> lexicon : mapLexicon){
				IterablePosting iterablePosting = postingIndex.getPostings(lexicon.getValue());
				while (!iterablePosting.endOfPostings()){
					iterablePosting.next();
					if (iterablePosting.getId() == documentId ){
						map.put(lexicon.getKey(), iterablePosting.getFrequency());
						break;
					}
				}
			}
	        contenido.append("<DOC>\n");
	        contenido.append("<DOCNO>"+ documentId +"</DOCNO>\n");
	        /* Terminos */
	        for (String term : map.keySet()){
	        	Integer freq =  map.get(term);
	        	for (Integer i=0;i<freq;i++){
	        		contenido.append(term).append(" ");
	        	}
	        }
	        contenido.append("\n</DOC>\n");
	        contenido.append("\n");
	        if (contenido.length() > 1073741824L){
	        	write(fullPath, contenido);
	        	contenido = new StringBuffer();
	        }
		}
        if (contenido.length() > 0){
        	write(fullPath, contenido);
        }
	}

	private static void write(String fullPath, StringBuffer contenido) throws IOException {
		FileWriter fichero = new FileWriter(fullPath,Boolean.TRUE);
		PrintWriter pw = new PrintWriter(fichero);
		pw.print(contenido);
		pw.close();
		fichero.close();
	}

	
}
