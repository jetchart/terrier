package partitioning;

import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.io.Reader;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.terrier.structures.Index;

import util.CUtil;

public class CSizeTokensByDocuments implements IPartitionByDocuments {

	public Collection<String> createCorpus(String folderPath, String destinationFolderPath, Integer cantidadCorpus, Index index) {
		Collection<String> colCorpusTotal = new ArrayList<String>();
		FileWriter fichero = null;
        PrintWriter pw = null;
    	Map<Integer,Integer> tokensByCorpus;
        try
        {
        	System.out.println("Metodo de particion: " + CSizeTokensByDocuments.class.getName());
        	/* Se obtienen todos los ficheros del folder "folderPath" */
        	java.util.Collection<String> filesPath = CUtil.getFilesFromFolder(new ArrayList<String>(),folderPath, Boolean.TRUE);
        	/* Se obtiene cantidad de ficheros */
        	int cantidadTotalArchivos = filesPath.size();
        	System.out.println("Cantidad de corpus a crear: " + cantidadCorpus);
        	System.out.println("Cantidad de documentos: " + cantidadTotalArchivos);
        	/* Inicializar mapa tokens */
        	tokensByCorpus = this.inicializarMapa(cantidadCorpus);
        	/* Se crean los corpus vacios, y se agregan a la coleccion de corpus total */
        	colCorpusTotal.addAll(CUtil.crearCorpusVacios(destinationFolderPath, cantidadCorpus));
            Long docno = Long.valueOf(0);
            /* Se recorren los archivos del folder */
        	for (String filePath : filesPath){
        		Integer corpusId = getIdCorpusSmallestTokens(tokensByCorpus);
       			/* Creo nuevo corpus */
        		String corpusPath = destinationFolderPath + "corpus"+ corpusId +".txt";
        	    FileOutputStream fileOutputStream = new FileOutputStream(new File(corpusPath), Boolean.TRUE);
                pw = new PrintWriter(fileOutputStream);     
        		/* Escribo contenido del archivo en el corpus con formato TREC */
                pw.append("<DOC>");
                pw.append("<DOCNO>"+ docno++ +"</DOCNO>");
                /* TODO ¡REVISAR SI LA CANTIDAD MAXIMA DE CARACTERES PARA EL DOCPATH ALCANZA BIEN! */
                pw.append("<DOCPATH>" + filePath + "</DOCPATH>");
                pw.append("<TEXT>");
                /* Obtengo Reader del contenido del archivo */
                Reader reader = CUtil.getReaderArchivo(filePath);
                /* Obtengo la cantidad de tokens unicos que tiene el archivo y se lo sumo al corpus */
                Integer cantidadTokensUnicos = CUtil.getAmountUniqueTokensInReader(reader);
                tokensByCorpus.put(corpusId, tokensByCorpus.get(corpusId) + cantidadTokensUnicos);
                System.out.println("Se eligió al corpus " + corpusId + " porque tiene " + tokensByCorpus.get(corpusId)+ " tokens");
                /* Obtengo el contenido del Reader sin tags */
                String contenido = CUtil.extractContentInReader(reader);
                pw.append(contenido);
                pw.append("</TEXT>");
                pw.append("</DOC>");
                pw.append("");  
                pw.close();
        	}
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
           try {
           if (null != fichero)
              fichero.close();
           } catch (Exception e2) {
              e2.printStackTrace();
           }
        }
		return colCorpusTotal;
	}

	private Map<Integer, Integer> inicializarMapa(Integer cantidadCorpus) {
		Map<Integer,Integer> mapa = new HashMap<Integer,Integer>();
		for (int i = 0;i<cantidadCorpus;i++){
			mapa.put(i,0);
		}
		return mapa;
	}

	/* Devuelvo el ID del corpus de menor tamaño */
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
}
