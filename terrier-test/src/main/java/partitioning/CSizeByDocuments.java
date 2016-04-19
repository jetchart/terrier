package partitioning;

import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collection;

import org.terrier.structures.Index;

import util.CUtil;

public class CSizeByDocuments implements IPartitionByDocuments {

	public Collection<String> createCorpus(String folderPath, String destinationFolderPath, int cantidadCorpus, Index index) {
		Collection<String> colCorpusTotal = new ArrayList<String>();
		FileWriter fichero = null;
        PrintWriter pw = null;
    	int corpusId=0;
        try
        {
        	System.out.println("Metodo de particion: " + CSizeByDocuments.class.getName());
        	/* Se obtienen todos los ficheros del folder "folderPath" */
        	java.util.Collection<String> filesPath = CUtil.getFilesFromFolder(new ArrayList<String>(),folderPath, Boolean.TRUE);
        	/* Se obtiene cantidad de ficheros */
        	int cantidadTotalArchivos = filesPath.size();
        	System.out.println("Cantidad de corpus a crear: " + cantidadCorpus);
        	System.out.println("Cantidad de documentos: " + cantidadTotalArchivos);
        	/* Se crea el primer corpus */
        	String corpusPath = destinationFolderPath + "corpus"+ corpusId +".txt";
            fichero = new FileWriter(corpusPath);
            pw = new PrintWriter(fichero);      
            /* Creo los corpus */
            int i;
        	for (i=0;i<cantidadCorpus;i++){
        			/* Creo nuevo corpus */
        			corpusPath = destinationFolderPath + "corpus"+ i +".txt";
                    fichero = new FileWriter(corpusPath);
                    pw = new PrintWriter(fichero, true);      
                    pw.close();
            		/* Se agrega path del corpus creado a la coleccion de corpus */
            		colCorpusTotal.add(corpusPath);
        	}
            Long docno = Long.valueOf(0);
            /* Se recorren los archivos del folder */
        	for (String filePath : filesPath){
       			/* Creo nuevo corpus */
        		corpusPath = destinationFolderPath + "corpus"+ getIdSmallestDocument(destinationFolderPath, cantidadCorpus) +".txt";
        	    FileOutputStream fileOutputStream = new FileOutputStream(new File(corpusPath), Boolean.TRUE);
                pw = new PrintWriter(fileOutputStream);     
        		/* Escribo contenido del archivo en el corpus con formato TREC */
                pw.append("<DOC>");
                pw.append("<DOCNO>"+ docno++ +"</DOCNO>");
                /* TODO ¡REVISAR SI LA CANTIDAD MAXIMA DE CARACTERES PARA EL DOCPATH ALCANZA BIEN! */
                pw.append("<DOCPATH>" + filePath + "</DOCPATH>");
                pw.append("<TEXT>");
                /* Obtengo contenido del archivo sin tags */
                pw.append(CUtil.leerArchivo(filePath));
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

	/* Devuelvo el ID del corpus de menor tamaño */
	private int getIdSmallestDocument(String destinationFolderPath, int cantidadCorpus){
        int i;
        long size = -1;
        int idSmallestDocument = 0;
    	for (i=0;i<cantidadCorpus;i++){
    			String corpusPath = destinationFolderPath + "corpus"+ i +".txt";
    			File file = new File (corpusPath);
    			if (file.length() < size || i == 0){
    				size = file.length();
    				idSmallestDocument = i;
    			}
    	}
		return idSmallestDocument;
	}
}
