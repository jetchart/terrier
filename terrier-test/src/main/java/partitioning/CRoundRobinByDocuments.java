package partitioning;

import java.io.FileWriter;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collection;

import org.terrier.structures.Index;

import util.CUtil;

public class CRoundRobinByDocuments implements IPartitionByDocuments {

	public Collection<String> createCorpus(String folderPath, String destinationFolderPath, Integer cantidadCorpus, Index index) {
		Collection<String> colCorpusTotal = new ArrayList<String>();
		FileWriter fichero = null;
        PrintWriter pw = null;
        try
        {
        	System.out.println("Metodo de particion: " + CRoundRobinByDocuments.class.getName());
        	/* Se obtienen todos los ficheros del folder "folderPath" */
        	java.util.Collection<String> filesPath = CUtil.getFilesFromFolder(new ArrayList<String>(),folderPath, Boolean.TRUE);
        	/* Se obtiene cantidad de ficheros */
        	int cantidadTotalArchivos = filesPath.size();
        	/* Se obtiene la cantidad de ficheros que tendrá cada corpus */
        	int cantidadArchivosPorCorpus = cantidadTotalArchivos / cantidadCorpus;

        	System.out.println("Cantidad de corpus a crear: " + cantidadCorpus);
        	System.out.println("Cantidad de documentos: " + cantidadTotalArchivos);
        	System.out.println("Cantidad de documentos por corpus: " + cantidadArchivosPorCorpus);
        	
        	/* Se crean los corpus vacios, y se agregan a la coleccion de corpus total */
        	colCorpusTotal.addAll(CUtil.crearCorpusVacios(destinationFolderPath, cantidadCorpus));
            /* Inicializo DOCNO */
            Long docno = Long.valueOf(0);
            /* Se recorren los archivos del folder */
        	for (String filePath : filesPath){
        		Long resto = docno % cantidadCorpus;
        		/* Indico corpus */
        		String corpusPath = destinationFolderPath + "corpus"+ resto +".txt";
        		fichero = new FileWriter(corpusPath,true);
                pw = new PrintWriter(fichero);             			
        		/* Escribo contenido del archivo en el corpus con formato TREC */
                pw.println("<DOC>");
                pw.println("<DOCNO>"+ docno++ +"</DOCNO>");
                /* TODO ¡REVISAR SI LA CANTIDAD MAXIMA DE CARACTERES PARA EL DOCPATH ALCANZA BIEN! */
                pw.println("<DOCPATH>" + filePath + "</DOCPATH>");
                pw.println("<TEXT>");
                /* Obtengo contenido del archivo sin tags */
                pw.println(CUtil.leerArchivo(filePath));
                pw.println("</TEXT>");
                pw.println("</DOC>");
                pw.println("");  
                fichero.close();
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
	
	

}
