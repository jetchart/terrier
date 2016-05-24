package partitioningBkp;

import java.io.FileWriter;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collection;

import org.terrier.structures.Index;

import util.CUtil;

public class CRoundRobinByDocuments implements IPartitionByDocuments {

	public Collection<String> createCorpus(String folderPath, String destinationFolderPath, int cantidadCorpus, Index index) {
		Collection<String> colCorpusTotal = new ArrayList<String>();
		FileWriter fichero = null;
        PrintWriter pw = null;
    	int corpusId=0;
    	int contador = 0;
        try
        {
        	System.out.println("Metodo de particion: " + CRoundRobinByDocuments.class.getName());
        	/* Se obtienen todos los ficheros del folder "folderPath" */
        	java.util.Collection<String> filesPath = CUtil.getFilesFromFolder(new ArrayList<String>(),folderPath, Boolean.TRUE);
        	/* Se obtiene cantidad de ficheros */
        	int cantidadTotalArchivos = filesPath.size();
        	/* Se obtiene la cantidad de ficheros que tendrá cada corpus */
        	int cantidadArchivosPorCorpus = cantidadTotalArchivos / cantidadCorpus;
        	/* Se obtiene resto para los casos que no dé exacta la división */
        	int resto = cantidadTotalArchivos % cantidadCorpus;
        	/* Si la división no da exacta se agregará 1 documento extra en cada corpus por la cantidad de resto que haya */
        	int extra = 0;
        	if (resto>0)
        		extra = 1;
        	System.out.println("Cantidad de corpus a crear: " + cantidadCorpus);
        	System.out.println("Cantidad de documentos: " + cantidadTotalArchivos);
        	System.out.println("Cantidad de documentos por corpus: " + cantidadArchivosPorCorpus);
        	/* Se crea el primer corpus */
        	String corpusPath = destinationFolderPath + "corpus"+ corpusId +".txt";
            fichero = new FileWriter(corpusPath);
        	/* Se agrega path del corpus creado a la coleccion de corpus */            
            colCorpusTotal.add(corpusPath);
            pw = new PrintWriter(fichero);      
            Long docno = Long.valueOf(0);
            /* Se recorren los archivos del folder */
        	for (String filePath : filesPath){
        		/* Divide la cantidad de documentos lo mas equitativamente posible entre los corpus */
        		if (++contador > cantidadArchivosPorCorpus + extra){
        			if (--resto<1)
        				extra=0;
        			contador = 1;
        			/* Cierro el corpus anterior */
        			fichero.close();
        			/* Creo nuevo corpus */
        			corpusPath = destinationFolderPath + "corpus"+ ++corpusId +".txt";
        			/* Se agrega path del corpus creado a la coleccion de corpus */
        			colCorpusTotal.add(corpusPath);
                    fichero = new FileWriter(corpusPath);
                    pw = new PrintWriter(fichero);             			
        		}
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
