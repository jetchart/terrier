import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.text.Normalizer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.terrier.indexing.Collection;
import org.terrier.indexing.TRECCollection;
import org.terrier.matching.ResultSet;
import org.terrier.querying.Manager;
import org.terrier.querying.SearchRequest;
import org.terrier.structures.Index;
import org.terrier.structures.MetaIndex;
import org.terrier.structures.indexing.Indexer;
import org.terrier.structures.indexing.classical.BasicIndexer;

import configuration.INodeConfiguration;


public class Main {

	static Map<Long, String> mapFilesPath = new HashMap<Long, String>();
	/* Terrier Home */
	static String terrierHome = "/home/jetchart/terrier-4.0";
	/* Carpeta donde se extraerán los documentos a indexar */
	static String folderPath = terrierHome + "/wiki-large/";
	/* Carpeta donde se almacenarán los Corpus a indexar */
	static String destinationFolderPath = terrierHome + "/miColeccionCorpus/";
	
	public static void main(String[] args) {
		
		try {
			/* Tirar por linea de comandos para llenar el collection.spec con los documentos */
			/* find /home/jetchart/terrier-4.0/miColeccion/ -type f | grep -v "PATTERN" > /home/jetchart/terrier-4.0/etc/collection.spec */
			/* Defino propiedades de Terrier */
			System.setProperty("terrier.home",terrierHome);
			System.setProperty("terrier.etc",terrierHome + "/etc/");
			System.setProperty("terrier.setup",terrierHome + "/etc/terrier.properties");
			/* Pido cantidad de corpus y query */
			Scanner scanner = new Scanner(System.in);
			System.out.print("Ingrese la cantidad de corpus a crear: ");
			int cantidadCorpus= Integer.valueOf(scanner.nextLine());
			/* Se pide query y se lo parsea */
			System.out.print("Ingrese el query: ");
			String query = parseString(scanner.nextLine());
			scanner.close();
			/* Creo corpus */
			Long inicioCreacionCorpus = System.currentTimeMillis();
			crearCorpus(folderPath, destinationFolderPath, cantidadCorpus);
			Long finCreacionCorpus = System.currentTimeMillis() - inicioCreacionCorpus;
			System.out.println("Creación Corpus tardó " + finCreacionCorpus + " milisegundos");
			/* Indexo */
			Long inicioIndexacion = System.currentTimeMillis();
			Index index = indexar();
			Long finIndexacion = System.currentTimeMillis() - inicioIndexacion;
			System.out.println("Indexación tardó " + finIndexacion + " milisegundos");	
			/* Recupero */
			Long inicioRecuperacion = System.currentTimeMillis();
			ResultSet rs = recuperar(index, query);
			Long finRecuperacion = System.currentTimeMillis() - inicioRecuperacion;
			System.out.println("Recuperación tardó " + finRecuperacion + " milisegundos");	 
			/* Muestro Resultados */
			mostrarResultados(rs, index, query);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/* En base a un archivo, devuelve su contenido sin tags */
	private static String leerArchivo(String filePath){
		StringBuffer retorno = new StringBuffer();
	    FileReader f;
	    BufferedReader b;
	    String cadena;
		try {
			/* Creo FileReader con la ruta completa del file recibido por parametro */
			f = new FileReader(filePath);
		    b = new BufferedReader(f);
		    /* Recorro el archivo */ 
		    while((cadena = b.readLine())!=null) {
		    	/* Agrego contenido al StringBuffer que será devuelto */
		    	retorno.append(cadena).append(" ");
		    }
		    b.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
		/* Se parsea documento con librería Jsoup */
		return parseString(retorno.toString());
	}
	
	/* Se recorre carpeta "folderPath", se leen los archivos y se crea corpus con formato TREC en "destinationFolderPath" */
    private static void crearCorpus(String folderPath, String destinationFolderPath, int cantidadCorpus)
    {
    	FileWriter fichero = null;
        PrintWriter pw = null;
    	int corpusId=0;
    	int contador = 0;
        try
        {
        	/* Se obtienen todos los ficheros del folder "folderPath" */
        	java.util.Collection<String> filesPath = getFilesFromFolder(new ArrayList<String>(),folderPath, Boolean.TRUE);
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
        	/* Se crea colección que tendra los path's de los corpus creados */
        	java.util.Collection<String> colCorpusPath = new ArrayList<String>();
        	/* Se crea el primer corpus */
        	String corpusPath = destinationFolderPath + "corpus"+ corpusId +".txt";
            fichero = new FileWriter(corpusPath);
            colCorpusPath.add(corpusPath);
            pw = new PrintWriter(fichero);      
            Long docno = Long.valueOf(0);
            /* Se recorren los archivos del folder */
        	for (String filePath : filesPath){
        		/* Guardo en mapa la relacion: DOCNO, filePath */
        		mapFilesPath.put(++docno, filePath);
        		/* Divide la cantidad de documentos lo mas equitativamente posible entre los corpus */
        		if (++contador > cantidadArchivosPorCorpus + extra){
        			if (--resto<1)
        				extra=0;
        			contador = 1;
        			/* Cierro el corpus anterior */
        			fichero.close();
        			/* Creo nuevo corpus */
        			corpusPath = destinationFolderPath + "corpus"+ ++corpusId +".txt";
        			colCorpusPath.add(corpusPath);
                    fichero = new FileWriter(corpusPath);
                    pw = new PrintWriter(fichero);             			
        		}
        		/* Escribo contenido del archivo en el corpus con formato TREC */
                pw.println("<DOC>");
                pw.println("<DOCNO>"+ docno +"</DOCNO>");
                /* TODO ¡REVISAR SI LA CANTIDAD MAXIMA DE CARACTERES PARA EL DOCPATH ALCANZA BIEN! */
                pw.println("<DOCPATH>" + filePath + "</DOCPATH>");
                pw.println("<TEXT>");
                /* Obtengo contenido del archivo sin tags */
                pw.println(leerArchivo(filePath));
                pw.println("</TEXT>");
                pw.println("</DOC>");
                pw.println("");  
        	}
        	/* Agrego todos los corpus al archivo /etc/collection.spec/ para que se tengan en cuenta en la Indexacion */
        	agregarCorpus(colCorpusPath);
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
    }
    
    private static void agregarCorpus(java.util.Collection<String> colCorpusPath){
    	FileWriter fichero = null;
        PrintWriter pw = null;
        try{
        	/* Defino donde se encuentra el archivo collection.spec utilizando la property inicializada en un principio */
        	String collectionSpecPath = System.getProperty("terrier.etc") + "collection.spec";
        	/* Creo archivo */
	        fichero = new FileWriter(collectionSpecPath);
	        pw = new PrintWriter(fichero);
	        /* Recorro los path's de los corpus creados y los agrego al collection.spec */
	        for (String corpusPath : colCorpusPath){
	        	pw.println(corpusPath);
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
    }
    
    /* Obtiene todos los archivos a partir de un directorio raiz (si recursive=True navega tambien sobre subdirectorios) */
    private static java.util.Collection<String> getFilesFromFolder(java.util.Collection<String> colFilesPath, String rootFolder, Boolean recursive){
    	File f = new File(rootFolder);
    	File[] ficheros = f.listFiles();
    	for (File file : ficheros){
    		if (recursive && file.isDirectory()){
    			getFilesFromFolder(colFilesPath, file.getAbsolutePath(), recursive);
    		}else{
    			colFilesPath.add(file.getAbsolutePath());
    		}
    	}
    	return colFilesPath;
    }
    
    private static String parseString(String cadena){
    	Document doc = Jsoup.parse(cadena);
    	cadena = doc.text().replaceAll("\\p{Cntrl}", "");
    	cadena = doc.text().replaceAll("/[^A-Za-z0-9 ]/", "");
    	// TODO REVISAR ESTO
    	//Normalizamos en la forma NFD (Canonical decomposition)
    	cadena = Normalizer.normalize(cadena, Normalizer.Form.NFD);
    	//Reemplazamos los acentos con una una expresión regular de Bloque Unicode
    	return cadena.replaceAll("\\p{InCombiningDiacriticalMarks}+", "");
    }
    
    private static Index indexar(){
    	TRECCollection coleccion = null;
    	/* Creo una nueva coleccion */
		coleccion = new TRECCollection();
		/* Instancio Indexador */
		Indexer indexador = new BasicIndexer(terrierHome +"/var/index/", INodeConfiguration.indexName);
		/* Indico las colecciones a indexar */
		Collection[] col = new Collection[1];
		col[0] = coleccion;
		/* Indexo las colecciones */
		indexador.createDirectIndex(col);
		/* Creo el Indice Invertido */
		indexador.createInvertedIndex();
		/* Cierro Coleccion */
		coleccion.close();
    	/* Devuelvo el indice creado */
		return Index.createIndex(terrierHome +"/var/index/", INodeConfiguration.indexName);		
    }
    
    private static ResultSet recuperar(Index index, String query){
		/* Instancio Manager con el indice creado */
		 Manager m = new Manager(index);
		 /* Creo el SearchRequest con la query */
		 SearchRequest srq = m.newSearchRequest("Q1", query);
		 /* Indico modelo de Matching */
		 srq.addMatchingModel("Matching", "TF_IDF");
		 /* Corro query */
		 m.runPreProcessing(srq);
		 m.runMatching(srq);
		 m.runPostProcessing(srq);
		 m.runPostFilters(srq);
		 /* Devuelvo ResultSet */
		 return srq.getResultSet();
    }
    
    public static void mostrarResultados(ResultSet rs, Index index, String query) throws Exception{
		 int[] docIds = rs.getDocids();
		 double[] scores = rs.getScores();
		 int posicion = 0;
		 /* Imprimo resultados */
		 System.out.println(docIds.length +" documentos para la query: " + query);
		 for (int id : docIds){
			 /* Obtengo el MetaIndex para acceder a los metadatos */
			 MetaIndex meta = index.getMetaIndex();
			 /* A partir del id obtengo el DOCNO */
			 String docPath = meta.getItem("DOCPATH", id);
			 System.out.println("\t "+ ++posicion + "- Documento: " + docPath + " con score: " + scores[posicion-1]);
		 }
    }
}
