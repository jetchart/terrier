package util;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.text.Normalizer;
import java.util.regex.Pattern;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.terrier.matching.ResultSet;
import org.terrier.structures.Index;
import org.terrier.structures.MetaIndex;

import configuration.INodeConfiguration;


public class CUtil {
	public static String separator = Pattern.quote("*");
	private static String[] indexFiles = {INodeConfiguration.indexName + ".direct.bf",INodeConfiguration.indexName + "document.fsarrayfile",
			INodeConfiguration.indexName + "inverted.bf",INodeConfiguration.indexName + "lexicon.fsomapfile",INodeConfiguration.indexName + "lexicon.fsomapfile",
			INodeConfiguration.indexName + "lexicon.fsomaphash",INodeConfiguration.indexName + "lexicon.fsomapid",INodeConfiguration.indexName + "meta.idx",
			INodeConfiguration.indexName + "meta.zdata",INodeConfiguration.indexName + "properties"};
	
	/* En base a un archivo, devuelve su contenido sin tags */
	public static String leerArchivo(String filePath){
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
	
	public static void agregarCorpus(java.util.Collection<String> colCorpusPath){
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
	public static java.util.Collection<String> getFilesFromFolder(java.util.Collection<String> colFilesPath, String rootFolder, Boolean recursive){
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
    
	public static String parseString(String cadena){
    	Document doc = Jsoup.parse(cadena);
    	cadena = doc.text().replaceAll("\\p{Cntrl}", "");
    	cadena = doc.text().replaceAll("/[^A-Za-z0-9 ]/", "");
    	// TODO REVISAR ESTO
    	//Normalizamos en la forma NFD (Canonical decomposition)
    	cadena = Normalizer.normalize(cadena, Normalizer.Form.NFD);
    	//Reemplazamos los acentos con una una expresión regular de Bloque Unicode
    	return cadena.replaceAll("\\p{InCombiningDiacriticalMarks}+", "");
    }
	
    public static void mostrarResultados(ResultSet rs, Index index, String query) throws Exception{
		System.out.println("------------------------------------");
		System.out.println("RESULTADOS");
		System.out.println("------------------------------------");
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
    
    public static void deleteIndexFiles(String etcFolder, String prefixName){
    	for (String indexFile : indexFiles){
            File fichero = new File(etcFolder + prefixName + indexFile);
            fichero.delete();
    	}
    }
    
    public static Boolean existeIndice(String indexPath){
    	File fichero = new File(indexPath);
    	return fichero.exists();
    }
}
