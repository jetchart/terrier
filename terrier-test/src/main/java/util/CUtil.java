package util;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.Reader;
import java.text.Normalizer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Pattern;

import org.apache.log4j.Logger;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.terrier.indexing.tokenisation.Tokeniser;
import org.terrier.matching.ResultSet;
import org.terrier.structures.Index;
import org.terrier.structures.MetaIndex;

import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.Session;
import com.jcraft.jsch.UserInfo;


public class CUtil {
	static final Logger logger = Logger.getLogger(CUtil.class);
	public static String separator = Pattern.quote("*");
	private static String[] indexFiles = {
			".direct.bf",
			".document.fsarrayfile",
			".inverted.bf",
			".lexicon.fsomapfile",
			".lexicon.fsomaphash",
			".lexicon.fsomapid",
			".meta.idx",
			".meta.zdata",
			".properties"
		};
	
	/**
	 * Devuelve el contenido dentro del Reader sin tags
	 * @param reader		Reader sobre el que se requiere obtener el contenido sin tags
	 * @return
	 */
	public static String extractContentInReader(Reader reader){
		StringBuffer retorno = new StringBuffer();
	    BufferedReader b = (BufferedReader) reader;
	    String cadena;
		try {
		    /* Recorro el archivo */ 
		    while((cadena = b.readLine())!=null) {
		    	/* Agrego contenido al StringBuffer que será devuelto */
		    	retorno.append(cadena).append("\n");
		    }
		    b.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
		/* Se parsea documento con librería Jsoup */
		return parseString(retorno.toString());
	}
	
	/**
	 * Devuelve el contenido del archivo filePath sin tags
	 * @param filePath		Ruta del archivo sobre el que se requiere obtener el contenido sin tags
	 * @return
	 */
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
	
	/**
	 * Devuelve el contenido del archivo en BufferedReader
	 * @param filePath		Ruta del archivo sobre el que se requiere obtener el contenido
	 * @return
	 * @throws FileNotFoundException 
	 */
	public static Reader getReaderArchivo(String filePath) throws FileNotFoundException{
		return (Reader) new BufferedReader(new FileReader(filePath));
	}
	
	/**
	 * Devuelve el contenido del archivo en FileReader
	 * @param filePath		Ruta del archivo sobre el que se requiere obtener el contenido
	 * @return
	 * @throws FileNotFoundException 
	 */
	public static FileReader getFileReaderArchivo(String filePath) throws FileNotFoundException{
		return new FileReader(filePath);
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
    
	/**
	 * Obtiene todos los archivos a partir de un directorio raiz (si recursive=True navega tambien sobre subdirectorios)
	 * @param colFilesPath		Almacena los archivos encontrados (se requiere cuando recursive=TRUE, ya que irá acumulando los archivos)
	 * @param rootFolder		Ruta desde donde se inicia la búsqueda de archivos
	 * @param recursive 		Si es TRUE, busca tambien dentro de los subdirectorios
	 * @return
	 */
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
		logger.info("------------------------------------");
		logger.info("RESULTADOS");
		logger.info("------------------------------------");
		 int[] docIds = rs.getDocids();
		 double[] scores = rs.getScores();
		 int posicion = 0;
		 /* Imprimo resultados */
		 logger.info(docIds.length +" documentos para la query: " + query);
		 for (int id : docIds){
			 if (index != null){
				 /* Obtengo el MetaIndex para acceder a los metadatos */
				 MetaIndex meta = index.getMetaIndex();
				 /* A partir del id obtengo el DOCNO */
				 String docPath = meta.getItem("DOCPATH", id);
				 logger.info("\t "+ ++posicion + "- Documento: " + docPath + " con score: " + scores[posicion-1]);				 
			 }else{
				 logger.info("\t "+ ++posicion + "- DOCNO: " + id + " con score: " + scores[posicion-1]);
			 }

		 }
    }
    
    public static void deleteIndexFiles(String etcFolder, String prefixName){
		logger.info("------------------------------------");
		logger.info("INICIO LIMPIEZA DE ARCHIVOS DE INDICES ANTERIORES ");
		logger.info("------------------------------------");
    	for (String indexFile : indexFiles){
            File fichero = new File(etcFolder + prefixName + indexFile);
            if (fichero.delete())
            	logger.info("Se eliminó el archivo: " + etcFolder + prefixName + indexFile);
    	}
		logger.info("------------------------------------");
		logger.info("FIN LIMPIEZA DE ARCHIVOS DE INDICES ANTERIORES ");
		logger.info("------------------------------------");
    }
    
    public static Boolean existeIndice(String indexPath){
    	File fichero = new File(indexPath);
    	return fichero.exists();
    }
    
    public static void copyFileSFTP(String source, String target, String user, String pass, String host, Integer port){
		logger.info("------------------------------------");
		logger.info("INICIO COPIA DE CORPUS DESDE MASTER A SLAVE");
		logger.info("------------------------------------");
		Long inicioCopiaCorpus = System.currentTimeMillis();
        try {
	        JSch jsch = new JSch();
	        Session session = jsch.getSession(user, host, port);
	        UserInfo ui = new SUserInfo(pass, null);
	 
	        session.setUserInfo(ui);
	        session.setPassword(pass);
	        
			session.connect();
	
	        ChannelSftp sftp = (ChannelSftp)session.openChannel("sftp");
	        sftp.connect();

	        sftp.get(source, target);
	        logger.info("Archivo copiado");
	 
	        sftp.exit();
	        sftp.disconnect();
	        session.disconnect();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		Long finCopiaCorpus = System.currentTimeMillis() - inicioCopiaCorpus;
		logger.info("Copia del corpus tardó " + finCopiaCorpus + " milisegundos");
		logger.info("------------------------------------");
		logger.info("FIN COPIA DE CORPUS DESDE MASTER A SLAVE");
		logger.info("------------------------------------");
    }
    
    /**
	 * Crea Corpus vacios que luego se llenaran con los documentos.
	 * 
	 * @param destinationFolderPath		Indica la carpeta en donde se crearán
	 * @param cantidadCorpus			Indica la cantidad de archivos a crear
	 * @return							{@link Collection} de {@link String} con todos los path's de los archivos creados
	 * @throws IOException
	 */
	public static Collection<String> crearCorpusVacios(String path, String metodoParticionamiento, Integer cantidadNodos) throws IOException {
		Collection<String> col = new ArrayList<String>();
		Integer i;
		for (i=0;i<cantidadNodos;i++){
			String corpusPath = CUtil.generarPathArchivoCorpus(path, i.toString(), metodoParticionamiento, cantidadNodos.toString());
			FileWriter fichero = new FileWriter(corpusPath, Boolean.FALSE);
			PrintWriter pw = new PrintWriter(fichero);
			pw.close();
			fichero.close();
			col.add(corpusPath);
		}
		return col;
	}
	
	/**
	 * Crea Corpus con sus contenidos extrayendo la informacion del mapa recibido.
	 * 
	 * @param mapa		Donde la key es el path del corpus, y el value es el contenido del corpus
	 * @param append	Indica si debe escribirse sobre el final del archivo o sobreescribirse
	 * @throws IOException
	 */
	public static void crearCorpusConDocumentos(Map<String, StringBuffer> mapa, Boolean append) throws IOException {
		for (Entry<String, StringBuffer> corpus : mapa.entrySet()){
			FileWriter fichero = new FileWriter(corpus.getKey(),append);
			PrintWriter pw = new PrintWriter(fichero);
			pw.print(corpus.getValue());
			pw.close();
			fichero.close();
		}
	}
	
	/**
	 * Obtiene la cantidad de tokens unicos (sin repeticiones) existentes en el archivo que se encuentra en "filePath"
	 * @param filePath		Ruta del archivo
	 * @return				Cantidad de tokens unicos
	 * @throws IOException
	 */
	public static Integer getAmountUniqueTokensInFile(String filePath) throws IOException{
		Reader reader = new BufferedReader(new FileReader(filePath));
		Tokeniser tokeniser = Tokeniser.getTokeniser();
		String[] tokens = tokeniser.getTokens(reader);
		Collection<String> tokensUnicos = new ArrayList<String>();
		for (String token : tokens){
			if (!tokensUnicos.contains(token)){
				tokensUnicos.add(token);
			}
		}
		return tokensUnicos.size();
	}
	
	/**
	 * Obtiene la cantidad de tokens unicos (sin repeticiones) existentes en el {@link Reader} recibido
	 * @param reader		Reader con el contenido del archivo
	 * @return				Cantidad de tokens unicos
	 * @throws IOException
	 */
	public static Integer getAmountUniqueTokensInReader(Reader reader) throws IOException{
		Tokeniser tokeniser = Tokeniser.getTokeniser();
		String[] tokens = tokeniser.getTokens(reader);
		Collection<String> tokensUnicos = new ArrayList<String>();
		for (String token : tokens){
			if (!tokensUnicos.contains(token)){
				tokensUnicos.add(token);
			}
		}
		return tokensUnicos.size();
	}
	
	/**
	 * Arma el path completo del corpus a crear en base a los parámetros recibidos, con el fin 
	 * de diferenciar los corpus entre corridas y que no queden cacheados.
	 * TODO --> ¡¡¡¡¡¡¡¡¡¡¡¡¡¡¡¡¡¡ Agregar parámetros: indexaMaster, metodoComunicacion. !!!!!!!!!!!!!!!!!!!!
	 * 
	 * @param path
	 * @param id
	 * @param metodoParticionamiento
	 * @param cantidadNodos
	 * @return
	 */
	public static String generarPathArchivoCorpus(String path, String id, String metodoParticionamiento, String cantidadNodos){
		return path + "corpus" + id + "_" + metodoParticionamiento + "_" + cantidadNodos + "_" + ".txt";
	}
}
