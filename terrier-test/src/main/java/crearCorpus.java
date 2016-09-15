import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;


public class crearCorpus {

static final Logger logger = Logger.getLogger(crearCorpus.class);
	
	public static void main(String[] args) throws IOException{
		
		// 0 --> procesar/maximo
		// 1 --> sourcePath
		// 2 --> targetPath
		// 4 --> desde
		// 5 --> hasta
		String accion = args[0];
		String sourcePath = args[1];
		String targetPath = args[2];
		Long desde = Long.valueOf(args[3]);
		Long hasta = Long.valueOf(args[4]);
		
		if (accion.equals("maximo")){
			logger.info(new Timestamp(System.currentTimeMillis()) + " Buscando máximo DOCNO");
			Long maxDOCNO = getMaximoDOCNO(sourcePath);
			logger.info(new Timestamp(System.currentTimeMillis()) + " Maximo DOCNO: " + maxDOCNO);
			return;
		}

		Map<Long,StringBuffer> docContenidos = new HashMap<Long, StringBuffer>();
		for (Long i= desde;i<=hasta;i++){
			StringBuffer contenido = getContenidoByDOCNO(sourcePath, i);
			logger.info(new Timestamp(System.currentTimeMillis()) + " DOCNO " + i + " procesado");
			docContenidos.put(i, contenido);
			if (docContenidos.keySet().size() > 100){
				write(targetPath, docContenidos);
				docContenidos = new HashMap<Long, StringBuffer>();
			}
		}
		if (docContenidos.keySet().size() > 0){
			write(targetPath, docContenidos);
		}
		
	}
	
	private static void write(String fullPath, Map<Long,StringBuffer> docContenidos) throws IOException {
		FileWriter fichero = new FileWriter(fullPath,Boolean.TRUE);
		PrintWriter pw = new PrintWriter(fichero);
		for (Long docNo : docContenidos.keySet()){
			if (docContenidos.get(docNo).length() > 0){
				pw.print("<DOC>\n");
				pw.print("<DOCNO>"+docNo+"</DOCNO>\n");
				pw.print(docContenidos.get(docNo)+"\n");
				pw.print("</DOC>\n");
			}
		}
		pw.close();
		fichero.close();
	}
	
	public static Long getMaximoDOCNO(String filePath){
	    FileReader f;
	    BufferedReader b;
	    String cadena;
	    Long maximoDOCNO = -1L;
		try {
			/* Creo FileReader con la ruta completa del file recibido por parametro */
			f = new FileReader(filePath);
		    b = new BufferedReader(f);
		    /* Recorro el archivo */ 
		    while((cadena = b.readLine())!=null) {
		    	if (cadena.startsWith("<DOCNO>")){
		    		Long docNo = Long.valueOf(cadena.replace("<DOCNO>", "").replace("</DOCNO>", ""));
		    		if (docNo > maximoDOCNO){
		    			maximoDOCNO = docNo;
		    		}
		    	}
		    }
		    b.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
		/* Se devuelve el maximo DOCNO */
		return maximoDOCNO;
	}
	
	public static StringBuffer getContenidoByDOCNO(String filePath, Long docnoBuscado){
	    FileReader f;
	    BufferedReader b;
	    String cadena;
	    StringBuffer contenido = new StringBuffer();
		try {
			/* Creo FileReader con la ruta completa del file recibido por parametro */
			f = new FileReader(filePath);
		    b = new BufferedReader(f);
		    /* Recorro el archivo */ 
		    while((cadena = b.readLine())!=null) {
		    	if (cadena.startsWith("<DOCNO>")){
		    		Long docNo = Long.valueOf(cadena.replace("<DOCNO>", "").replace("</DOCNO>", ""));
		    		if (docNo == docnoBuscado){
		    			cadena = b.readLine();
		    			while (!cadena.equals("</DOC>")){
		    				contenido.append(cadena).append(" ");
		    				cadena = b.readLine();
		    				
		    			}
		    		}
		    	}
		    }
		    b.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return contenido;
	}
}
