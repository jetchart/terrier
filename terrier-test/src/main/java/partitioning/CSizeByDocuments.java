package partitioning;

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.terrier.structures.Index;

import configuration.CParameters;
import util.CUtil;

public class CSizeByDocuments implements IPartitionByDocuments {

	static final Logger logger = Logger.getLogger(CSizeByDocuments.class);
	static Long cantidadDocumentosAntesCierre = 1000L;
	
	public Collection<String> createCorpus(String folderPath, String destinationFolderPath, Integer cantidadCorpus, Index index, CParameters parameters) {
		List<String> colCorpusTotal = new ArrayList<String>();
        Map<String, StringBuffer> mapaCorpusContenido = new HashMap<String, StringBuffer>();
        try
        {
        	logger.info("Metodo de particion: " + CSizeByDocuments.class.getName());
        	/* Se obtienen todos los ficheros del folder "folderPath" */
        	java.util.Collection<String> filesPath = CUtil.getFilesFromFolder(new ArrayList<String>(),folderPath, Boolean.TRUE);
        	/* Se obtiene cantidad de ficheros */
        	int cantidadTotalArchivos = filesPath.size();
        	logger.info("Cantidad de corpus a crear: " + cantidadCorpus);
        	logger.info("Cantidad de documentos: " + cantidadTotalArchivos);
        	/* Se crean los corpus vacios, y se agregan a la coleccion de corpus total */
        	colCorpusTotal.addAll(CUtil.crearCorpusVacios(destinationFolderPath, CSizeByDocuments.class.getName(), cantidadCorpus, parameters));
        	/* Inicializo el mapa con la ruta de los corpus vacios */
        	for (String pathCorpus : colCorpusTotal){
        		mapaCorpusContenido.put(pathCorpus, new StringBuffer());
        	}
            Long docno = Long.valueOf(0);
            Long i = 0L;
            /* Se recorren los archivos del folder */
        	for (String filePath : filesPath){
        		/* Abro el corpus correspondiente */
        		String corpusPath = colCorpusTotal.get(getIdSmallestDocument(destinationFolderPath, cantidadCorpus, parameters));
        		StringBuffer contenido = mapaCorpusContenido.get(corpusPath);  
        		/* Escribo contenido del archivo en el corpus con formato TREC */
                contenido.append("<DOC>").append("\n");
                contenido.append("<DOCNO>"+ docno++ +"</DOCNO>").append("\n");
                /* TODO ¡REVISAR SI LA CANTIDAD MAXIMA DE CARACTERES PARA EL DOCPATH ALCANZA BIEN! */
                contenido.append("<DOCPATH>" + filePath + "</DOCPATH>").append("\n");
                contenido.append("<TEXT>").append("\n");
                /* Obtengo contenido del archivo sin tags */
                contenido.append(CUtil.leerArchivo(filePath)).append("\n");
                contenido.append("</TEXT>").append("\n");
                contenido.append("</DOC>").append("\n");
                mapaCorpusContenido.put(corpusPath, contenido);
                /* Si ya se procesaron mas de la cantidad de archivos permitidas, se impactan */
                if (i++ > cantidadDocumentosAntesCierre){
                	i = 0L;
                	/* Guardo el contenido de todos los corpus en los archivos sobreescribiendo si ya existe */
                	CUtil.crearCorpusConDocumentos(mapaCorpusContenido, Boolean.TRUE);
                	/* Inicializo el mapa con la ruta de los corpus vacios */
                	for (String pathCorpus : colCorpusTotal){
                		mapaCorpusContenido.put(pathCorpus, new StringBuffer());
                	}
                }
        	}
        	if (i > 0){
	        	/* Guardo el contenido de todos los corpus en los archivos sobreescribiendo si ya existe */
	        	CUtil.crearCorpusConDocumentos(mapaCorpusContenido, Boolean.TRUE);
        	}
        } catch (Exception e) {
            e.printStackTrace();
        }
		return colCorpusTotal;
	}

	/* Devuelvo el ID del corpus de menor tamaño */
	private Integer getIdSmallestDocument(String destinationFolderPath, Integer cantidadCorpus, CParameters parameters){
        Integer i;
        long size = -1;
        Integer idSmallestDocument = 0;
    	for (i=0;i<cantidadCorpus;i++){
    		String corpusPath = CUtil.generarPathArchivoCorpus(parameters, destinationFolderPath, i.toString());
    			File file = new File (corpusPath);
    			if (file.length() < size || i == 0){
    				size = file.length();
    				idSmallestDocument = i;
    			}
    	}
		return idSmallestDocument;
	}
}
