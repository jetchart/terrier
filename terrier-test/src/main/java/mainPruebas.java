import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.apache.log4j.Logger;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import util.CUtil;


public class mainPruebas {

	static final Logger logger = Logger.getLogger(mainPruebas.class);
	
	public static void main(String[] args) throws IOException, ParserConfigurationException, SAXException {
		
		
		StringBuffer cadena = new StringBuffer();
		cadena.append("  442769 a 432345  ");
		
		if (cadena != null){
			System.out.println(CUtil.hasTerms(cadena));
			return;
		}
		
		
		/* ACLARACION */
		/* Si al corpus source le faltan las etiquetas de <ROOT></ROOT> utilizar el siguiente script:
		  		sed -i '1i <ROOT>' /home/javier/TERRIER_JAVA/master/corpus/corpusSource.trec;
				sed -i '$a </ROOT>' /home/javier/TERRIER_JAVA/master/corpus/corpusSource.trec;
		*/
		String sourcePath = args[0]; /* --> Desde qué archivo se extraerán los datos */
		String targetPath = args[1]; /* --> Hacia qué archivo se guardarán los datos */
		Long cantidadMaximaDocumentosProcesados = Long.valueOf(args[2]); /* --> Indica la cantidad máxima de documentos procesados antes de impacar en archivo físico */
//		String sourcePath = "/home/javier/TERRIER_JAVA/master/corpus/corpus0_run10_partitioning.CRoundRobinByTerms_1_false_PATH_true_2016-09-14_09:58:50.936.trec";
//		String targetPath = "/home/javier/TERRIER_JAVA/master/corpus/resultado.trec";
		DocumentBuilderFactory docBuilderFactory = DocumentBuilderFactory.newInstance(); 
		DocumentBuilder docBuilder = docBuilderFactory.newDocumentBuilder(); 
		Document documento = (Document) docBuilder.parse (new File(sourcePath));
		documento.getDocumentElement().normalize ();
		logger.info("El elemento raíz es " + documento.getDocumentElement().getNodeName());
		NodeList listaDOC = documento.getElementsByTagName("DOC");
		logger.info("Cantidad de documentos a procesar: " + listaDOC.getLength());
		Collection<String> docNoProcesados = new ArrayList<String>();
		Map<String,StringBuffer> docContenidos = new HashMap<String,StringBuffer>();
		Long cantidadDocumentosProcesados = 0L;
		for (Integer i=0;i<listaDOC.getLength();i++){
			Node docPrincipal = listaDOC.item(i);
			Element elementoPrincipal = (Element) docPrincipal;
			String docNoPrincipal = elementoPrincipal.getElementsByTagName("DOCNO").item(0).getTextContent();
			if (!docNoProcesados.contains(docNoPrincipal)){
				cantidadDocumentosProcesados++;
				StringBuffer contenido = new StringBuffer();
				for (Integer e=0;e<listaDOC.getLength();e++){
					Node doc = listaDOC.item(e);
					Element elemento = (Element) doc;
					String docNo = elemento.getElementsByTagName("DOCNO").item(0).getTextContent();
					String texto = doc.getTextContent().replace("DOCNO:", "").replace(docNo, "").replace("\n", "");
					if (docNo.equals(docNoPrincipal)){
						contenido.append(" ").append(texto);
					}
				}
				logger.info("Documento procesado: " + docNoPrincipal);
				logger.info("Cantidad documentos procesados: " + docNoProcesados.size());
//				System.out.println("contenido: " + contenido);
				docNoProcesados.add(docNoPrincipal);
				docContenidos.put(docNoPrincipal,contenido);
				if (cantidadDocumentosProcesados > cantidadMaximaDocumentosProcesados){
					write(targetPath,docContenidos);
					docContenidos = new HashMap<String,StringBuffer>();
					cantidadDocumentosProcesados = 0L;
				}
			}
		}
		if (cantidadDocumentosProcesados > 0){
			write(targetPath,docContenidos);
		}
	}

	private static void write(String fullPath, Map<String,StringBuffer> docContenidos) throws IOException {
		FileWriter fichero = new FileWriter(fullPath,Boolean.TRUE);
		PrintWriter pw = new PrintWriter(fichero);
		for (String docNo : docContenidos.keySet()){
			pw.print("<DOC>\n");
			pw.print("<DOCNO>"+docNo+"</DOCNO>\n");
			pw.print(docContenidos.get(docNo)+"\n");
			pw.print("</DOC>\n");
		}
		pw.close();
		fichero.close();
	}

	
}
