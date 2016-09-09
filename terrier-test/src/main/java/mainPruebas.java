import java.io.File;
import java.io.IOException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;


public class mainPruebas {

	public static void main(String[] args) throws IOException, ParserConfigurationException, SAXException {
		
		DocumentBuilderFactory docBuilderFactory = DocumentBuilderFactory.newInstance(); 
		DocumentBuilder docBuilder = docBuilderFactory.newDocumentBuilder(); 
		Document documento = (Document) docBuilder.parse (new File("/home/javier/TERRIER_JAVA/colecciones/coleccionTerrier/1.xml"));
		documento.getDocumentElement().normalize ();
		System.out.println ("El elemento raíz es " + documento.getDocumentElement().getNodeName());
		NodeList listaDOC = documento.getElementsByTagName("DOC");
		System.out.println("Cantidad: " + listaDOC.getLength());
		for (Integer i=0;i<listaDOC.getLength();i++){
			Node doc = listaDOC.item(i);
//			Element elemento = (Element) doc;
//			System.out.println("DOCNO: " + elemento.get("DOCNO"));
//			System.out.println("DOCNO: " + elemento.getAttribute("DOCNO"));
		}
	}

}
