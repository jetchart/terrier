import java.io.IOException;
import java.io.Reader;

import util.CUtil;


public class mainPruebas {

	public static void main(String[] args) throws IOException {
		
        Reader reader = CUtil.getReaderArchivo("/home/javier/pgadmin.log");
//        Integer cantidad = CUtil.getAmountUniqueTokensInReader(reader);
        reader.markSupported();
        reader.mark(1);
//        String contenido = CUtil.extractContentInReader(reader);
		
	}

}
