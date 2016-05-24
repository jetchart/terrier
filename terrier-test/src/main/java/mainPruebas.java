import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.terrier.indexing.tokenisation.Tokeniser;


public class mainPruebas {

	public static void main(String[] args) throws IOException {
		
		Reader reader = new BufferedReader(new FileReader("/home/jetchart/tokens.txt"));
		Tokeniser tokeniser = Tokeniser.getTokeniser();
		String[] tokens = tokeniser.getTokens(reader);
		Collection<String> tokensUnicos = new ArrayList<String>();
		for (String token : tokens){
			if (!tokensUnicos.contains(token)){
				tokensUnicos.add(token);
			}
		}
		System.out.println(tokensUnicos.size());
		
		
	}

}
