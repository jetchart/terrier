package connections;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.net.Socket;

import org.apache.log4j.Logger;
import org.terrier.matching.ResultSet;

public class CClient{

	static final Logger logger = Logger.getLogger(CClient.class);
	
	DataOutputStream flujoSalida;
	DataInputStream flujoEntrada;
	ObjectInputStream objetoEntrada;
	Socket miCliente;
	
	private String host;
	private Integer port;
	private String tarea;
	private String nodoColCorpus;
	private String query;
	private ResultSet resultSetNodo;
	
	public CClient(String host, Integer port){
		 try {
			 this.host = host;
			 this.port = port;
			 miCliente = new Socket( host,port );
			 flujoSalida= new DataOutputStream(miCliente.getOutputStream());
			 flujoEntrada= new DataInputStream(miCliente.getInputStream());
		 } catch( IOException e ) {
			 logger.info( e );
		 }
	}
	
	public String recibir(){
		String msg = null;
		 try {
			 msg = flujoEntrada.readUTF();
			 logger.info("El cliente recibió: " + msg);
		 } catch( IOException e ) {
		 logger.info( e );
		 }
		 return msg;
	}
	
	public ResultSet recibirObjeto() throws ClassNotFoundException{
		ResultSet resultSet = null;
		 try {
			 objetoEntrada= new ObjectInputStream(miCliente.getInputStream());
			 resultSet = (ResultSet) objetoEntrada.readObject();
			 logger.info("El cliente recibió: objeto ResultSet");
		 } catch( IOException e ) {
		 logger.info( e );
		 }
		 return resultSet;
	}
	
	public void enviar(String mensaje){
		 try {
		 flujoSalida.writeUTF(mensaje);
		 logger.info("El Cliente envió al Nodo " + this.port + " el mensaje: " + mensaje);
		 } catch( IOException e ) {
		 logger.info( e );
		 }
	}
	
	private void cerrar(){
		 /* Cierro */
		 try {
			flujoSalida.close();
			flujoEntrada.close();
			miCliente.close();
			logger.info("El cliente terminó su ejecución correctamente");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public Socket getSocket(){
		return miCliente;
	}
	
	public void aetSocket(Socket miCliente){
		this.miCliente = miCliente;
	}

	public String getHost() {
		return host;
	}

	public void setHost(String host) {
		this.host = host;
	}

	public Integer getPort() {
		return port;
	}

	public void setPort(Integer port) {
		this.port = port;
	}
	
	public String getTarea() {
		return tarea;
	}

	public void setTarea(String tarea) {
		this.tarea = tarea;
	}

	public String getNodoColCorpus() {
		return nodoColCorpus;
	}

	public void setNodoColCorpus(String nodoColCorpus) {
		this.nodoColCorpus = nodoColCorpus;
	}

	public String getQuery() {
		return query;
	}

	public void setQuery(String query) {
		this.query = query;
	}

	public ResultSet getResultSetNodo() {
		return resultSetNodo;
	}

	public void setResultSetNodo(ResultSet resultSetNodo) {
		this.resultSetNodo = resultSetNodo;
	}
}
