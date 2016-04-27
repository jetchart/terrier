package connections;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;

import util.CUtil;

public class Client{

	DataOutputStream flujoSalida;
	DataInputStream flujoEntrada;
	Socket miCliente;
	
	private String host;
	private Integer port;
	private String tarea;
	private String nodoColCorpus;
	
	
	public Client(String host, Integer port){
		 try {
			 this.host = host;
			 this.port = port;
			 miCliente = new Socket( host,port );
			 flujoSalida= new DataOutputStream(miCliente.getOutputStream());
			 flujoEntrada= new DataInputStream( miCliente.getInputStream());
		 } catch( IOException e ) {
			 System.out.println( e );
		 }
	}
	
	private void test(){
		 try {
		 /* Enviar */
		 flujoSalida= new DataOutputStream(miCliente.getOutputStream());
		 enviar("mensaje");
		 /* Recibir*/
		 flujoEntrada= new DataInputStream( miCliente.getInputStream());
		 recibir();
		 /* Cierro */
		 cerrar();
		 System.out.println("El cliente terminó su ejecución correctamente");
		 } catch( IOException e ) {
			 System.out.println( e );
		 }
	}
	public String recibir(){
		String msg = null;
		 try {
			 msg = flujoEntrada.readUTF();
			 System.out.println("El cliente recibió: " + msg);
		 } catch( IOException e ) {
		 System.out.println( e );
		 }
		 return msg;
	}
	
	public void enviar(String mensaje){
		 try {
		 flujoSalida.writeUTF(mensaje);
		 System.out.println("El Cliente envió al Nodo " + this.port + " el mensaje: " + mensaje);
		 } catch( IOException e ) {
		 System.out.println( e );
		 }
	}
	
	private void cerrar(){
		 /* Cierro */
		 try {
			flujoSalida.close();
			flujoEntrada.close();
			miCliente.close();
			System.out.println("El cliente terminó su ejecución correctamente");
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

//	@Override
//	public void run() {
//		if ("Inicializar".equals(getTarea())){
//			this.enviar("setId" + CUtil.separator + getPort());
//			this.enviar("setColCorpus" + CUtil.separator + getNodoColCorpus());
//			this.recibir();
//			this.cerrar();
//		}else if ("Indexar".equals(getTarea())){
//			this.enviar("createIndex");
//			this.recibir();
//			this.cerrar();
//		}
//	}

	public void run() {
		if ("Inicializar".equals(getTarea())){
			this.enviar("setId" + CUtil.separator + getPort());
			this.enviar("setColCorpus" + CUtil.separator + getNodoColCorpus());
			this.recibir();
			this.cerrar();
		}else if ("Indexar".equals(getTarea())){
			this.enviar("createIndex");
			this.recibir();
			this.cerrar();
		}
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
}
