package connections;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;

import node.ISlaveNode;

import org.apache.log4j.Logger;
import org.terrier.matching.ResultSet;

import util.CUtil;

public class CServer {

	static final Logger logger = Logger.getLogger(CServer.class);
	
	DataOutputStream flujoSalida;
	ObjectOutputStream objetoSalida;
	DataInputStream flujoEntrada;
	Socket socketServicio;
	ServerSocket miServicio;
	ISlaveNode slaveNode;
	
	public CServer(Integer port, ISlaveNode slaveNode){
		this.slaveNode = slaveNode;
		iniciarServidor(port);
		/* El siguiente "System.out.println" es recibido por el Master para confirmar que se levantó el esclavo por SSH correctamente */
		System.out.println("Esclavo con id " + slaveNode.getNodeConfiguration().getIdNode() + " iniciado");
	}
	
	private void iniciarServidor(Integer port) {
		 try {
		 miServicio = new ServerSocket( port );
		 } catch( IOException e ) {
			 logger.info( e );
		 }
	}

	public void setSlaveNode(ISlaveNode slaveNode){
		this.slaveNode = slaveNode;
	}
	
	public void listen(){
		 logger.info("------------------------------------");
		 logger.info("COMIENZA NUEVO CICLO");
		 logger.info("------------------------------------");
		 socketServicio = null;
		 try {
		 socketServicio = miServicio.accept();
		 String mensaje;
		 flujoEntrada= new DataInputStream( socketServicio.getInputStream());
		 while ((mensaje = recibir()) != null){
			 if (mensaje.startsWith("retrieval")){
				 logger.info(">>>>>>>>>>** "+mensaje+" **<<<<<<<<<");
				 objetoSalida= new ObjectOutputStream(socketServicio.getOutputStream());
				 enviarObjeto(slaveNode.getResultSet());
			 }else{
				 logger.info(">>>>>>>>>> "+mensaje+" <<<<<<<<<");
				flujoSalida= new DataOutputStream(socketServicio.getOutputStream());
			 	enviar("Nodo " + slaveNode.getNodeConfiguration().getIdNode() + " recibio: " + mensaje);
			 }
			 if ("salir".equals(mensaje)){
				break; 
			 }
			 flujoEntrada= new DataInputStream( socketServicio.getInputStream());
		 }
//		 /* Recibir */
//		 flujoEntrada= new DataInputStream( socketServicio.getInputStream());
//		 recibir();
//		 flujoEntrada= new DataInputStream( socketServicio.getInputStream());
//		 recibir();
//		 /* Enviar */
//		 flujoSalida= new DataOutputStream(socketServicio.getOutputStream());
//		 enviar("Nodo " + slaveNode.getNodeConfiguration().getIdNode() + " recibio datos requeridos para indexar");
//		 flujoEntrada= new DataInputStream( socketServicio.getInputStream());
//
//		 flujoEntrada= new DataInputStream( socketServicio.getInputStream());
//		 recibir();
//		 enviar("Nodo " + slaveNode.getNodeConfiguration().getIdNode() + " recibio orden para limpiar indices");
//		 recibir();
//		 /* Enviar */
//		 flujoSalida= new DataOutputStream(socketServicio.getOutputStream());
//		 enviar("Nodo " + slaveNode.getNodeConfiguration().getIdNode() + " indexó la colección");
//		 /* Recibir*/
//		 flujoEntrada= new DataInputStream( socketServicio.getInputStream());
//		 recibir();
//		 /* Enviar */
//		 objetoSalida= new ObjectOutputStream(socketServicio.getOutputStream());
////		 enviar("Nodo " + slaveNode.getNodeConfiguration().getIdNode() + " recuperó en base a la query ");
//		 enviarObjeto(slaveNode.getResultSet());
		 logger.info("------------------------------------");
		 logger.info("TERMINÓ CICLO");
		 logger.info("------------------------------------");
		 cerrar();
		 } catch( IOException e ) {
			 logger.info( e );
		 }
	}
	
	public String recibir(){
		 String msg = null;
		 try {
			 msg = flujoEntrada.readUTF();
			 logger.info("El servidor recibió: " + msg);
			 slaveNode.executeMessage(msg);
		 } catch( IOException e ) {
			 logger.info( e );
		 }
		 return msg;
	}
	
	private void enviar(String mensaje){
		 try {
		 flujoSalida.writeUTF(mensaje);
		 logger.info("El servidor envió: " + mensaje);
		 } catch( IOException e ) {
			 logger.info( e );
		 }
	}
	
	private void enviarObjeto(ResultSet resultSet){
		 try {
		 objetoSalida.writeObject(resultSet);
		 logger.info("El servidor envió: resultSet serializado");
		 } catch( IOException e ) {
			 logger.info( e );
		 }
	}
	
	private void cerrar(){
		/* Cierro */
		try {
			flujoSalida.close();
			flujoEntrada.close();
			socketServicio.close();
			miServicio.close();
			logger.info("El servidor terminó su ejecución correctamente");
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
