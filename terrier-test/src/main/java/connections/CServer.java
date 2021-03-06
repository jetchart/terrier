package connections;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;

import node.CNode;
import node.ISlaveNode;

import org.apache.log4j.Logger;
import org.terrier.matching.ResultSet;

import configuration.INodeConfiguration;

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
	
	public void listen(Boolean cycle){
		 logger.info("------------------------------------");
		 logger.info("COMIENZA NUEVO CICLO");
		 logger.info("------------------------------------");
		 if (cycle){
			 logger.info("Se eligió el comportamiento \"cycle\" por lo tanto se realizará este ciclo hasta que se cierre manualmente");
		 }else{
			 logger.info("Se eligió el comportamiento \"noCycle\" por lo tanto el master se encarga de levantar y destruir el esclavo");
		 }
		 socketServicio = null;
		 try {
			 socketServicio = miServicio.accept();
			 String mensaje;
			 flujoEntrada= new DataInputStream( socketServicio.getInputStream());
			 while ((mensaje = recibir()) != null){
				 if (mensaje.startsWith(CNode.task_RETRIEVAL)){
					 logger.info(">>>>>>>>>>** "+mensaje+" **<<<<<<<<<");
					 objetoSalida= new ObjectOutputStream(socketServicio.getOutputStream());
					 enviarObjeto(slaveNode.getResultSet());
				 }else if (mensaje.startsWith(CNode.task_INITIALIZE)){
					 logger.info(">>>>>>>>>> "+mensaje+" <<<<<<<<<");
					 flujoSalida= new DataOutputStream(socketServicio.getOutputStream());
					 enviar(CNode.task_INITIALIZE + slaveNode.getNodeConfiguration().getIndexPath() + INodeConfiguration.prefixIndex + slaveNode.getNodeConfiguration().getIdNode());
				 }else{
					 logger.info(">>>>>>>>>> "+mensaje+" <<<<<<<<<");
					flujoSalida= new DataOutputStream(socketServicio.getOutputStream());
				 	enviar("Nodo " + slaveNode.getNodeConfiguration().getIdNode() + " recibio: " + mensaje);
				 }
				 if (CNode.task_CLOSE.equals(mensaje)){
					break; 
				 }
				 flujoEntrada= new DataInputStream( socketServicio.getInputStream());
			 }
			 logger.info("------------------------------------");
			 logger.info("TERMINÓ CICLO");
			 logger.info("------------------------------------");
			 if (cycle){
				 listen(cycle);
			 }
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
