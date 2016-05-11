package connections;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;

import node.ISlaveNode;

public class Server {

	DataOutputStream flujoSalida;
	DataInputStream flujoEntrada;
	Socket socketServicio;
	ServerSocket miServicio;
	ISlaveNode slaveNode;
	
	public Server(int port){
		System.out.println("Bienvenido al Servidor!");
		 try {
		 miServicio = new ServerSocket( port );
		 } catch( IOException e ) {
			 System.out.println( e );
		 }
	}
	
	public void setSlaveNode(ISlaveNode slaveNode){
		this.slaveNode = slaveNode;
	}
	
	public void listen(){
		 socketServicio = null;
		 try {
		 socketServicio = miServicio.accept();
		 /* Recibir */
		 flujoEntrada= new DataInputStream( socketServicio.getInputStream());
		 String mensaje = recibir();
		 flujoEntrada= new DataInputStream( socketServicio.getInputStream());
		 recibir();
		 /* Enviar */
		 flujoSalida= new DataOutputStream(socketServicio.getOutputStream());
		 enviar("Nodo " + slaveNode.getId() + " recibio datos requeridos para indexar");
		 flujoEntrada= new DataInputStream( socketServicio.getInputStream());
		 recibir();
		 /* Enviar */
		 flujoSalida= new DataOutputStream(socketServicio.getOutputStream());
		 enviar("Nodo " + slaveNode.getId() + " indexó la colección");
		 /* Recibir*/
		 flujoEntrada= new DataInputStream( socketServicio.getInputStream());
		 mensaje = recibir();
		 /* Enviar */
		 flujoSalida= new DataOutputStream(socketServicio.getOutputStream());
		 enviar("Nodo " + slaveNode.getId() + " recuperó en base a la query ");		 
		 cerrar();
		 } catch( IOException e ) {
			 System.out.println( e );
		 }
	}
	private void test(){
		 socketServicio = null;
		 try {
		 socketServicio = miServicio.accept();
		 /* Recibir*/
		 flujoEntrada= new DataInputStream( socketServicio.getInputStream());
		 recibir();
		 /* Enviar */
		 flujoSalida= new DataOutputStream(socketServicio.getOutputStream());
		 enviar("mensajeDesdeServidor");
		 } catch( IOException e ) {
			 System.out.println( e );
		 }
	}
	
	public String recibir(){
		 String msg = null;
		 try {
			 msg = flujoEntrada.readUTF();
			 System.out.println("El servidor recibió: " + msg);
			 slaveNode.executeMessage(msg);
		 } catch( IOException e ) {
			 System.out.println( e );
		 }
		 return msg;
	}
	
	private void enviar(String mensaje){
		 try {
		 flujoSalida.writeUTF(mensaje);
		 System.out.println("El servidor envió: " + mensaje);
		 } catch( IOException e ) {
			 System.out.println( e );
		 }
	}
	
	private void cerrar(){
		/* Cierro */
		try {
			flujoSalida.close();
			flujoEntrada.close();
			socketServicio.close();
			miServicio.close();
			System.out.println("El servidor terminó su ejecución correctamente");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
