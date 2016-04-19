package connections.bkp;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
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
		 /* Recibir*/
		 flujoEntrada= new DataInputStream( socketServicio.getInputStream());
		 recibir();
		 /* Enviar */
		 flujoSalida= new DataOutputStream(socketServicio.getOutputStream());
		 enviar("mensajeDesdeServidor");
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
	
	public void recibir(){
		 try {
			 String msg = flujoEntrada.readUTF();
			 System.out.println("El servidor recibió: " + msg);
			 slaveNode.executeMessage(msg);
		 } catch( IOException e ) {
			 System.out.println( e );
		 }
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
