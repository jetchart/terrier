package connections;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;

import node.ISlaveNode;

public class SServer {

	DataOutputStream flujoSalida;
	DataInputStream flujoEntrada;
	Socket socketServicio;
	ServerSocket miServicio;
	ISlaveNode slaveNode;
	
	public SServer(Integer port, ISlaveNode slaveNode){
		this.slaveNode = slaveNode;
		System.out.println("Bienvenido al Servidor!");
		System.out.println("Id: " + slaveNode.getNodeConfiguration().getIdNode());
		System.out.println("Puerto: " + port);
		iniciarServidor(port);
	}
	
	private void iniciarServidor(Integer port) {
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
		 System.out.println("------------------------------------");
		 System.out.println("COMIENZA NUEVO CICLO");
		 System.out.println("------------------------------------");
		 socketServicio = null;
		 try {
		 socketServicio = miServicio.accept();
		 /* Recibir */
		 flujoEntrada= new DataInputStream( socketServicio.getInputStream());
		 recibir();
		 flujoEntrada= new DataInputStream( socketServicio.getInputStream());
		 recibir();
		 /* Enviar */
		 flujoSalida= new DataOutputStream(socketServicio.getOutputStream());
		 enviar("Nodo " + slaveNode.getNodeConfiguration().getIdNode() + " recibio datos requeridos para indexar");
		 flujoEntrada= new DataInputStream( socketServicio.getInputStream());
		 recibir();
		 /* Enviar */
		 flujoSalida= new DataOutputStream(socketServicio.getOutputStream());
		 enviar("Nodo " + slaveNode.getNodeConfiguration().getIdNode() + " indexó la colección");
		 /* Recibir*/
		 flujoEntrada= new DataInputStream( socketServicio.getInputStream());
		 recibir();
		 /* Enviar */
		 flujoSalida= new DataOutputStream(socketServicio.getOutputStream());
		 enviar("Nodo " + slaveNode.getNodeConfiguration().getIdNode() + " recuperó en base a la query ");
		 System.out.println("------------------------------------");
		 System.out.println("TERMINÓ CICLO");
		 System.out.println("------------------------------------");
		 cerrar();
		 /* Inicio el ciclo de nuevo */
		 iniciarServidor(slaveNode.getNodeConfiguration().getPort());
		 listen();
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
