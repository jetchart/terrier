package connections.bkp;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;

public class Client {

	DataOutputStream flujoSalida;
	DataInputStream flujoEntrada;
	Socket miCliente;
	
	public static String host = "localhost";
	public static int port = 1234;
	
	public Client(){
		 try {
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
	private void recibir(){
		 try {
		 System.out.println("El cliente recibió: " + flujoEntrada.readUTF());
		 } catch( IOException e ) {
		 System.out.println( e );
		 }
	}
	
	public void enviar(String mensaje){
		 try {
		 flujoSalida.writeUTF(mensaje);
		 System.out.println("El Cliente envió " + mensaje);
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
}
