import java.util.Scanner;

import node.CMasterNode;
import node.CSlaveNode;
import node.IMasterNode;
import node.ISlaveNode;
import util.CUtil;
import Factory.CFactoryPartitionMethod;
import connections.Server;

public class NewMain {

	public static void main(String[] args) {
		Scanner scanner = new Scanner(System.in);
		System.out.println("Escriba 'cliente' o 'servidor' - puerto. Ejemplo: servidor-1234 ");
		String[] obj = scanner.next().split("-");
		String opcion = obj[0];
		if ("cliente".equals(opcion)){
			createClient();
		}else if ("servidor".equals(opcion)){
			int port = Integer.valueOf(obj[1]);
			createServer(port);
		}else
			System.out.println("Parámetro incorrecto");
		scanner.close();
	}
	
	private static void createClient(){
		try {
		/* Creo Nodo Master */
		IMasterNode nodo = new CMasterNode();
		/* Pido cantidad de corpus y query */
		Scanner scanner = new Scanner(System.in);
		String recrearCorpus = "";
		/* Indica si el Master debe indexar o no */
		System.out.print("El master debe indexar? S/N: ");
		String indexa = scanner.nextLine().toUpperCase();
		nodo.setIndexa("S".equals(indexa)?Boolean.TRUE:Boolean.FALSE);
		System.out.print("Desea recrear el corpus? S/N: ");
		while (!recrearCorpus.equals("S") && !recrearCorpus.equals("N") && !recrearCorpus.equals("s") && !recrearCorpus.equals("s"))
			recrearCorpus = CUtil.parseString(scanner.nextLine());
		int cantidadCorpus=0;
		if (recrearCorpus.equals("S") || recrearCorpus.equals("s")){
			/* Pido el metodo de particionamiento del corpus */
			System.out.println("Ingrese el numero del metodo de particionamiento ");
			System.out.println("1- Particionamiento por Documentos: RoundRobin");
			System.out.println("2- Particionamiento por Documentos: Por tamaño");
			System.out.println("3- Particionamiento por Terminos: RoundRobin");
			System.out.println("4- Particionamiento por Terminos: Por tamaño");
			System.out.print("Opción: ");
			int metodoId = 0;
			while (metodoId > 4 || metodoId < 1)
				metodoId = Integer.parseInt(scanner.nextLine());
			/* Instancio el metodo de particionamiento */
			nodo.setPartitionMethod(CFactoryPartitionMethod.getInstance(metodoId));
			/* Cantidad de corpus a crear */
			System.out.print("Ingrese la cantidad de nodos: ");
			cantidadCorpus = Integer.valueOf(scanner.nextLine());
			nodo.setCantidadCorpus(cantidadCorpus);
			nodo.createSlaveNodes(nodo.getCantidadCorpus());
		}
		/* Se pide query y se lo parsea */
		System.out.print("Ingrese el query: ");
		String query = CUtil.parseString(scanner.nextLine());
		scanner.close();
		if (recrearCorpus.equals("S") || recrearCorpus.equals("s")){
			/* Creo Corpus */
			nodo.createCorpus();
		   	/* Agrego todos los corpus al archivo /etc/collection.spec/ para que se tengan en cuenta en la Indexacion */
			/* Cuando se tengan Slave nodes se dividirán los Corpus */
			nodo.sendCorpusToNodes();

		}
		/* Creo Index */
		nodo.sendOrderToIndex(recrearCorpus, nodo.getPartitionMethod().getClass().getName());
		/* Recupero */
		nodo.sendOrderToRetrieval(query);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private static void createServer(int port){
		/* Creo Servidor */
		Server server = new Server(port);
		/* Creo Nodo Esclavo */
		ISlaveNode slaveNode = new CSlaveNode(server);
		server.setSlaveNode(slaveNode);
		server.listen();
	}
}
