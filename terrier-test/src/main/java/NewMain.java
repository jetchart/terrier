import java.io.IOException;
import java.util.Scanner;

import node.CMasterNode;
import node.CSlaveNode;
import node.IMasterNode;
import node.INode;
import node.ISlaveNode;
import util.CUtil;
import Factory.CFactoryPartitionMethod;
import connections.CClient;
import connections.CServer;

public class NewMain {

	public static void main(String[] args) throws IOException {
		Scanner scanner = new Scanner(System.in);
		String opcion = args.length>0?args[0]:null;
		if (opcion!=null && INode.ID_MASTER.equals(opcion.toUpperCase())){
			createMaster();
		}else if (opcion!=null && INode.ID_SLAVE.equals(opcion.toUpperCase())){
			/* TODO creo que habría que sacar esto */
			/* Si se especificó un puerto lo utilizo, sino lo extraigo de configuration.properties */
			Integer port = null;
			createSlave(port);
		}else{
			System.out.println("Parámetro incorrecto");
			System.out.println("Parámetros disponibles:");
			System.out.println("\tmaster");
			System.out.println("\tslave");
		}
		scanner.close();
	}
	
	private static void createMaster(){
		try {
		/* Creo Nodo Master */
		IMasterNode nodo = new CMasterNode(INode.ID_MASTER);
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
			System.out.println("2- Particionamiento por Documentos: Por tamaño (archivos)");
			System.out.println("3- Particionamiento por Documentos: Por tamaño (cantidad de tokens unicos)");
			System.out.println("4- Particionamiento por Terminos: RoundRobin");
			System.out.println("5- Particionamiento por Terminos: Por tamaño");
			System.out.print("Opción: ");
			int metodoId = 0;
			while (metodoId > 5 || metodoId < 1)
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
		/* Mostrar resultados del master */
		if (nodo.getIndexa()){
			System.out.println("\nResultados del Master:");
			CUtil.mostrarResultados(nodo.getResultSet(), nodo.getIndex(), query);
		}
		/* Mostrar resultados de los esclavos */
		for (CClient client : nodo.getNodes()){
			System.out.println("\nResultados del Esclavo:" + client.getHost() + ":" + client.getPort());
			CUtil.mostrarResultados(client.getResultSetNodo(), null, query);
		}
		
		
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private static void createSlave(Integer port){
		/* Creo Nodo Esclavo */
		ISlaveNode slaveNode = new CSlaveNode(INode.ID_SLAVE);
		/* Creo Servidor */
		if (port == null){
			port = slaveNode.getNodeConfiguration().getPort();
		}
		CServer server = new CServer(port,slaveNode);
		server.listen();
	}
	
   
}
