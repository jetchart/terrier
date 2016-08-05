package configuration;

import partitioning.IPartitionMethod;
import Factory.CFactoryPartitionMethod;

public class CParameters {

	private String nodeType;
	private Boolean masterIndexa;
	private Boolean recrearCorpus;
	private IPartitionMethod metodoParticionamiento;
	private Integer cantidadNodos;
	private String metodoComunicacion;
	private String query;
	
	public CParameters(String nodeType, String masterIndexa, String recrearCorpus, String metodoParticionamiento, String cantidadNodos, String metodoComunicacion, String query){
		this.nodeType = nodeType;
		this.masterIndexa = masterIndexa.toUpperCase().equals("S");
		this.recrearCorpus = recrearCorpus.toUpperCase().equals("S");
		this.metodoParticionamiento = CFactoryPartitionMethod.getInstance(Integer.valueOf(metodoParticionamiento));
		this.cantidadNodos = Integer.valueOf(cantidadNodos);
		this.metodoComunicacion = metodoComunicacion;
		this.query = query;
	}
	
	public String getQuery() {
		return query;
	}

	public void setQuery(String query) {
		this.query = query;
	}

	public String getTipoNodo() {
		return nodeType;
	}

	public void setTipoNodo(String tipoNodo) {
		this.nodeType = tipoNodo;
	}

	public Boolean getMasterIndexa() {
		return masterIndexa;
	}

	public void setMasterIndexa(Boolean masterIndexa) {
		this.masterIndexa = masterIndexa;
	}

	public Boolean getRecrearCorpus() {
		return recrearCorpus;
	}

	public void setRecrearCorpus(Boolean recrearCorpus) {
		this.recrearCorpus = recrearCorpus;
	}

	public IPartitionMethod getMetodoParticionamiento() {
		return metodoParticionamiento;
	}

	public void setMetodoParticionamiento(IPartitionMethod metodoParticionamiento) {
		this.metodoParticionamiento = metodoParticionamiento;
	}

	public Integer getCantidadNodos() {
		return cantidadNodos;
	}

	public void setCantidadNodos(Integer cantidadNodos) {
		this.cantidadNodos = cantidadNodos;
	}

	public String getMetodoComunicacion() {
		return metodoComunicacion;
	}

	public void setMetodoComunicacion(String metodoComunicacion) {
		this.metodoComunicacion = metodoComunicacion;
	}
}
