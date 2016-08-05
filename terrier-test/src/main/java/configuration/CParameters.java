package configuration;

public class CParameters {

	private String tipoNodo;
	private String masterIndexa;
	private String recrearCorpus;
	private String metodoParticionamiento;
	private String cantidadNodos;
	private String metodoComunicacion;
	private String query;
	
	public CParameters(String tipoNodo, String masterIndexa, String recrearCorpus, String metodoParticionamiento, String cantidadNodos, String metodoComunicacion, String query){
		this.tipoNodo = tipoNodo;
		this.masterIndexa = masterIndexa;
		this.recrearCorpus = recrearCorpus;
		this.metodoParticionamiento = metodoParticionamiento;
		this.cantidadNodos = cantidadNodos;
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
		return tipoNodo;
	}

	public void setTipoNodo(String tipoNodo) {
		this.tipoNodo = tipoNodo;
	}

	public String getMasterIndexa() {
		return masterIndexa;
	}

	public void setMasterIndexa(String masterIndexa) {
		this.masterIndexa = masterIndexa;
	}

	public String getRecrearCorpus() {
		return recrearCorpus;
	}

	public void setRecrearCorpus(String recrearCorpus) {
		this.recrearCorpus = recrearCorpus;
	}

	public String getMetodoParticionamiento() {
		return metodoParticionamiento;
	}

	public void setMetodoParticionamiento(String metodoParticionamiento) {
		this.metodoParticionamiento = metodoParticionamiento;
	}

	public String getCantidadNodos() {
		return cantidadNodos;
	}

	public void setCantidadNodos(String cantidadNodos) {
		this.cantidadNodos = cantidadNodos;
	}

	public String getMetodoComunicacion() {
		return metodoComunicacion;
	}

	public void setMetodoComunicacion(String metodoComunicacion) {
		this.metodoComunicacion = metodoComunicacion;
	}
}
