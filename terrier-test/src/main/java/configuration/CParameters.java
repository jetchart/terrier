package configuration;

import partitioning.IPartitionMethod;
import Factory.CFactoryPartitionMethod;

public class CParameters {

	private String nodeType;
	private String action;
	private Boolean masterIndexa;
	private Boolean recrearCorpus;
	private IPartitionMethod metodoParticionamiento;
	private String carpetaColeccion;
	private Integer cantidadNodos;
	private String metodoComunicacion;
	private Boolean wakeUpSlaves;
	private String query;
	private Boolean eliminarCorpus;
	private Boolean mergearIndices;
	private String indexName;
	private String previousIndexName;
	private String runName;
	
	public final static String action_INDEX = "INDEX";
	public final static String action_RETRIEVAL = "RETRIEVAL";
	public final static String action_ALL = "ALL";
	
	public final static String metodoComunicacion_SSH = "SSH";
	public final static String metodoComunicacion_PATH = "PATH";
	
	public CParameters(String nodeType, String action, String masterIndexa, String recrearCorpus, String metodoParticionamiento, String carpetaColeccion, String cantidadNodos, String metodoComunicacion, String wakeUpSlaves, String query, String eliminarCorpus, String mergearIndices, String previousIndexName, String runName){
		this.nodeType = nodeType;
		this.action = action;
		this.masterIndexa = masterIndexa.toUpperCase().equals("S");
		this.recrearCorpus = recrearCorpus.toUpperCase().equals("S");
		this.metodoParticionamiento = CFactoryPartitionMethod.getInstance(Integer.valueOf(metodoParticionamiento));
		this.carpetaColeccion = carpetaColeccion;
		this.cantidadNodos = Integer.valueOf(cantidadNodos);
		this.metodoComunicacion = metodoComunicacion.toUpperCase();
		this.wakeUpSlaves = wakeUpSlaves.toUpperCase().equals("S");
		this.query = query;
		this.eliminarCorpus = eliminarCorpus.toUpperCase().equals("S");
		this.mergearIndices = mergearIndices.toUpperCase().equals("S");
		this.previousIndexName = previousIndexName.trim();
		this.runName = runName.toLowerCase();
	}
	
	public CParameters(String nodeType, String action, String indexName, String query, String runName){
		this.nodeType = nodeType;
		this.action = action;
		this.indexName = indexName;
		this.query = query;
		this.cantidadNodos = 1;
		this.masterIndexa = Boolean.TRUE;
		this.wakeUpSlaves = Boolean.TRUE;
		this.runName = runName.toLowerCase();
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

	public String getAction() {
		return action;
	}

	public void setAction(String action) {
		this.action = action;
	}

	public String getCarpetaColeccion() {
		return carpetaColeccion;
	}

	public void setCarpetaColeccion(String carpetaColeccion) {
		this.carpetaColeccion = carpetaColeccion;
	}

	public Boolean getEliminarCorpus() {
		return eliminarCorpus;
	}

	public void setEliminarCorpus(Boolean eliminarCorpus) {
		this.eliminarCorpus = eliminarCorpus;
	}

	public Boolean getWakeUpSlaves() {
		return wakeUpSlaves;
	}

	public void setWakeUpSlaves(Boolean wakeUpSlaves) {
		this.wakeUpSlaves = wakeUpSlaves;
	}

	public Boolean getMergearIndices() {
		return mergearIndices;
	}

	public void setMergearIndices(Boolean mergearIndices) {
		this.mergearIndices = mergearIndices;
	}

	public String getIndexName() {
		return indexName;
	}

	public void setIndexName(String indexName) {
		this.indexName = indexName;
	}

	public String getRunName() {
		return runName;
	}

	public void setRunName(String runName) {
		this.runName = runName;
	}

	public String getPreviousIndexName() {
		return previousIndexName;
	}

	public void setPreviousIndexName(String previousIndexName) {
		this.previousIndexName = previousIndexName;
	}

}