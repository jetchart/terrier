package node;



public interface ISlaveNode extends INode, IServer {
	void executeMessage(String msg);
	void copiarCorpusDesdeMaster(String corpusPathOnMaster,String corpusPathOnSlave, String userSFTP, String passwordSFTP,String masterSFTPHost, Integer masterSFTPPort);
}
