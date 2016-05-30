package node;



public interface ISlaveNode extends INode, IServer {
	void executeMessage(String msg);

}
