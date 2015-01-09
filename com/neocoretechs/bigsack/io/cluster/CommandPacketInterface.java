package com.neocoretechs.bigsack.io.cluster;

import java.io.Serializable;
/**
 * Command packet interface bound for WorkBoot nodes to activate threads
 * to operate on a specific port, tablespace, and database, all determined by master node
 * controller
 * @author jg
 *
 */
public interface CommandPacketInterface extends Serializable {
	public String getDatabase();
	public void setDatabase(String database);
	public int getTablespace();
	public void setTablespace(int tablespace);
	public int getPort();
	public void setPort(int port);
}
