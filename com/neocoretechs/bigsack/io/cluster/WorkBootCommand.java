package com.neocoretechs.bigsack.io.cluster;

import java.io.Serializable;
/**
 * This class carries the command from the UDPMaster to the WorkBoot TCP server instance on the respective node.
 * It carries the necessary setup information to spin the UDPWorker threads on that node. The setup information
 * is contained in this class, which is used once during the initial boot up of the node.
 * The design is one per node and if running locally one for all tablespaces
 * @author jg
 *
 */
public final class WorkBootCommand implements CommandPacketInterface, Serializable {
	private static final long serialVersionUID = 893462083293613273L;
	private String database;
	private int tablespace;
	private String masterPort;
	private String slavePort;
	private String transport;
	private String remoteMaster;
	
	public WorkBootCommand(){}
	
	@Override
	public String getDatabase() {
		return database;
	}
	@Override
	public void setDatabase(String database) {
		this.database = database;
	}
	@Override
	public int getTablespace() {
		return tablespace;
	}
	@Override
	public void setTablespace(int tablespace) {
		this.tablespace = tablespace;
	}
	@Override
	public String getMasterPort() {
		return masterPort;
	}
	@Override
	public void setMasterPort(String port) {
		this.masterPort = port;	
	}
	@Override
	public String toString() {
		return database+" tablespace:"+tablespace+" master port:"+masterPort+" slave:"+slavePort+" transport "+transport;
	}
	@Override
	public String getSlavePort() {
		return slavePort;
	}
	@Override
	public void setSlavePort(String port) {
			this.slavePort = port;	
	}

	@Override
	public String getTransport() {
		return transport;
	}

	@Override
	public void setTransport(String transport) {
		this.transport = transport;	
	}

	@Override
	public String getRemoteMaster() {
		return remoteMaster;
	}

	@Override
	public void setRemoteMaster(String remoteMaster) {
		this.remoteMaster = remoteMaster;	
	}
	
}
