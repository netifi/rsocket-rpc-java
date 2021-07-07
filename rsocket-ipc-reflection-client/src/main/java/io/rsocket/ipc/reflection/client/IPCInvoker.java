package io.rsocket.ipc.reflection.client;

public interface IPCInvoker {

	public Object invoke(Object[] args) throws Throwable;
}
