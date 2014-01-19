package io.github.dmitrib.finagle.msgpack;

import org.msgpack.MessagePackable;
import org.msgpack.annotation.Message;
import org.msgpack.packer.Packer;
import org.msgpack.unpacker.Unpacker;

import java.io.IOException;
import java.util.Arrays;

/**
 * @author Dmitri Babaev (dmitri.babaev@gmail.com)
 */
@Message
public class RpcRequest implements MessagePackable {
    private String method;
    private String serviceId;
    private Object[] args;
    private Class<?>[] paramTypes;

    @SuppressWarnings("unused")
    public RpcRequest() {
        //for msgpack serialization
    }

    public RpcRequest(String method, String serviceId, Object[] args, Class<?>[] paramTypes) {
        this.method = method;
        this.serviceId = serviceId;
        this.args = args;
        this.paramTypes = paramTypes;
    }

    public String getMethod() {
        return method;
    }

    public String getServiceId() {
        return serviceId;
    }

    public Object[] getArgs() {
        return args;
    }

    public Class<?>[] getParamTypes() {
        return paramTypes;
    }

    public String getCallId() {
        return String.format("%s:%s", serviceId, method);
    }

    @Override
    public void writeTo(Packer pk) throws IOException {
        pk.write(method);
        pk.write(serviceId);
        pk.writeArrayBegin(paramTypes.length);
        for (Class<?> paramType: paramTypes) {
            pk.write(paramType.getName());
        }
        pk.writeArrayEnd();
        pk.writeArrayBegin(args.length);
        for (Object arg: args) {
            pk.write(arg);
        }
        pk.writeArrayEnd();
    }

    @Override
    public void readFrom(Unpacker u) throws IOException {
        method = u.readString();
        serviceId = u.readString();

        int paramLength = u.readArrayBegin();
        paramTypes = new Class<?>[paramLength];
        for (int i = 0; i < paramLength; ++i) {
            String typeName = u.readString();
            try {
                paramTypes[i] = Class.forName(typeName);
            } catch (Exception e) {
                throw new RpcException("can't deserialize message", e);
            }
        }
        u.readArrayEnd();

        int argLength = u.readArrayBegin();
        args = new Object[argLength];
        for (int i = 0; i < argLength; ++i) {
            //can't be implemented in Scala, read(T to) method overload would always be called
            args[i] = u.read(paramTypes[i]);
        }
        u.readArrayEnd();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        RpcRequest that = (RpcRequest) o;

        if (!Arrays.equals(args, that.args)) return false;
        if (method != null ? !method.equals(that.method) : that.method != null) return false;
        if (!Arrays.equals(paramTypes, that.paramTypes)) return false;
        if (serviceId != null ? !serviceId.equals(that.serviceId) : that.serviceId != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = method != null ? method.hashCode() : 0;
        result = 31 * result + (serviceId != null ? serviceId.hashCode() : 0);
        result = 31 * result + (args != null ? Arrays.hashCode(args) : 0);
        result = 31 * result + (paramTypes != null ? Arrays.hashCode(paramTypes) : 0);
        return result;
    }
}
