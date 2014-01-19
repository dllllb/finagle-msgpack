package io.github.dmitrib.finagle.msgpack;

import org.msgpack.MessagePackable;
import org.msgpack.packer.Packer;
import org.msgpack.unpacker.Unpacker;

import java.io.IOException;

/**
 * @author Dmitri Babaev (dmitri.babaev@gmail.com)
 */
public class RpcResponse implements MessagePackable {
    private Object response;
    private boolean failed;

    @SuppressWarnings("unused")
    public RpcResponse() {

        //for msgpack serialization
    }

    public RpcResponse(Object response, boolean failed) {
        this.response = response;
        this.failed = failed;
    }

    public Object getResponse() {
        return response;
    }

    public boolean isFailed() {
        return failed;
    }

    @Override
    public void writeTo(Packer pk) throws IOException {
        pk.write(response.getClass().getName());
        pk.write(response);
        pk.write(failed);
    }

    @Override
    public void readFrom(Unpacker u) throws IOException {
        String responseType = u.readString();
        try {
            Class<?> klass = Class.forName(responseType);
            //can't be implemented in Scala, read(T to) method overload would always be called
            response = u.read(klass);
        } catch (Exception e) {
            throw new RpcException("can't deserialize message", e);
        }
        failed = u.readBoolean();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        RpcResponse that = (RpcResponse) o;

        if (failed != that.failed) return false;
        if (response != null ? !response.equals(that.response) : that.response != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = response != null ? response.hashCode() : 0;
        result = 31 * result + (failed ? 1 : 0);
        return result;
    }
}
