package io.github.dmitrib.finagle.msgpack;

import org.msgpack.annotation.Message;

import java.util.List;

/**
 * @author Dmitri Babaev (dmitri.babaev@gmail.com)
 */
@Message
public class MsgPackRequest {
    public MsgPackRequest(List<Object> args, String method, String service) {
        this.args = args;
        this.method = method;
        this.service = service;
    }

    public List<Object> args;
    public String method;
    public String service;
}
