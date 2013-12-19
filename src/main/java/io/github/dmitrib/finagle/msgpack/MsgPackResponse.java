package io.github.dmitrib.finagle.msgpack;

import org.msgpack.annotation.Message;

/**
 * @author Dmitri Babaev (dmitri.babaev@gmail.com)
 */
@Message
public class MsgPackResponse {
    public MsgPackResponse(Object response) {
        this.response = response;
    }

    public Object response;
}
