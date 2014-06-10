package com.timepath.major;

import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.MessageLite;
import com.timepath.major.proto.Messages.Meta;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.lang.reflect.Method;
import java.net.Socket;
import java.text.MessageFormat;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * @author TimePath
 */
public abstract class ProtoConnection {

    private static final Logger LOG = Logger.getLogger(ProtoConnection.class.getName());
    private final OutputStream os;
    private final InputStream  is;

    public ProtoConnection(Socket s) throws IOException {
        this.os = s.getOutputStream();
        this.is = s.getInputStream();
    }

    public Meta read() throws IOException {
        return Meta.parseDelimitedFrom(is);
    }

    protected void callback(Meta msg) {
        if(msg == null) return;
        Map<FieldDescriptor, Object> allFields = msg.getAllFields();
        for(Object field : allFields.values()) {
            if(field == null) continue;
            Method callback = null;
            for(Method method : getClass().getDeclaredMethods()) {
                if(isApplicable(method, field)) {
                    callback = method;
                    break;
                }
            }
            if(callback == null) {
                LOG.log(Level.WARNING, "No callback for ''{0}''", field);
                continue;
            }
            try {
                callback.setAccessible(true);
                callback.invoke(this, field);
            } catch(Throwable e) {
                LOG.log(Level.WARNING, MessageFormat.format("Callback failed for ''{0}''", field), e);
            }
        }
    }

    private boolean isApplicable(Method method, Object o) {
        if(method.getAnnotation(Callback.class) == null) return false;
        Class<?>[] c = method.getParameterTypes();
        if(c.length != 1) return false;
        return c[0].isInstance(o);
    }

    public void write(MessageLite m) throws IOException {
        m.writeDelimitedTo(os);
    }

    @Target(ElementType.METHOD)
    @Retention(RetentionPolicy.RUNTIME)
    public @interface Callback {}
}
