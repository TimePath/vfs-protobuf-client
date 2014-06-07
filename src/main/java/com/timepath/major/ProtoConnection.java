package com.timepath.major;

import com.google.protobuf.MessageLite;
import com.timepath.major.proto.Files.FileListing;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.lang.reflect.Method;
import java.net.Socket;

/**
 * @author TimePath
 */
public abstract class ProtoConnection {

    private final OutputStream os;
    private final InputStream  is;

    public ProtoConnection(Socket s) throws IOException {
        this.os = s.getOutputStream();
        this.is = s.getInputStream();
    }

    public void read() throws IOException {
        int length = ( is.read() << 8 ) | is.read();
        byte[] buf = new byte[length];
        int total = 0;
        while(total < length) {
            total += is.read(buf, total, length - total);
        }
        callback(FileListing.parseFrom(new ByteArrayInputStream(buf, 0, total)));
    }

    private void callback(Object o) {
        if(o == null) throw new RuntimeException("Null callback object.");
        Method m = null;
        for(Method method : getClass().getDeclaredMethods()) {
            if(isApplicable(method, o)) {
                m = method;
                break;
            }
        }
        if(m == null) {
            throw new RuntimeException("No callback for '" + o.getClass() + "'.");
        }
        try {
            m.setAccessible(true);
            m.invoke(this, o);
        } catch(Exception e) {
            throw new RuntimeException("Callback failed for '" + o.getClass() + "'.", e);
        }
    }

    private boolean isApplicable(Method method, Object o) {
        if(method.getAnnotation(Callback.class) == null) return false;
        Class[] c = method.getParameterTypes();
        if(c.length > 1) return false;
        return c[0].isInstance(o);
    }

    public void write(MessageLite m) throws IOException {
        int length = m.getSerializedSize();
        os.write(( length & 0xFF00 ) >> 8);
        os.write(length & 0xFF);
        m.writeTo(os);
    }

    @Target(ElementType.METHOD)
    @Retention(RetentionPolicy.RUNTIME)
    public @interface Callback {}
}
