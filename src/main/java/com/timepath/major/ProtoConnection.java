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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * This class represents a connected socket which communicated via protocol buffer messages
 * <p/>
 * To use, extend and create @{@link com.timepath.major.ProtoConnection.Callback} annotated methods with two
 * parameters:
 * <ul>
 * <li>The message type to register a callback on</li>
 * <li>A {@link Meta.Builder} provided to respond to the message</li>
 * </ul>
 *
 * @author TimePath
 */
public abstract class ProtoConnection {

    private static final Logger LOG = Logger.getLogger(ProtoConnection.class.getName());
    private OutputStream output;
    private InputStream  input;
    private AtomicInteger counter = new AtomicInteger();

    protected ProtoConnection() {
    }

    public ProtoConnection(Socket socket) throws IOException {
        this.output = socket.getOutputStream();
        this.input = socket.getInputStream();
    }

    /**
     * @return a container message with a unique tag
     */
    public Meta.Builder newBuilder() {
        return Meta.newBuilder().setTag(counter.getAndIncrement());
    }

    /**
     * Read messages until disconnected
     *
     * @throws IOException
     */
    public void readLoop() throws IOException {
        for(Meta m; ( m = read() ) != null; ) {
            receive(m);
        }
    }

    /**
     * @return a single message
     *
     * @throws IOException
     */
    public Meta read() throws IOException {
        return Meta.parseDelimitedFrom(input);
    }

    /**
     * Called in response to receiving a message. Fires callbacks and sends the response if there was one
     *
     * @param message
     *         the message
     */
    protected void receive(Meta message) {
        if(message == null) return;
        Meta.Builder responseBuilder = Meta.newBuilder().setTag(message.getTag());
        int initialSize = responseBuilder.clone().build().getSerializedSize();
        fireCallbacks(message, this, responseBuilder);
        Meta response = responseBuilder.build();
        if(response.getSerializedSize() > initialSize) { // There is new data to send back
            try {
                write(response);
            } catch(IOException e) {
                LOG.log(Level.WARNING, MessageFormat.format("Unable to reply to ''{0}''", this), e);
            }
        }
    }

    /**
     * Fires callbacks to a listener in response to a message
     *
     * @param message
     *         the message
     * @param listener
     *         the listening object
     * @param responseBuilder
     *         a response object to respond with
     */
    protected void fireCallbacks(Meta message, Object listener, Meta.Builder responseBuilder) {
        Map<FieldDescriptor, Object> allFields = message.getAllFields();
        for(Object field : allFields.values()) {
            if(!( field instanceof MessageLite )) continue;
            Method callback = null;
            for(Method method : listener.getClass().getDeclaredMethods()) {
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
                callback.invoke(listener, field, responseBuilder);
            } catch(Throwable e) {
                LOG.log(Level.WARNING, MessageFormat.format("Callback failed for ''{0}''", field), e);
            }
        }
    }

    private boolean isApplicable(Method method, Object o) {
        if(method.getAnnotation(Callback.class) == null) return false;
        Class<?>[] c = method.getParameterTypes();
        if(c.length != 2) return false;
        return c[0].isInstance(o) && Meta.Builder.class.isAssignableFrom(c[1]);
    }

    /**
     * Write a message to the server. It is recommended to use {@link #newBuilder()} to create a message container
     *
     * @param message
     *         The message
     *
     * @throws IOException
     */
    public void write(MessageLite message) throws IOException {
        message.writeDelimitedTo(output);
    }

    /**
     * Marks a method as a callback candidate for a {@link com.timepath.major.ProtoConnection}
     */
    @Target(ElementType.METHOD)
    @Retention(RetentionPolicy.RUNTIME)
    @SuppressWarnings("UnusedDeclaration")
    public @interface Callback {}
}
