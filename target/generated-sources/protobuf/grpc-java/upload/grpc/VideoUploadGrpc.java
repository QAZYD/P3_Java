package upload.grpc;

import static io.grpc.MethodDescriptor.generateFullMethodName;

/**
 * <pre>
 * Producer streams one video to the consumer; consumer replies once at the end.
 * </pre>
 */
@javax.annotation.Generated(
    value = "by gRPC proto compiler (version 1.68.1)",
    comments = "Source: video_upload.proto")
@io.grpc.stub.annotations.GrpcGenerated
public final class VideoUploadGrpc {

  private VideoUploadGrpc() {}

  public static final java.lang.String SERVICE_NAME = "upload.VideoUpload";

  // Static method descriptors that strictly reflect the proto.
  private static volatile io.grpc.MethodDescriptor<upload.grpc.UploadChunk,
      upload.grpc.UploadReply> getUploadMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "Upload",
      requestType = upload.grpc.UploadChunk.class,
      responseType = upload.grpc.UploadReply.class,
      methodType = io.grpc.MethodDescriptor.MethodType.CLIENT_STREAMING)
  public static io.grpc.MethodDescriptor<upload.grpc.UploadChunk,
      upload.grpc.UploadReply> getUploadMethod() {
    io.grpc.MethodDescriptor<upload.grpc.UploadChunk, upload.grpc.UploadReply> getUploadMethod;
    if ((getUploadMethod = VideoUploadGrpc.getUploadMethod) == null) {
      synchronized (VideoUploadGrpc.class) {
        if ((getUploadMethod = VideoUploadGrpc.getUploadMethod) == null) {
          VideoUploadGrpc.getUploadMethod = getUploadMethod =
              io.grpc.MethodDescriptor.<upload.grpc.UploadChunk, upload.grpc.UploadReply>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.CLIENT_STREAMING)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "Upload"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  upload.grpc.UploadChunk.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  upload.grpc.UploadReply.getDefaultInstance()))
              .setSchemaDescriptor(new VideoUploadMethodDescriptorSupplier("Upload"))
              .build();
        }
      }
    }
    return getUploadMethod;
  }

  /**
   * Creates a new async stub that supports all call types for the service
   */
  public static VideoUploadStub newStub(io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<VideoUploadStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<VideoUploadStub>() {
        @java.lang.Override
        public VideoUploadStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new VideoUploadStub(channel, callOptions);
        }
      };
    return VideoUploadStub.newStub(factory, channel);
  }

  /**
   * Creates a new blocking-style stub that supports unary and streaming output calls on the service
   */
  public static VideoUploadBlockingStub newBlockingStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<VideoUploadBlockingStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<VideoUploadBlockingStub>() {
        @java.lang.Override
        public VideoUploadBlockingStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new VideoUploadBlockingStub(channel, callOptions);
        }
      };
    return VideoUploadBlockingStub.newStub(factory, channel);
  }

  /**
   * Creates a new ListenableFuture-style stub that supports unary calls on the service
   */
  public static VideoUploadFutureStub newFutureStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<VideoUploadFutureStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<VideoUploadFutureStub>() {
        @java.lang.Override
        public VideoUploadFutureStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new VideoUploadFutureStub(channel, callOptions);
        }
      };
    return VideoUploadFutureStub.newStub(factory, channel);
  }

  /**
   * <pre>
   * Producer streams one video to the consumer; consumer replies once at the end.
   * </pre>
   */
  public interface AsyncService {

    /**
     */
    default io.grpc.stub.StreamObserver<upload.grpc.UploadChunk> upload(
        io.grpc.stub.StreamObserver<upload.grpc.UploadReply> responseObserver) {
      return io.grpc.stub.ServerCalls.asyncUnimplementedStreamingCall(getUploadMethod(), responseObserver);
    }
  }

  /**
   * Base class for the server implementation of the service VideoUpload.
   * <pre>
   * Producer streams one video to the consumer; consumer replies once at the end.
   * </pre>
   */
  public static abstract class VideoUploadImplBase
      implements io.grpc.BindableService, AsyncService {

    @java.lang.Override public final io.grpc.ServerServiceDefinition bindService() {
      return VideoUploadGrpc.bindService(this);
    }
  }

  /**
   * A stub to allow clients to do asynchronous rpc calls to service VideoUpload.
   * <pre>
   * Producer streams one video to the consumer; consumer replies once at the end.
   * </pre>
   */
  public static final class VideoUploadStub
      extends io.grpc.stub.AbstractAsyncStub<VideoUploadStub> {
    private VideoUploadStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected VideoUploadStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new VideoUploadStub(channel, callOptions);
    }

    /**
     */
    public io.grpc.stub.StreamObserver<upload.grpc.UploadChunk> upload(
        io.grpc.stub.StreamObserver<upload.grpc.UploadReply> responseObserver) {
      return io.grpc.stub.ClientCalls.asyncClientStreamingCall(
          getChannel().newCall(getUploadMethod(), getCallOptions()), responseObserver);
    }
  }

  /**
   * A stub to allow clients to do synchronous rpc calls to service VideoUpload.
   * <pre>
   * Producer streams one video to the consumer; consumer replies once at the end.
   * </pre>
   */
  public static final class VideoUploadBlockingStub
      extends io.grpc.stub.AbstractBlockingStub<VideoUploadBlockingStub> {
    private VideoUploadBlockingStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected VideoUploadBlockingStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new VideoUploadBlockingStub(channel, callOptions);
    }
  }

  /**
   * A stub to allow clients to do ListenableFuture-style rpc calls to service VideoUpload.
   * <pre>
   * Producer streams one video to the consumer; consumer replies once at the end.
   * </pre>
   */
  public static final class VideoUploadFutureStub
      extends io.grpc.stub.AbstractFutureStub<VideoUploadFutureStub> {
    private VideoUploadFutureStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected VideoUploadFutureStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new VideoUploadFutureStub(channel, callOptions);
    }
  }

  private static final int METHODID_UPLOAD = 0;

  private static final class MethodHandlers<Req, Resp> implements
      io.grpc.stub.ServerCalls.UnaryMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ServerStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ClientStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.BidiStreamingMethod<Req, Resp> {
    private final AsyncService serviceImpl;
    private final int methodId;

    MethodHandlers(AsyncService serviceImpl, int methodId) {
      this.serviceImpl = serviceImpl;
      this.methodId = methodId;
    }

    @java.lang.Override
    @java.lang.SuppressWarnings("unchecked")
    public void invoke(Req request, io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        default:
          throw new AssertionError();
      }
    }

    @java.lang.Override
    @java.lang.SuppressWarnings("unchecked")
    public io.grpc.stub.StreamObserver<Req> invoke(
        io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        case METHODID_UPLOAD:
          return (io.grpc.stub.StreamObserver<Req>) serviceImpl.upload(
              (io.grpc.stub.StreamObserver<upload.grpc.UploadReply>) responseObserver);
        default:
          throw new AssertionError();
      }
    }
  }

  public static final io.grpc.ServerServiceDefinition bindService(AsyncService service) {
    return io.grpc.ServerServiceDefinition.builder(getServiceDescriptor())
        .addMethod(
          getUploadMethod(),
          io.grpc.stub.ServerCalls.asyncClientStreamingCall(
            new MethodHandlers<
              upload.grpc.UploadChunk,
              upload.grpc.UploadReply>(
                service, METHODID_UPLOAD)))
        .build();
  }

  private static abstract class VideoUploadBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoFileDescriptorSupplier, io.grpc.protobuf.ProtoServiceDescriptorSupplier {
    VideoUploadBaseDescriptorSupplier() {}

    @java.lang.Override
    public com.google.protobuf.Descriptors.FileDescriptor getFileDescriptor() {
      return upload.grpc.VideoUploadOuterClass.getDescriptor();
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.ServiceDescriptor getServiceDescriptor() {
      return getFileDescriptor().findServiceByName("VideoUpload");
    }
  }

  private static final class VideoUploadFileDescriptorSupplier
      extends VideoUploadBaseDescriptorSupplier {
    VideoUploadFileDescriptorSupplier() {}
  }

  private static final class VideoUploadMethodDescriptorSupplier
      extends VideoUploadBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoMethodDescriptorSupplier {
    private final java.lang.String methodName;

    VideoUploadMethodDescriptorSupplier(java.lang.String methodName) {
      this.methodName = methodName;
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.MethodDescriptor getMethodDescriptor() {
      return getServiceDescriptor().findMethodByName(methodName);
    }
  }

  private static volatile io.grpc.ServiceDescriptor serviceDescriptor;

  public static io.grpc.ServiceDescriptor getServiceDescriptor() {
    io.grpc.ServiceDescriptor result = serviceDescriptor;
    if (result == null) {
      synchronized (VideoUploadGrpc.class) {
        result = serviceDescriptor;
        if (result == null) {
          serviceDescriptor = result = io.grpc.ServiceDescriptor.newBuilder(SERVICE_NAME)
              .setSchemaDescriptor(new VideoUploadFileDescriptorSupplier())
              .addMethod(getUploadMethod())
              .build();
        }
      }
    }
    return result;
  }
}
