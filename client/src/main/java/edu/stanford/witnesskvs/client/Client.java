/* Based off of https://github.com/grpc/grpc-java/blob/master/examples/src/main/java/io/grpc/examples/helloworld/HelloWorldClient.java
 *
 * Copyright 2015 The gRPC Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package edu.stanford.witnesskvs.client;

import io.grpc.Channel;
import io.grpc.Grpc;
import io.grpc.InsecureChannelCredentials;
import io.grpc.ManagedChannel;
import io.grpc.StatusRuntimeException;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import edu.stanford.witnesskvs.KvsGrpc;
import edu.stanford.witnesskvs.GetRequest;
import edu.stanford.witnesskvs.GetResponse;
import edu.stanford.witnesskvs.PutRequest;
import edu.stanford.witnesskvs.PutResponse;
import edu.stanford.witnesskvs.DeleteRequest;
import edu.stanford.witnesskvs.DeleteResponse;

/**
 * A simple client that requests a greeting from the {@link HelloWorldServer}.
 */
public class Client {
  private static final Logger logger = Logger.getLogger(Client.class.getName());

  private final KvsGrpc.KvsBlockingStub blockingStub;

  /** Construct client for accessing HelloWorld server using the existing channel. */
  public Client(Channel channel) {
    // 'channel' here is a Channel, not a ManagedChannel, so it is not this code's responsibility to
    // shut it down.

    // Passing Channels to code makes code easier to test and makes it easier to reuse Channels.
    blockingStub = KvsGrpc.newBlockingStub(channel);
  }

  /** Get from server. */
  public void get(String key) {
    logger.info("Will try to get " + key + " ...");
    GetRequest request = GetRequest.newBuilder().setKey(key).build();
    GetResponse response;
    try {
      response = blockingStub.get(request);
    } catch (StatusRuntimeException e) {
      logger.log(Level.WARNING, "RPC failed: {0}", e.getStatus());
      return;
    }
    logger.info("Value: " + response.getValue());
  }

  /** Put to server. */
  public void put(String key, String value) {
    logger.info("Will try to put " + key + " " + value + " ...");
    PutRequest request = PutRequest.newBuilder().setKey(key).setValue(value).build();
    PutResponse response;
    try {
      response = blockingStub.put(request);
    } catch (StatusRuntimeException e) {
      logger.log(Level.WARNING, "RPC failed: {0}", e.getStatus());
      return;
    }
    logger.info("Status: " + response.getStatus());
  }

  /** Delete from server. */
  public void delete(String key) {
    logger.info("Will try to delete " + key + " ...");
    DeleteRequest request = DeleteRequest.newBuilder().setKey(key).build();
    DeleteResponse response;
    try {
      response = blockingStub.delete(request);
    } catch (StatusRuntimeException e) {
      logger.log(Level.WARNING, "RPC failed: {0}", e.getStatus());
      return;
    }
    logger.info("Status: " + response.getStatus());
  }

  public static void usage(String errmsg) {
    if (errmsg.length() > 0) {
      System.err.println(errmsg);
    }
    System.err.println("Usage: <host:port> <operation> <key> [value]");
    System.err.println("");
    System.err.println("  host:port       The host:port to contact");
    System.err.println("  operation  One of get|put|delete");
    System.err.println("  key        The key to operate on");
    System.err.println("  value      Optional value if needed");
    System.exit(1);
  }

  /**
   * Greet server. If provided, the first element of {@code args} is the name to use in the
   * greeting. The second argument is the target server.
   */
  public static void main(String[] args) throws Exception {
    String operation = "get";
    // Access a service running on the local machine on port 50051
    String hostport = "localhost:50051";
    // Allow passing in the user and target strings as command line arguments
    if (args.length <= 2 || args.length > 4 || "--help".equals(args[0])) {
      usage("");
    }
    hostport = args[0];
    operation = args[1];
    String key = args[2];
    String value = "";
    if (args.length > 3) {
      value = args[3];
    }

    // Create a communication channel to the server, known as a Channel. Channels are thread-safe
    // and reusable. It is common to create channels at the beginning of your application and reuse
    // them until the application shuts down.
    //
    // For the example we use plaintext insecure credentials to avoid needing TLS certificates. To
    // use TLS, use TlsChannelCredentials instead.
    ManagedChannel channel = Grpc.newChannelBuilder(hostport, InsecureChannelCredentials.create())
        .build();
    try {
      Client client = new Client(channel);
      if (operation.equals("get")) {
        client.get(key);
      } else if (operation.equals("put")) {
        if (args.length < 4) {
          usage("No value passed in.");
        }
        client.put(key, value);
      } else if (operation.equals("delete")) {
        client.delete(key);
      } else {
        usage("Unknown operation" + operation);
      }
    } finally {
      // ManagedChannels use resources like threads and TCP connections. To prevent leaking these
      // resources the channel should be shut down when it will no longer be used. If it may be used
      // again leave it running.
      channel.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
    }
  }
}
