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
import io.grpc.Status;
import io.grpc.protobuf.lite.ProtoLiteUtils;
import io.grpc.InsecureChannelCredentials;
import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.StatusRuntimeException;
import java.lang.reflect.Method;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.lang3.ObjectUtils.Null;

import java.util.HashMap;
import java.util.HashSet;
import edu.stanford.witnesskvs.KvsGrpc;
import edu.stanford.witnesskvs.KvsStatus;
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

  private HashMap<String, ManagedChannel> channel_map;
  private HashMap<String, KvsGrpc.KvsBlockingStub> stub_map;
  private String preferred;

  protected void finalize() throws Throwable {
    channel_map.forEach((h, channel) -> {
      try {
        channel.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
      }
    });
  }

  /*
   * Returns a blocking stub for hostport, creating it if necessary, storing it in
   * a hashmap along with the channel in another hashmap.
   */
  private KvsGrpc.KvsBlockingStub GetStub(String hostport) {
    if (!stub_map.containsKey(hostport)) {
      if (!channel_map.containsKey(hostport)) {
        ManagedChannel channel = Grpc.newChannelBuilder(hostport, InsecureChannelCredentials.create())
            .build();
        channel_map.put(hostport, channel);
      }
      ManagedChannel channel = channel_map.get(hostport);
      KvsGrpc.KvsBlockingStub stub = KvsGrpc.newBlockingStub(channel);
      stub_map.put(hostport, stub);
    }
    return stub_map.get(hostport);
  }

  public Client(String[] hostports, String preferred) {
    this.channel_map = new HashMap<>();
    this.stub_map = new HashMap<>();
    this.preferred = preferred;
    for (String hostport : hostports) {
      GetStub(hostport);
    }
  }

  private static final Metadata.Key<KvsStatus> STATUS_DETAILS_KEY = Metadata.Key.of(
      "grpc-status-details-bin",
      ProtoLiteUtils.metadataMarshaller(KvsStatus.getDefaultInstance()));

  /** Get from server. */
  public String get(String key) {
    logger.info("Will try to get " + key + " ...");
    GetRequest request = GetRequest.newBuilder().setKey(key).build();
    GetResponse response;
    try {
      response = GetStub(preferred).get(request);
      return response.getValue();
    } catch (StatusRuntimeException e) {
      // TODO(mmucklo): Maybe figure out a way to cleanup this duplicated code.
      logger.log(Level.WARNING, "RPC failed: {0}", e.getStatus());
      Metadata trailers = e.getTrailers();
      KvsStatus status = trailers.get(STATUS_DETAILS_KEY);
      if (status != null && status.getType() == KvsStatus.Type.REDIRECT) {
        String ip_port = status.getRedirectDetails().getIpAddressWithPort();
        try {
          preferred = ip_port;
          response = GetStub(ip_port).get(request);
          return response.getValue();
        } catch (StatusRuntimeException ex) {
          logger.log(Level.WARNING, "RPC failed: {0}", ex.getStatus());
          return null;
        }
      }
      return null;
    }
  }

  /** Put to server. */
  public Boolean put(String key, String value) {
    logger.info("Will try to put " + key + " " + value + " ...");
    PutRequest request = PutRequest.newBuilder().setKey(key).setValue(value).build();
    PutResponse response;
    try {
      response = GetStub(preferred).put(request);
      return true;
    } catch (StatusRuntimeException e) {
      logger.log(Level.WARNING, "RPC failed: {0}", e.getStatus());
      Metadata trailers = e.getTrailers();
      KvsStatus status = trailers.get(STATUS_DETAILS_KEY);
      if (status != null && status.getType() == KvsStatus.Type.REDIRECT) {
        String ip_port = status.getRedirectDetails().getIpAddressWithPort();
        try {
          preferred = ip_port;
          response = GetStub(ip_port).put(request);
          return true;
        } catch (StatusRuntimeException ex) {
          logger.log(Level.WARNING, "RPC failed: {0}", ex.getStatus());
          return false;
        }
      }
      return false;
    }
  }

  /** Delete from server. */
  public Boolean delete(String key) {
    logger.info("Will try to delete " + key + " ...");
    DeleteRequest request = DeleteRequest.newBuilder().setKey(key).build();
    DeleteResponse response;
    try {
      response = GetStub(preferred).delete(request);
      return true;
    } catch (StatusRuntimeException e) {
      logger.log(Level.WARNING, "RPC failed: {0}", e.getStatus());
      Metadata trailers = e.getTrailers();
      KvsStatus status = trailers.get(STATUS_DETAILS_KEY);
      if (status != null && status.getType() == KvsStatus.Type.REDIRECT) {
        String ip_port = status.getRedirectDetails().getIpAddressWithPort();
        try {
          preferred = ip_port;
          response = GetStub(ip_port).delete(request);
          return true;
        } catch (StatusRuntimeException ex) {
          logger.log(Level.WARNING, "RPC failed: {0}", ex.getStatus());
          return false;
        }
      }
      return false;
    }
  }

  public static void usage(String errmsg) {
    if (errmsg.length() > 0) {
      System.err.println(errmsg);
    }
    System.err.println("Usage: <host:port,host:port,...> <operation> <key> [value]");
    System.err.println("");
    System.err.println("  host:port  The host:port(s) to contact");
    System.err.println("  operation  One of get|put|delete");
    System.err.println("  key        The key to operate on");
    System.err.println("  value      Optional value if needed");
    System.exit(1);
  }

  /**
   * Greet server. If provided, the first element of {@code args} is the name to
   * use in the
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

    String[] hostports = hostport.split(",");

    Client client = new Client(hostports, hostports[0]);
    if (operation.equals("get")) {
      String result = client.get(key);
      if (result != null) {
        System.out.println(result);
      }
    } else if (operation.equals("put")) {
      if (args.length < 4) {
        usage("No value passed in.");
      }
      if (client.put(key, value)) {
        System.out.println("Success (put)!");
      }
    } else if (operation.equals("delete")) {
      if (client.delete(key)) {
        System.out.println("Success (delete)!");
      }
    } else {
      usage("Unknown operation" + operation);
    }
  }
}
