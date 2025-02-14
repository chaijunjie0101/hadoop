/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdfs.server.federation.router.async;

import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.protocol.SnapshotDiffReport;
import org.apache.hadoop.hdfs.protocol.SnapshotDiffReportListing;
import org.apache.hadoop.hdfs.protocol.SnapshotStatus;
import org.apache.hadoop.hdfs.protocol.SnapshottableDirectoryStatus;
import org.apache.hadoop.hdfs.server.federation.resolver.ActiveNamenodeResolver;
import org.apache.hadoop.hdfs.server.federation.resolver.FederationNamespaceInfo;
import org.apache.hadoop.hdfs.server.federation.resolver.RemoteLocation;
import org.apache.hadoop.hdfs.server.federation.router.RemoteMethod;
import org.apache.hadoop.hdfs.server.federation.router.RemoteParam;
import org.apache.hadoop.hdfs.server.federation.router.RemoteResult;
import org.apache.hadoop.hdfs.server.federation.router.RouterRpcClient;
import org.apache.hadoop.hdfs.server.federation.router.RouterRpcServer;
import org.apache.hadoop.hdfs.server.federation.router.RouterSnapshot;
import org.apache.hadoop.hdfs.server.federation.router.async.utils.ApplyFunction;
import org.apache.hadoop.hdfs.server.namenode.NameNode;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.hadoop.hdfs.server.federation.router.async.utils.AsyncUtil.asyncApply;
import static org.apache.hadoop.hdfs.server.federation.router.async.utils.AsyncUtil.asyncReturn;

/**
 * Module that implements all the asynchronous RPC calls related to snapshots in
 * {@link ClientProtocol} in the {@link RouterRpcServer}.
 */
public class RouterAsyncSnapshot extends RouterSnapshot {
  /** RPC server to receive client calls. */
  private final RouterRpcServer rpcServer;
  /** RPC clients to connect to the Namenodes. */
  private final RouterRpcClient rpcClient;
  /** Find generic locations. */
  private final ActiveNamenodeResolver namenodeResolver;

  public RouterAsyncSnapshot(RouterRpcServer server) {
    super(server);
    this.rpcServer = server;
    this.rpcClient = this.rpcServer.getRPCClient();
    this.namenodeResolver = rpcServer.getNamenodeResolver();
  }

  /**
   * Asynchronously creates a snapshot with the given root and name.
   * This method checks the operation category and then invokes the createSnapshot
   * method concurrently across all namespaces, returning the first successful response.
   *
   * @param snapshotRoot The root path of the snapshot.
   * @param snapshotName The name of the snapshot.
   * @return The path of the created snapshot.
   * @throws IOException If an I/O error occurs.
   */
  @Override
  public String createSnapshot(String snapshotRoot, String snapshotName) throws IOException {
    rpcServer.checkOperation(NameNode.OperationCategory.WRITE);

    final List<RemoteLocation> locations =
        rpcServer.getLocationsForPath(snapshotRoot, true, false);
    RemoteMethod method = new RemoteMethod("createSnapshot",
        new Class<?>[] {String.class, String.class}, new RemoteParam(),
        snapshotName);

    if (rpcServer.isInvokeConcurrent(snapshotRoot)) {
      rpcClient.invokeConcurrent(locations, method, String.class);
      asyncApply((ApplyFunction<Map<RemoteLocation, String>, String>)
          results -> {
          Map.Entry<RemoteLocation, String> firstelement =
              results.entrySet().iterator().next();
          RemoteLocation loc = firstelement.getKey();
          String result = firstelement.getValue();
          return result.replaceFirst(loc.getDest(), loc.getSrc());
        });
    } else {
      rpcClient.invokeSequential(method, locations, String.class, null);
      asyncApply((ApplyFunction<RemoteResult<RemoteLocation, String>, String>)
          response -> {
          RemoteLocation loc = response.getLocation();
          String invokedResult = response.getResult();
          return invokedResult.replaceFirst(loc.getDest(), loc.getSrc());
        });
    }
    return asyncReturn(String.class);
  }

  /**
   * Asynchronously get an array of snapshottable directory listings.
   * This method checks the operation category and then invokes the
   * getSnapshottableDirListing method concurrently across all namespaces, merging
   * the results into a single array.
   *
   * @return Array of SnapshottableDirectoryStatus.
   * @throws IOException If an I/O error occurs.
   */
  @Override
  public SnapshottableDirectoryStatus[] getSnapshottableDirListing() throws IOException {
    rpcServer.checkOperation(NameNode.OperationCategory.READ);

    RemoteMethod method = new RemoteMethod("getSnapshottableDirListing");
    Set<FederationNamespaceInfo> nss = namenodeResolver.getNamespaces();
    rpcClient.invokeConcurrent(
            nss, method, true, false, SnapshottableDirectoryStatus[].class);
    asyncApply((ApplyFunction<Map<FederationNamespaceInfo, SnapshottableDirectoryStatus[]>,
        SnapshottableDirectoryStatus[]>)
        ret -> RouterRpcServer.merge(ret, SnapshottableDirectoryStatus.class));
    return asyncReturn(SnapshottableDirectoryStatus[].class);
  }

  /**
   * Asynchronously get an array of snapshot listings for the given snapshot root.
   * This method checks the operation category and then invokes the
   * getSnapshotListing method, either sequentially or concurrently based on the
   * configuration, and returns the merged results.
   *
   * @param snapshotRoot The root path of the snapshots.
   * @return Array of SnapshotStatus.
   * @throws IOException If an I/O error occurs.
   */
  @Override
  public SnapshotStatus[] getSnapshotListing(String snapshotRoot) throws IOException {
    rpcServer.checkOperation(NameNode.OperationCategory.READ);
    final List<RemoteLocation> locations =
        rpcServer.getLocationsForPath(snapshotRoot, true, false);
    RemoteMethod remoteMethod = new RemoteMethod("getSnapshotListing",
        new Class<?>[]{String.class},
        new RemoteParam());
    if (rpcServer.isInvokeConcurrent(snapshotRoot)) {
      rpcClient.invokeConcurrent(
          locations, remoteMethod, true, false, SnapshotStatus[].class);
      asyncApply((ApplyFunction<Map<RemoteLocation, SnapshotStatus[]>, SnapshotStatus[]>)
          ret -> {
          SnapshotStatus[] response = ret.values().iterator().next();
          String src = ret.keySet().iterator().next().getSrc();
          String dst = ret.keySet().iterator().next().getDest();
          for (SnapshotStatus s : response) {
            String mountPath = DFSUtil.bytes2String(s.getParentFullPath()).
                replaceFirst(src, dst);
            s.setParentFullPath(DFSUtil.string2Bytes(mountPath));
          }
          return response;
        });
    } else {
      rpcClient
          .invokeSequential(remoteMethod, locations, SnapshotStatus[].class,
              null);
      asyncApply((ApplyFunction<RemoteResult<RemoteLocation, SnapshotStatus[]>, SnapshotStatus[]>)
          invokedResponse -> {
          RemoteLocation loc = invokedResponse.getLocation();
          SnapshotStatus[] response = invokedResponse.getResult();
          for (SnapshotStatus s : response) {
            String mountPath = DFSUtil.bytes2String(s.getParentFullPath()).
                replaceFirst(loc.getDest(), loc.getSrc());
            s.setParentFullPath(DFSUtil.string2Bytes(mountPath));
          }
          return response;
        });
    }
    return asyncReturn(SnapshotStatus[].class);
  }

  /**
   * Asynchronously get a snapshot diff report for the given root and snapshot names.
   * This method checks the operation category and then invokes the
   * getSnapshotDiffReport method, either sequentially or concurrently based on the
   * configuration, and returns the result.
   *
   * @param snapshotRoot The root path of the snapshot.
   * @param earlierSnapshotName The name of the earlier snapshot.
   * @param laterSnapshotName The name of the later snapshot.
   * @return SnapshotDiffReport for the snapshots.
   * @throws IOException If an I/O error occurs.
   */
  @Override
  public SnapshotDiffReport getSnapshotDiffReport(
      String snapshotRoot, String earlierSnapshotName,
      String laterSnapshotName) throws IOException {
    rpcServer.checkOperation(NameNode.OperationCategory.READ);

    final List<RemoteLocation> locations =
        rpcServer.getLocationsForPath(snapshotRoot, true, false);
    RemoteMethod remoteMethod = new RemoteMethod("getSnapshotDiffReport",
        new Class<?>[] {String.class, String.class, String.class},
        new RemoteParam(), earlierSnapshotName, laterSnapshotName);

    if (rpcServer.isInvokeConcurrent(snapshotRoot)) {
      rpcClient.invokeConcurrent(
          locations, remoteMethod, true, false, SnapshotDiffReport.class);
      asyncApply((ApplyFunction<Map<RemoteLocation, SnapshotDiffReport>, SnapshotDiffReport>)
          ret -> ret.values().iterator().next());
      return asyncReturn(SnapshotDiffReport.class);
    } else {
      return rpcClient.invokeSequential(
          locations, remoteMethod, SnapshotDiffReport.class, null);
    }
  }

  /**
   * Asynchronously get a snapshot diff report listing for the given root and snapshot names.
   * This method checks the operation category and then invokes the
   * getSnapshotDiffReportListing method, either sequentially or concurrently based
   * on the configuration, and returns the result.
   *
   * @param snapshotRoot The root path of the snapshot.
   * @param earlierSnapshotName The name of the earlier snapshot.
   * @param laterSnapshotName The name of the later snapshot.
   * @param startPath The starting path for the diff report.
   * @param index The index for the diff report listing.
   * @return SnapshotDiffReportListing for the snapshots.
   * @throws IOException If an I/O error occurs.
   */
  @Override
  public SnapshotDiffReportListing getSnapshotDiffReportListing(
      String snapshotRoot, String earlierSnapshotName, String laterSnapshotName,
      byte[] startPath, int index) throws IOException {
    rpcServer.checkOperation(NameNode.OperationCategory.READ);

    final List<RemoteLocation> locations =
        rpcServer.getLocationsForPath(snapshotRoot, true, false);
    Class<?>[] params = new Class<?>[] {
        String.class, String.class, String.class,
        byte[].class, int.class};
    RemoteMethod remoteMethod = new RemoteMethod(
        "getSnapshotDiffReportListing", params,
        new RemoteParam(), earlierSnapshotName, laterSnapshotName,
        startPath, index);

    if (rpcServer.isInvokeConcurrent(snapshotRoot)) {
      rpcClient.invokeConcurrent(locations, remoteMethod, false, false,
              SnapshotDiffReportListing.class);
      asyncApply((ApplyFunction<Map<RemoteLocation, SnapshotDiffReportListing>,
          SnapshotDiffReportListing>) ret -> {
          Collection<SnapshotDiffReportListing> listings = ret.values();
          SnapshotDiffReportListing listing0 = listings.iterator().next();
          return listing0;
        });
      return asyncReturn(SnapshotDiffReportListing.class);
    } else {
      return rpcClient.invokeSequential(
          locations, remoteMethod, SnapshotDiffReportListing.class, null);
    }
  }
}
