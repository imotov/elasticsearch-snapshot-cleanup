package org.motovs.elasticsearch.snapshots;

/**
 * Created by igor on 10/21/15.
 */

import org.elasticsearch.action.admin.cluster.repositories.put.PutRepositoryResponse;
import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ProcessedClusterStateUpdateTask;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.metadata.SnapshotId;
import org.elasticsearch.cluster.metadata.SnapshotMetaData;
import org.elasticsearch.common.collect.ImmutableList;
import org.elasticsearch.common.collect.ImmutableMap;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.node.Node;
import org.elasticsearch.snapshots.ConcurrentSnapshotExecutionException;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;

/**
 */
@ElasticsearchIntegrationTest.ClusterScope(scope = ElasticsearchIntegrationTest.Scope.TEST, numDataNodes = 0)
public class StuckCompletedSnapshotTest extends ElasticsearchIntegrationTest {

    @Test
    public void restorePersistentSettingsTest() throws Exception {
        logger.info("--> start 2 nodes");
        Settings nodeSettings = settingsBuilder()
                .build();
        internalCluster().startNode(nodeSettings);
        Client client = client();
        final String secondNode = internalCluster().startNode(nodeSettings);
        logger.info("--> wait for the second node to join the cluster");
        assertThat(client.admin().cluster().prepareHealth().setWaitForNodes("2").get().isTimedOut(), equalTo(false));

        logger.info("--> create repository");
        PutRepositoryResponse putRepositoryResponse = client.admin().cluster().preparePutRepository("test-repo")
                .setType("fs").setSettings(ImmutableSettings.settingsBuilder().put("location", newTempDir())).execute().actionGet();
        assertThat(putRepositoryResponse.isAcknowledged(), equalTo(true));

        logger.info("--> create stuck snapshot");
        final CountDownLatch latch = new CountDownLatch(1);
        final SnapshotId snapshotId = new SnapshotId("test-repo", "test-snap-1");
        String masterName = client().admin().cluster().prepareState().execute().actionGet().getState().nodes().masterNode().name();
        ClusterService masterClusterService = internalCluster().getInstance(ClusterService.class, masterName);
        masterClusterService.submitStateUpdateTask("create stuck snapshot", new ProcessedClusterStateUpdateTask() {
            @Override
            public void clusterStateProcessed(String s, ClusterState clusterState, ClusterState clusterState1) {
                latch.countDown();
            }

            @Override
            public ClusterState execute(ClusterState clusterState) throws Exception {
                MetaData metaData = clusterState.metaData();
                MetaData.Builder mdBuilder = MetaData.builder(clusterState.metaData());
                SnapshotMetaData snapshots = metaData.custom(SnapshotMetaData.TYPE);
                SnapshotMetaData.Entry newSnapshot;
                assert (snapshots == null || snapshots.entries().isEmpty());
                ImmutableList<String> indices = ImmutableList.of("test-idx");
                SnapshotMetaData.ShardSnapshotStatus shardSnapshotStatus = new SnapshotMetaData.ShardSnapshotStatus(clusterState.nodes().resolveNode(secondNode).id(), SnapshotMetaData.State.SUCCESS);
                ImmutableMap<ShardId, SnapshotMetaData.ShardSnapshotStatus> shards = ImmutableMap.of(new ShardId("test-idx", 0), shardSnapshotStatus);
                newSnapshot = new SnapshotMetaData.Entry(snapshotId, false, SnapshotMetaData.State.SUCCESS, indices, shards);
                snapshots = new SnapshotMetaData(newSnapshot);
                mdBuilder.putCustom(SnapshotMetaData.TYPE, snapshots);
                return ClusterState.builder(clusterState).metaData(mdBuilder).build();
            }

            @Override
            public void onFailure(String s, Throwable throwable) {
                latch.countDown();
            }
        });
        assertTrue(latch.await(1, TimeUnit.SECONDS));
        logger.info(client.admin().cluster().prepareState().get().getState().toString());

        logger.info("--> create an index that will have some unallocated shards");
        assertAcked(prepareCreate("test-idx-2", 2, settingsBuilder().put("number_of_shards", 6).put("number_of_replicas", 0)));
        ensureGreen();

        logger.info("--> indexing some data into test-idx-some");
        for (int i = 0; i < 100; i++) {
            index("test-idx-2", "doc", Integer.toString(i), "foo", "bar" + i);
        }
        refresh();
        assertThat(client().prepareCount("test-idx-2").get().getCount(), equalTo(100L));


        String clientNodeId = internalCluster().startNode(settingsBuilder().put(nodeSettings).put("node.client", true));
        Node clientNode = internalCluster().getInstance(Node.class, clientNodeId);
        AbortedSnapshotCleaner abortedSnapshotCleaner = new AbortedSnapshotCleaner(clientNode, nodeSettings);
        abortedSnapshotCleaner.cleanSnapshots();

        assertBusy(new Runnable() {
            @Override
            public void run() {
                // Try creating another snapshot
                try {
                    CreateSnapshotResponse createSnapshotResponse = client().admin().cluster().prepareCreateSnapshot("test-repo", "test-snap-2").setWaitForCompletion(true).setIndices("test-idx-2").execute().actionGet();
                    assertThat(createSnapshotResponse.getSnapshotInfo().totalShards(), equalTo(6));
                    assertThat(createSnapshotResponse.getSnapshotInfo().successfulShards(), equalTo(6));
                } catch (ConcurrentSnapshotExecutionException ex) {
                    logger.info("Snapshot is still running");
                    fail();
                }
            }
        });
    }
}
