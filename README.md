Elasticsearch Snapshot Cleanup Utilty
==================================

This utility cleans up snapshots stuck due to the [Issue 5968](https://github.com/elasticsearch/elasticsearch/issues/5958).

Usage:

- Untar the [tar.gz file](https://www.dropbox.com/s/lcmj244ztzv67ds/elasticsearch-snapshot-cleanup-1.0-SNAPSHOT.tar.gz) into a temporary directory on a machine that has access to the cluster.
- Modify `config/elasticsearch.yml` file with cluster connection settings.
- Execute the script `bin/cleanup`


License
-------

    This software is licensed under the Apache 2 license, quoted below.

    Copyright 2009-2014 Elasticsearch <http://www.elasticsearch.org>

    Licensed under the Apache License, Version 2.0 (the "License"); you may not
    use this file except in compliance with the License. You may obtain a copy of
    the License at

        http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
    License for the specific language governing permissions and limitations under
    the License.
