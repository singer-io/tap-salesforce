import unittest
from unittest import mock

from tap_salesforce import prefetch_bulk_jobs
from tap_salesforce.salesforce import Salesforce
from tap_salesforce.salesforce.bulk import Bulk
from tap_salesforce.salesforce.exceptions import TapSalesforceException

EXPORT_BUILD_SECONDS = 100   # how long Salesforce takes to build each export
STREAM_READ_SECONDS = 50     # how long the tap takes to read one finished export


def make_sf(prefetch=None, api_type="BULK"):
    return Salesforce(default_start_date="2020-01-01T00:00:00Z", api_type=api_type,
                      bulk_prefetch_streams=prefetch)


def make_entry(name, selected=True):
    return {
        "stream": name,
        "tap_stream_id": name,
        "metadata": [{"breadcrumb": [], "metadata": {"selected": selected, "replication-key": "SystemModstamp"}}],
    }


class FakeBulkServer:
    """Simulated Bulk API with a virtual clock: building an export takes EXPORT_BUILD_SECONDS from the moment
    its batch is submitted, and reading it takes STREAM_READ_SECONDS."""

    def __init__(self, fail_create_for=()):
        self.now = 0
        self.ready_at = {}
        self.created = []
        self.closed = []
        self.fail_create_for = set(fail_create_for)

    def create_job(self, catalog_entry, pk_chunking=False):  # pylint: disable=unused-argument
        if catalog_entry["stream"] in self.fail_create_for:
            raise TapSalesforceException("cannot create job for " + catalog_entry["stream"])
        job_id = "job-" + catalog_entry["stream"]
        self.created.append(job_id)
        return job_id

    def add_batch(self, catalog_entry, job_id, start_date, end_date=None, order_by_clause=True):  # pylint: disable=unused-argument
        self.ready_at[job_id] = self.now + EXPORT_BUILD_SECONDS
        return "batch-" + catalog_entry["stream"]

    def close_job(self, job_id):
        self.closed.append(job_id)

    def get_batch(self, job_id, batch_id):  # pylint: disable=unused-argument
        return {"state": "Completed" if self.now >= self.ready_at[job_id] else "InProgress"}

    def sleep(self, seconds):
        self.now += seconds

    def get_batch_results(self, job_id, batch_id, catalog_entry):  # pylint: disable=unused-argument
        self.now += STREAM_READ_SECONDS
        yield {"Id": catalog_entry["stream"]}


class BulkPrefetchTestCase(unittest.TestCase):

    def run_sync(self, sf, entries, server):
        """Replays what do_sync does for every stream: prefetch, then read the stream."""
        catalog = {"streams": entries}
        state = {}
        records = []
        with mock.patch.object(Bulk, "_create_job", side_effect=server.create_job), \
                mock.patch.object(Bulk, "_add_batch", side_effect=server.add_batch), \
                mock.patch.object(Bulk, "_close_job", side_effect=server.close_job), \
                mock.patch.object(Bulk, "_get_batch", side_effect=server.get_batch), \
                mock.patch.object(Bulk, "get_batch_results", side_effect=server.get_batch_results), \
                mock.patch.object(Bulk, "check_bulk_quota_usage"), \
                mock.patch("tap_salesforce.salesforce.bulk.time.sleep", side_effect=server.sleep):
            for index, entry in enumerate(entries):
                prefetch_bulk_jobs(sf, catalog, index, state)
                records.extend(r["Id"] for r in sf.query(entry, state))
        return records

    def test_prefetch_hides_export_wait_behind_streaming(self):
        entries = [make_entry(name) for name in ("A", "B", "C", "D")]

        sequential = FakeBulkServer()
        sequential_records = self.run_sync(make_sf(), entries, sequential)
        pipelined = FakeBulkServer()
        pipelined_records = self.run_sync(make_sf(prefetch=3), entries, pipelined)

        self.assertEqual(sequential_records, ["A", "B", "C", "D"])
        self.assertEqual(pipelined_records, sequential_records)
        self.assertEqual(sorted(pipelined.created), sorted(sequential.created))   # no extra jobs
        # every export is waited for one after another, versus one wait overlapped with the reads
        self.assertGreater(sequential.now, 4 * (EXPORT_BUILD_SECONDS + STREAM_READ_SECONDS) - 1)
        self.assertLess(pipelined.now, EXPORT_BUILD_SECONDS + 4 * STREAM_READ_SECONDS + 1)

    def test_default_does_not_prefetch(self):
        sf = make_sf()
        self.assertEqual(sf.bulk_prefetch_streams, 0)
        server = FakeBulkServer()
        with mock.patch.object(Bulk, "_create_job", side_effect=server.create_job) as create_job:
            prefetch_bulk_jobs(sf, {"streams": [make_entry("A")]}, 0, {})
        create_job.assert_not_called()

    def test_prefetch_only_looks_ahead_by_configured_streams(self):
        sf = make_sf(prefetch=2)
        entries = [make_entry(name) for name in ("A", "B", "C", "D")]
        server = FakeBulkServer()
        with mock.patch.object(Bulk, "_create_job", side_effect=server.create_job), \
                mock.patch.object(Bulk, "_add_batch", side_effect=server.add_batch), \
                mock.patch.object(Bulk, "_close_job", side_effect=server.close_job):
            prefetch_bulk_jobs(sf, {"streams": entries}, 0, {})
        self.assertEqual(sorted(sf.pending_bulk_jobs), ["A", "B", "C"])

    def test_prefetch_skips_unselected_blacklisted_and_resumable_streams(self):
        sf = make_sf(prefetch=5)
        entries = [make_entry("A"), make_entry("B", selected=False), make_entry("Vote"),
                   make_entry("Resumable"), make_entry("E")]
        state = {"bookmarks": {"Resumable": {"JobID": "750x", "BatchIDs": ["751x"]}}}
        server = FakeBulkServer()
        self.assertIn("Vote", sf.get_blacklisted_objects())
        with mock.patch.object(Bulk, "_create_job", side_effect=server.create_job), \
                mock.patch.object(Bulk, "_add_batch", side_effect=server.add_batch), \
                mock.patch.object(Bulk, "_close_job", side_effect=server.close_job):
            prefetch_bulk_jobs(sf, {"streams": entries}, 0, state)
        self.assertEqual(sorted(sf.pending_bulk_jobs), ["A", "E"])

    def test_failed_prefetch_is_not_fatal_and_is_not_retried(self):
        sf = make_sf(prefetch=3)
        entries = [make_entry("A"), make_entry("B"), make_entry("C")]
        server = FakeBulkServer(fail_create_for=["B"])
        with mock.patch.object(Bulk, "_create_job", side_effect=server.create_job) as create_job, \
                mock.patch.object(Bulk, "_add_batch", side_effect=server.add_batch), \
                mock.patch.object(Bulk, "_close_job", side_effect=server.close_job):
            prefetch_bulk_jobs(sf, {"streams": entries}, 0, {})
            prefetch_bulk_jobs(sf, {"streams": entries}, 1, {})
        self.assertEqual(sorted(sf.pending_bulk_jobs), ["A", "C"])
        self.assertEqual(sf.failed_bulk_prefetches, {"B"})
        self.assertEqual([c.args[0]["stream"] for c in create_job.call_args_list], ["A", "B", "C"])

    def test_failure_of_a_stream_whose_prefetch_failed_surfaces_normally(self):
        sf = make_sf(prefetch=1)
        entries = [make_entry("A"), make_entry("B")]
        server = FakeBulkServer(fail_create_for=["B"])
        with self.assertRaises(TapSalesforceException):
            self.run_sync(sf, entries, server)

    def test_failed_add_batch_closes_the_orphan_job(self):
        sf = make_sf(prefetch=1)
        server = FakeBulkServer()
        with mock.patch.object(Bulk, "_create_job", side_effect=server.create_job), \
                mock.patch.object(Bulk, "_add_batch", side_effect=TapSalesforceException("boom")), \
                mock.patch.object(Bulk, "_close_job", side_effect=server.close_job):
            Bulk(sf).prefetch_query(make_entry("A"), {})
        self.assertEqual(server.closed, ["job-A"])
        self.assertEqual(sf.pending_bulk_jobs, {})

    def test_prefetch_is_ignored_for_rest_api(self):
        sf = make_sf(prefetch=3, api_type="REST")
        with mock.patch.object(Bulk, "prefetch_query") as prefetch_query:
            prefetch_bulk_jobs(sf, {"streams": [make_entry("A")]}, 0, {})
        prefetch_query.assert_not_called()

    def test_config_value_validation(self):
        self.assertEqual(make_sf(prefetch="3").bulk_prefetch_streams, 3)
        self.assertEqual(make_sf(prefetch="").bulk_prefetch_streams, 0)
        for bad in (-1, 11, "many"):
            with self.assertRaises(TapSalesforceException):
                make_sf(prefetch=bad)


if __name__ == "__main__":
    unittest.main()
