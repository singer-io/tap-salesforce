import os
import unittest
from functools import reduce

from base import SalesforceBaseTest


class SalesforceQuotas(SalesforceBaseTest):

    def name(self):
        return "tap_tester_salesforce_quotas"

    def get_properties(self):
        return {
            'start_date' : '2017-01-01T00:00:00Z',
            'instance_url': 'https://cs95.salesforce.com',
            'quota_percent_total': "-1",
            'select_fields_by_default': 'false',
            'api_type': 'BULK',
            'is_sandbox': 'true'
        }

    def test_run(self):
        conn_id = connections.ensure_connection(self)

        #run in check mode
        check_job_name = runner.run_check_mode(self, conn_id)

        #verify check  exit codes
        exit_status = menagerie.get_exit_status(conn_id, check_job_name)

        self.assertGreater(exit_status.get('discovery_exit_status'), 0, msg="exit status should be failed due to a quota issue")

