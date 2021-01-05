import os
import unittest
from functools import reduce

import tap_tester.connections as connections
import tap_tester.menagerie   as menagerie
import tap_tester.runner      as runner


class SalesforceQuotas(unittest.TestCase):

    def name(self):
        return "tap_tester_salesforce_quotas"

    def tap_name(self):
        return "tap-salesforce"


    def setUp(self):
        missing_envs = [x for x in [os.getenv('TAP_SALESFORCE_CLIENT_ID'),
                                    os.getenv('TAP_SALESFORCE_CLIENT_SECRET'),
                                    os.getenv('TAP_SALESFORCE_REFRESH_TOKEN')] if x == None]
        if len(missing_envs) != 0:
            raise Exception("set TAP_SALESFORCE_CLIENT_ID, TAP_SALESFORCE_CLIENT_SECRET, TAP_SALESFORCE_REFRESH_TOKEN")

    def get_type(self):
        return "platform.salesforce"

    def get_credentials(self):
        return {'refresh_token': os.getenv('TAP_SALESFORCE_REFRESH_TOKEN'),
                'client_id': os.getenv('TAP_SALESFORCE_CLIENT_ID'),
                'client_secret': os.getenv('TAP_SALESFORCE_CLIENT_SECRET')}

    def get_properties(self):
        return {
            'start_date' : '2017-01-01 00:00:00',
            'instance_url': 'https://cs95.salesforce.com',
            'quota_percent_total': "-1",
            'select_fields_by_default': 'false',
            'api_type': 'bulk',
            'is_sandbox': 'true'
        }

    def test_run(self):
        conn_id = connections.ensure_connection(self)

        #run in check mode
        check_job_name = runner.run_check_mode(self, conn_id)

        #verify check  exit codes
        exit_status = menagerie.get_exit_status(conn_id, check_job_name)

        self.assertGreater(exit_status.get('discovery_exit_status'), 0, msg="exit status should be failed due to a quota issue")

