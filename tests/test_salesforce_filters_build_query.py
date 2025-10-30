import unittest
from unittest.mock import patch
from tap_salesforce.salesforce import Salesforce

class TestSalesforce(unittest.TestCase):
    @patch('tap_salesforce.salesforce.Salesforce.describe')
    def test_build_query_string_with_custom_filters(self, mock_describe):
        # Mock the describe API response
        mock_describe.return_value = {
            "streams": [{
                "stream": "Account",
                "tap_stream_id": "Account",
                "schema": {
                    "properties": {
                        "IsDeleted": {"type": ["null", "boolean"]},
                        "Name": {"type": ["null", "string"]},
                        "Id": {"type": ["null", "string"]},
                        "OwnerId": {"type": ["null", "string"]},
                    }
                },
                "metadata": [
                    {
                        "breadcrumb": [],
                        "metadata": {
                            "selected": True,
                            "replication-method": "FULL_TABLE"
                        }
                    },
                    {
                        "breadcrumb": ["properties", "IsDeleted"],
                        "metadata": {"selected": True}
                    },
                    {
                        "breadcrumb": ["properties", "Name"],
                        "metadata": {"selected": True}
                    },
                    {
                        "breadcrumb": ["properties", "OwnerId"],
                        "metadata": {"selected": True}
                    }
                ]
            }]
        }

        sf = Salesforce(
            filters=[
                "OwnerId = '005XXXXXXXXXXXXXXX'",
                "IsDeleted = false"
            ],
            default_start_date="2022-05-02T00:00:00.000000Z"
        )

        catalog = sf.describe(["Account"])
        account_stream = catalog['streams'][0]

        query_string = sf._build_query_string(account_stream, "2023-01-01T00:00:00Z")

        expected_fields = ["IsDeleted", "Name", "OwnerId"]
        expected_query = (
            f"SELECT {','.join(expected_fields)} FROM Account "
            f"WHERE OwnerId = '005XXXXXXXXXXXXXXX' AND IsDeleted = false"
        )

        self.assertEqual(query_string, expected_query)

        # Test without any filters
        sf = Salesforce(
            default_start_date="2022-05-02T00:00:00.000000Z"
        )

        query_string = sf._build_query_string(account_stream, "2023-01-01T00:00:00Z")
        expected_query = f"SELECT {','.join(expected_fields)} FROM Account"

        self.assertEqual(query_string, expected_query)
