from test_custom_objects_rest import SalesforceCustomObjects


class SalesforceCustomObjectsBulk(SalesforceCustomObjects):
    """Test that all fields can be replicated for a stream that is a custom object (BULK API)"""

    salesforce_api = 'BULK'

    @staticmethod
    def name():
        return "tt_salesforce_custom_obj_bulk"

    def test_run(self):
        self.custom_objects_test()
