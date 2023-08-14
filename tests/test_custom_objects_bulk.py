from test_custom_objects_rest import SalesforceCustomObjects


class SalesforceCustomObjectsBulk(SalesforceCustomObjects):
    """Test that all fields can be replicated for a stream that is a custom object (BULK API)"""

    salesforce_api = 'BULK'

    @staticmethod
    def name():
        return "tt_salesforce_custom_obj_bulk"

    @staticmethod
    def streams_to_selected_fields():
        """Note: if this is overridden you are not selecting all fields.
        Therefore this should rarely if ever be used for this test."""
        return {}

    def test_run(self):
        self.custom_objects_test()
