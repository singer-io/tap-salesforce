"""
Setup expectations for test sub classes
Run discovery for as a prerequisite for most tests
"""
import unittest
import copy
import os
from datetime import timedelta
from datetime import datetime as dt
from datetime import timezone as tz

from tap_tester import connections, menagerie, runner


class SalesforceBaseTest(unittest.TestCase):
    """
    Setup expectations for test sub classes
    Run discovery for as a prerequisite for most tests
    """
    AUTOMATIC_FIELDS = "automatic"
    REPLICATION_KEYS = "valid-replication-keys"
    PRIMARY_KEYS = "table-key-properties"
    FOREIGN_KEYS = "table-foreign-key-properties"
    REPLICATION_METHOD = "forced-replication-method"
    API_LIMIT = "max-row-limit"
    INCREMENTAL = "INCREMENTAL"
    FULL_TABLE = "FULL_TABLE"
    START_DATE_FORMAT = "%Y-%m-%dT00:00:00Z"

    @staticmethod
    def tap_name():
        """The name of the tap"""
        return "tap-salesforce"

    @staticmethod
    def get_type():
        """the expected url route ending"""
        return "platform.salesforce"

    def get_properties(self, original: bool = True):
        """Configuration properties required for the tap."""
        return {
            'start_date' : '2017-01-01T00:00:00Z',
            'instance_url': 'https://cs95.salesforce.com',
            'select_fields_by_default': 'true',
            'api_type': "BULK",
            'is_sandbox': 'true'
        }

        if original:
            return return_value

        # This test needs the new connections start date to be larger than the default
        assert self.start_date > return_value["start_date"]

        return_value["start_date"] = self.start_date
        return return_value

    @staticmethod
    def get_credentials():
        """Authentication information for the test account"""
        return {'refresh_token': os.getenv('TAP_SALESFORCE_REFRESH_TOKEN'),
                'client_id': os.getenv('TAP_SALESFORCE_CLIENT_ID'),
                'client_secret': os.getenv('TAP_SALESFORCE_CLIENT_SECRET')}

    def expected_metadata(self):
        """The expected streams and metadata about the streams"""
        return {
            'Account': {
                self.PRIMARY_KEYS: {"Id"},
                self.REPLICATION_METHOD: self.INCREMENTAL,
                self.REPLICATION_KEYS: {"LastModifiedTime"}
            },
            'Task': {
                # self.PRIMARY_KEYS: {"Id"},
                # self.REPLICATION_METHOD: self.INCREMENTAL,
                # self.REPLICATION_KEYS: {"LastModifiedTime"}
            },
            'Attachment': {
                # self.PRIMARY_KEYS: {"Id"},
                # self.REPLICATION_METHOD: self.INCREMENTAL,
                # self.REPLICATION_KEYS: {"LastModifiedTime"}
            },
        }

    def expected_streams(self):
        """A set of expected stream names"""
        return set(self.expected_metadata().keys())

    def child_streams(self):
        """
        Return a set of streams that are child streams
        based on having foreign key metadata
        """
        return {stream for stream, metadata in self.expected_metadata().items()
                if metadata.get(self.FOREIGN_KEYS)}

    def expected_primary_keys(self):
        """
        return a dictionary with key of table name
        and value as a set of primary key fields
        """
        return {table: properties.get(self.PRIMARY_KEYS, set())
                for table, properties
                in self.expected_metadata().items()}

    def expected_replication_keys(self):
        """
        return a dictionary with key of table name
        and value as a set of replication key fields
        """
        return {table: properties.get(self.REPLICATION_KEYS, set())
                for table, properties
                in self.expected_metadata().items()}

    def expected_foreign_keys(self):
        """
        return a dictionary with key of table name
        and value as a set of foreign key fields
        """
        return {table: properties.get(self.FOREIGN_KEYS, set())
                for table, properties
                in self.expected_metadata().items()}

    def expected_replication_method(self):
        """return a dictionary with key of table name nd value of replication method"""
        return {table: properties.get(self.REPLICATION_METHOD, None)
                for table, properties
                in self.expected_metadata().items()}

    def setUp(self):
        """Verify that you have set the prerequisites to run the tap (creds, etc.)"""
        missing_envs = [x for x in ['TAP_SALESFORCE_CLIENT_ID',
                                    'TAP_SALESFORCE_CLIENT_SECRET',
                                    'TAP_SALESFORCE_REFRESH_TOKEN']
                        if os.getenv(x) is None]

        if missing_envs:
            raise Exception("set environment variables")

    #########################
    #   Helper Methods      #
    #########################

    def create_connection(self, original_properties: bool = True):
        """Create a new connection with the test name"""
        # Create the connection
        conn_id = connections.ensure_connection(self, original_properties)

        # Run a check job using orchestrator (discovery)
        check_job_name = runner.run_check_mode(self, conn_id)

        # Assert that the check job succeeded
        exit_status = menagerie.get_exit_status(conn_id, check_job_name)
        menagerie.verify_check_exit_status(self, exit_status, check_job_name)
        return conn_id

    def run_and_verify_check_mode(self, conn_id):
        """
        Run the tap in check mode and verify it succeeds.
        This should be ran prior to field selection and initial sync.

        Return the connection id and found catalogs from menagerie.
        """
        # run in check mode
        check_job_name = runner.run_check_mode(self, conn_id)

        # verify check exit codes
        exit_status = menagerie.get_exit_status(conn_id, check_job_name)
        menagerie.verify_check_exit_status(self, exit_status, check_job_name)

        found_catalogs = menagerie.get_catalogs(conn_id)
        self.assertGreater(len(found_catalogs), 0, msg="unable to locate schemas for connection {}".format(conn_id))

        found_catalog_names = set(map(lambda c: c['tap_stream_id'], found_catalogs))
        self.assertSetEqual(self.expected_streams(), found_catalog_names, msg="discovered schemas do not match")
        print("discovered schemas are OK")

        return found_catalogs

    def run_and_verify_sync(self, conn_id):
        """
        Run a sync job and make sure it exited properly.
        Return a dictionary with keys of streams synced
        and values of records synced for each stream
        """
        # Run a sync job using orchestrator
        sync_job_name = runner.run_sync_mode(self, conn_id)

        # Verify tap and target exit codes
        exit_status = menagerie.get_exit_status(conn_id, sync_job_name)
        menagerie.verify_sync_exit_status(self, exit_status, sync_job_name)

        # Verify actual rows were synced
        sync_record_count = runner.examine_target_output_file(
            self, conn_id, self.expected_streams(), self.expected_primary_keys())
        self.assertGreater(
            sum(sync_record_count.values()), 0,
            msg="failed to replicate any data: {}".format(sync_record_count)
        )
        print("total replicated row count: {}".format(sum(sync_record_count.values())))

        return sync_record_count

    @staticmethod
    def local_to_utc(date: dt):
        """Convert a datetime with timezone information to utc"""
        utc = dt(date.year, date.month, date.day, date.hour, date.minute,
                 date.second, date.microsecond, tz.utc)

        if date.tzinfo and hasattr(date.tzinfo, "_offset"):
            utc += date.tzinfo._offset

        return utc

    def max_bookmarks_by_stream(self, sync_records):
        """
        Return the maximum value for the replication key for each stream
        which is the bookmark expected value.

        Comparisons are based on the class of the bookmark value. Dates will be
        string compared which works for ISO date-time strings
        """
        max_bookmarks = {}
        for stream, batch in sync_records.items():

            upsert_messages = [m for m in batch.get('messages') if m['action'] == 'upsert']
            stream_bookmark_key = self.expected_replication_keys().get(stream, set())
            assert len(stream_bookmark_key) == 1  # There shouldn't be a compound replication key
            stream_bookmark_key = stream_bookmark_key.pop()

            bk_values = [message["data"].get(stream_bookmark_key) for message in upsert_messages]
            max_bookmarks[stream] = {stream_bookmark_key: None}
            for bk_value in bk_values:
                if bk_value is None:
                    continue

                if max_bookmarks[stream][stream_bookmark_key] is None:
                    max_bookmarks[stream][stream_bookmark_key] = bk_value

                if bk_value > max_bookmarks[stream][stream_bookmark_key]:
                    max_bookmarks[stream][stream_bookmark_key] = bk_value
        return max_bookmarks

    def min_bookmarks_by_stream(self, sync_records):
        """Return the minimum value for the replication key for each stream"""
        min_bookmarks = {}
        for stream, batch in sync_records.items():

            upsert_messages = [m for m in batch.get('messages') if m['action'] == 'upsert']
            stream_bookmark_key = self.expected_replication_keys().get(stream, set())
            assert len(stream_bookmark_key) == 1  # There shouldn't be a compound replication key
            (stream_bookmark_key, ) = stream_bookmark_key

            bk_values = [message["data"].get(stream_bookmark_key) for message in upsert_messages]
            min_bookmarks[stream] = {stream_bookmark_key: None}
            for bk_value in bk_values:
                if bk_value is None:
                    continue

                if min_bookmarks[stream][stream_bookmark_key] is None:
                    min_bookmarks[stream][stream_bookmark_key] = bk_value

                if bk_value < min_bookmarks[stream][stream_bookmark_key]:
                    min_bookmarks[stream][stream_bookmark_key] = bk_value
        return min_bookmarks

    def perform_and_verify_table_and_field_selection(self,
                                                     conn_id,
                                                     test_catalogs,
                                                     select_all_fields=True):
        """
        Perform table and field selection based off of the streams to select
        set and field selection parameters.

        Verify this results in the expected streams selected and all or no
        fields selected for those streams.
        """

        # Select all available fields or select no fields from all testable streams
        self.select_all_streams_and_fields(
            conn_id=conn_id, catalogs=test_catalogs, select_all_fields=select_all_fields
        )

        catalogs = menagerie.get_catalogs(conn_id)

        # Ensure our selection affects the catalog
        expected_selected = [tc.get('tap_stream_id') for tc in test_catalogs]
        for cat in catalogs:
            catalog_entry = menagerie.get_annotated_schema(conn_id, cat['stream_id'])

            # Verify all testable streams are selected
            selected = catalog_entry.get('annotated-schema').get('selected')
            print("Validating selection on {}: {}".format(cat['stream_name'], selected))
            if cat['stream_name'] not in expected_selected:
                self.assertFalse(selected, msg="Stream selected, but not testable.")
                continue # Skip remaining assertions if we aren't selecting this stream
            self.assertTrue(selected, msg="Stream not selected.")

            if select_all_fields:
                # Verify all fields within each selected stream are selected
                for field, field_props in catalog_entry.get('annotated-schema').get('properties').items():
                    field_selected = field_props.get('selected')
                    print("\tValidating selection on {}.{}: {}".format(
                        cat['stream_name'], field, field_selected))
                    self.assertTrue(field_selected, msg="Field not selected.")
            else:
                # Verify only automatic fields are selected
                expected_automatic_fields = self.expected_automatic_fields().get(cat['tap_stream_id'])
                selected_fields = self.get_selected_fields_from_metadata(catalog_entry['metadata'])
                self.assertEqual(expected_automatic_fields, selected_fields)

    @staticmethod
    def get_selected_fields_from_metadata(metadata):
        selected_fields = set()
        for field in metadata:
            is_field_metadata = len(field['breadcrumb']) > 1
            if field['metadata'].get('inclusion') is None and is_field_metadata:  # BUG_SRCE-4313 remove when addressed
                print("Error {} has no inclusion key in metadata".format(field))  # BUG_SRCE-4313 remove when addressed
                continue  # BUG_SRCE-4313 remove when addressed
            inclusion_automatic_or_selected = (
                field['metadata']['selected'] is True or \
                field['metadata']['inclusion'] == 'automatic'
            )
            if is_field_metadata and inclusion_automatic_or_selected:
                selected_fields.add(field['breadcrumb'][1])
        return selected_fields


    @staticmethod
    def select_all_streams_and_fields(conn_id, catalogs, select_all_fields: bool = True):
        """Select all streams and all fields within streams"""
        for catalog in catalogs:
            schema = menagerie.get_annotated_schema(conn_id, catalog['stream_id'])

            non_selected_properties = []
            if not select_all_fields:
                # get a list of all properties so that none are selected
                non_selected_properties = schema.get('annotated-schema', {}).get(
                    'properties', {}).keys()

            connections.select_catalog_and_fields_via_metadata(
                conn_id, catalog, schema, [], non_selected_properties)

    @staticmethod
    def parse_date(date_value):
        """
        Pass in string-formatted-datetime, parse the value, and return it as an unformatted datetime object.
        """
        try:
            date_stripped = dt.strptime(date_value, "%Y-%m-%dT%H:%M:%S.%fZ")
            return date_stripped
        except ValueError:
            try:
                date_stripped = dt.strptime(date_value, "%Y-%m-%dT%H:%M:%SZ")
                return date_stripped
            except ValueError:
                try:
                    date_stripped = dt.strptime(date_value, "%Y-%m-%dT%H:%M:%S.%f+00:00")
                    return date_stripped
                except ValueError:
                    try:
                        date_stripped = dt.strptime(date_value, "%Y-%m-%dT%H:%M:%S+00:00")
                        return date_stripped
                    except ValueError:
                        raise NotImplementedError("We are not accounting for dates of this format: {}".format(date_value))

    ##########################################################################
    ### Tap Specific Methods
    ##########################################################################

    def get_account_id_with_report_data(self):
        """
        Of the 3 bing accounts only the Stitch account has data for report streams.

        return the id of the Stitch account.
        """
        return '163078754'

    @staticmethod
    def select_specific_fields(conn_id, catalogs, select_all_fields: bool = True, specific_fields: dict = {}):
        """Select all streams and all fields within streams"""
        for catalog in catalogs:
            schema = menagerie.get_annotated_schema(conn_id, catalog['stream_id'])

            non_selected_properties = []
            if not select_all_fields:
                # get a list of all properties and remove measuer fields
                non_selected_properties = set(schema.get('annotated-schema', {}).get(
                    'properties', {}).keys())
                spec_fields = specific_fields.get(catalog['stream_name'], set())
                non_selected_properties_adjusted = non_selected_properties.difference(spec_fields)

            connections.select_catalog_and_fields_via_metadata(
                conn_id, catalog, schema, [], non_selected_properties_adjusted)

    def perform_and_verify_adjusted_selection(self,
                                              conn_id,
                                              test_catalogs,
                                              select_all_fields,
                                              specific_fields):
        """
        Perform table and field selection based off of the streams to select
        set and field selection parameters.

        Verify this results in the expected streams selected and all or no
        fields selected for those streams.
        """

        # Select specifc fields from all testable streams
        self.select_specific_fields(conn_id=conn_id, catalogs=test_catalogs,
                                    select_all_fields=select_all_fields,
                                    specific_fields=specific_fields)

        catalogs = menagerie.get_catalogs(conn_id)

        # Ensure our selection affects the catalog
        expected_selected = [tc.get('tap_stream_id') for tc in test_catalogs]
        for cat in catalogs:
            with self.subTest(cat=cat):
                catalog_entry = menagerie.get_annotated_schema(conn_id, cat['stream_id'])

                # Verify intended streams are selected
                selected = catalog_entry.get('annotated-schema').get('selected')
                print("Validating selection on {}: {}".format(cat['tap_stream_id'], selected))
                if cat['stream_name'] not in expected_selected:
                    continue  # Skip remaining assertions if we aren't selecting this stream

                self.assertTrue(selected, msg="Stream not selected.")

                if select_all_fields:
                    # Verify all fields within each selected stream are selected
                    for field, field_props in catalog_entry.get('annotated-schema').get('properties').items():
                        field_selected = field_props.get('selected')
                        print("\tValidating selection on {}.{}: {}".format(
                            cat['stream_name'], field, field_selected))
                        self.assertTrue(field_selected, msg="Field not selected.")
                else:
                    for field, field_props in catalog_entry.get('annotated-schema').get('properties').items():
                        field_selected = field_props.get('selected')
                        if field_selected:
                            print("\tValidating selection on {}.{}: {}".format(
                                cat['stream_name'], field, field_selected))

                    # Verify only automatic fields are selected
                    # Uncomment lines below to reporduce BUG_SRCE-4313 from automatic fields tests
                    # expected_fields = self.expected_automatic_fields().get(cat['tap_stream_id']) | \
                    #     specific_fields.get(cat['tap_stream_id'], set())
                    # if cat['tap_stream_id'].endswith('_report'):
                    #     expected_fields.update({'_sdc_report_datetime'})  # tap applies sdc as pk for all reports
                    # selected_fields = self.get_selected_fields_from_metadata(catalog_entry['metadata'])
                    # self.assertSetEqual(expected_fields, selected_fields)

    def expected_automatic_fields(self):
        auto_fields = {}
        for k, v in self.expected_metadata().items():
            auto_fields[k] = v.get(self.PRIMARY_KEYS, set()) | v.get(self.REPLICATION_KEYS, set()) \
                | v.get(self.FOREIGN_KEYS, set()) | v.get(self.REQUIRED_KEYS, set())
        return auto_fields

    def expected_required_fields(self):
        return {table: properties.get(self.REQUIRED_KEYS, set())
                for table, properties
                in self.expected_metadata().items()}

    ##########################################################################
    ### Exclusions Handling
    ##########################################################################

    def get_all_attributes(self):
        """A dictionary of reports to Attributes"""
        return {
            'campaign_performance_report': {
                'BidMatchType', 'BudgetAssociationStatus', 'BudgetName', 'BudgetStatus',
                'DeviceOS', 'Goal', 'GoalType', 'TopVsOther'
            },
            'ad_group_performance_report': {
                'BidMatchType','DeviceOS','Goal','GoalType','TopVsOther'
            },
        }

    def get_all_statistics(self):
        """A dictionary of reports to ImpressionSharePerformanceStatistics"""
        return {
            'campaign_performance_report': {
                'AbsoluteTopImpressionRatePercent',
                'AbsoluteTopImpressionShareLostToBudgetPercent',
                'AbsoluteTopImpressionShareLostToRankPercent',
                'AbsoluteTopImpressionSharePercent',
                'AudienceImpressionLostToBudgetPercent',
                'AudienceImpressionLostToRankPercent',
                'AudienceImpressionSharePercent',
                'ClickSharePercent',
                'ExactMatchImpressionSharePercent',
                'ImpressionLostToBudgetPercent',
                'ImpressionLostToRankAggPercent',
                'ImpressionSharePercent',
                'RelativeCtr',
                'TopImpressionRatePercent',
                'TopImpressionShareLostToBudgetPercent',
                'TopImpressionShareLostToRankPercent',
                'TopImpressionSharePercent'
            },
            # NOTE: The fields marked below as Undocumented are not documented in this exclusion
            #       group by bing, but have been confirmed manually to belong to this group.
            #       Selecting one results in an InternalError with the Restricted Columns message.
            'ad_group_performance_report': {
                'AbsoluteTopImpressionRatePercent',
                'AbsoluteTopImpressionShareLostToBudgetPercent',
                'AbsoluteTopImpressionShareLostToRankPercent',
                'AbsoluteTopImpressionSharePercent',
                'AudienceImpressionLostToBudgetPercent',
                'AudienceImpressionLostToRankPercent',
                'AudienceImpressionSharePercent',
                'ClickSharePercent',
                'ExactMatchImpressionSharePercent',
                'ImpressionLostToBudgetPercent',
                'ImpressionLostToRankAggPercent',
                'ImpressionSharePercent',
                'RelativeCtr',
                'TopImpressionRatePercent',  # Undocumented
                'TopImpressionShareLostToBudgetPercent',  # Undocumented
                'TopImpressionShareLostToRankPercent',  # Undocumented
                'TopImpressionSharePercent',  # Undocumented
            },
        }

    def get_uncategorized_exclusions(self):
        """
        Some exclusions are uncategorized and exlcude specific streams.

        From bing docs:
          In addition, if you include any of the AudienceImpressionLostToBudgetPercent,
          AudienceImpressionLostToRankPercent, AudienceImpressionSharePercent, or RelativeCtr
          columns, then you must exclude the CustomerId, CustomerName, and DeliveredMatchType
          attribute columns, and vice versa.

        We will not select these fields in our test.
        """
        uncategorized_exclusion_set = {
            'AudienceImpressionLostToBudgetPercent',
            'AudienceImpressionLostToRankPercent',
            'AudienceImpressionSharePercent',
            'RelativeCtr'
        }

        return {
            'campaign_performance_report': uncategorized_exclusion_set,
            'ad_group_performance_report': uncategorized_exclusion_set,
        }

    def get_all_fields(self):
        return {
            'campaign_performance_report': {
                'BudgetName',
                'TopImpressionRatePercent',
                'HistoricalQualityScore',
                'ImpressionLostToBudgetPercent',
                'LowQualityClicksPercent',
                'AbsoluteTopImpressionShareLostToBudgetPercent',
                'AdDistribution',
                'Assists',
                'Ctr',
                'HistoricalAdRelevance',
                'TopVsOther',
                'ExactMatchImpressionSharePercent',
                'CustomerName',
                'Goal',
                'QualityScore',
                'CurrencyCode',
                'CostPerAssist',
                'BidMatchType',
                'RevenuePerConversion',
                'DeviceType',
                'BaseCampaignId',
                'AllRevenuePerConversion',
                'CampaignStatus',
                'AccountNumber',
                'AbsoluteTopImpressionShareLostToRankPercent',
                'Spend',
                'PhoneCalls',
                'ConversionRate',
                'BudgetStatus',
                'RelativeCtr',
                'LowQualityGeneralClicks',
                'AudienceImpressionLostToBudgetPercent',
                'ImpressionLostToRankAggPercent',
                'TopImpressionShareLostToRankPercent',
                'LowQualityConversionRate',
                'CustomerId',
                'AccountId',
                'AudienceImpressionSharePercent',
                'AbsoluteTopImpressionRatePercent',
                'HistoricalLandingPageExperience',
                'AllReturnOnAdSpend',
                'ReturnOnAdSpend',
                'GoalType',
                'CampaignName',
                'LowQualityImpressionsPercent',
                'Ptr',
                'DeliveredMatchType',
                'AllConversions',
                'ClickSharePercent',
                'TopImpressionShareLostToBudgetPercent',
                'BudgetAssociationStatus',
                'LandingPageExperience',
                'CustomParameters',
                'Conversions',
                'ImpressionSharePercent',
                'PhoneImpressions',
                'AdRelevance',
                'AllRevenue',
                'TrackingTemplate',
                'Revenue',
                'CostPerConversion',
                'AveragePosition',
                'Clicks',
                'LowQualitySophisticatedClicks',
                'TimePeriod',
                'AllConversionRate',
                'CampaignLabels',
                'Impressions',
                'FinalUrlSuffix',
                'LowQualityConversions',
                'LowQualityClicks',
                'RevenuePerAssist',
                'HistoricalExpectedCtr',
                'AccountStatus',
                'Network',
                'ExpectedCtr',
                'DeviceOS',
                'CampaignType',
                'LowQualityImpressions',
                'TopImpressionSharePercent',
                'AbsoluteTopImpressionSharePercent',
                'ViewThroughConversions',
                'AudienceImpressionLostToRankPercent',
                'AverageCpc',
                'AccountName',
                'CampaignId',
                'AllCostPerConversion'
            },
            'ad_group_performance_report': {
                'HistoricalExpectedCtr',
                'DeliveredMatchType',
                'AdGroupId',
                'AccountId',
                'AbsoluteTopImpressionShareLostToRankPercent',
                'CampaignType',
                'AdGroupType',
                'Goal',
                'FinalUrlSuffix',
                'QualityScore',
                'AudienceImpressionSharePercent',
                'CostPerConversion',
                'AllConversionRate',
                'ConversionRate',
                'DeviceType',
                'Language',
                'AdRelevance',
                'DeviceOS',
                'ClickSharePercent',
                'CustomerId',
                'Assists',
                'AbsoluteTopImpressionShareLostToBudgetPercent',
                'AdGroupLabels',
                'Spend',
                'PhoneImpressions',
                'AllRevenue',
                'AdGroupName',
                'CurrencyCode',
                'ExpectedCtr',
                'TimePeriod',
                'AccountNumber',
                'Revenue',
                'AdDistribution',
                'AudienceImpressionLostToRankPercent',
                'BidMatchType',
                'ReturnOnAdSpend',
                'TopImpressionShareLostToRankPercent',
                'PhoneCalls',
                'CustomParameters',
                'ViewThroughConversions',
                'CampaignName',
                'ImpressionLostToRankAggPercent',
                'CampaignStatus',
                'Status',
                'RevenuePerAssist',
                'BaseCampaignId',
                'ImpressionLostToBudgetPercent',
                'Impressions',
                'RevenuePerConversion',
                'ExactMatchImpressionSharePercent',
                'Conversions',
                'LandingPageExperience',
                'TopVsOther',
                'ImpressionSharePercent',
                'Ctr',
                'TrackingTemplate',
                'TopImpressionShareLostToBudgetPercent',
                'CostPerAssist',
                'GoalType',
                'AllReturnOnAdSpend',
                'HistoricalQualityScore',
                'Clicks',
                'AllConversions',
                'AllCostPerConversion',
                'Network',
                'HistoricalLandingPageExperience',
                'RelativeCtr',
                'TopImpressionRatePercent',
                'HistoricalAdRelevance',
                'AveragePosition',
                'AccountName',
                'AccountStatus',
                'CustomerName',
                'Ptr',
                'AudienceImpressionLostToBudgetPercent',
                'AverageCpc',
                'TopImpressionSharePercent',
                'AbsoluteTopImpressionSharePercent',
                'AllRevenuePerConversion',
                'CampaignId',
                'AbsoluteTopImpressionRatePercent'
            }
        }

    def max_replication_key_values_by_stream(self, sync_records):
        """
        Return the maximum value for the replication key for each stream
        which is normally the expected value for a bookmark. But in the case of reports,
        the bookmark will be the day the sync runs.

        Comparisons are based on the class of the bookmark value. Dates will be
        string compared which works for ISO date-time strings
        """
        max_bookmarks = dict()
        datetime_minimum_formatted = dt.strftime(dt.min, self.BOOKMARK_COMPARISON_FORMAT)
        account_to_test = self.get_account_id_with_report_data()

        for stream, batch in sync_records.items():

            if self.expected_replication_method().get(stream) != self.INCREMENTAL:
                continue  # skip full table streams

            stream_replication_key = self.expected_replication_keys().get(stream, set()).pop()

            # use bookmark key instead of replication key and drop the prefixed account id if necessary
            if self.is_report(stream):
                stream_bookmark_key = 'date'
                prefix = account_to_test + '_'

            elif stream == 'accounts':
                stream_bookmark_key = 'last_record'
                prefix = ''

            else:
                stream_bookmark_key = stream_replication_key
                prefix = ''

            prefixed_stream = prefix + stream

            # we don't care about activate version meessages
            upsert_messages = [m for m in batch.get('messages') if m['action'] == 'upsert']
            bookmark_values = [message["data"].get(stream_replication_key)
                               for message in upsert_messages]
            max_bookmarks[prefixed_stream] = {stream_bookmark_key: None}

            for bookmark_value in bookmark_values:
                if bookmark_value is None:
                    continue

                if bookmark_value > max_bookmarks[prefixed_stream].get(stream_bookmark_key, datetime_minimum_formatted):
                    max_bookmarks[prefixed_stream][stream_bookmark_key] = bookkmark_value

        return max_bookmarks

    def timedelta_formatted(self, dtime, days=0):
        try:
            date_stripped = dt.strptime(dtime, self.START_DATE_FORMAT)
            return_date = date_stripped + timedelta(days=days)

            return dt.strftime(return_date, self.START_DATE_FORMAT)

        except ValueError:
            try:
                date_stripped = dt.strptime(dtime, self.BOOKMARK_COMPARISON_FORMAT)
                return_date = date_stripped + timedelta(days=days)

                return dt.strftime(return_date, self.BOOKMARK_COMPARISON_FORMAT)

            except ValueError:
                return Exception("Datetime object is not of the format: {}".format(self.START_DATE_FORMAT))

    def expected_streams_with_exclusions(self):
        return {'campaign_performance_report', 'ad_group_performance_report'}

    def get_as_many_fields_as_possbible_excluding_statistics(self, stream):
        stats = self.get_all_statistics().get(stream, set())
        all_fields = self.get_all_fields().get(stream, set())
        uncategorized = self.get_uncategorized_exclusions().get(stream, set())

        return all_fields.difference(stats).difference(uncategorized)

    def get_as_many_fields_as_possbible_excluding_attributes(self, stream):
        attributes = self.get_all_attributes().get(stream, set())
        all_fields = self.get_all_fields().get(stream, set())
        uncategorized = self.get_uncategorized_exclusions().get(stream, set())

        return all_fields.difference(attributes).difference(uncategorized)
