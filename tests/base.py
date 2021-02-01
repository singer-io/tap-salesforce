"""
Setup expectations for test sub classes
Run discovery for as a prerequisite for most tests
"""
import unittest
import os
from datetime import timedelta
from datetime import datetime as dt

import singer
from tap_tester import connections, menagerie, runner


class SalesforceBaseTest(unittest.TestCase):
    """
    Setup expectations for test sub classes.
    Metadata describing streams.

    A bunch of shared methods that are used in tap-tester tests.
    Shared tap-specific methods (as needed).

    FUTURE WORK
      custom fields | https://stitchdata.atlassian.net/browse/SRCE-4813
      bookmarks     | https://stitchdata.atlassian.net/browse/SRCE-4814
      REST API      | https://stitchdata.atlassian.net/browse/SRCE-4815
      timing        | https://stitchdata.atlassian.net/browse/SRCE-4816
      more data     | https://stitchdata.atlassian.net/browse/SRCE-4824
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
    BOOKMARK_COMPARISON_FORMAT = "%Y-%m-%dT00:00:00.000000Z"

    LOGGER = singer.get_logger()

    start_date = ""

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
        return_value = {
            'start_date': '2017-01-01T00:00:00Z',
            'instance_url': 'https://cs95.salesforce.com',
            'select_fields_by_default': 'true',
            'quota_percent_total': "80",
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
        default = {
            self.PRIMARY_KEYS: {"Id"},
            self.REPLICATION_METHOD: self.INCREMENTAL,
            self.REPLICATION_KEYS: {"SystemModstamp"}
        }
        default_full = {
            self.PRIMARY_KEYS: {"Id"},
            self.REPLICATION_METHOD: self.FULL_TABLE,
        }

        return {
            'Account': default,
            'AccountContactRelation': default,
            'AccountContactRole': default,
            'AccountFeed': default,
            'AccountHistory': {
                self.REPLICATION_KEYS: {'CreatedDate'},
                self.PRIMARY_KEYS: {'Id'},
                self.REPLICATION_METHOD: self.INCREMENTAL,
            },
            'AccountPartner': default,
            'AccountShare': {
                self.PRIMARY_KEYS: {'Id'},
                self.REPLICATION_KEYS: {'LastModifiedDate'},
                self.REPLICATION_METHOD: self.INCREMENTAL,
            },
            'ActionLinkGroupTemplate': default,
            'ActionLinkTemplate': default,
            'AdditionalNumber': default,
            'ApexClass': default,
            'ApexComponent': default,
            'ApexPage': default,
            'ApexPageInfo': default_full,
            'ApexTestQueueItem': default,
            'ApexTestResult': default,
            'ApexTestResultLimits': default,
            'ApexTestRunResult': default,
            'ApexTestSuite': default,
            'ApexTrigger': default,
            'AppMenuItem': default,
            'Approval': default,
            'Asset': default,
            'AssetFeed': default,
            'AssetHistory': {
                self.REPLICATION_KEYS: {'CreatedDate'},
                self.PRIMARY_KEYS: {'Id'},
                self.REPLICATION_METHOD: self.INCREMENTAL,
            },
            'AssetRelationship': default,
            'AssetRelationshipFeed': default,
            'AssetRelationshipHistory': {
                self.REPLICATION_KEYS: {'CreatedDate'},
                self.PRIMARY_KEYS: {'Id'},
                self.REPLICATION_METHOD: self.INCREMENTAL,
            },
            'AssetShare': {
                self.PRIMARY_KEYS: {'Id'},
                self.REPLICATION_KEYS: {'LastModifiedDate'},
                self.REPLICATION_METHOD: self.INCREMENTAL,
            },
            'AssignmentRule': default,
            'AssociatedLocation': default,
            'AssociatedLocationHistory': {
                self.REPLICATION_KEYS: {'CreatedDate'},
                self.PRIMARY_KEYS: {'Id'},
                self.REPLICATION_METHOD: self.INCREMENTAL,
            },
            'AsyncApexJob': {
                self.REPLICATION_KEYS: {'CreatedDate'},
                self.PRIMARY_KEYS: {'Id'},
                self.REPLICATION_METHOD: self.INCREMENTAL,
            },
            'Attachment': default,
            'AuraDefinition': default,
            'AuraDefinitionBundle': default,
            'AuraDefinitionBundleInfo': default_full,
            'AuraDefinitionInfo': {
                self.PRIMARY_KEYS: {'Id'},
                self.REPLICATION_KEYS: {'LastModifiedDate'},
                self.REPLICATION_METHOD: self.INCREMENTAL,
            },
            'AuthConfig': default,
            'AuthConfigProviders': default,
            'AuthSession': {
                self.PRIMARY_KEYS: {'Id'},
                self.REPLICATION_KEYS: {'LastModifiedDate'},
                self.REPLICATION_METHOD: self.INCREMENTAL,
            },
            'BackgroundOperation': default,
            'BrandTemplate': default,
            'BrandingSet': default,
            'BrandingSetProperty': default,
            'BusinessHours': default,
            'BusinessProcess': default,
            'CallCenter': default,
            'Campaign': default,
            'CampaignFeed': default,
            'CampaignHistory': {
                self.REPLICATION_KEYS: {'CreatedDate'},
                self.PRIMARY_KEYS: {'Id'},
                self.REPLICATION_METHOD: self.INCREMENTAL,
            },
            'CampaignInfluenceModel': default,
            'CampaignMember': default,
            'CampaignMemberStatus': default,
            'CampaignShare': {
                self.PRIMARY_KEYS: {'Id'},
                self.REPLICATION_KEYS: {'LastModifiedDate'},
                self.REPLICATION_METHOD: self.INCREMENTAL,
            },
            'Case': default,
            'CaseComment': default,
            'CaseContactRole': default,
            'CaseFeed': default,
            'CaseHistory': {
                self.REPLICATION_KEYS: {'CreatedDate'},
                self.PRIMARY_KEYS: {'Id'},
                self.REPLICATION_METHOD: self.INCREMENTAL,
            },
            'CaseShare': {
                self.PRIMARY_KEYS: {'Id'},
                self.REPLICATION_KEYS: {'LastModifiedDate'},
                self.REPLICATION_METHOD: self.INCREMENTAL,
            },
            'CaseSolution': default,
            'CaseTeamMember': default,
            'CaseTeamRole': default,
            'CaseTeamTemplate': default,
            'CaseTeamTemplateMember': default,
            'CaseTeamTemplateRecord': default,
            'CategoryNode': default,
            'ChatterActivity': default,
            'ChatterExtension': default,
            'ChatterExtensionConfig': default,
            'ClientBrowser': {
                self.REPLICATION_KEYS: {'CreatedDate'},
                self.PRIMARY_KEYS: {'Id'},
                self.REPLICATION_METHOD: self.INCREMENTAL,
            },
            'CollaborationGroup': default,
            'CollaborationGroupFeed': default,
            'CollaborationGroupMember': default,
            'CollaborationGroupMemberRequest': default,
            'CollaborationInvitation': default,
            'Community': default,
            'ConferenceNumber': default,
            'ConnectedApplication': default_full,  # INSUFFICIENT_ACCESS
            'Contact': default,
            'ContactFeed': default,
            'ContactHistory': {
                self.REPLICATION_KEYS: {'CreatedDate'},
                self.PRIMARY_KEYS: {'Id'},
                self.REPLICATION_METHOD: self.INCREMENTAL,
            },
            'ContactShare': {
                self.PRIMARY_KEYS: {'Id'},
                self.REPLICATION_KEYS: {'LastModifiedDate'},
                self.REPLICATION_METHOD: self.INCREMENTAL,
            },
            'ContentAsset': default,
            'ContentDistribution': default,
            'ContentDistributionView': default,
            'ContentDocument': default,
            'ContentDocumentFeed': default,
            'ContentDocumentHistory': {
                self.REPLICATION_KEYS: {'CreatedDate'},
                self.PRIMARY_KEYS: {'Id'},
                self.REPLICATION_METHOD: self.INCREMENTAL,
            },
            'ContentFolder': default,
            'ContentFolderLink': default_full,
            'ContentNote': {
                self.PRIMARY_KEYS: {'Id'},
                self.REPLICATION_KEYS: {'LastModifiedDate'},
                self.REPLICATION_METHOD: self.INCREMENTAL,
            },
            'ContentVersion': default,
            'ContentVersionHistory': {
                self.REPLICATION_KEYS: {'CreatedDate'},
                self.PRIMARY_KEYS: {'Id'},
                self.REPLICATION_METHOD: self.INCREMENTAL,
            },
            'ContentWorkspace': default,
            'ContentWorkspaceDoc': default,
            'ContentWorkspaceMember': {
                self.REPLICATION_KEYS: {'CreatedDate'},
                self.PRIMARY_KEYS: {'Id'},
                self.REPLICATION_METHOD: self.INCREMENTAL,
            },
            'ContentWorkspacePermission': default,
            'Contract': default,
            'ContractContactRole': default,
            'ContractFeed': default,
            'ContractHistory': {
                self.REPLICATION_KEYS: {'CreatedDate'},
                self.PRIMARY_KEYS: {'Id'},
                self.REPLICATION_METHOD: self.INCREMENTAL,
            },
            'CronJobDetail': default_full,
            'CronTrigger': {
                self.REPLICATION_KEYS: {'CreatedDate'},
                self.PRIMARY_KEYS: {'Id'},
                self.REPLICATION_METHOD: self.INCREMENTAL,
            },
            'CustomBrand': {
                self.PRIMARY_KEYS: {'Id'},
                self.REPLICATION_KEYS: {'LastModifiedDate'},
                self.REPLICATION_METHOD: self.INCREMENTAL,
            },
            'CustomBrandAsset': {
                self.PRIMARY_KEYS: {'Id'},
                self.REPLICATION_KEYS: {'LastModifiedDate'},
                self.REPLICATION_METHOD: self.INCREMENTAL,
            },
            'CustomObjectUserLicenseMetrics': default,
            'CustomPermission': default,
            'CustomPermissionDependency': default,
            'Dashboard': default,
            'DashboardComponent': default_full,
            'DashboardComponentFeed': default,
            'DashboardFeed': default,
            'DatacloudAddress': default_full,
            'Document': default,
            'DocumentAttachmentMap': {
                self.REPLICATION_KEYS: {'CreatedDate'},
                self.PRIMARY_KEYS: {'Id'},
                self.REPLICATION_METHOD: self.INCREMENTAL,
            },
            'Domain': default,
            'DomainSite': default,
            'DuplicateRule': default,
            'EmailMessage': default,
            'EmailMessageRelation': default,
            'EmailServicesAddress': default,
            'EmailServicesFunction': default,
            'EmailTemplate': default,
            'EmbeddedServiceDetail': default_full,
            'EntityDefinition': {
                self.PRIMARY_KEYS: {'Id'},
                self.REPLICATION_KEYS: {'LastModifiedDate'},
                self.REPLICATION_METHOD: self.INCREMENTAL,
            },
            'EntitySubscription': {
                self.REPLICATION_KEYS: {'CreatedDate'},
                self.PRIMARY_KEYS: {'Id'},
                self.REPLICATION_METHOD: self.INCREMENTAL,
            },
            'Event': default,
            'EventBusSubscriber': default_full,
            'EventFeed': default,
            'EventRelation': default,
            'ExternalDataSource': default,
            'ExternalDataUserAuth': default,
            'ExternalEvent': default,
            'ExternalEventMapping': default,
            'ExternalEventMappingShare': {
                self.PRIMARY_KEYS: {'Id'},
                self.REPLICATION_KEYS: {'LastModifiedDate'},
                self.REPLICATION_METHOD: self.INCREMENTAL,
            },
            'FeedAttachment': default_full,
            'FeedComment': default,
            'FeedItem': default,
            'FeedRevision': default,
            'FieldPermissions': default,
            'FiscalYearSettings': default,
            'FlowInterview': default,
            'FlowInterviewShare': {
                self.PRIMARY_KEYS: {'Id'},
                self.REPLICATION_KEYS: {'LastModifiedDate'},
                self.REPLICATION_METHOD: self.INCREMENTAL,
            },
            'Folder': default,
            'ForecastingAdjustment': default,
            'ForecastingCategoryMapping': default,
            'ForecastingDisplayedFamily': default,
            'ForecastingFact': default,
            'ForecastingItem': default,
            'ForecastingOwnerAdjustment': default,
            'ForecastingQuota': default,
            'ForecastingShare': default,
            'ForecastingType': default,
            'ForecastingTypeToCategory': default,
            'ForecastingUserPreference': default_full,
            'GrantedByLicense': default,
            'Group': default,
            'GroupMember': default,
            'Holiday': default,
            'Idea': default,
            'InstalledMobileApp': default,
            'KnowledgeableUser': default,
            'Lead': default,
            'LeadFeed': default,
            'LeadHistory': {
                self.REPLICATION_KEYS: {'CreatedDate'},
                self.PRIMARY_KEYS: {'Id'},
                self.REPLICATION_METHOD: self.INCREMENTAL,
            },
            'LeadShare': {
                self.PRIMARY_KEYS: {'Id'},
                self.REPLICATION_KEYS: {'LastModifiedDate'},
                self.REPLICATION_METHOD: self.INCREMENTAL,
            },
            'LeadStatus': default,
            'ListView': default,
            'ListViewChart': default,
            'Location': default,
            'LocationFeed': default,
            'LocationHistory': {
                self.REPLICATION_KEYS: {'CreatedDate'},
                self.PRIMARY_KEYS: {'Id'},
                self.REPLICATION_METHOD: self.INCREMENTAL,
            },
            'LocationShare': {
                self.PRIMARY_KEYS: {'Id'},
                self.REPLICATION_KEYS: {'LastModifiedDate'},
                self.REPLICATION_METHOD: self.INCREMENTAL,
            },
            'LoginHistory': {
                self.PRIMARY_KEYS: {'Id'},
                self.REPLICATION_KEYS: {'LoginTime'},
                self.REPLICATION_METHOD: self.INCREMENTAL,
            },
            'LoginIp': {
                self.REPLICATION_KEYS: {'CreatedDate'},
                self.PRIMARY_KEYS: {'Id'},
                self.REPLICATION_METHOD: self.INCREMENTAL,
            },
            'Macro': default,
            'MacroHistory': {
                self.REPLICATION_KEYS: {'CreatedDate'},
                self.PRIMARY_KEYS: {'Id'},
                self.REPLICATION_METHOD: self.INCREMENTAL,
            },
            'MacroInstruction': default,
            'MacroShare': {
                self.PRIMARY_KEYS: {'Id'},
                self.REPLICATION_KEYS: {'LastModifiedDate'},
                self.REPLICATION_METHOD: self.INCREMENTAL,
            },
            'MailmergeTemplate': default,
            'MatchingInformation': default,
            'MatchingRule': default,
            'MatchingRuleItem': default,
            'MobileApplicationDetail': default,
            'NamedCredential': default,
            'Note': default,
            'OauthToken': {
                self.REPLICATION_KEYS: {'CreatedDate'},
                self.PRIMARY_KEYS: {'Id'},
                self.REPLICATION_METHOD: self.INCREMENTAL,
            },
            'ObjectPermissions': default,
            'Opportunity': default,
            'OpportunityCompetitor': default,
            'OpportunityContactRole': default,
            'OpportunityFeed': default,
            'OpportunityFieldHistory': {
                self.REPLICATION_KEYS: {'CreatedDate'},
                self.PRIMARY_KEYS: {'Id'},
                self.REPLICATION_METHOD: self.INCREMENTAL,
            },
            'OpportunityHistory': default,
            'OpportunityLineItem': default,
            'OpportunityPartner': default,
            'OpportunityShare': {
                self.PRIMARY_KEYS: {'Id'},
                self.REPLICATION_KEYS: {'LastModifiedDate'},
                self.REPLICATION_METHOD: self.INCREMENTAL,
            },
            'OpportunityStage': default,
            'Order': default,
            'OrderFeed': default,
            'OrderHistory': {
                self.REPLICATION_KEYS: {'CreatedDate'},
                self.PRIMARY_KEYS: {'Id'},
                self.REPLICATION_METHOD: self.INCREMENTAL,
            },
            'OrderItem': default,
            'OrderItemFeed': default,
            'OrderItemHistory': {
                self.REPLICATION_KEYS: {'CreatedDate'},
                self.PRIMARY_KEYS: {'Id'},
                self.REPLICATION_METHOD: self.INCREMENTAL,
            },
            'OrderShare': {
                self.PRIMARY_KEYS: {'Id'},
                self.REPLICATION_KEYS: {'LastModifiedDate'},
                self.REPLICATION_METHOD: self.INCREMENTAL,
            },
            'OrgWideEmailAddress': default,
            'Organization': default,
            'Partner': default,
            'Period': default,
            'PermissionSet': default,
            'PermissionSetAssignment': default,
            'PermissionSetGroup': default,
            'PermissionSetGroupComponent': default,
            'PermissionSetLicense': default,
            'PermissionSetLicenseAssign': default,
            'PlatformCachePartition': default,
            'PlatformCachePartitionType': default,
            'Pricebook2': default,
            'Pricebook2History': {
                self.REPLICATION_KEYS: {'CreatedDate'},
                self.PRIMARY_KEYS: {'Id'},
                self.REPLICATION_METHOD: self.INCREMENTAL,
            },
            'PricebookEntry': default,
            'ProcessDefinition': default,
            'ProcessInstance': default,
            'ProcessInstanceNode': default,
            'ProcessInstanceStep': default,
            'ProcessInstanceWorkitem': default,
            'ProcessNode': default,
            'Product2': default,
            'Product2Feed': default,
            'Product2History': {
                self.REPLICATION_KEYS: {'CreatedDate'},
                self.PRIMARY_KEYS: {'Id'},
                self.REPLICATION_METHOD: self.INCREMENTAL,
            },
            'Profile': default,
            'ProfileSkill': default,
            'ProfileSkillEndorsement': default,
            'ProfileSkillEndorsementFeed': default,
            'ProfileSkillEndorsementHistory': {
                self.REPLICATION_KEYS: {'CreatedDate'},
                self.PRIMARY_KEYS: {'Id'},
                self.REPLICATION_METHOD: self.INCREMENTAL,
            },
            'ProfileSkillFeed': default,
            'ProfileSkillHistory': {
                self.REPLICATION_KEYS: {'CreatedDate'},
                self.PRIMARY_KEYS: {'Id'},
                self.REPLICATION_METHOD: self.INCREMENTAL,
            },
            'ProfileSkillShare': {
                self.PRIMARY_KEYS: {'Id'},
                self.REPLICATION_KEYS: {'LastModifiedDate'},
                self.REPLICATION_METHOD: self.INCREMENTAL,
            },
            'ProfileSkillUser': default,
            'ProfileSkillUserFeed': default,
            'ProfileSkillUserHistory': {
                self.REPLICATION_KEYS: {'CreatedDate'},
                self.PRIMARY_KEYS: {'Id'},
                self.REPLICATION_METHOD: self.INCREMENTAL,
            },
            'Publisher': default_full,
            'QueueSobject': default,
            'QuickText': default,
            'QuickTextHistory': {
                self.REPLICATION_KEYS: {'CreatedDate'},
                self.PRIMARY_KEYS: {'Id'},
                self.REPLICATION_METHOD: self.INCREMENTAL,
            },
            'QuickTextShare': {
                self.PRIMARY_KEYS: {'Id'},
                self.REPLICATION_KEYS: {'LastModifiedDate'},
                self.REPLICATION_METHOD: self.INCREMENTAL,
            },
            'Quote': default,
            'QuoteDocument': default,
            'QuoteFeed': default,
            'QuoteLineItem': default,
            'QuoteShare': {
                self.PRIMARY_KEYS: {'Id'},
                self.REPLICATION_KEYS: {'LastModifiedDate'},
                self.REPLICATION_METHOD: self.INCREMENTAL,
            },
            'RecordType': default,
            'Report': default,
            'ReportFeed': default,
            'SamlSsoConfig': default,
            'Scontrol': default,
            'SessionPermSetActivation': default,
            'SetupAuditTrail': {
                self.REPLICATION_KEYS: {'CreatedDate'},
                self.PRIMARY_KEYS: {'Id'},
                self.REPLICATION_METHOD: self.INCREMENTAL,
            },
            'SetupEntityAccess': default,
            'Site': default,
            'SiteFeed': default,
            'SiteHistory': {
                self.REPLICATION_KEYS: {'CreatedDate'},
                self.PRIMARY_KEYS: {'Id'},
                self.REPLICATION_METHOD: self.INCREMENTAL,
            },
            'Solution': default,
            'SolutionFeed': default,
            'SolutionHistory': {
                self.REPLICATION_KEYS: {'CreatedDate'},
                self.PRIMARY_KEYS: {'Id'},
                self.REPLICATION_METHOD: self.INCREMENTAL,
            },
            'Stamp': default,
            'StampAssignment': default,
            'StaticResource': default,
            'Task': default,
            'TaskFeed': default,
            'TaskRelation': default,
            'TenantUsageEntitlement': default,
            'TestSuiteMembership': default,
            'ThirdPartyAccountLink': default_full,
            'TodayGoal': default,
            'TodayGoalShare': {
                self.PRIMARY_KEYS: {'Id'},
                self.REPLICATION_KEYS: {'LastModifiedDate'},
                self.REPLICATION_METHOD: self.INCREMENTAL,
            },
            'Topic': default,
            'TopicAssignment': default,
            'TopicFeed': default,
            'User': default,
            'UserAppInfo': default,
            'UserAppMenuCustomization': default,
            'UserAppMenuCustomizationShare': {
                self.PRIMARY_KEYS: {'Id'},
                self.REPLICATION_KEYS: {'LastModifiedDate'},
                self.REPLICATION_METHOD: self.INCREMENTAL,
            },
            'UserAppMenuItem': default_full,
            'UserFeed': default,
            'UserLicense': default,
            'UserListView': default,
            'UserListViewCriterion': default,
            'UserLogin': {
                self.PRIMARY_KEYS: {'Id'},
                self.REPLICATION_KEYS: {'LastModifiedDate'},
                self.REPLICATION_METHOD: self.INCREMENTAL,
            },
            'UserPermissionAccess': default_full,
            'UserPreference': default,
            'UserRole': default,
            'UserShare': {
                self.PRIMARY_KEYS: {'Id'},
                self.REPLICATION_KEYS: {'LastModifiedDate'},
                self.REPLICATION_METHOD: self.INCREMENTAL,
            },
            'WaveCompatibilityCheckItem': default,
            'WorkAccess': default,
            'WorkAccessShare': {
                self.PRIMARY_KEYS: {'Id'},
                self.REPLICATION_KEYS: {'LastModifiedDate'},
                self.REPLICATION_METHOD: self.INCREMENTAL,
            },
            'WorkBadge': default,
            'WorkBadgeDefinition': default,
            'WorkBadgeDefinitionFeed': default,
            'WorkBadgeDefinitionHistory': {
                self.REPLICATION_KEYS: {'CreatedDate'},
                self.PRIMARY_KEYS: {'Id'},
                self.REPLICATION_METHOD: self.INCREMENTAL,
            },
            'WorkBadgeDefinitionShare': {
                self.PRIMARY_KEYS: {'Id'},
                self.REPLICATION_KEYS: {'LastModifiedDate'},
                self.REPLICATION_METHOD: self.INCREMENTAL,
            },
            'WorkThanks': default,
            'WorkThanksShare': {
                self.PRIMARY_KEYS: {'Id'},
                self.REPLICATION_KEYS: {'LastModifiedDate'},
                self.REPLICATION_METHOD: self.INCREMENTAL,
            },
            'cbit__ClearbitLog__c': default,
            'cbit__ClearbitProspectorSearch__c': default,
            'cbit__ClearbitRequest__c': default,
            'cbit__ClearbitStats__c': default,
            'cbit__Clearbit_User_Settings__c': default,
            'cbit__Clearbit__c': default,
            'cbit__Mapping__Share': {
                self.PRIMARY_KEYS: {'Id'},
                self.REPLICATION_KEYS: {'LastModifiedDate'},
                self.REPLICATION_METHOD: self.INCREMENTAL,
            },
            'cbit__Mapping__c': default,
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


    def expected_automatic_fields(self):
        auto_fields = {}
        for k, v in self.expected_metadata().items():
            auto_fields[k] = v.get(self.PRIMARY_KEYS, set()) | v.get(self.REPLICATION_KEYS, set()) \
                | v.get(self.FOREIGN_KEYS, set())
        return auto_fields

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

    def set_replication_methods(self, conn_id, catalogs, replication_methods):

        replication_keys = self.expected_replication_keys()

        for catalog in catalogs:

            replication_method = replication_methods.get(catalog['stream_name'])

            if replication_method == self.INCREMENTAL:
                replication_key = list(replication_keys.get(catalog['stream_name']))[0]
                replication_md = [{ "breadcrumb": [], "metadata": {'replication-key': replication_key, "replication-method" : replication_method, "selected" : True}}]
            else:
                replication_md = [{ "breadcrumb": [], "metadata": {'replication-key': None, "replication-method" : "FULL_TABLE", "selected" : True}}]

            connections.set_non_discoverable_metadata(
                conn_id, catalog, menagerie.get_annotated_schema(conn_id, catalog['stream_id']), replication_md)

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
                    except ValueError as e_final:
                        raise NotImplementedError("We are not accounting for dates of this format: {}".format(date_value)) from e_final

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

    ##########################################################################
    ### Tap Specific Methods
    ##########################################################################

    @staticmethod
    def get_unsupported_by_rest_api():
        """The streams listed here are not supported by the REST API"""
        unsupported_streams = {
            'Announcement',
            'CollaborationGroupRecord',
            'ContentDocumentLink',
            'ContentFolderMember',
            'DataStatistics',
            'EntityParticle',
            'FieldDefinition',
            'FlexQueueItem',
            'IdeaComment',
            'OwnerChangeOptionInfo',
            'PicklistValueInfo',
            'PlatformAction',
            'RelationshipDomain',
            'RelationshipInfo',
            'SearchLayout',
            'SiteDetail',
            'UserEntityAccess',
            'UserFieldAccess',
            'Vote'
        }
        return unsupported_streams

    def get_unsupported_by_bulk_api(self):
        unsupported_streams_rest = self.get_unsupported_by_rest_api()
        unsupported_streams_bulk_only= {
            'AcceptedEventRelation',
            'AssetTokenEvent',
            'AttachedContentNote',
            'CaseStatus',
            'ContentFolderItem',
            'ContractStatus',
            'DeclinedEventRelation',
            'EventWhoRelation',
            'PartnerRole',
            'QuoteTemplateRichTextData',
            'RecentlyViewed',
            'SolutionStatus',
            'TaskPriority',
            'TaskWhoRelation',
            'TaskStatus',
            'UndecidedEventRelation'
        }

        return unsupported_streams_bulk_only | unsupported_streams_rest

    def is_unsupported_by_rest_api(self, stream):
        """returns True if stream is unsupported by REST API"""

        return stream in self.get_unsupported_by_rest_api()

    def is_unsupported_by_bulk_api(self, stream):
        """
        returns True if stream is unsupported by BULK API

        BULK API does not support any streams that are unsupported by the REST API and
        in addition does not support the streams listed below.
        """
        return stream in self.get_unsupported_by_bulk_api()
