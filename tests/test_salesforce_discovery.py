"""Test tap discovery mode and metadata/annotated-schema."""
from tap_tester import menagerie, connections

from base import SalesforceBaseTest


class DiscoveryTest(SalesforceBaseTest):
    """Test tap discovery mode and metadata/annotated-schema conforms to standards."""

    @staticmethod
    def name():
        return "tap_tester_salesforce_discovery_test"

    def test_run(self):
        """
        Verify that discover creates the appropriate catalog, schema, metadata, etc.

        • Verify number of actual streams discovered match expected
        • Verify the stream names discovered were what we expect
        • Verify stream names follow naming convention
          streams should only have lowercase alphas and underscores
        • verify there is only 1 top level breadcrumb
        • verify replication key(s)
        • verify primary key(s)
        • verify that if there is a replication key we are doing INCREMENTAL otherwise FULL
        • verify the actual replication matches our expected replication method
        • verify that primary, replication and foreign keys
          are given the inclusion of automatic (metadata and annotated schema).
        • verify that all other fields have inclusion of available (metadata and schema)
        """
        # BUG | https://jira.talendforge.org/browse/TDL-15748
        #      The following streams stopped being discovered 10/10/2021
        #      When bug is addressed fix the marked lines
        missing_streams = {'DataAssetUsageTrackingInfo', 'DataAssetSemanticGraphEdge'}

        streams_to_test = self.expected_streams() - missing_streams # BUG_TDL-15748
        streams_to_test_prime = self.expected_streams().difference(self.get_unsupported_by_bulk_api())
        # self.assertEqual(len(streams_to_test), len(streams_to_test_prime), msg="Expectations are invalid.") # BUG_TDL-15748

        conn_id = connections.ensure_connection(self)

        found_catalogs = self.run_and_verify_check_mode(conn_id)

        # verify the tap only discovers the expected streams
        found_catalog_names = set(map(lambda c: c['tap_stream_id'], found_catalogs))
        self.assertSetEqual(streams_to_test, found_catalog_names)
        print("discovered schemas are OK")

        # NOTE: The following assertion is not backwards compatible with older taps, but it
        #       SHOULD BE IMPLEMENTED in future taps, leaving here as a comment for reference

        # Verify stream names follow naming convention
        # streams should only have lowercase alphas and underscores
        # found_catalog_names = {c['tap_stream_id'] for c in found_catalogs}
        # self.assertTrue(all([re.fullmatch(r"[a-z_]+",  name) for name in found_catalog_names]),
        #                 msg="One or more streams don't follow standard naming")

        for stream in streams_to_test:
            with self.subTest(stream=stream):
                catalog = next(iter([catalog for catalog in found_catalogs
                                     if catalog["stream_name"] == stream]))
                assert catalog  # based on previous tests this should always be found

                schema_and_metadata = menagerie.get_annotated_schema(conn_id, catalog['stream_id'])
                metadata = schema_and_metadata["metadata"]
                schema = schema_and_metadata["annotated-schema"]

                # verify there is only 1 top level breadcrumb
                stream_properties = [item for item in metadata if item.get("breadcrumb") == []]
                self.assertTrue(len(stream_properties) == 1,
                                msg="There is NOT only one top level breadcrumb for {}".format(stream) + \
                                "\nstream_properties | {}".format(stream_properties))

                # verify replication key(s)
                self.assertEqual(
                    set(stream_properties[0].get(
                        "metadata", {self.REPLICATION_KEYS: []}).get(self.REPLICATION_KEYS, [])),
                    self.expected_replication_keys()[stream],
                    msg="expected replication key {} but actual is {}".format(
                        self.expected_replication_keys()[stream],
                        set(stream_properties[0].get(
                            "metadata", {self.REPLICATION_KEYS: None}).get(
                                self.REPLICATION_KEYS, []))))

                # verify primary key(s)
                self.assertEqual(
                    set(stream_properties[0].get(
                        "metadata", {self.PRIMARY_KEYS: []}).get(self.PRIMARY_KEYS, [])),
                    self.expected_primary_keys()[stream],
                    msg="expected primary key {} but actual is {}".format(
                        self.expected_primary_keys()[stream],
                        set(stream_properties[0].get(
                            "metadata", {self.PRIMARY_KEYS: None}).get(self.PRIMARY_KEYS, []))))

                actual_replication_method = stream_properties[0].get(
                    "metadata", {self.REPLICATION_METHOD: None}).get(self.REPLICATION_METHOD)


                # BUG_1 | https://stitchdata.atlassian.net/browse/SRCE-4794
                # {'reason': 'No replication keys found from the Salesforce API', 'replication-method': 'FULL_TABLE'} != "FULL_TABLE"
                failing_full_table_streams = {  # BUG_1
                    'DashboardComponent', 'ForecastingUserPreference', 'DatacloudAddress',
                    'UserPermissionAccess', 'ApexPageInfo', 'ConnectedApplication',
                    'EmbeddedServiceDetail', 'ContentFolderLink', 'EventBusSubscriber',
                    'UserAppMenuItem', 'CronJobDetail', 'AuraDefinitionBundleInfo',
                    'FeedAttachment', 'Publisher', 'ThirdPartyAccountLink',
                    'AppDefinition','FormulaFunction','PlatformEventUsageMetric',
                    'FormulaFunctionAllowedType','FormulaFunctionCategory',
                    'EmbeddedServiceLabel','UserSetupEntityAccess','TabDefinition'
                }
                # 'INCREMENTAL' != None
                failing_streams_without_replication_method = {  # BUG_1
                    'CaseTeamTemplateRecord', 'ExternalEvent', 'cbit__Mapping__c', 'Topic', 'Contract', 'CustomBrand',
                    'Approval', 'CaseContactRole', 'AsyncApexJob', 'EmailMessageRelation', 'AssignmentRule', 'AuthSession',
                    'CaseSolution', 'ApexClass', 'ContentVersionHistory', 'CronTrigger', 'DashboardFeed', 'Pricebook2History',
                    'ForecastingFact', 'SetupAuditTrail', 'ContentAsset', 'DuplicateRule', 'ProcessInstance', 'CustomPermissionDependency',
                    'SessionPermSetActivation', 'OrgWideEmailAddress', 'TaskRelation', 'QuickTextShare', 'DocumentAttachmentMap', 'Folder',
                    'FieldPermissions', 'ContentDocumentFeed', 'ForecastingType', 'OpportunityFeed', 'CaseTeamTemplateMember', 'ForecastingOwnerAdjustment',
                    'ForecastingQuota', 'SiteFeed', 'AppMenuItem', 'TestSuiteMembership', 'CollaborationGroupMember', 'ContentDistribution',
                    'Dashboard', 'SetupEntityAccess', 'ContactHistory', 'MatchingInformation', 'Site', 'UserListViewCriterion',
                    'ContentWorkspacePermission', 'WorkThanks', 'CaseHistory', 'CampaignInfluenceModel', 'LeadShare', 'CampaignFeed',
                    'AdditionalNumber', 'EventFeed', 'PlatformCachePartition', 'EntityDefinition', 'AccountFeed', 'LeadHistory',
                    'OrderItemHistory', 'ProfileSkillUser', 'ForecastingShare', 'Order', 'OpportunityCompetitor', 'UserRole', 'AuthConfig',
                    'WorkBadgeDefinitionShare', 'UserLogin', 'OpportunityPartner', 'TodayGoalShare', 'AssociatedLocation', 'ContentWorkspaceMember',
                    'Case', 'WorkBadgeDefinitionHistory', 'LeadStatus', 'LocationFeed', 'CustomBrandAsset', 'OpportunityFieldHistory',
                    'Campaign', 'ForecastingDisplayedFamily', 'ContractFeed', 'PermissionSetLicense', 'FeedItem', 'User', 'AssetFeed',
                    'ContentDocumentHistory', 'ProcessInstanceNode', 'Pricebook2', 'Period', 'cbit__Mapping__Share', 'StaticResource',
                    'UserListView', 'RecordType', 'PermissionSetLicenseAssign', 'ContentDocument', 'Note', 'OpportunityShare',
                    'Organization', 'UserAppMenuCustomization', 'Location', 'CaseComment', 'ExternalDataSource', 'Report',
                    'DashboardComponentFeed', 'AssetHistory', 'ActionLinkGroupTemplate', 'CustomPermission', 'cbit__ClearbitLog__c', 'LocationShare',
                    'QuoteShare', 'WorkAccessShare', 'ProfileSkill', 'Macro', 'EmailServicesFunction', 'ApexComponent',
                    'WorkBadge', 'OpportunityLineItem', 'ApexTestResult', 'ContentWorkspaceDoc', 'CaseShare', 'TaskFeed',
                    'cbit__ClearbitStats__c', 'SolutionFeed', 'CampaignHistory', 'ProfileSkillEndorsementFeed', 'OrderItem', 'AccountHistory',
                    'PermissionSetGroup', 'ProcessNode', 'CollaborationGroupMemberRequest', 'AssetShare', 'Quote', 'ProfileSkillShare',
                    'OrderHistory', 'SolutionHistory', 'ApexTestRunResult', 'ClientBrowser', 'LocationHistory', 'AccountPartner',
                    'TenantUsageEntitlement', 'SiteHistory', 'Holiday', 'AccountContactRelation', 'KnowledgeableUser', 'ListViewChart',
                    'FlowInterview', 'ForecastingItem', 'ChatterExtensionConfig', 'ContactFeed', 'CaseTeamTemplate', 'ContractHistory', 'Partner',
                    'ReportFeed', 'ContentFolder', 'BackgroundOperation', 'ProfileSkillFeed', 'SamlSsoConfig', 'ProcessDefinition',
                    'TodayGoal',  'ProfileSkillEndorsementHistory', 'UserLicense', 'AssociatedLocationHistory',
                    'UserAppMenuCustomizationShare', 'cbit__Clearbit_User_Settings__c', 'cbit__ClearbitProspectorSearch__c', 'ApexTrigger', 'PermissionSetAssignment', 'OpportunityStage',
                    'ProfileSkillEndorsement', 'CollaborationGroupFeed', 'CampaignMemberStatus', 'PlatformCachePartitionType',
                     'ProfileSkillUserHistory', 'QuickText', 'OauthToken', 'AssetRelationshipFeed', 'ExternalDataUserAuth', 'ForecastingAdjustment',
                    'EmailTemplate', 'CollaborationInvitation', 'PermissionSetGroupComponent', 'ContactShare', 'ApexTestSuite', 'MobileApplicationDetail',
                    'FeedRevision', 'CallCenter', 'GroupMember', 'Scontrol', 'StampAssignment', 'EntitySubscription',
                    'FlowInterviewShare', 'Stamp', 'AssetRelationship', 'CampaignShare', 'BusinessHours', 'MatchingRule',
                    'QueueSobject', 'AuthConfigProviders', 'FiscalYearSettings', 'BrandTemplate', 'MacroShare', 'Task',
                    'QuickTextHistory', 'NamedCredential', 'WorkBadgeDefinitionFeed', 'TopicAssignment', 'CaseTeamMember', 'ContentWorkspace',
                    'UserShare', 'ContentDistributionView', 'ActionLinkTemplate', 'Asset', 'AccountContactRole', 'ProfileSkillUserFeed',
                    'BrandingSet', 'Attachment', 'WorkBadgeDefinition', 'ContractContactRole', 'AccountShare', 'Event',
                    'AuraDefinitionBundle', 'Product2History', 'ApexPage', 'MacroHistory', 'UserFeed', 'ForecastingCategoryMapping',
                    'Account', 'Opportunity', 'ProcessInstanceStep', 'ChatterExtension', 'CaseTeamRole', 'OrderFeed',
                    'Lead', 'Group', 'CollaborationGroup', 'ConferenceNumber', 'ExternalEventMapping', 'ForecastingTypeToCategory',
                    'ProcessInstanceWorkitem', 'MacroInstruction', 'AuraDefinition', 'EventRelation', 'Domain', 'CaseFeed',
                    'UserPreference', 'Document', 'AssetRelationshipHistory', 'LoginIp', 'OpportunityHistory', 'GrantedByLicense',
                    'LeadFeed', 'OrderItemFeed', 'InstalledMobileApp', 'ApexTestResultLimits', 'AuraDefinitionInfo', 'WaveCompatibilityCheckItem',
                    'ListView', 'Community', 'cbit__Clearbit__c', 'Profile', 'PermissionSet', 'ExternalEventMappingShare',
                    'Contact', 'Solution', 'ChatterActivity', 'QuoteLineItem', 'DomainSite', 'UserAppInfo',
                    'OpportunityContactRole', 'CampaignMember', 'Product2Feed', 'cbit__ClearbitRequest__c',
                    'Product2', 'ContentNote', 'ProfileSkillHistory', 'ObjectPermissions', 'CategoryNode', 'EmailServicesAddress',
                    'WorkThanksShare', 'BrandingSetProperty', 'WorkAccess', 'LoginHistory', 'FeedComment', 'TopicFeed',
                    'QuoteFeed', 'ContentVersion', 'EmailMessage', 'OrderShare', 'MailmergeTemplate', 'Idea',
                    'QuoteDocument', 'BusinessProcess', 'PricebookEntry', 'ApexTestQueueItem', 'MatchingRuleItem', 'CustomObjectUserLicenseMetrics',
                    'ActiveFeatureLicenseMetric', 'ActivePermSetLicenseMetric', 'ActiveProfileMetric',
                    'AppDefinition', 'AuthorizationForm', 'AuthorizationFormConsent', 'AuthorizationFormConsentHistory', 'AuthorizationFormConsentShare',
                    'AuthorizationFormDataUse', 'AuthorizationFormDataUseHistory', 'AuthorizationFormDataUseShare', 'AuthorizationFormHistory', 'AuthorizationFormShare',
                    'AuthorizationFormText', 'AuthorizationFormTextFeed', 'AuthorizationFormTextHistory', 'Calendar', 'CalendarView', 'CalendarViewShare',
                    'CallCoachingMediaProvider', 'CommSubscription', 'CommSubscriptionChannelType', 'CommSubscriptionChannelTypeFeed', 'CommSubscriptionChannelTypeHistory',
                    'CommSubscriptionChannelTypeShare', 'CommSubscriptionConsent', 'CommSubscriptionConsentFeed', 'CommSubscriptionConsentHistory', 'CommSubscriptionConsentShare',
                    'CommSubscriptionFeed', 'CommSubscriptionHistory', 'CommSubscriptionShare', 'CommSubscriptionTiming', 'CommSubscriptionTimingFeed',
                    'CommSubscriptionTimingHistory', 'ContactPointAddress', 'ContactPointAddressHistory', 'ContactPointAddressShare', 'ContactPointConsent',
                    'ContactPointConsentHistory', 'ContactPointConsentShare', 'ContactPointEmail', 'ContactPointEmailHistory', 'ContactPointEmailShare',
                    'ContactPointPhone', 'ContactPointPhoneHistory', 'ContactPointPhoneShare', 'ContactPointTypeConsent', 'ContactPointTypeConsentHistory',
                    'ContactPointTypeConsentShare', 'CustomHelpMenuItem', 'CustomHelpMenuSection', 'CustomHttpHeader', 'CustomNotificationType',
                    'DataAssetSemanticGraphEdge', 'DataAssetUsageTrackingInfo', 'DataIntegrationRecordPurchasePermission', 'DataUseLegalBasis', 'DataUseLegalBasisHistory',
                    'DataUseLegalBasisShare', 'DataUsePurpose', 'DataUsePurposeHistory', 'DataUsePurposeShare', 'DeleteEvent', 'EmbeddedServiceLabel',
                    'EngagementChannelType', 'EngagementChannelTypeFeed', 'EngagementChannelTypeHistory', 'EngagementChannelTypeShare', 'EnhancedLetterhead',
                    'EnhancedLetterheadFeed', 'ExpressionFilter', 'ExpressionFilterCriteria', 'FlowDefinitionView', 'FlowInterviewLog', 'FlowInterviewLogEntry',
                    'FlowInterviewLogShare', 'FlowRecordRelation', 'FlowStageRelation', 'ForecastingSourceDefinition', 'ForecastingTypeSource', 'FormulaFunction',
                    'FormulaFunctionAllowedType', 'FormulaFunctionCategory', 'IframeWhiteListUrl', 'Individual', 'IndividualHistory', 'IndividualShare',
                    'LightningExperienceTheme', 'LightningOnboardingConfig', 'MacroUsage', 'MacroUsageShare', 'MutingPermissionSet', 'MyDomainDiscoverableLogin',
                    'OnboardingMetrics', 'PartyConsent', 'PartyConsentFeed', 'PartyConsentHistory', 'PartyConsentShare', 'PermissionSetTabSetting', 'PlatformEventUsageMetric',
                    'PricebookEntryHistory', 'Prompt', 'PromptVersion', 'QuickTextUsage', 'QuickTextUsageShare', 'Recommendation', 'RecordAction', 'RedirectWhitelistUrl',
                    'SiteIframeWhiteListUrl', 'SiteRedirectMapping', 'TabDefinition', 'Translation', 'UiFormulaCriterion', 'UiFormulaRule', 'UserEmailPreferredPerson',
                    'UserEmailPreferredPersonShare', 'UserSetupEntityAccess', 'WaveAutoInstallRequest'
                }

                if stream in failing_full_table_streams | failing_streams_without_replication_method:  # BUG_1
                    self.LOGGER.warning("Skipping 'expected replication method' asssertions for %s", stream)
                else:  # BUG_1
                    # verify the actual replication matches our expected replication method
                    self.assertEqual(
                        self.expected_replication_method().get(stream, None),
                        actual_replication_method,
                        msg="The actual replication method {} doesn't match the expected {}".format(
                            actual_replication_method,
                            self.expected_replication_method().get(stream, None)))

                    # verify that if there is a replication key we are doing INCREMENTAL otherwise FULL
                    if stream_properties[0].get(
                            "metadata", {self.REPLICATION_KEYS: []}).get(self.REPLICATION_KEYS, []):
                        self.assertTrue(actual_replication_method == self.INCREMENTAL,
                                        msg="Expected INCREMENTAL replication "
                                            "since there is a replication key")
                    else:
                        self.assertTrue(actual_replication_method == self.FULL_TABLE,
                                        msg="Expected FULL replication since there is no replication key")


                expected_primary_keys = self.expected_primary_keys()[stream]
                expected_replication_keys = self.expected_replication_keys()[stream]
                expected_automatic_fields = expected_primary_keys | expected_replication_keys

                # verify that primary, replication and foreign keys
                # are given the inclusion of automatic in annotated schema.
                actual_automatic_fields = {key for key, value in schema["properties"].items()
                                           if value.get("inclusion") == "automatic"}
                self.assertEqual(expected_automatic_fields, actual_automatic_fields)

                # BUG_2 | https://stitchdata.atlassian.net/browse/SRCE-4793
                failing_available_streams = {
                    'cbit__Clearbit__c', 'Order', 'Account', 'StaticResource', 'Contract',
                    'Contact', 'EntityDefinition', 'Quote', 'Organization', 'Scontrol', 'Location',
                    'ContentNote', 'Document', 'Attachment', 'MobileApplicationDetail',
                    'QuoteDocument', 'User', 'ContentVersion', 'MailmergeTemplate', 'Lead', 'ContactPointAddress'
                }

                # verify that all other fields have inclusion of available
                # This assumes there are no unsupported fields for SaaS sources
                if stream in failing_available_streams:  # BUG_2 comment to reproduce
                    self.LOGGER.warning("Skipping 'schema inclusion available' asssertion for %s", stream)
                else:  # BUG_2 comment to reproduce
                    self.assertTrue(
                        all({value.get("inclusion") == "available" for key, value
                             in schema["properties"].items()
                             if key not in actual_automatic_fields}),
                        msg="Not all non key properties are set to available in annotated schema")

                # verify that primary, replication and foreign keys
                # are given the inclusion of automatic in metadata.
                actual_automatic_fields = {item.get("breadcrumb", ["properties", None])[1]
                                           for item in metadata
                                           if item.get("metadata").get("inclusion") == "automatic"}
                self.assertEqual(expected_automatic_fields,
                                 actual_automatic_fields,
                                 msg="expected {} automatic fields but got {}".format(
                                     expected_automatic_fields,
                                     actual_automatic_fields))

                # verify that all other fields have inclusion of available
                # This assumes there are no unsupported fields for SaaS sources
                if stream in failing_available_streams:  # BUG_3
                    self.LOGGER.warning("Skipping 'metadata inclusion available' asssertion for %s", stream)
                else:  # BUG_3
                    self.assertTrue(
                        all({item.get("metadata").get("inclusion") == "available"
                             for item in metadata
                             if item.get("breadcrumb", []) != []
                             and item.get("breadcrumb", ["properties", None])[1]
                             not in actual_automatic_fields}),
                        msg="Not all non key properties are set to available in metadata")
