"""
Base class to use the new tap-tester framework
Setup expectations for test sub classes
Run discovery for as a prerequisite for most tests
"""
import unittest
import os
from datetime import timedelta
from datetime import datetime as dt

from tap_tester import connections, menagerie, runner, LOGGER
from tap_tester.base_suite_tests.base_case import BaseCase


class SFBaseTest(BaseCase):

    salesforce_api = "BULK"

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
            'start_date': '2020-11-23T00:00:00Z',
            'instance_url': 'https://singer2-dev-ed.my.salesforce.com',
            'select_fields_by_default': 'true',
            'quota_percent_total': '80',
            'api_type': self.salesforce_api,
            'is_sandbox': 'false'
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

    @staticmethod
    def expected_metadata():
        """The expected streams and metadata about the streams"""
        default = {
            BaseCase.PRIMARY_KEYS: {"Id"},
            BaseCase.REPLICATION_METHOD: BaseCase.INCREMENTAL,
            BaseCase.REPLICATION_KEYS: {"SystemModstamp"}
        }
        default_full = {
            BaseCase.PRIMARY_KEYS: {"Id"},
            BaseCase.REPLICATION_METHOD: BaseCase.FULL_TABLE,
        }

        incremental_created_date = {
            BaseCase.REPLICATION_KEYS: {'CreatedDate'},
            BaseCase.PRIMARY_KEYS: {'Id'},
            BaseCase.REPLICATION_METHOD: BaseCase.INCREMENTAL,
        }

        incremental_last_modified = {
            BaseCase.PRIMARY_KEYS: {'Id'},
            BaseCase.REPLICATION_KEYS: {'LastModifiedDate'},
            BaseCase.REPLICATION_METHOD: BaseCase.INCREMENTAL,
        }
        return {
            'AIApplication': default,  # removed # 6/13/2022 added back 7/10/2022
            'AIApplicationConfig': default,  # removed # 6/13/2022 added back 7/10/2022
            'AIInsightAction': default,  # removed # 6/13/2022 added back 7/10/2022
            'AIInsightFeedback': default,  # removed # 6/13/2022 added back 7/10/2022
            'AIInsightReason': default,  # removed # 6/13/2022 added back 7/10/2022
            'AIInsightValue': default,  # removed # 6/13/2022 added back 7/10/2022
            'AIRecordInsight': default,  # removed # 6/13/2022 added back 7/10/2022
            'Account': default,
            'AccountCleanInfo': default,  # new
            'AccountContactRole': default,
            'AccountFeed': default,
            'AccountHistory': incremental_created_date,
            'AccountPartner': default,
            'AccountShare': incremental_last_modified,
            'ActionLinkGroupTemplate': default,
            'ActionLinkTemplate': default,
            'ActiveFeatureLicenseMetric': default,
            'ActivePermSetLicenseMetric': default,
            'ActiveProfileMetric': default,
            'AdditionalNumber': default,
            'AlternativePaymentMethod': default,  # new
            'AlternativePaymentMethodShare': incremental_last_modified,  # new
            'ApexClass': default,
            'ApexComponent': default,
            'ApexEmailNotification': default,  # new
            'ApexLog': default,  # new
            'ApexPage': default,
            'ApexPageInfo': default_full,
            'ApexTestQueueItem': default,
            'ApexTestResult': default,
            'ApexTestResultLimits': default,
            'ApexTestRunResult': default,
            'ApexTestSuite': default,
            'ApexTrigger': default,
            'ApiAnomalyEventStore': default,  # new
            'ApiAnomalyEventStoreFeed': default,  # new
            'ApiEvent': incremental_created_date,  # new
            'AppAnalyticsQueryRequest': default,  # new
            'AppDefinition': default_full,
            'AppMenuItem': default,
            'AppUsageAssignment': default,  # removed # 6/13/2022 added back 7/10/2022
            'AppointmentAssignmentPolicy': default,  # new
            'AppointmentScheduleAggr': default,  # new
            'AppointmentScheduleLog': default,  # new
            'AppointmentSchedulingPolicy': default,  # new
            'AppointmentTopicTimeSlot': default,  # new
            'AppointmentTopicTimeSlotFeed': default,  # new
            'AppointmentTopicTimeSlotHistory': incremental_created_date,  # new
            'Asset': default,
            'AssetAction': default,  # new
            'AssetActionSource': default,  # new
            'AssetFeed': default,
            'AssetHistory': incremental_created_date,
            'AssetRelationship': default,
            'AssetRelationshipFeed': default,
            'AssetRelationshipHistory': incremental_created_date,
            'AssetShare': incremental_last_modified,
            'AssetStatePeriod': default,  # new
            'AssignedResource': default,  # new
            'AssignedResourceFeed': default,  # new
            'AssignmentRule': default,
            'AssociatedLocation': default,
            'AssociatedLocationHistory': incremental_created_date,
            'AsyncApexJob': incremental_created_date,
            'AsyncOperationLog': default,  # new
            'Attachment': default,
            'AuraDefinition': default,
            'AuraDefinitionBundle': default,
            'AuraDefinitionBundleInfo': default_full,
            'AuraDefinitionInfo': incremental_last_modified,
            'AuthConfig': default,
            'AuthConfigProviders': default,
            'AuthProvider': incremental_created_date,  # new
            'AuthSession': incremental_last_modified,
            'AuthorizationForm': default,
            'AuthorizationFormConsent': default,
            'AuthorizationFormConsentHistory': incremental_created_date,
            'AuthorizationFormConsentShare': incremental_last_modified,
            'AuthorizationFormDataUse': default,
            'AuthorizationFormDataUseHistory': incremental_created_date,
            'AuthorizationFormDataUseShare': incremental_last_modified,
            'AuthorizationFormHistory': incremental_created_date,
            'AuthorizationFormShare': incremental_last_modified,
            'AuthorizationFormText': default,
            'AuthorizationFormTextFeed': default,
            'AuthorizationFormTextHistory': incremental_created_date,
            'BackgroundOperation': default,  # new
            'BrandTemplate': default,
            'BrandingSet': default,
            'BrandingSetProperty': default,
            'BulkApiResultEventStore': incremental_created_date,  # new
            'BusinessHours': default,
            'BusinessProcess': default,
            'Calendar': default,
            'CalendarView': default,
            'CalendarViewShare': incremental_last_modified,
            'CallCenter': default,
            'CallCoachingMediaProvider': default,
            'Campaign': default,
            'CampaignFeed': default,
            'CampaignHistory': incremental_created_date,
            'CampaignMember': default,
            'CampaignMemberStatus': default,
            'CampaignShare': incremental_last_modified,
            'CardPaymentMethod': default,  # new
            'Case': default,
            'CaseComment': default,
            'CaseContactRole': default,
            'CaseFeed': default,
            'CaseHistory': incremental_created_date,
            'CaseMilestone': default,  # new
            'CaseShare': incremental_last_modified,
            'CaseSolution': default,
            'CaseTeamMember': default,
            'CaseTeamRole': default,
            'CaseTeamTemplate': default,
            'CaseTeamTemplateMember': default,
            'CaseTeamTemplateRecord': default,
            'CategoryData': default,  # new
            'CategoryNode': default,
            'ChatterActivity': default,
            'ChatterExtension': default,
            'ChatterExtensionConfig': default,
            'ClientBrowser': incremental_created_date,
            'CollaborationGroup': default,
            'CollaborationGroupFeed': default,
            'CollaborationGroupMember': default,
            'CollaborationGroupMemberRequest': default,
            'CollaborationInvitation': default,
            'CommSubscription': default,
            'CommSubscriptionChannelType': default,
            'CommSubscriptionChannelTypeFeed': default,
            'CommSubscriptionChannelTypeHistory': incremental_created_date,
            'CommSubscriptionChannelTypeShare': incremental_last_modified,
            'CommSubscriptionConsent': default,
            'CommSubscriptionConsentFeed': default,
            'CommSubscriptionConsentHistory': incremental_created_date,
            'CommSubscriptionConsentShare': incremental_last_modified,
            'CommSubscriptionFeed': default,
            'CommSubscriptionHistory': incremental_created_date,
            'CommSubscriptionShare': incremental_last_modified,
            'CommSubscriptionTiming': default,
            'CommSubscriptionTimingFeed': default,
            'CommSubscriptionTimingHistory': incremental_created_date,
            'Community': default,
            'ConferenceNumber': default,
            'ConnectedApplication': default,
            'ConsumptionRate': default,  # new
            'ConsumptionRateHistory': incremental_created_date,  # new
            'ConsumptionSchedule': default,  # new
            'ConsumptionScheduleFeed': default,  # new
            'ConsumptionScheduleHistory': incremental_created_date,  # new
            'ConsumptionScheduleShare': incremental_last_modified,  # new
            'Contact': default,
            'ContactCleanInfo': default,  # new
            'ContactFeed': default,
            'ContactHistory': incremental_created_date,
            'ContactPointAddress': default,
            'ContactPointAddressHistory': incremental_created_date,
            'ContactPointAddressShare': incremental_last_modified,
            'ContactPointConsent': default,
            'ContactPointConsentHistory': incremental_created_date,
            'ContactPointConsentShare': incremental_last_modified,
            'ContactPointEmail': default,
            'ContactPointEmailHistory': incremental_created_date,
            'ContactPointEmailShare': incremental_last_modified,
            'ContactPointPhone': default,
            'ContactPointPhoneHistory': incremental_created_date,
            'ContactPointPhoneShare': incremental_last_modified,
            'ContactPointTypeConsent': default,
            'ContactPointTypeConsentHistory': incremental_created_date,
            'ContactPointTypeConsentShare': incremental_last_modified,
            'ContactRequest': default,  # new
            'ContactRequestShare': incremental_last_modified,  # new
            'ContactShare': incremental_last_modified,
            'ContentAsset': default,
            'ContentDistribution': default,
            'ContentDistributionView': default,
            'ContentDocument': default,
            'ContentDocumentFeed': default,
            'ContentDocumentHistory': incremental_created_date,
            'ContentDocumentSubscription': default_full,  # new
            'ContentFolder': default,
            'ContentFolderLink': default_full,
            'ContentNotification': incremental_created_date,  # new
            'ContentTagSubscription': default_full,  # new
            'ContentUserSubscription': default_full,  # new
            'ContentVersion': default,
            'ContentVersionComment': incremental_created_date,  # new
            'ContentVersionHistory': incremental_created_date,
            'ContentVersionRating': incremental_last_modified,  # new
            'ContentWorkspace': default,
            'ContentWorkspaceDoc': default,
            'ContentWorkspaceMember': incremental_created_date,
            'ContentWorkspacePermission': default,
            'ContentWorkspaceSubscription': default_full,  # new
            'Contract': default,
            'ContractContactRole': default,
            'ContractFeed': default,
            'ContractHistory': incremental_created_date,
            'ContractLineItem': default,  # new
            'ContractLineItemHistory': incremental_created_date,  # new
            'ConversationEntry': default,  # new
            'CorsWhitelistEntry': default,  # new
            'CredentialStuffingEventStore': default,  # new
            'CredentialStuffingEventStoreFeed': default,  # new
            'CreditMemo': default,  # new
            'CreditMemoFeed': default,  # new
            'CreditMemoHistory': incremental_created_date,  # new
            'CreditMemoInvApplication': default,  # new
            'CreditMemoInvApplicationFeed': default,  # new 6/13/2022
            'CreditMemoInvApplicationHistory': incremental_created_date,  # new 6/13/2022
            'CreditMemoLine': default,  # new
            'CreditMemoLineFeed': default,  # new
            'CreditMemoLineHistory': incremental_created_date,  # new
            'CreditMemoShare': incremental_last_modified,  # new
            'CronJobDetail': default_full,
            'CronTrigger': incremental_created_date,
            'CspTrustedSite': default,  # new
            'CustomBrand': incremental_last_modified,
            'CustomBrandAsset': incremental_last_modified,
            'CustomHelpMenuItem': default,
            'CustomHelpMenuSection': default,
            'CustomHttpHeader': default,
            'CustomNotificationType': default,
            'CustomObjectUserLicenseMetrics': default,
            'CustomPermission': default,
            'CustomPermissionDependency': default,
            'DandBCompany': default,  # new
            'Dashboard': default,
            'DashboardComponent': default_full,
            'DashboardComponentFeed': default,
            'DashboardFeed': default,
            'DataAssessmentFieldMetric': default,  # new
            'DataAssessmentMetric': default,  # new
            'DataAssessmentValueMetric': default,  # new
            'DataUseLegalBasis': default,
            'DataUseLegalBasisHistory': incremental_created_date,
            'DataUseLegalBasisShare': incremental_last_modified,
            'DataUsePurpose': default,
            'DataUsePurposeHistory': incremental_created_date,
            'DataUsePurposeShare': incremental_last_modified,
            'DatacloudAddress': default_full,
            'DatacloudCompany': default_full,  # new
            'DatacloudContact': default_full,  # new
            'DatacloudDandBCompany': default_full,  # new
            'DatacloudOwnedEntity': default,  # new
            'DatacloudPurchaseUsage': default,  # new
            'DeleteEvent': default,
            'DigitalWallet': default,  # new
            'Document': default,
            'DocumentAttachmentMap': incremental_created_date,
            'Domain': default,
            'DomainSite': default,
            'DuplicateRecordItem': default,  # new
            'DuplicateRecordSet': default,  # new
            'DuplicateRule': default,
            'EmailCapture': default,  # new
            'EmailDomainFilter': default,  # new
            'EmailDomainKey': default,  # new
            'EmailMessage': default,
            'EmailMessageRelation': default,
            'EmailRelay': default,  # new
            'EmailServicesAddress': default,
            'EmailServicesFunction': default,
            'EmailTemplate': default,
            'EmbeddedServiceDetail': default_full,
            'EmbeddedServiceLabel': default_full,
            'EngagementChannelType': default,
            'EngagementChannelTypeFeed': default,
            'EngagementChannelTypeHistory': incremental_created_date,
            'EngagementChannelTypeShare': incremental_last_modified,
            'EnhancedLetterhead': default,
            'EnhancedLetterheadFeed': default,
            'Entitlement': default,  # new
            'EntitlementContact': default,  # new
            'EntitlementFeed': default,  # new
            'EntitlementHistory': incremental_created_date,  # new
            'EntitlementTemplate': default,  # new
            'EntityDefinition': incremental_last_modified,
            'EntityMilestone': default,  # new
            'EntityMilestoneFeed': default,  # new
            'EntityMilestoneHistory': incremental_created_date,  # new
            'EntitySubscription': incremental_created_date,
            'Event': default,
            'EventBusSubscriber': default_full,
            'EventFeed': default,
            'EventLogFile': default,  # new
            'EventRelation': default,
            'ExpressionFilter': default,
            'ExpressionFilterCriteria': default,
            'ExternalDataSource': default,
            'ExternalDataUserAuth': default,
            'ExternalEvent': default,
            'ExternalEventMapping': default,
            'ExternalEventMappingShare': incremental_last_modified,
            'FeedAttachment': default_full,
            'FeedComment': default,
            'FeedItem': default,
            'FeedPollChoice': incremental_created_date,  # new
            'FeedPollVote': incremental_last_modified,  # new
            'FeedRevision': default,
            'FieldPermissions': default,
            'FieldSecurityClassification': default,  # new
            'FileSearchActivity': default,  # new
            'FinanceBalanceSnapshot': default,  # new
            'FinanceBalanceSnapshotShare': incremental_last_modified,  # new
            'FinanceTransaction': default,  # new
            'FinanceTransactionShare': incremental_last_modified,  # new
            'FiscalYearSettings': default,
            'FlowDefinitionView': incremental_last_modified,
            'FlowInterview': default,
            'FlowInterviewLog': default,
            'FlowInterviewLogEntry': default,
            'FlowInterviewLogShare': incremental_last_modified,
            'FlowInterviewShare': incremental_last_modified,
            'FlowRecordRelation': default,
            'FlowStageRelation': default,
            'Folder': default,
            'FormulaFunction': default_full,
            'FormulaFunctionAllowedType': default_full,
            'FormulaFunctionCategory': default_full,
            'GrantedByLicense': default,
            'Group': default,
            'GroupMember': default,
            'GtwyProvPaymentMethodType': default,  # new
            'Holiday': default,
            'IPAddressRange': default,  # new
            'Idea': default,
            'IdentityProviderEventStore': incremental_created_date,  # new
            'IdentityVerificationEvent': incremental_created_date,  # new
            'IdpEventLog': default_full,  # new
            'IframeWhiteListUrl': default,
            'Image': default,  # new
            'ImageFeed': default,  # new
            'ImageHistory': incremental_created_date,  # new
            'ImageShare': incremental_last_modified,  # new
            'Individual': default,
            'IndividualHistory': incremental_created_date,
            'IndividualShare': incremental_last_modified,
            'InstalledMobileApp': default,
            'Invoice': default,  # new
            'InvoiceFeed': default,  # new
            'InvoiceHistory': incremental_created_date,  # new
            'InvoiceLine': default,  # new
            'InvoiceLineFeed': default,  # new
            'InvoiceLineHistory': incremental_created_date,  # new
            'InvoiceShare': incremental_last_modified,  # new
            'KnowledgeableUser': default,
            'Lead': default,
            'LeadCleanInfo': default,  # new
            'LeadFeed': default,
            'LeadHistory': incremental_created_date,
            'LeadShare': incremental_last_modified,
            'LeadStatus': default,
            'LegalEntity': default,  # new
            'LegalEntityFeed': default,  # new
            'LegalEntityHistory': incremental_created_date,  # new
            'LegalEntityShare': incremental_last_modified,  # new
            'LightningExitByPageMetrics': default,  # new
            'LightningExperienceTheme': default,
            'LightningOnboardingConfig': default,
            'LightningToggleMetrics': default,  # new
            'LightningUriEvent': default_full,  # new
            'LightningUsageByAppTypeMetrics': default,  # new
            'LightningUsageByBrowserMetrics': default,  # new
            'LightningUsageByFlexiPageMetrics': default,  # new
            'LightningUsageByPageMetrics': default,  # new
            'ListEmail': default,  # new
            'ListEmailIndividualRecipient': default,  # new
            'ListEmailRecipientSource': default,  # new
            'ListEmailShare': incremental_last_modified,  # new
            'ListView': default,
            'ListViewChart': default,
            'ListViewEvent': incremental_created_date,  # new
            'LiveChatSensitiveDataRule': default,  # new
            'Location': default,
            'LocationFeed': default,
            'LocationGroup': default,  # new
            'LocationGroupAssignment': default,  # new
            'LocationGroupFeed': default,  # new
            'LocationGroupHistory': incremental_created_date,  # new
            'LocationGroupShare': incremental_last_modified,  # new
            'LocationHistory': incremental_created_date,
            'LocationShare': incremental_last_modified,
            'LoginAsEvent': incremental_created_date,  # new
            'LoginEvent': default_full,  # new
            'LoginGeo': default,  # new
            'LoginHistory': {BaseCase.PRIMARY_KEYS: {'Id'}, BaseCase.REPLICATION_KEYS: {'LoginTime'},BaseCase.REPLICATION_METHOD: BaseCase.INCREMENTAL,},
            'LoginIp': incremental_created_date,
            'LogoutEvent': default_full,  # new
            # 'MLField': default,  # removed # 6/13/2022 added back 7/10/2022, removed 06/12/2023
            'MLPredictionDefinition': default,  # removed # 6/13/2022 added back 7/10/2022
            'Macro': default,
            'MacroHistory': incremental_created_date,
            'MacroInstruction': default,
            'MacroShare': incremental_last_modified,
            'MacroUsage': default,
            'MacroUsageShare': incremental_last_modified,
            'MailmergeTemplate': default,
            'MatchingInformation': default,
            'MatchingRule': default,
            'MatchingRuleItem': default,
            'MessagingChannel': default,  # new
            'MessagingChannelSkill': default,  # new
            'MessagingConfiguration': default,  # new
            'MessagingDeliveryError': default,  # new
            'MessagingEndUser': default,  # new
            'MessagingEndUserHistory': incremental_created_date,  # new
            'MessagingEndUserShare': incremental_last_modified,  # new
            'MessagingLink': default,  # new
            'MessagingSession': default,  # new
            'MessagingSessionFeed': default,  # new
            'MessagingSessionHistory': incremental_created_date,  # new
            'MessagingSessionShare': incremental_last_modified,  # new
            'MessagingTemplate': default,  # new
            'MilestoneType': default,  # new
            'MobileApplicationDetail': default,
            'MsgChannelLanguageKeyword': default,  # new
            'MutingPermissionSet': default,
            'MyDomainDiscoverableLogin': default,
            'NamedCredential': default,
            'Note': default,
            'OauthCustomScope': default,  # new
            'OauthCustomScopeApp': default,  # new
            'OauthToken': incremental_created_date,
            'ObjectPermissions': default,
            'OnboardingMetrics': default,
            'OperatingHours': default,  # new
            'OperatingHoursFeed': default,  # new
            'Opportunity': default,
            'OpportunityCompetitor': default,
            'OpportunityContactRole': default,
            'OpportunityFeed': default,
            'OpportunityFieldHistory': incremental_created_date,
            'OpportunityHistory': default,
            'OpportunityLineItem': default,
            'OpportunityPartner': default,
            'OpportunityShare': incremental_last_modified,
            'OpportunityStage': default,
            'Order': default,
            'OrderFeed': default,
            'OrderHistory': incremental_created_date,
            'OrderItem': default,
            'OrderItemFeed': default,
            'OrderItemHistory': incremental_created_date,
            'OrderShare': incremental_last_modified,
            'OrgDeleteRequest': default,  # new
            'OrgDeleteRequestShare': incremental_last_modified,  # new
            'OrgMetric': default,  # new
            'OrgMetricScanResult': default,  # new
            'OrgMetricScanSummary': default,  # new
            'OrgWideEmailAddress': default,
            'Organization': default,
            'PackageLicense': default,  # new
            'Partner': default,
            'PartyConsent': default,
            'PartyConsentFeed': default,
            'PartyConsentHistory': incremental_created_date,
            'PartyConsentShare': incremental_last_modified,
            'Payment': default,  # new
            'PaymentAuthAdjustment': default,  # new
            'PaymentAuthorization': default,  # new
            'PaymentGateway': default,  # new
            'PaymentGatewayLog': default,  # new
            'PaymentGatewayProvider': default,  # new
            'PaymentGroup': default,  # new
            'PaymentLineInvoice': default,  # new
            'PaymentMethod': default,  # new
            'Period': default,
            'PermissionSet': default,
            'PermissionSetAssignment': default,
            'PermissionSetGroup': default,
            'PermissionSetGroupComponent': default,
            'PermissionSetLicense': default,
            'PermissionSetLicenseAssign': default,
            'PermissionSetTabSetting': default,
            'PlatformCachePartition': default,
            'PlatformCachePartitionType': default,
            'PlatformEventUsageMetric': default_full,
            'Pricebook2': default,
            'Pricebook2History': incremental_created_date,
            'PricebookEntry': default,
            'PricebookEntryHistory': incremental_created_date,
            'ProcessDefinition': default,
            'ProcessException': default,  # new
            'ProcessExceptionShare': incremental_last_modified,  # new
            'ProcessInstance': default,
            'ProcessInstanceNode': default,
            'ProcessInstanceStep': default,
            'ProcessInstanceWorkitem': default,
            'ProcessNode': default,
            'Product2': default,
            'Product2Feed': default,
            'Product2History': incremental_created_date,
            'ProductConsumptionSchedule': default,  # new
            'ProductEntitlementTemplate': default,  # new
            'Profile': default,
            'Prompt': default,
            'PromptAction': default,  # new
            'PromptActionShare': incremental_last_modified,  # new
            'PromptError': default,  # new
            'PromptErrorShare': incremental_last_modified,  # new
            'PromptVersion': default,
            'Publisher': default_full,
            'PushTopic': default,  # new
            'QueueSobject': default,
            'QuickText': default,
            'QuickTextHistory': incremental_created_date,
            'QuickTextShare': incremental_last_modified,
            'QuickTextUsage': default,
            'QuickTextUsageShare': incremental_last_modified,
            'Recommendation': default,
            'RecommendationResponse': default,  # new 6/13/2022
            'RecordAction': default,
            'RecordType': default,
            'RedirectWhitelistUrl': default,
            'Refund': default,  # new
            'RefundLinePayment': default,  # new
            'Report': default,
            'ReportAnomalyEventStore': default,  # new
            'ReportAnomalyEventStoreFeed': default,  # new
            'ReportEvent': default_full,  # new
            'ReportFeed': default,
            'ResourceAbsence': default,  # new
            'ResourceAbsenceFeed': default,  # new
            'ResourceAbsenceHistory': incremental_created_date,  # new
            'ResourcePreference': default,  # new
            'ResourcePreferenceFeed': default,  # new
            'ResourcePreferenceHistory': incremental_created_date,  # new
            'ReturnOrder': default,  # new
            'ReturnOrderFeed': default,  # new
            'ReturnOrderHistory': incremental_created_date,  # new
            'ReturnOrderItemAdjustment': default,  # new
            'ReturnOrderItemTax': default,  # new
            'ReturnOrderLineItem': default,  # new
            'ReturnOrderLineItemFeed': default,  # new
            'ReturnOrderLineItemHistory': incremental_created_date,  # new
            'ReturnOrderShare': incremental_last_modified,  # new
            'SPSamlAttributes': default,  # new
            'SamlSsoConfig': default,
            'Scontrol': default,
            'SearchPromotionRule': default,  # new
            'SecurityCustomBaseline': default,  # new
            'ServiceAppointment': default,  # new
            'ServiceAppointmentFeed': default,  # new
            'ServiceAppointmentHistory': incremental_created_date,  # new
            'ServiceAppointmentShare': incremental_last_modified,  # new
            'ServiceAppointmentStatus': default,  # new
            'ServiceContract': default,  # new
            'ServiceContractFeed': default,  # new
            'ServiceContractHistory': incremental_created_date,  # new
            'ServiceContractShare': incremental_last_modified,  # new
            'ServiceResource': default,  # new
            'ServiceResourceFeed': default,  # new
            'ServiceResourceHistory': incremental_created_date,  # new
            'ServiceResourceShare': incremental_last_modified,  # new
            'ServiceResourceSkill': default,  # new
            'ServiceResourceSkillFeed': default,  # new
            'ServiceResourceSkillHistory': incremental_created_date,  # new
            'ServiceSetupProvisioning': default,  # new
            'ServiceTerritory': default,  # new
            'ServiceTerritoryFeed': default,  # new
            'ServiceTerritoryHistory': incremental_created_date,  # new
            'ServiceTerritoryMember': default,  # new
            'ServiceTerritoryMemberFeed': default,  # new
            'ServiceTerritoryMemberHistory': incremental_created_date,  # new
            'ServiceTerritoryShare': incremental_last_modified,  # new
            'ServiceTerritoryWorkType': default,  # new
            'ServiceTerritoryWorkTypeFeed': default,  # new
            'ServiceTerritoryWorkTypeHistory': incremental_created_date,  # new
            'SessionHijackingEventStore': default,  # new
            'SessionHijackingEventStoreFeed': default,  # new
            'SessionPermSetActivation': default,
            'SetupAssistantStep': default,  # new
            'SetupAuditTrail': incremental_created_date,
            'SetupEntityAccess': default,
            'Site': default,
            'SiteFeed': default,
            'SiteHistory': incremental_created_date,
            'SiteIframeWhiteListUrl': default,
            'SiteRedirectMapping': default,
            'Skill': default,  # new
            'SkillRequirement': default,  # new
            'SkillRequirementFeed': default,  # new
            'SkillRequirementHistory': incremental_created_date,  # new
            'SlaProcess': default,  # new
            'Solution': default,
            'SolutionFeed': default,
            'SolutionHistory': incremental_created_date,
            'Stamp': default,
            'StampAssignment': default,
            'StaticResource': default,
            'StreamingChannel': default,  # new
            'StreamingChannelShare': incremental_last_modified,  # new
            'TabDefinition': default_full,
            'Task': default,
            'TaskFeed': default,
            'TenantUsageEntitlement': default,
            'TestSuiteMembership': default,
            'ThirdPartyAccountLink': default_full,
            'ThreatDetectionFeedback': default,  # new
            'ThreatDetectionFeedbackFeed': default,  # new
            'TimeSlot': default,  # new
            'TodayGoal': default,
            'TodayGoalShare': incremental_last_modified,
            'Topic': default,
            'TopicAssignment': default,
            'TopicFeed': default,
            'TopicUserEvent': incremental_created_date,  # new
            'TransactionSecurityPolicy': default,  # new
            'Translation': default,
            'UiFormulaCriterion': default,
            'UiFormulaRule': default,
            'UriEvent': default_full,  # new
            'User': default,
            'UserAppInfo': default,
            'UserAppMenuCustomization': default,
            'UserAppMenuCustomizationShare': incremental_last_modified,
            'UserAppMenuItem': default_full,
            'UserEmailPreferredPerson': default,
            'UserEmailPreferredPersonShare': incremental_last_modified,
            'UserFeed': default,
            'UserLicense': default,
            'UserListView': default,
            'UserListViewCriterion': default,
            'UserLogin': incremental_last_modified,
            'UserPackageLicense': default,  # new
            'UserPermissionAccess': default_full,
            'UserPreference': default,
            'UserProvAccount': default,  # new
            'UserProvAccountStaging': default,  # new
            'UserProvMockTarget': default,  # new
            'UserProvisioningConfig': default,  # new
            'UserProvisioningLog': default,  # new
            'UserProvisioningRequest': default,  # new
            'UserProvisioningRequestShare': incremental_last_modified,  # new
            'UserRole': default,
            'UserSetupEntityAccess': default_full,
            'UserShare': incremental_last_modified,
            'VerificationHistory': default,  # new
            'VisualforceAccessMetrics': default,  # new
            'WaveAutoInstallRequest': default,
            'WaveCompatibilityCheckItem': default,
            'WebLink': default,  # new
            'WorkOrder': default,  # new
            'WorkOrderFeed': default,  # new
            'WorkOrderHistory': incremental_created_date,  # new
            'WorkOrderLineItem': default,  # new
            'WorkOrderLineItemFeed': default,  # new
            'WorkOrderLineItemHistory': incremental_created_date,  # new
            'WorkOrderLineItemStatus': default,  # new
            'WorkOrderShare': incremental_last_modified,  # new
            'WorkOrderStatus': default,  # new
            'WorkType': default,  # new
            'WorkTypeFeed': default,  # new
            'WorkTypeGroup': default,  # new
            'WorkTypeGroupFeed': default,  # new
            'WorkTypeGroupHistory': incremental_created_date,  # new
            'WorkTypeGroupMember': default,  # new
            'WorkTypeGroupMemberFeed': default,  # new
            'WorkTypeGroupMemberHistory': incremental_created_date,  # new
            'WorkTypeGroupShare': incremental_last_modified,  # new
            'WorkTypeHistory': incremental_created_date,  # new
            'WorkTypeShare': incremental_last_modified,  # new
            #'RecentlyViewed': default_full,  # new TODO verify this is not a bug  - Stream not discovered
            #'TaskPriority': default,  # new TODO  - Stream not discovered
            #'DeclinedEventRelation': default,  # new TODO  - Stream not discovered
            #'AcceptedEventRelation': default,  # new TODO -  - Stream not discovered
            #'OrderStatus': default,  # new TODO  - Stream not discovered
            #'SolutionStatus': default,  # new TODO  - Stream not discovered
            #'CaseStatus': default,  # new TODO  - Stream not discovered
            #'TaskStatus': default,  # new TODO - Stream not discovered
            # 'PartnerRole': default,  # new TODO - Stream not discovered
            #'ContractStatus': default,  # new TODO  - Stream not discovered
            #'UndecidedEventRelation': default,  # new TODO  - Stream not discovered
            # Newly discovered as of 2/12/2022
            'BriefcaseAssignment': default,
            'BriefcaseDefinition': default,
            'BriefcaseRule': default,
            'BriefcaseRuleFilter': default,
            'CartCheckoutSession': default,  # removed # 6/13/2022
            'CartDeliveryGroup': default,  # removed # 6/13/2022
            'CartItem': default,  # removed # 6/13/2022
            'CartItemPriceAdjustment': default, # added # 10/19/2022
            'CartRelatedItem': default,  # removed # 6/13/2022
            'CartTax': default,  # removed # 6/13/2022
            'CartValidationOutput': default,  # removed # 6/13/2022
            'Conversation': incremental_last_modified,  # added # 4/11/2023
            'ConversationParticipant': incremental_last_modified,  # added # 4/11/2023
            'Coupon': default,  # added # 10/18/2022
            'CouponHistory': incremental_created_date,  # added # 10/18/2022
            'CouponShare': incremental_last_modified, # added # 10/18/2022
            'OperatingHoursHoliday': default,
            'OperatingHoursHolidayFeed': default,
            #'PaymentFeed': default,  # added # 10/18/2022 # removed 01/13/23
            'PermissionSetEventStore': incremental_created_date,
            'ProductAttribute': default,  # added # 10/18/2022
            'ProductAttributeSet': default,  # added # 10/18/2022
            'ProductAttributeSetItem': default,  # added # 10/18/2022
            'ProductAttributeSetProduct': default,  # added # 10/18/2022
            'Promotion': default,  # added # 10/18/2022
            'PromotionFeed': default,  # added # 10/18/2022
            'PromotionHistory': incremental_created_date,  # added # 10/18/2022
            'PromotionSegment': default,  # added # 10/18/2022
            'PromotionMarketSegment': default,  # added # 10/18/2022
            'PromotionMarketSegmentFeed': default,  # added # 10/18/2022
            'PromotionMarketSegmentHistory': incremental_created_date,  # added # 10/18/2022
            'PromotionSegmentHistory': incremental_created_date,  # added # 10/18/2022
            'PromotionSegmentSalesStore': default,  # added # 10/18/2022
            'PromotionSegmentSalesStoreHistory': incremental_created_date,  # added # 10/18/2022
            'PromotionSegmentShare': incremental_last_modified,  # added # 10/18/2022
            'PromotionShare': incremental_last_modified,  # added # 10/18/2022
            'PromotionTarget': default,  # added # 10/18/2022
            'PromotionTargetHistory': incremental_created_date,  # added # 10/18/2022
            'PromotionQualifier': default,  # added # 10/18/2022
            'PromotionQualifierHistory': incremental_created_date,  # added # 10/18/2022
            'SalesStore': default_full,  # added # 02/15/2023
            'Shift': default,
            'ShiftFeed': default,
            'ShiftHistory': incremental_created_date,
            'ShiftShare': incremental_last_modified,
            'ShiftStatus': default,
            'TapTester__c': default, # added 8/4/2023
            'TapTester__Share': incremental_last_modified, # added 8/4/2023
            'WebCart': default,  # re-added # 10/18/2022
            'WebCartAdjustmentGroup': default,  # added # 10/18/2022
            'WebCartHistory': incremental_created_date,  # re-added # 10/18/2022
            'WebCartShare': incremental_last_modified,  # re-added # 10/18/2022
            'WebStore': default,  # re-added # 10/18/2022
            'WebStoreShare': incremental_last_modified,  # re-added # 10/18/2022
            'WorkPlan': default,
            'WorkPlanFeed': default,
            'WorkPlanHistory': incremental_created_date,
            'WorkPlanShare': incremental_last_modified,
            'WorkPlanTemplate': default,
            'WorkPlanTemplateEntry': default,
            'WorkPlanTemplateEntryFeed': default,
            'WorkPlanTemplateEntryHistory': incremental_created_date,
            'WorkPlanTemplateFeed': default,
            'WorkPlanTemplateHistory': incremental_created_date,
            'WorkPlanTemplateShare': incremental_last_modified,
            'WorkStep': default,
            'WorkStepFeed': default,
            'WorkStepHistory': incremental_created_date,
            'WorkStepStatus': default,
            'WorkStepTemplate': default,
            'WorkStepTemplateFeed': default,
            'WorkStepTemplateHistory': incremental_created_date,
            'WorkStepTemplateShare': incremental_last_modified,
        }

    def rest_only_streams(self):
        """A group of streams that is only discovered when the REST API is in use."""
        return {
            'CaseStatus',
            'DeclinedEventRelation',
            'RecentlyViewed',
            'SolutionStatus',
            'TaskStatus',
            'OrderStatus',
            'AcceptedEventRelation',
            'ContractStatus',
            'PartnerRole',
            'TaskPriority',
            'UndecidedEventRelation',
        }

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

    @classmethod
    def setUpClass(cls):
        """Verify that you have set the prerequisites to run the tap (creds, etc.)"""
        missing_envs = [x for x in ['TAP_SALESFORCE_CLIENT_ID',
                                    'TAP_SALESFORCE_CLIENT_SECRET',
                                    'TAP_SALESFORCE_REFRESH_TOKEN']
                        if os.getenv(x) is None]

        if missing_envs:
            raise Exception("set environment variables")
