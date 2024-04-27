#!/usr/bin/env python3
from contextlib import suppress
from datetime import datetime, timezone
from collections.abc import Iterable
from itertools import chain
import asyncio
import random
import string
import json
import sys
import os
import re

from gql import Client, gql
from gql.transport.aiohttp import AIOHTTPTransport
from gql.transport.exceptions import TransportQueryError
import asyncpg

MAX_OFFSET = 2000
MAX_RESULTS = 5000

# TODO: automatically parse this stuff from accepted_schema.sql
DOCUMENT_FIELDS = (
    "documentId",
    "collectionName",
    "fieldName",
    "editedAt",
    "updateType",
    "version",
    "commitMessage",
    "userId",
    "draft",
    "html",
    "markdown",
    "draftJS",
    "ckEditorMarkup",
    "wordCount",
    "changeMetrics",
    "googleDocMetadata",
    "_id",
    "schemaVersion",
    "createdAt",
    "legacyData",
    "currentUserVote",
    "currentUserExtendedVote",
    "voteCount",
    "baseScore",
    "extendedScore",
    "score",
    "afBaseScore",
    "afExtendedScore",
    "afVoteCount",
)

# USER_FIELDS = ("_id", "username", "emails", "isAdmin", "profile", "services", "displayName", "previousDisplayName", "email", "slug", "noindex", "groups", "lwWikiImport", "theme", "lastUsedTimezone", "legacy", "commentSorting", "sortDraftsBy", "reactPaletteStyle", "noKibitz", "showHideKarmaOption", "showPostAuthorCard", "hideIntercom", "markDownPostEditor", "hideElicitPredictions", "hideAFNonMemberInitialWarning", "noSingleLineComments", "noCollapseCommentsPosts", "noCollapseCommentsFrontpage", "hideCommunitySection", "expandedFrontpageSections", "showCommunityInRecentDiscussion", "hidePostsRecommendations", "petrovOptOut", "acceptedTos", "hideNavigationSidebar", "currentFrontpageFilter", "frontpageFilterSettings", "hideFrontpageFilterSettingsDesktop", "allPostsTimeframe", "allPostsFilter", "allPostsSorting", "allPostsShowLowKarma", "allPostsIncludeEvents", "allPostsHideCommunity", "allPostsOpenSettings", "draftsListSorting", "draftsListShowArchived", "draftsListShowShared", "karma", "goodHeartTokens", "moderationStyle", "moderatorAssistance", "collapseModerationGuidelines", "bannedUserIds", "bannedPersonalUserIds", "bookmarkedPostsMetadata", "hiddenPostsMetadata", "legacyId", "deleted", "voteBanned", "nullifyVotes", "deleteContent", "auto_subscribe_to_my_posts", "auto_subscribe_to_my_comments", "autoSubscribeAsOrganizer", "notificationCommentsOnSubscribedPost", "notificationShortformContent", "notificationRepliesToMyComments", "notificationRepliesToSubscribedComments", "notificationSubscribedUserPost", "notificationSubscribedUserComment", "notificationPostsInGroups", "notificationSubscribedTagPost", "notificationSubscribedSequencePost", "notificationPrivateMessage", "notificationSharedWithMe", "notificationAlignmentSubmissionApproved", "notificationEventInRadius", "notificationKarmaPowersGained", "notificationRSVPs", "notificationGroupAdministration", "notificationCommentsOnDraft", "notificationPostsNominatedReview", "notificationSubforumUnread", "notificationNewMention", "notificationDialogueMessages", "notificationPublishedDialogueMessages", "notificationAddedAsCoauthor", "notificationDebateCommentsOnSubscribedPost", "notificationDebateReplies", "notificationDialogueMatch", "notificationNewDialogueChecks", "notificationYourTurnMatchForm", "hideDialogueFacilitation", "revealChecksToAdmins", "optedInToDialogueFacilitation", "showDialoguesList", "showMyDialogues", "showMatches", "showRecommendedPartners", "hideActiveDialogueUsers", "karmaChangeNotifierSettings", "emailSubscribedToCurated", "subscribedToDigest", "unsubscribeFromAll", "hideSubscribePoke", "hideMeetupsPoke", "hideHomeRHS", "frontpagePostCount", "sequenceCount", "sequenceDraftCount", "mongoLocation", "googleLocation", "location", "mapLocation", "mapLocationSet", "mapMarkerText", "htmlMapMarkerText", "nearbyEventsNotifications", "nearbyEventsNotificationsLocation", "nearbyEventsNotificationsMongoLocation", "nearbyEventsNotificationsRadius", "nearbyPeopleNotificationThreshold", "hideFrontpageMap", "hideTaggingProgressBar", "hideFrontpageBookAd", "hideFrontpageBook2019Ad", "hideFrontpageBook2020Ad", "sunshineNotes", "sunshineFlagged", "needsReview", "sunshineSnoozed", "snoozedUntilContentCount", "reviewedByUserId", "afKarma", "voteCount", "smallUpvoteCount", "smallDownvoteCount", "bigUpvoteCount", "bigDownvoteCount", "voteReceivedCount", "smallUpvoteReceivedCount", "smallDownvoteReceivedCount", "bigUpvoteReceivedCount", "bigDownvoteReceivedCount", "usersContactedBeforeReview", "fullName", "shortformFeedId", "viewUnreviewedComments", "partiallyReadSequences", "beta", "reviewVotesQuadratic", "reviewVotesQuadratic2019", "reviewVotesQuadratic2020", "defaultToCKEditor", "signUpReCaptchaRating", "oldSlugs", "noExpandUnreadCommentsReview", "postCount", "maxPostCount", "commentCount", "maxCommentCount", "tagRevisionCount", "abTestKey", "abTestOverrides", "reenableDraftJs", "walledGardenInvite", "hideWalledGardenUI", "walledGardenPortalOnboarded", "taggingDashboardCollapsed", "usernameUnset", "paymentEmail", "paymentInfo", "profileImageId", "jobTitle", "organization", "careerStage", "website", "fmCrosspostUserId", "linkedinProfileURL", "facebookProfileURL", "twitterProfileURL", "githubProfileURL", "profileTagIds", "organizerOfGroupIds", "programParticipation", "postingDisabled", "allCommentingDisabled", "commentingOnOtherUsersDisabled", "conversationsDisabled", "acknowledgedNewUserGuidelines", "subforumPreferredLayout", "allowDatadogSessionReplay", "afPostCount", "afCommentCount", "afSequenceCount", "afSequenceDraftCount", "reviewForAlignmentForumUserId", "afApplicationText", "afSubmittedApplication", "hideSunshineSidebar", "schemaVersion", "legacyData", "moderationGuidelines_latest", "howOthersCanHelpMe_latest", "howICanHelpOthers_latest", "biography_latest", "recommendationSettings")
USER_FIELDS = (
    "_id",
    "username",
    "emails",
    "isAdmin",
    "profile",
    "services",
    "displayName",
    "previousDisplayName",
    "email",
    "slug",
    "noindex",
    "groups",
    "lwWikiImport",
    "lastUsedTimezone",
    "commentSorting",
    "sortDraftsBy",
    "noKibitz",
    "showHideKarmaOption",
    "showPostAuthorCard",
    "hideIntercom",
    "markDownPostEditor",
    "noSingleLineComments",
    "noCollapseCommentsPosts",
    "noCollapseCommentsFrontpage",
    "hideCommunitySection",
    "expandedFrontpageSections",
    "showCommunityInRecentDiscussion",
    "hidePostsRecommendations",
    "petrovOptOut",
    "hideNavigationSidebar",
    "currentFrontpageFilter",
    "frontpageFilterSettings",
    "hideFrontpageFilterSettingsDesktop",
    "allPostsTimeframe",
    "allPostsFilter",
    "allPostsSorting",
    "allPostsShowLowKarma",
    "allPostsIncludeEvents",
    "allPostsHideCommunity",
    "allPostsOpenSettings",
    "draftsListSorting",
    "draftsListShowArchived",
    "draftsListShowShared",
    "karma",
    "goodHeartTokens",
    "moderationStyle",
    "moderatorAssistance",
    "collapseModerationGuidelines",
    "bannedUserIds",
    "bannedPersonalUserIds",
    "legacyId",
    "deleted",
    "voteBanned",
    "nullifyVotes",
    "deleteContent",
    "auto_subscribe_to_my_posts",
    "auto_subscribe_to_my_comments",
    "autoSubscribeAsOrganizer",
    "subscribedToDigest",
    "unsubscribeFromAll",
    "frontpagePostCount",
    "sequenceCount",
    "sequenceDraftCount",
    "mongoLocation",
    "googleLocation",
    "location",
    "mapLocation",
    "mapLocationSet",
    "mapMarkerText",
    "htmlMapMarkerText",
    "nearbyEventsNotifications",
    "nearbyEventsNotificationsLocation",
    "nearbyEventsNotificationsMongoLocation",
    "nearbyEventsNotificationsRadius",
    "nearbyPeopleNotificationThreshold",
    "hideFrontpageMap",
    "hideTaggingProgressBar",
    "hideFrontpageBookAd",
    "hideFrontpageBook2019Ad",
    "hideFrontpageBook2020Ad",
    "snoozedUntilContentCount",
    "reviewedByUserId",
    "afKarma",
    "voteCount",
    "smallUpvoteCount",
    "smallDownvoteCount",
    "bigUpvoteCount",
    "bigDownvoteCount",
    "voteReceivedCount",
    "smallUpvoteReceivedCount",
    "smallDownvoteReceivedCount",
    "bigUpvoteReceivedCount",
    "bigDownvoteReceivedCount",
    "usersContactedBeforeReview",
    "fullName",
    "shortformFeedId",
    "viewUnreviewedComments",
    "partiallyReadSequences",
    "beta",
    "reviewVotesQuadratic",
    "reviewVotesQuadratic2019",
    "reviewVotesQuadratic2020",
    "defaultToCKEditor",
    "signUpReCaptchaRating",
    "oldSlugs",
    "noExpandUnreadCommentsReview",
    "postCount",
    "maxPostCount",
    "commentCount",
    "maxCommentCount",
    "tagRevisionCount",
    "abTestKey",
    "abTestOverrides",
    "reenableDraftJs",
    "walledGardenInvite",
    "hideWalledGardenUI",
    "walledGardenPortalOnboarded",
    "taggingDashboardCollapsed",
    "usernameUnset",
    "paymentEmail",
    "paymentInfo",
    "profileImageId",
    "jobTitle",
    "organization",
    "careerStage",
    "website",
    "fmCrosspostUserId",
    "linkedinProfileURL",
    "facebookProfileURL",
    "twitterProfileURL",
    "githubProfileURL",
    "profileTagIds",
    "organizerOfGroupIds",
    "programParticipation",
    "postingDisabled",
    "allCommentingDisabled",
    "commentingOnOtherUsersDisabled",
    "conversationsDisabled",
    "acknowledgedNewUserGuidelines",
    "subforumPreferredLayout",
    "allowDatadogSessionReplay",
    "afPostCount",
    "afCommentCount",
    "afSequenceCount",
    "afSequenceDraftCount",
    "reviewForAlignmentForumUserId",
    "afApplicationText",
    "afSubmittedApplication",
    "schemaVersion",
    "legacyData",
    "moderationGuidelines_latest",
    "howOthersCanHelpMe_latest",
    "howICanHelpOthers_latest",
    "biography_latest",
    "recommendationSettings",
)
USER_DATETIMES = (
    "whenConfirmationEmailSent",
    "lastNotificationsCheck",
    "banned",
    "karmaChangeLastOpened",
    "karmaChangeBatchStart",
    "reviewedAt",
    "petrovPressedButtonDate",
    "petrovLaunchCodeDate",
    "hideJobAdUntil",
    "inactiveSurveyEmailSentAt",
    "createdAt",
)
USER_DOCUMENTS = (
    "moderationGuidelines",
    "howOthersCanHelpMe",
    "howICanHelpOthers",
    "biography",
)

# POST_FIELDS = ("_id", "url", "postCategory", "title", "slug", "viewCount", "clickCount", "deletedDraft", "status", "isFuture", "sticky", "stickyPriority", "userIP", "userAgent", "referrer", "author", "userId", "question", "authorIsUnreviewed", "readTimeMinutesOverride", "submitToFrontpage", "hiddenRelatedQuestion", "originalPostRelationSourceId", "shortform", "canonicalSource", "nominationCount2018", "nominationCount2019", "reviewCount2018", "reviewCount2019", "reviewCount", "reviewVoteCount", "positiveReviewVoteCount", "manifoldReviewMarketId", "annualReviewMarketCommentId", "reviewVoteScoreAF", "reviewVotesAF", "reviewVoteScoreHighKarma", "reviewVotesHighKarma", "reviewVoteScoreAllKarma", "reviewVotesAllKarma", "finalReviewVoteScoreHighKarma", "finalReviewVotesHighKarma", "finalReviewVoteScoreAllKarma", "finalReviewVotesAllKarma", "finalReviewVoteScoreAF", "finalReviewVotesAF", "tagRelevance", "noIndex", "rsvps", "activateRSVPs", "nextDayReminderSent", "onlyVisibleToLoggedIn", "onlyVisibleToEstablishedAccounts", "hideFromRecentDiscussions", "votingSystem", "podcastEpisodeId", "forceAllowType3Audio", "legacy", "legacyId", "legacySpam", "feedId", "feedLink", "suggestForCuratedUserIds", "collectionTitle", "coauthorStatuses", "hasCoauthorPermission", "socialPreviewImageId", "socialPreviewImageAutoUrl", "socialPreview", "fmCrosspost", "canonicalSequenceId", "canonicalCollectionSlug", "canonicalBookId", "canonicalNextPostSlug", "canonicalPrevPostSlug", "unlisted", "disableRecommendation", "defaultRecommendation", "hideFromPopularComments", "draft", "wasEverUndrafted", "meta", "hideFrontpageComments", "maxBaseScore", "bannedUserIds", "commentsLocked", "organizerIds", "groupId", "eventType", "isEvent", "reviewedByUserId", "reviewForCuratedUserId", "eventRegistrationLink", "joinEventLink", "onlineEvent", "globalEvent", "mongoLocation", "googleLocation", "location", "contactInfo", "facebookLink", "meetupLink", "website", "eventImageId", "types", "metaSticky", "sharingSettings", "shareWithUsers", "linkSharingKey", "linkSharingKeyUsedBy", "commentSortOrder", "hideAuthor", "sideCommentVisibility", "moderationStyle", "ignoreRateLimits", "hideCommentKarma", "commentCount", "topLevelCommentCount", "criticismTipsDismissed", "debate", "collabEditorDialogue", "rejected", "rejectedReason", "rejectedByUserId", "subforumTagId", "af", "afCommentCount", "afSticky", "suggestForAlignmentUserIds", "reviewForAlignmentUserId", "agentFoundationsId", "schemaVersion", "legacyData", "contents_latest", "pingbacks", "moderationGuidelines_latest", "customHighlight_latest", "voteCount", "baseScore", "extendedScore", "score", "afBaseScore", "afExtendedScore", "afVoteCount")
# POST_DATETIMES = ("postedAt", "modifiedAt", "lastCommentedAt", "lastCommentPromotedAt", "curatedDate", "metaDate", "frontpageDate", "scoreExceeded2Date", "scoreExceeded30Date", "scoreExceeded45Date", "scoreExceeded75Date", "scoreExceeded125Date", "scoreExceeded200Date", "commentsLockedToAccountsCreatedAfter", "startTime", "localStartTime", "endTime", "localEndTime", "mostRecentPublishedDialogueResponseDate", "afDate", "afLastCommentedAt", "createdAt")
POST_FIELDS = (
    "_id",
    "url",
    "postCategory",
    "title",
    "slug",
    "deletedDraft",
    "status",
    "isFuture",
    "sticky",
    "stickyPriority",
    "author",
    "userId",
    "question",
    "authorIsUnreviewed",
    "readTimeMinutesOverride",
    "submitToFrontpage",
    "hiddenRelatedQuestion",
    "originalPostRelationSourceId",
    "shortform",
    "canonicalSource",
    "nominationCount2018",
    "nominationCount2019",
    "reviewCount2018",
    "reviewCount2019",
    "reviewCount",
    "reviewVoteCount",
    "positiveReviewVoteCount",
    "manifoldReviewMarketId",
    "annualReviewMarketCommentId",
    "reviewVoteScoreAF",
    "reviewVotesAF",
    "reviewVoteScoreHighKarma",
    "reviewVotesHighKarma",
    "reviewVoteScoreAllKarma",
    "reviewVotesAllKarma",
    "finalReviewVoteScoreHighKarma",
    "finalReviewVotesHighKarma",
    "finalReviewVoteScoreAllKarma",
    "finalReviewVotesAllKarma",
    "finalReviewVoteScoreAF",
    "finalReviewVotesAF",
    "tagRelevance",
    "noIndex",
    "rsvps",
    "activateRSVPs",
    "nextDayReminderSent",
    "onlyVisibleToLoggedIn",
    "onlyVisibleToEstablishedAccounts",
    "hideFromRecentDiscussions",
    "votingSystem",
    "podcastEpisodeId",
    "forceAllowType3Audio",
    "legacy",
    "legacyId",
    "legacySpam",
    "feedId",
    "feedLink",
    "suggestForCuratedUserIds",
    "collectionTitle",
    "coauthorStatuses",
    "hasCoauthorPermission",
    "socialPreviewImageId",
    "socialPreviewImageAutoUrl",
    "socialPreview",
    "fmCrosspost",
    "canonicalSequenceId",
    "canonicalCollectionSlug",
    "canonicalBookId",
    "canonicalNextPostSlug",
    "canonicalPrevPostSlug",
    "unlisted",
    "disableRecommendation",
    "defaultRecommendation",
    "draft",
    "wasEverUndrafted",
    "meta",
    "hideFrontpageComments",
    "maxBaseScore",
    "bannedUserIds",
    "commentsLocked",
    "organizerIds",
    "groupId",
    "eventType",
    "isEvent",
    "reviewedByUserId",
    "reviewForCuratedUserId",
    "eventRegistrationLink",
    "joinEventLink",
    "onlineEvent",
    "globalEvent",
    "mongoLocation",
    "googleLocation",
    "location",
    "contactInfo",
    "facebookLink",
    "meetupLink",
    "website",
    "eventImageId",
    "types",
    "metaSticky",
    "sharingSettings",
    "shareWithUsers",
    "linkSharingKey",
    "linkSharingKeyUsedBy",
    "commentSortOrder",
    "hideAuthor",
    "sideCommentVisibility",
    "moderationStyle",
    "ignoreRateLimits",
    "hideCommentKarma",
    "commentCount",
    "topLevelCommentCount",
    "criticismTipsDismissed",
    "debate",
    "collabEditorDialogue",
    "rejected",
    "rejectedReason",
    "rejectedByUserId",
    "subforumTagId",
    "af",
    "afCommentCount",
    "afSticky",
    "suggestForAlignmentUserIds",
    "reviewForAlignmentUserId",
    "agentFoundationsId",
    "schemaVersion",
    "legacyData",
    "contents_latest",
    "pingbacks",
    "moderationGuidelines_latest",
    "customHighlight_latest",
    "voteCount",
    "baseScore",
    "extendedScore",
    "score",
    "afBaseScore",
    "afExtendedScore",
    "afVoteCount",
)
POST_DATETIMES = (
    "postedAt",
    "modifiedAt",
    "lastCommentedAt",
    "lastCommentPromotedAt",
    "curatedDate",
    "metaDate",
    "frontpageDate",
    "scoreExceeded2Date",
    "scoreExceeded30Date",
    "scoreExceeded45Date",
    "scoreExceeded75Date",
    "scoreExceeded125Date",
    "scoreExceeded200Date",
    "commentsLockedToAccountsCreatedAfter",
    "startTime",
    "localStartTime",
    "endTime",
    "localEndTime",
    "mostRecentPublishedDialogueResponseDate",
    "afDate",
    "afLastCommentedAt",
)
POST_DOCUMENTS = ("contents", "customHighlight", "moderationGuidelines")

TAG_FIELDS = (
    "_id",
    "name",
    "shortName",
    "subtitle",
    "slug",
    "oldSlugs",
    "core",
    "isPostType",
    "suggestedAsFilter",
    "defaultOrder",
    "descriptionTruncationCount",
    "postCount",
    "userId",
    "adminOnly",
    "canEditUserIds",
    "charsAdded",
    "charsRemoved",
    "deleted",
    "needsReview",
    "reviewedByUserId",
    "wikiGrade",
    "wikiOnly",
    "bannerImageId",
    "squareImageId",
    "tagFlagsIds",
    "lesswrongWikiImportRevision",
    "lesswrongWikiImportSlug",
    "lesswrongWikiImportCompleted",
    "htmlWithContributorAnnotations",
    "contributionStats",
    "introSequenceId",
    "postsDefaultSortOrder",
    "canVoteOnRels",
    "isSubforum",
    "subforumModeratorIds",
    "subforumIntroPostId",
    "parentTagId",
    "subTagIds",
    "autoTagModel",
    "autoTagPrompt",
    "noindex",
    "schemaVersion",
    "legacyData",
    "description_latest",
    "subforumWelcomeText_latest",
    "moderationGuidelines_latest",
)
TAG_DATETIMES = ("lastCommentedAt", "lastSubforumCommentAt", "createdAt")
TAG_DOCUMENTS = ("description", "subforumWelcomeText", "moderationGuidelines")

TAG_REL_FIELDS = (
    "_id",
    "tagId",
    "postId",
    "deleted",
    "userId",
    "backfilled",
    "schemaVersion",
    "legacyData",
    "voteCount",
    "baseScore",
    "extendedScore",
    "score",
    "afBaseScore",
    "afExtendedScore",
    "afVoteCount",
)
TAG_REL_DATETIMES = ("createdAt",)
TAG_REL_DOCUMENTS = ()

COMMENT_FIELDS = (
    "_id",
    "parentCommentId",
    "topLevelCommentId",
    "author",
    "postId",
    "tagId",
    "tagCommentType",
    "subforumStickyPriority",
    "userId",
    "userIP",
    "userAgent",
    "referrer",
    "authorIsUnreviewed",
    "answer",
    "parentAnswerId",
    "directChildrenCount",
    "descendentCount",
    "shortform",
    "shortformFrontpage",
    "nominatedForReview",
    "reviewingForReview",
    "postVersion",
    "promoted",
    "promotedByUserId",
    "hideKarma",
    "legacy",
    "legacyId",
    "legacyPoll",
    "legacyParentId",
    "retracted",
    "deleted",
    "deletedPublic",
    "deletedReason",
    "deletedByUserId",
    "spam",
    "needsReview",
    "reviewedByUserId",
    "hideAuthor",
    "moderatorHat",
    "hideModeratorHat",
    "isPinnedOnProfile",
    "title",
    "relevantTagIds",
    "debateResponse",
    "rejected",
    "modGPTAnalysis",
    "modGPTRecommendation",
    "rejectedReason",
    "rejectedByUserId",
    "af",
    "suggestForAlignmentUserIds",
    "reviewForAlignmentUserId",
    "moveToAlignmentUserId",
    "agentFoundationsId",
    "originalDialogueId",
    "schemaVersion",
    "legacyData",
    "contents_latest",
    "pingbacks",
    "voteCount",
    "baseScore",
    "extendedScore",
    "score",
    "afBaseScore",
    "afExtendedScore",
    "afVoteCount",
)
COMMENT_DATETIMES = (
    "postedAt",
    "lastSubthreadActivity",
    "promotedAt",
    "deletedDate",
    "repliesBlockedUntil",
    "afDate",
    "createdAt",
)
COMMENT_DOCUMENTS = ("contents",)


EPOCH = datetime(1970, 1, 1, 0, 0, 0, tzinfo=timezone.utc)


def random_alphanumeric(length):
    alphanumeric = string.ascii_letters + string.digits
    return "".join(random.choice(alphanumeric) for _ in range(length))


def json_encode_dicts(x, ignore=(str, bytes)):
    if isinstance(x, dict):
        return json.dumps(x)
    elif isinstance(x, Iterable) and not any(isinstance(x, ty) for ty in ignore):
        return type(x)(json_encode_dicts(item) for item in x)
    else:
        return x


def to_api_datetime(dt):
    try:
        return dt.isoformat(timespec="milliseconds").replace("+00:00", "Z")
    except:
        return dt


def from_api_datetime(dt):
    try:
        return datetime.fromisoformat(re.sub(r"Z$", "+00:00", dt))
    except:
        return dt


def add_results_db(db, results, table, fields, documents, datetimes):
    return db.copy_records_to_table(
        table,
        columns=chain(fields, documents, datetimes),
        records=(
            (
                *(json_encode_dicts(result[field]) for field in fields),
                *(json_encode_dicts(result[doc]) for doc in documents),
                *(from_api_datetime(result[dt]) for dt in datetimes),
            )
            for result in results
        ),
    )


async def add_results(pool, results, table, fields, documents, datetimes):
    async with pool.acquire() as db:
        return await add_results_db(db, results, table, fields, documents, datetimes)


async def add_new_results_db(db, results, table, fields, documents, datetimes):
    temp_table = f"_{table}_{random_alphanumeric(61)}"[:63]
    await db.execute(
        f'CREATE TEMPORARY TABLE "{temp_table}" (LIKE "{table}" INCLUDING ALL)'
    )
    await add_results_db(db, results, temp_table, fields, documents, datetimes)
    insert_status = await db.execute(
        f"""
        INSERT INTO "{table}" SELECT * FROM "{temp_table}"
        WHERE NOT EXISTS(SELECT 1 FROM "{table}" WHERE "{table}"._id = "{temp_table}"._id)
    """
    )
    await db.execute(f'DROP TABLE "{temp_table}"')
    return int(insert_status.split()[2])


async def add_new_results(pool, results, table, fields, documents, datetimes):
    async with pool.acquire() as db:
        return await add_new_results_db(
            db, results, table, fields, documents, datetimes
        )


async def add_single(pool, api, table, id, fields, documents, datetimes, tweak=None):
    if not table.endswith("s"):
        raise ValueError("table must end with 's'")
    query_fn = table[0].lower() + table[1:-1]
    query = gql(
        f"""
        query get{table}($id: String) {{
            {query_fn}(input: {{ selector: {{ _id: $id }}, enableCache: false }}) {{                
                result {{
                    {" ".join(fields)}
                    {" ".join(datetimes)}
                    {" ".join(f"{doc} {{ {', '.join(DOCUMENT_FIELDS)} }}" for doc in documents)}
                }}
            }}
        }}
    """
    )

    result = await api.execute(query, variable_values={"id": id})
    result = result[query_fn]["result"]

    if tweak:
        await tweak(result)

    async with pool.acquire() as db:
        await add_new_results_db(db, (result,), table, fields, documents, datetimes)
        count = await db.fetchval(f'SELECT COUNT(1) FROM "{table}"')

    print(f"{count} {table} imported")


async def try_add_single(
    pool, api, table, id, fields, documents, datetimes, tweak=None
):
    try:
        await add_single(pool, api, table, id, fields, documents, datetimes, tweak)
    except TransportQueryError as e:
        print(f"error adding {table[:-1]} {id}: {e}", file=sys.stderr)


async def add_descending(
    pool,
    api,
    table,
    terms,
    fields,
    documents,
    datetimes,
    tweak=None,
    max_offset=MAX_OFFSET,
    max_results=MAX_RESULTS,
    sort_field="createdAt",
):
    query_fn = table[0].lower() + table[1:]
    query = gql(
        f"""
        query get{table}($terms: JSON) {{
            {query_fn}(input: {{ terms: $terms, enableCache: false, enableTotal: false }}) {{
                results {{
                    {" ".join(fields)}
                    {" ".join(datetimes)}
                    {" ".join(f"{doc} {{ {', '.join(DOCUMENT_FIELDS)} }}" for doc in documents)}
                }}
            }}
        }}
    """
    )

    async with pool.acquire() as db:
        newest = (
            await db.fetchval(f'SELECT MAX("{sort_field}") FROM "{table}"') or EPOCH
        )
        count = old_count = await db.fetchval(f'SELECT COUNT(1) FROM "{table}"')

    offset = 0
    skipped = False
    while True:
        limit = 1 if not offset and not count else max_results
        results = await api.execute(
            query,
            variable_values={
                "terms": {
                    **terms,
                    "offset": min(offset, max_offset),
                    **({"limit": limit} if limit < float("inf") else {}),
                },
            },
        )
        results = results[query_fn]["results"]
        original_results_len = len(results)
        results = results[max(offset - max_offset, 0) :]

        if not results:
            break

        if not skipped:
            async with pool.acquire() as db:
                with suppress(StopAsyncIteration):
                    first_seen_index = await anext(
                        i
                        for i, result in enumerate(results)
                        if (
                            from_api_datetime(result[sort_field]) <= newest
                            and await db.fetchval(
                                f'SELECT EXISTS(SELECT 1 FROM "{table}" WHERE _id = $1)',
                                result["_id"],
                            )
                        )
                    )
                    del results[first_seen_index : first_seen_index + old_count]
                    offset += old_count
                    skipped = True

        if results:
            if tweak:
                await tweak(results)

            async with pool.acquire() as db:
                added = await add_new_results(
                    pool, results, table, fields, documents, datetimes
                )

                try:
                    oldest = next(r[sort_field] for r in results[::-1] if r[sort_field])
                except StopIteration:
                    oldest = None

                if added:
                    count = await db.fetchval(f'SELECT COUNT(1) FROM "{table}"')
                    print(f"{count} {table} imported (back to {oldest})")
                else:
                    print(
                        f"fetched {original_results_len} already-imported {table} @ offset {offset}",
                        file=sys.stderr,
                    )

        # TODO: per-query max offsets exist, we should take them into account
        if offset >= max_offset:
            print(
                f"get{table}({terms}) reached maximum offset; we may have missed some",
                file=sys.stderr,
            )
            break
        elif original_results_len < max_results:
            break

        offset += original_results_len


async def add_ascending_date(
    pool,
    api,
    table,
    terms,
    fields,
    documents,
    datetimes,
    tweak=None,
    max_results=float("inf"),
    sort_field="createdAt",
):
    query_fn = table[0].lower() + table[1:]
    query = gql(
        f"""
        query get{table}($terms: JSON) {{
            {query_fn}(input: {{ terms: $terms, enableCache: false, enableTotal: false }}) {{
                results {{
                    {" ".join(fields)}
                    {" ".join(datetimes)}
                    {" ".join(f"{doc} {{ {', '.join(DOCUMENT_FIELDS)} }}" for doc in documents)}
                }}
            }}
        }}
    """
    )

    async with pool.acquire() as db:
        newest = (
            await db.fetchval(f'SELECT MAX("{sort_field}") FROM "{table}"') or EPOCH
        )

    while True:
        async with pool.acquire() as db:
            offset = await db.fetchval(
                f'SELECT COUNT(1) FROM "{table}" WHERE "{sort_field}" = $1', newest
            )

        results = await api.execute(
            query,
            variable_values={
                "terms": {
                    **terms,
                    "after": to_api_datetime(newest),
                    "offset": offset,
                    **({"limit": max_results} if max_results < float("inf") else {}),
                },
            },
        )
        results = results[query_fn]["results"]

        if not results:
            break

        if tweak:
            await tweak(results)

        async with pool.acquire() as db:
            added = await add_new_results_db(
                db, results, table, fields, documents, datetimes
            )

            try:
                newest = next(r[sort_field] for r in results[::-1] if r[sort_field])
            except StopIteration:
                newest = None

            if added:
                count = await db.fetchval(f'SELECT COUNT(1) FROM "{table}"')
                print(f"{count} {table} imported (up to {newest})")
            else:
                print(
                    f"fetched {len(results)} already-imported {table} @ offset {offset}",
                    file=sys.stderr,
                )

        if len(results) < max_results < float("inf"):
            break


async def add_ascending(
    pool,
    api,
    table,
    terms,
    fields,
    documents,
    datetimes,
    tweak=None,
    max_offset=MAX_OFFSET,
    max_results=MAX_RESULTS,
):
    query_fn = table[0].lower() + table[1:]
    query = gql(
        f"""
        query get{table}($terms: JSON) {{
            {query_fn}(input: {{ terms: $terms, enableCache: false, enableTotal: false }}) {{
                results {{
                    {" ".join(fields)}
                    {" ".join(datetimes)}
                    {" ".join(f"{doc} {{ {', '.join(DOCUMENT_FIELDS)} }}" for doc in documents)}
                }}
            }}
        }}
    """
    )

    # TODO: fetch Answers as well

    offset = 0
    while True:
        results = await api.execute(
            query,
            variable_values={
                "terms": {
                    **terms,
                    "offset": min(offset, max_offset),
                }
            },
        )
        results = results[query_fn]["results"]
        original_results_len = len(results)
        results = results[max(offset - max_offset, 0) :]

        if not results:
            break

        if tweak:
            await tweak(results)

        async with pool.acquire() as db:
            added = await add_new_results_db(
                db, results, table, fields, documents, datetimes
            )
            if added:
                count = await db.fetchval(f'SELECT COUNT(1) FROM "{table}"')
                print(f"{count} {table} imported")
            else:
                print(
                    f"fetched {len(results)} already-imported {table} @ offset {offset}",
                    file=sys.stderr,
                )

        if offset >= max_offset:
            print(
                f"get{table}({terms}) reached maximum offset; we may have missed some",
                file=sys.stderr,
            )
            break
        elif original_results_len < max_results < float("inf"):
            break

        offset += len(results)


async def add_tag_rels_for_tag(pool, api, tag_id):
    terms = {
        "view": "postsWithTag",
        "tagId": tag_id,
    }

    await add_ascending(
        pool,
        api,
        "TagRels",
        terms,
        TAG_REL_FIELDS,
        TAG_REL_DOCUMENTS,
        TAG_REL_DATETIMES,
    )


async def add_tag_rels_for_post(pool, api, post_id):
    terms = {
        "view": "tagsOnPost",
        "postId": post_id,
    }

    await add_ascending(
        pool,
        api,
        "TagRels",
        terms,
        TAG_REL_FIELDS,
        TAG_REL_DOCUMENTS,
        TAG_REL_DATETIMES,
    )


async def add_tags_and_tag_rels(pool, api):
    terms = {
        "view": "newTags",
    }

    async def tweak(tags):
        for tag in tags:
            # TODO: why is this necessary?
            tag["descriptionTruncationCount"] = (
                tag.get("descriptionTruncationCount") or 0
            )
            tag["needsReview"] = (
                tag.get("needsReview") or True
            )  # is True the right default

        await asyncio.gather(
            *(add_tag_rels_for_tag(pool, api, tag["_id"]) for tag in tags)
        )

    await add_descending(
        pool,
        api,
        "Tags",
        terms,
        TAG_FIELDS,
        TAG_DOCUMENTS,
        TAG_DATETIMES,
        tweak=tweak,
        max_offset=float("inf"),
    )

    print("finished bulk importing Tags and TagRels")


async def add_users(pool, api):
    terms = {
        "view": "allUsers",
        "sort": {"createdAt": 1},
    }

    async def tweak(users):
        for user in users:
            # TODO: a more elegant way to express this (or actually generate clientId's)
            user["abTestKey"] = "test-user-ab-test-key"
            # TODO: why is this necessary? afKarma sometimes returns 0 and sometimes null (disallowed)
            user["afKarma"] = user["afKarma"] or 0

    await add_descending(
        pool,
        api,
        "Users",
        terms,
        USER_FIELDS,
        USER_DOCUMENTS,
        USER_DATETIMES,
        tweak=tweak,
        max_results=1000,
    )

    print("finished bulk importing Users")


async def add_comments_for_post(pool, api, post_id):
    terms = {
        "view": "postCommentsDeleted",
        "postId": post_id,
    }

    async def tweak(comments):
        for comment in comments:
            # TODO: probably not right
            comment["createdAt"] = comment["postedAt"]

    await add_ascending(
        pool,
        api,
        "Comments",
        terms,
        COMMENT_FIELDS,
        COMMENT_DOCUMENTS,
        COMMENT_DATETIMES,
        tweak=tweak,
    )


async def add_posts_and_comments(pool, api):
    terms = {
        "karmaThreshold": -2147483648,
        "excludeEvents": False,
        "hideCommunity": False,
        "filter": "all",
        "sortedBy": "old",
    }

    async def tweak(posts):
        await asyncio.gather(
            *(add_comments_for_post(pool, api, post["_id"]) for post in posts)
        )

    await add_ascending_date(
        pool,
        api,
        "Posts",
        terms,
        POST_FIELDS,
        POST_DOCUMENTS,
        POST_DATETIMES,
        sort_field="postedAt",
        max_results=1000,
    )

    print("finished importing Posts")


async def add_missing_users(pool, api):
    async with pool.acquire() as db:
        # use references.sql missing_users()
        missing_users = await db.fetch("""SELECT * FROM missing_users()""")

    # TODO: consolidate with other user tweak
    async def tweak(user):
        # TODO: a more elegant way to express this (or actually generate clientId's)
        user["abTestKey"] = "test-user-ab-test-key"
        # TODO: why is this necessary? afKarma sometimes returns 0 and sometimes null (disallowed)
        user["afKarma"] = user["afKarma"] or 0

    await asyncio.gather(
        *(
            try_add_single(
                pool,
                api,
                "Users",
                user["userId"],
                USER_FIELDS,
                USER_DOCUMENTS,
                USER_DATETIMES,
                tweak=tweak,
            )
            for user in missing_users
        )
    )


async def add_missing_tags_and_tag_rels(pool, api):
    async with pool.acquire() as db:
        missing_tags = await db.fetch(
            """
            SELECT DISTINCT "tagId" FROM "Posts" post
            CROSS JOIN jsonb_object_keys(post."tagRelevance") AS "tagId"
            WHERE "tagId" NOT IN (SELECT _id FROM "Tags")
        """
        )

    async def tweak(tag):
        # TODO: why is this necessary?
        tag["descriptionTruncationCount"] = tag.get("descriptionTruncationCount") or 0
        tag["needsReview"] = tag.get("needsReview") or True  # is True the right default

    await asyncio.gather(
        *(
            try_add_single(
                pool,
                api,
                "Tags",
                tag["tagId"],
                TAG_FIELDS,
                TAG_DOCUMENTS,
                TAG_DATETIMES,
                tweak=tweak,
            )
            for tag in missing_tags
        )
    )

    # now, in case there are any tagRel stragglers (e.g. if >5k tagRels for tag)
    # find all (post, tagId) pairs such that the tagId is referenced by (i.e. is
    # one of the keys of the `jsonb` object for) the post's `tagRelevance` field
    # then, for each such pair, as we don't actually know the id of the tagRel &
    # the only multi-tagRel views are "postsWithTag" and "tagsOnPost", look up &
    # attempt to re-add all the tagRels on that post, including the missing ones
    async with pool.acquire() as db:
        posts_with_missing_tag_rels = await db.fetch(
            """
            SELECT _id FROM "Posts" post
            WHERE EXISTS(
                SELECT 1 FROM jsonb_object_keys(post."tagRelevance") AS tagId
                WHERE NOT EXISTS(
                    SELECT 1 FROM "TagRels" tagRel
                    WHERE tagRel."postId" = post._id AND tagRel."tagId" = tagId
                )
            )
        """
        )

    await asyncio.gather(
        *(
            add_tag_rels_for_post(pool, api, post["_id"])
            for post in posts_with_missing_tag_rels
        )
    )


async def add_missing_comments(pool, api):
    async with pool.acquire() as db:
        posts_with_missing_comments = await db.fetch(
            """
            SELECT _id FROM "Posts" post
            WHERE post."commentCount" > (
                SELECT COUNT(1) FROM "Comments" c
                WHERE c."postId" = post._id
            )
        """
        )

    await asyncio.gather(
        *(
            add_comments_for_post(pool, api, post["_id"])
            for post in posts_with_missing_comments
        )
    )

    # TODO: now that we got here, impl something like add_missing_users


async def main():
    try:
        with open("cookies.json") as f:
            cookies = json.load(f)
    except FileNotFoundError:
        cookies = None

    pool = await asyncpg.create_pool(database=os.environ.get("PGDATABASE", "lesswrong"))

    transport = AIOHTTPTransport(
        url="https://www.lesswrong.com/graphql",
        headers={
            "User-Agent": "ratpile 0.1",
            "From": "milkey-mouse",
        },
        cookies=cookies,
    )
    client = Client(transport=transport, execute_timeout=None)
    # api = await client.connect_async(reconnecting=True)
    api = await client.connect_async()

    print("bulk downloading via getMulti endpoints")
    # await asyncio.gather(
    #    add_users(pool, api),
    #    add_posts_and_comments(pool, api),
    #    add_tags_and_tag_rels(pool, api),
    # )

    print("fixing up missing records via getSingle endpoints")
    await asyncio.gather(
        add_missing_users(pool, api),
        add_missing_tags_and_tag_rels(pool, api),
        add_missing_comments(pool, api),
    )

    # TODO: iterate to fixed point don't just add_missing_* once

    # we may have added records referencing new un-fetched users,
    # so search for and download those records again
    await add_missing_users(pool, api)

    # TODO: why can't I add user markkrieg

    await client.close_async()
    await pool.close()


asyncio.run(main())
