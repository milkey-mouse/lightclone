CREATE OR REPLACE FUNCTION users_referenced_by_comments()
    RETURNS TABLE ("userId" TEXT)
    AS '
        SELECT DISTINCT "userId" FROM (
            SELECT "userId"
            FROM "Comments" WHERE "userId" IS NOT NULL
            UNION
            SELECT "promotedByUserId" AS "userId"
            FROM "Comments" WHERE "userId" IS NOT NULL
            UNION
            SELECT "deletedByUserId" AS "userId"
            FROM "Comments" WHERE "userId" IS NOT NULL
            UNION
            SELECT "reviewedByUserId" AS "userId"
            FROM "Comments" WHERE "userId" IS NOT NULL
            UNION
            SELECT "rejectedByUserId" AS "userId"
            FROM "Comments" WHERE "userId" IS NOT NULL
            UNION
            SELECT unnest("suggestForAlignmentUserIds") AS "userId"
            FROM "Comments" WHERE "userId" IS NOT NULL
            UNION
            SELECT "reviewForAlignmentUserId" AS "userId"
            FROM "Comments" WHERE "userId" IS NOT NULL
            UNION
            SELECT "moveToAlignmentUserId" AS "userId"
            FROM "Comments" WHERE "userId" IS NOT NULL
        ) referencedUsers
        WHERE "userId" IS NOT NULL;
    ' LANGUAGE SQL;

CREATE OR REPLACE FUNCTION users_referenced_by_tag_rels()
    RETURNS TABLE ("userId" TEXT)
    AS '
        SELECT DISTINCT "userId"
        FROM "TagRels"
        WHERE "userId" IS NOT NULL;
    ' LANGUAGE SQL;

CREATE OR REPLACE FUNCTION users_referenced_by_tags()
    RETURNS TABLE ("userId" TEXT)
    AS '
        SELECT DISTINCT "userId" FROM (
            SELECT "userId"
            FROM "Tags" WHERE "userId" IS NOT NULL
            UNION
            SELECT unnest("canEditUserIds") AS "userId"
            FROM "Tags" WHERE "userId" IS NOT NULL
            UNION
            SELECT "reviewedByUserId" AS "userId"
            FROM "Tags" WHERE "userId" IS NOT NULL
            UNION
            SELECT unnest("subforumModeratorIds") AS "userId"
            FROM "Tags" WHERE "userId" IS NOT NULL
        ) referencedUsers
        WHERE "userId" IS NOT NULL;
    ' LANGUAGE SQL;

CREATE OR REPLACE FUNCTION users_referenced_by_posts()
    RETURNS TABLE ("userId" TEXT)
    AS '
        SELECT DISTINCT "userId" FROM (
            SELECT "userId"
            FROM "Posts" WHERE "userId" IS NOT NULL
            UNION
            SELECT unnest("rsvps") ->> ''userId'' AS "userId"
            FROM "Posts" WHERE "userId" IS NOT NULL
            UNION
            SELECT unnest("suggestForCuratedUserIds") AS "userId"
            FROM "Posts" WHERE "userId" IS NOT NULL
            UNION
            SELECT unnest("coauthorStatuses") ->> ''userId'' AS "userId"
            FROM "Posts" WHERE "userId" IS NOT NULL
            UNION
            SELECT unnest("bannedUserIds") AS "userId"
            FROM "Posts" WHERE "userId" IS NOT NULL
            UNION
            SELECT unnest("organizerIds") AS "userId"
            FROM "Posts" WHERE "userId" IS NOT NULL
            UNION
            SELECT "reviewedByUserId" AS "userId"
            FROM "Posts" WHERE "userId" IS NOT NULL
            UNION
            SELECT "reviewForCuratedUserId" AS "userId"
            FROM "Posts" WHERE "userId" IS NOT NULL
            UNION
            SELECT unnest("shareWithUsers") AS "userId"
            FROM "Posts" WHERE "userId" IS NOT NULL
            UNION
            SELECT unnest("linkSharingKeyUsedBy") AS "userId"
            FROM "Posts" WHERE "userId" IS NOT NULL
            UNION
            SELECT "rejectedByUserId" AS "userId"
            FROM "Posts" WHERE "userId" IS NOT NULL
            UNION
            SELECT unnest("suggestForAlignmentUserIds") AS "userId"
            FROM "Posts" WHERE "userId" IS NOT NULL
            UNION
            SELECT "reviewForAlignmentUserId" AS "userId"
            FROM "Posts" WHERE "userId" IS NOT NULL
            UNION
            SELECT unnest("bannedUserIds") AS "userId"
            FROM "Posts" WHERE "userId" IS NOT NULL
            UNION
            SELECT unnest("bannedPersonalUserIds") AS "userId"
            FROM "Posts" WHERE "userId" IS NOT NULL
            UNION
            SELECT "reviewedByUserId" AS "userId"
            FROM "Posts" WHERE "userId" IS NOT NULL
            UNION
            SELECT unnest("usersContactedBeforeReview") AS "userId"
            FROM "Posts" WHERE "userId" IS NOT NULL
            UNION
            SELECT "reviewForAlignmentForumUserId" AS "userId"
            FROM "Posts" WHERE "userId" IS NOT NULL
            UNION
            SELECT "moderationGuidelines" ->> ''userId'' AS "userId" FROM "Posts"
            UNION
            SELECT "howOthersCanHelpMe" ->> ''userId'' AS "userId" FROM "Posts"
            UNION
            SELECT "howICanHelpOthers" ->> ''userId'' AS "userId" FROM "Posts"
            UNION
            SELECT "biography" ->> ''userId'' AS "userId" FROM "Posts"
        ) referencedUsers
        WHERE "userId" IS NOT NULL;
    ' LANGUAGE SQL;

CREATE OR REPLACE FUNCTION users_referenced_by_users()
    RETURNS TABLE ("userId" TEXT)
    AS '
        SELECT DISTINCT "userId" FROM (
            SELECT unnest("bannedUserIds") AS "userId"
            FROM "Users" WHERE "userId" IS NOT NULL
            SELECT unnest("bannedPersonalUserIds") AS "userId"
            FROM "Users" WHERE "userId" IS NOT NULL
            SELECT "reviewedByUserId" AS "userId"
            FROM "Users" WHERE "userId" IS NOT NULL
            SELECT unnest("usersContactedBeforeReview") AS "userId"
            FROM "Users" WHERE "userId" IS NOT NULL
            SELECT "reviewForAlignmentForumUserId" AS "userId"
            FROM "Users" WHERE "userId" IS NOT NULL
        ) referencedUsers
        WHERE "userId" IS NOT NULL;
    ' LANGUAGE SQL;

CREATE OR REPLACE FUNCTION missing_users()
    RETURNS TABLE ("userId" TEXT)
    AS '
        SELECT DISTINCT "userId" FROM (
            SELECT "userId" FROM users_referenced_by_comments()
            UNION
            SELECT "userId" FROM users_referenced_by_tag_rels()
            UNION
            SELECT "userId" FROM users_referenced_by_tags()
            UNION
            SELECT "userId" FROM users_referenced_by_posts()
        ) referencedUser
        WHERE "userId" IS NOT NULL AND NOT EXISTS (
            SELECT 1 FROM "Users" u
            WHERE u._id = referencedUser."userId"
        );
    ' LANGUAGE SQL;
