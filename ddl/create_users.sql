CREATE TABLE
    Users (
        "Id" BIGINT NOT NULL,
        "Reputation" INT,
        "CreationDate" TIMESTAMP,
        "DisplayName" TEXT,
        "LastAccessDate" TIMESTAMP,
        "WebsiteUrl" TEXT,
        "Location" TEXT,
        "AboutMe" TEXT,
        "Views" INT,
        "UpVotes" INT,
        "DownVotes" INT,
        "ProfileImageUrl" TEXT,
        "EmailHash" TEXT,
        "AccountId" BIGINT
    )