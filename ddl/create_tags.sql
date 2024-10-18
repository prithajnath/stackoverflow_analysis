CREATE TABLE
    Tags (
        "Id" BIGINT NOT NULL,
        "TagName" TEXT,
        "Count" INT,
        "ExcerptPostId" BIGINT,
        "WikiPostId" BIGINT,
        "IsModeratorOnly" BOOLEAN,
        "IsRequired" BOOLEAN
    )