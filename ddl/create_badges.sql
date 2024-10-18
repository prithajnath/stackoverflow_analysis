CREATE TABLE
    Badges (
        "Id" BIGINT NOT NULL,
        "UserId" BIGINT,
        "Name" TEXT,
        "Date" TIMESTAMP,
        "Class" INT,
        "TagBased" BOOLEAN
    )