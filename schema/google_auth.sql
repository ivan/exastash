-- OAuth 2.0 application secrets used to generate OAuth 2.0 access tokens

CREATE TABLE google_application_secrets (
    domain_id  smallint  PRIMARY KEY REFERENCES google_domains(id),
    secret     jsonb     NOT NULL
);

CREATE TRIGGER google_application_secrets_update
    BEFORE UPDATE ON google_application_secrets
    FOR EACH ROW
    WHEN (OLD.domain_id != NEW.domain_id)
    EXECUTE FUNCTION raise_exception('cannot change domain_id');

CREATE TRIGGER google_application_secrets_forbid_truncate
    BEFORE TRUNCATE ON google_application_secrets
    EXECUTE FUNCTION raise_exception('truncate is forbidden');



-- OAuth 2.0 bearer access tokens used to create, read, and delete Google Drive files

CREATE TABLE google_access_tokens (
    expires_at     timestamptz  NOT NULL,
    -- For now, we don't need more than one access token per account
    owner_id       int          PRIMARY KEY REFERENCES gdrive_owners (id),
    access_token   text         NOT NULL,
    refresh_token  text         NOT NULL
);

CREATE TRIGGER google_access_tokens_update
    BEFORE UPDATE ON google_access_tokens
    FOR EACH ROW
    EXECUTE FUNCTION raise_exception('cannot change row');

CREATE TRIGGER google_access_tokens_forbid_truncate
    BEFORE TRUNCATE ON google_access_tokens
    EXECUTE FUNCTION raise_exception('truncate is forbidden');



CREATE DOMAIN email AS text CHECK(VALUE ~ '@' AND VALUE = lower(VALUE));

-- Service accounts used to create, read, and, delete Google Drive files

CREATE TABLE google_service_accounts (
    owner_id                     int    NOT NULL REFERENCES gdrive_owners(id),
    -- All fields below come directly from the .json file downloaded from Google.
    --
    -- For now, we don't need more than one service account key per service account.
    client_email                 email  PRIMARY KEY,
    client_id                    text   NOT NULL,
    project_id                   text   NOT NULL,
    private_key_id               text   NOT NULL,
    private_key                  text   NOT NULL,
    auth_uri                     text   NOT NULL,
    token_uri                    text   NOT NULL,
    auth_provider_x509_cert_url  text   NOT NULL,
    client_x509_cert_url         text   NOT NULL
);

CREATE TRIGGER google_service_accounts_update
    BEFORE UPDATE ON google_service_accounts
    FOR EACH ROW
    EXECUTE FUNCTION raise_exception('cannot change row');

CREATE TRIGGER google_service_accounts_forbid_truncate
    BEFORE TRUNCATE ON google_service_accounts
    EXECUTE FUNCTION raise_exception('truncate is forbidden');



CREATE TABLE google_service_accounts_stats (
    client_email          email        PRIMARY KEY,
    -- Set back to NULL when not over quota
    last_over_quota_time  timestamptz
);



CREATE VIEW google_service_accounts_view AS
    SELECT
        google_service_accounts.*,
        last_over_quota_time
    FROM stash.google_service_accounts
    LEFT JOIN stash.google_service_accounts_stats ON google_service_accounts_stats.client_email = google_service_accounts.client_email;
