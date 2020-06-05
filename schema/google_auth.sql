-- OAuth 2.0 application secrets used to generate OAuth 2.0 access tokens

CREATE TABLE gsuite_application_secrets (
    domain_id  smallint  PRIMARY KEY REFERENCES gsuite_domains(id),
    secret     jsonb     NOT NULL
);

CREATE TRIGGER gsuite_application_secrets_update
    BEFORE UPDATE ON gsuite_application_secrets
    FOR EACH ROW
    WHEN (OLD.domain_id != NEW.domain_id)
    EXECUTE FUNCTION raise_exception('cannot change domain_id');

CREATE TRIGGER gsuite_application_secrets_forbid_truncate
    BEFORE TRUNCATE ON gsuite_application_secrets
    EXECUTE FUNCTION raise_exception('truncate is forbidden');



-- OAuth 2.0 bearer access tokens used to create, read, and delete Google Drive files

CREATE TABLE gsuite_access_tokens (
    expires_at     timestamptz  NOT NULL,
    -- For now, we don't need more than one access token per account
    owner_id       int          PRIMARY KEY REFERENCES gdrive_owners (id),
    access_token   text         NOT NULL,
    refresh_token  text         NOT NULL
);

CREATE TRIGGER gsuite_access_tokens_update
    BEFORE UPDATE ON gsuite_access_tokens
    FOR EACH ROW
    WHEN (OLD.owner_id != NEW.owner_id)
    EXECUTE FUNCTION raise_exception('cannot change owner_id');

CREATE TRIGGER gsuite_access_tokens_forbid_truncate
    BEFORE TRUNCATE ON gsuite_access_tokens
    EXECUTE FUNCTION raise_exception('truncate is forbidden');



CREATE DOMAIN email AS text CHECK(VALUE ~ '@' AND VALUE = lower(VALUE));

-- Service accounts used to create, read, and, delete Google Drive files

CREATE TABLE gsuite_service_accounts (
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

CREATE TRIGGER gsuite_service_accounts_update
    BEFORE UPDATE ON gsuite_service_accounts
    FOR EACH ROW
    EXECUTE FUNCTION raise_exception('cannot change row');

CREATE TRIGGER gsuite_service_accounts_forbid_truncate
    BEFORE TRUNCATE ON gsuite_service_accounts
    EXECUTE FUNCTION raise_exception('truncate is forbidden');