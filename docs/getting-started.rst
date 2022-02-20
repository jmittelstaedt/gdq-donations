Getting started
===============

You need a twitter developer profile and "app" to use with this.
Once created, you must note the Bearer token, which must be listed in the ".env" file in the project directory with the key ``bearer_token``.
Refer to `twarc docs <https://twarc-project.readthedocs.io/en/latest/twarc2_en_us/>`.
You also need youtube OAuth credentials, in the ``youtube_datqa_oauth_credentials.json`` in the format
``
{"installed":
    {"client_id": your_client_id,
     "project_id": your_project_id,
     "auth_uri":"https://accounts.google.com/o/oauth2/auth",
     "token_uri":"https://oauth2.googleapis.com/token",
     "auth_provider_x509_cert_url":"https://www.googleapis.com/oauth2/v1/certs",
     "client_secret": your_client_secret,
     "redirect_uris":[your, uris]}
}
``
which you should be able to directly download once setting up a google developer profile and project.