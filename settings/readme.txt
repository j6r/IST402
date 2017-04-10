To customize the location of files and google API key for your use, create a file named
local_settings.yml in this directory, add the following lines, and replace the placeholder
text with your values. DO NOT CHECK IN THIS FILE.

ingestion_settings:
  staging_db_location: 'PATH'
  data_directory: 'PATH'

google_api:
  api_key: KEY