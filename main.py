import boto
import json
import boto.s3.connection
import os
import logging

logging.info("s3 data grabber starting!")

try:
  logging.info("loading config from environment!")
  cfg_json = os.environ['SDS_PLUGIN_CONFIG_JSON']
except KeyError:
  logging.error("Failed to load config.")
  raise

try: 
  logging.info("parsing config.")
  cfg = json.loads(cfg_json)
except StandardError:
  logging.error("failed to parse config as json---try running it through a validator.")
  raise

try:
  logging.info("setting option values from config!")
  access_key = cfg['iamAccessKey']
  secret_key = cfg['iamSecretKey']
  region     = cfg['region']
  key        = cfg['key']
  bucket     = cfg['bucket']
  path       = cfg['outPath']
except KeyError:
  logging.error("Required options not specified.")
  raise

try:
  logging.info("Connecting to S3!")
  conn = boto.connect_to_region(
    region,
    aws_access_key_id = access_key,
    aws_secret_access_key = secret_key,
    #is_secure=False,               # uncomment if you are not using ssl
    calling_format = boto.s3.connection.OrdinaryCallingFormat()
  )
except StandardError:
  logging.error("Attempt to connect to S3 failed!.")
  raise

try:
  logging.info("GET: bucket %s" % bucket)
  bucket = conn.get_bucket(bucket)
except StandardError:
  logging.error("Bucket not found.")
  raise

try:
  logging.info("GET: key %s" % key)
  key = bucket.get_key(key)
except StandardError:
  logging.error("Key not found.")
  raise

try:
  logging.info("downloading data")
  key.get_contents_to_filename(path)
except StandardError:
  logging.error("download failed.")
  raise


