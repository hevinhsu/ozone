# Defer heavy imports so that simple keywords (like Hello World) work without boto3 installed.

def generate_presigned_put_object_url(
    aws_access_key_id=None,
    aws_secret_access_key=None,
    bucket_name=None,
    object_key=None,
    region_name='us-east-1',
    expiration=3600,
    content_type=None,
    endpoint_url=None,
):
  """
  Generate a presigned URL for PUT Object. This function creates the S3 client internally.
  """
  try:
    # Import here to avoid requiring boto3 for simple keywords/tests
    import boto3

    client_args = {
      'service_name': 's3',
      'region_name': region_name,
    }

    if aws_access_key_id and aws_secret_access_key:
      client_args['aws_access_key_id'] = aws_access_key_id
      client_args['aws_secret_access_key'] = aws_secret_access_key

    if endpoint_url:
      client_args['endpoint_url'] = endpoint_url

    s3_client = boto3.client(**client_args)

    params = {
      'Bucket': bucket_name,
      'Key': object_key,
    }

    if content_type:
      params['ContentType'] = content_type

    presigned_url = s3_client.generate_presigned_url(
      ClientMethod='put_object',
      Params=params,
      ExpiresIn=expiration
    )

    return presigned_url

  except Exception as e:
    raise Exception(f"Failed to generate presigned URL: {str(e)}")
