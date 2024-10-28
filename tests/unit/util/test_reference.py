from datetime import datetime, timezone

from cladetime.util.reference import _get_s3_object_url

# @mock.patch("subprocess.run")
# def test_get_nextclade_dataset(tmp_path):
#     dataset_path = get_nextclade_dataset("2021-09-01", tmp_path)

#     # the dataset_path being returned should contain the correct nextclade
#     # datasetset version, as determined by the as_of_date being passed
#     # (returned version is temporarily hard-coded until Nextstrain provides the info we need)
#     assert "2024-07-17--12-57-03Z" in str(dataset_path)


def test__get_s3_object_url(s3_setup):
    s3_client, bucket_name, s3_object_keys = s3_setup

    target_date = datetime.strptime("2023-02-15", "%Y-%m-%d").replace(tzinfo=timezone.utc)
    object_key = s3_object_keys["sequence_metadata"]

    version_id, version_url = _get_s3_object_url(bucket_name, object_key, target_date)

    assert version_id is not None
    s3_object = s3_client.get_object(Bucket=bucket_name, Key=object_key, VersionId=version_id)
    last_modified = s3_object["LastModified"]

    assert last_modified <= target_date
    assert last_modified == datetime.strptime("2023-02-05 14:33:06", "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
    assert version_url == f"https://{bucket_name}.s3.amazonaws.com/{object_key}?versionId={version_id}"
