"""
Utilities for interacting with the UDN Gateway
"""
import requests

from utilities import write_to_logs


def call_udngateway_mark_complete(file_id, secret, logger):
    """
    Call the UDN Gateway API to mark the file as complete
    """
    try:
        headers = {
            'Content-Type': 'application/json',
            'Authorization': 'Token {token}'.format(token=secret['udn_api_token'])
        }

        url = '{}/api/dbgap/exported_files/{}/complete'.format(secret['udn_api_url'], file_id)

        resp = requests.post(url, headers=headers, verify=False, timeout=5)

        if resp.status_code == 200:
            msg = "Step 4: Mark File Complete: Successfully marked file {} complete".format(file_id)
        else:
            msg = "Step 4: Mark File Complete: Failed to mark file {} complete with status code {}".format(
                file_id, resp.status_code)

        write_to_logs(msg, logger)
    except Exception as exc:
        msg = "Step 4: Mark File Complete: Failed to mark file {} complete with error {}".format(id, exc)
        write_to_logs(msg, logger)
