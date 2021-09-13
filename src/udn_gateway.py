"""
Utilities for interacting with the UDN Gateway
"""
import json
import requests

from utilities import write_to_logs


def call_udngateway_mark_complete(id, secret, logger):
    """
    Call the UDN Gateway API to mark the file as complete
    """
    try:
        headers = {
            'Content-Type': 'application/json',
            'Authorization': 'Token {token}'.format(token=secret['udn_api_token'])
        }

        resp = requests.post(secret['udn_api_url'], data=json.dumps(
            {'id': id}), headers=headers, verify=False, timeout=5)

        if resp.status_code == 200:
            write_to_logs("Step 4: Mark File Complete: Successfully marked file with export id {} complete".format(id), logger)
        else:
            write_to_logs("Step 4: Mark File Complete: Failed to mark file with export id {} complete with status code {}".format(
                id, resp.status_code), logger)
    except Exception as exc:
        write_to_logs(
            "Step 4: Mark File Complete: Failed to mark file with export id {} complete with error {}".format(id, exc), logger)
