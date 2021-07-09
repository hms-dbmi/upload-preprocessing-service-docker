"""
Utilities for interacting with the UDN Gateway
"""
import json
import requests


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
            success_message = "Marked File Complete: {}".format(id)
            print(success_message, flush=True)
            logger.debug(success_message)
        else:
            error_message = "Failed to Mark File Complete: {}".format(id)
            print(error_message, flush=True)
            logger.debug(error_message)
    except Exception:
        error_message = "Failed to Mark File Complete: {}".format(id)
        print(error_message, flush=True)
        logger.debug(error_message)
