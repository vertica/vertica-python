# Copyright (c) 2024 Open Text.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from __future__ import print_function, division, absolute_import, annotations

import requests
import warnings

from ..errors import OAuthConfigurationError, OAuthEndpointDiscoveryError, OAuthTokenRefreshError


class OAuthManager:
    def __init__(self, refresh_token):
        self.refresh_token = refresh_token
        self.client_id = ""
        self.client_secret = ""
        self.token_url = ""
        self.discovery_url = ""
        self.scope = ""
        self.validate_cert_hostname = None
        self.refresh_attempted = False

    def set_config(self, configs, not_set_only=False) -> None:
        valid_keys = {'refresh_token', 'client_id', 'client_secret', 'token_url', 'discovery_url',
                      'scope', 'validate_hostname', 'auth_url'}
        try:
            for k, v in configs.items():
                if k not in valid_keys:
                    invalid_key = f'Unrecognized OAuth config property: {k}'
                    warnings.warn(invalid_key)
                    continue
                if v is None or v == "":  # ignore empty value
                    continue
                if k == 'refresh_token' and not (not_set_only and self.refresh_token):
                    self.refresh_token = str(v)
                elif k == 'client_id' and not (not_set_only and self.client_id):
                    self.client_id = str(v)
                elif k == 'client_secret' and not (not_set_only and self.client_secret):
                    self.client_secret = str(v)
                elif k == 'token_url' and not (not_set_only and self.token_url):
                    self.token_url = str(v)
                elif k == 'discovery_url' and not (not_set_only and self.discovery_url):
                    self.discovery_url = str(v)
                elif k == 'scope' and not (not_set_only and self.scope):
                    self.scope = str(v)
                elif k == 'validate_hostname' and not (not_set_only and self.validate_cert_hostname is not None):
                    self.validate_cert_hostname = bool(v)
        except Exception as e:
            raise OAuthConfigurationError('Failed setting OAuth configuration.') from e

    def get_access_token_using_refresh_token(self) -> str:
        """Issue a new access token using a valid refresh token."""
        headers = {
            "Content-Type": "application/x-www-form-urlencoded",
            "Cache-Control": "no-cache",
            "Pragma": "no-cache",
            "Expires": "0",
        }
        params = {
          "client_id": self.client_id,
          "client_secret": self.client_secret,
          "grant_type": "refresh_token",
          "refresh_token": self.refresh_token,
        }
        if self.scope:
            params["scope"] = self.scope
        err_msg = 'Failed getting OAuth access token from a refresh token.'
        try:
            # TODO handle self.validate_cert_hostname
            response = requests.post(self.token_url, headers=headers, data=params, verify=False)
            response.raise_for_status()
            return response.json()["access_token"]
        except requests.exceptions.HTTPError as err:
            msg = f'{err_msg}\n{err}\n{response.json()}'
            raise OAuthTokenRefreshError(msg)
        except Exception as e:
            raise OAuthTokenRefreshError(err_msg) from e

    def get_token_url_from_discovery_url(self) -> str:
        try:
            headers = {
                "Cache-Control": "no-cache",
                "Pragma": "no-cache",
                "Expires": "0",
            }
            # TODO handle self.validate_cert_hostname
            response = requests.get(self.discovery_url, headers=headers, verify=False)
            response.raise_for_status()
            return response.json()["token_endpoint"]
        except Exception as e:
            err_msg = 'Failed getting token url from discovery url.'
            raise OAuthEndpointDiscoveryError(err_msg) from e

    def do_token_refresh(self) -> str:
        self.refresh_attempted = True

        if len(self.token_url) == 0 and len(self.discovery_url) == 0:
            raise OAuthTokenRefreshError('Token URL or Discovery URL must be set.')
        if len(self.client_id) == 0:
            raise OAuthTokenRefreshError('OAuth client id is missing.')
        if len(self.client_secret) == 0:
            raise OAuthTokenRefreshError('OAuth client secret is missing.')
        if len(self.refresh_token) == 0:
            raise OAuthTokenRefreshError('OAuth refresh token is missing.')

        # If the token url is not set, get it from the discovery url
        if len(self.token_url) == 0:
            self.token_url = self.get_token_url_from_discovery_url()

        return self.get_access_token_using_refresh_token()


