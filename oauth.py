""""This is modified from https://github.com/sfc-gh-bhess/st_oauth"""

from urllib.parse import urlencode
import requests
import streamlit as st
from streamlit.components.v1 import html

import string
import random

_STKEY = 'ST_OAUTH'
_DEFAULT_SECKEY = 'oauth'

@st.cache_resource(ttl=300)
def qparms_cache(key):
    return {}

def logout():
    if _STKEY in st.session_state:
        del st.session_state[_STKEY]

def string_num_generator(size):
    chars = string.ascii_uppercase + string.ascii_lowercase + string.digits
    return ''.join(random.choice(chars) for _ in range(size))

def validate_config(config):
    required_config_options = [ 'authorization_endpoint', 
                                'token_endpoint',
                                'redirect_uri',
                                'client_id',
                                'client_secret',
                                'scope' ]
    return all([k in config for k in required_config_options])

def open_page(url):
    open_script= """
        <script type="text/javascript">
            window.open('%s', '_blank').focus();
        </script>
    """ % (url)
    html(open_script)

def show_auth_link(config, label, but):
    state_parameter = string_num_generator(15)
    query_params = urlencode({'redirect_uri': config['redirect_uri'], 'client_id': config['client_id'], 'response_type': 'code', 'state': state_parameter, 'scope': config['scope']})
    request_url = f"{config['authorization_endpoint']}?{query_params}"
    if st.query_params():
        qpcache = qparms_cache(state_parameter)
        qpcache = st.query_params()
    but.button(label, on_click=open_page, args=(request_url,), type="primary")
    st.stop()

def st_oauth(config=None, label="Login via OAuth", but=None):
    if not config:
        config = _DEFAULT_SECKEY
    if isinstance(config, str):
        config = st.secrets[config]
    if _STKEY in st.session_state:
        token = st.session_state[_STKEY]
    if _STKEY not in st.session_state:
        if not validate_config(config):
            st.error("Invalid OAuth Configuration")
            st.stop()
        if 'code' not in st.query_params():
            show_auth_link(config, label, but)
        code = st.query_params()['code'][0]
        state = st.query_params()['state'][0]
        qpcache = qparms_cache(state)
        qparms = qpcache
        qpcache = {}
        st.experimental_set_query_params(**qparms)
        theaders = {
                        'Content-type': 'application/x-www-form-urlencoded;charset=utf-8'
                    }
        tdata = {
                    'grant_type': 'authorization_code',
                    'redirect_uri': config['redirect_uri'],
                    'client_id': config['client_id'],
                    'client_secret': config['client_secret'],
                    'scope': config['scope'],
                    'state': state,
                    'code': code,
                }
        try:
            ret = requests.post(config["token_endpoint"], headers=theaders, data=urlencode(tdata).encode("utf-8"))
            ret.raise_for_status()
        except requests.exceptions.RequestException as e:
            st.error(e)
            show_auth_link(config, label)
        token = ret.json()
        st.session_state[_STKEY] = token

    if _STKEY in st.session_state:
        st.button("Logout", on_click=logout, type="secondary")
