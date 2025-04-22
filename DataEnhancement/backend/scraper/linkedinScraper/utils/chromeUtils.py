import os
import socket
import json
import logging
import time
import random
import tempfile
from pathlib import Path
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import WebDriverException
from selenium_stealth import stealth
from ..utils.proxyUtils import format_proxy_for_chrome
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager

# Configurable constants
DEBUG_PORT_START = 9222
DEBUG_PORT_END = 9230
DEBUG_FOLDER = Path("backend/linkedinScraper/debug")
CHROME_INFO_FILE = DEBUG_FOLDER / "chrome_debug_info.json"
SMARTPROXY_USER = os.getenv("SMARTPROXY_USERNAME", "spvy76kscp")
SMARTPROXY_PASS = os.getenv("SMARTPROXY_PASSWORD", "bCE_1m7qkO0D1lzdjf")
SMARTPROXY_GATEWAY = os.getenv("SMARTPROXY_GATEWAY", "us.smartproxy.com")
SMARTPROXY_PORT = int(os.getenv("SMARTPROXY_PORT", 10001))


# Ensure debug folder exists
DEBUG_FOLDER.mkdir(exist_ok=True)

def is_port_available(port):
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.bind(('localhost', port))
        s.close()
        return True
    except:
        return False

def find_available_port():
    for port in range(DEBUG_PORT_START, DEBUG_PORT_END + 1):
        if is_port_available(port):
            return port
    raise RuntimeError("No available Chrome debug ports found.")

def save_chrome_info(port, user_data_dir):
    chrome_info = {
        'port': port,
        'user_data_dir': str(user_data_dir)
    }
    with open(CHROME_INFO_FILE, 'w') as f:
        json.dump(chrome_info, f)
    logging.info(f"Chrome debugging info saved to {CHROME_INFO_FILE}")

def load_chrome_info():
    if CHROME_INFO_FILE.exists():
        try:
            with open(CHROME_INFO_FILE, 'r') as f:
                chrome_info = json.load(f)
            return chrome_info
        except Exception as e:
            logging.error(f"Error loading Chrome info: {e}")
    return None

def is_chrome_running(port):
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        result = s.connect_ex(('localhost', port))
        s.close()
        return result == 0
    except:
        return False

def is_driver_active(driver):
    if driver is None:
        return False
    try:
        _ = driver.current_url
        return True
    except:
        return False

def get_chrome_driver(headless=False, max_retries=3, proxy_url=None):
    from ..utils.proxyUtils import SMARTPROXY_USER, SMARTPROXY_PASS, SMARTPROXY_GATEWAY, SMARTPROXY_PORT

    default_user_agent = (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/134.0.0.0 Safari/537.36"
    )

    for attempt in range(max_retries):
        port = find_available_port()
        logging.info(f"Launching Chrome in Incognito mode on port {port} (Attempt {attempt + 1})")

        chrome_options = Options()
        chrome_options.add_argument("--incognito")
        chrome_options.add_argument(f"--remote-debugging-port={port}")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--disable-blink-features=AutomationControlled")
        chrome_options.add_argument("--start-maximized")
        chrome_options.add_argument("--disable-notifications")
        chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
        chrome_options.add_experimental_option("useAutomationExtension", False)
        chrome_options.add_argument(f"--user-agent={default_user_agent}")

        # 💡 Use Smartproxy auth extension if proxy_url provided
        if proxy_url:
            plugin_path = create_proxy_auth_extension(
                SMARTPROXY_GATEWAY, SMARTPROXY_PORT,
                SMARTPROXY_USER, SMARTPROXY_PASS
            )
            chrome_options.add_extension(plugin_path)
            logging.info("🌐 Smartproxy authentication extension loaded.")

        if headless:
            chrome_options.add_argument("--headless=new")

        try:
            service = Service(ChromeDriverManager().install())
            driver = webdriver.Chrome(service=service, options=chrome_options)

            # 🥷 Apply stealth mode
            stealth(driver,
                languages=["en-US", "en"],
                vendor="Google Inc.",
                platform="Win32",
                webgl_vendor="Intel Inc.",
                renderer="Intel Iris OpenGL Engine",
                fix_hairline=True,
            )

            driver.implicitly_wait(10)
            save_chrome_info(port, "linkedin_profile_dummy")
            return driver

        except WebDriverException as e:
            logging.error(f"Chrome launch failed (Attempt {attempt + 1}): {e}")
            time.sleep(random.uniform(1.5, 3.0))

    raise RuntimeError("All attempts to launch Chrome driver failed.")


def create_proxy_auth_extension(proxy_host, proxy_port, proxy_user, proxy_pass):
    import zipfile
    import string
    import random

    manifest_json = """
    {
        "version": "1.0.0",
        "manifest_version": 2,
        "name": "Smartproxy Auth Extension",
        "permissions": [
            "proxy",
            "tabs",
            "unlimitedStorage",
            "storage",
            "<all_urls>",
            "webRequest",
            "webRequestBlocking"
        ],
        "background": {
            "scripts": ["background.js"]
        }
    }
    """

    background_js = f"""
    chrome.runtime.onInstalled.addListener(function() {{
        chrome.proxy.settings.set({{
            value: {{
                mode: "fixed_servers",
                rules: {{
                    singleProxy: {{
                        scheme: "http",
                        host: "{proxy_host}",
                        port: {proxy_port}
                    }},
                    bypassList: ["localhost"]
                }}
            }},
            scope: "regular"
        }}, function() {{}});

        chrome.webRequest.onAuthRequired.addListener(
            function(details) {{
                return {{
                    authCredentials: {{
                        username: "{proxy_user}",
                        password: "{proxy_pass}"
                    }}
                }};
            }},
            {{urls: ["<all_urls>"]}},
            ['blocking']
        );
    }});
    """

    plugin_file = f"smartproxy_auth_{''.join(random.choices(string.ascii_lowercase, k=5))}.zip"
    temp_dir = tempfile.gettempdir()
    plugin_path = os.path.join(temp_dir, plugin_file)  # ✅ platform-safe

    with zipfile.ZipFile(plugin_path, 'w') as zp:
        zp.writestr("manifest.json", manifest_json)
        zp.writestr("background.js", background_js)

    return plugin_path