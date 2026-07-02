import streamlit as st
import yaml
from mstr_robotics.redis_db import RedisBiAnalysis
from mstr_robotics.json_compare import JSONPathHelper, JSONFilterUtils, remove_after_last_dot_if_bracket
import requests
import uuid
import json
import pandas as pd
import os
from pathlib import Path
from dotenv import load_dotenv

# ==================== CONFIGURATION ====================

# Load environment variables from config/streamlit.env
config_dir = Path(__file__).parent.parent / 'config'
env_file = config_dir / 'streamlit.env'
if env_file.exists():
    load_dotenv(env_file)

# Configuration with fallback defaults
class StreamlitConfig:
    """Centralized configuration for Streamlit app"""

    # API Server
    API_SERVER_HOST = os.getenv('API_SERVER_HOST', 'localhost')
    API_SERVER_PORT = os.getenv('API_SERVER_PORT', '8000')
    API_SERVER_BASE_URL = os.getenv('API_SERVER_BASE_URL', f'http://{API_SERVER_HOST}:{API_SERVER_PORT}')

    # MicroStrategy
    MSTR_BASE_URL = os.getenv('MSTR_BASE_URL', 'http://217.154.213.84:8080/MicroStrategyLibrary/api')
    MSTR_USERNAME = os.getenv('MSTR_USERNAME', 'Administrator')
    MSTR_PASSWORD = os.getenv('MSTR_PASSWORD', '[REMOVED-PASSWORD]')

    # Default Object Loader
    DEFAULT_REDIS_KEY = os.getenv('DEFAULT_REDIS_KEY', 'DOCUMENT_DEFINITION:98EB31B54122FFB738E6E08A2F29421A')

# ==================== UTILITY CLASSES ====================


class UIComponents:
    """Reusable UI components for the application"""

    @staticmethod
    def render_expand_depth_control():
        """Render expand depth control section"""
        st.divider()
        st.markdown("**⚙️ Expand Depth Control**")
        col_button, col_label, col_slider = st.columns([1, 1, 3])

        with col_button:
            if st.button("📄 Show Complete", help="Reload objects and display complete definition", use_container_width=True):
                # Clear all session state objects and navigation to force reload
                if 'org_obj_def' in st.session_state:
                    del st.session_state.org_obj_def
                if 'comp_obj_def' in st.session_state:
                    del st.session_state.comp_obj_def
                st.session_state.path = []
                if 'selected_diff_paths' in st.session_state:
                    st.session_state.selected_diff_paths = []
                if 'selected_diff_types' in st.session_state:
                    st.session_state.selected_diff_types = []
                # Set flag to trigger auto-reload
                st.session_state.auto_reload_requested = True
                st.rerun()

        with col_label:
            st.write("Levels to expand:")

        with col_slider:
            st.slider(
                "Levels to expand",
                min_value=0,
                max_value=5,
                value=st.session_state.expand_depth,
                key="expand_depth",
                label_visibility="collapsed",
                help="Controls how many levels deep the JSON is expanded. 0=collapsed, 5=deeply expanded. Both viewers will sync to this depth."
            )

    @staticmethod
    def render_manual_path_input(org_obj_def, comp_obj_def):
        """Render manual path input section"""
        st.write("Enter a comma-separated path (e.g., `advancedProperties, drillOptions, drillingEnableReportDrilling`)")

        col_input, col_buttons = st.columns([3, 1])

        with col_input:
            path_input = st.text_input(
                "Path:",
                placeholder="key1, key2, key3",
                label_visibility="collapsed"
            )

        with col_buttons:
            if st.button("Navigate to Path", use_container_width=True):
                if path_input:
                    # Parse the path
                    new_path = [p.strip() for p in path_input.split(',')]
                    # Try to convert numeric strings to integers for list indices
                    parsed_path = []
                    for p in new_path:
                        if p.isdigit():
                            parsed_path.append(int(p))
                        else:
                            parsed_path.append(p)

                    # Validate path exists in object(s)
                    test_1 = JSONPathHelper.get_nested_value(org_obj_def, parsed_path)
                    test_2 = JSONPathHelper.get_nested_value(comp_obj_def, parsed_path) if comp_obj_def else None

                    if test_1 is not None or test_2 is not None:
                        st.session_state.path = parsed_path
                        st.success(f"✅ Navigated to: {' > '.join(str(p) for p in parsed_path)}")
                        st.rerun()
                    else:
                        st.error("❌ Path not found in object(s)")

    @staticmethod
    def render_structure_depth_control():
        """Render structure discovery depth control"""
        st.divider()

        depth_col1, depth_col2 = st.columns([2, 3])
        with depth_col1:
            st.write("**Structure Discovery Depth:**")
        with depth_col2:
            if 'structure_depth' not in st.session_state:
                st.session_state.structure_depth = 3
            structure_depth = st.number_input(
                "Depth",
                min_value=1,
                max_value=6,
                value=st.session_state.structure_depth,
                key="structure_depth_input",
                label_visibility="collapsed",
                help="How deep to search in JSON structure (1-6 levels)"
            )
        return structure_depth

    @staticmethod
    def render_navigation_controls():
        """Render Back and Reset navigation controls"""
        st.divider()

        col_back, col_reset = st.columns([1, 1])

        with col_back:
            if st.button("⬅️ Back") and len(st.session_state.path) > 0:
                st.session_state.path.pop()
                st.rerun()

        with col_reset:
            if st.button("🏠 Reset to Root"):
                st.session_state.path = []
                st.rerun()


class ThreeLevelNavigator:
    """Handles 3-level hierarchical navigation with up/down buttons"""

    def __init__(self, key_prefix, data_dict, org_obj_def, comp_obj_def):
        """
        Args:
            key_prefix: Unique prefix for session state keys (e.g., 'struct', 'diff')
            data_dict: Dictionary of {category: [(path, label), ...]} or {category: [path, ...]}
            org_obj_def: First JSON object for validation
            comp_obj_def: Second JSON object for validation
        """
        self.key_prefix = key_prefix
        self.data_dict = data_dict
        self.org_obj_def = org_obj_def
        self.comp_obj_def = comp_obj_def
        self._init_session_state()

    def _init_session_state(self):
        """Initialize session state variables"""
        if f'{self.key_prefix}_category_index' not in st.session_state:
            st.session_state[f'{self.key_prefix}_category_index'] = 0
        if f'{self.key_prefix}_subcategory_index' not in st.session_state:
            st.session_state[f'{self.key_prefix}_subcategory_index'] = 0
        if f'{self.key_prefix}_path_index' not in st.session_state:
            st.session_state[f'{self.key_prefix}_path_index'] = 0

    def _render_level_selector(self, label, options, current_index, button_prefix, allow_blank=True):
        """Render a single level with up/down buttons and selectbox"""
        # Add blank option at the beginning if allowed
        if allow_blank and (not options or options[0] != ""):
            display_options = [""] + list(options)
        else:
            display_options = list(options)

        col_nav, col_select = st.columns([1, 8])

        with col_nav:
            col_up, col_down = st.columns(2)
            with col_up:
                if st.button("⬆️", key=f"{button_prefix}_up", help=f"Previous {label}", use_container_width=True):
                    return (current_index - 1) % len(display_options), 'up'
            with col_down:
                if st.button("⬇️", key=f"{button_prefix}_down", help=f"Next {label}", use_container_width=True):
                    return (current_index + 1) % len(display_options), 'down'

        with col_select:
            selected = st.selectbox(
                f"{label}:",
                options=display_options,
                index=current_index,
                key=f"{button_prefix}_selector",
                label_visibility="collapsed"
            )
            if selected:
                new_index = display_options.index(selected)
                if new_index != current_index:
                    return new_index, 'select'
            elif selected == "":
                # Blank option selected
                new_index = 0
                if new_index != current_index:
                    return new_index, 'select'

        return current_index, None

    def _group_by_second_level(self, category_items):
        """Group items by second level path component
        Ensures all level 1 paths are available in level 2 and level 3
        """
        subcategories = {}
        level_1_items = set()  # Track unique level 1 paths

        for item in category_items:
            # Handle both tuple (path, label) and string path
            path_str = item[0] if isinstance(item, tuple) else item
            parts = path_str.split('.')

            # Track level 1 path
            level_1 = parts[0]
            level_1_items.add(level_1)

            if len(parts) >= 2:
                subcat = '.'.join(parts[:2])
            else:
                subcat = parts[0]

            if subcat not in subcategories:
                subcategories[subcat] = []
            subcategories[subcat].append(item)

        # Ensure all level 1 paths exist in subcategories
        for level_1 in level_1_items:
            if level_1 not in subcategories:
                # Add level 1 path as its own subcategory
                # Create appropriate format (tuple or string)
                if category_items and isinstance(category_items[0], tuple):
                    subcategories[level_1] = [(level_1, level_1)]
                else:
                    subcategories[level_1] = [level_1]

        return subcategories

    def _ensure_parent_paths_in_level3(self, paths, subcategory):
        """Ensure that the parent path (subcategory) exists in level 3 paths
        Also ensures level 1 path exists if subcategory is level 2
        """
        if not paths:
            return paths

        is_tuple_format = isinstance(paths[0], tuple)
        path_strings = [p[0] if is_tuple_format else p for p in paths]

        # Extract level 1 and level 2 components
        parts = subcategory.split('.')
        level_1 = parts[0]

        # Check if level 1 path exists in level 3
        if level_1 not in path_strings:
            if is_tuple_format:
                paths.insert(0, (level_1, level_1))
            else:
                paths.insert(0, level_1)
            path_strings.insert(0, level_1)

        # Check if level 2 path (subcategory) exists in level 3
        if len(parts) >= 2 and subcategory not in path_strings:
            if is_tuple_format:
                paths.insert(1, (subcategory, subcategory))
            else:
                paths.insert(1, subcategory)

        return paths

    def render(self):
        """Render the complete 3-level navigation and return selected path"""
        if not self.data_dict:
            return None

        # Level 1: Category (with blank option)
        categories = [""] + list(self.data_dict.keys())
        cat_index = st.session_state[f'{self.key_prefix}_category_index']

        new_cat_index, action = self._render_level_selector(
            "Level 1", categories, cat_index, f"{self.key_prefix}_cat", allow_blank=False
        )

        if action:
            st.session_state[f'{self.key_prefix}_category_index'] = new_cat_index
            st.session_state[f'{self.key_prefix}_subcategory_index'] = 0
            st.session_state[f'{self.key_prefix}_path_index'] = 0
            if action in ['up', 'down']:
                st.rerun()

        category = categories[st.session_state[f'{self.key_prefix}_category_index']]

        # If blank selected at level 1, stop here
        if not category or category == "":
            return None

        # Level 2: Subcategory (with blank option)
        subcategories = self._group_by_second_level(self.data_dict[category])
        subcat_keys = [""] + list(subcategories.keys())
        subcat_index = st.session_state[f'{self.key_prefix}_subcategory_index']

        new_subcat_index, action = self._render_level_selector(
            "Level 2", subcat_keys, subcat_index, f"{self.key_prefix}_subcat", allow_blank=False
        )

        if action:
            st.session_state[f'{self.key_prefix}_subcategory_index'] = new_subcat_index
            st.session_state[f'{self.key_prefix}_path_index'] = 0
            if action in ['up', 'down']:
                st.rerun()

        subcategory = subcat_keys[st.session_state[f'{self.key_prefix}_subcategory_index']]

        # If blank selected at level 2, return the category path
        if not subcategory or subcategory == "":
            return category

        # Level 3: Paths (with blank option)
        paths = subcategories[subcategory]
        path_index = st.session_state[f'{self.key_prefix}_path_index']

        # Ensure level 2 path exists in level 3 paths
        paths = self._ensure_parent_paths_in_level3(paths, subcategory)

        # Create display options
        if paths and isinstance(paths[0], tuple):
            # Structure format: (numeric_path, display_path)
            display_options = [display_path for _, display_path in paths]
        else:
            # Differences format: just strings
            display_options = paths

        # Add blank option
        display_options = [""] + display_options
        paths = [None] + paths

        new_path_index, action = self._render_level_selector(
            "Level 3", display_options, path_index, f"{self.key_prefix}_path", allow_blank=False
        )

        if action:
            st.session_state[f'{self.key_prefix}_path_index'] = new_path_index
            if action in ['up', 'down']:
                # Auto-navigate when using arrows
                selected_item = paths[new_path_index]
                if selected_item:
                    return self._extract_numeric_path(selected_item)
                else:
                    return None

        st.session_state[f'{self.key_prefix}_path_index'] = new_path_index

        # Return selected path (for Go button)
        if path_index < len(paths) and paths[path_index]:
            return self._extract_numeric_path(paths[path_index])
        # If blank at level 3, return the subcategory path
        return subcategory

    def _extract_numeric_path(self, item):
        """Extract numeric path from item (handles both tuple and string)"""
        if isinstance(item, tuple):
            return item[0]  # Return numeric path from (numeric_path, display_path)
        return item  # Return string path directly


# ==================== MANAGER CLASSES ====================

class RedisManager:
    """Handles Redis connection and data operations"""

    @staticmethod
    def load_redis_config():
        """Load Redis configuration from YAML file"""
        try:
            import os
            script_dir = os.path.dirname(os.path.abspath(__file__))
            config_path = os.path.join(os.path.dirname(script_dir), 'config', 'mstr_redis_y.yml')

            with open(config_path, 'r') as openfile:
                return yaml.safe_load(openfile)
        except Exception as e:
            st.error(f"Error loading config: {e}")
            return None

    @staticmethod
    def connect_to_redis(redis_con_d):
        """Connect to Redis database"""
        try:
            i_redis = RedisBiAnalysis(
                host=redis_con_d["host"],
                port=redis_con_d["port"],
                password=redis_con_d["password"],
                username=redis_con_d["username"],
                decode_responses=redis_con_d["decode_responses"]
            )
            return i_redis
        except Exception as e:
            st.error(f"Failed to connect to Redis: {e}")
            return None

    @staticmethod
    def fetch_objects_from_redis(i_redis, prefix_1, prefix_2, redis_key):
        """Fetch objects from Redis and update session state"""
        redis_key_1 = f"{prefix_1}:{redis_key}" if prefix_1 else redis_key

        obj_1 = i_redis.fetch_key_value(redis_key_1)
        if obj_1:
            st.session_state.org_obj_def = obj_1["value"]
            st.session_state.path = []
        else:
            st.error(f"Key not found: {redis_key_1}")
            obj_1 = None

        obj_2 = None
        if prefix_2:
            redis_key_2 = f"{prefix_2}:{redis_key}"
            obj_2 = i_redis.fetch_key_value(redis_key_2)
            if obj_2:
                st.session_state.comp_obj_def = obj_2["value"]
            else:
                st.error(f"Key not found: {redis_key_2}")
                obj_2 = None

        return obj_1, obj_2


class ComparisonManager:
    """Handles comparison operations and analysis"""

    @staticmethod
    def run_comparison_analysis(uploaded_file, username, password, base_url, redis_config, selected_redis_env):
        """Process comparison analysis and return differences

        Args:
            uploaded_file: The uploaded file object from Streamlit
            username: Username credential
            password: Password credential
            base_url: MicroStrategy API base URL
            redis_config: Redis configuration dictionary
            selected_redis_env: Selected Redis environment name

        Returns:
            list: List of dictionaries with differences
        """
        differences = []

        playbook_d = None
        if uploaded_file is not None:
            file_name = uploaded_file.name
            file_extension = file_name.split('.')[-1].lower()
            file_content = uploaded_file.read()

            if file_extension in ['yaml', 'yml']:
                playbook_d = yaml.safe_load(file_content)
            elif file_extension == 'json':
                playbook_d = json.loads(file_content)

        conn_params = {
            "username": username,
            "password": password,
            "base_url": base_url
        }
        session_id = str(uuid.uuid4())
        api_base_url = StreamlitConfig.API_SERVER_BASE_URL

        # Login to MSTR
        requests.post(f"{api_base_url}/login", json={
            "session_id": session_id,
            "conn_params": conn_params
            }
        )

        # Connect to Redis
        requests.post(f"{api_base_url}/connect_redis", json={
            "session_id": session_id,
            "redis_config": redis_config,
            "selected_env": selected_redis_env
            }
        )

        response = requests.post(f"{api_base_url}/run_comparison",
            json={"session_id": session_id,
                  "play_compare_d": playbook_d}
        )

        differences = response.json()
        return differences

    @staticmethod
    def update_found_differences(differences_list):
        """Update the found differences in session state

        IMPORTANT: Always clears existing differences before adding new ones
        """
        st.session_state.found_differences = []
        st.session_state.found_differences = differences_list["result"]

        if 'selected_diff_key' in st.session_state:
            del st.session_state.selected_diff_key

        if 'temp_redis_key' in st.session_state:
            del st.session_state.temp_redis_key
        if 'temp_diff_paths' in st.session_state:
            del st.session_state.temp_diff_paths
        if 'selected_diff_paths' in st.session_state:
            del st.session_state.selected_diff_paths


class DifferencesRenderer:
    """Handles rendering of differences UI components"""

    @staticmethod
    def render_redis_config_uploader():
        """Render Redis config file uploader

        Returns:
            dict: Uploaded Redis configuration or None
        """
        uploaded_config_file = st.file_uploader(
            "Upload Redis Configuration (mstr_redis_y.yml):",
            type=['yml', 'yaml', 'json'],
            help="Upload the mstr_redis_y.yml file containing Redis environment configurations"
        )

        if uploaded_config_file is not None:
            try:
                file_extension = uploaded_config_file.name.split('.')[-1].lower()
                file_content = uploaded_config_file.read()

                if file_extension in ['yaml', 'yml']:
                    redis_config = yaml.safe_load(file_content)
                elif file_extension == 'json':
                    redis_config = json.loads(file_content)
                else:
                    st.error("Unsupported file format")
                    return None

                # Validate config structure
                if "redis_env_d" not in redis_config:
                    st.error("Invalid config file: missing 'redis_env_d' key")
                    return None

                st.session_state.uploaded_redis_config = redis_config
                st.success(f"✅ Config loaded: {len(redis_config['redis_env_d'])} environment(s) found")
                return redis_config

            except Exception as e:
                st.error(f"Error loading config file: {e}")
                return None

        # Return previously uploaded config from session state
        return st.session_state.get('uploaded_redis_config', None)

    @staticmethod
    def render_redis_environment_selector(redis_config):
        """Render Redis environment selection with connection status

        Returns:
            tuple: (selected_env, i_redis) - selected environment and Redis connection
        """
        if not redis_config or "redis_env_d" not in redis_config:
            st.warning("⚠️ Please upload a Redis configuration file first")
            return None, None

        col_env, col_status = st.columns([3, 1])
        with col_env:
            available_envs = list(redis_config.get("redis_env_d", {}).keys())
            if not available_envs:
                st.warning("⚠️ No Redis environments found in config")
                return None, None

            selected_env = st.selectbox("Select Redis Environment:", available_envs)

        with col_status:
            st.write("")
            st.write("")

        i_redis = None
        if selected_env:
            redis_con_d = redis_config["redis_env_d"][selected_env]
            i_redis = RedisManager.connect_to_redis(redis_con_d)

            if i_redis:
                with col_status:
                    st.success(f"✅ Connected to Redis ({selected_env})")

        return selected_env, i_redis

    @staticmethod
    def render_comparison_section():
        """Render comparison section with file upload and run button"""
        st.divider()
        st.subheader("Run Comparison")

        uploaded_file = st.file_uploader(
            "Upload comparison file:",
            type=None,
            help="Upload a file for comparison analysis"
        )

        base_url = st.text_input(
            "Base URL:",
            value=StreamlitConfig.MSTR_BASE_URL,
            placeholder="Enter MicroStrategy API base URL"
        )

        col_user, col_pass = st.columns(2)
        with col_user:
            username = st.text_input(
                "Username:",
                value=StreamlitConfig.MSTR_USERNAME,
                placeholder="Enter username"
            )

        with col_pass:
            password = st.text_input(
                "Password:",
                value=StreamlitConfig.MSTR_PASSWORD,
                type="password",
                placeholder="Enter password"
            )

        return uploaded_file, username, password, base_url

    @staticmethod
    def handle_comparison_run_button(uploaded_file, username, password, base_url, redis_config, selected_redis_env):
        """Handle the run comparison button click"""
        if st.button("▶️ Run", type="primary", use_container_width=True):
            if uploaded_file is None:
                st.warning("⚠️ Please upload a file first")
            elif not redis_config:
                st.warning("⚠️ Please upload Redis configuration first")
            elif not selected_redis_env:
                st.warning("⚠️ Please select a Redis environment first")
            else:
                with st.spinner("Running comparison analysis..."):
                    try:
                        differences = ComparisonManager.run_comparison_analysis(
                            uploaded_file, username, password, base_url, redis_config, selected_redis_env
                        )
                        ComparisonManager.update_found_differences(differences)
                        st.success(f"✅ Comparison completed! Found {len(differences)} objects with differences")
                        st.rerun()
                    except Exception as e:
                        st.error(f"❌ Error during comparison: {e}")

    @staticmethod
    def render_found_differences_row(idx, item):
        """Render a single row in the found differences table"""
        obj_key = item['org_obj_key']
        diff_paths = item['json_key_path']
        no_number_paths = item.get('no_number_path', diff_paths)
        diff_count = len(diff_paths)

        if diff_count <= 3:
            diff_display = ", ".join(no_number_paths)
        else:
            diff_display = ", ".join(no_number_paths[:2]) + f" ... (+{diff_count - 2} more)"

        col1, col2, col3 = st.columns([4, 6, 1])

        with col1:
            st.text(obj_key)
        with col2:
            st.text(diff_display)
        with col3:
            return st.button("Load", key=f"load_btn_{idx}")

    @staticmethod
    def handle_load_difference(obj_key, diff_paths, diff_types):
        """Handle loading a specific difference from the table"""
        if ":" in obj_key:
            parts = obj_key.split(":", 1)
            prefix = parts[0]
            redis_key = parts[1]
        else:
            prefix = ""
            redis_key = obj_key

        # Purge existing form values
        if 'org_obj_def' in st.session_state:
            del st.session_state.org_obj_def
        if 'comp_obj_def' in st.session_state:
            del st.session_state.comp_obj_def

        st.session_state.path = []
        st.session_state.selected_diff_paths = diff_paths
        st.session_state.selected_diff_types = diff_types

        # Reset navigator indices
        for key in list(st.session_state.keys()):
            if key.startswith('diff_category_index') or key.startswith('diff_subcategory_index') or key.startswith('diff_path_index'):
                del st.session_state[key]

        # Set session state and trigger auto-load
        st.session_state.selected_redis_key = redis_key
        st.session_state.selected_prefix_1 = prefix
        st.session_state.auto_load_comparison = True
        st.rerun()

    @staticmethod
    def render_found_differences_table():
        """Render the found differences table"""
        st.divider()
        st.subheader("📊 Found Differences")

        if st.session_state.found_differences:
            st.markdown("### Objects with Differences")

            col1, col2, col3 = st.columns([4, 6, 1])
            with col1:
                st.markdown("**Object Key**")
            with col2:
                st.markdown("**Differences**")
            with col3:
                st.markdown("**Action**")

            st.markdown("---")

            for idx, item in enumerate(st.session_state.found_differences):
                load_clicked = DifferencesRenderer.render_found_differences_row(idx, item)
                if load_clicked:
                    DifferencesRenderer.handle_load_difference(
                        item['org_obj_key'],
                        item['json_key_path'],
                        item.get('diff_types', [])
                    )

                if idx < len(st.session_state.found_differences) - 1:
                    st.markdown("---")
        else:
            st.info("ℹ️ No differences found. Run a comparison first.")


class ObjectLoaderRenderer:
    """Handles rendering of object loading UI components"""

    @staticmethod
    def get_prefix_inputs():
        """Render prefix input fields and return values"""
        # Initialize widgets in session state if not present
        if 'prefix_1_widget' not in st.session_state:
            st.session_state.prefix_1_widget = ''
        if 'prefix_2_widget' not in st.session_state:
            st.session_state.prefix_2_widget = 'mstr_test'

        # Check if we're auto-loading and should update prefix_1
        auto_load = st.session_state.get('auto_load_comparison', False)
        if auto_load:
            selected_prefix = st.session_state.get('selected_prefix_1', '')
            if selected_prefix:
                st.session_state.prefix_1_widget = selected_prefix

        col1, col2 = st.columns(2)

        with col1:
            prefix_1 = st.text_input(
                "Prefix 1:",
                placeholder="mstr_dev",
                key="prefix_1_widget"
            )

        with col2:
            prefix_2 = st.text_input(
                "Prefix 2:",
                placeholder="mstr_test",
                key="prefix_2_widget"
            )

        return prefix_1, prefix_2

    @staticmethod
    def get_redis_key_input():
        """Render redis key input and handle auto-load"""
        auto_load = st.session_state.get('auto_load_comparison', False)

        if 'redis_key_widget' not in st.session_state:
            st.session_state.redis_key_widget = StreamlitConfig.DEFAULT_REDIS_KEY

        if auto_load:
            selected_key = st.session_state.get('selected_redis_key', '')
            if selected_key:
                st.session_state.redis_key_widget = selected_key
            st.session_state.auto_load_comparison = False

        if 'temp_redis_key' in st.session_state:
            st.session_state.redis_key_widget = st.session_state.temp_redis_key
            del st.session_state.temp_redis_key

        redis_key = st.text_input(
            "Object Key:",
            placeholder="TYPE:object_id",
            key="redis_key_widget"
        )

        return redis_key

    @staticmethod
    def render_loaded_status(prefix_1, prefix_2):
        """Render status indicators for loaded objects"""
        col_status1, col_status2 = st.columns(2)
        with col_status1:
            if 'org_obj_def' in st.session_state:
                st.success(f"✅ Connected to Redis ({prefix_1})")

        with col_status2:
            if prefix_2 and 'comp_obj_def' in st.session_state:
                st.success(f"✅ Connected to Redis ({prefix_2})")


class NavigationRenderer:
    """Handles rendering of navigation UI components"""

    @staticmethod
    def render_structure_navigator(org_obj_def, comp_obj_def, structure_depth):
        """Render structure navigation panel"""
        st.write("**📊 Structure**")

        paths_obj1 = extract_all_paths(org_obj_def, max_depth=structure_depth)
        paths_obj2 = extract_all_paths(comp_obj_def, max_depth=structure_depth) if comp_obj_def else []

        all_paths_dict = {}
        for numeric_path, display_path in paths_obj1 + paths_obj2:
            if numeric_path not in all_paths_dict:
                all_paths_dict[numeric_path] = display_path

        all_paths = sorted(all_paths_dict.items(), key=lambda x: x[1])

        structure_categories = {}
        for numeric_path, display_path in all_paths:
            top_key = numeric_path.split('.')[0].split('[')[0]
            if top_key not in structure_categories:
                structure_categories[top_key] = []
            structure_categories[top_key].append((numeric_path, display_path))

        if structure_categories:
            navigator = ThreeLevelNavigator('structure', structure_categories, org_obj_def, comp_obj_def)
            selected_path_str = navigator.render()

            if st.button("📍 Go", key="struct_go", use_container_width=True):
                if selected_path_str:
                    parsed_path = parse_path_string(selected_path_str)
                    parsed_path = remove_after_last_dot_if_bracket(parsed_path)
                    get_nested_value(org_obj_def, parsed_path)
                    if comp_obj_def:
                        get_nested_value(comp_obj_def, parsed_path)
                    st.rerun()

    @staticmethod
    def render_differences_navigator(org_obj_def, comp_obj_def):
        """Render differences navigation panel"""
        st.write("**🔍 Differences**")

        if 'selected_diff_paths' in st.session_state and st.session_state.selected_diff_paths:
            path_categories = {}
            for path in st.session_state.selected_diff_paths:
                top_key = path.split('.')[0].split('[')[0]
                if top_key not in path_categories:
                    path_categories[top_key] = []
                path_categories[top_key].append(path)
        else:
            path_categories = {}

        navigator = ThreeLevelNavigator('diff', path_categories, org_obj_def, comp_obj_def)
        selected_path_str = navigator.render()

        if st.button("📍 Go", key="diff_go", use_container_width=True):
            if selected_path_str:
                parsed_path = parse_path_string(selected_path_str)
                test_1 = get_nested_value(org_obj_def, parsed_path)
                test_2 = get_nested_value(comp_obj_def, parsed_path) if comp_obj_def else None

                if test_1 is not None and not isinstance(test_1, (dict, list)):
                    parsed_path = parsed_path[:-1]
                elif test_2 is not None and not isinstance(test_2, (dict, list)):
                    parsed_path = parsed_path[:-1]

                st.session_state.path = parsed_path
                st.rerun()

    @staticmethod
    def render_navigation(data, current_path, auto_navigate=True):
        """Render JSON view with navigation

        Args:
            data: The JSON data to navigate
            current_path: Current navigation path
            auto_navigate: Whether to auto-navigate into single-key dicts (default True)
        """
        # Get the value at the path
        current_data = get_nested_value(data, current_path)

        if current_data is None:
            st.error(f"❌ Path not found! Path was: {current_path}")
            return

        # Show current path
        path_str = " > ".join([str(p) for p in current_path]) if current_path else "Root"
        st.caption(f"Current path: {path_str}")

        # If at root and data is a dict with single key, navigate into it automatically
        if auto_navigate and not current_path and isinstance(current_data, dict) and len(current_data) == 1:
            first_key = list(current_data.keys())[0]
            st.info(f"Auto-navigating into '{first_key}'")
            current_data = current_data[first_key]

        # Display JSON with synchronized expand state
        # Using expand_depth from session state for synchronized depth control
        st.json(current_data, expanded=st.session_state.expand_depth)


class ComparisonViewRenderer:
    """Handles rendering of comparison view components"""

    @staticmethod
    def render_object_info_tables():
        """Render object information tables if available"""
        if not st.session_state.found_differences or len(st.session_state.found_differences) == 0:
            return

        # Find the currently loaded object based on selected_redis_key
        current_item = None
        selected_redis_key = st.session_state.get('selected_redis_key', '')

        if selected_redis_key:
            # Try to find matching object by looking at org_obj_key
            for item in st.session_state.found_differences:
                obj_key = item.get('org_obj_key', '')
                # Extract just the key part after the prefix (e.g., "mstr_dev:ATTRIBUTE:ABC" -> "ATTRIBUTE:ABC")
                if ':' in obj_key:
                    key_part = obj_key.split(':', 1)[1]  # Get everything after first colon
                else:
                    key_part = obj_key

                if key_part == selected_redis_key or obj_key == selected_redis_key:
                    current_item = item
                    break

        # If no match found or no key selected, use first item as fallback
        if current_item is None and st.session_state.found_differences:
            current_item = st.session_state.found_differences[0]

        if current_item:
            org_obj_info = current_item.get('org_obj_info', None)
            comp_obj_info = current_item.get('comp_obj_info', None)

            if org_obj_info and comp_obj_info:
                with st.expander("📋 Object Information", expanded=False):
                    col1, col2 = st.columns(2)

                    with col1:
                        st.subheader("🔷 Object 1 (Original)")
                        df_org = pd.DataFrame([org_obj_info]).T
                        df_org.columns = ['Value']
                        df_org.index.name = 'Property'
                        st.dataframe(df_org, use_container_width=True)

                    with col2:
                        st.subheader("🔶 Object 2 (Comparison)")
                        df_comp = pd.DataFrame([comp_obj_info]).T
                        df_comp.columns = ['Value']
                        df_comp.index.name = 'Property'
                        st.dataframe(df_comp, use_container_width=True)

    @staticmethod
    def render_different_values_view(different_value_paths, org_obj_def, comp_obj_def):
        """Render the different values expander view"""
        with st.expander(f"📌 Different Values ({len(different_value_paths)} paths)", expanded=False):
            if different_value_paths:
                filtered_obj1 = JSONFilterUtils.filter_json_by_paths(org_obj_def, different_value_paths, [])
                filtered_obj2 = JSONFilterUtils.filter_json_by_paths(comp_obj_def, different_value_paths, [])

                if st.session_state.path:
                    filtered_obj1 = get_nested_value(filtered_obj1, st.session_state.path) if filtered_obj1 else None
                    filtered_obj2 = get_nested_value(filtered_obj2, st.session_state.path) if filtered_obj2 else None

                if filtered_obj1 or filtered_obj2:
                    col1, col2 = st.columns(2)
                    with col1:
                        st.subheader("📂 Object 1")
                        NavigationRenderer.render_navigation(filtered_obj1 if filtered_obj1 else {}, [], auto_navigate=False)
                    with col2:
                        st.subheader("📂 Object 2")
                        NavigationRenderer.render_navigation(filtered_obj2 if filtered_obj2 else {}, [], auto_navigate=False)
                else:
                    st.info("ℹ️ No different values at current location")
            else:
                st.info("ℹ️ No paths found with different values")

    @staticmethod
    def render_side_by_side_comparison(org_obj_def, comp_obj_def):
        """Render side-by-side comparison of two objects"""
        if comp_obj_def:
            with st.expander("📊 Side-by-Side Comparison", expanded=True):
                col1, col2 = st.columns(2)

                with col1:
                    st.subheader("📂 Object 1")
                    NavigationRenderer.render_navigation(org_obj_def, st.session_state.path)

                with col2:
                    st.subheader("📂 Object 2")
                    NavigationRenderer.render_navigation(comp_obj_def, st.session_state.path)
        else:
            with st.expander("📊 Object View", expanded=True):
                st.subheader("📂 Object 1")
                NavigationRenderer.render_navigation(org_obj_def, st.session_state.path)

st.set_page_config(page_title="MSTR JSON Analyzer", layout="wide")
st.title("MSTR Object Definition Analyzer")

# Initialize session state for navigation
if 'path' not in st.session_state:
    st.session_state.path = []
if 'expand_depth' not in st.session_state:
    st.session_state.expand_depth = 2
if 'found_differences' not in st.session_state:
    # Initialize as empty - will be populated when comparison runs
    st.session_state.found_differences = []

# Initialize session state for auto-load functionality
if 'auto_load_comparison' not in st.session_state:
    st.session_state.auto_load_comparison = False
if 'selected_redis_key' not in st.session_state:
    st.session_state.selected_redis_key = ''
if 'selected_prefix_1' not in st.session_state:
    st.session_state.selected_prefix_1 = ''
if 'selected_diff_paths' not in st.session_state:
    st.session_state.selected_diff_paths = []
if 'selected_diff_types' not in st.session_state:
    st.session_state.selected_diff_types = []

# Cached wrapper for Redis config loading
@st.cache_resource
def load_redis_config():
    """Cached wrapper for RedisManager.load_redis_config()"""
    return RedisManager.load_redis_config()

# Convenience aliases for backward compatibility
get_nested_value = JSONPathHelper.get_nested_value
parse_path_string = JSONPathHelper.parse_path_string
extract_all_paths = JSONPathHelper.extract_all_paths

# Fetch from Redis
org_obj_def = None
comp_obj_def = None

# Upload Redis Configuration
redis_config = DifferencesRenderer.render_redis_config_uploader()

# If no uploaded config, try to load default from file system
if redis_config is None:
    redis_config = load_redis_config()
    if redis_config:
        st.info("ℹ️ Using default Redis configuration from file system. Upload a custom config to override.")

if redis_config is not None:
    # Select Redis environment
    selected_env, i_redis = DifferencesRenderer.render_redis_environment_selector(redis_config)

    if i_redis:
        # Run Comparison Section
        uploaded_file, username, password, base_url = DifferencesRenderer.render_comparison_section()
        DifferencesRenderer.handle_comparison_run_button(
            uploaded_file, username, password, base_url, redis_config, selected_env
        )

        # Found Differences Section
        DifferencesRenderer.render_found_differences_table()

        st.divider()

        # Check for auto-load conditions BEFORE rendering the expander
        auto_load = st.session_state.get('auto_load_comparison', False)
        if 'temp_load_requested' in st.session_state and st.session_state.temp_load_requested:
            auto_load = True
            st.session_state.temp_load_requested = False
        if 'auto_reload_requested' in st.session_state and st.session_state.auto_reload_requested:
            auto_load = True
            st.session_state.auto_reload_requested = False

        # Collapsible Compare and Load Object section
        with st.expander("🔧 Compare & Load Settings", expanded=False):
            st.subheader("Compare")
            prefix_1, prefix_2 = ObjectLoaderRenderer.get_prefix_inputs()

            st.subheader("Load Object")
            redis_key = ObjectLoaderRenderer.get_redis_key_input()

            ObjectLoaderRenderer.render_loaded_status(prefix_1, prefix_2)

            st.divider()

            compare_clicked = st.button("🔍 Compare Objects", type="primary", use_container_width=True)

        # Handle button click or auto-load outside expander
        if compare_clicked or auto_load:
            with st.spinner("Fetching objects from Redis..."):
                try:
                    obj_1, obj_2 = RedisManager.fetch_objects_from_redis(i_redis, prefix_1, prefix_2, redis_key)

                    # Success message
                    if obj_1 and obj_2:
                        st.success(f"✅ Both objects fetched successfully!")
                        if 'temp_diff_paths' in st.session_state:
                            st.session_state.selected_diff_paths = st.session_state.temp_diff_paths
                            del st.session_state.temp_diff_paths
                        st.rerun()
                    elif obj_1 and not prefix_2:
                        st.success(f"✅ Object 1 fetched successfully!")
                        if 'temp_diff_paths' in st.session_state:
                            st.session_state.selected_diff_paths = st.session_state.temp_diff_paths
                            del st.session_state.temp_diff_paths
                        st.rerun()
                    elif obj_1:
                        st.warning("⚠️ Only Object 1 was fetched")
                    elif obj_2:
                        st.warning("⚠️ Only Object 2 was fetched")

                except Exception as e:
                    st.error(f"Error fetching objects: {e}")

# Load objects from session state
if 'org_obj_def' in st.session_state:
    org_obj_def = st.session_state.org_obj_def
if 'comp_obj_def' in st.session_state:
    comp_obj_def = st.session_state.comp_obj_def

if org_obj_def:
    # Quick path navigation
    st.divider()
    with st.expander("🎯 Quick Path Navigation", expanded=True):
        col_structure, col_differences = st.columns(2)

        if 'structure_depth' not in st.session_state:
            st.session_state.structure_depth = 3
        structure_depth = st.session_state.structure_depth

        with col_structure:
            NavigationRenderer.render_structure_navigator(org_obj_def, comp_obj_def, structure_depth)

        with col_differences:
            NavigationRenderer.render_differences_navigator(org_obj_def, comp_obj_def)

        st.divider()
        UIComponents.render_manual_path_input(org_obj_def, comp_obj_def if comp_obj_def else None)
        structure_depth = UIComponents.render_structure_depth_control()

    st.divider()
    UIComponents.render_expand_depth_control()

    # ==================== FILTERED DIFFERENCE VIEWS ====================
    if comp_obj_def and 'selected_diff_types' in st.session_state and st.session_state.selected_diff_types:
        st.divider()
        st.header("📊 Compare JSON - Files")

        ComparisonViewRenderer.render_object_info_tables()

        diff_paths = st.session_state.selected_diff_paths
        diff_types = st.session_state.selected_diff_types

        different_value_paths = JSONFilterUtils.extract_different_value_paths(diff_paths, diff_types, org_obj_def, comp_obj_def)
        ComparisonViewRenderer.render_different_values_view(different_value_paths, org_obj_def, comp_obj_def)

    # Side-by-side comparison
    ComparisonViewRenderer.render_side_by_side_comparison(org_obj_def, comp_obj_def if comp_obj_def else None)
