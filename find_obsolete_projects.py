#!/usr/bin/env python3
# Copyright 2026 Concret.io
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
Find Obsolete GCP Projects

This script analyzes all GCP projects to identify which ones are obsolete based on:
- Last activity/usage
- Resources present in the project
- Project age and activity patterns
"""

import sys
import json
import subprocess
import time
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading


class ProjectAnalyzer:
    """Analyze GCP projects to identify obsolete ones."""

    # Output files
    REPORT_FILE = Path(__file__).parent / 'obsolete_projects_report.json'
    DELETION_FILE = Path(__file__).parent / 'projects_for_deletion.json'

    def __init__(self, verbose: bool = True, timeout: int = 30, skip_on_timeout: bool = True,
                 skip_compute: bool = False, skip_storage: bool = False,
                 skip_sql: bool = False, skip_other: bool = False, workers: int = 10):
        self.verbose = verbose
        self.start_time = time.time()
        self.timeout = timeout  # Timeout in seconds for each gcloud command
        self.skip_on_timeout = skip_on_timeout
        self.skip_compute = skip_compute
        self.skip_storage = skip_storage
        self.skip_sql = skip_sql
        self.skip_other = skip_other
        self.workers = workers
        self._log_lock = threading.Lock()  # Thread-safe logging
        self._progress_lock = threading.Lock()  # Thread-safe progress tracking
        self._save_lock = threading.Lock()  # Thread-safe file saving
        self._completed_count = 0
        self._total_count = 0
        # In-memory analyses keyed by project_id
        self._analyses = {}  # project_id -> analysis

    def _log(self, message: str, level: str = "INFO"):
        """Log a message with timestamp (thread-safe)."""
        timestamp = datetime.now().strftime("%H:%M:%S")
        with self._log_lock:
            print(f"[{timestamp}] {level}: {message}", flush=True)

    def _log_progress(self, current: int, total: int, item: str = "item"):
        """Log progress indicator."""
        percent = (current / total * 100) if total > 0 else 0
        bar_length = 30
        filled = int(bar_length * current / total) if total > 0 else 0
        bar = '█' * filled + '░' * (bar_length - filled)
        self._log(f"Progress: [{bar}] {current}/{total} ({percent:.1f}%) - {item}", "PROGRESS")

    def _save_files(self, in_progress: bool = True):
        """Save both report and deletion files (thread-safe). Called after every project."""
        with self._save_lock:
            analyses = list(self._analyses.values())
            obsolete = [a for a in analyses if a.get('is_obsolete')]
            potentially_obsolete = [a for a in analyses if not a.get('is_obsolete') and a.get('obsolete_reasons')]
            active = [a for a in analyses if not a.get('is_obsolete') and not a.get('obsolete_reasons')]

            # 1. Save report file (full categorized report)
            report = {
                'metadata': {
                    'generated_at': datetime.now().isoformat(),
                    'total_analyzed': len(analyses),
                    'in_progress': in_progress
                },
                'summary': {
                    'obsolete': len(obsolete),
                    'potentially_obsolete': len(potentially_obsolete),
                    'active': len(active)
                },
                'obsolete': obsolete,
                'potentially_obsolete': potentially_obsolete,
                'active': active
            }

            try:
                with open(self.REPORT_FILE, 'w') as f:
                    json.dump(report, f, indent=2, default=str)
            except IOError as e:
                self._log(f"Warning: Could not save report: {e}", "WARN")

            # 2. Save deletion file (deletion-ready format)
            deletion_data = {
                'metadata': {
                    'generated_at': datetime.now().isoformat(),
                    'generated_by': 'find_obsolete_projects.py',
                    'version': '1.0',
                    'in_progress': in_progress
                },
                'summary': {
                    'total_safe_to_delete': len(obsolete),
                    'total_need_review': len(potentially_obsolete),
                    'total_candidates': len(obsolete) + len(potentially_obsolete)
                },
                'projects_to_delete': [],
                'projects_to_review': []
            }

            # Add obsolete projects (safe to delete)
            for project in obsolete:
                counts = project.get('resource_counts', {})
                deletion_data['projects_to_delete'].append({
                    'project_id': project['project_id'],
                    'project_name': project['project_name'],
                    'project_number': project['project_number'],
                    'lifecycle_state': project['lifecycle_state'],
                    'total_resources': project['total_resources'],
                    'last_activity': project['last_activity'],
                    'days_since_activity': project['days_since_activity'],
                    'obsolete_reasons': project['obsolete_reasons'],
                    'deletion_status': 'safe_to_delete',
                    'resource_counts': counts
                })

            # Add potentially obsolete projects (need review)
            for project in potentially_obsolete:
                counts = project.get('resource_counts', {})
                deletion_data['projects_to_review'].append({
                    'project_id': project['project_id'],
                    'project_name': project['project_name'],
                    'project_number': project['project_number'],
                    'lifecycle_state': project['lifecycle_state'],
                    'total_resources': project['total_resources'],
                    'last_activity': project['last_activity'],
                    'days_since_activity': project['days_since_activity'],
                    'obsolete_reasons': project['obsolete_reasons'],
                    'deletion_status': 'review_required',
                    'resource_counts': counts
                })

            try:
                with open(self.DELETION_FILE, 'w') as f:
                    json.dump(deletion_data, f, indent=2, default=str)
            except IOError as e:
                self._log(f"Warning: Could not save deletion file: {e}", "WARN")

    def load_progress(self) -> Dict[str, Any]:
        """Load existing report file for resume capability."""
        self._analyses = {}
        if self.REPORT_FILE.exists():
            try:
                with open(self.REPORT_FILE, 'r') as f:
                    data = json.load(f)
                # Combine all categories back into analyses dict
                for category in ['obsolete', 'potentially_obsolete', 'active']:
                    for analysis in data.get(category, []):
                        project_id = analysis.get('project_id')
                        if project_id:
                            self._analyses[project_id] = analysis
                analyzed_count = len(self._analyses)
                if analyzed_count > 0:
                    self._log(f"Loaded progress: {analyzed_count} projects already analyzed", "SUCCESS")
            except (IOError, json.JSONDecodeError) as e:
                self._log(f"Could not load progress: {e}", "WARN")
        return self._analyses

    def add_analysis(self, project_id: str, analysis: Dict[str, Any]):
        """Add an analysis and save both files immediately."""
        with self._progress_lock:
            self._analyses[project_id] = analysis
        # Save both files after every project
        self._save_files(in_progress=True)

    def get_pending_projects(self, all_projects: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Return projects that haven't been analyzed yet."""
        analyzed_ids = set(self._analyses.keys())
        pending = [p for p in all_projects if p.get('projectId') not in analyzed_ids]
        skipped = len(all_projects) - len(pending)
        if skipped > 0:
            self._log(f"Skipping {skipped} already-analyzed projects", "INFO")
        return pending

    def get_all_analyses(self) -> List[Dict[str, Any]]:
        """Get all analyses."""
        return list(self._analyses.values())

    def clear_progress(self):
        """Delete output files to start fresh."""
        if self.REPORT_FILE.exists():
            self.REPORT_FILE.unlink()
        if self.DELETION_FILE.exists():
            self.DELETION_FILE.unlink()
        self._analyses = {}
        self._log("Output files cleared", "INFO")

    def save_final_report(self):
        """Save the final report (marks as complete)."""
        self._save_files(in_progress=False)
        self._log(f"Final files saved:", "SUCCESS")
        self._log(f"  - {self.REPORT_FILE}", "INFO")
        self._log(f"  - {self.DELETION_FILE}", "INFO")

    def _run_gcloud(self, args: List[str], format_json: bool = True, timeout: Optional[int] = None) -> Dict[str, Any]:
        """Run a gcloud command with timeout."""
        command = ['gcloud'] + args
        if format_json and '--format' not in ' '.join(args):
            command.extend(['--format', 'json'])
        
        if timeout is None:
            timeout = self.timeout
        
        if self.verbose:
            cmd_str = ' '.join(command[:5]) + ('...' if len(command) > 5 else '')
            self._log(f"Executing: {cmd_str} (timeout: {timeout}s)", "CMD")
        
        try:
            start = time.time()
            result = subprocess.run(
                command,
                capture_output=True,
                text=True,
                check=False,
                timeout=timeout
            )
            elapsed = time.time() - start
            
            if self.verbose:
                if result.returncode == 0:
                    data_size = len(result.stdout) if result.stdout else 0
                    self._log(f"✓ Completed in {elapsed:.2f}s (output: {data_size} bytes)", "SUCCESS")
                else:
                    self._log(f"✗ Failed in {elapsed:.2f}s (exit code: {result.returncode})", "ERROR")
                    if result.stderr:
                        error_preview = result.stderr[:150].replace('\n', ' ')
                        self._log(f"  Error preview: {error_preview}...", "ERROR")
            
            if result.returncode == 0 and format_json:
                try:
                    data = json.loads(result.stdout)
                    if self.verbose and isinstance(data, list):
                        self._log(f"  Retrieved {len(data)} item(s)", "INFO")
                    return {'success': True, 'data': data}
                except json.JSONDecodeError:
                    if self.verbose:
                        self._log("  Warning: Could not parse JSON response", "WARN")
                    return {'success': True, 'data': []}
            return {'success': result.returncode == 0, 'data': result.stdout}
        except subprocess.TimeoutExpired:
            elapsed = time.time() - start
            if self.verbose:
                self._log(f"✗ Timeout after {elapsed:.2f}s (limit: {timeout}s)", "ERROR")
                self._log(f"  Command took too long, skipping...", "WARN")
            return {'success': False, 'data': [], 'error': f'Timeout after {timeout}s', 'timeout': True}
        except Exception as e:
            if self.verbose:
                self._log(f"✗ Exception: {str(e)}", "ERROR")
            return {'success': False, 'data': [], 'error': str(e)}
    
    def get_all_projects(self) -> List[Dict[str, Any]]:
        """Get all accessible projects."""
        self._log("Fetching list of all accessible projects...", "INFO")
        result = self._run_gcloud(['projects', 'list'])
        if result['success']:
            projects = result['data'] if isinstance(result['data'], list) else []
            self._log(f"Found {len(projects)} project(s) accessible", "SUCCESS")
            return projects
        self._log("Failed to fetch projects list", "ERROR")
        return []
    
    def get_project_info(self, project_id: str) -> Dict[str, Any]:
        """Get detailed information about a project."""
        if self.verbose:
            self._log(f"Getting project details for: {project_id}", "INFO")
        result = self._run_gcloud(['projects', 'describe', project_id])
        if result['success']:
            return result['data']
        if self.verbose:
            self._log(f"Failed to get project info for {project_id}", "WARN")
        return {}
    
    def check_all_resources_asset_inventory(self, project_id: str) -> Dict[str, Any]:
        """Check all resources using Cloud Asset Inventory API (much faster!)."""
        if self.verbose:
            self._log("  Checking all resources via Asset Inventory API...", "INFO")
        
        resources = {
            'instances': [],
            'disks': [],
            'snapshots': [],
            'images': [],
            'buckets': [],
            'sql_instances': [],
            'app_engines': [],
            'cloud_functions': [],
            'other': [],
            'by_type': {},
            'total_count': 0
        }
        
        try:
            # Use Asset Inventory API to get all resources in one call
            result = self._run_gcloud([
                'asset', 'search-all-resources',
                '--scope', f'projects/{project_id}',
                '--format', 'json'
            ], timeout=self.timeout * 2)  # Give it more time as it's doing more work
            
            if result.get('timeout'):
                if self.skip_on_timeout:
                    self._log("  ⚠️  Asset Inventory API timed out, skipping", "WARN")
                    return resources
                else:
                    self._log("  ✗ Asset Inventory API timed out", "ERROR")
                    return resources
            
            if not result['success']:
                error_msg = result.get('error', 'Unknown error')
                self._log(f"  ✗ Asset Inventory API failed: {error_msg}", "ERROR")
                self._log("  → Falling back to individual service checks...", "WARN")
                return self._fallback_to_individual_checks(project_id)
            
            # Parse the asset inventory results
            assets = result['data'] if isinstance(result['data'], list) else []
            
            if self.verbose:
                self._log(f"  ✓ Retrieved {len(assets)} asset(s) from Asset Inventory API", "SUCCESS")
            
            # Map asset types to our resource categories
            asset_type_mapping = {
                'compute.googleapis.com/Instance': 'instances',
                'compute.googleapis.com/Disk': 'disks',
                'compute.googleapis.com/Snapshot': 'snapshots',
                'compute.googleapis.com/Image': 'images',
                'storage.googleapis.com/Bucket': 'buckets',
                'sqladmin.googleapis.com/Instance': 'sql_instances',
                'appengine.googleapis.com/Application': 'app_engines',
                'appengine.googleapis.com/Version': 'app_engines',
                'cloudfunctions.googleapis.com/CloudFunction': 'cloud_functions',
            }
            
            # Group assets by type
            for asset in assets:
                asset_type = asset.get('assetType', '')
                resources['by_type'][asset_type] = resources['by_type'].get(asset_type, [])
                resources['by_type'][asset_type].append(asset)
                
                # Map to our categories
                category = asset_type_mapping.get(asset_type, 'other')
                if category in resources:
                    resources[category].append(asset)
                else:
                    resources['other'].append(asset)
            
            # Calculate totals
            resources['total_count'] = (
                len(resources['instances']) +
                len(resources['disks']) +
                len(resources['snapshots']) +
                len(resources['images']) +
                len(resources['buckets']) +
                len(resources['sql_instances']) +
                len(resources['app_engines']) +
                len(resources['cloud_functions']) +
                len(resources['other'])
            )
            
            if self.verbose:
                self._log(f"  ✓ Resource breakdown:", "SUCCESS")
                self._log(f"    • Compute: {len(resources['instances'])} instances, "
                      f"{len(resources['disks'])} disks, {len(resources['snapshots'])} snapshots, "
                      f"{len(resources['images'])} images", "INFO")
                self._log(f"    • Storage: {len(resources['buckets'])} buckets", "INFO")
                self._log(f"    • SQL: {len(resources['sql_instances'])} instances", "INFO")
                self._log(f"    • App Engine: {len(resources['app_engines'])}", "INFO")
                self._log(f"    • Cloud Functions: {len(resources['cloud_functions'])}", "INFO")
                self._log(f"    • Other: {len(resources['other'])}", "INFO")
                self._log(f"    • Total: {resources['total_count']} resources", "INFO")
            
        except Exception as e:
            self._log(f"  ✗ Error using Asset Inventory API: {e}", "ERROR")
            self._log("  → Falling back to individual service checks...", "WARN")
            return self._fallback_to_individual_checks(project_id)
        
        return resources
    
    def _fallback_to_individual_checks(self, project_id: str) -> Dict[str, Any]:
        """Fallback to individual service checks if Asset Inventory API fails."""
        self._log("  Using fallback: individual service checks", "WARN")
        resources = {
            'instances': [],
            'disks': [],
            'snapshots': [],
            'images': [],
            'buckets': [],
            'sql_instances': [],
            'app_engines': [],
            'cloud_functions': [],
            'other': [],
            'by_type': {},
            'total_count': 0
        }
        
        # Use existing individual check methods
        if not self.skip_compute:
            compute = self.check_compute_resources(project_id)
            resources['instances'] = compute.get('instances', [])
            resources['disks'] = compute.get('disks', [])
            resources['snapshots'] = compute.get('snapshots', [])
            resources['images'] = compute.get('images', [])
        
        if not self.skip_storage:
            storage = self.check_storage_resources(project_id)
            resources['buckets'] = storage.get('buckets', [])
        
        if not self.skip_sql:
            sql = self.check_sql_resources(project_id)
            resources['sql_instances'] = sql.get('instances', [])
        
        if not self.skip_other:
            other = self.check_other_resources(project_id)
            resources['app_engines'] = other.get('app_engines', [])
            resources['cloud_functions'] = other.get('cloud_functions', [])
        
        resources['total_count'] = (
            len(resources['instances']) + len(resources['disks']) +
            len(resources['snapshots']) + len(resources['images']) +
            len(resources['buckets']) + len(resources['sql_instances']) +
            len(resources['app_engines']) + len(resources['cloud_functions']) +
            len(resources['other'])
        )
        
        return resources
    
    def check_compute_resources(self, project_id: str) -> Dict[str, Any]:
        """Check compute resources in a project."""
        if self.verbose:
            self._log("  Checking compute resources...", "INFO")
        resources = {
            'instances': [],
            'disks': [],
            'snapshots': [],
            'images': [],
            'load_balancers': [],
            'total_count': 0
        }
        
        try:
            # List compute instances
            if self.verbose:
                self._log("    → Checking VM instances...", "INFO")
            result = self._run_gcloud([
                'compute', 'instances', 'list',
                '--project', project_id
            ])
            if result.get('timeout'):
                if self.skip_on_timeout:
                    self._log("    ⚠️  Skipping compute resources due to timeout", "WARN")
                    return resources
            if result['success']:
                resources['instances'] = result['data'] if isinstance(result['data'], list) else []
                if self.verbose:
                    self._log(f"    ✓ Found {len(resources['instances'])} instance(s)", "SUCCESS")
            
            # List disks
            if self.verbose:
                self._log("    → Checking disks...", "INFO")
            result = self._run_gcloud([
                'compute', 'disks', 'list',
                '--project', project_id
            ])
            if result.get('timeout'):
                if self.skip_on_timeout:
                    self._log("    ⚠️  Skipping remaining compute checks due to timeout", "WARN")
                    resources['total_count'] = len(resources['instances'])
                    return resources
            if result['success']:
                resources['disks'] = result['data'] if isinstance(result['data'], list) else []
                if self.verbose:
                    self._log(f"    ✓ Found {len(resources['disks'])} disk(s)", "SUCCESS")
            
            # List snapshots
            if self.verbose:
                self._log("    → Checking snapshots...", "INFO")
            result = self._run_gcloud([
                'compute', 'snapshots', 'list',
                '--project', project_id
            ])
            if result.get('timeout'):
                if self.skip_on_timeout:
                    self._log("    ⚠️  Skipping remaining compute checks due to timeout", "WARN")
                    resources['total_count'] = (
                        len(resources['instances']) +
                        len(resources['disks'])
                    )
                    return resources
            if result['success']:
                resources['snapshots'] = result['data'] if isinstance(result['data'], list) else []
                if self.verbose:
                    self._log(f"    ✓ Found {len(resources['snapshots'])} snapshot(s)", "SUCCESS")
            
            # List images
            if self.verbose:
                self._log("    → Checking images...", "INFO")
            result = self._run_gcloud([
                'compute', 'images', 'list',
                '--project', project_id
            ])
            if result.get('timeout'):
                if self.skip_on_timeout:
                    self._log("    ⚠️  Skipping remaining compute checks due to timeout", "WARN")
                    resources['total_count'] = (
                        len(resources['instances']) +
                        len(resources['disks']) +
                        len(resources['snapshots'])
                    )
                    return resources
            if result['success']:
                resources['images'] = result['data'] if isinstance(result['data'], list) else []
                if self.verbose:
                    self._log(f"    ✓ Found {len(resources['images'])} image(s)", "SUCCESS")
            
            resources['total_count'] = (
                len(resources['instances']) +
                len(resources['disks']) +
                len(resources['snapshots']) +
                len(resources['images'])
            )
            
            if self.verbose:
                self._log(f"  ✓ Compute resources check complete: {resources['total_count']} total", "SUCCESS")
            
        except Exception as e:
            self._log(f"  ✗ Error checking compute resources: {e}", "ERROR")
        
        return resources
    
    def check_storage_resources(self, project_id: str) -> Dict[str, Any]:
        """Check storage resources in a project."""
        if self.verbose:
            self._log("  Checking storage resources...", "INFO")
        resources = {
            'buckets': [],
            'total_count': 0
        }
        
        try:
            result = self._run_gcloud([
                'storage', 'buckets', 'list',
                '--project', project_id
            ])
            if result['success']:
                resources['buckets'] = result['data'] if isinstance(result['data'], list) else []
                resources['total_count'] = len(resources['buckets'])
                if self.verbose:
                    self._log(f"  ✓ Found {resources['total_count']} storage bucket(s)", "SUCCESS")
        except Exception as e:
            self._log(f"  ✗ Error checking storage resources: {e}", "ERROR")
        
        return resources
    
    def check_sql_resources(self, project_id: str) -> Dict[str, Any]:
        """Check SQL resources in a project."""
        if self.verbose:
            self._log("  Checking SQL resources...", "INFO")
        resources = {
            'instances': [],
            'total_count': 0
        }
        
        try:
            result = self._run_gcloud([
                'sql', 'instances', 'list',
                '--project', project_id
            ])
            if result['success']:
                resources['instances'] = result['data'] if isinstance(result['data'], list) else []
                resources['total_count'] = len(resources['instances'])
                if self.verbose:
                    self._log(f"  ✓ Found {resources['total_count']} SQL instance(s)", "SUCCESS")
        except Exception as e:
            self._log(f"  ✗ Error checking SQL resources: {e}", "ERROR")
        
        return resources
    
    def check_other_resources(self, project_id: str) -> Dict[str, Any]:
        """Check other common resources."""
        if self.verbose:
            self._log("  Checking other resources (App Engine, Cloud Functions)...", "INFO")
        resources = {
            'app_engines': [],
            'cloud_functions': [],
            'total_count': 0
        }
        
        try:
            # Check App Engine
            if self.verbose:
                self._log("    → Checking App Engine...", "INFO")
            result = self._run_gcloud([
                'app', 'instances', 'list',
                '--project', project_id
            ])
            if result['success']:
                resources['app_engines'] = result['data'] if isinstance(result['data'], list) else []
                if self.verbose:
                    self._log(f"    ✓ Found {len(resources['app_engines'])} App Engine instance(s)", "SUCCESS")
        except Exception as e:
            if self.verbose:
                self._log("    → App Engine not enabled or accessible", "WARN")
        
        try:
            # Check Cloud Functions
            if self.verbose:
                self._log("    → Checking Cloud Functions...", "INFO")
            result = self._run_gcloud([
                'functions', 'list',
                '--project', project_id
            ])
            if result['success']:
                resources['cloud_functions'] = result['data'] if isinstance(result['data'], list) else []
                if self.verbose:
                    self._log(f"    ✓ Found {len(resources['cloud_functions'])} Cloud Function(s)", "SUCCESS")
        except Exception as e:
            if self.verbose:
                self._log("    → Cloud Functions not enabled or accessible", "WARN")
        
        resources['total_count'] = len(resources['app_engines']) + len(resources['cloud_functions'])
        if self.verbose:
            self._log(f"  ✓ Other resources check complete: {resources['total_count']} total", "SUCCESS")
        return resources
    
    def get_resource_creation_dates(self, resources: Dict[str, Any]) -> List[datetime]:
        """Extract creation dates from resources to determine last activity."""
        dates = []
        
        # Handle Asset Inventory API format (has updateTime and createTime in asset)
        # Also handle individual service API format (has creationTimestamp, timeCreated, etc.)
        
        # Check all resource types
        resource_types = ['instances', 'disks', 'snapshots', 'images', 'buckets', 
                         'sql_instances', 'app_engines', 'cloud_functions', 'other']
        
        for resource_type in resource_types:
            for resource in resources.get(resource_type, []):
                # Asset Inventory API format: resource has 'updateTime' and 'createTime'
                if 'updateTime' in resource:
                    try:
                        date_str = resource['updateTime']
                        dt = datetime.fromisoformat(date_str.replace('Z', '+00:00'))
                        dates.append(dt.replace(tzinfo=None))
                        continue
                    except:
                        pass
                
                if 'createTime' in resource:
                    try:
                        date_str = resource['createTime']
                        dt = datetime.fromisoformat(date_str.replace('Z', '+00:00'))
                        dates.append(dt.replace(tzinfo=None))
                        continue
                    except:
                        pass
                
                # Individual service API format
                for date_field in ['creationTimestamp', 'timeCreated', 'createTime']:
                    if date_field in resource:
                        try:
                            date_str = resource[date_field]
                            # Parse ISO format date
                            if 'T' in date_str:
                                date_str = date_str.split('T')[0] + ' ' + date_str.split('T')[1].split('.')[0]
                            dt = datetime.fromisoformat(date_str.replace('Z', '+00:00'))
                            dates.append(dt.replace(tzinfo=None))
                            break
                        except:
                            continue
        
        return dates
    
    def analyze_project(self, project: Dict[str, Any]) -> Dict[str, Any]:
        """Analyze a single project for obsolescence."""
        project_id = project.get('projectId', '')
        project_name = project.get('name', 'N/A')

        # Try to get projectNumber and lifecycleState from the project dict first
        # (already available from 'gcloud projects list' output)
        # Only call 'projects describe' if the data is missing - this saves one API call per project
        project_number = project.get('projectNumber')
        lifecycle_state = project.get('lifecycleState')

        if not project_number or not lifecycle_state:
            # Fallback: fetch project details if not available in the list output
            self._log(f"Fetching project details for: {project_id}", "INFO")
            project_info = self.get_project_info(project_id)
            project_number = project_info.get('projectNumber', 'N/A')
            lifecycle_state = project_info.get('lifecycleState', 'UNKNOWN')
        else:
            project_number = str(project_number)  # Ensure it's a string

        self._log(f"📋 Analyzing: {project_name} ({project_id}) - State: {lifecycle_state}", "INFO")
        
        # Check resources using Asset Inventory API (much faster!)
        self._log("Starting resource inventory via Asset Inventory API...", "INFO")
        all_resources = self.check_all_resources_asset_inventory(project_id)
        
        # Apply skip filters if needed (for backward compatibility)
        if self.skip_compute:
            self._log("  ⏭️  Filtering out compute resources (--skip-compute)", "INFO")
            all_resources['instances'] = []
            all_resources['disks'] = []
            all_resources['snapshots'] = []
            all_resources['images'] = []
        
        if self.skip_storage:
            self._log("  ⏭️  Filtering out storage resources (--skip-storage)", "INFO")
            all_resources['buckets'] = []
        
        if self.skip_sql:
            self._log("  ⏭️  Filtering out SQL resources (--skip-sql)", "INFO")
            all_resources['sql_instances'] = []
        
        if self.skip_other:
            self._log("  ⏭️  Filtering out other resources (--skip-other)", "INFO")
            all_resources['app_engines'] = []
            all_resources['cloud_functions'] = []
        
        # Recalculate total after filtering
        all_resources['total_count'] = (
            len(all_resources['instances']) +
            len(all_resources['disks']) +
            len(all_resources['snapshots']) +
            len(all_resources['images']) +
            len(all_resources['buckets']) +
            len(all_resources['sql_instances']) +
            len(all_resources['app_engines']) +
            len(all_resources['cloud_functions']) +
            len(all_resources['other'])
        )
        
        total_resources = all_resources['total_count']
        
        self._log("Resource Summary:", "INFO")
        self._log(f"  • Compute: {len(all_resources['instances'])} instances, "
              f"{len(all_resources['disks'])} disks, {len(all_resources['snapshots'])} snapshots, "
              f"{len(all_resources['images'])} images", "INFO")
        self._log(f"  • Storage: {len(all_resources['buckets'])} buckets", "INFO")
        self._log(f"  • SQL: {len(all_resources['sql_instances'])} instances", "INFO")
        self._log(f"  • App Engine: {len(all_resources['app_engines'])}", "INFO")
        self._log(f"  • Cloud Functions: {len(all_resources['cloud_functions'])}", "INFO")
        self._log(f"  • Other: {len(all_resources['other'])}", "INFO")
        self._log(f"  • Total: {total_resources}", "INFO")
        
        # Get creation dates to determine last activity
        self._log("Analyzing resource creation dates for activity tracking...", "INFO")
        creation_dates = self.get_resource_creation_dates(all_resources)
        
        last_activity = None
        if creation_dates:
            last_activity = max(creation_dates)
            days_since_activity = (datetime.now() - last_activity).days
            self._log(f"Last Activity: {last_activity.strftime('%Y-%m-%d')} ({days_since_activity} days ago)", "INFO")
        else:
            self._log("Last Activity: No resources found", "INFO")
        
        # Determine if obsolete
        self._log("Evaluating obsolescence criteria...", "INFO")
        is_obsolete = False
        obsolete_reasons = []
        
        if total_resources == 0:
            is_obsolete = True
            obsolete_reasons.append("No resources found")
            self._log("  → Marked as obsolete: No resources found", "WARN")
        
        if last_activity:
            days_since = (datetime.now() - last_activity).days
            if days_since > 180:  # 6 months
                is_obsolete = True
                obsolete_reasons.append(f"No activity for {days_since} days")
                self._log(f"  → Marked as obsolete: No activity for {days_since} days (>180 days)", "WARN")
            elif days_since > 90:  # 3 months
                obsolete_reasons.append(f"Low activity (last used {days_since} days ago)")
                self._log(f"  → Potentially obsolete: Low activity ({days_since} days ago)", "WARN")
        
        if lifecycle_state != 'ACTIVE':
            is_obsolete = True
            obsolete_reasons.append(f"Project state: {lifecycle_state}")
            self._log(f"  → Marked as obsolete: Project state is {lifecycle_state}", "WARN")
        
        status = "OBSOLETE" if is_obsolete else ("POTENTIALLY OBSOLETE" if obsolete_reasons else "ACTIVE")
        self._log(f"✓ Analysis complete - Status: {status}", "SUCCESS")
        
        # Return compact format - only counts, not full resource lists
        return {
            'project_id': project_id,
            'project_name': project_name,
            'project_number': project_number,
            'lifecycle_state': lifecycle_state,
            'total_resources': total_resources,
            'resource_counts': {
                'instances': len(all_resources['instances']),
                'disks': len(all_resources['disks']),
                'snapshots': len(all_resources['snapshots']),
                'images': len(all_resources['images']),
                'buckets': len(all_resources['buckets']),
                'sql_instances': len(all_resources['sql_instances']),
                'app_engines': len(all_resources['app_engines']),
                'cloud_functions': len(all_resources['cloud_functions']),
                'other': len(all_resources['other'])
            },
            'last_activity': last_activity.isoformat() if last_activity else None,
            'days_since_activity': (datetime.now() - last_activity).days if last_activity else None,
            'is_obsolete': is_obsolete,
            'obsolete_reasons': obsolete_reasons
        }

    def _analyze_project_worker(self, project: Dict[str, Any], index: int) -> Optional[Dict[str, Any]]:
        """Worker function to analyze a single project (for parallel execution)."""
        project_id = project.get('projectId', 'unknown')
        try:
            analysis = self.analyze_project(project)
            # Save to report immediately after successful analysis
            self.add_analysis(project_id, analysis)
            with self._progress_lock:
                self._completed_count += 1
                self._log(f"[{self._completed_count}/{self._total_count}] ✓ Completed: {project_id}", "SUCCESS")
            return analysis
        except subprocess.TimeoutExpired:
            with self._progress_lock:
                self._completed_count += 1
                self._log(f"[{self._completed_count}/{self._total_count}] ✗ Timeout: {project_id}", "ERROR")
            if self.skip_on_timeout:
                return None
            raise
        except Exception as e:
            with self._progress_lock:
                self._completed_count += 1
                self._log(f"[{self._completed_count}/{self._total_count}] ✗ Error analyzing {project_id}: {e}", "ERROR")
            return None

    def analyze_projects_parallel(self, projects: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Analyze multiple projects in parallel using ThreadPoolExecutor."""
        self._total_count = len(projects)
        self._completed_count = 0
        analyses = []

        self._log(f"Starting parallel analysis with {self.workers} workers...", "INFO")
        self._log(f"Analyzing {self._total_count} projects...", "INFO")

        with ThreadPoolExecutor(max_workers=self.workers) as executor:
            # Submit all tasks
            future_to_project = {
                executor.submit(self._analyze_project_worker, project, i): project
                for i, project in enumerate(projects)
            }

            # Collect results as they complete
            for future in as_completed(future_to_project):
                project = future_to_project[future]
                try:
                    result = future.result()
                    if result is not None:
                        analyses.append(result)
                except Exception as e:
                    self._log(f"✗ Unexpected error for {project.get('projectId', 'unknown')}: {e}", "ERROR")

        self._log(f"✓ Parallel analysis complete: {len(analyses)}/{self._total_count} projects analyzed", "SUCCESS")
        return analyses

    def generate_report(self, analyses: List[Dict[str, Any]]):
        """Print a summary report of obsolete projects (files already saved incrementally)."""
        self._log("", "INFO")
        self._log("=" * 80, "INFO")
        self._log("📊 ANALYSIS SUMMARY", "INFO")
        self._log("=" * 80, "INFO")

        obsolete_projects = [a for a in analyses if a['is_obsolete']]
        potentially_obsolete = [a for a in analyses if not a['is_obsolete'] and a['obsolete_reasons']]
        active_projects = [a for a in analyses if not a['is_obsolete'] and not a['obsolete_reasons']]

        self._log("", "INFO")
        self._log(f"🔴 DEFINITELY OBSOLETE ({len(obsolete_projects)} projects):", "INFO")
        self._log("-" * 80, "INFO")
        if obsolete_projects:
            for analysis in obsolete_projects:
                self._log("", "INFO")
                self._log(f"  • {analysis['project_name']} ({analysis['project_id']})", "INFO")
                self._log(f"    State: {analysis['lifecycle_state']}", "INFO")
                self._log(f"    Resources: {analysis['total_resources']}", "INFO")
                if analysis['last_activity']:
                    last_date = analysis['last_activity'][:10] if analysis['last_activity'] else 'N/A'
                    self._log(f"    Last Activity: {last_date} "
                          f"({analysis['days_since_activity']} days ago)", "INFO")
                self._log(f"    Reasons: {', '.join(analysis['obsolete_reasons'])}", "INFO")
        else:
            self._log("  None found", "INFO")

        self._log("", "INFO")
        self._log(f"🟡 POTENTIALLY OBSOLETE ({len(potentially_obsolete)} projects):", "INFO")
        self._log("-" * 80, "INFO")
        if potentially_obsolete:
            for analysis in potentially_obsolete:
                self._log("", "INFO")
                self._log(f"  • {analysis['project_name']} ({analysis['project_id']})", "INFO")
                self._log(f"    Resources: {analysis['total_resources']}", "INFO")
                if analysis['last_activity']:
                    last_date = analysis['last_activity'][:10] if analysis['last_activity'] else 'N/A'
                    self._log(f"    Last Activity: {last_date} "
                          f"({analysis['days_since_activity']} days ago)", "INFO")
                self._log(f"    Concerns: {', '.join(analysis['obsolete_reasons'])}", "INFO")
        else:
            self._log("  None found", "INFO")

        self._log("", "INFO")
        self._log(f"🟢 ACTIVE PROJECTS ({len(active_projects)} projects):", "INFO")
        self._log("-" * 80, "INFO")
        if active_projects:
            for analysis in active_projects[:10]:  # Show first 10
                self._log(f"  • {analysis['project_name']} ({analysis['project_id']}) - "
                      f"{analysis['total_resources']} resources", "INFO")
        else:
            self._log("  None found", "INFO")

        return {
            'obsolete': obsolete_projects,
            'potentially_obsolete': potentially_obsolete,
            'active': active_projects
        }


def main():
    """Main function to analyze all projects."""
    import argparse
    
    parser = argparse.ArgumentParser(
        description='Find obsolete GCP projects',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Analyze all projects (resumes from previous run by default)
  python3 find_obsolete_projects.py

  # Start fresh (clears progress and re-analyzes everything)
  python3 find_obsolete_projects.py --fresh

  # Use more workers for faster processing (20 workers)
  python3 find_obsolete_projects.py --workers 20

  # Analyze first 10 projects only (for testing)
  python3 find_obsolete_projects.py --limit 10

  # Use sequential mode (slower, for debugging)
  python3 find_obsolete_projects.py --sequential

  # Enable Asset Inventory API first (if not already enabled):
  # gcloud services enable cloudasset.googleapis.com
        """
    )
    
    parser.add_argument(
        '--timeout',
        type=int,
        default=30,
        help='Timeout in seconds for each gcloud command (default: 30)'
    )
    parser.add_argument(
        '--no-skip-timeout',
        action='store_true',
        help='Fail instead of skipping projects when commands timeout'
    )
    parser.add_argument(
        '--limit',
        type=int,
        default=None,
        help='Limit number of projects to analyze (useful for testing)'
    )
    parser.add_argument(
        '--skip-compute',
        action='store_true',
        help='Skip checking compute resources (VM instances, disks, snapshots, images)'
    )
    parser.add_argument(
        '--skip-storage',
        action='store_true',
        help='Skip checking storage resources (Cloud Storage buckets)'
    )
    parser.add_argument(
        '--skip-sql',
        action='store_true',
        help='Skip checking SQL resources (Cloud SQL instances)'
    )
    parser.add_argument(
        '--skip-other',
        action='store_true',
        help='Skip checking other resources (App Engine, Cloud Functions)'
    )
    parser.add_argument(
        '--workers',
        type=int,
        default=10,
        help='Number of parallel workers (default: 10). Higher values = faster but more API load.'
    )
    parser.add_argument(
        '--sequential',
        action='store_true',
        help='Disable parallel processing (use sequential mode like before)'
    )
    parser.add_argument(
        '--fresh',
        action='store_true',
        help='Clear output files and start fresh analysis (default: resume)'
    )

    args = parser.parse_args()

    start_time = time.time()
    print("🔍 Google Cloud Project Obsolete Analysis")
    print("=" * 80)
    print(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Using: Cloud Asset Inventory API (fast!)")
    print(f"Timeout per command: {args.timeout}s")
    if args.sequential:
        print("Mode: Sequential (single-threaded)")
    else:
        print(f"Mode: Parallel ({args.workers} workers)")
    if args.fresh:
        print("Fresh: Starting from scratch (clearing existing progress)")
    else:
        print("Resume: Will skip already-analyzed projects (use --fresh to restart)")
    if args.limit:
        print(f"Limit: Analyzing first {args.limit} projects only")
    print("=" * 80)

    analyzer = ProjectAnalyzer(
        verbose=True,
        timeout=args.timeout,
        skip_on_timeout=not args.no_skip_timeout,
        skip_compute=args.skip_compute,
        skip_storage=args.skip_storage,
        skip_sql=args.skip_sql,
        skip_other=args.skip_other,
        workers=args.workers
    )

    # Handle progress: resume by default, --fresh to start over
    if args.fresh:
        analyzer.clear_progress()
    else:
        # Default: resume from existing progress (safe for long-running analyses)
        analyzer.load_progress()

    # Show what services are being checked
    skipped_services = []
    if args.skip_compute:
        skipped_services.append("Compute")
    if args.skip_storage:
        skipped_services.append("Storage")
    if args.skip_sql:
        skipped_services.append("SQL")
    if args.skip_other:
        skipped_services.append("Other (App Engine, Cloud Functions)")

    if skipped_services:
        print(f"Skipping services: {', '.join(skipped_services)}")
        print("=" * 80)

    # Get all projects
    analyzer._log("", "INFO")
    analyzer._log("📦 STEP 1: Fetching all projects...", "INFO")
    all_projects = analyzer.get_all_projects()

    if not all_projects:
        analyzer._log("❌ No projects found or unable to access projects.", "ERROR")
        analyzer._log("   Make sure you're authenticated: gcloud auth login", "INFO")
        return

    # Apply limit if specified
    if args.limit and args.limit < len(all_projects):
        all_projects = all_projects[:args.limit]
        analyzer._log(f"⚠️  Limited to first {args.limit} projects for analysis", "WARN")

    analyzer._log(f"✅ Found {len(all_projects)} project(s) total", "SUCCESS")

    # Filter out already-analyzed projects (resume is default)
    projects = analyzer.get_pending_projects(all_projects)
    if not projects:
        analyzer._log("All projects already analyzed! Generating final report...", "SUCCESS")
        analyzer._log("", "INFO")
        analyzer._log("STEP 3: Generating report...", "INFO")
        analyzer.save_final_report()
        report = analyzer.generate_report(analyzer.get_all_analyses())
        return

    analyzer._log(f"Will analyze {len(projects)} project(s)", "INFO")
    analyzer._log("", "INFO")

    # Analyze each project
    analyzer._log("STEP 2: Analyzing each project...", "INFO")

    if args.sequential:
        # Sequential mode (original behavior)
        analyses = []
        timeout_count = 0
        for i, project in enumerate(projects, 1):
            project_id = project.get('projectId', 'unknown')
            analyzer._log("", "INFO")
            analyzer._log(f"[{i}/{len(projects)}] Starting analysis...", "INFO")
            try:
                analysis = analyzer.analyze_project(project)
                analyzer.add_analysis(project_id, analysis)
                analyses.append(analysis)
                analyzer._log(f"[{i}/{len(projects)}] Completed successfully", "SUCCESS")
            except subprocess.TimeoutExpired:
                timeout_count += 1
                analyzer._log(f"[{i}/{len(projects)}] Timeout analyzing project", "ERROR")
                if not args.no_skip_timeout:
                    analyzer._log(f"  Skipping project due to timeout", "WARN")
                    continue
                else:
                    analyzer._log(f"  Failing due to --no-skip-timeout flag", "ERROR")
                    break
            except Exception as e:
                analyzer._log(f"[{i}/{len(projects)}] Error analyzing project: {e}", "ERROR")
                continue
    else:
        # Parallel mode (new default - much faster!)
        analyses = analyzer.analyze_projects_parallel(projects)

    # Get all analyses (includes resumed + new)
    all_analyses = analyzer.get_all_analyses()
    if len(all_analyses) > len(analyses):
        analyzer._log(f"Combined {len(all_analyses)} total analyses (existing + new)", "INFO")

    # Mark as complete and print summary
    analyzer._log("", "INFO")
    analyzer._log("STEP 3: Finalizing...", "INFO")
    analyzer.save_final_report()
    report = analyzer.generate_report(all_analyses)

    elapsed_time = time.time() - start_time
    analyzer._log("", "INFO")
    analyzer._log("=" * 80, "INFO")
    analyzer._log("✅ Analysis complete!", "SUCCESS")
    analyzer._log(f"Total time: {elapsed_time:.2f} seconds ({elapsed_time/60:.2f} minutes)", "INFO")
    failed_count = len(projects) - len(analyses)
    if failed_count > 0:
        analyzer._log(f"⚠️  {failed_count} project(s) failed/timed out and were skipped", "WARN")
    analyzer._log("", "INFO")
    analyzer._log("Generated files (updated after every project):", "INFO")
    analyzer._log("  - obsolete_projects_report.json - Full categorized report (also used for resume)", "INFO")
    analyzer._log("  - projects_for_deletion.json - Deletion-ready file", "INFO")
    analyzer._log("", "INFO")
    analyzer._log("Next steps:", "INFO")
    analyzer._log("  1. Review projects_for_deletion.json for deletion candidates", "INFO")
    analyzer._log("  2. Run: python3 delete_projects.py (dry run)", "INFO")
    analyzer._log("  3. Run: python3 delete_projects.py --execute (actual deletion)", "INFO")
    analyzer._log("=" * 80, "INFO")


if __name__ == '__main__':
    main()
