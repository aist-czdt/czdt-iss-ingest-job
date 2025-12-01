#!/usr/bin/env python3
"""
CZDT ISS GeoServer Ingest Tool

A simple script for ingesting geospatial data into GeoServer.
Supports shapefiles, zipped shapefiles, GeoPackages, and PostGIS datastores.

Usage:
    from geoserver_ingest import GeoServerClient
    
    client = GeoServerClient("http://localhost:8080/geoserver", "admin", "geoserver")
    client.create_workspace("my_workspace")
    client.upload_shapefile("data.shp", "my_workspace")

Requirements:
    - geoserver-rest
    - geopandas
    - requests
"""

import os
import tempfile
import zipfile
import argparse
import sys
from pathlib import Path
from typing import Optional, Dict, Any, List, Tuple
import logging

try:
    from geo.Geoserver import Geoserver
except ImportError:
    raise ImportError(
        "geoserver-rest package is required. Install with: pip install geoserver-rest"
    )

import requests
import geopandas as gpd


class GeoServerClient:
    """
    A client for interacting with GeoServer to manage workspaces, datastores, and data.
    
    Example:
        client = GeoServerClient("http://localhost:8080/geoserver", "admin", "geoserver")
        client.create_workspace("my_workspace")
        client.upload_shapefile("data.shp", "my_workspace")
    """

    def __init__(
        self,
        url: str,
        username: str = "admin",
        password: str = "geoserver",
        verify_ssl: bool = True,
    ):
        """
        Initialize GeoServer client.

        Args:
            url: GeoServer base URL (e.g., 'http://localhost:8080/geoserver')
            username: GeoServer admin username
            password: GeoServer admin password
            verify_ssl: Whether to verify SSL certificates
        """
        self.url = url.rstrip("/")
        self.username = username
        self.password = password
        self.verify_ssl = verify_ssl

        # Initialize geoserver-rest client
        self.geo = Geoserver(self.url, username=username, password=password)

        # Set up logging
        self.logger = logging.getLogger(__name__)
        logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')

        # Test connection
        self._test_connection()

    def _test_connection(self) -> None:
        """Test connection to GeoServer."""
        try:
            # Use a simple API call to test connection
            self.geo.get_workspaces()
            self.logger.info(f"Connected to GeoServer at {self.url}")
            # Get parsed workspace count
            workspaces = self.list_workspaces()
            self.logger.info(f"Found {len(workspaces)} existing workspaces")
        except Exception as e:
            raise ConnectionError(f"Failed to connect to GeoServer: {e}")

    def create_workspace(self, workspace_name: str) -> bool:
        """
        Create a new workspace in GeoServer.

        Args:
            workspace_name: Name of the workspace to create

        Returns:
            True if workspace was created or already exists, False otherwise
        """
        try:
            # Check if workspace already exists
            existing_workspaces = self.list_workspaces()
            if workspace_name in [ws.get("name", "") for ws in existing_workspaces]:
                self.logger.info(f"Workspace '{workspace_name}' already exists")
                return True

            # Create new workspace
            result = self.geo.create_workspace(workspace_name)
            if result:
                self.logger.info(f"Successfully created workspace '{workspace_name}'")
                return True
            else:
                self.logger.error(f"Failed to create workspace '{workspace_name}'")
                return False

        except Exception as e:
            self.logger.error(f"Error creating workspace '{workspace_name}': {e}")
            return False

    def create_postgis_datastore(
        self,
        workspace: str,
        datastore_name: str,
        host: str,
        port: int = 5432,
        database: str = "gis",
        schema: str = "public",
        username: str = "postgres",
        password: str = "postgres",
    ) -> bool:
        """
        Create a PostGIS datastore.

        Args:
            workspace: Workspace name
            datastore_name: Name for the datastore
            host: PostgreSQL host
            port: PostgreSQL port
            database: Database name
            schema: Schema name
            username: Database username
            password: Database password

        Returns:
            True if datastore was created successfully, False otherwise
        """
        try:
            result = self.geo.create_datastore(
                datastore_name,
                workspace,
                "postgis",
                {
                    "host": host,
                    "port": port,
                    "database": database,
                    "schema": schema,
                    "user": username,
                    "passwd": password,
                },
            )

            if result:
                self.logger.info(
                    f"Successfully created PostGIS datastore '{datastore_name}' in workspace '{workspace}'"
                )
                return True
            else:
                self.logger.error(
                    f"Failed to create PostGIS datastore '{datastore_name}'"
                )
                return False

        except Exception as e:
            self.logger.error(f"Error creating PostGIS datastore: {e}")
            return False

    def upload_shapefile(
        self,
        file_path: str,
        workspace: str,
        datastore_name: Optional[str] = None,
        layer_name: Optional[str] = None,
    ) -> bool:
        """
        Upload a shapefile (or zipped shapefile) to GeoServer.

        Args:
            file_path: Path to shapefile (.shp) or zip file containing shapefile
            workspace: Target workspace
            datastore_name: Name for the datastore (defaults to filename)
            layer_name: Name for the layer (defaults to filename)

        Returns:
            True if upload was successful, False otherwise
        """
        file_path = Path(file_path)

        # Set default names
        if datastore_name is None:
            datastore_name = file_path.stem
        if layer_name is None:
            layer_name = file_path.stem

        try:
            if file_path.suffix.lower() == ".zip":
                return self._upload_zipped_shapefile(
                    file_path, workspace, datastore_name, layer_name
                )
            elif file_path.suffix.lower() == ".shp":
                return self._upload_shapefile_direct(
                    file_path, workspace, datastore_name, layer_name
                )
            else:
                self.logger.error(f"Unsupported file type: {file_path.suffix}")
                return False

        except Exception as e:
            self.logger.error(f"Error uploading shapefile: {e}")
            return False

    def _upload_zipped_shapefile(
        self, zip_path: Path, workspace: str, datastore_name: str, layer_name: str
    ) -> bool:
        """Upload a zipped shapefile."""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Extract zip file
            with zipfile.ZipFile(zip_path, "r") as zip_ref:
                zip_ref.extractall(temp_dir)

            # Find the .shp file
            shp_files = list(Path(temp_dir).glob("**/*.shp"))
            if not shp_files:
                self.logger.error("No .shp file found in the zip archive")
                return False

            shp_file = shp_files[0]
            return self._upload_shapefile_direct(
                shp_file, workspace, datastore_name, layer_name
            )

    def _upload_shapefile_direct(
        self, shp_path: Path, workspace: str, datastore_name: str, layer_name: str
    ) -> bool:
        """Upload a shapefile directly."""
        try:
            # Validate shapefile components exist
            base_path = shp_path.parent / shp_path.stem
            required_files = [".shp", ".shx", ".dbf"]
            missing_files = []

            for ext in required_files:
                if not (base_path.with_suffix(ext)).exists():
                    missing_files.append(ext)

            if missing_files:
                self.logger.error(
                    f"Missing required shapefile components: {missing_files}"
                )
                return False

            # Use geoserver-rest to upload
            result = self.geo.publish_featurestore(
                workspace=workspace,
                store_name=datastore_name,
                pg_table=str(shp_path),
            )

            if result:
                self.logger.info(
                    f"Successfully uploaded shapefile '{shp_path.name}' as layer '{layer_name}'"
                )
                return True
            else:
                self.logger.error(f"Failed to upload shapefile '{shp_path.name}'")
                return False

        except Exception as e:
            self.logger.error(f"Error uploading shapefile directly: {e}")
            return False

    def upload_geopackage(
        self,
        file_path: str,
        workspace: str,
        datastore_name: Optional[str] = None,
        layer_names: Optional[List[str]] = None,
    ) -> Tuple[bool, List[str]]:
        """
        Upload a GeoPackage file to GeoServer.

        Args:
            file_path: Path to GeoPackage (.gpkg) file
            workspace: Target workspace
            datastore_name: Name for the datastore (defaults to filename)
            layer_names: Specific layer names to publish (publishes all if None)

        Returns:
            Tuple of (success_status, list_of_successful_layer_names)
        """
        file_path = Path(file_path)

        if file_path.suffix.lower() != ".gpkg":
            self.logger.error(f"Expected .gpkg file, got: {file_path.suffix}")
            return False, []

        if datastore_name is None:
            datastore_name = file_path.stem

        try:
            # Get actual table names from GeoPackage for publishing, but use filename for layer name
            actual_table_names = []
            if layer_names is None:
                try:
                    # Read actual table names from the GeoPackage
                    layers = gpd.list_layers(str(file_path))
                    if len(layers) > 0:
                        actual_table_names = list(layers.name)
                        # Use filename as the layer name to avoid conflicts
                        layer_names = [file_path.stem]
                        self.logger.info(f"Found tables in GeoPackage: {actual_table_names}")
                        self.logger.info(f"Will publish as layer name: '{file_path.stem}'")
                    else:
                        # Fallback to filename if no tables found
                        actual_table_names = [file_path.stem]
                        layer_names = [file_path.stem]
                        self.logger.info(f"No tables found, using filename '{file_path.stem}' as layer name")
                except Exception as e:
                    # Fallback to filename if reading fails
                    actual_table_names = [file_path.stem]
                    layer_names = [file_path.stem]
                    self.logger.warning(f"Could not read GeoPackage tables ({e}), using filename '{file_path.stem}' as layer name")
            else:
                # If layer names were specified, use them for both
                actual_table_names = layer_names

            # Get list of layers BEFORE any datastore operations for comparison
            try:
                layers_before = self.list_layers(workspace)
                layer_names_before = set(layer.get('name', '') for layer in layers_before)
                self.logger.info(f"Layers before datastore operations: {sorted(layer_names_before)}")
            except Exception as e:
                self.logger.warning(f"Could not get layers before datastore operations: {e}")
                layer_names_before = set()

            # Check if datastore already exists
            existing_datastores = self.list_datastores(workspace)
            datastore_exists = any(ds.get('name', '') == datastore_name for ds in existing_datastores)
            
            if not datastore_exists:
                self.logger.info(f"Creating GeoPackage datastore '{datastore_name}' (will auto-publish layers)")
                # Create datastore for GeoPackage
                result = self.geo.create_gpkg_datastore(
                    store_name=datastore_name,
                    workspace=workspace,
                    path=str(file_path.absolute())
                )
                print(result)

                if not result:
                    self.logger.error(f"Failed to create GeoPackage datastore '{datastore_name}'")
                    return False, []
                else:
                    self.logger.info(f"Successfully created datastore '{datastore_name}'")
            else:
                self.logger.info(f"Datastore '{datastore_name}' already exists, skipping creation")

            # Verify datastore exists before proceeding
            import time
            time.sleep(1)  # Brief pause to ensure datastore is ready
            
            # Re-check that datastore exists
            existing_datastores = self.list_datastores(workspace)
            datastore_exists = any(ds.get('name', '') == datastore_name for ds in existing_datastores)
            
            if not datastore_exists:
                self.logger.error(f"Datastore '{datastore_name}' was not found after creation attempt")
                return False, []
            
            self.logger.info(f"Datastore '{datastore_name}' confirmed to exist, verifying auto-published layers")
            
            # Wait a moment for GeoServer to auto-publish layers from the datastore
            import time
            time.sleep(3)
            
            # Verify what layers were auto-published
            self.logger.info(f"Verifying auto-published layers from GeoPackage datastore...")
            try:
                # Get list of layers AFTER datastore creation
                layers_after = self.list_layers(workspace)
                layer_names_after = set(layer.get('name', '') for layer in layers_after)
                
                # Find new layers that were created
                new_layers = layer_names_after - layer_names_before
                
                self.logger.info(f"Layers after datastore creation: {sorted(layer_names_after)}")
                self.logger.info(f"Auto-published layers: {sorted(new_layers)}")
                
                # Check if any layers from our GeoPackage were published
                success_count = 0
                successful_layers = []
                expected_layers = actual_table_names
                
                for expected_layer in expected_layers:
                    # Look for exact match or auto-incremented versions
                    matching_layers = [layer for layer in new_layers if layer.startswith(expected_layer)]
                    
                    if matching_layers:
                        created_layer_name = matching_layers[0]
                        self.logger.info(f"✓ Layer '{created_layer_name}' was auto-published from table '{expected_layer}'")
                        if created_layer_name != expected_layer:
                            self.logger.info(f"  Note: GeoServer auto-incremented the name due to conflict")
                        
                        final_layer_name = created_layer_name  # Default to created name
                        
                        # Rename layer to match filename using direct REST API
                        try:
                            self.logger.info(f"Renaming layer '{created_layer_name}' to '{datastore_name}'")
                            
                            # Build URLs
                            layer_url = f"{self.geo.service_url}/rest/layers/{workspace}:{created_layer_name}"
                            resource_url = f"{self.geo.service_url}/rest/workspaces/{workspace}/datastores/{datastore_name}/featuretypes/{created_layer_name}"
                            
                            headers = {'Content-Type': 'application/json'}
                            auth = (self.geo.username, self.geo.password)
                            
                            # Update the resource (feature type) name first
                            resource_data = {
                                "featureType": {
                                    "name": datastore_name,
                                    "nativeName": expected_layer  # Keep original table name as nativeName
                                }
                            }
                            
                            put_response = requests.put(resource_url, json=resource_data, headers=headers, auth=auth)
                            
                            if put_response.status_code == 200:
                                self.logger.info(f"✓ Successfully renamed layer to '{datastore_name}'")
                                final_layer_name = datastore_name  # Use renamed name
                            else:
                                self.logger.warning(f"⚠ Failed to rename layer: {put_response.status_code} - {put_response.text}")
                                self.logger.info(f"  Keeping original name '{created_layer_name}'")
                                
                        except Exception as e:
                            self.logger.warning(f"⚠ Error renaming layer: {e}")
                            self.logger.info(f"  Keeping original name '{created_layer_name}'")
                        
                        successful_layers.append(final_layer_name)
                        success_count += 1
                    else:
                        # Check if the layer already existed
                        if expected_layer in layer_names_after:
                            self.logger.warning(f"⚠ Layer '{expected_layer}' already exists (not created by this upload)")
                        else:
                            self.logger.warning(f"✗ Expected layer '{expected_layer}' was not auto-published")
                
                # Also count any other new layers as potential successes
                if new_layers and success_count == 0:
                    self.logger.info(f"✓ {len(new_layers)} layer(s) were auto-published, though names may differ from expectations")
                    successful_layers.extend(list(new_layers))
                    success_count = len(new_layers)
                
                if success_count > 0:
                    self.logger.info(f"Successfully uploaded GeoPackage with {success_count} auto-published layer(s)")
                    return True, successful_layers
                else:
                    self.logger.error("No layers were auto-published from GeoPackage")
                    return False, []
                    
            except Exception as verify_error:
                self.logger.warning(f"Could not verify auto-published layers: {verify_error}")
                # If we can't verify, assume success if datastore was created successfully
                self.logger.info("Assuming upload was successful since datastore was created")
                return True, [datastore_name]  # Return datastore name as fallback

        except Exception as e:
            self.logger.error(f"Error uploading GeoPackage: {e}")
            return False, []

    def list_workspaces(self) -> List[Dict[str, Any]]:
        """
        Get list of all workspaces.

        Returns:
            List of workspace information dictionaries
        """
        try:
            response = self.geo.get_workspaces()
            # Handle nested response structure: {'workspaces': {'workspace': [...]}}
            if isinstance(response, dict):
                if 'workspaces' in response and 'workspace' in response['workspaces']:
                    return response['workspaces']['workspace']
                elif 'workspaces' in response:
                    return response['workspaces']
            return response if isinstance(response, list) else []
        except Exception as e:
            self.logger.error(f"Error listing workspaces: {e}")
            return []

    def list_datastores(self, workspace: str) -> List[Dict[str, Any]]:
        """
        Get list of datastores in a workspace.

        Args:
            workspace: Workspace name

        Returns:
            List of datastore information dictionaries
        """
        try:
            response = self.geo.get_datastores(workspace)
            # Handle nested response structure: {'dataStores': {'dataStore': [...]}}
            if isinstance(response, dict):
                if 'dataStores' in response and 'dataStore' in response['dataStores']:
                    return response['dataStores']['dataStore']
                elif 'dataStores' in response:
                    return response['dataStores']
            return response if isinstance(response, list) else []
        except Exception as e:
            self.logger.error(f"Error listing datastores for workspace '{workspace}': {e}")
            return []

    def list_layers(self, workspace: str) -> List[Dict[str, Any]]:
        """
        Get list of layers in a workspace.

        Args:
            workspace: Workspace name

        Returns:
            List of layer information dictionaries
        """
        try:
            response = self.geo.get_layers(workspace)
            # Handle nested response structure: {'layers': {'layer': [...]}}
            if isinstance(response, dict):
                if 'layers' in response and 'layer' in response['layers']:
                    return response['layers']['layer']
                elif 'layers' in response:
                    return response['layers']
            return response if isinstance(response, list) else []
        except Exception as e:
            self.logger.error(f"Error listing layers for workspace '{workspace}': {e}")
            return []




def create_parser():
    """Create and configure the argument parser."""
    parser = argparse.ArgumentParser(
        description="CZDT ISS GeoServer Ingest Tool - Manage GeoServer workspaces and data",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s create-workspace my_workspace
  %(prog)s upload-shapefile data.shp my_workspace  
  %(prog)s upload-geopackage data.gpkg my_workspace
  %(prog)s list-workspaces
  %(prog)s list-layers my_workspace

Environment Variables:
  GEOSERVER_URL      GeoServer base URL (default: http://localhost:8080/geoserver)
  GEOSERVER_USERNAME GeoServer username (default: admin)
  GEOSERVER_PASSWORD GeoServer password (default: geoserver)
        """
    )
    
    # Global connection options
    parser.add_argument(
        "--url",
        default=os.getenv("GEOSERVER_URL", "http://localhost:8080/geoserver"),
        help="GeoServer base URL (default: %(default)s)"
    )
    parser.add_argument(
        "--username", "-u",
        default=os.getenv("GEOSERVER_USERNAME", "admin"),
        help="GeoServer username (default: %(default)s)"
    )
    parser.add_argument(
        "--password", "-p",
        default=os.getenv("GEOSERVER_PASSWORD", "geoserver"),
        help="GeoServer password (default: %(default)s)"
    )
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Enable verbose logging"
    )
    
    # Subcommands
    subparsers = parser.add_subparsers(dest="command", help="Available commands")
    
    # create-workspace
    create_ws_parser = subparsers.add_parser(
        "create-workspace",
        help="Create a new workspace"
    )
    create_ws_parser.add_argument("workspace_name", help="Name of the workspace to create")
    
    # upload-shapefile
    upload_shp_parser = subparsers.add_parser(
        "upload-shapefile",
        help="Upload a shapefile (.shp or .zip)"
    )
    upload_shp_parser.add_argument("file_path", help="Path to shapefile (.shp) or zip file")
    upload_shp_parser.add_argument("workspace", help="Target workspace")
    upload_shp_parser.add_argument("--datastore", help="Datastore name (defaults to filename)")
    upload_shp_parser.add_argument("--layer", help="Layer name (defaults to filename)")
    
    # upload-geopackage
    upload_gpkg_parser = subparsers.add_parser(
        "upload-geopackage",
        help="Upload a GeoPackage (.gpkg)"
    )
    upload_gpkg_parser.add_argument("file_path", help="Path to GeoPackage (.gpkg) file")
    upload_gpkg_parser.add_argument("workspace", help="Target workspace")
    upload_gpkg_parser.add_argument("--datastore", help="Datastore name (defaults to filename)")
    upload_gpkg_parser.add_argument("--layers", help="Comma-separated list of layer names to publish")
    
    # create-postgis-store
    postgis_parser = subparsers.add_parser(
        "create-postgis-store",
        help="Create a PostGIS datastore"
    )
    postgis_parser.add_argument("workspace", help="Workspace name")
    postgis_parser.add_argument("datastore_name", help="Name for the datastore")
    postgis_parser.add_argument("--host", required=True, help="PostgreSQL host")
    postgis_parser.add_argument("--port", type=int, default=5432, help="PostgreSQL port (default: 5432)")
    postgis_parser.add_argument("--database", default="gis", help="Database name (default: gis)")
    postgis_parser.add_argument("--schema", default="public", help="Schema name (default: public)")
    postgis_parser.add_argument("--db-username", default="postgres", help="Database username (default: postgres)")
    postgis_parser.add_argument("--db-password", default="postgres", help="Database password (default: postgres)")
    
    # list-workspaces
    subparsers.add_parser(
        "list-workspaces",
        help="List all workspaces"
    )
    
    # list-datastores
    list_ds_parser = subparsers.add_parser(
        "list-datastores",
        help="List datastores in a workspace"
    )
    list_ds_parser.add_argument("workspace", help="Workspace name")
    
    # list-layers
    list_layers_parser = subparsers.add_parser(
        "list-layers",
        help="List layers in a workspace"
    )
    list_layers_parser.add_argument("workspace", help="Workspace name")
    

    

    
    return parser


def main():
    """Main CLI entry point."""
    parser = create_parser()
    
    # Show help if no arguments provided
    if len(sys.argv) == 1:
        parser.print_help()
        return
        
    args = parser.parse_args()
    
    # Set up logging based on verbose flag
    if args.verbose:
        logging.basicConfig(level=logging.DEBUG, format='%(levelname)s: %(message)s')
    else:
        logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
    
    # No command specified
    if not args.command:
        parser.print_help()
        return
    
    try:
        # Initialize GeoServer client
        client = GeoServerClient(args.url, args.username, args.password)
        
        # Execute command
        if args.command == "create-workspace":
            success = client.create_workspace(args.workspace_name)
            if success:
                print(f"✓ Workspace '{args.workspace_name}' created successfully")
            else:
                print(f"✗ Failed to create workspace '{args.workspace_name}'")
                sys.exit(1)
                
        elif args.command == "upload-shapefile":
            success = client.upload_shapefile(
                file_path=args.file_path,
                workspace=args.workspace,
                datastore_name=args.datastore,
                layer_name=args.layer
            )
            if success:
                print(f"✓ Shapefile uploaded successfully to workspace '{args.workspace}'")
            else:
                print("✗ Failed to upload shapefile")
                sys.exit(1)
                
        elif args.command == "upload-geopackage":
            layer_names = None
            if args.layers:
                layer_names = [layer.strip() for layer in args.layers.split(",")]
            
            success, successful_layer_names = client.upload_geopackage(
                file_path=args.file_path,
                workspace=args.workspace,
                datastore_name=args.datastore,
                layer_names=layer_names
            )
            if success:
                print(f"✓ GeoPackage uploaded successfully")
                for layer_name in successful_layer_names:
                    print(f"{args.workspace}:{layer_name}")
            else:
                print("✗ Failed to upload GeoPackage")
                sys.exit(1)
                
        elif args.command == "create-postgis-store":
            success = client.create_postgis_datastore(
                workspace=args.workspace,
                datastore_name=args.datastore_name,
                host=args.host,
                port=args.port,
                database=args.database,
                schema=args.schema,
                username=args.db_username,
                password=args.db_password
            )
            if success:
                print(f"✓ PostGIS datastore '{args.datastore_name}' created successfully")
            else:
                print(f"✗ Failed to create PostGIS datastore '{args.datastore_name}'")
                sys.exit(1)
                
        elif args.command == "list-workspaces":
            workspaces = client.list_workspaces()
            if workspaces:
                print(f"Found {len(workspaces)} workspaces:")
                for ws in workspaces:
                    name = ws.get('name', 'Unknown') if isinstance(ws, dict) else str(ws)
                    print(f"  - {name}")
            else:
                print("No workspaces found")
                
        elif args.command == "list-datastores":
            datastores = client.list_datastores(args.workspace)
            if datastores:
                print(f"Found {len(datastores)} datastores in workspace '{args.workspace}':")
                for ds in datastores:
                    name = ds.get('name', 'Unknown') if isinstance(ds, dict) else str(ds)
                    print(f"  - {name}")
            else:
                print(f"No datastores found in workspace '{args.workspace}'")
                
        elif args.command == "list-layers":
            layers = client.list_layers(args.workspace)
            if layers:
                print(f"Found {len(layers)} layers in workspace '{args.workspace}':")
                for layer in layers:
                    name = layer.get('name', 'Unknown') if isinstance(layer, dict) else str(layer)
                    print(f"  - {name}")
            else:
                print(f"No layers found in workspace '{args.workspace}'")
                
        
            
    except ConnectionError as e:
        print(f"✗ Connection Error: {e}")
        print("Make sure GeoServer is running and accessible")
        sys.exit(1)
    except Exception as e:
        print(f"✗ Error: {e}")
        if args.verbose:
            import traceback
            traceback.print_exc()
        sys.exit(1)




if __name__ == "__main__":
    main() 