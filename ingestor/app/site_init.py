"""
Site initialisation from YAML configuration.

Reads a YAML file describing the site and upserts the Site record
(and links its Datasources) on ingestor startup.
"""

import logging
from pathlib import Path

import yaml

from shared.database import session_scope
from shared.models import Datasource, Site, SiteType

logger = logging.getLogger(__name__)


def _load_yaml(path: str | Path) -> dict:
    """Parse and validate the site YAML config file."""
    path = Path(path)
    if not path.exists():
        raise FileNotFoundError(f"Site config not found: {path}")

    with open(path) as f:
        raw = yaml.safe_load(f)

    cfg = raw.get("site")
    if not cfg:
        raise ValueError("site_config.yaml must contain a top-level 'site' key")

    # Validate required fields
    for key in ("name", "location", "site_type", "owner", "administrator_email"):
        if key not in cfg:
            raise ValueError(f"site_config.yaml: missing required field 'site.{key}'")

    # Validate site_type against the enum
    SiteType(cfg["site_type"])

    # Validate GeoJSON location
    loc = cfg["location"]
    if loc.get("type") != "Point" or not isinstance(loc.get("coordinates"), list):
        raise ValueError("site.location must be a GeoJSON Point with a coordinates list")

    return cfg


def init_site_from_yaml(config_path: str | Path) -> None:
    """Upsert the Site row from a YAML config file and link datasources."""
    cfg = _load_yaml(config_path)

    name = cfg["name"]
    site_type = SiteType(cfg["site_type"])
    location = cfg["location"]
    owner = cfg["owner"]
    admin_email = cfg["administrator_email"]

    with session_scope() as session:
        site = session.query(Site).filter(Site.name == name).first()

        if site is None:
            site = Site(
                name=name,
                location=location,
                site_type=site_type,
                owner=owner,
                administrator_email=admin_email,
            )
            session.add(site)
            logger.info("Created site '%s'", name)
        else:
            site.location = location
            site.site_type = site_type
            site.owner = owner
            site.administrator_email = admin_email
            logger.info("Updated site '%s'", name)

        session.flush()  # ensure site.id is available

        # Link datasources by external_id
        ext_ids = cfg.get("data_sources", [])
        if ext_ids:
            datasources = (
                session.query(Datasource)
                .filter(Datasource.external_id.in_(ext_ids))
                .all()
            )
            for ds in datasources:
                ds.site_id = site.id

            found_ids = {ds.external_id for ds in datasources}
            missing = set(ext_ids) - found_ids
            if missing:
                logger.warning("Datasources not found for linking: %s", missing)
            logger.info(
                "Linked %d/%d datasources to site '%s'",
                len(found_ids), len(ext_ids), name,
            )
