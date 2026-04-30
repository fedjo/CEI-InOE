"""
Bootstrap configuration from YAML files.

Loads datasources.yaml and site_config.yaml from the conf directory
and upserts records into the database on ingestor startup.
"""

import logging
from pathlib import Path

import yaml

from shared.database import session_scope
from shared.models import Datasource, Site, SiteType

logger = logging.getLogger(__name__)


def _load_yaml(path: Path) -> dict:
    """Read and parse a YAML file."""
    with open(path) as f:
        return yaml.safe_load(f) or {}


# ============================================================================
# Datasource bootstrap
# ============================================================================

def _upsert_datasources(session, entries: list[dict]) -> int:
    """Upsert datasource rows by external_id. Returns count of upserted rows."""
    count = 0
    for entry in entries:
        ext_id = entry["external_id"]
        ds = session.query(Datasource).filter(Datasource.external_id == ext_id).first()

        if ds is None:
            ds = Datasource(
                external_id=ext_id,
                source_category=entry["source_category"],
                data_type=entry["data_type"],
                alias=entry.get("alias"),
                client=entry.get("client", ""),
                description=entry.get("description"),
                status="offline",  # Default all datasources to offline
                timezone=entry.get("timezone", "UTC"),
                metadata_=entry.get("metadata", {}),
            )
            session.add(ds)
            logger.debug("Created datasource '%s'", ext_id)
        else:
            ds.source_category = entry["source_category"]
            ds.data_type = entry["data_type"]
            ds.alias = entry.get("alias")
            ds.client = entry.get("client", "")
            ds.description = entry.get("description")
            ds.status = "offline"  # Default all datasources to offline
            ds.timezone = entry.get("timezone", "UTC")
            ds.metadata_ = entry.get("metadata", {})
            logger.debug("Updated datasource '%s'", ext_id)
        count += 1

    session.flush()
    return count


# ============================================================================
# Site bootstrap
# ============================================================================

def _upsert_site(session, cfg: dict) -> Site:
    """Upsert a Site row and link its datasources."""
    name = cfg["name"]
    site_type = SiteType(cfg["site_type"])
    location = cfg["location"]
    owner = cfg["owner"]
    admin_email = cfg["administrator_email"]

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

    session.flush()

    # Link datasources by external_id and activate them
    ext_ids = cfg.get("data_sources", [])
    if ext_ids:
        datasources = (
            session.query(Datasource)
            .filter(Datasource.external_id.in_(ext_ids))
            .all()
        )
        for ds in datasources:
            ds.site_id = site.id
            ds.status = "online"  # Activate datasources linked to this site

        found_ids = {ds.external_id for ds in datasources}
        missing = set(ext_ids) - found_ids
        if missing:
            logger.warning("Datasources not found for site linking: %s", missing)
        logger.info("Linked and activated %d/%d datasources for site '%s'",
                     len(found_ids), len(ext_ids), name)

    return site


# ============================================================================
# Public entry point
# ============================================================================

def bootstrap_from_conf(conf_dir: str) -> None:
    """
    Load datasources.yaml and site_config.yaml from *conf_dir* and upsert
    into the database.  Datasources are loaded first so that site linking
    can resolve external_ids immediately.
    """
    conf = Path(conf_dir)
    if not conf.is_dir():
        logger.warning("Conf directory not found: %s — skipping bootstrap", conf_dir)
        return

    with session_scope() as session:
        # 1. Datasources (must come first)
        ds_path = conf / "datasources.yaml"
        if ds_path.exists():
            data = _load_yaml(ds_path)
            entries = data.get("datasources", [])
            n = _upsert_datasources(session, entries)
            logger.info("Bootstrapped %d datasources from %s", n, ds_path)
        else:
            logger.info("No datasources.yaml in %s — skipping", conf_dir)

        # 2. Site (links to datasources)
        site_path = conf / "site_config.yaml"
        if site_path.exists():
            data = _load_yaml(site_path)
            site_cfg = data.get("site")
            if site_cfg:
                _upsert_site(session, site_cfg)
            else:
                logger.warning("site_config.yaml has no 'site' key — skipping")
        else:
            logger.info("No site_config.yaml in %s — skipping", conf_dir)
